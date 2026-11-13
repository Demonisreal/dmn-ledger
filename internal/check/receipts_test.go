package check

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/disk"
	"github.com/Demonisreal/dmn-ledger/internal/raft"
	"github.com/Demonisreal/dmn-ledger/internal/replica"
	"github.com/Demonisreal/dmn-ledger/internal/sim"
)

func calmWatch(t *testing.T) (*watch, replica.Status, replica.Status) {
	t.Helper()
	w := newWatch(Config{Seed: 1})
	w.sample = 1
	c, err := sim.NewCluster(sim.ClusterConfig{Seed: 1, Profile: sim.ProfileCalm, Nodes: 3})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Run(); err != nil {
		t.Fatal(err)
	}
	w.c = c
	var lead, follower replica.Status
	for id := uint64(1); id <= 3; id++ {
		st := c.Node(id).Status()
		if st.Role == raft.Leader {
			lead = st
		} else {
			follower = st
		}
	}
	if lead.ID == 0 || lead.Vote != lead.ID || follower.Vote == follower.ID {
		t.Fatalf("leader %+v, follower %+v", lead, follower)
	}
	return w, lead, follower
}

func TestKeepsComparesVotesWithTheDisk(t *testing.T) {
	w, lead, follower := calmWatch(t)
	cases := []struct {
		name   string
		from   uint64
		m      raft.Message
		reason string
	}{
		{"leader asked for its own votes", lead.ID, raft.Message{Type: raft.MsgVote, Term: lead.Term}, ""},
		{"follower never asked", follower.ID, raft.Message{Type: raft.MsgVote, Term: follower.Term}, "asked for votes"},
		{"vote for another node", lead.ID, raft.Message{Type: raft.MsgVoteResp, To: follower.ID, Term: lead.Term}, "granted"},
		{"vote in a term the disk does not hold", lead.ID, raft.Message{Type: raft.MsgVote, Term: lead.Term + 1}, "holds term"},
		{"disk holds a later term", follower.ID, raft.Message{Type: raft.MsgAppResp, Index: 1, Term: follower.Term - 1}, "holds term"},
		{"acknowledged what the disk holds", follower.ID, raft.Message{Type: raft.MsgAppResp, Index: follower.Last, Term: follower.Term}, ""},
		{"acknowledged past the end", follower.ID, raft.Message{Type: raft.MsgAppResp, Index: follower.Last + 1, Term: follower.Term}, "acknowledged"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wantViolation(t, w.keeps(tc.from, tc.m), "S5", tc.reason)
		})
	}
}

func TestReceiptsAreCheckedOnSync(t *testing.T) {
	type said struct {
		from uint64
		m    raft.Message
	}
	cases := []struct {
		name    string
		said    func(lead, follower replica.Status) []said
		viaFile bool
		reason  string
	}{
		{"everything on disk", func(lead, follower replica.Status) []said {
			return []said{
				{lead.ID, raft.Message{Type: raft.MsgVote, Term: lead.Term}},
				{follower.ID, raft.Message{Type: raft.MsgVoteResp, To: lead.ID, Term: lead.Term}},
				{follower.ID, raft.Message{Type: raft.MsgAppResp, Index: follower.Last, Term: follower.Term}},
			}
		}, false, ""},
		{"own vote request", func(_, follower replica.Status) []said {
			return []said{{follower.ID, raft.Message{Type: raft.MsgVote, Term: follower.Term}}}
		}, true, "voted for"},
		{"granted vote", func(lead, follower replica.Status) []said {
			return []said{{lead.ID, raft.Message{Type: raft.MsgVoteResp, To: follower.ID, Term: lead.Term}}}
		}, false, "voted for"},
		{"acknowledged past the end", func(_, follower replica.Status) []said {
			return []said{{follower.ID, raft.Message{Type: raft.MsgAppResp, Index: follower.Last + 1, Term: follower.Term}}}
		}, true, "acknowledged up to"},
		{"acknowledgement of an older term", func(_, follower replica.Status) []said {
			return []said{
				{follower.ID, raft.Message{Type: raft.MsgAppResp, Index: follower.Last + 100, Term: follower.Term - 1}},
				{follower.ID, raft.Message{Type: raft.MsgAppResp, Index: follower.Last, Term: follower.Term}},
			}
		}, false, ""},
		{"late acknowledgement of less", func(_, follower replica.Status) []said {
			return []said{
				{follower.ID, raft.Message{Type: raft.MsgAppResp, Index: follower.Last + 1, Term: follower.Term}},
				{follower.ID, raft.Message{Type: raft.MsgAppResp, Index: follower.Last - 1, Term: follower.Term}},
			}
		}, false, "acknowledged up to"},
		{"later term", func(lead, _ replica.Status) []said {
			return []said{{lead.ID, raft.Message{Type: raft.MsgVote, Term: lead.Term + 1}}}
		}, false, "promised in term"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w, lead, follower := calmWatch(t)
			var id uint64
			for _, s := range tc.said(lead, follower) {
				if err := w.said(s.from, []raft.Message{s.m}); err != nil {
					t.Fatal(err)
				}
				id = s.from
			}
			fsys := &watchedFS{FS: w.c.World().Disk(id), w: w, id: id}
			if tc.viaFile {
				f, err := fsys.Open("/ledger/meta")
				if err != nil {
					t.Fatal(err)
				}
				if err := f.Sync(); err != nil {
					t.Fatal(err)
				}
			} else if err := fsys.SyncDir("/ledger"); err != nil {
				t.Fatal(err)
			}
			wantViolation(t, w.stuck, "S5", tc.reason)
		})
	}
}

func TestSaidChecksVoteRequestsAgainstTheDisk(t *testing.T) {
	w, _, follower := calmWatch(t)
	w.promises = promiseSample - 1
	wantViolation(t, w.said(follower.ID, []raft.Message{{Type: raft.MsgVote, Term: follower.Term}}), "S5", "asked for votes")
}

type unsyncedHardState struct {
	disk.FS
}

type unsyncedFile struct {
	disk.File
}

func (unsyncedFile) Sync() error {
	return nil
}

func (u unsyncedHardState) Create(name string) (disk.File, error) {
	f, err := u.FS.Create(name)
	if err != nil || !strings.HasPrefix(filepath.Base(name), "meta") {
		return f, err
	}
	return unsyncedFile{f}, nil
}

func (u unsyncedHardState) Open(name string) (disk.File, error) {
	f, err := u.FS.Open(name)
	if err != nil || !strings.HasPrefix(filepath.Base(name), "meta") {
		return f, err
	}
	return unsyncedFile{f}, nil
}

func TestPowerLossInTheMiddleOfARound(t *testing.T) {
	w := newWatch(Config{Seed: 1, Profile: sim.ProfileCalm})
	w.sample = 1
	watched := w.disk()
	_, err := w.run(func(id uint64, fsys disk.FS) disk.FS { return watched(id, unsyncedHardState{fsys}) })
	wantViolation(t, err, "S5", "in the middle of a round")
}
