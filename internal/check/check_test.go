package check

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/anishathalye/porcupine"

	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/disk"
	"github.com/Demonisreal/dmn-ledger/internal/oracle"
	"github.com/Demonisreal/dmn-ledger/internal/raft"
	"github.com/Demonisreal/dmn-ledger/internal/sim"
	"github.com/Demonisreal/dmn-ledger/internal/state"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
)

var profiles = sim.Profiles()

func TestSeeds(t *testing.T) {
	for seed := range uint64(50) {
		t.Run(strconv.FormatUint(seed, 10), func(t *testing.T) {
			t.Parallel()
			p := profiles[seed%uint64(len(profiles))]
			rep, err := Run(Config{Seed: seed, Profile: p})
			if err != nil {
				t.Fatalf("profile %s: %v", p, err)
			}
			if rep.Calls == 0 || rep.Widest > 32 {
				t.Fatalf("profile %s: %d calls, widest window %d", p, rep.Calls, rep.Widest)
			}
		})
	}
}

func TestTraceHashFullStack(t *testing.T) {
	seeds := uint64(200)
	if testing.Short() {
		seeds = 40
	}
	for seed := range seeds {
		t.Run(strconv.FormatUint(seed, 10), func(t *testing.T) {
			t.Parallel()
			p := profiles[seed%uint64(len(profiles))]
			a, err := Run(Config{Seed: seed, Profile: p})
			if err != nil {
				t.Fatal(err)
			}
			b, err := Run(Config{Seed: seed, Profile: p})
			if err != nil {
				t.Fatal(err)
			}
			if a.Trace != b.Trace {
				t.Fatalf("profile %s: trace hash %x then %x", p, a.Trace, b.Trace)
			}
			c, err := sim.NewCluster(sim.ClusterConfig{Seed: seed, Profile: p})
			if err != nil {
				t.Fatal(err)
			}
			if err := c.Run(); err != nil {
				t.Fatal(err)
			}
			if h := c.World().TraceHash(); h != a.Trace {
				t.Fatalf("profile %s: the checkers changed the run, trace hash %x without them, %x with", p, h, a.Trace)
			}
		})
	}
}

func TestEvictProfileEvicts(t *testing.T) {
	evicted, rejoined, late := 0, 0, 0
	for seed := range uint64(20) {
		c, err := sim.NewCluster(sim.ClusterConfig{Seed: seed, Profile: sim.ProfileEvict})
		if err != nil {
			t.Fatal(err)
		}
		if err := c.Run(); err != nil {
			t.Fatal(err)
		}
		ids := make(map[uint64]map[u128.Uint128]bool)
		first := make(map[uint64]uint64)
		for _, call := range c.Calls() {
			if call.Reply.Status == client.SessionUnknown {
				evicted++
			}
			if ids[call.Client] == nil {
				ids[call.Client] = make(map[u128.Uint128]bool)
				first[call.Client] = call.Start
			}
			ids[call.Client][call.ID] = true
		}
		for addr, set := range ids {
			if len(set) > 1 {
				rejoined++
			}
			if first[addr] > c.World().Now()/4 {
				late++
			}
		}
	}
	if evicted == 0 || rejoined == 0 || late == 0 {
		t.Fatalf("in 20 seeds %d calls got SessionUnknown, %d clients opened a second session, %d started late", evicted, rejoined, late)
	}
}

type refusingFS struct {
	disk.FS
}

func (r refusingFS) Open(name string) (disk.File, error) {
	if strings.HasSuffix(name, "hardstate") {
		return nil, errors.New("hard state refused")
	}
	return r.FS.Open(name)
}

func TestStartFailureIsAFinding(t *testing.T) {
	w := &watch{cfg: Config{Seed: 3}}
	eio := fmt.Errorf("replica: %w", &fs.PathError{Op: "write", Path: "/ledger/hardstate", Err: syscall.EIO})
	if err := w.failed(2, eio); err != nil {
		t.Fatalf("a disk error is no finding: %v", err)
	}
	c, err := sim.NewCluster(sim.ClusterConfig{
		Seed: 3,
		Disk: func(id uint64, fsys disk.FS) disk.FS {
			if id == 2 {
				return refusingFS{fsys}
			}
			return fsys
		},
		Failed: w.failed,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = c.Run()
	v, ok := errors.AsType[*Violation](err)
	if !ok || v.Check != "F1" || v.Node != 2 || !strings.Contains(v.Reason, "hard state refused") {
		t.Fatalf("want F1 on n2, got %v", err)
	}
	if now := c.World().Now(); now > 20_000_000 {
		t.Fatalf("the run went on until %d", now)
	}
}

func TestSaidFindsTwoLeadersAndTwoVotes(t *testing.T) {
	type said struct {
		from uint64
		m    raft.Message
	}
	cases := []struct {
		name  string
		check string
		msgs  []said
	}{
		{"two leaders", "S1", []said{
			{1, raft.Message{Type: raft.MsgApp, To: 2, Term: 3}},
			{2, raft.Message{Type: raft.MsgSnap, To: 3, Term: 3}},
		}},
		{"two votes", "S2", []said{
			{3, raft.Message{Type: raft.MsgVoteResp, To: 1, Term: 4}},
			{3, raft.Message{Type: raft.MsgVoteResp, To: 2, Term: 4}},
		}},
		{"leaders of different terms", "", []said{
			{1, raft.Message{Type: raft.MsgApp, To: 2, Term: 3}},
			{2, raft.Message{Type: raft.MsgApp, To: 1, Term: 4}},
			{3, raft.Message{Type: raft.MsgVoteResp, To: 1, Term: 4, Reject: true}},
			{3, raft.Message{Type: raft.MsgVoteResp, To: 2, Term: 4}},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newWatch(Config{})
			var err error
			for _, s := range tc.msgs {
				if err = w.said(s.from, []raft.Message{s.m}); err != nil {
					break
				}
			}
			v, ok := errors.AsType[*Violation](err)
			switch {
			case tc.check == "" && err != nil:
				t.Fatalf("unexpected %v", err)
			case tc.check != "" && (!ok || v.Check != tc.check):
				t.Fatalf("want %s, got %v", tc.check, err)
			}
		})
	}
}

func TestModelRejectsStaleRead(t *testing.T) {
	acct := state.Account{ID: u128.Uint128{Lo: 7}, Ledger: 1, Code: 1}
	body := state.AppendAccount(nil, acct)
	write := &sim.Call{Op: client.CreateAccounts, Body: body, Start: 10, End: 20, Reply: client.Reply{Status: client.OK, Time: 100}}
	read := func(start, at uint64, found bool) *sim.Call {
		r := &sim.Call{Op: client.LookupAccounts, Body: acct.ID.AppendBytes(nil), Start: start, End: start + 10, Reply: client.Reply{Status: client.OK, Time: at}}
		if found {
			a := acct
			a.Timestamp = 100
			r.Reply.Body = state.AppendAccount(nil, a)
		}
		return r
	}
	cases := []struct {
		name string
		read *sim.Call
		ok   bool
	}{
		{"fresh read after the write", read(30, 100, true), true},
		{"stale read after the write", read(30, 0, false), false},
		{"read overlapping the write sees nothing", read(15, 0, false), true},
		{"read with a time no write gave", read(30, 99, true), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ops := []porcupine.Operation{
				{Input: write, Call: int64(write.Start), Return: int64(write.End)},
				{Input: tc.read, Call: int64(tc.read.Start), Return: int64(tc.read.End)},
			}
			if got := porcupine.CheckOperations(model(oracle.New()), ops); got != tc.ok {
				t.Fatalf("linearizable %v, want %v", got, tc.ok)
			}
		})
	}
}

func TestModelNeedsTheReplyOfTheOracle(t *testing.T) {
	tr := state.Transfer{ID: u128.Uint128{Lo: 1}, DebitAccountID: u128.Uint128{Lo: 8}, CreditAccountID: u128.Uint128{Lo: 9}, Amount: u128.Uint128{Lo: 5}, Ledger: 1, Code: 1}
	body := state.AppendTransfer(nil, tr)
	var fails []byte
	fails = binary.BigEndian.AppendUint32(fails, 0)
	fails = binary.BigEndian.AppendUint32(fails, uint32(state.DebitAccountNotFound))
	for _, tc := range []struct {
		name  string
		reply []byte
		ok    bool
	}{
		{"oracle failure", fails, true},
		{"claims success", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			call := &sim.Call{Op: client.CreateTransfers, Body: body, Start: 1, End: 2, Reply: client.Reply{Status: client.OK, Time: 50, Body: tc.reply}}
			ops := []porcupine.Operation{{Input: call, Call: 1, Return: 2}}
			if got := porcupine.CheckOperations(model(oracle.New()), ops); got != tc.ok {
				t.Fatalf("linearizable %v, want %v", got, tc.ok)
			}
		})
	}
}

func TestSweepReportsTheFirstFailingSeed(t *testing.T) {
	for _, workers := range []int{1, 3, 8} {
		seed, err := Sweep(10, 100, workers, func(seed uint64) error {
			if seed == 34 || seed == 60 || seed == 91 {
				return errors.New("bad")
			}
			return nil
		})
		if err == nil || seed != 34 {
			t.Fatalf("%d workers: seed %d, error %v, want 34", workers, seed, err)
		}
	}
	var mu sync.Mutex
	var tried []uint64
	if _, err := Sweep(10, 100, 4, func(seed uint64) error {
		mu.Lock()
		tried = append(tried, seed)
		mu.Unlock()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	slices.Sort(tried)
	if len(tried) != 100 {
		t.Fatalf("tried %d seeds, want 100", len(tried))
	}
	for i, seed := range tried {
		if seed != 10+uint64(i) {
			t.Fatalf("tried %v, want 10 to 109 once each", tried)
		}
	}
}

func TestSweepWaitsForTheSmallerSeed(t *testing.T) {
	late := make(chan struct{})
	seed, err := Sweep(10, 100, 4, func(seed uint64) error {
		switch seed {
		case 34:
			<-late
			return errors.New("bad")
		case 60:
			close(late)
			return errors.New("bad")
		}
		return nil
	})
	if err == nil || seed != 34 {
		t.Fatalf("seed %d, error %v, want 34", seed, err)
	}
}

func TestSweepTurnsAPanicIntoAFinding(t *testing.T) {
	for _, workers := range []int{1, 4} {
		seed, err := Sweep(10, 100, workers, func(seed uint64) error {
			switch seed {
			case 34:
				panic("no log for n2")
			case 60:
				return errors.New("bad")
			}
			return nil
		})
		p, ok := errors.AsType[*Panic](err)
		if !ok || seed != 34 || p.Seed != 34 || !strings.Contains(p.Error(), "no log for n2") {
			t.Fatalf("%d workers: seed %d, error %v, want the panic of seed 34", workers, seed, err)
		}
	}
}
