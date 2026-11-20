package server

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/raft"
	"github.com/Demonisreal/dmn-ledger/internal/wal"
	"github.com/Demonisreal/dmn-ledger/internal/wire"
)

func fill(c *testClient, requests int) {
	c.t.Helper()
	for i := range requests {
		batch := make([]uint64, 100)
		for k := range batch {
			batch[k] = uint64(i*100+k) + 1
		}
		c.ok(client.CreateAccounts, accounts(batch...))
	}
}

const smallSegment = 64 << 10

func spoolsLeft(t *testing.T, dir string) []os.DirEntry {
	t.Helper()
	left, err := os.ReadDir(filepath.Join(dir, recvDir))
	if err != nil {
		t.Fatal(err)
	}
	return left
}

func TestLaggingNodeGetsTheSnapshotAsAStream(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pn := newPipeNet()
		var mu sync.Mutex
		var snaps, chunks, biggest int
		pn.sent = func(frame []byte) {
			f, _, err := wire.Decode(frame, testCluster, wire.MaxPeerBody)
			if err != nil {
				return
			}
			mu.Lock()
			defer mu.Unlock()
			switch f.Type {
			case wire.TypeMessage + wire.Type(raft.MsgSnap):
				snaps++
			case wire.TypeSnapshotChunk:
				chunks++
				biggest = max(biggest, len(f.Body))
			}
		}
		cfgs := make([]Config, 3)
		nodes := make([]*testNode, 3)
		outs := make([]*logLines, 3)
		for i := range cfgs {
			cfgs[i] = pn.config(uint64(i)+1, 3, dataDir(t, uint64(i)+1, 3))
			cfgs[i].SnapshotEvery, cfgs[i].SegmentSize = 20, smallSegment
			outs[i] = logged(&cfgs[i])
			nodes[i] = start(t, cfgs[i])
		}
		lead := leaderOf(t, pn, 3)
		lag := lead%3 + 1
		if err := nodes[lag-1].halt(); err != nil {
			t.Fatal(err)
		}
		c := pn.connect(t, clientAddr(lead), 1)
		c.register()
		fill(c, 45)
		cfg := pn.config(lag, 3, cfgs[lag-1].Dir)
		cfg.SnapshotEvery, cfg.SegmentSize = 20, smallSegment
		took := logged(&cfg)
		nodes[lag-1] = start(t, cfg)

		want := c.status()
		probe := pn.connect(t, clientAddr(lag), 2)
		var got wire.Status
		for range 1500 {
			if got = probe.status(); got.Applied >= want.Applied {
				break
			}
			time.Sleep(testTick)
		}
		if got.Applied < want.Applied {
			t.Fatalf("node %d applied %d after its restart, the leader %d", lag, got.Applied, want.Applied)
		}
		if now := c.status(); now.Applied != got.Applied || now.ApplyHash != got.ApplyHash {
			t.Fatalf("node %d at %d with hash %x, the leader at %d with %x", lag, got.Applied, got.ApplyHash[:4], now.Applied, now.ApplyHash[:4])
		}
		mu.Lock()
		defer mu.Unlock()
		if snaps == 0 || chunks < 2 || biggest > wire.ChunkSize {
			t.Fatalf("%d snapshots in %d chunks of at most %d bytes, want at least one in two or more chunks of at most %d", snaps, chunks, biggest, wire.ChunkSize)
		}
		if left := spoolsLeft(t, cfg.Dir); len(left) != 0 {
			t.Fatalf("%d spool files left on node %d", len(left), lag)
		}
		for part, out := range map[string]*logLines{
			"sending snapshot": outs[lead-1], "sent snapshot": outs[lead-1],
			"receiving snapshot": took, "received snapshot": took,
		} {
			if out.count(part) == 0 {
				t.Errorf("no %q in the log:\n%s", part, strings.Join(out.lines(), "\n"))
			}
		}
	})
}

func slowSnapshot(t *testing.T) (*testNode, *fakePeer) {
	t.Helper()
	pn := newPipeNet()
	behind := pn.fake(t, 2, 1, deaf)
	pn.fake(t, 3, 1, following)
	cfg := pn.config(1, 3, dataDir(t, 1, 3))
	cfg.SnapshotEvery, cfg.SegmentSize = 10, smallSegment
	n := start(t, cfg)
	c := pn.connect(t, clientAddr(1), 1)
	c.leading()
	c.register()
	fill(c, 40)
	behind.mu.Lock()
	behind.pace = 2 * time.Second
	behind.mu.Unlock()
	behind.set(lagging)
	for range 200 {
		time.Sleep(100 * time.Millisecond)
		behind.mu.Lock()
		ends := behind.ends
		behind.mu.Unlock()
		if ends > 0 {
			return n, behind
		}
	}
	t.Fatal("the snapshot did not reach peer 2 within 20s")
	return nil, nil
}

func TestHeartbeatsContinueDuringASnapshot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n, behind := slowSnapshot(t)
		behind.mu.Lock()
		first, last := behind.chunks[0], behind.chunks[len(behind.chunks)-1]
		during := 0
		for _, at := range behind.beats {
			if at.After(first) && at.Before(last) {
				during++
			}
		}
		behind.mu.Unlock()
		if want := int(last.Sub(first) / (2 * heartbeatTicks * testTick)); during < want {
			t.Fatalf("%d heartbeats in the %s between the first and the last chunk, want at least %d", during, last.Sub(first), want)
		}
		if err := n.halt(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestSecondSnapshotIsDropped(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n, behind := slowSnapshot(t)
		time.Sleep(3 * time.Second)
		behind.mu.Lock()
		most, took := behind.most, behind.spell
		behind.mu.Unlock()
		if most != 1 {
			t.Fatalf("%d snapshot connections to one peer at a time, want 1", most)
		}
		if took <= snapshotTicks*testTick {
			t.Fatalf("the transfer took only %s, too short for the leader to send a second snapshot", took)
		}
		if err := n.halt(); err != nil {
			t.Fatal(err)
		}

		pn := newPipeNet()
		pn.fake(t, 2, 1, listening)
		pn.fake(t, 3, 1, listening)
		n = start(t, pn.config(1, 3, dataDir(t, 1, 3)))
		first := pn.greet(t, 1, 2, wire.KindSnapshot)
		first.send(raft.Message{Type: raft.MsgSnap, From: 2, To: 1, Term: 2, Index: 50, LogTerm: 2})
		first.write(wire.AppendChunk(nil, testCluster, []byte("part of a snapshot")))
		second, err := pn.dial(t.Context(), peerAddr(1))
		if err != nil {
			t.Fatal(err)
		}
		toss(second, wire.AppendHello(nil, testCluster, wire.Hello{From: 2, To: 1, MaxSessions: testSessions, Kind: wire.KindSnapshot}))
		_ = second.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		if b, err := io.ReadAll(second); err != nil || len(b) != 0 {
			t.Fatalf("a second snapshot connection from peer 2 read %d bytes and %v, want it closed without a hello", len(b), err)
		}
		if first.shut(100 * time.Millisecond) {
			t.Fatal("the first snapshot connection was closed")
		}
		if err := n.halt(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestSnapshotCutOffIsDiscarded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pn := newPipeNet()
		pn.fake(t, 2, 1, listening)
		pn.fake(t, 3, 1, listening)
		cfg := pn.config(1, 3, dataDir(t, 1, 3))
		n := start(t, cfg)
		probe := pn.connect(t, clientAddr(1), 1)
		junk := []byte(strings.Repeat("not a snapshot ", 100))
		sum := crc32.Checksum(junk, castagnoli)
		size := uint64(len(junk))
		snap := raft.Message{Type: raft.MsgSnap, From: 2, To: 1, Term: 2, Index: 50, LogTerm: 2}
		for name, tail := range map[string][]byte{
			"no end":       nil,
			"short length": wire.AppendSnapshotEnd(nil, testCluster, size-1, sum),
			"long length":  wire.AppendSnapshotEnd(nil, testCluster, size+1, sum),
			"bad checksum": wire.AppendSnapshotEnd(nil, testCluster, size, sum^1),
			"message":      mustMessage(t, raft.Message{Type: raft.MsgAppResp, From: 2, To: 1, Term: 2}),
		} {
			p := pn.greet(t, 1, 2, wire.KindSnapshot)
			p.send(snap)
			p.write(wire.AppendChunk(nil, testCluster, junk))
			if tail != nil {
				toss(p.conn, tail)
			}
			_ = p.conn.Close()
			time.Sleep(testTick)
			if left := spoolsLeft(t, cfg.Dir); len(left) != 0 {
				t.Fatalf("%s: %d spool files left", name, len(left))
			}
			if st := probe.status(); st.Term != 0 {
				t.Fatalf("%s: the node saw the snapshot message, term %d", name, st.Term)
			}
		}
		p := pn.greet(t, 1, 2, wire.KindSnapshot)
		p.send(snap)
		p.write(wire.AppendChunk(nil, testCluster, junk))
		p.write(wire.AppendSnapshotEnd(nil, testCluster, size, sum))
		if err := n.wait(patience); err == nil {
			t.Fatal("a complete stream of junk did not reach the node")
		}
		if left := spoolsLeft(t, cfg.Dir); len(left) != 0 {
			t.Fatalf("%d spool files left after the node stopped", len(left))
		}
	})
}

func TestSnapshotOverTheLimitIsRefused(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pn := newPipeNet()
		pn.fake(t, 2, 1, listening)
		pn.fake(t, 3, 1, listening)
		cfg := pn.config(1, 3, dataDir(t, 1, 3))
		srv := open(t, cfg)
		srv.spoolMax = 2 * wire.ChunkSize
		n := run(t, srv)
		probe := pn.connect(t, clientAddr(1), 1)
		chunk := make([]byte, wire.ChunkSize)
		snap := raft.Message{Type: raft.MsgSnap, From: 2, To: 1, Term: 2, Index: 50, LogTerm: 2}

		over := pn.greet(t, 1, 2, wire.KindSnapshot)
		over.send(snap)
		over.write(wire.AppendChunk(nil, testCluster, chunk))
		over.write(wire.AppendChunk(nil, testCluster, chunk))
		toss(over.conn, wire.AppendChunk(nil, testCluster, chunk[:1]))
		if !over.shut(100 * time.Millisecond) {
			t.Fatal("a stream one byte over the limit stays open")
		}
		if left := spoolsLeft(t, cfg.Dir); len(left) != 0 {
			t.Fatalf("%d spool files left after a stream over the limit", len(left))
		}
		if st := probe.status(); st.Term != 0 {
			t.Fatalf("the node saw the snapshot message, term %d", st.Term)
		}

		full := pn.greet(t, 1, 2, wire.KindSnapshot)
		full.send(snap)
		full.write(wire.AppendChunk(nil, testCluster, chunk))
		full.write(wire.AppendChunk(nil, testCluster, chunk))
		sum := crc32.Update(crc32.Checksum(chunk, castagnoli), castagnoli, chunk)
		full.write(wire.AppendSnapshotEnd(nil, testCluster, 2*wire.ChunkSize, sum))
		if err := n.wait(patience); err == nil {
			t.Fatal("a stream right at the limit did not reach the node")
		}
	})
}

func behindLeader(t *testing.T, pn *pipeNet, fsys *hookFS) (*testNode, *fakePeer) {
	t.Helper()
	behind := pn.fake(t, 2, 1, deaf)
	pn.fake(t, 3, 1, following)
	cfg := pn.config(1, 3, dataDir(t, 1, 3))
	cfg.FS, cfg.SnapshotEvery, cfg.SegmentSize = fsys, 10, smallSegment
	n := start(t, cfg)
	c := pn.connect(t, clientAddr(1), 1)
	c.leading()
	c.register()
	fill(c, 40)
	return n, behind
}

func isSnap(name string) bool {
	return filepath.Base(filepath.Dir(name)) == snapDir
}

func TestNothingOfASnapshotLeavesBeforeItsCheck(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pn := newPipeNet()
		var mu sync.Mutex
		var checked time.Time
		var early []string
		pn.sent = func(frame []byte) {
			f, _, err := wire.Decode(frame, testCluster, wire.MaxPeerBody)
			if err != nil {
				return
			}
			h, err := f.Hello()
			if f.Type != wire.TypeMessage+wire.Type(raft.MsgSnap) && (err != nil || h.Kind != wire.KindSnapshot) {
				return
			}
			mu.Lock()
			if checked.IsZero() {
				early = append(early, fmt.Sprintf("type %#x", uint8(f.Type)))
			}
			mu.Unlock()
		}
		fsys := newHookFS()
		var once sync.Once
		fsys.reading(func(name string, _ int64) {
			if isSnap(name) {
				once.Do(func() {
					time.Sleep(2 * bodyTimeout)
					mu.Lock()
					checked = time.Now()
					mu.Unlock()
				})
			}
		})
		n, behind := behindLeader(t, pn, fsys)
		behind.set(lagging)
		for range 400 {
			time.Sleep(100 * time.Millisecond)
			behind.mu.Lock()
			ends := behind.ends
			behind.mu.Unlock()
			if ends > 0 {
				break
			}
		}
		mu.Lock()
		defer mu.Unlock()
		if checked.IsZero() {
			t.Fatal("no snapshot was read within 40s")
		}
		if len(early) != 0 {
			t.Fatalf("frames of a snapshot connection left while the file was still being checked: %v", early)
		}
		behind.mu.Lock()
		ends := behind.ends
		behind.mu.Unlock()
		if ends == 0 {
			t.Fatal("the snapshot did not reach peer 2 after a check of 20s")
		}
		if err := n.halt(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestSnapshotThatChangesAfterItsCheckIsNotEnded(t *testing.T) {
	for name, edit := range map[string]func(t *testing.T, file string){
		"flipped": func(t *testing.T, file string) {
			f, err := os.OpenFile(file, os.O_RDWR, 0)
			if err != nil {
				t.Error(err)
				return
			}
			b := make([]byte, 1)
			if _, err := f.ReadAt(b, 200); err != nil {
				t.Error(err)
			}
			b[0] ^= 0x10
			if _, err := f.WriteAt(b, 200); err != nil {
				t.Error(err)
			}
			if err := f.Close(); err != nil {
				t.Error(err)
			}
		},
		"cut": func(t *testing.T, file string) {
			st, err := os.Stat(file)
			if err != nil {
				t.Error(err)
				return
			}
			if err := os.Truncate(file, st.Size()/2); err != nil {
				t.Error(err)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				fsys := newHookFS()
				n, behind := behindLeader(t, newPipeNet(), fsys)
				var mu sync.Mutex
				starts := 0
				fsys.reading(func(file string, off int64) {
					if !isSnap(file) || off != 0 {
						return
					}
					mu.Lock()
					starts++
					second := starts == 2
					mu.Unlock()
					if second {
						edit(t, file)
					}
				})
				behind.set(lagging)
				if err := n.wait(3 * patience); !errors.Is(err, wal.ErrCorrupt) {
					t.Fatalf("Run returned %v, want the snapshot that changed after its check to stop the node", err)
				}
				behind.mu.Lock()
				defer behind.mu.Unlock()
				if behind.ends != 0 || len(behind.chunks) == 0 {
					t.Fatalf("%d chunks and %d ends reached peer 2, want chunks and no end", len(behind.chunks), behind.ends)
				}
			})
		})
	}
}

func TestBrokenSnapshotStopsTheSender(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pn := newPipeNet()
		behind := pn.fake(t, 2, 1, deaf)
		pn.fake(t, 3, 1, following)
		cfg := pn.config(1, 3, dataDir(t, 1, 3))
		cfg.SnapshotEvery, cfg.SegmentSize = 10, smallSegment
		n := start(t, cfg)
		c := pn.connect(t, clientAddr(1), 1)
		c.leading()
		c.register()
		fill(c, 25)
		snaps, err := filepath.Glob(filepath.Join(cfg.Dir, snapDir, "*.snap"))
		if err != nil || len(snaps) == 0 {
			t.Fatalf("no snapshot files on the leader: %v", err)
		}
		for _, name := range snaps {
			f, err := os.OpenFile(name, os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			b := make([]byte, 1)
			if _, err := f.ReadAt(b, 200); err != nil {
				t.Fatal(err)
			}
			b[0] ^= 0x10
			if _, err := f.WriteAt(b, 200); err != nil {
				t.Fatal(err)
			}
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}
		}
		behind.set(lagging)
		if err := n.wait(3 * patience); !errors.Is(err, wal.ErrCorrupt) {
			t.Fatalf("Run returned %v, want the checksum error of the snapshot", err)
		}
	})
}
