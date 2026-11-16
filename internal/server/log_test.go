package server

import (
	"log"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/disk"
	"github.com/Demonisreal/dmn-ledger/internal/raft"
	"github.com/Demonisreal/dmn-ledger/internal/wal"
	"github.com/Demonisreal/dmn-ledger/internal/wire"
)

type logLines struct {
	mu   sync.Mutex
	text strings.Builder
}

func (l *logLines) Write(b []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.text.Write(b)
}

func (l *logLines) lines() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Split(strings.TrimSpace(l.text.String()), "\n")
}

func (l *logLines) count(part string) int {
	k := 0
	for _, line := range l.lines() {
		if strings.Contains(line, part) {
			k++
		}
	}
	return k
}

func logged(cfg *Config) *logLines {
	out := &logLines{}
	cfg.Log = log.New(out, "", 0)
	return out
}

func TestLogShowsRoleChangesAndNoRequests(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pn, cfg := single(t, nil)
		out := logged(&cfg)
		n := start(t, cfg)
		c := pn.connect(t, clientAddr(1), 1)
		c.leading()
		c.register()
		c.ok(client.CreateAccounts, accounts(1, 2))
		for i := range 20 {
			c.ok(client.CreateTransfers, transfer(100+uint64(i), 1, 2, 1))
			c.status()
		}
		time.Sleep(time.Second)
		if err := n.halt(); err != nil {
			t.Fatal(err)
		}
		want := []string{"leader in term 1, leader 1"}
		if got := out.lines(); strings.Join(got, "|") != strings.Join(want, "|") {
			t.Fatalf("log of a single node that took the lead and served 42 requests:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
		}
	})
}

func TestLogShowsTheEndOfRepair(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pn := newPipeNet()
		dir := t.TempDir()
		meta, _, err := wal.OpenMeta(disk.OS{}, dir)
		if err != nil {
			t.Fatal(err)
		}
		if err := meta.Save(1, 0, true); err != nil {
			t.Fatal(err)
		}
		if err := meta.Close(); err != nil {
			t.Fatal(err)
		}
		pn.fake(t, 3, 1, listening)
		lead := pn.fake(t, 2, 1, listening)
		cfg := pn.config(1, 3, dir)
		out := logged(&cfg)
		n := start(t, cfg)
		p := pn.greet(t, 1, 2, wire.KindMessages)
		beat := raft.Message{Type: raft.MsgApp, From: 2, To: 1, Term: 2}
		p.send(beat)
		var probe raft.Message
		for range 100 {
			if probes := lead.messages(raft.MsgRepairProbe); len(probes) > 0 {
				probe = probes[0]
				break
			}
			time.Sleep(testTick)
		}
		if probe.Type == 0 {
			t.Fatal("no repair probe within 1s")
		}
		time.Sleep(testTick)
		if got := out.count("repair"); got != 0 {
			t.Fatalf("%d repair lines before the repair ended", got)
		}
		p.send(raft.Message{Type: raft.MsgRepairInfo, From: 2, To: 1, Term: 2, Seq: probe.Seq})
		p.send(beat)
		time.Sleep(testTick)
		if st := pn.connect(t, clientAddr(1), 1).status(); st.Repair {
			t.Fatal("the node is still in repair")
		}
		if err := n.halt(); err != nil {
			t.Fatal(err)
		}
		if got := out.lines(); len(got) != 1 || got[0] != "repair flag cleared in term 2" {
			t.Fatalf("log: %q, want only the end of the repair", got)
		}
	})
}

func TestLogThrottlesRefusedConnections(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pn := newPipeNet()
		cfg := pn.config(1, 3, t.TempDir())
		cfg.MaxClients = 1
		out := logged(&cfg)
		n := start(t, cfg)
		stranger := wire.AppendHello(nil, testCluster, wire.Hello{From: 9, To: 1, Kind: wire.KindMessages})
		knock := func() {
			conn, err := pn.dial(t.Context(), peerAddr(1))
			if err != nil {
				t.Fatal(err)
			}
			toss(conn, stranger)
			_ = conn.Close()
		}
		for range 10 {
			knock()
			time.Sleep(50 * time.Millisecond)
		}
		pn.connect(t, clientAddr(1), 1).status()
		for range 10 {
			if extra := pn.connect(t, clientAddr(1), 2); !extra.shut(10 * time.Millisecond) {
				t.Fatal("a second client stays connected with MaxClients 1")
			}
		}
		synctest.Wait()
		if got := out.count("refused peer hello"); got != 1 {
			t.Fatalf("%d lines for ten strangers within a second, want 1:\n%s", got, strings.Join(out.lines(), "\n"))
		}
		if got := out.count("refused client"); got != 1 {
			t.Fatalf("%d lines for ten refused clients within a second, want 1:\n%s", got, strings.Join(out.lines(), "\n"))
		}
		time.Sleep(refusalGap)
		knock()
		synctest.Wait()
		if err := n.halt(); err != nil {
			t.Fatal(err)
		}
		if got := out.count("refused peer hello"); got != 2 {
			t.Fatalf("%d lines after another stranger a second later, want 2", got)
		}
	})
}
