package server

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/client"
)

func TestStatsCountRounds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const pause = 7 * time.Millisecond
		fsys := newHookFS()
		pn, cfg := single(t, fsys)
		n := start(t, cfg)
		c := pn.connect(t, clientAddr(1), 1)
		c.leading()
		c.register()
		c.ok(client.CreateAccounts, accounts(1, 2))
		before := n.srv.Stats()
		time.Sleep(3 * testTick)
		fsys.hook(func(string) error {
			time.Sleep(pause)
			return nil
		})
		c.ok(client.CreateTransfers, transfer(10, 1, 2, 5))
		after := n.srv.Stats()
		again := n.srv.Stats()
		switch {
		case after.Rounds < before.Rounds+3:
			t.Fatalf("rounds went from %d to %d over three idle ticks and a transfer", before.Rounds, after.Rounds)
		case after.Process-before.Process != 2*pause:
			t.Fatalf("Process grew by %s, want the two syncs of %s and nothing of the wait", after.Process-before.Process, pause)
		case after.Wait-before.Wait < 3*testTick:
			t.Fatalf("Wait grew by %s over three idle ticks", after.Wait-before.Wait)
		case after.Slowest != 2*pause:
			t.Fatalf("slowest Process %s, want %s", after.Slowest, 2*pause)
		case again.Slowest != 0:
			t.Fatalf("read again at once: slowest %s, want 0", again.Slowest)
		}
		fsys.hook(nil)
		time.Sleep(2 * testTick)
		if idle := n.srv.Stats(); idle.Rounds < after.Rounds+2 || idle.Slowest != 0 {
			t.Fatalf("after two idle ticks: %d rounds, slowest %s, want at least %d and 0", idle.Rounds, idle.Slowest, after.Rounds+2)
		}
		if err := n.halt(); err != nil {
			t.Fatal(err)
		}
	})
}
