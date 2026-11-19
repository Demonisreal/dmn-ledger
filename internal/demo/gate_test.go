//go:build linux

package demo

import (
	"errors"
	"io"
	"log"
	"net/netip"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Demonisreal/dmn-ledger/ledger"
)

var kinds = []Kind{Kill, Freeze, Cut, Flip}

func addr(last byte) netip.Prefix {
	return netip.PrefixFrom(netip.AddrFrom4([4]byte{192, 0, 2, last}), 32)
}

func steady() *Stage {
	s := New(Config{Log: log.New(io.Discard, "", 0)})
	s.st.Phase = "steady"
	s.sight = sight{sizes: [3]int64{64 << 10, 64 << 10, 64 << 10}, leader: 1}
	for i := range s.sight.sts {
		s.sight.sts[i] = &ledger.NodeStatus{ID: uint64(i + 1), Role: ledger.Follower, Term: 2, Leader: 1}
	}
	s.sight.sts[0].Role = ledger.Leader
	return s
}

func act(s *Stage, kind Kind, target int, from netip.Prefix) (time.Time, error) {
	s.mu.Lock()
	s.sight.at = time.Now()
	s.mu.Unlock()
	return s.Act(kind, target, from)
}

func done(t *testing.T, s *Stage) {
	t.Helper()
	select {
	case <-s.wishes:
	default:
		t.Fatal("no action in the queue")
	}
	s.mu.Lock()
	s.acting, s.ended = false, time.Now()
	s.mu.Unlock()
}

func TestOneActionAtATime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := steady()
		if _, err := act(s, Kill, 1, addr(1)); err != nil {
			t.Fatalf("first action: %v", err)
		}
		time.Sleep(time.Minute)
		for i, kind := range kinds {
			if _, err := act(s, kind, 2, addr(byte(10+i))); !errors.Is(err, ErrBusy) {
				t.Fatalf("%s while an action runs: %v, want ErrBusy", kind, err)
			}
		}
		if len(s.wishes) != 1 {
			t.Fatalf("%d actions queued, want 1", len(s.wishes))
		}
		done(t, s)
		time.Sleep(s.pace.calm)
		if _, err := act(s, Freeze, 2, addr(2)); err != nil {
			t.Fatalf("after the first ended: %v", err)
		}
	})
}

func TestCooldownAfterAnAction(t *testing.T) {
	for _, c := range []struct {
		name       string
		runs, wait time.Duration
	}{
		{"ten seconds after the beginning", time.Second, 10 * time.Second},
		{"four seconds after the end", 8 * time.Second, 12 * time.Second},
	} {
		t.Run(c.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s := steady()
				began := time.Now()
				if _, err := act(s, Kill, 1, addr(1)); err != nil {
					t.Fatal(err)
				}
				time.Sleep(c.runs)
				done(t, s)
				time.Sleep(c.wait - c.runs - time.Millisecond)
				until, err := act(s, Cut, 2, addr(2))
				if !errors.Is(err, ErrCooling) || !until.Equal(began.Add(c.wait)) {
					t.Fatalf("1ms before %v: %v until %v, want ErrCooling until %v", c.wait, err, until.Sub(began), c.wait)
				}
				time.Sleep(time.Millisecond)
				if _, err := act(s, Cut, 2, addr(3)); err != nil {
					t.Fatalf("at %v: %v", c.wait, err)
				}
			})
		})
	}
}

func TestPerAddressLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := steady()
		began := time.Now()
		if _, err := act(s, Kill, 1, addr(1)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		done(t, s)
		time.Sleep(9 * time.Second)
		until, err := act(s, Kill, 1, addr(1))
		if !errors.Is(err, ErrLimited) || !until.Equal(began.Add(30*time.Second)) {
			t.Fatalf("same address after 10s: %v until %v, want ErrLimited until 30s", err, until.Sub(began))
		}
		if _, err := act(s, Kill, 1, addr(2)); err != nil {
			t.Fatalf("another address after 10s: %v", err)
		}
		time.Sleep(time.Second)
		done(t, s)
		time.Sleep(19*time.Second - time.Millisecond)
		if _, err := act(s, Freeze, 3, addr(1)); !errors.Is(err, ErrLimited) {
			t.Fatalf("same address 1ms before 30s: %v, want ErrLimited", err)
		}
		time.Sleep(time.Millisecond)
		if _, err := act(s, Freeze, 3, addr(1)); err != nil {
			t.Fatalf("same address after 30s: %v", err)
		}
	})
}

func TestAcceptedActionNamesTheLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := steady()
		began := time.Now()
		until, err := act(s, Kill, 1, addr(1))
		if err != nil || !until.Equal(began.Add(30*time.Second)) {
			t.Fatalf("accepted: %v until %v, want the end of the 30s of this address", err, until.Sub(began))
		}
		time.Sleep(time.Second)
		done(t, s)
		time.Sleep(time.Until(until) - time.Millisecond)
		if _, err := act(s, Freeze, 2, addr(1)); !errors.Is(err, ErrLimited) {
			t.Fatalf("1ms before the named time: %v, want ErrLimited", err)
		}
		time.Sleep(time.Millisecond)
		if _, err := act(s, Freeze, 2, addr(1)); err != nil {
			t.Fatalf("at the named time: %v", err)
		}
	})
}

func TestRefusedActionTouchesNothing(t *testing.T) {
	for _, c := range []struct {
		name string
		want error
		set  func(t *testing.T, s *Stage)
		kind Kind
	}{
		{"setup", ErrRecovering, func(t *testing.T, s *Stage) { s.st.Phase = "setup" }, Kill},
		{"finding", ErrRecovering, func(t *testing.T, s *Stage) { s.st.Phase = "finding" }, Kill},
		{"busy", ErrBusy, func(t *testing.T, s *Stage) {
			if _, err := act(s, Kill, 1, addr(9)); err != nil {
				t.Fatal(err)
			}
		}, Cut},
		{"cooling", ErrCooling, func(t *testing.T, s *Stage) {
			if _, err := act(s, Kill, 1, addr(9)); err != nil {
				t.Fatal(err)
			}
			done(t, s)
		}, Cut},
		{"limited", ErrLimited, func(t *testing.T, s *Stage) {
			if _, err := act(s, Kill, 1, addr(1)); err != nil {
				t.Fatal(err)
			}
			done(t, s)
			time.Sleep(15 * time.Second)
		}, Cut},
		{"repair", ErrRecovering, func(t *testing.T, s *Stage) { s.sight.sts[2].Repair = true }, Freeze},
		{"young", ErrYoung, func(t *testing.T, s *Stage) { s.sight.sizes[1] = 100 }, Flip},
	} {
		t.Run(c.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s := steady()
				s.say("demo", "before")
				c.set(t, s)
				s.mu.Lock()
				visits, queued, lines, began, acting := slices.Clone(s.visits), len(s.wishes), s.journal.next, s.began, s.acting
				s.mu.Unlock()
				if _, err := act(s, c.kind, 2, addr(1)); !errors.Is(err, c.want) {
					t.Fatalf("%v, want %v", err, c.want)
				}
				s.mu.Lock()
				defer s.mu.Unlock()
				switch {
				case !slices.Equal(s.visits, visits):
					t.Fatalf("addresses %v, before %v", s.visits, visits)
				case len(s.wishes) != queued:
					t.Fatalf("%d actions queued, before %d", len(s.wishes), queued)
				case s.journal.next != lines:
					t.Fatalf("the journal went from %d to %d lines", lines, s.journal.next)
				case !s.began.Equal(began) || s.acting != acting:
					t.Fatalf("last action began %v, acting %v; before %v, %v", s.began, s.acting, began, acting)
				}
			})
		})
	}
}

func TestNoActionDuringRepair(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for id := range 3 {
			s := steady()
			s.sight.sts[id].Repair = true
			for _, kind := range kinds {
				if _, err := act(s, kind, 0, addr(1)); !errors.Is(err, ErrRecovering) {
					t.Fatalf("%s with n%d in repair: %v, want ErrRecovering", kind, id+1, err)
				}
			}
		}
	})
}

func TestNoActionWithoutThreeNodes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for id := range 3 {
			s := steady()
			s.sight.sts[id] = nil
			if _, err := act(s, Kill, 1+(id+1)%3, addr(1)); !errors.Is(err, ErrRecovering) {
				t.Fatalf("n%d does not answer: %v, want ErrRecovering", id+1, err)
			}
		}
		s := steady()
		s.sight.sts[2].Role = ledger.Leader
		if _, err := act(s, Kill, 1, addr(1)); !errors.Is(err, ErrRecovering) {
			t.Fatalf("two leaders: %v, want ErrRecovering", err)
		}
		s = steady()
		s.sight.sts[0].Role = ledger.Candidate
		if _, err := act(s, Kill, 1, addr(1)); !errors.Is(err, ErrRecovering) {
			t.Fatalf("no leader: %v, want ErrRecovering", err)
		}
		s = steady()
		s.sight.at = time.Now()
		time.Sleep(freshSight + time.Millisecond)
		if _, err := s.Act(Kill, 1, addr(1)); !errors.Is(err, ErrRecovering) {
			t.Fatalf("sight %v old: %v, want ErrRecovering", freshSight+time.Millisecond, err)
		}
		s.sight.at = time.Now()
		time.Sleep(freshSight)
		if _, err := s.Act(Kill, 1, addr(1)); err != nil {
			t.Fatalf("sight %v old: %v", freshSight, err)
		}
	})
}

func TestNoFlipOnAYoungLog(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := steady()
		s.sight.sizes[1] = youngLog - 1
		if _, err := act(s, Flip, 2, addr(1)); !errors.Is(err, ErrYoung) {
			t.Fatalf("flip on n2 with %d bytes: %v, want ErrYoung", youngLog-1, err)
		}
		for i, kind := range []Kind{Kill, Freeze, Cut} {
			s := steady()
			s.sight.sizes[1] = youngLog - 1
			if _, err := act(s, kind, 2, addr(byte(i))); err != nil {
				t.Fatalf("%s on n2 with a young log: %v", kind, err)
			}
		}
		s = steady()
		s.sight.sizes[0] = youngLog - 1
		if _, err := act(s, Flip, 0, addr(1)); !errors.Is(err, ErrYoung) {
			t.Fatalf("flip on the leader n1 with a young log: %v, want ErrYoung", err)
		}
		s = steady()
		s.sight.sizes[1], s.sight.sizes[2] = 0, 0
		if _, err := act(s, Flip, 0, addr(1)); err != nil {
			t.Fatalf("flip on the leader with %d bytes, followers young: %v", s.sight.sizes[0], err)
		}
		s = steady()
		s.sight.sizes[2] = youngLog
		if _, err := act(s, Flip, 3, addr(1)); err != nil {
			t.Fatalf("flip on n3 with %d bytes: %v", youngLog, err)
		}
	})
}

func TestAddressIsForgottenWithoutAnotherAction(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := steady()
		if _, err := act(s, Kill, 1, addr(1)); err != nil {
			t.Fatal(err)
		}
		done(t, s)
		time.Sleep(s.pace.per - time.Second)
		s.look(t.Context(), false)
		if len(s.visits) != 1 {
			t.Fatalf("%d addresses kept %v after the action, want the one still limited", len(s.visits), s.pace.per-time.Second)
		}
		time.Sleep(time.Second)
		s.look(t.Context(), false)
		if len(s.visits) != 0 {
			t.Fatalf("%d addresses kept %v after the action and no action since; want none", len(s.visits), s.pace.per)
		}
	})
}
