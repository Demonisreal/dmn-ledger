package report

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Demonisreal/dmn-ledger/bench/control"
	"github.com/Demonisreal/dmn-ledger/bench/load"
	"github.com/Demonisreal/dmn-ledger/bench/raw"
)

type oneServer struct {
	load.Null
	per  time.Duration
	busy chan struct{}
}

func (s oneServer) Do(ctx context.Context, _ int, _ load.Op) (load.Outcome, error) {
	select {
	case s.busy <- struct{}{}:
	case <-ctx.Done():
		return load.Outcome{}, ctx.Err()
	}
	defer func() { <-s.busy }()
	timer := time.NewTimer(s.per)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return load.Outcome{}, ctx.Err()
	case <-timer.C:
		return load.Outcome{}, nil
	}
}

func stageHead(id, m string, pass, rate, need int) raw.Head {
	return raw.Head{
		ID: id, Measurement: m, Pass: pass,
		Build:      raw.Build{Commit: "testdata", Tags: []string{}, Go: "go1.27.4"},
		Machine:    control.Machine{CPU: "test CPU", Threads: 16, GoMaxProcs: 8, Kernel: "test kernel", Filesystem: "ext4", Disk: "nvme0n1"},
		Target:     raw.Target{Kind: "ledger", Nodes: 1, Lanes: 4, Cores: "0,1"},
		Load:       raw.Load{Workload: "uniform-10k", Accounts: 10_000, Rate: rate, Warmup: 5 * time.Second, Window: 15 * time.Second, Drain: 5 * time.Second, Need: need, Seed: 7},
		Probe:      control.Probe{Means: []control.ProbeMean{{Speed: control.Slow}}},
		Thresholds: control.Defaults(),
	}
}

func slowSecond() control.Second {
	return control.Second{Span: time.Second, Flushes: 300, FlushTime: 300 * slow, Generator: 0.5, Target: 0.2, Foreign: 0.2}
}

func cappedStage(t *testing.T, rate, pass int) *raw.File {
	var f *raw.File
	synctest.Test(t, func(t *testing.T) {
		srv := oneServer{per: 500 * time.Microsecond, busy: make(chan struct{}, 1)}
		var ctl []control.Second
		p := load.Plan{Rate: rate, Lanes: 4, Warmup: 5 * time.Second, Window: 15 * time.Second, Drain: 5 * time.Second, OpenLimit: 3000,
			Tick: func(int) bool {
				ctl = append(ctl, slowSecond())
				return true
			}}
		w, _ := load.Named("uniform-10k")
		run, err := load.Drive(context.Background(), srv, load.NewStream(w, 7), p)
		if err != nil {
			t.Fatal(err)
		}
		f, err = raw.Assemble(stageHead(fmt.Sprintf("m1-l1-uniform-10k-%d-%d", rate, pass), "m1", pass, rate, 8), run, ctl)
		if err != nil {
			t.Fatal(err)
		}
		f.Verified(load.Tally{}, nil)
	})
	return f
}

func TestCappedStageHasThroughputUnderOverload(t *testing.T) {
	var runs []*run
	for pass := 1; pass <= 2; pass++ {
		for _, rate := range []int{1000, 4000} {
			f := cappedStage(t, rate, pass)
			v := f.Verdict
			if rate == 4000 && (!v.Capped || !v.Saturated || !v.Valid || f.Bands.Slow.Seconds != 15) {
				t.Fatalf("pass %d, 4000/s against a target that holds 2000/s, open limit 3000: capped %v, saturated %v, valid %v %q, %d slow seconds, want a capped stage with its whole window",
					pass, v.Capped, v.Saturated, v.Valid, v.Reasons, f.Bands.Slow.Seconds)
			}
			runs = append(runs, &run{name: f.ID + ".json", f: f})
		}
	}
	o := chains(groups(runs))[0].overload()
	text, ok := o.judge()
	tp := o.throughput()
	if tp.n != 2 || tp.med < 1800 || tp.med > 2200 {
		t.Errorf("throughput under overload %q from %d runs, want about 2000/s from two: a stage held back at the open limit still measures", tp.show(grouped), tp.n)
	}
	if !ok || o.held.med != 1000 {
		t.Errorf("last held %v, check says %q for two passes that both saturated at the same stage", o.held.med, text)
	}
}

type stalling struct {
	load.Null
	from, until time.Time
	blind       bool
}

func (s stalling) Do(ctx context.Context, _ int, _ load.Op) (load.Outcome, error) {
	wait := time.Millisecond
	if now := time.Now(); !now.Before(s.from) && now.Before(s.until) {
		wait += s.until.Sub(now)
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return load.Outcome{}, ctx.Err()
	case <-timer.C:
		return load.Outcome{}, nil
	}
}

func (s stalling) Sample(context.Context) (load.Inside, error) {
	if now := time.Now(); s.blind && !now.Before(s.from) && now.Before(s.until) {
		return nil, errors.New("status timed out")
	}
	return load.Inside{{Name: "n1", Role: "leader", Term: 1}}, nil
}

func TestStalledSecondsMakeTheRunInvalid(t *testing.T) {
	for _, c := range []struct {
		name        string
		from, until time.Duration
		blind       bool
		flushes     map[int]uint64
	}{
		{"status times out at the tick", 2300 * time.Millisecond, 3300 * time.Millisecond, true, nil},
		{"the stalled second has too few flushes", 2 * time.Second, 3 * time.Second, false, map[int]uint64{2: 5}},
	} {
		t.Run(c.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				start := time.Now()
				target := stalling{from: start.Add(c.from), until: start.Add(c.until), blind: c.blind}
				var ctl []control.Second
				p := load.Plan{Rate: 1000, Lanes: 32, Warmup: time.Second, Window: 6 * time.Second, Drain: 2 * time.Second,
					Tick: func(sec int) bool {
						s := slowSecond()
						if n, ok := c.flushes[sec]; ok {
							s.Flushes, s.FlushTime = n, time.Duration(n)*slow
						}
						ctl = append(ctl, s)
						return true
					}}
				w, _ := load.Named("uniform-10k")
				run, err := load.Drive(context.Background(), target, load.NewStream(w, 7), p)
				if err != nil {
					t.Fatal(err)
				}
				h := stageHead("m2-l1-uniform-10k-1000-1", "m2", 1, 1000, 3)
				f, err := raw.Assemble(h, run, ctl)
				if err != nil {
					t.Fatal(err)
				}
				if f.Verdict.Valid || !slices.Contains(f.Verdict.Reasons, raw.WhyStalled) {
					t.Fatalf("the target stood for one second and 1000 arrivals waited up to 1s, yet the run is valid %v %q with band max %v", f.Verdict.Valid, f.Verdict.Reasons, f.Bands.Slow.Max)
				}
				h.AllowStalls = true
				if f, err = raw.Assemble(h, run, ctl); err != nil {
					t.Fatal(err)
				}
				if !f.Verdict.Valid || f.Verdict.Excluded != 2 || f.Verdict.ExcludedP99 < 900*time.Millisecond {
					t.Fatalf("stalls allowed: valid %v %q, %d seconds excluded with p99 up to %v, want the stall in the verdict", f.Verdict.Valid, f.Verdict.Reasons, f.Verdict.Excluded, f.Verdict.ExcludedP99)
				}
			})
		})
	}
}
