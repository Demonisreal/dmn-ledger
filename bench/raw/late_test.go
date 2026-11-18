package raw

import (
	"slices"
	"testing"
	"time"

	"github.com/Demonisreal/dmn-ledger/bench/load"
)

func TestGivenUpTransfersAreTheLimitOfTheTarget(t *testing.T) {
	b := newBuilt()
	for range 3 {
		b.add(load.Window, 100, 10, ms(1.4), ms(10))
	}
	for i := range b.run.Seconds {
		sec := &b.run.Seconds[i]
		sec.Due, sec.Shed = 20_000, 4_000
		for range 300 {
			sec.Late.Add(3 * time.Millisecond)
		}
	}
	b.run.Saturated, b.run.OpenAtEnd = true, 6_000
	h := head(2)
	h.Load.Rate = 20_000
	f, err := Assemble(h, b.run, b.ctl)
	if err != nil {
		t.Fatal(err)
	}
	if v := f.Verdict; !v.Valid || v.Limit != LimitTarget {
		t.Errorf("a target gave up a fifth of the transfers due, %d open at %d/s, dispatcher late by %v: valid %v %q, limit %q, want the target",
			v.OpenAtEnd, h.Load.Rate, v.LateP99, v.Valid, v.Reasons, v.Limit)
	}
}

func lateRun(share int) *built {
	b := newBuilt()
	for range 6 {
		b.add(load.Window, 100, 10, ms(1.4), ms(10))
	}
	for _, sec := range b.run.Seconds {
		for i := range 999 {
			late := 50 * time.Microsecond
			if i%100 < share {
				late = 8 * time.Millisecond
			}
			sec.Late.Add(late)
		}
	}
	return b
}

func TestTheCollectorOfTheGeneratorIsNotItsLimit(t *testing.T) {
	h := head(3)
	h.Load.Rate = 256_000
	f, err := Assemble(h, lateRun(5).run, lateRun(5).ctl)
	if err != nil {
		t.Fatal(err)
	}
	if v := f.Verdict; !v.Valid || slices.Contains(v.Reasons, WhyGenerator) {
		t.Errorf("a held stage whose dispatcher was 8 ms late for five arrivals in a hundred (p99 %v) and on time for the rest: valid %v %q; "+
			"the garbage collector of the generator delays a few arrivals, the stage was still offered and answered", v.LateP99, v.Valid, v.Reasons)
	}
	behind := lateRun(60)
	if f, err = Assemble(h, behind.run, behind.ctl); err != nil {
		t.Fatal(err)
	}
	if v := f.Verdict; v.Valid || !slices.Contains(v.Reasons, WhyGenerator) {
		t.Errorf("a dispatcher 8 ms late for most arrivals no longer offers the plan: valid %v %q, want %q", v.Valid, v.Reasons, WhyGenerator)
	}
	full := lateRun(5)
	full.run.Saturated, full.run.Capped, full.run.OpenAtEnd = true, true, 100_000
	if f, err = Assemble(h, full.run, full.ctl); err != nil {
		t.Fatal(err)
	}
	if v := f.Verdict; !v.Valid || v.Limit != LimitTarget {
		t.Errorf("256,000/s, 100,000 calls open at the end, dispatcher on time for 95 in a hundred: limit %q, want the target; the open limit is "+
			"below one second of arrivals from 128,000/s on, so a late p99 alone calls every such stage a limit of the generator", v.Limit)
	}
}
