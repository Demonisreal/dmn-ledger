package raw

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Demonisreal/dmn-ledger/bench/control"
	"github.com/Demonisreal/dmn-ledger/bench/load"
)

var update = flag.Bool("update", false, "rewrite testdata/example.json")

func fill(v reflect.Value, path string) {
	switch v.Interface().(type) {
	case time.Time:
		v.Set(reflect.ValueOf(time.Date(2026, 11, 16, 21, 4, 5, 6, time.UTC)))
		return
	case control.Speed:
		v.Set(reflect.ValueOf(control.Slow))
		return
	}
	switch v.Kind() {
	case reflect.Struct:
		for i := range v.NumField() {
			fill(v.Field(i), path+"."+v.Type().Field(i).Name)
		}
	case reflect.Pointer:
		v.Set(reflect.New(v.Type().Elem()))
		fill(v.Elem(), path)
	case reflect.Slice:
		v.Set(reflect.MakeSlice(v.Type(), 2, 2))
		for i := range 2 {
			fill(v.Index(i), fmt.Sprintf("%s[%d]", path, i))
		}
	case reflect.Array:
		for i := range v.Len() {
			fill(v.Index(i), path)
		}
	case reflect.Map:
		v.Set(reflect.MakeMap(v.Type()))
		key, elem := reflect.New(v.Type().Key()).Elem(), reflect.New(v.Type().Elem()).Elem()
		fill(key, path+".key")
		fill(elem, path)
		v.SetMapIndex(key, elem)
	case reflect.String:
		v.SetString(path)
	case reflect.Bool:
		v.SetBool(true)
	case reflect.Int, reflect.Int64:
		v.SetInt(int64(len(path)) * 1_000_003)
	case reflect.Uint8, reflect.Uint64:
		v.SetUint(uint64(len(path)))
	case reflect.Float64:
		v.SetFloat(float64(len(path)) / 7)
	default:
		panic("fill: no value for " + path + " of kind " + v.Kind().String())
	}
}

func zeros(v reflect.Value, path string, found *[]string) {
	switch v.Kind() {
	case reflect.Struct:
		if _, ok := v.Interface().(time.Time); ok {
			if v.Interface().(time.Time).IsZero() {
				*found = append(*found, path)
			}
			return
		}
		for i := range v.NumField() {
			zeros(v.Field(i), path+"."+v.Type().Field(i).Name, found)
		}
	case reflect.Pointer:
		if v.IsNil() {
			*found = append(*found, path)
			return
		}
		zeros(v.Elem(), path, found)
	case reflect.Slice, reflect.Map:
		if v.Len() == 0 {
			*found = append(*found, path)
		}
		if v.Kind() == reflect.Slice {
			for i := range v.Len() {
				zeros(v.Index(i), path, found)
			}
		}
	default:
		if v.IsZero() {
			*found = append(*found, path)
		}
	}
}

func TestRawRoundTrip(t *testing.T) {
	var f File
	fill(reflect.ValueOf(&f).Elem(), "file")
	f.Schema = Schema
	var empty []string
	zeros(reflect.ValueOf(f), "file", &empty)
	if len(empty) > 0 {
		t.Fatalf("filler left fields empty: %q", empty)
	}
	var buf bytes.Buffer
	if err := Write(&buf, &f); err != nil {
		t.Fatal(err)
	}
	back, err := Read(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(*back, f) {
		var lost []string
		zeros(reflect.ValueOf(*back), "file", &lost)
		t.Fatalf("round trip changed the file; empty after reading: %q", lost)
	}

	for _, old := range []string{`"schema": 1`, `"schema": 2`, `"schema": 4`} {
		if _, err := Read(strings.NewReader(strings.Replace(buf.String(), `"schema": 3`, old, 1))); !errors.Is(err, ErrSchema) {
			t.Errorf("%s: %v", old, err)
		}
	}
	if _, err := Read(strings.NewReader(strings.Replace(buf.String(), `"pass":`, `"passes": 1, "pass":`, 1))); err == nil {
		t.Error("unknown field read without complaint")
	}
	if _, err := Read(strings.NewReader("{")); err == nil {
		t.Error("broken file read without complaint")
	}
}

func ms(f float64) time.Duration {
	return time.Duration(f * float64(time.Millisecond))
}

type built struct {
	run *load.Run
	ctl []control.Second
}

func (b *built) add(phase load.Phase, booked, syncs uint64, flushMean time.Duration, lat ...time.Duration) *control.Second {
	sec := load.Second{Phase: phase, Due: uint64(len(lat)), Booked: booked, Latency: new(load.Histogram), Late: new(load.Histogram), Sampled: true,
		Inside: load.Inside{{Name: "n1", Role: "leader", Term: 2, Syncs: syncs, Requests: syncs * 2}}}
	for _, d := range lat {
		sec.Latency.Add(d)
		sec.Late.Add(20 * time.Microsecond)
	}
	b.run.Seconds = append(b.run.Seconds, sec)
	b.ctl = append(b.ctl, control.Second{Span: time.Second, Flushes: 300, FlushTime: 300 * flushMean, Generator: 0.5})
	return &b.ctl[len(b.ctl)-1]
}

func newBuilt() *built {
	return &built{run: &load.Run{Before: load.Inside{{Name: "n1", Term: 2}}}}
}

func head(need int) Head {
	return Head{Load: Load{Need: need}, Thresholds: control.Defaults(), Machine: control.Machine{GoMaxProcs: 4}}
}

func TestInvalidSecondsStayOut(t *testing.T) {
	b := newBuilt()
	b.add(load.Warmup, 100, 10, ms(1.4), ms(900), ms(900))
	b.add(load.Window, 100, 20, ms(1.4), ms(10), ms(11), ms(12))
	b.add(load.Window, 100, 30, ms(1.4), ms(10), ms(11), ms(12)).SwappedOut = 1
	b.run.Seconds[2].Latency.Add(5 * time.Second)
	b.add(load.Window, 100, 45, ms(0.7), ms(800))
	b.add(load.Window, 100, 50, ms(1.4), ms(700))
	b.add(load.Window, 100, 60, ms(1.4), ms(10), ms(11), ms(12))
	b.add(load.Window, 100, 70, ms(1.5), ms(10), ms(11), ms(12))
	b.run.Seconds[6].Sampled = false
	b.add(load.Window, 300, 80, ms(1.5), ms(10), ms(11), ms(12))
	b.add(load.Window, 100, 90, ms(1.5), ms(10), ms(11), ms(12))
	b.add(load.Drain, 0, 95, ms(1.5))
	f, err := Assemble(head(2), b.run, b.ctl)
	if err != nil {
		t.Fatal(err)
	}
	if f.Verdict.Valid || !slices.Contains(f.Verdict.Reasons, WhyStalled) {
		t.Fatalf("a window second without a sample left the run valid: %+v", f.Verdict)
	}
	h := head(2)
	h.AllowStalls = true
	if f, err = Assemble(h, b.run, b.ctl); err != nil {
		t.Fatal(err)
	}
	valid := []bool{true, true, false, false, false, true, false, false, true, true}
	for i, s := range f.Seconds {
		if s.Valid != valid[i] {
			t.Errorf("second %d valid %v %q, want %v", i, s.Valid, s.Why, valid[i])
		}
	}
	slow := f.Bands.Slow
	if slow.Seconds != 3 || slow.Max > ms(12.5) || slow.Unfinished != 0 {
		t.Fatalf("slow band: %d seconds, max %v; only seconds 1, 5 and 8 count", slow.Seconds, slow.Max)
	}
	if slow.Throughput != 100 || slow.Syncs != 0.1 || slow.PartSyncs["n1"] != 0.1 || slow.PerRequest != 5 {
		t.Errorf("slow band: %.1f/s, %.3f fsync and %.1f per request, want 100, 0.1 and 5", slow.Throughput, slow.Syncs, slow.PerRequest)
	}
	if slow.Flush != (2*ms(1.4)+ms(1.5))/3 || slow.P50Flushes < 7.5 || slow.P50Flushes > 8 {
		t.Errorf("slow band: flush %v, p50 %v is %.2f flushes", slow.Flush, slow.P50, slow.P50Flushes)
	}
	if f.Bands.Fast.Seconds != 0 || f.Bands.Fast.Throughput != 0 {
		t.Errorf("fast band has %d seconds", f.Bands.Fast.Seconds)
	}
	if !f.Verdict.Valid || f.Verdict.Speed != control.Slow || f.Verdict.Throughput != 125 {
		t.Errorf("verdict %+v", f.Verdict)
	}
	if f.Seconds[2].P99 < 4*time.Second || f.Seconds[2].Count != 4 {
		t.Errorf("an invalid second keeps its own numbers: p99 %v, count %d", f.Seconds[2].P99, f.Seconds[2].Count)
	}
	if f.Verdict.Excluded != 5 || f.Verdict.ExcludedP99 != f.Seconds[2].P99 {
		t.Errorf("verdict: %d seconds excluded with p99 up to %v, want 5 of 8 and the p99 %v of second 2", f.Verdict.Excluded, f.Verdict.ExcludedP99, f.Seconds[2].P99)
	}
}

func TestRequestsFollowTheLeader(t *testing.T) {
	b := newBuilt()
	b.run.Before = load.Inside{
		{Name: "n1", Role: "leader", Term: 2, Requests: 1000, Counters: map[string]uint64{"applied": 1000}},
		{Name: "n2", Role: "follower", Term: 2, Counters: map[string]uint64{"applied": 1000}},
	}
	cluster := func(n1, n2 load.Part) {
		b.add(load.Window, 100, 0, ms(1.4), ms(10))
		b.run.Seconds[len(b.run.Seconds)-1].Inside = load.Inside{n1, n2}
	}
	part := func(name, role string, term, requests, applied uint64) load.Part {
		return load.Part{Name: name, Role: role, Term: term, Requests: requests, Counters: map[string]uint64{"applied": applied}}
	}
	cluster(part("n1", "leader", 2, 1100, 1100), part("n2", "follower", 2, 0, 1100))
	cluster(part("n1", "candidate", 3, 0, 1150), part("n2", "follower", 3, 0, 1150))
	cluster(part("n1", "follower", 4, 0, 1250), part("n2", "leader", 4, 1250, 1250))
	b.ctl[len(b.ctl)-1].SwappedOut = 1
	cluster(part("n1", "follower", 4, 0, 1340), part("n2", "leader", 4, 1350, 1350))
	f, err := Assemble(head(3), b.run, b.ctl)
	if err != nil {
		t.Fatal(err)
	}
	if slow := f.Bands.Slow; slow.Seconds != 3 || slow.PerRequest != 300.0/250 {
		t.Fatalf("slow band: %d seconds, %.4f transfers per request; the leader applied 100, 50 and 100 entries in the counted seconds, so 300 transfers took 250 requests",
			slow.Seconds, slow.PerRequest)
	}
}

func TestStallsMakeTheRunInvalid(t *testing.T) {
	for _, c := range []struct {
		name  string
		spoil func(b *built)
		why   string
	}{
		{"no sample", func(b *built) { b.run.Seconds[3].Sampled = false }, WhyNoSample},
		{"too few flushes", func(b *built) { b.ctl[3].Flushes, b.ctl[3].FlushTime = 5, 5*ms(1.4) }, control.WhyFewFlushes},
		{"stale status", func(b *built) { b.run.Seconds[3].Inside[0].Stale = true }, WhyStale},
		{"no sample in the last second of the warmup", func(b *built) { b.run.Seconds[0].Sampled = false }, WhyNoSample},
		{"an idle second with few flushes in a paced run", func(b *built) {
			b.ctl[3].Flushes, b.ctl[3].FlushTime, b.ctl[3].Target = 8, 16*time.Millisecond, 0.4
		}, control.WhyFewFlushes},
		{"an unpaced target that stands", func(b *built) {
			for i := range b.ctl {
				b.ctl[i].Flushes, b.ctl[i].FlushTime, b.ctl[i].Target = 8, 16*time.Millisecond, 1.0
			}
			b.ctl[3].Target = 0.02
		}, control.WhyIdle},
	} {
		t.Run(c.name, func(t *testing.T) {
			b := newBuilt()
			b.add(load.Warmup, 100, 10, ms(1.4), ms(10))
			for i := range 6 {
				b.add(load.Window, 100, uint64(20+10*i), ms(1.4), ms(10), ms(11))
			}
			b.add(load.Drain, 0, 90, ms(1.4))
			c.spoil(b)
			f, err := Assemble(head(2), b.run, b.ctl)
			if err != nil {
				t.Fatal(err)
			}
			var why []string
			for _, s := range f.Seconds {
				why = append(why, s.Why...)
			}
			if !slices.Contains(why, c.why) {
				t.Fatalf("seconds give %q, want %q", why, c.why)
			}
			if f.Verdict.Valid || !slices.Contains(f.Verdict.Reasons, WhyStalled) {
				t.Fatalf("a stalled target left the run valid: %+v", f.Verdict)
			}
			h := head(2)
			h.AllowStalls = true
			if f, _ = Assemble(h, b.run, b.ctl); !f.Verdict.Valid || f.Verdict.Excluded == 0 || !f.AllowStalls {
				t.Fatalf("stalls allowed: valid %v %q, %d seconds excluded", f.Verdict.Valid, f.Verdict.Reasons, f.Verdict.Excluded)
			}
		})
	}
	busy := newBuilt()
	busy.add(load.Warmup, 100, 10, ms(1.4), ms(10))
	for i := range 6 {
		busy.add(load.Window, 100, uint64(20+10*i), ms(1.4), ms(10), ms(11))
	}
	busy.add(load.Drain, 0, 90, ms(1.4))
	busy.ctl[3].Flushes, busy.ctl[3].FlushTime, busy.ctl[3].Target = 8, 16*time.Millisecond, 0.6
	busy.run.Seconds[3].Latency.Add(700 * time.Millisecond)
	f, err := Assemble(head(2), busy.run, busy.ctl)
	if err != nil {
		t.Fatal(err)
	}
	v := f.Verdict
	if !v.Valid || f.Seconds[3].Valid || !slices.Contains(f.Seconds[3].Why, control.WhyFewFlushes) || v.Excluded != 2 || v.ExcludedP99 < 700*time.Millisecond {
		t.Fatalf("a target on 0.6 cores with 8 flushes in one second of a paced run: valid %v %q, second 3 valid %v %q, %d excluded with p99 up to %v; "+
			"want a valid run that leaves that second and the next one out with their p99", v.Valid, v.Reasons, f.Seconds[3].Valid, f.Seconds[3].Why, v.Excluded, v.ExcludedP99)
	}

	b := newBuilt()
	for i := range 4 {
		b.add(load.Window, 100, uint64(10+10*i), ms(1.4), ms(10)).Throttled = uint64(700 * (i % 2))
	}
	b.add(load.Drain, 0, 50, ms(1.4)).Throttled = 3
	b.run.Seconds[4].Inside[0].Stale = true
	f, _ = Assemble(head(1), b.run, b.ctl)
	if !f.Verdict.Valid || f.Seconds[4].Valid || !slices.Contains(f.Seconds[4].Why, WhyStale) {
		t.Fatalf("throttled seconds and a stale status in the drain: valid %v %q, drain %v %q", f.Verdict.Valid, f.Verdict.Reasons, f.Seconds[4].Valid, f.Seconds[4].Why)
	}
	if f.Bands.Slow.Seconds != 4 || f.Verdict.Throttled != 2 || f.Seconds[1].Throttled != 700 {
		t.Errorf("throttled window seconds: %d of 4 in the band, %d marked thermal, counter %d, want all 4 counted and 2 marked",
			f.Bands.Slow.Seconds, f.Verdict.Throttled, f.Seconds[1].Throttled)
	}
}

func TestBusyTargetWithoutFlushes(t *testing.T) {
	b := newBuilt()
	b.add(load.Warmup, 100, 10, ms(1.4), ms(10))
	for i := range 6 {
		sec := b.add(load.Window, 2000, uint64(11+i), ms(1.4), ms(10), ms(11))
		sec.Flushes, sec.FlushTime, sec.Target = 6+uint64(i), time.Duration(6+i)*ms(3), 1.0
	}
	b.ctl[3].Flushes, b.ctl[3].FlushTime = 300, 300*ms(1.4)
	b.add(load.Drain, 0, 20, ms(1.4))
	f, err := Assemble(head(4), b.run, b.ctl)
	if err != nil {
		t.Fatal(err)
	}
	v, u := f.Verdict, f.Bands.Unpaced
	if !v.Valid || v.Speed != control.Unpaced || u.Seconds != 6 || u.Throughput != 2000 || f.Bands.Slow.Seconds != 0 {
		t.Fatalf("a target on a full core with few flushes: valid %v %q, speed %v, %d unpaced seconds at %.0f/s, %d slow; want a valid unpaced run of 6 seconds",
			v.Valid, v.Reasons, v.Speed, u.Seconds, u.Throughput, f.Bands.Slow.Seconds)
	}
	if u.Flush != 0 || u.P50Flushes != 0 || u.P50 == 0 || u.Syncs == 0 {
		t.Errorf("unpaced band: flush %v, p50 %v in %.2f flushes, %.3f fsync per transfer; want no flush time and no flush units", u.Flush, u.P50, u.P50Flushes, u.Syncs)
	}
	for i, s := range f.Seconds {
		if s.Speed != control.Unpaced || slices.Contains(s.Why, control.WhyFewFlushes) || slices.Contains(s.Why, control.WhyChange) {
			t.Errorf("second %d: %v %q, want unpaced without the reasons of a paced run", i, s.Speed, s.Why)
		}
	}
}

func TestGeneratorLimitNeedsAnEmptyBacklog(t *testing.T) {
	for _, c := range []struct {
		name  string
		rate  int
		open  uint64
		limit string
	}{
		{"a late dispatcher in front of a full backlog", 64_000, 100_000, LimitTarget},
		{"a late dispatcher at the open limit, little backlog", 512_000, 90_000, LimitGenerator},
	} {
		b := newBuilt()
		for range 3 {
			b.add(load.Window, 100, 10, ms(1.4), ms(10))
		}
		for _, sec := range b.run.Seconds {
			for range 300 {
				sec.Late.Add(14 * time.Millisecond)
			}
		}
		b.run.Saturated, b.run.Capped, b.run.OpenAtEnd = true, true, c.open
		h := head(2)
		h.Load.Rate = c.rate
		f, err := Assemble(h, b.run, b.ctl)
		if err != nil {
			t.Fatal(err)
		}
		if v := f.Verdict; !v.Valid || v.Limit != c.limit {
			t.Errorf("%s: valid %v %q, limit %q, want %q; late %v, %d open at %d/s", c.name, v.Valid, v.Reasons, v.Limit, c.limit, v.LateP99, v.OpenAtEnd, c.rate)
		}
	}
}

func TestRestless(t *testing.T) {
	b := newBuilt()
	b.add(load.Window, 100, 10, ms(1.4), ms(10))
	b.add(load.Window, 100, 20, ms(0.2), ms(10))
	b.add(load.Window, 100, 30, ms(0.2), ms(10))
	b.add(load.Window, 100, 40, ms(0.2), ms(10))
	f, err := Assemble(head(3), b.run, b.ctl)
	if err != nil {
		t.Fatal(err)
	}
	if f.Verdict.Valid || f.Verdict.Speed != control.Restless || !strings.Contains(strings.Join(f.Verdict.Reasons, ","), WhyRestless) {
		t.Fatalf("two valid fast seconds where three are needed: %+v", f.Verdict)
	}
	b.run.Capped, b.run.Saturated = true, true
	if f, _ = Assemble(head(3), b.run, b.ctl); f.Verdict.Valid || f.Verdict.Limit != LimitTarget || !slices.Contains(f.Verdict.Reasons, WhyRestless) {
		t.Fatalf("a capped stage has a window like any other and is restless without enough seconds: %+v", f.Verdict)
	}
	if f, _ = Assemble(head(2), b.run, b.ctl); f.Verdict.Speed != control.Fast || f.Bands.Fast.Seconds != 2 || f.Bands.Slow.Seconds != 1 {
		t.Fatalf("speed %v with %d fast and %d slow seconds", f.Verdict.Speed, f.Bands.Fast.Seconds, f.Bands.Slow.Seconds)
	}
	if _, err := Assemble(head(2), b.run, b.ctl[:3]); !errors.Is(err, ErrMismatch) {
		t.Errorf("one control second short: %v", err)
	}
	if _, err := Assemble(head(0), b.run, b.ctl); err == nil {
		t.Error("need of zero accepted")
	}
}

func TestGeneratorLimitIsReported(t *testing.T) {
	for _, c := range []struct {
		name      string
		saturated bool
		late      time.Duration
		cpu       float64
		valid     bool
		limit     string
	}{
		{"held, generator fine", false, 50 * time.Microsecond, 1.0, true, ""},
		{"held, dispatcher late", false, 3 * time.Millisecond, 1.0, false, ""},
		{"held, generator busy", false, 50 * time.Microsecond, 3.3, false, ""},
		{"saturated, generator fine", true, 50 * time.Microsecond, 3.1, true, LimitTarget},
		{"saturated, dispatcher late", true, 3 * time.Millisecond, 1.0, true, LimitGenerator},
		{"saturated, generator busy", true, 50 * time.Microsecond, 3.3, true, LimitGenerator},
	} {
		b := newBuilt()
		for range 3 {
			b.add(load.Window, 100, 10, ms(1.4), ms(10)).Generator = c.cpu
		}
		for _, sec := range b.run.Seconds {
			for range 300 {
				sec.Late.Add(c.late)
			}
		}
		b.run.Saturated = c.saturated
		f, err := Assemble(head(2), b.run, b.ctl)
		if err != nil {
			t.Fatal(err)
		}
		v := f.Verdict
		if v.Valid != c.valid || v.Limit != c.limit || !c.valid && v.Reasons[0] != WhyGenerator {
			t.Errorf("%s: valid %v, limit %q, reasons %q; late %v, share %.2f", c.name, v.Valid, v.Limit, v.Reasons, v.LateP99, v.GeneratorShare)
		}
	}
}

func TestTargetShare(t *testing.T) {
	for _, c := range []struct {
		cores   string
		threads int
		cpu     float64
		want    float64
	}{
		{"0,1 2,3 4,5", 16, 1.5, 0.25},
		{"0,1", 16, 0.5, 0.25},
		{"0-5", 16, 3, 0.5},
		{"", 16, 4, 0.25},
	} {
		b := newBuilt()
		b.add(load.Warmup, 100, 10, ms(1.4), ms(10)).Target = 9
		for range 3 {
			b.add(load.Window, 100, 10, ms(1.4), ms(10)).Target = c.cpu
		}
		b.add(load.Drain, 0, 10, ms(1.4)).Target = 9
		h := head(2)
		h.Target.Cores, h.Machine.Threads = c.cores, c.threads
		f, err := Assemble(h, b.run, b.ctl)
		if err != nil {
			t.Fatal(err)
		}
		if got := f.Verdict.TargetShare; got < c.want-1e-9 || got > c.want+1e-9 {
			t.Errorf("cores %q, %.1f cores busy in each window second: target share %.3f, want %.3f", c.cores, c.cpu, got, c.want)
		}
	}
}

func TestFailuresMakeARunInvalid(t *testing.T) {
	b := newBuilt()
	for range 3 {
		b.add(load.Window, 100, 10, ms(1.4), ms(10))
	}
	b.run.Seconds[1].Failed = 2
	b.run.Problems = []string{"ledger: outcome unknown"}
	f, _ := Assemble(head(2), b.run, b.ctl)
	if f.Verdict.Valid || f.Verdict.Reasons[0] != WhyFailed || f.Verdict.Problems[0] != "ledger: outcome unknown" {
		t.Fatalf("failed calls: %+v", f.Verdict)
	}
	b.run.Saturated = true
	if f, _ = Assemble(head(2), b.run, b.ctl); !f.Verdict.Valid {
		t.Fatalf("failed calls in a saturated stage: %+v", f.Verdict)
	}
}

func TestTermAndVerify(t *testing.T) {
	b := newBuilt()
	b.add(load.Warmup, 100, 10, ms(1.4), ms(10))
	b.add(load.Window, 100, 20, ms(1.4), ms(10))
	b.add(load.Window, 100, 30, ms(1.4), ms(10))
	b.add(load.Drain, 100, 40, ms(1.4))
	b.add(load.Drain, 100, 40, ms(1.4))
	b.run.Seconds[4].Inside[0].Term = 3
	f, _ := Assemble(head(2), b.run, b.ctl)
	if f.TermChanged() {
		t.Fatal("a new term in the second second of the drain counted")
	}
	b.run.Seconds[3].Inside[0].Term = 3
	if f, _ = Assemble(head(2), b.run, b.ctl); !f.TermChanged() {
		t.Fatal("a new term in the first second of the drain not seen: a leader that hung at the end of the window shows it only there")
	}
	b.run.Seconds[3].Inside[0].Term, b.run.Seconds[4].Inside[0].Term = 2, 2
	b.run.Seconds[0].Inside[0].Term = 1
	if f, _ = Assemble(head(2), b.run, b.ctl); !f.TermChanged() {
		t.Fatal("a new term between warmup and window not seen")
	}
	f.Verified(load.Tally{Found: 1000, DebitsPosted: 7, CreditsPosted: 7}, nil)
	if !f.Verdict.Valid || !f.Books.Verified || f.Books.Found != 1000 || f.Books.DebitsPosted != 7 {
		t.Fatalf("verified: %+v %+v", f.Verdict, f.Books)
	}
	f.Verified(load.Tally{Found: 999}, errors.New("transfer 5 missing"))
	f.Refuse(WhyTerm)
	f.Refuse(WhyTerm)
	if f.Verdict.Valid || f.Books.Verified || len(f.Verdict.Reasons) != 2 || f.Verdict.Reasons[0] != WhyVerify+": transfer 5 missing" {
		t.Fatalf("refused: %+v", f.Verdict)
	}
}

func TestBounceKeepsTheRoundsAfterTheReturn(t *testing.T) {
	node := func(name string, slowest time.Duration) Part {
		return Part{Name: name, Counters: map[string]uint64{"slowest": uint64(slowest)}}
	}
	f := &File{Seconds: []Second{
		{Parts: []Part{node("n1", ms(3)), node("n2", 2900*time.Millisecond)}},
		{Parts: []Part{node("n1", ms(520))}},
		{Parts: []Part{node("n1", ms(3)), node("n2", ms(40))}},
		{Parts: []Part{node("n1", 1900*time.Millisecond), node("n2", 1348*time.Millisecond)}},
	}}
	f.Bounced(Bounce{Node: "n2", Role: "follower", Snapshot: 312 << 20})
	if b := f.Bounce; b == nil || b.Node != "n2" || b.Role != "follower" || b.Snapshot != 312<<20 || b.Slowest != 1348*time.Millisecond {
		t.Fatalf("%+v, want n2 with its snapshot and 1.348s, the longest round of n2 after its return", b)
	}
	f = &File{Seconds: f.Seconds[:1]}
	f.Bounced(Bounce{Node: "n2", Role: "follower"})
	if b := f.Bounce; b.Slowest != 0 || b.Snapshot != 0 {
		t.Errorf("a node that never left: %+v", b)
	}
}

type counting struct {
	load.Null
	mu    sync.Mutex
	syncs uint64
}

func (c *counting) Do(context.Context, int, load.Op) (load.Outcome, error) {
	time.Sleep(3 * time.Millisecond)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.syncs++
	return load.Outcome{Retries: int(c.syncs % 2)}, nil
}

func (c *counting) Sample(context.Context) (load.Inside, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return load.Inside{{Name: "n1", Role: "leader", Term: 1, Syncs: c.syncs / 4, SyncTime: time.Duration(c.syncs/4) * ms(1.4), Requests: c.syncs / 2,
		Counters: map[string]uint64{"rounds": c.syncs / 4, "logsync": c.syncs / 8, "metasync": c.syncs / 8}}}, nil
}

func TestRawFormatIsPinned(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w, err := load.Named("twophase")
		if err != nil {
			t.Fatal(err)
		}
		target := &counting{}
		run, err := load.Drive(context.Background(), target, load.NewStream(w, 42), load.Plan{Rate: 20, Lanes: 2, Warmup: time.Second, Window: 3 * time.Second, Drain: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		var ctl []control.Second
		for i := range run.Seconds {
			mean := ms(1.4)
			if i == 0 {
				mean = ms(0.7)
			}
			ctl = append(ctl, control.Second{Span: time.Second, Flushes: 120, FlushTime: 120 * mean, Writes: 360, WriteTime: 360 * ms(0.05),
				Generator: 0.02, Target: 0.04, Foreign: 0.3, MHz: []int{2500, 2480}, Available: 7 << 30})
		}
		h := Head{
			ID:          "m2-l1-twophase-20-1",
			Measurement: "m2",
			Pass:        1,
			Build:       Build{Commit: "6d4b109", Tags: []string{}, Go: "go1.27.4"},
			Machine: control.Machine{CPU: "12th Gen Intel(R) Core(TM) i5-12500H", Threads: 16, Allowed: "8-15", GoMaxProcs: 8, RAM: 16430473216,
				Kernel: "6.12.117+deb13-amd64", Governor: "powersave", Turbo: "on", Power: "mains", Filesystem: "ext4", Mount: "rw,relatime",
				Disk: "nvme0n1", Model: "PC801 NVMe SK hynix 512GB", Used: 0.75},
			Target: Target{Kind: "ledger", Nodes: 1, Lanes: 2, Cores: "0,1", Env: map[string]string{"LEDGER_SNAPSHOT_EVERY": "4294967296"}},
			Load:   Load{Workload: w.Name, Accounts: w.Accounts, Rate: 20, Warmup: time.Second, Window: 3 * time.Second, MaxWindow: 3 * time.Second, Drain: time.Second, Need: 2, Seed: 42},
			Probe: control.Probe{
				Growing:  []control.ProbeSecond{{Syncs: 2012, SyncTime: 999 * time.Millisecond, Flushes: 2016, FlushTime: 438 * time.Millisecond, Speed: control.Fast}},
				Prealloc: []control.ProbeSecond{{Syncs: 4242, SyncTime: 999 * time.Millisecond, Flushes: 4242, FlushTime: 911 * time.Millisecond, Speed: control.Fast}},
				Means:    []control.ProbeMean{{Speed: control.Slow}, {Speed: control.Fast, GrowingSeconds: 1, Growing: 496857, PreallocSeconds: 1, Prealloc: 235693, Ratio: 2.108}},
			},
			Thresholds: control.Defaults(),
		}
		f, err := Assemble(h, run, ctl)
		if err != nil {
			t.Fatal(err)
		}
		f.Verified(load.Tally{Found: len(run.Books.Sample), DebitsPosted: 1, CreditsPosted: 1}, nil)
		var buf bytes.Buffer
		if err := Write(&buf, f); err != nil {
			t.Fatal(err)
		}
		const path = "testdata/example.json"
		if *update {
			if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
				t.Fatal(err)
			}
			return
		}
		want, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(buf.Bytes(), want) {
			got, pinned := strings.Split(buf.String(), "\n"), strings.Split(string(want), "\n")
			for i := range min(len(got), len(pinned)) {
				if got[i] != pinned[i] {
					t.Fatalf("line %d: %s, pinned %s", i+1, got[i], pinned[i])
				}
			}
			t.Fatalf("%d lines, %d pinned", len(got), len(pinned))
		}
		back, err := Read(bytes.NewReader(want))
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(back, f) {
			t.Fatal("the pinned file reads back different")
		}
	})
}
