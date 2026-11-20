package control

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"testing/fstest"
	"testing/synctest"
	"time"
)

func TestDiskstats(t *testing.T) {
	f, err := os.Open("testdata/diskstats-6.12")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	d, err := ParseDiskstats(f, "nvme0n1")
	if err != nil {
		t.Fatal(err)
	}
	want := Disk{Writes: 17286669, WriteTime: 30436554 * time.Millisecond, Flushes: 5089928, FlushTime: 3573229 * time.Millisecond}
	if d != want {
		t.Fatalf("nvme0n1: %+v, want %+v", d, want)
	}
	if _, err := f.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	if p, err := ParseDiskstats(f, "nvme0n1p2"); err != nil || p.Flushes != 0 || p.Writes != 16287700 {
		t.Fatalf("a partition counts no flushes: %+v, %v", p, err)
	}
	if _, err := ParseDiskstats(strings.NewReader(""), "sdz"); !errors.Is(err, ErrNoDisk) {
		t.Errorf("missing disk: %v", err)
	}
	old := "   8       0 sda 100 0 800 10 200 0 1600 30 0 40 40\n"
	if _, err := ParseDiskstats(strings.NewReader(old), "sda"); !errors.Is(err, ErrDiskstats) {
		t.Errorf("line without flush columns: %v", err)
	}
}

func flushes(n uint64, mean time.Duration) Second {
	return Second{Flushes: n, FlushTime: time.Duration(n) * mean} //nolint:gosec // G115: a small test count
}

func TestSpeedOfASecond(t *testing.T) {
	th := Defaults()
	for _, c := range []struct {
		n    uint64
		mean time.Duration
		want Speed
	}{
		{100, 220 * time.Microsecond, Fast},
		{100, 499 * time.Microsecond, Fast},
		{100, 500 * time.Microsecond, Transition},
		{100, time.Millisecond, Transition},
		{100, 1001 * time.Microsecond, Slow},
		{100, 1500 * time.Microsecond, Slow},
		{20, 1500 * time.Microsecond, Slow},
		{19, 1500 * time.Microsecond, None},
		{0, 0, None},
	} {
		s := flushes(c.n, c.mean)
		if got := th.Speed(s.Flushes, s.FlushTime); got != c.want {
			t.Errorf("%d flushes of %v: %v, want %v", c.n, c.mean, got, c.want)
		}
	}

	slow, fast, mid := flushes(300, 1400*time.Microsecond), flushes(2000, 220*time.Microsecond), flushes(500, 700*time.Microsecond)
	busy, hot, swapped := slow, slow, slow
	busy.Foreign, hot.Throttled, swapped.SwappedOut = 1.2, 3, 8
	unread := Second{Unread: []string{"diskstats: gone"}}
	secs := []Second{slow, slow, fast, fast, mid, fast, fast, flushes(5, time.Millisecond), fast, slow, slow, busy, hot, swapped, unread, slow}
	want := []struct {
		speed Speed
		why   string
	}{
		{Slow, ""}, {Slow, ""}, {Fast, WhyChange}, {Fast, ""}, {Transition, WhyTransition}, {Fast, WhyChange}, {Fast, ""},
		{None, WhyFewFlushes}, {Fast, WhyChange}, {Slow, WhyChange}, {Slow, ""}, {Slow, WhyForeign}, {Slow, ""},
		{Slow, WhySwapped}, {None, WhyUnread}, {Slow, WhyChange},
	}
	marks := th.Judge(secs)
	for i, m := range marks {
		if m.Speed != want[i].speed || m.Valid != (want[i].why == "") || want[i].why != "" && !slices.Contains(m.Why, want[i].why) {
			t.Errorf("second %d: %v valid %v %q, want %v %q", i, m.Speed, m.Valid, m.Why, want[i].speed, want[i].why)
		}
	}
}

func TestUnpace(t *testing.T) {
	th := Defaults()
	busy := func(n uint64, mean time.Duration) Second {
		s := flushes(n, mean)
		s.Target = 1.0
		return s
	}
	run := func(few int) []Second {
		secs := []Second{busy(300, 1400*time.Microsecond)}
		for i := range 15 {
			switch {
			case i < few:
				secs = append(secs, busy(8, 2*time.Millisecond))
			case i%3 == 0:
				secs = append(secs, busy(300, 1400*time.Microsecond))
			case i%3 == 1:
				secs = append(secs, busy(2000, 220*time.Microsecond))
			default:
				secs = append(secs, busy(500, 700*time.Microsecond))
			}
		}
		return append(secs, flushes(3, time.Millisecond))
	}
	window := func(i int) bool { return i >= 1 && i <= 15 }

	secs := run(7)
	marks := th.Judge(secs)
	before := slices.Clone(marks)
	if th.Unpace(marks, secs, window) {
		t.Fatal("7 of 15 window seconds with few flushes made the run unpaced")
	}
	for i := range marks {
		if marks[i].Speed != before[i].Speed || marks[i].Valid != before[i].Valid || !slices.Equal(marks[i].Why, before[i].Why) {
			t.Errorf("second %d changed in a paced run: %+v, was %+v", i, marks[i], before[i])
		}
	}

	secs = run(8)
	secs[13].Foreign = 1.5
	secs[4].Target = 0.02
	marks = th.Judge(secs)
	if !th.Unpace(marks, secs, window) {
		t.Fatal("8 of 15 window seconds with few flushes: not unpaced")
	}
	for i, m := range marks {
		var want []string
		switch i {
		case 4:
			want = []string{WhyIdle}
		case 13:
			want = []string{WhyForeign}
		}
		if m.Speed != Unpaced || !slices.Equal(m.Why, want) || m.Valid != (len(want) == 0) {
			t.Errorf("second %d: %v valid %v %q, want unpaced with %q", i, m.Speed, m.Valid, m.Why, want)
		}
	}
	if marks[16].Speed != Unpaced || !marks[16].Valid {
		t.Errorf("an idle second after the window: %+v, want unpaced and valid, only window seconds can stand", marks[16])
	}
}

func TestSettle(t *testing.T) {
	mark := func(s Speed, valid bool) Mark { return Mark{Speed: s, Valid: valid} }
	var window []Mark
	for range 8 {
		window = append(window, mark(Slow, true), mark(Fast, true), mark(Slow, false))
	}
	if got := Settle(window, 8); got != Slow {
		t.Errorf("8 valid slow and 8 valid fast: %v, want slow", got)
	}
	if got := Settle(window, 9); got != Restless {
		t.Errorf("8 valid of each, 9 needed: %v, want restless", got)
	}
	window = append(window, mark(Fast, true))
	if got := Settle(window, 9); got != Fast {
		t.Errorf("9 valid fast, 8 valid slow: %v, want fast", got)
	}
	if got := Settle(window[:3], 1); got != Slow {
		t.Errorf("slow wins when both have enough: %v", got)
	}
	var unpaced []Mark
	for range 8 {
		unpaced = append(unpaced, mark(Unpaced, true), mark(Unpaced, false))
	}
	if got := Settle(unpaced, 8); got != Unpaced {
		t.Errorf("8 valid unpaced seconds, 8 needed: %v, want unpaced", got)
	}
	if got := Settle(unpaced, 9); got != Restless {
		t.Errorf("8 valid unpaced seconds, 9 needed: %v, want restless", got)
	}
	if got := Settle(append(unpaced, window...), 8); got != Slow {
		t.Errorf("slow before unpaced: %v", got)
	}
}

func TestAgreement(t *testing.T) {
	ms := func(f float64) time.Duration { return time.Duration(f * float64(time.Millisecond)) }
	run := func(s Speed, flush, p50 float64, tput, syncs float64) Figures {
		return Figures{Valid: true, Speed: s, Flush: ms(flush), P50: ms(p50), Throughput: tput, Syncs: syncs}
	}
	invalid := run(Slow, 1.5, 11, 2000, 0.13)
	invalid.Valid = false
	for _, c := range []struct {
		name string
		runs []Figures
		want Accord
		pair [2]int
	}{
		{"same disk, within", []Figures{run(Slow, 1.50, 11, 2000, 0.13), run(Slow, 1.40, 10.2, 1950, 0.125)}, Agreed, [2]int{0, 1}},
		{"same disk, p50 off", []Figures{run(Slow, 1.50, 11, 2000, 0.13), run(Slow, 1.45, 12.5, 2000, 0.13)}, Missed, [2]int{0, 1}},
		{"same disk, throughput off", []Figures{run(Slow, 1.50, 11, 2000, 0.13), run(Slow, 1.45, 11, 1780, 0.13)}, Missed, [2]int{0, 1}},
		{"same disk, fsync off", []Figures{run(Slow, 1.50, 11, 2000, 0.13), run(Slow, 1.45, 11, 2000, 0.15)}, Missed, [2]int{0, 1}},
		{"disks differ", []Figures{run(Slow, 1.2, 9, 2000, 0.13), run(Slow, 1.6, 12, 2000, 0.13), run(Slow, 1.9, 14, 2000, 0.13)}, Undecided, [2]int{}},
		{"first equal pair decides", []Figures{run(Slow, 1.2, 9, 2000, 0.13), run(Slow, 1.6, 12, 2000, 0.13), run(Slow, 1.25, 12, 2000, 0.13), run(Slow, 1.21, 9, 2000, 0.13)}, Missed, [2]int{0, 2}},
		{"four runs, flush units", []Figures{run(Slow, 1.2, 9, 2000, 0.13), run(Slow, 1.6, 12, 1500, 0.13), run(Slow, 2.0, 15.2, 2000, 0.13), run(Slow, 2.6, 19, 2000, 0.13)}, AgreedInFlushes, [2]int{1, 2}},
		{"four runs, flush units off", []Figures{run(Slow, 1.2, 9, 2000, 0.13), run(Slow, 1.6, 14, 2000, 0.13), run(Slow, 2.0, 12, 2000, 0.13), run(Slow, 2.6, 19, 2000, 0.13)}, Missed, [2]int{1, 2}},
		{"slow before fast", []Figures{run(Slow, 1.2, 9, 2000, 0.13), run(Fast, 0.22, 2, 2000, 0.13), run(Fast, 0.23, 2.1, 2000, 0.13), run(Slow, 1.6, 12, 2000, 0.13)}, Undecided, [2]int{}},
		{"fast when no slow", []Figures{run(Fast, 0.22, 2, 2000, 0.13), invalid, run(Fast, 0.23, 2.1, 2050, 0.13)}, Agreed, [2]int{0, 2}},
		{"invalid runs do not count", []Figures{invalid, run(Slow, 1.5, 11, 2000, 0.13), invalid}, Undecided, [2]int{}},
		{"unpaced, any disk", []Figures{run(Unpaced, 0.3, 40, 18000, 0.001), run(Unpaced, 20, 41, 18500, 0.001)}, Agreed, [2]int{0, 1}},
		{"unpaced, throughput off", []Figures{run(Unpaced, 0.3, 40, 18000, 0.001), run(Unpaced, 20, 41, 20500, 0.001)}, Missed, [2]int{0, 1}},
		{"unpaced alone", []Figures{run(Unpaced, 0.3, 40, 18000, 0.001)}, Undecided, [2]int{}},
		{"unpaced, four runs, never in flush units", []Figures{run(Unpaced, 0.3, 40, 18000, 0.001), run(Unpaced, 20, 41, 18500, 0.001),
			run(Unpaced, 0.31, 60, 18100, 0.001), run(Unpaced, 21, 90, 18200, 0.001)}, Agreed, [2]int{0, 1}},
		{"fast before unpaced", []Figures{run(Unpaced, 0.3, 40, 18000, 0.001), run(Fast, 0.22, 2, 2000, 0.13), run(Unpaced, 20, 41, 18500, 0.001)}, Undecided, [2]int{}},
		{"nothing valid", []Figures{invalid}, Undecided, [2]int{}},
	} {
		ag := Agree(c.runs)
		if ag.Accord != c.want || ag.Pair != c.pair {
			t.Errorf("%s: %v on %v (%s), want %v on %v", c.name, ag.Accord, ag.Pair, ag.Why, c.want, c.pair)
		}
	}
}

func machineFS(flushes, flushMs, busy, self, pid, cg, khz, throttles, swapped uint64) fstest.MapFS {
	file := func(s string) *fstest.MapFile { return &fstest.MapFile{Data: []byte(s)} }
	n := func(v uint64) string { return strconv.FormatUint(v, 10) }
	return fstest.MapFS{
		"proc/diskstats":            file(" 259 0 nvme0n1 1 0 8 1 " + n(flushes*3) + " 0 80 " + n(flushMs*2) + " 0 9 9 0 0 0 0 " + n(flushes) + " " + n(flushMs) + "\n"),
		"proc/stat":                 file("cpu  " + n(busy) + " 0 0 999999 77 0 0 0 0 0\ncpu0 1 2 3\n"),
		"proc/self/stat":            file("42 (bench test) S 1 1 1 0 -1 0 0 0 0 0 " + n(self) + " 0 0 0 20 0 9\n"),
		"proc/4242/stat":            file("4242 (ledgerd) S 1 1 1 0 -1 0 0 0 0 0 " + n(pid) + " " + n(pid) + " 0 0 20 0 9\n"),
		"proc/meminfo":              file("MemTotal: 16000000 kB\nMemAvailable:    7000000 kB\n"),
		"proc/vmstat":               file("pswpin 3\npswpout " + n(swapped) + "\n"),
		"sys/fs/cgroup/pg/cpu.stat": file("usage_usec " + n(cg) + "\nuser_usec 1\n"),
		"sys/devices/system/cpu/cpu0/cpufreq/scaling_cur_freq":                file(n(khz) + "\n"),
		"sys/devices/system/cpu/cpu0/thermal_throttle/core_throttle_count":    file(n(throttles) + "\n"),
		"sys/devices/system/cpu/cpu0/thermal_throttle/package_throttle_count": file("5\n"),
	}
}

func TestMeter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := Watch{Disk: "nvme0n1", Pids: []int{4242}, Cgroups: []string{"pg"}, CPUs: []int{0}}
		m := NewMeter(machineFS(1000, 500, 10_000, 100, 50, 1_000_000, 2_400_000, 7, 11), w)
		time.Sleep(2 * time.Second)
		m.fsys = machineFS(1300, 950, 10_550, 160, 80, 1_400_000, 3_100_000, 7, 11)
		s := m.Tick()
		want := Second{Span: 2 * time.Second, Flushes: 300, FlushTime: 450 * time.Millisecond, Writes: 900, WriteTime: 900 * time.Millisecond,
			Generator: 0.3, Target: 0.5, Foreign: 2.75 - 0.3 - 0.5, MHz: []int{3100}, Available: 7_000_000 * 1024}
		if !same(s, want) {
			t.Fatalf("second\n %+v, want\n %+v", s, want)
		}
		time.Sleep(time.Second)
		m.fsys = machineFS(1300, 950, 10_550, 160, 80, 1_400_000, 3_100_000, 9, 15)
		delete(m.fsys.(fstest.MapFS), "proc/4242/stat")
		s = m.Tick()
		if len(s.Unread) != 1 || !strings.Contains(s.Unread[0], "pid 4242") || s.Throttled != 0 {
			t.Fatalf("a gone process: unread %q, throttled %d", s.Unread, s.Throttled)
		}
		time.Sleep(time.Second)
		m.fsys = machineFS(1300, 950, 10_550, 160, 80, 1_400_000, 3_100_000, 10, 16)
		if s = m.Tick(); len(s.Unread) != 1 {
			t.Fatalf("the second after a failed reading needs both ends: unread %q", s.Unread)
		}
		time.Sleep(time.Second)
		m.fsys = machineFS(1300, 950, 10_550, 160, 80, 1_400_000, 3_100_000, 12, 20)
		if s = m.Tick(); len(s.Unread) != 0 || s.Throttled != 2 || s.SwappedOut != 4 {
			t.Fatalf("throttled %d, swapped %d, unread %q, want 2, 4 and none", s.Throttled, s.SwappedOut, s.Unread)
		}
		if len(m.Seconds()) != 4 {
			t.Fatalf("%d seconds kept, want 4", len(m.Seconds()))
		}
	})
}

func same(a, b Second) bool {
	close := func(x, y float64) bool { return x-y < 1e-9 && y-x < 1e-9 }
	return a.Span == b.Span && a.Flushes == b.Flushes && a.FlushTime == b.FlushTime && a.Writes == b.Writes && a.WriteTime == b.WriteTime &&
		close(a.Generator, b.Generator) && close(a.Target, b.Target) && close(a.Foreign, b.Foreign) && slices.Equal(a.MHz, b.MHz) &&
		a.Throttled == b.Throttled && a.Available == b.Available && a.SwappedOut == b.SwappedOut && len(a.Unread) == len(b.Unread)
}

func TestMeterOnThisMachine(t *testing.T) {
	disk, err := DiskOf(os.TempDir())
	if err != nil {
		t.Skipf("no block device under %s: %v", os.TempDir(), err)
	}
	if _, err := os.Stat("/sys/devices/system/cpu/cpu0/thermal_throttle"); err != nil {
		t.Skipf("no thermal counters on this machine: %v", err)
	}
	m := NewMeter(os.DirFS("/"), Watch{Disk: disk, Pids: []int{os.Getpid()}, CPUs: []int{0, 1}})
	time.Sleep(200 * time.Millisecond)
	s := m.Tick()
	if len(s.Unread) > 0 {
		t.Fatalf("unread on this machine: %q", s.Unread)
	}
	if len(s.MHz) != 2 || s.MHz[0] <= 0 || s.Available == 0 {
		t.Fatalf("second %+v", s)
	}
}

func TestProbe(t *testing.T) {
	dir := t.TempDir()
	disk, err := DiskOf(dir)
	if err != nil {
		t.Skipf("no block device under %s: %v", dir, err)
	}
	p, err := RunProbe(context.Background(), dir, disk, time.Second, Defaults())
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range [][]ProbeSecond{p.Growing, p.Prealloc} {
		if len(kind) != 1 || kind[0].Syncs == 0 || kind[0].SyncTime <= 0 {
			t.Fatalf("probe seconds %+v", kind)
		}
	}
	t.Logf("growing %+v, prealloc %+v, means %+v", p.Growing, p.Prealloc, p.Means)
	left, err := filepath.Glob(filepath.Join(dir, "probe-*"))
	if err != nil || len(left) > 0 {
		t.Fatalf("probe left %q behind", left)
	}
	if len(p.Means) != 2 || p.Means[0].Speed != Slow || p.Means[1].Speed != Fast {
		t.Fatalf("means %+v", p.Means)
	}
}

func TestProbeMeans(t *testing.T) {
	sec := func(s Speed, n uint64, took time.Duration) ProbeSecond {
		return ProbeSecond{Speed: s, Syncs: n, SyncTime: took}
	}
	p := Probe{
		Growing:  []ProbeSecond{sec(Slow, 300, 900*time.Millisecond), sec(Slow, 100, 400*time.Millisecond), sec(Fast, 2000, time.Second), sec(Transition, 9, time.Second)},
		Prealloc: []ProbeSecond{sec(Slow, 600, 990*time.Millisecond), sec(Fast, 4000, 960*time.Millisecond)},
	}
	got := means(p)
	want := []ProbeMean{
		{Speed: Slow, GrowingSeconds: 2, Growing: 3250 * time.Microsecond, PreallocSeconds: 1, Prealloc: 1650 * time.Microsecond, Ratio: 3250.0 / 1650},
		{Speed: Fast, GrowingSeconds: 1, Growing: 500 * time.Microsecond, PreallocSeconds: 1, Prealloc: 240 * time.Microsecond, Ratio: 500.0 / 240},
	}
	if !slices.Equal(got, want) {
		t.Fatalf("means %+v, want %+v", got, want)
	}
}

func TestReadMachine(t *testing.T) {
	m, err := ReadMachine(os.TempDir())
	if err != nil {
		t.Skipf("no machine facts under %s: %v", os.TempDir(), err)
	}
	if m.Disk == "" || m.Filesystem == "" || m.Kernel == "" || m.Threads == 0 || m.RAM == 0 || m.Used <= 0 || m.Used >= 1 || m.GoMaxProcs == 0 {
		t.Fatalf("machine %+v", m)
	}
	t.Logf("%+v", m)

	mounts := strings.Join([]string{
		"26 1 259:2 / / rw,relatime shared:1 - ext4 /dev/nvme0n1p2 rw,errors=remount-ro",
		"40 26 0:35 / /var/tmp/x rw,nosuid,nodev shared:20 - tmpfs tmpfs rw,size=1g",
		"41 26 0:36 / /var/tmpfs rw shared:21 - tmpfs tmpfs rw",
	}, "\n")
	for dir, want := range map[string][2]string{
		"/var/tmp/bench": {"ext4", "rw,relatime"},
		"/var/tmp/x/run": {"tmpfs", "rw,nosuid,nodev"},
		"/var/tmp/x":     {"tmpfs", "rw,nosuid,nodev"},
		"/var/tmpfs2":    {"ext4", "rw,relatime"},
	} {
		if fs, opts := mountOf(strings.NewReader(mounts), dir); fs != want[0] || opts != want[1] {
			t.Errorf("%s: %s %s, want %v", dir, fs, opts, want)
		}
	}
	model, threads := cpuinfo(strings.NewReader("processor\t: 0\nmodel name\t: 12th Gen Intel(R) Core(TM) i5-12500H\n\nprocessor\t: 1\nmodel name\t: 12th Gen Intel(R) Core(TM) i5-12500H\n"))
	if model != "12th Gen Intel(R) Core(TM) i5-12500H" || threads != 2 {
		t.Errorf("cpuinfo: %q, %d", model, threads)
	}
}

func TestReadHeat(t *testing.T) {
	file := func(s string) *fstest.MapFile { return &fstest.MapFile{Data: []byte(s + "\n")} }
	cpu := "sys/devices/system/cpu/"
	fsys := fstest.MapFS{
		cpu + "cpu0/cpufreq/energy_performance_preference":           file("balance_performance"),
		"sys/firmware/acpi/platform_profile":                         file("performance"),
		cpu + "intel_pstate/max_perf_pct":                            file("100"),
		"sys/class/thermal/thermal_zone1/type":                       file("SKIN"),
		"sys/class/thermal/thermal_zone1/temp":                       file("40050"),
		"sys/class/thermal/thermal_zone11/type":                      file("x86_pkg_temp"),
		"sys/class/thermal/thermal_zone11/temp":                      file("59000"),
		cpu + "cpu0/topology/physical_package_id":                    file("0"),
		cpu + "cpu0/thermal_throttle/package_throttle_count":         file("62114"),
		cpu + "cpu0/thermal_throttle/package_throttle_total_time_ms": file("273576"),
		cpu + "cpu1/topology/physical_package_id":                    file("0"),
		cpu + "cpu1/thermal_throttle/package_throttle_count":         file("62114"),
		cpu + "cpu1/thermal_throttle/package_throttle_total_time_ms": file("273576"),
		cpu + "cpufreq/policy0/scaling_governor":                     file("powersave"),
	}
	h := ReadHeat(fsys)
	want := Heat{Preference: "balance_performance", Profile: "performance", MaxPerf: "100", PackageTemp: 59,
		Packages: []PackageHeat{{ID: "0", Throttles: 62114, ThrottledMs: 273576}}}
	if !reflect.DeepEqual(h, want) {
		t.Fatalf("heat %+v, want %+v", h, want)
	}
	if h := ReadHeat(fstest.MapFS{}); h.Preference != "" || h.PackageTemp != 0 || len(h.Packages) != 0 {
		t.Fatalf("heat of a machine without these files: %+v", h)
	}
	if h := ReadHeat(os.DirFS("/")); len(h.Packages) == 0 {
		t.Logf("no thermal counters on this machine: %+v", h)
	}
}

func TestCheckStart(t *testing.T) {
	taken, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer taken.Close()
	busy := taken.Addr().(*net.TCPAddr).Port
	free, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	open := free.Addr().(*net.TCPAddr).Port
	free.Close()

	calm := fstest.MapFS{
		"proc/meminfo":  {Data: []byte("MemTotal: 16000000 kB\nMemAvailable:    6500000 kB\n")},
		"proc/1/comm":   {Data: []byte("systemd\n")},
		"proc/812/comm": {Data: []byte("ledgerctl\n")},
	}
	if err := CheckStart(calm, Machine{Power: "mains"}, open); err != nil {
		t.Fatalf("calm machine: %v", err)
	}
	crowded := fstest.MapFS{
		"proc/meminfo":  {Data: []byte("MemAvailable:    3500000 kB\n")},
		"proc/1/comm":   {Data: []byte("systemd\n")},
		"proc/977/comm": {Data: []byte("ledgerd\n")},
	}
	err = CheckStart(crowded, Machine{Power: "battery"}, busy)
	if !errors.Is(err, ErrAbort) {
		t.Fatalf("crowded machine: %v", err)
	}
	for _, want := range []string{"not on mains power", "3417 MiB available", "ledgerd already runs as 977", fmt.Sprintf("port %d taken", busy)} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("%q does not say %q", err, want)
		}
	}
	if TooManyBad(2, 8) || !TooManyBad(3, 8) || TooManyBad(0, 0) {
		t.Error("more than a quarter of a block bad stops the plan, a quarter does not")
	}
}
