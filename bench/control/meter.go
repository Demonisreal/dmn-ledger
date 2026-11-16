package control

import (
	"bufio"
	"bytes"
	"fmt"
	"io/fs"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"
)

const userHZ = 100

type Watch struct {
	Disk    string
	Pids    []int
	Cgroups []string
	CPUs    []int
}

type Second struct {
	Span       time.Duration
	Flushes    uint64
	FlushTime  time.Duration
	Writes     uint64
	WriteTime  time.Duration
	Generator  float64
	Target     float64
	Foreign    float64
	MHz        []int
	Throttled  uint64
	Available  uint64
	SwappedOut uint64
	Unread     []string
}

type reading struct {
	at        time.Time
	disk      Disk
	busy      uint64
	self      uint64
	target    time.Duration
	throttles uint64
	swapped   uint64
	available uint64
	mhz       []int
	unread    []string
}

type Meter struct {
	fsys  fs.FS
	watch Watch
	last  reading
	secs  []Second
}

func NewMeter(fsys fs.FS, w Watch) *Meter {
	m := &Meter{fsys: fsys, watch: w}
	m.last = m.read()
	return m
}

func (m *Meter) Tick() Second {
	now := m.read()
	prev := m.last
	m.last = now
	span := now.at.Sub(prev.at)
	cores := func(ticks uint64) float64 { return float64(ticks) / userHZ / span.Seconds() }
	if unread := slices.Concat(prev.unread, now.unread); len(unread) > 0 {
		s := Second{Span: span, MHz: now.mhz, Available: now.available, Unread: unread}
		m.secs = append(m.secs, s)
		return s
	}
	disk := now.disk.since(prev.disk)
	s := Second{
		Span:       span,
		Flushes:    disk.Flushes,
		FlushTime:  disk.FlushTime,
		Writes:     disk.Writes,
		WriteTime:  disk.WriteTime,
		Generator:  cores(now.self - prev.self),
		Target:     (now.target - prev.target).Seconds() / span.Seconds(),
		MHz:        now.mhz,
		Throttled:  now.throttles - prev.throttles,
		Available:  now.available,
		SwappedOut: now.swapped - prev.swapped,
	}
	s.Foreign = max(cores(now.busy-prev.busy)-s.Generator-s.Target, 0)
	m.secs = append(m.secs, s)
	return s
}

func (m *Meter) Seconds() []Second {
	return m.secs
}

func (m *Meter) read() reading {
	r := reading{at: time.Now()}
	fail := func(what string, err error) {
		r.unread = append(r.unread, fmt.Sprintf("%s: %v", what, err))
	}
	if b, err := fs.ReadFile(m.fsys, "proc/diskstats"); err != nil {
		fail("diskstats", err)
	} else if r.disk, err = ParseDiskstats(bytes.NewReader(b), m.watch.Disk); err != nil {
		fail("diskstats", err)
	}
	var err error
	if r.busy, err = m.busy(); err != nil {
		fail("stat", err)
	}
	if r.self, err = m.ticks("proc/self/stat"); err != nil {
		fail("self", err)
	}
	for _, pid := range m.watch.Pids {
		n, err := m.ticks(fmt.Sprintf("proc/%d/stat", pid))
		if err != nil {
			fail(fmt.Sprintf("pid %d", pid), err)
		}
		r.target += time.Duration(n) * time.Second / userHZ //nolint:gosec // G115: CPU ticks since start
	}
	for _, cg := range m.watch.Cgroups {
		n, err := field(m.fsys, path.Join("sys/fs/cgroup", cg, "cpu.stat"), "usage_usec")
		if err != nil {
			fail("cgroup "+cg, err)
		}
		r.target += time.Duration(n) * time.Microsecond //nolint:gosec // G115: CPU time since start
	}
	for _, cpu := range m.watch.CPUs {
		dir := fmt.Sprintf("sys/devices/system/cpu/cpu%d", cpu)
		khz, err := m.number(dir + "/cpufreq/scaling_cur_freq")
		if err != nil {
			fail(fmt.Sprintf("cpu %d", cpu), err)
		}
		r.mhz = append(r.mhz, int(khz/1000)) //nolint:gosec // G115: a clock in MHz
		for _, name := range []string{"core_throttle_count", "package_throttle_count"} {
			n, err := m.number(dir + "/thermal_throttle/" + name)
			if err != nil {
				fail(fmt.Sprintf("cpu %d", cpu), err)
			}
			r.throttles += n
		}
	}
	kb, err := field(m.fsys, "proc/meminfo", "MemAvailable:")
	if err != nil {
		fail("meminfo", err)
	}
	r.available = kb * 1024
	if r.swapped, err = field(m.fsys, "proc/vmstat", "pswpout"); err != nil {
		fail("vmstat", err)
	}
	return r
}

func (m *Meter) busy() (uint64, error) {
	b, err := fs.ReadFile(m.fsys, "proc/stat")
	if err != nil {
		return 0, err
	}
	line, _, _ := bytes.Cut(b, []byte("\n"))
	f := strings.Fields(string(line))
	if len(f) < 9 || f[0] != "cpu" {
		return 0, fmt.Errorf("control: no cpu line in /proc/stat")
	}
	var busy uint64
	for i, col := range f[1:9] {
		n, err := strconv.ParseUint(col, 10, 64)
		if err != nil {
			return 0, err
		}
		if i != 3 && i != 4 {
			busy += n
		}
	}
	return busy, nil
}

func (m *Meter) ticks(name string) (uint64, error) {
	b, err := fs.ReadFile(m.fsys, name)
	if err != nil {
		return 0, err
	}
	f := strings.Fields(string(b[bytes.LastIndexByte(b, ')')+1:]))
	if len(f) < 13 {
		return 0, fmt.Errorf("control: short %s", name)
	}
	user, err := strconv.ParseUint(f[11], 10, 64)
	if err != nil {
		return 0, err
	}
	sys, err := strconv.ParseUint(f[12], 10, 64)
	return user + sys, err
}

func (m *Meter) number(name string) (uint64, error) {
	b, err := fs.ReadFile(m.fsys, name)
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
}

func field(fsys fs.FS, name, key string) (uint64, error) {
	b, err := fs.ReadFile(fsys, name)
	if err != nil {
		return 0, err
	}
	sc := bufio.NewScanner(bytes.NewReader(b))
	for sc.Scan() {
		f := strings.Fields(sc.Text())
		if len(f) >= 2 && f[0] == key {
			return strconv.ParseUint(f[1], 10, 64)
		}
	}
	return 0, fmt.Errorf("control: no %s in %s", key, name)
}
