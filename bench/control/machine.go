package control

import (
	"bufio"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"syscall"
)

const Mains = "mains"

type Machine struct {
	CPU        string  `json:"cpu"`
	Threads    int     `json:"threads"`
	Allowed    string  `json:"allowed"`
	GoMaxProcs int     `json:"gomaxprocs"`
	RAM        uint64  `json:"ram"`
	Kernel     string  `json:"kernel"`
	Governor   string  `json:"governor"`
	Turbo      string  `json:"turbo"`
	Power      string  `json:"power"`
	Filesystem string  `json:"filesystem"`
	Mount      string  `json:"mount"`
	Disk       string  `json:"disk"`
	Model      string  `json:"model"`
	Used       float64 `json:"used"`
}

func ReadMachine(dir string) (Machine, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return Machine{}, err
	}
	m := Machine{GoMaxProcs: runtime.GOMAXPROCS(0), Kernel: line("/proc/sys/kernel/osrelease"), Governor: line("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor")}
	if m.Disk, err = DiskOf(abs); err != nil {
		return Machine{}, err
	}
	m.Model = line("/sys/block/" + m.Disk + "/device/model")
	switch line("/sys/devices/system/cpu/intel_pstate/no_turbo") {
	case "0":
		m.Turbo = "on"
	case "1":
		m.Turbo = "off"
	}
	m.Power = power()
	if f, err := os.Open("/proc/cpuinfo"); err == nil {
		m.CPU, m.Threads = cpuinfo(f)
		_ = f.Close()
	}
	if f, err := os.Open("/proc/self/status"); err == nil {
		m.Allowed = keyed(f, "Cpus_allowed_list:")
		_ = f.Close()
	}
	if f, err := os.Open("/proc/meminfo"); err == nil {
		if kb, err := strconv.ParseUint(strings.TrimSuffix(keyed(f, "MemTotal:"), " kB"), 10, 64); err == nil {
			m.RAM = kb * 1024
		}
		_ = f.Close()
	}
	if f, err := os.Open("/proc/self/mountinfo"); err == nil {
		m.Filesystem, m.Mount = mountOf(f, abs)
		_ = f.Close()
	}
	var st syscall.Statfs_t
	if err := syscall.Statfs(abs, &st); err != nil {
		return Machine{}, fmt.Errorf("control: statfs %s: %w", abs, err)
	}
	if used := st.Blocks - st.Bfree; used+st.Bavail > 0 {
		m.Used = float64(used) / float64(used+st.Bavail)
	}
	return m, nil
}

func line(name string) string {
	b, err := os.ReadFile(name) //nolint:gosec // G304: fixed paths under /proc and /sys
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

func power() string {
	supplies, err := filepath.Glob("/sys/class/power_supply/*")
	if err != nil {
		return ""
	}
	seen := false
	for _, dir := range supplies {
		if line(filepath.Join(dir, "type")) != "Mains" {
			continue
		}
		seen = true
		if line(filepath.Join(dir, "online")) == "1" {
			return Mains
		}
	}
	if seen {
		return "battery"
	}
	return ""
}

func cpuinfo(r io.Reader) (model string, threads int) {
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		key, val, ok := strings.Cut(sc.Text(), ":")
		switch key = strings.TrimSpace(key); {
		case !ok:
		case key == "processor":
			threads++
		case key == "model name" && model == "":
			model = strings.TrimSpace(val)
		}
	}
	return model, threads
}

func keyed(r io.Reader, key string) string {
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		if rest, ok := strings.CutPrefix(sc.Text(), key); ok {
			return strings.TrimSpace(rest)
		}
	}
	return ""
}

func mountOf(r io.Reader, dir string) (fstype, options string) {
	best := ""
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		pre, post, ok := strings.Cut(sc.Text(), " - ")
		f, g := strings.Fields(pre), strings.Fields(post)
		if !ok || len(f) < 6 || len(g) < 1 {
			continue
		}
		at := f[4]
		inside := dir == at || strings.HasPrefix(dir, strings.TrimSuffix(at, "/")+"/")
		if inside && len(at) >= len(best) {
			best, fstype, options = at, g[0], f[5]
		}
	}
	return fstype, options
}

type Heat struct {
	Preference  string        `json:"energy_performance_preference"`
	Profile     string        `json:"platform_profile"`
	MaxPerf     string        `json:"max_perf_pct"`
	PackageTemp float64       `json:"x86_pkg_temp"`
	Packages    []PackageHeat `json:"packages"`
}

type PackageHeat struct {
	ID          string `json:"id"`
	Throttles   uint64 `json:"package_throttle_count"`
	ThrottledMs uint64 `json:"package_throttle_total_time_ms"`
}

func ReadHeat(fsys fs.FS) Heat {
	read := func(name string) string {
		b, err := fs.ReadFile(fsys, name)
		if err != nil {
			return ""
		}
		return strings.TrimSpace(string(b))
	}
	const cpus = "sys/devices/system/cpu/"
	h := Heat{Preference: read(cpus + "cpu0/cpufreq/energy_performance_preference"), Profile: read("sys/firmware/acpi/platform_profile"),
		MaxPerf: read(cpus + "intel_pstate/max_perf_pct")}
	zones, _ := fs.Glob(fsys, "sys/class/thermal/thermal_zone*")
	for _, zone := range zones {
		if read(zone+"/type") != "x86_pkg_temp" {
			continue
		}
		if milli, err := strconv.ParseFloat(read(zone+"/temp"), 64); err == nil {
			h.PackageTemp = milli / 1000
		}
	}
	ids, _ := fs.Glob(fsys, cpus+"cpu[0-9]*/topology/physical_package_id")
	for _, name := range ids {
		id := read(name)
		if slices.ContainsFunc(h.Packages, func(p PackageHeat) bool { return p.ID == id }) {
			continue
		}
		dir := path.Dir(path.Dir(name)) + "/thermal_throttle/"
		count, errCount := strconv.ParseUint(read(dir+"package_throttle_count"), 10, 64)
		ms, errMs := strconv.ParseUint(read(dir+"package_throttle_total_time_ms"), 10, 64)
		if errCount == nil && errMs == nil {
			h.Packages = append(h.Packages, PackageHeat{ID: id, Throttles: count, ThrottledMs: ms})
		}
	}
	return h
}
