package control

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

const (
	probeRecord = 137
	probeSpan   = 4 << 20
)

type ProbeSecond struct {
	Syncs     uint64        `json:"syncs"`
	SyncTime  time.Duration `json:"sync_ns"`
	Flushes   uint64        `json:"flushes"`
	FlushTime time.Duration `json:"flush_ns"`
	Speed     Speed         `json:"speed"`
}

type ProbeMean struct {
	Speed           Speed         `json:"speed"`
	GrowingSeconds  int           `json:"growing_seconds"`
	Growing         time.Duration `json:"growing_ns"`
	PreallocSeconds int           `json:"prealloc_seconds"`
	Prealloc        time.Duration `json:"prealloc_ns"`
	Ratio           float64       `json:"ratio"`
}

type Probe struct {
	Growing  []ProbeSecond `json:"growing"`
	Prealloc []ProbeSecond `json:"prealloc"`
	Means    []ProbeMean   `json:"means"`
}

func RunProbe(ctx context.Context, dir, disk string, each time.Duration, th Thresholds) (Probe, error) {
	var p Probe
	grow, err := os.CreateTemp(dir, "probe-growing-")
	if err != nil {
		return p, err
	}
	defer func() { _ = os.Remove(grow.Name()) }()
	defer func() { _ = grow.Close() }()
	pre, err := os.CreateTemp(dir, "probe-prealloc-")
	if err != nil {
		return p, err
	}
	defer func() { _ = os.Remove(pre.Name()) }()
	defer func() { _ = pre.Close() }()
	if _, err := pre.Write(make([]byte, probeSpan)); err != nil {
		return p, err
	}
	if err := pre.Sync(); err != nil {
		return p, err
	}
	if err := syncDir(dir); err != nil {
		return p, err
	}

	rec := make([]byte, probeRecord)
	for i := range rec {
		rec[i] = byte('a' + i%26)
	}
	if p.Growing, err = probe(ctx, disk, each, th, func() error {
		if _, err := grow.Write(rec); err != nil {
			return err
		}
		return grow.Sync()
	}); err != nil {
		return p, err
	}
	var off int64
	if p.Prealloc, err = probe(ctx, disk, each, th, func() error {
		if _, err := pre.WriteAt(rec, off); err != nil {
			return err
		}
		if off += probeRecord; off+probeRecord > probeSpan {
			off = 0
		}
		return syscall.Fdatasync(int(pre.Fd())) //nolint:gosec // G115: a file descriptor
	}); err != nil {
		return p, err
	}
	p.Means = means(p)
	return p, nil
}

func syncDir(dir string) error {
	d, err := os.Open(filepath.Clean(dir))
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	return d.Sync()
}

func probe(ctx context.Context, disk string, each time.Duration, th Thresholds, write func() error) ([]ProbeSecond, error) {
	var secs []ProbeSecond
	prev, err := readDisk(disk)
	if err != nil {
		return nil, err
	}
	start := time.Now()
	var cur ProbeSecond
	next := start.Add(time.Second)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		t0 := time.Now()
		if !t0.Before(next) {
			now, err := readDisk(disk)
			if err != nil {
				return nil, err
			}
			d := now.since(prev)
			prev = now
			cur.Flushes, cur.FlushTime = d.Flushes, d.FlushTime
			cur.Speed = th.Speed(d.Flushes, d.FlushTime)
			secs = append(secs, cur)
			cur = ProbeSecond{}
			if t0.Sub(start) >= each {
				return secs, nil
			}
			next = next.Add(time.Second)
		}
		if err := write(); err != nil {
			return nil, fmt.Errorf("control: probe: %w", err)
		}
		cur.Syncs++
		cur.SyncTime += time.Since(t0)
	}
}

func readDisk(disk string) (Disk, error) {
	f, err := os.Open("/proc/diskstats")
	if err != nil {
		return Disk{}, err
	}
	defer func() { _ = f.Close() }()
	return ParseDiskstats(f, disk)
}

func means(p Probe) []ProbeMean {
	var out []ProbeMean
	for _, speed := range []Speed{Slow, Fast} {
		m := ProbeMean{Speed: speed}
		var gn, pn uint64
		var gt, pt time.Duration
		for _, s := range p.Growing {
			if s.Speed == speed && s.Syncs > 0 {
				m.GrowingSeconds++
				gn, gt = gn+s.Syncs, gt+s.SyncTime
			}
		}
		for _, s := range p.Prealloc {
			if s.Speed == speed && s.Syncs > 0 {
				m.PreallocSeconds++
				pn, pt = pn+s.Syncs, pt+s.SyncTime
			}
		}
		if gn > 0 {
			m.Growing = gt / time.Duration(gn) //nolint:gosec // G115: syncs in a few seconds
		}
		if pn > 0 {
			m.Prealloc = pt / time.Duration(pn) //nolint:gosec // G115: syncs in a few seconds
		}
		if m.Growing > 0 && m.Prealloc > 0 {
			m.Ratio = float64(m.Growing) / float64(m.Prealloc)
		}
		out = append(out, m)
	}
	return out
}
