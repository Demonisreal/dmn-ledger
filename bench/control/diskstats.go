package control

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

var (
	ErrNoDisk    = errors.New("control: disk not in /proc/diskstats")
	ErrDiskstats = errors.New("control: diskstats without flush counters")
)

type Disk struct {
	Writes    uint64
	WriteTime time.Duration
	Flushes   uint64
	FlushTime time.Duration
}

func ParseDiskstats(r io.Reader, name string) (Disk, error) {
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		f := strings.Fields(sc.Text())
		if len(f) < 3 || f[2] != name {
			continue
		}
		if len(f) < 20 {
			return Disk{}, fmt.Errorf("%w: %d fields for %s", ErrDiskstats, len(f), name)
		}
		var n [4]uint64
		for i, col := range []int{7, 10, 18, 19} {
			v, err := strconv.ParseUint(f[col], 10, 64)
			if err != nil {
				return Disk{}, fmt.Errorf("%w: %s column %d: %w", ErrDiskstats, name, col, err)
			}
			n[i] = v
		}
		return Disk{
			Writes:    n[0],
			WriteTime: time.Duration(n[1]) * time.Millisecond, //nolint:gosec // G115: milliseconds since boot
			Flushes:   n[2],
			FlushTime: time.Duration(n[3]) * time.Millisecond, //nolint:gosec // G115: milliseconds since boot
		}, nil
	}
	if err := sc.Err(); err != nil {
		return Disk{}, err
	}
	return Disk{}, fmt.Errorf("%w: %s", ErrNoDisk, name)
}

func (d Disk) since(prev Disk) Disk {
	return Disk{
		Writes:    d.Writes - prev.Writes,
		WriteTime: d.WriteTime - prev.WriteTime,
		Flushes:   d.Flushes - prev.Flushes,
		FlushTime: d.FlushTime - prev.FlushTime,
	}
}

func DiskOf(dir string) (string, error) {
	var st syscall.Stat_t
	if err := syscall.Stat(dir, &st); err != nil {
		return "", fmt.Errorf("control: disk of %s: %w", dir, err)
	}
	dev := st.Dev
	major := (dev>>8)&0xfff | (dev>>32)&^0xfff
	minor := dev&0xff | (dev>>12)&^0xff
	block, err := filepath.EvalSymlinks(fmt.Sprintf("/sys/dev/block/%d:%d", major, minor))
	if err != nil {
		return "", fmt.Errorf("control: disk of %s: %w", dir, err)
	}
	if _, err := os.Stat(filepath.Join(block, "partition")); err == nil {
		block = filepath.Dir(block)
	}
	return filepath.Base(block), nil
}
