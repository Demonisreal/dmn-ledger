package ledgerrun

import (
	"errors"
	"testing"
)

func TestStatsParse(t *testing.T) {
	const real = "2026/11/16 09:40:42.488371 stats up=1611274457 rounds=255 wait=1532788677 process=69113556 slowest=8232027 logsync=46 logsync_ns=40083211 metasync=50 metasync_ns=27433883 othersync=4 othersync_ns=1334566 written=11225"
	fields, err := parseStats(real)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]uint64{
		"up": 1611274457, "rounds": 255, "wait": 1532788677, "process": 69113556, "slowest": 8232027,
		"logsync": 46, "logsync_ns": 40083211, "metasync": 50, "metasync_ns": 27433883,
		"othersync": 4, "othersync_ns": 1334566, "written": 11225,
	}
	if len(fields) != len(want) {
		t.Fatalf("got %d fields %v, want %v", len(fields), fields, want)
	}
	for name, n := range want {
		if fields[name] != n {
			t.Errorf("%s = %d, want %d", name, fields[name], n)
		}
	}

	fields, err = parseStats(real + " readsync=7")
	if err != nil || fields["readsync"] != 7 || fields["written"] != 11225 {
		t.Fatalf("with an unknown field at the end: %v, %v", fields, err)
	}

	for _, line := range []string{
		"2026/11/16 09:40:41.600118 leader in term 1, leader 1",
		"2026/11/16 09:40:42.488371 stats up=1 rounds=2",
		real + " logsync=x",
		real + " stray",
		"2026/11/16 09:40:42.488371 stats up=-1 rounds=255 wait=1 process=1 slowest=1 logsync=1 logsync_ns=1 metasync=1 metasync_ns=1 othersync=1 othersync_ns=1 written=1",
	} {
		if _, err := parseStats(line); !errors.Is(err, ErrStats) {
			t.Errorf("%q: got %v, want ErrStats", line, err)
		}
	}
}

func TestStatsLogKeepsTheLastLine(t *testing.T) {
	s := &statsLog{file: devNull(t), fresh: make(chan struct{})}
	fresh := s.next()
	for _, part := range []string{"2026/11/16 09:40:41 leader in term 1\n2026/11/16 09:40:42 sta", "ts up=1 written=2\n2026/11/16 09:40:43 st", "ats up=3 written=4\nhalf"} {
		if _, err := s.Write([]byte(part)); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-fresh:
	default:
		t.Fatal("no word of a new stats line")
	}
	if got, want := s.line(), "2026/11/16 09:40:43 stats up=3 written=4"; got != want {
		t.Fatalf("last line %q, want %q", got, want)
	}
	select {
	case <-s.next():
		t.Fatal("word of a stats line that has not come yet")
	default:
	}
}
