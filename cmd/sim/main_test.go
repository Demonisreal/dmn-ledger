package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/check"
	"github.com/Demonisreal/dmn-ledger/internal/sim"
)

func TestUsage(t *testing.T) {
	for _, args := range [][]string{
		{"-profile", "storm"},
		{"-nodes", "4"},
		{"-seeds", "0"},
		{"-seeds", "5", "-trace"},
		{"-seeds", "5", "-seed", "3"},
		{"-from", "7"},
		{"extra"},
		{"-bug", "B99"},
		{"-seeds", "3", "-from", "18446744073709551614"},
	} {
		var out, errs bytes.Buffer
		if code := run(args, &out, &errs); code != 2 || errs.Len() == 0 {
			t.Errorf("%v: exit %d, stderr %q", args, code, errs.String())
		}
	}
}

func TestRuns(t *testing.T) {
	for _, args := range [][]string{
		{"-seed", "7", "-profile", "crash"},
		{"-seeds", "20", "-from", "100", "-profile", "evict"},
		{"-seeds", "5", "-nodes", "5"},
		{"-seeds", "1", "-from", "18446744073709551615", "-profile", "calm"},
	} {
		var out, errs bytes.Buffer
		if code := run(args, &out, &errs); code != 0 || !strings.Contains(out.String(), ": ok") {
			t.Errorf("%v: exit %d, stdout %q, stderr %q", args, code, out.String(), errs.String())
		}
	}
}

func TestTraceGoesToStdout(t *testing.T) {
	var out, errs bytes.Buffer
	if code := run([]string{"-seed", "3", "-trace", "-profile", "calm"}, &out, &errs); code != 0 {
		t.Fatalf("exit %d: %s", code, errs.String())
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) < 100 || !strings.Contains(lines[0], " plan ") || !strings.HasPrefix(lines[len(lines)-1], "seed 3 profile calm: ok") {
		t.Fatalf("%d lines, first %q, last %q", len(lines), lines[0], lines[len(lines)-1])
	}
}

func TestHelpExitsZero(t *testing.T) {
	var out, errs bytes.Buffer
	if code := run([]string{"-h"}, &out, &errs); code != 0 || !strings.Contains(errs.String(), "-profile") {
		t.Fatalf("exit %d, stderr %q", code, errs.String())
	}
	for _, p := range sim.Profiles() {
		if !strings.Contains(errs.String(), " "+string(p)) {
			t.Errorf("help does not name profile %s", p)
		}
	}
}

func TestReportTellsFindingsFromRunErrors(t *testing.T) {
	cases := []struct {
		name string
		o    options
		err  error
		code int
		want string
	}{
		{"violation", options{profile: "crash", nodes: 5}, &check.Violation{Seed: 9, Check: "S3"}, 1,
			"reproduce: go run ./cmd/sim -seed 9 -profile crash -nodes 5 -trace"},
		{"planted bug", options{profile: "all", bug: "B3"}, &check.Violation{Seed: 9, Check: "S5"}, 1,
			"reproduce: go run -tags simbug ./cmd/sim -bug B3 -seed 9 -profile all -trace"},
		{"panic", options{profile: "net"}, &check.Panic{Seed: 9, Value: "boom"}, 1,
			"reproduce: go run ./cmd/sim -seed 9 -profile net -trace"},
		{"run error", options{profile: "net"}, errors.New("trace: disk full"), 3,
			"seed 9 profile net: run error: trace: disk full"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			code := tc.o.report(&out, 9, tc.err)
			lines := strings.Split(strings.TrimSpace(out.String()), "\n")
			if code != tc.code || lines[len(lines)-1] != tc.want {
				t.Fatalf("exit %d, output %q, want exit %d and %q", code, lines, tc.code, tc.want)
			}
			if tc.code == 3 && strings.Contains(out.String(), "reproduce") {
				t.Fatalf("a run error got a reproduce line: %q", out.String())
			}
		})
	}
}
