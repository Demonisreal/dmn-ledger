package main

import (
	"bytes"
	"strings"
	"testing"
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
