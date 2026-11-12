//go:build simbug

package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/simbug"
)

func TestReproduceCommand(t *testing.T) {
	t.Cleanup(func() { _ = simbug.Set("") })
	var out, errs bytes.Buffer
	if code := run([]string{"-bug", "B12", "-seeds", "30", "-from", "5"}, &out, &errs); code != 1 {
		t.Fatalf("exit %d: %s %s", code, out.String(), errs.String())
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	const want = "reproduce: go run -tags simbug ./cmd/sim -bug B12 -seed 5 -profile all -trace"
	if len(lines) != 2 || lines[1] != want {
		t.Fatalf("got %q, want the finding and %q", lines, want)
	}
	var again bytes.Buffer
	if code := run([]string{"-bug", "B12", "-seed", "5", "-profile", "all"}, &again, &errs); code != 1 {
		t.Fatalf("exit %d", code)
	}
	if first := strings.SplitN(again.String(), "\n", 2)[0]; first != lines[0] {
		t.Fatalf("replay says %q, the sweep said %q", first, lines[0])
	}
}
