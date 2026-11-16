//go:build simbug

package main

import (
	"strings"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/simbug"
)

func TestBugIsPlanted(t *testing.T) {
	t.Cleanup(func() { _ = simbug.Set("") })
	vars := required()
	vars["LEDGER_BUG"] = "B11"
	if _, err := parse(environ(vars)); err != nil || !simbug.On("B11") {
		t.Fatalf("LEDGER_BUG=B11: %v, planted %v", err, simbug.On("B11"))
	}
	vars["LEDGER_BUG"] = "B99"
	if _, err := parse(environ(vars)); err == nil || !strings.Contains(err.Error(), `LEDGER_BUG="B99"`) {
		t.Fatalf("LEDGER_BUG=B99: %v", err)
	}
}
