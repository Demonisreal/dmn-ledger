//go:build !simbug

package main

import "testing"

func TestBugNeedsTheTag(t *testing.T) {
	vars := required()
	vars["LEDGER_BUG"] = "B11"
	if _, err := parse(environ(vars)); err == nil || err.Error() != "unknown variable LEDGER_BUG" {
		t.Fatalf("LEDGER_BUG without -tags simbug: %v", err)
	}
}
