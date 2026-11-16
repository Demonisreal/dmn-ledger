//go:build simbug

package main

import (
	"fmt"

	"github.com/Demonisreal/dmn-ledger/internal/simbug"
)

func takeBug(vars map[string]string) error {
	name, ok := vars["LEDGER_BUG"]
	if !ok {
		return nil
	}
	delete(vars, "LEDGER_BUG")
	if err := simbug.Set(name); err != nil {
		return fmt.Errorf("LEDGER_BUG=%q: %w", name, err)
	}
	return nil
}
