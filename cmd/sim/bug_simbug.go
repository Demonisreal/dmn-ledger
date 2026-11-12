//go:build simbug

package main

import "github.com/Demonisreal/dmn-ledger/internal/simbug"

func setBug(name string) error {
	return simbug.Set(name)
}
