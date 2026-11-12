//go:build simbug

package simbug

import (
	"fmt"
	"slices"
)

var Names = []string{"B1", "B2", "B3", "B4", "B5", "B6", "B7", "B8", "B9", "B10", "B11", "B12"}

var active string

func Set(name string) error {
	if name != "" && !slices.Contains(Names, name) {
		return fmt.Errorf("simbug: unknown bug %q, want one of %v", name, Names)
	}
	active = name
	return nil
}

func On(name string) bool {
	return active == name
}
