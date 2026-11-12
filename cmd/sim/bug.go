//go:build !simbug

package main

import "errors"

func setBug(name string) error {
	if name != "" {
		return errors.New("-bug needs a build with -tags simbug")
	}
	return nil
}
