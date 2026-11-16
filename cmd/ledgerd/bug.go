//go:build !simbug

package main

func takeBug(map[string]string) error {
	return nil
}
