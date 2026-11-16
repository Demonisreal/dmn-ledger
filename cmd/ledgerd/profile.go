//go:build !pprof

package main

func takeProfile(map[string]string) (string, error) {
	return "", nil
}

func startProfile(string) (func() error, error) {
	return func() error { return nil }, nil
}
