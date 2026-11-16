//go:build pprof

package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
)

const profileRate = 250

func takeProfile(vars map[string]string) (string, error) {
	path, ok := vars["LEDGER_PROFILE"]
	if !ok {
		return "", nil
	}
	delete(vars, "LEDGER_PROFILE")
	if !filepath.IsAbs(path) {
		return "", fmt.Errorf("LEDGER_PROFILE=%q: want an absolute path", path)
	}
	return path, nil
}

func startProfile(path string) (func() error, error) {
	if path == "" {
		return func() error { return nil }, nil
	}
	f, err := os.OpenFile(filepath.Clean(path), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, err
	}
	runtime.SetCPUProfileRate(profileRate)
	if err := pprof.StartCPUProfile(f); err != nil {
		return nil, errors.Join(err, f.Close())
	}
	return func() error {
		pprof.StopCPUProfile()
		return f.Close()
	}, nil
}
