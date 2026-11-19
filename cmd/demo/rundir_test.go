package main

import (
	"bytes"
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestForeignRunDirSurvives(t *testing.T) {
	quit := "/usr/bin/true"
	if _, err := os.Stat(quit); err != nil {
		t.Skip(err)
	}
	dir := t.TempDir()
	kept := filepath.Join(dir, "run", "user", "1000", "bus")
	if err := os.MkdirAll(filepath.Dir(kept), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(kept, []byte("not from a demo\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
	defer cancel()
	var stderr bytes.Buffer
	code := run(ctx, []string{"DEMO_DIR=" + dir, "DEMO_ADDR=" + addr, "DEMO_LEDGERD=" + quit}, &stderr)
	if _, err := os.Stat(kept); err != nil {
		t.Fatalf("exit %d, and %s is gone: %v\n%s", code, kept, err, stderr.String())
	}
	if code != 3 {
		t.Fatalf("exit %d, want 3 for a DEMO_DIR whose run directory holds foreign files\n%s", code, stderr.String())
	}
}

func TestOwnRunDirIsTaken(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "ledgerd")
	if err := os.WriteFile(bin, []byte("#!/bin/true\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	for _, name := range []string{"n1/wal", "n2", "n3"} {
		if err := os.MkdirAll(filepath.Join(dir, "run", name), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"n1.key", "n2.key", "n3.key", "n1/wal/wal-00000000000000000001.log"} {
		if err := os.WriteFile(filepath.Join(dir, "run", name), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := (config{dir: dir, ledgerd: bin}).prepare(); err != nil {
		t.Fatalf("a run directory a demo left behind: %v", err)
	}
}
