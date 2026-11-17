//go:build pg

package main

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/Demonisreal/dmn-ledger/bench/pg"
)

func TestFinishStopsTheContainer(t *testing.T) {
	dir := t.TempDir()
	srv, err := pg.Start(t.Context(), filepath.Join(dir, "pg"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if running, _ := containerExists(context.Background()); running {
			_ = srv.Stop()
		}
	})
	b := &bench{root: os.DirFS("/"), dir: dir, out: t.TempDir(), srv: srv, log: io.Discard}
	if err := b.finish(context.Background(), errors.New("a run failed")); err != nil {
		t.Fatal(err)
	}
	if running, err := containerExists(t.Context()); err != nil || running {
		t.Fatalf("%s after a failed plan: running %v, %v", pg.Name, running, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "pg")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("data of the container after a failed plan: %v", err)
	}
}
