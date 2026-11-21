package main

import (
	"bytes"
	"errors"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/disk"
)

func tree(t *testing.T, dir string) map[string]string {
	t.Helper()
	files := make(map[string]string)
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		switch {
		case err != nil:
			return err
		case d.IsDir():
			files[path] = "directory"
		default:
			files[path] = string(read(t, path))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}

func initialise(line string) (int, string, string) {
	var out, errs bytes.Buffer
	code := run(args(line), &out, &errs)
	return code, out.String(), errs.String()
}

func mkdir(t *testing.T, dir string) string {
	t.Helper()
	if err := os.Mkdir(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestInitRefusals(t *testing.T) {
	root := t.TempDir()
	at := func(name string) string { return filepath.Join(root, name) }

	never := at("never")
	for _, tc := range []struct {
		line, cluster, errs string
	}{
		{"init -nodes 3 " + never, "7", "ledgerctl init: -id: want a number from 1 to 3\n"},
		{"init -id 4 -nodes 3 " + never, "7", "ledgerctl init: -id: want a number from 1 to 3\n"},
		{"init -id 0 -nodes 5 " + never, "7", "ledgerctl init: -id: want a number from 1 to 5\n"},
		{"init -id 1 " + never, "7", "ledgerctl init: -nodes: want 1, 3 or 5\n"},
		{"init -id 1 -nodes 2 " + never, "7", "ledgerctl init: -nodes: want 1, 3 or 5\n"},
		{"init -id 1 -nodes 7 " + never, "7", "ledgerctl init: -nodes: want 1, 3 or 5\n"},
		{"init -id 1 -nodes 1", "7", "ledgerctl init: want one directory\n"},
		{"init -id 1 -nodes 1 " + never + " " + at("other"), "7", "ledgerctl init: want one directory\n"},
		{"init " + never + " -id 1 -nodes 1", "", "ledgerctl: LEDGER_CLUSTER is missing\n"},
		{"init -id 1 -nodes 1 " + never, "0", "ledgerctl: LEDGER_CLUSTER=\"0\": want a number from 1 to 2^64-1, decimal or 0x\n"},
	} {
		t.Setenv("LEDGER_CLUSTER", tc.cluster)
		if tc.cluster == "" {
			unset(t, "LEDGER_CLUSTER")
		}
		code, out, errs := initialise(tc.line)
		if code != 2 || out != "" || errs != tc.errs {
			t.Errorf("%s: exit %d, stdout %q, stderr %q, want exit 2 and %q", tc.line, code, out, errs, tc.errs)
		}
		if _, err := os.Stat(never); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("%s: %s exists after a refusal: %v", tc.line, never, err)
		}
	}
	t.Setenv("LEDGER_CLUSTER", "7")

	left := mkdir(t, at("left"))
	write(t, filepath.Join(left, disk.LockName), nil)
	write(t, filepath.Join(left, "meta.tmp"), []byte("half of an earlier init"))
	code, out, errs := initialise("init " + left + " -id 2 -nodes 3")
	want := "initialised " + left + " as node 2 of 3 in cluster 7\n" +
		"start it only if node 2 has never run in cluster 7: a node that ran has voted and acknowledged entries, and this directory knows none of that\n"
	if code != 0 || out != want || errs != "" {
		t.Fatalf("init over LOCK and meta.tmp: exit %d, stdout %q, stderr %q, want exit 0 and %q", code, out, errs, want)
	}
	made := []string{left, filepath.Join(left, disk.LockName), filepath.Join(left, "meta")}
	if names := slices.Sorted(maps.Keys(tree(t, left))); !slices.Equal(names, made) {
		t.Fatalf("init over LOCK and meta.tmp left %v, want %v", names, made)
	}

	withWal := mkdir(t, at("with-wal"))
	mkdir(t, filepath.Join(withWal, "wal"))
	withSnap := mkdir(t, at("with-snap"))
	mkdir(t, filepath.Join(withSnap, "snap"))
	write(t, filepath.Join(withSnap, disk.LockName), nil)
	foreign := mkdir(t, at("foreign"))
	write(t, filepath.Join(foreign, "notes"), []byte("not a node"))
	locked := mkdir(t, at("locked"))
	lock, err := disk.OS{}.Lock(locked)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	plain := write(t, at("plain"), []byte("a file, not a directory"))

	const used = "ledgerctl: init: init takes only a directory that no node has used: replica: the directory is not empty: "
	initialised := "ledgerctl: init: " + left + " is already initialised, nothing was changed\n"
	for _, tc := range []struct {
		dir, flags, cluster string
		code                int
		errs                string
	}{
		{left, "-id 2 -nodes 3", "7", 2, initialised},
		{left, "-id 3 -nodes 5", "7", 2, initialised},
		{left, "-id 2 -nodes 3", "8", 2, initialised},
		{withWal, "-id 1 -nodes 1", "7", 2, used + withWal + " holds wal\n"},
		{withSnap, "-id 1 -nodes 1", "7", 2, used + withSnap + " holds snap\n"},
		{foreign, "-id 1 -nodes 1", "7", 2, used + foreign + " holds notes\n"},
		{locked, "-id 1 -nodes 1", "7", 2, "ledgerctl: init: " + locked + " is locked by a running node\n"},
		{plain, "-id 1 -nodes 1", "7", 1, "ledgerctl: init: open " + plain + ": not a directory\n"},
	} {
		t.Setenv("LEDGER_CLUSTER", tc.cluster)
		before := tree(t, tc.dir)
		code, out, errs := initialise("init " + tc.flags + " " + tc.dir)
		if code != tc.code || out != "" || errs != tc.errs {
			t.Errorf("init %s %s: exit %d, stdout %q, stderr %q, want exit %d and %q", tc.flags, tc.dir, code, out, errs, tc.code, tc.errs)
		}
		if after := tree(t, tc.dir); !maps.Equal(after, before) {
			t.Errorf("init %s %s changed the directory: %v, was %v", tc.flags, tc.dir, after, before)
		}
	}
}
