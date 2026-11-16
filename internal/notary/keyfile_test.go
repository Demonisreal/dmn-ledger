package notary_test

import (
	"encoding/hex"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/notary"
)

func TestKeyFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.key")
	if err := notary.WriteKey(path, key); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode().Perm() != 0o600 {
		t.Errorf("key file has mode %04o", st.Mode().Perm())
	}
	text, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if want := hex.EncodeToString(key.Seed()) + "\n"; string(text) != want {
		t.Errorf("key file holds %q, want %q", text, want)
	}
	read, err := notary.ReadKey(path)
	if err != nil {
		t.Fatal(err)
	}
	if !read.Equal(key) {
		t.Error("read back another key")
	}

	if err := notary.WriteKey(path, otherKey); !errors.Is(err, fs.ErrExist) {
		t.Errorf("second WriteKey: %v", err)
	}
	if read, err := notary.ReadKey(path); err != nil || !read.Equal(key) {
		t.Errorf("key file overwritten: %v", err)
	}
}

func TestReadKeyRefuses(t *testing.T) {
	dir := t.TempDir()
	seed := hex.EncodeToString(key.Seed())
	cases := []struct {
		name string
		text string
		mode os.FileMode
	}{
		{"readable by the group", seed + "\n", 0o640},
		{"readable by others", seed + "\n", 0o604},
		{"writable by the group", seed + "\n", 0o620},
		{"empty", "", 0o600},
		{"no newline", seed, 0o600},
		{"two newlines", seed + "\n\n", 0o600},
		{"windows line end", seed + "\r\n", 0o600},
		{"one digit short", seed[1:] + "\n", 0o600},
		{"one digit more", seed + "0\n", 0o600},
		{"not hex", strings.Replace(seed, seed[:1], "g", 1) + "\n", 0o600},
		{"space before", " " + seed[1:] + "\n", 0o600},
		{"private key instead of seed", hex.EncodeToString(key) + "\n", 0o600},
	}
	for i, c := range cases {
		path := filepath.Join(dir, string(rune('a'+i)))
		if err := os.WriteFile(path, []byte(c.text), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, c.mode); err != nil {
			t.Fatal(err)
		}
		if _, err := notary.ReadKey(path); !errors.Is(err, notary.ErrKeyFile) {
			t.Errorf("%s: %v", c.name, err)
		}
	}
	if _, err := notary.ReadKey(dir); !errors.Is(err, notary.ErrKeyFile) {
		t.Errorf("directory: %v", err)
	}
	if _, err := notary.ReadKey(filepath.Join(dir, "missing")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("missing file: %v", err)
	}
}
