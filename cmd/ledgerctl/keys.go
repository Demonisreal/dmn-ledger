package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"

	"github.com/Demonisreal/dmn-ledger/internal/notary"
)

func (c *ctl) newKey(args []string) int {
	flags := c.flags("key new")
	files, code, ok := c.positional(flags, args)
	if !ok {
		return code
	}
	if len(files) != 1 {
		return c.misuse(flags, "want one file")
	}
	seed := make([]byte, ed25519.SeedSize)
	if _, err := rand.Read(seed); err != nil {
		fmt.Fprintln(c.stderr, "ledgerctl: key new:", err)
		return 1
	}
	key := ed25519.NewKeyFromSeed(seed)
	if err := notary.WriteKey(files[0], key); err != nil {
		fmt.Fprintln(c.stderr, "ledgerctl: key new:", err)
		if errors.Is(err, fs.ErrExist) {
			return 2
		}
		return 1
	}
	fmt.Fprintln(c.stdout, hex.EncodeToString(key.Public().(ed25519.PublicKey)))
	return 0
}

func (c *ctl) showKey(args []string) int {
	flags := c.flags("key show")
	files, code, ok := c.positional(flags, args)
	if !ok {
		return code
	}
	if len(files) != 1 {
		return c.misuse(flags, "want one file")
	}
	key, err := notary.ReadKey(files[0])
	if err != nil {
		fmt.Fprintln(c.stderr, "ledgerctl: key show:", err)
		return 1
	}
	fmt.Fprintln(c.stdout, hex.EncodeToString(key.Public().(ed25519.PublicKey)))
	return 0
}

func readKeys(path string) ([][]byte, error) {
	text, err := os.ReadFile(path) //nolint:gosec // G304: the operator names the file
	if err != nil {
		return nil, err
	}
	var keys [][]byte
	for n, line := range strings.Split(string(text), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		key, err := hex.DecodeString(line[:min(len(line), 2*ed25519.PublicKeySize)])
		if err != nil || len(key) != ed25519.PublicKeySize || len(line) > 2*ed25519.PublicKeySize && line[2*ed25519.PublicKeySize] != ' ' && line[2*ed25519.PublicKeySize] != '\t' {
			return nil, fmt.Errorf("%s, line %d: want 64 hex digits, then a space and any text", path, n+1)
		}
		keys = append(keys, key)
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("%s holds no keys", path)
	}
	return keys, nil
}
