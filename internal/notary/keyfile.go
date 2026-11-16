package notary

import (
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

var ErrKeyFile = errors.New("notary: bad key file")

const keyFileSize = 2*ed25519.SeedSize + 1

func ReadKey(path string) (ed25519.PrivateKey, error) {
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !st.Mode().IsRegular() {
		return nil, fmt.Errorf("%w: %s is not a regular file", ErrKeyFile, path)
	}
	if perm := st.Mode().Perm(); perm&0o077 != 0 {
		return nil, fmt.Errorf("%w: %s is open to group or others (mode %04o)", ErrKeyFile, path, perm)
	}
	text, err := io.ReadAll(io.LimitReader(f, keyFileSize+1))
	if err != nil {
		return nil, err
	}
	seed, err := hex.DecodeString(strings.TrimSuffix(string(text), "\n"))
	if err != nil || len(text) != keyFileSize {
		return nil, fmt.Errorf("%w: %s does not hold 64 hex digits and a newline", ErrKeyFile, path)
	}
	return ed25519.NewKeyFromSeed(seed), nil
}

func WriteKey(path string, key ed25519.PrivateKey) error {
	f, err := os.OpenFile(filepath.Clean(path), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	text := hex.AppendEncode(nil, key.Seed())
	if _, err := f.Write(append(text, '\n')); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
