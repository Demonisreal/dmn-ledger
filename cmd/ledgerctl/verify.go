package main

import (
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/audit"
	"github.com/Demonisreal/dmn-ledger/internal/disk"
	"github.com/Demonisreal/dmn-ledger/internal/replica"
	"github.com/Demonisreal/dmn-ledger/internal/wal"
	"github.com/Demonisreal/dmn-ledger/ledger"
)

const copyRoot = "/copy"

func rank(code int) int {
	return [...]int{0, 2, 3, 1}[code]
}

func (c *ctl) trust(flags *flag.FlagSet, path string) ([][]byte, int, bool) {
	if path == "" {
		return nil, c.misuse(flags, "-keys is missing"), false
	}
	if err := c.clusterEnv(); err != nil {
		fmt.Fprintln(c.stderr, "ledgerctl:", err)
		return nil, 2, false
	}
	keys, err := readKeys(path)
	if err != nil {
		return nil, c.misuse(flags, "%v", err), false
	}
	return keys, 0, true
}

func describe(path string, p ledger.Proof) string {
	var b strings.Builder
	fmt.Fprintf(&b, "file=%s kind=%s", path, p.Kind)
	switch p.Kind {
	case ledger.ProofAccount:
		fmt.Fprintf(&b, " id=%s timestamp=%s", p.Account.ID, stamp(p.Account.Timestamp))
	case ledger.ProofTransfer, ledger.ProofExpiry:
		fmt.Fprintf(&b, " id=%s timestamp=%s", p.Transfer.ID, stamp(p.Transfer.Timestamp))
	}
	if p.Kind != ledger.ProofHead {
		fmt.Fprintf(&b, " time=%s", stamp(p.Time))
	}
	fmt.Fprintf(&b, " seq=%d head=%s key=", p.HeadSeq, hex.EncodeToString(p.Head[:]))
	if p.Key == nil {
		b.WriteString("none")
	} else {
		b.WriteString(hex.EncodeToString(p.Key))
	}
	return b.String()
}

func (c *ctl) check(path string, keys [][]byte, unsigned bool) (ledger.Proof, int) {
	proof, err := readProof(path)
	if err != nil {
		fmt.Fprintln(c.stderr, "ledgerctl:", err)
		return ledger.Proof{}, 2
	}
	p, err := ledger.VerifyProof(proof, c.cluster, keys)
	switch {
	case errors.Is(err, ledger.ErrUnsigned) && unsigned:
		return p, 0
	case errors.Is(err, ledger.ErrUnsigned):
		fmt.Fprintf(c.stdout, "file=%s no verdict: %v; with -unsigned only the path from the record to head %d is checked, not who vouches for it\n", path, err, p.HeadSeq)
		return p, 3
	case err != nil:
		fmt.Fprintf(c.stdout, "file=%s invalid: %v\n", path, err)
		return p, 1
	}
	return p, 0
}

func (c *ctl) verifyProofs(args []string) int {
	flags := c.flags("verify proof")
	keysPath := flags.String("keys", "", "file with the trusted public keys, one per line")
	unsigned := flags.Bool("unsigned", false, "accept a proof without a stamp and check only the path from its record to its head")
	files, code, ok := c.positional(flags, args)
	if !ok {
		return code
	}
	if len(files) == 0 {
		return c.misuse(flags, "want at least one file")
	}
	keys, code, ok := c.trust(flags, *keysPath)
	if !ok {
		return code
	}
	worst := 0
	for _, path := range files {
		p, code := c.check(path, keys, *unsigned)
		if code == 0 {
			fmt.Fprintln(c.stdout, describe(path, p))
		}
		if rank(code) > rank(worst) {
			worst = code
		}
	}
	return worst
}

func (c *ctl) verifyChain(args []string) int {
	flags := c.flags("verify chain")
	keysPath := flags.String("keys", "", "file with the trusted public keys, one per line")
	linksPath := flags.String("links", "", "file with the links between the two heads; without it they come from the cluster")
	files, code, ok := c.positional(flags, args)
	if !ok {
		return code
	}
	if len(files) != 2 {
		return c.misuse(flags, "want the older and the newer proof")
	}
	keys, code, ok := c.trust(flags, *keysPath)
	if !ok {
		return code
	}
	var proofs [2]ledger.Proof
	for i, path := range files {
		if proofs[i], code = c.check(path, keys, false); code != 0 {
			return code
		}
	}
	older, newer := proofs[0], proofs[1]
	if older.HeadSeq > newer.HeadSeq {
		return c.misuse(flags, "%s is at link %d, after %s at link %d; give the older one first", files[0], older.HeadSeq, files[1], newer.HeadSeq)
	}
	want := (newer.HeadSeq - older.HeadSeq) * audit.LinkSize
	var links []byte
	if *linksPath != "" {
		var err error
		if links, err = readProof(*linksPath); err != nil {
			fmt.Fprintln(c.stderr, "ledgerctl:", err)
			return 2
		}
	} else {
		if err := c.nodesEnv(); err != nil {
			fmt.Fprintln(c.stderr, "ledgerctl:", err)
			return 2
		}
		code := c.call("verify chain", func(ctx context.Context, cl *ledger.Client) int {
			var err error
			links, err = cl.Links(ctx, older.HeadSeq, newer.HeadSeq)
			if err != nil || uint64(len(links)) < want {
				fmt.Fprintf(c.stdout, "no verdict: links %d to %d: got %d of %d from the cluster, %v\n", older.HeadSeq, newer.HeadSeq, uint64(len(links))/audit.LinkSize, want/audit.LinkSize, err)
				return 3
			}
			return 0
		})
		if code != 0 {
			return code
		}
	}
	err := ledger.ConnectProofs(older, newer, links)
	switch {
	case errors.Is(err, ledger.ErrConflict):
		fmt.Fprintf(c.stdout, "CONFLICT: %v\n%s\n%s\n", err, describe(files[0], older), describe(files[1], newer))
		return 1
	case err != nil:
		fmt.Fprintf(c.stdout, "invalid: %v\n", err)
		return 1
	}
	fmt.Fprintf(c.stdout, "connected: head %d %x leads to head %d %x over %d links\n", older.HeadSeq, older.Head[:8], newer.HeadSeq, newer.Head[:8], newer.HeadSeq-older.HeadSeq)
	return 0
}

func (c *ctl) verifyDir(args []string) int {
	flags := c.flags("verify dir")
	keysPath := flags.String("keys", "", "file with the trusted public keys, one per line")
	headPath := flags.String("head", "", "a proof or head saved earlier; the directory is recounted up to its link number")
	dirs, code, ok := c.positional(flags, args)
	if !ok {
		return code
	}
	switch {
	case len(dirs) != 1:
		return c.misuse(flags, "want one data directory")
	case *headPath == "":
		return c.misuse(flags, "-head is missing")
	}
	keys, code, ok := c.trust(flags, *keysPath)
	if !ok {
		return code
	}
	sessions, err := maxSessions()
	if err != nil {
		fmt.Fprintln(c.stderr, "ledgerctl:", err)
		return 2
	}
	p, code := c.check(*headPath, keys, false)
	if code != 0 {
		return code
	}
	if p.HeadSeq == 0 {
		return c.misuse(flags, "%s is the head before the first link, it fits every directory", *headPath)
	}
	unlock, err := lockDir(dirs[0])
	if errors.Is(err, disk.ErrLocked) {
		fmt.Fprintf(c.stdout, "no verdict: %s is locked by a running node; stop the node or give a copy of the directory\n", dirs[0])
		return 3
	}
	if err != nil {
		fmt.Fprintln(c.stderr, "ledgerctl:", err)
		return 2
	}
	copied, err := copyDir(dirs[0])
	unlock()
	if err != nil {
		fmt.Fprintln(c.stderr, "ledgerctl:", err)
		return 2
	}
	runs, err := replica.Recount(replica.Config{
		FS:             copied,
		Dir:            copyRoot,
		Clock:          func() uint64 { return uint64(time.Now().UnixNano()) }, //nolint:gosec // G115: the wall clock is after 1970
		Rand:           rand.New(rand.NewPCG(1, 1)),                            //nolint:gosec // G404: a cluster of one wins every election, whatever it draws
		MaxSessions:    sessions,
		SnapshotEvery:  1,
		SegmentSize:    wal.MaxSegmentSize,
		ClockStep:      1,
		ElectionTicks:  50,
		HeartbeatTicks: 5,
		SnapshotTicks:  500,
		MaxAppendBytes: 1 << 20,
		MaxUncommitted: 256,
	}, audit.Head{Seq: p.HeadSeq, Hash: p.Head})
	switch {
	case errors.Is(err, replica.ErrBehind):
		fmt.Fprintf(c.stdout, "no verdict: %v\n", err)
		return 3
	case errors.Is(err, replica.ErrOtherLog):
		fmt.Fprintf(c.stdout, "DIFFERS: %v; recount the directory of a second node to tell which\n", err)
		return 1
	case errors.Is(err, replica.ErrOtherHead):
		fmt.Fprintf(c.stdout, "DIFFERS: %v\n", err)
		return 1
	case errors.Is(err, replica.ErrMaxSessions):
		fmt.Fprintf(c.stderr, "ledgerctl: LEDGER_MAX_SESSIONS=%d is not the value of this directory: %v\n", sessions, err)
		return 2
	case errors.Is(err, wal.ErrCorrupt):
		fmt.Fprintf(c.stdout, "damaged: %v\n", err)
		return 1
	case err != nil:
		fmt.Fprintf(c.stdout, "does not open: %v\n", err)
		return 1
	}
	for _, r := range runs {
		fmt.Fprintf(c.stdout, "recounted from snapshot %d: %d links, committed for sure up to link %d\n", r.Snapshot, r.Links, r.Committed)
	}
	last := runs[len(runs)-1]
	fmt.Fprintf(c.stdout, "the directory leads to head %d %s; %d links after it are not checked\n", p.HeadSeq, hex.EncodeToString(p.Head[:]), last.Links-p.HeadSeq)
	return 0
}

func maxSessions() (int, error) {
	s, ok := os.LookupEnv("LEDGER_MAX_SESSIONS")
	if !ok {
		return 1024, nil
	}
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil || n < 1 || n > 1000000 {
		return 0, fmt.Errorf("LEDGER_MAX_SESSIONS=%q: want a number from 1 to 1000000", s)
	}
	return int(n), nil
}

func lockDir(dir string) (func(), error) {
	if _, err := os.Stat(filepath.Join(dir, "LOCK")); errors.Is(err, fs.ErrNotExist) {
		return func() {}, nil
	}
	lock, err := disk.OS{}.Lock(dir)
	if err != nil {
		return nil, err
	}
	return func() { _ = lock.Close() }, nil
}

func copyDir(dir string) (*disk.Sim, error) {
	copied := disk.NewSim(rand.New(rand.NewPCG(1, 2))) //nolint:gosec // G404: nothing is drawn, the copy never crashes
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		to := filepath.Join(copyRoot, rel)
		if d.IsDir() {
			return copied.MkdirAll(to)
		}
		if !d.Type().IsRegular() {
			return nil
		}
		b, err := os.ReadFile(path) //nolint:gosec // G304: a file under the directory the operator named
		if err != nil {
			return err
		}
		f, err := copied.Create(to)
		if err != nil {
			return err
		}
		if _, err := f.WriteAt(b, 0); err != nil {
			return err
		}
		if err := f.Sync(); err != nil {
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
		return copied.SyncDir(filepath.Dir(to))
	})
	return copied, err
}
