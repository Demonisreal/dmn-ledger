package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"io/fs"
	"maps"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/audit"
	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/disk"
	"github.com/Demonisreal/dmn-ledger/internal/notary"
	"github.com/Demonisreal/dmn-ledger/internal/replica"
	"github.com/Demonisreal/dmn-ledger/internal/wal"
	"github.com/Demonisreal/dmn-ledger/internal/wire"
	"github.com/Demonisreal/dmn-ledger/ledger"
)

func output(t *testing.T, line string) string {
	t.Helper()
	var out, errs bytes.Buffer
	if code := run(args(line), &out, &errs); code != 0 {
		t.Fatalf("%s: exit %d, stdout %q, stderr %q", line, code, out.String(), errs.String())
	}
	return out.String()
}

func write(t *testing.T, path string, b []byte) string {
	t.Helper()
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func read(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestKeyFiles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "node.key")
	pub := output(t, "key new "+path)
	if !regexp.MustCompile(`^[0-9a-f]{64}\n$`).MatchString(pub) {
		t.Fatalf("key new printed %q", pub)
	}
	if got := output(t, "key show "+path); got != pub {
		t.Fatalf("key show printed %q, key new %q", got, pub)
	}
	if st, err := os.Stat(path); err != nil || st.Mode().Perm() != 0o600 {
		t.Fatalf("key file %v, %v", st.Mode(), err)
	}
	before := read(t, path)
	for _, s := range []step{
		{args("key new " + path), 2, `key new: open .*: file exists\n$`},
		{args("key new"), 2, `ledgerctl key new: want one file\n$`},
		{args("key show " + filepath.Join(dir, "missing.key")), 1, `no such file`},
		{args("key show"), 2, `want one file`},
	} {
		s.check(t)
	}
	if !bytes.Equal(read(t, path), before) {
		t.Fatal("key new changed an existing key file")
	}
	if err := os.Chmod(path, 0o640); err != nil { //nolint:gosec // G302: the key file is opened to its group on purpose
		t.Fatal(err)
	}
	step{args("key show " + path), 1, `is open to group or others \(mode 0640\)\n$`}.check(t)
}

func digest(t *testing.T, dir string) [32]byte {
	t.Helper()
	h := sha256.New()
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		h.Write([]byte(path))
		h.Write(read(t, path))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return [32]byte(h.Sum(nil))
}

func lyingNode(t *testing.T, links []byte) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	answer := func(nc net.Conn) {
		defer nc.Close()
		for {
			frame, _, err := wire.ReadFrame(nc, nil, 1, wire.MaxClientBody)
			if err != nil {
				return
			}
			req, err := frame.Request()
			if err != nil {
				return
			}
			reply := client.Reply{Client: req.Client, Number: req.Number, Op: req.Op, Status: client.OK, Leader: 1}
			switch req.Op {
			case client.Register:
				reply.Body = binary.BigEndian.AppendUint64(nil, 1)
			case client.Links:
				reply.Body = links
			}
			if _, err := nc.Write(wire.AppendReply(nil, 1, reply)); err != nil {
				return
			}
		}
	}
	go func() {
		for {
			nc, err := l.Accept()
			if err != nil {
				return
			}
			answer(nc)
		}
	}()
	return l.Addr().String()
}

func TestProofsAndTheirChecks(t *testing.T) {
	files := t.TempDir()
	at := func(name string) string { return filepath.Join(files, name) }
	output(t, "key new "+at("node.key"))
	key, err := notary.ReadKey(at("node.key"))
	if err != nil {
		t.Fatal(err)
	}
	data := dataDir(t)
	addr, stop := serveNode(t, key, data)
	t.Setenv("LEDGER_CLUSTER", "1")
	t.Setenv("LEDGER_NODES", addr)

	output(t, "account create -id 1 -ledger 1 -code 10")
	output(t, "account create -id 2 -ledger 1 -code 10")
	booked := output(t, "transfer create -id 10 -debit 1 -credit 2 -amount 100 -ledger 1 -code 1")
	output(t, "transfer create -id 11 -debit 1 -credit 2 -amount 7 -ledger 1 -code 1 -pending -timeout 3600")
	when := strings.TrimSpace(strings.TrimPrefix(booked, "ok 10 at "))

	keys := output(t, "status -keys")
	if want := hex.EncodeToString(key.Public().(ed25519.PublicKey)) + " node 1\n"; keys != want {
		t.Fatalf("status -keys printed %q, want %q", keys, want)
	}
	write(t, at("keys"), []byte(keys))
	for _, s := range []step{
		{args("proof transfer 10 -o " + at("transfer")), 0, `^wrote 364 bytes to .*transfer\n$`},
		{args("proof account -o " + at("account") + " 1"), 0, `^wrote \d+ bytes to `},
		{args("proof batch " + when + " -o " + at("batch")), 0, `^wrote 264 bytes to `},
		{args("head -o " + at("head")), 0, `^wrote 146 bytes to `},
		{args("proof transfer 99"), 1, `^not found 99\n$`},
		{args("proof expiry 11"), 1, `^not found 11\n$`},
		{args("proof batch 12"), 1, `^not found 12\n$`},
		{args("proof batch yesterday"), 2, `want nanoseconds or a time like`},
		{args("proof transfer"), 2, `want one argument, got 0`},
		{args("head now"), 2, `unexpected arguments \[now\]`},
	} {
		s.check(t)
	}
	write(t, at("transfer.hex"), []byte(output(t, "proof transfer 10")))

	signed := ` head=[0-9a-f]{64} key=` + hex.EncodeToString(key.Public().(ed25519.PublicKey)) + `\n`
	step{args("verify proof " + at("transfer") + " " + at("transfer.hex") + " " + at("account") + " " + at("batch") + " " + at("head") + " -keys " + at("keys")), 0,
		`^file=\S+/transfer kind=transfer id=10 timestamp=` + when + ` time=` + when + ` seq=3` + signed +
			`file=\S+/transfer.hex kind=transfer id=10 timestamp=\S+ time=\S+ seq=3` + signed +
			`file=\S+/account kind=account id=1 timestamp=\S+ time=\S+ seq=1` + signed +
			`file=\S+/batch kind=batch time=` + when + ` seq=3` + signed +
			`file=\S+/head kind=head seq=4` + signed + `$`}.check(t)

	proof := read(t, at("transfer"))
	flipped := bytes.Clone(proof)
	flipped[len(flipped)/2] ^= 0x10
	write(t, at("flipped"), flipped)
	output(t, "key new "+at("other.key"))
	other, err := notary.ReadKey(at("other.key"))
	if err != nil {
		t.Fatal(err)
	}
	write(t, at("other.keys"), []byte(hex.EncodeToString(other.Public().(ed25519.PublicKey))+" someone else\n"))
	write(t, at("unsigned"), proof[:len(proof)-notary.StampSize])
	for _, s := range []step{
		{args("verify proof " + at("flipped") + " -keys " + at("keys")), 1, `^file=\S+ invalid: `},
		{args("verify proof " + at("transfer") + " -keys " + at("other.keys")), 1, `invalid: ledger: proof does not hold: notary: key is not trusted\n$`},
		{args("verify proof " + at("unsigned") + " -keys " + at("keys")), 3, `no verdict: ledger: proof carries no stamp; with -unsigned only the path from the record to head 3 is checked`},
		{args("verify proof " + at("unsigned") + " -keys " + at("keys") + " -unsigned"), 0, `^file=\S+ kind=transfer id=10 .* seq=3 head=[0-9a-f]{64} key=none\n$`},
		{args("verify proof " + at("transfer") + " " + at("flipped") + " " + at("unsigned") + " -keys " + at("keys")), 1, `invalid`},
		{args("verify proof " + at("unsigned") + " " + at("transfer") + " -keys " + at("keys")), 3, `no verdict`},
		{args("verify proof " + at("transfer")), 2, `-keys is missing`},
		{args("verify proof -keys " + at("keys")), 2, `want at least one file`},
		{args("verify proof " + at("missing") + " -keys " + at("keys")), 2, `no such file`},
		{args("verify proof " + at("transfer") + " -keys " + at("transfer")), 2, `line 1: want 64 hex digits`},
	} {
		s.check(t)
	}
	t.Setenv("LEDGER_CLUSTER", "2")
	step{args("verify proof " + at("transfer") + " -keys " + at("keys")), 1, `invalid: ledger: proof does not hold: notary: stamp is for another cluster`}.check(t)
	t.Setenv("LEDGER_CLUSTER", "1")

	for range 5 {
		output(t, "transfer create -debit 1 -credit 2 -amount 1 -ledger 1 -code 1")
	}
	output(t, "head -o "+at("later"))
	step{args("verify chain " + at("transfer") + " " + at("later") + " -keys " + at("keys")), 0, `^connected: head 3 [0-9a-f]{16} leads to head 9 [0-9a-f]{16} over 6 links\n$`}.check(t)
	step{args("verify chain " + at("transfer") + " " + at("head") + " -keys " + at("keys")), 0, `^connected: head 3 [0-9a-f]{16} leads to head 4 [0-9a-f]{16} over 1 link\n$`}.check(t)

	cl, err := ledger.Dial(context.Background(), ledger.Config{Cluster: 1, Nodes: []string{addr}})
	if err != nil {
		t.Fatal(err)
	}
	links, err := cl.Links(context.Background(), 3, 9)
	_ = cl.Close()
	if err != nil || len(links) != 6*audit.LinkSize {
		t.Fatalf("links 3 to 9: %d bytes, %v", len(links), err)
	}
	changed := bytes.Clone(links)
	changed[2*audit.LinkSize+40] ^= 1
	write(t, at("links"), links)
	write(t, at("changed"), changed)
	write(t, at("short"), links[audit.LinkSize:])
	const unconnected = `ledger: the links do not connect the heads: head 3 [0-9a-f]{16} does not lead to head 9 [0-9a-f]{16} over these links`
	const unproven = `this does not show that the heads contradict each other\n$`
	for _, s := range []step{
		{args("verify chain " + at("transfer") + " " + at("later") + " -keys " + at("keys") + " -links " + at("links")), 0, `^connected: head 3 `},
		{args("verify chain " + at("transfer") + " " + at("later") + " -keys " + at("keys") + " -links " + at("changed")), 1, `^file=\S+/changed invalid: ` + unconnected + `; ` + unproven},
		{args("verify chain " + at("transfer") + " " + at("later") + " -keys " + at("keys") + " -links " + at("short")), 1,
			`^file=\S+/short invalid: ledger: the links do not connect the heads: 565 bytes of links between heads 3 and 9, want 6 links; ` + unproven},
		{args("verify chain " + at("later") + " " + at("transfer") + " -keys " + at("keys")), 2, `give the older one first`},
		{args("verify chain " + at("transfer") + " " + at("later") + " -keys " + at("other.keys")), 1, `key is not trusted`},
		{args("verify chain " + at("unsigned") + " " + at("later") + " -keys " + at("keys")), 3, `no verdict`},
		{args("verify chain " + at("transfer") + " -keys " + at("keys")), 2, `want the older and the newer proof`},
	} {
		s.check(t)
	}

	const unstamped = `; links from the cluster carry no stamp, so ` + unproven
	for _, lie := range []struct {
		links []byte
		out   string
	}{
		{changed, `^no verdict: ` + unconnected + unstamped},
		{append(bytes.Clone(links), links[:audit.LinkSize]...), `^no verdict: ledger: the links do not connect the heads: 791 bytes of links between heads 3 and 9, want 6 links` + unstamped},
		{append(bytes.Clone(links), 0), `^no verdict: ledger: the links do not connect the heads: 679 bytes of links between heads 3 and 9, want 6 links` + unstamped},
		{links[audit.LinkSize:], `^no verdict: links 3 to 9: got 5 of 6 from the cluster, `},
	} {
		t.Setenv("LEDGER_NODES", lyingNode(t, lie.links))
		step{args("verify chain " + at("transfer") + " " + at("later") + " -keys " + at("keys")), 3, lie.out}.check(t)
	}
	t.Setenv("LEDGER_NODES", addr)

	step{args("verify dir " + data + " -head " + at("later") + " -keys " + at("keys")), 3, `^no verdict: \S+ is locked by a running node; stop the node or give a copy of the directory\n$`}.check(t)
	stop()
	before := digest(t, data)
	ahead := audit.Head{Seq: 1000}
	write(t, at("ahead"), append(audit.AppendHead(nil, ahead), notary.Sign(key, 1, ahead)...))
	later, err := ledger.VerifyProof(read(t, at("later")), 1, [][]byte{key.Public().(ed25519.PublicKey)})
	if err != nil {
		t.Fatal(err)
	}
	wrong := audit.Head{Seq: later.HeadSeq, Hash: later.Head}
	wrong.Hash[0] ^= 1
	write(t, at("wrong"), append(audit.AppendHead(nil, wrong), notary.Sign(key, 1, wrong)...))
	zero := audit.Head{}
	write(t, at("zero"), append(audit.AppendHead(nil, zero), notary.Sign(key, 1, zero)...))
	unset(t, "LEDGER_NODES")
	for _, s := range []step{
		{args("verify chain " + at("later") + " " + at("wrong") + " -keys " + at("keys")), 1,
			`^CONFLICT: ledger: the heads contradict each other: head 9 is [0-9a-f]{16} in one and [0-9a-f]{16} in the other\nfile=\S+/later kind=head seq=9` + signed + `file=\S+/wrong kind=head seq=9` + signed + `$`},
		{args("verify chain " + at("later") + " " + at("wrong") + " -keys " + at("keys") + " -links " + at("links")), 1, `^CONFLICT: ledger: the heads contradict each other: head 9 is `},
		{args("verify chain " + at("later") + " " + at("later") + " -keys " + at("keys")), 0, `^connected: head 9 [0-9a-f]{16} leads to head 9 [0-9a-f]{16} over 0 links\n$`},
		{args("verify chain " + at("later") + " " + at("later") + " -keys " + at("keys") + " -links " + at("links")), 1,
			`^file=\S+/links invalid: ledger: the links do not connect the heads: 678 bytes of links between heads 9 and 9, want 0 links; ` + unproven},
	} {
		s.check(t)
	}
	for _, s := range []step{
		{args("verify dir " + data + " -head " + at("later") + " -keys " + at("keys")), 0,
			`^recounted from snapshot 0: 9 links, committed for sure up to link 0\nthe directory leads to head 9 [0-9a-f]{64}; 0 links after it are not checked\n$`},
		{args("verify dir " + data + " -head " + at("transfer") + " -keys " + at("keys")), 0, `the directory leads to head 3 [0-9a-f]{64}; 6 links after it are not checked\n$`},
		{args("verify dir " + data + " -head " + at("wrong") + " -keys " + at("keys")), 1,
			`^DIFFERS: replica: the log leads to another head, an entry in it was changed or never committed: link 9 from snapshot 0 has head [0-9a-f]{8}, want [0-9a-f]{8}, and the snapshots end at link 0; recount the directory of a second node to tell which\n$`},
		{args("verify dir " + data + " -head " + at("ahead") + " -keys " + at("keys")), 3, `^no verdict: replica: the directory ends before the head: 9 links from snapshot 0, the head is link 1000\n$`},
		{args("verify dir " + data + " -head " + at("zero") + " -keys " + at("keys")), 2, `it fits every directory`},
		{args("verify dir " + data + " -head " + at("unsigned") + " -keys " + at("keys")), 3, `no verdict`},
		{args("verify dir " + data + " -keys " + at("keys")), 2, `-head is missing`},
		{args("verify dir " + at("missing") + " -head " + at("later") + " -keys " + at("keys")), 2, `no such file`},
	} {
		s.check(t)
	}
	t.Setenv("LEDGER_MAX_SESSIONS", "0")
	step{args("verify dir " + data + " -head " + at("later") + " -keys " + at("keys")), 2, `LEDGER_MAX_SESSIONS="0": want a number from 1 to 1000000`}.check(t)
	if digest(t, data) != before {
		t.Fatal("verify dir changed the directory it checked")
	}
}

func signedHead(t *testing.T, dir string, cluster uint64) (string, string) {
	t.Helper()
	key := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{7}, ed25519.SeedSize))
	head := audit.Head{Seq: 1, Hash: [32]byte{1}}
	keys := write(t, filepath.Join(dir, "keys"), []byte(hex.EncodeToString(key.Public().(ed25519.PublicKey))+"\n"))
	return write(t, filepath.Join(dir, "head"), append(audit.AppendHead(nil, head), notary.Sign(key, cluster, head)...)), keys
}

func TestVerifyDirOfAnotherClusterIsAUsageError(t *testing.T) {
	data := t.TempDir()
	if err := replica.Init(disk.OS{}, data, wal.Identity{Cluster: 1, Node: 2, Nodes: 3}); err != nil {
		t.Fatal(err)
	}
	head, keys := signedHead(t, t.TempDir(), 2)
	t.Setenv("LEDGER_CLUSTER", "2")
	before := tree(t, data)
	var out, errs bytes.Buffer
	code := run(args("verify dir "+data+" -head "+head+" -keys "+keys), &out, &errs)
	want := "ledgerctl: LEDGER_CLUSTER=2 is not the cluster of this directory: replica: the directory belongs to another node or cluster: /copy holds node 2 of 3 in cluster 1, not cluster 2\n"
	if code != 2 || out.String() != "" || errs.String() != want {
		t.Fatalf("exit %d, stdout %q, stderr %q, want exit 2 and %q", code, out.String(), errs.String(), want)
	}
	if !maps.Equal(tree(t, data), before) {
		t.Fatal("verify dir changed the directory it checked")
	}
}

func TestVerifyDirWithoutMetaDoesNotOpen(t *testing.T) {
	empty := t.TempDir()
	logged := t.TempDir()
	if err := os.Mkdir(filepath.Join(logged, "wal"), 0o750); err != nil {
		t.Fatal(err)
	}
	head, keys := signedHead(t, t.TempDir(), 1)
	t.Setenv("LEDGER_CLUSTER", "1")
	for _, dir := range []string{empty, logged} {
		before := tree(t, dir)
		var out, errs bytes.Buffer
		code := run(args("verify dir "+dir+" -head "+head+" -keys "+keys), &out, &errs)
		if want := "does not open: wal: no meta: /copy/meta\n"; code != 1 || out.String() != want || errs.String() != "" {
			t.Errorf("%s: exit %d, stdout %q, stderr %q, want exit 1 and %q", dir, code, out.String(), errs.String(), want)
		}
		if !maps.Equal(tree(t, dir), before) {
			t.Errorf("verify dir changed %s", dir)
		}
	}
}
