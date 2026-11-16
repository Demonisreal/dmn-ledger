package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/Demonisreal/dmn-ledger/ledger"
)

func (c *ctl) prove(kind ledger.ProofKind) func([]string) int {
	name := "proof " + kind.String()
	if kind == ledger.ProofHead {
		name = "head"
	}
	return func(args []string) int {
		flags := c.flags(name)
		out := flags.String("o", "", "write the proof to this file instead of hex to stdout")
		pos, code, ok := c.positional(flags, args)
		if !ok {
			return code
		}
		var fetch func(context.Context, *ledger.Client) ([]byte, error)
		switch {
		case kind == ledger.ProofHead && len(pos) == 0:
			fetch = func(ctx context.Context, cl *ledger.Client) ([]byte, error) { return cl.Head(ctx) }
		case kind == ledger.ProofHead:
			return c.misuse(flags, "unexpected arguments %v", pos)
		case len(pos) != 1:
			return c.misuse(flags, "want one argument, got %d", len(pos))
		case kind == ledger.ProofBatch:
			at, err := batchTime(pos[0])
			if err != nil {
				return c.misuse(flags, "%v", err)
			}
			fetch = func(ctx context.Context, cl *ledger.Client) ([]byte, error) { return cl.ProveBatch(ctx, at) }
		default:
			id, err := ledger.Parse(pos[0])
			if err != nil {
				return c.misuse(flags, "%v", err)
			}
			fetch = func(ctx context.Context, cl *ledger.Client) ([]byte, error) { return cl.Prove(ctx, kind, id) }
		}
		if err := c.env(); err != nil {
			fmt.Fprintln(c.stderr, "ledgerctl:", err)
			return 2
		}
		return c.call(name, func(ctx context.Context, cl *ledger.Client) int {
			proof, err := fetch(ctx, cl)
			switch {
			case err != nil:
				return c.fail(name, err)
			case proof == nil:
				fmt.Fprintf(c.stdout, "not found %s\n", pos[0])
				return 1
			case *out == "":
				fmt.Fprintln(c.stdout, hex.EncodeToString(proof))
				return 0
			}
			if err := os.WriteFile(*out, proof, 0o644); err != nil { //nolint:gosec // G306: a proof is meant to be passed on
				fmt.Fprintf(c.stderr, "ledgerctl: %s: %v\n", name, err)
				return 1
			}
			fmt.Fprintf(c.stdout, "wrote %d bytes to %s\n", len(proof), *out)
			return 0
		})
	}
}

func batchTime(s string) (uint64, error) {
	if ns, err := strconv.ParseUint(s, 10, 64); err == nil {
		return ns, nil
	}
	at, err := time.Parse(time.RFC3339Nano, s)
	if err != nil || at.UnixNano() <= 0 {
		return 0, fmt.Errorf("time %q: want nanoseconds or a time like 2026-11-16T05:51:47.473359071Z", s)
	}
	return uint64(at.UnixNano()), nil //nolint:gosec // G115: positive, checked above
}

func readProof(path string) ([]byte, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // G304: the operator names the file
	if err != nil {
		return nil, err
	}
	if text := bytes.TrimSuffix(raw, []byte("\n")); len(text) > 0 {
		if proof, err := hex.DecodeString(string(text)); err == nil {
			return proof, nil
		}
	}
	return raw, nil
}
