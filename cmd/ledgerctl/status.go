package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/Demonisreal/dmn-ledger/ledger"
)

const probeTimeout = 2 * time.Second

type probe struct {
	addr  string
	st    ledger.NodeStatus
	clock time.Duration
	err   error
}

func (c *ctl) status(args []string) int {
	if code, ok := c.parse(c.flags("status"), args, false); !ok {
		return code
	}
	probes := make([]probe, len(c.nodes))
	for i, addr := range c.nodes {
		ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
		sent := time.Now()
		st, err := ledger.Status(ctx, c.cluster, addr)
		probes[i] = probe{addr: addr, st: st, err: err, clock: offset(st.Clock, sent, time.Now())}
		cancel()
	}
	return report(c.stdout, probes)
}

func offset(clock uint64, sent, back time.Time) time.Duration {
	return time.Duration(int64(clock) - sent.Add(back.Sub(sent)/2).UnixNano()) //nolint:gosec // G115: a wall clock in nanoseconds fits an int64
}

func report(w io.Writer, probes []probe) int {
	rows := [][]string{{"node", "address", "role", "term", "commit", "applied", "apply hash", "repair", "ledger time", "clock", "next expiry"}}
	code, answered := 0, 0
	hashes := make(map[uint64][][32]byte)
	for i, p := range probes {
		node := strconv.Itoa(i + 1)
		if p.err != nil {
			rows = append(rows, []string{node, p.addr, "unreachable: " + p.err.Error()})
			code = 1
			continue
		}
		answered++
		repair := "no"
		if p.st.Repair {
			repair = "yes"
			code = 1
		}
		hashes[p.st.Applied] = append(hashes[p.st.Applied], p.st.ApplyHash)
		rows = append(rows, []string{node, p.addr, p.st.Role.String(),
			strconv.FormatUint(p.st.Term, 10), strconv.FormatUint(p.st.Commit, 10), strconv.FormatUint(p.st.Applied, 10),
			hex.EncodeToString(p.st.ApplyHash[:8]), repair, stamp(p.st.Time),
			fmt.Sprintf("%+dms", p.clock.Round(time.Millisecond).Milliseconds()), stamp(p.st.NextExpiry)})
	}
	table(w, rows)
	var index uint64
	shared := false
	for at, hs := range hashes {
		if len(hs) >= 2 && (!shared || at > index) {
			index, shared = at, true
		}
	}
	switch {
	case !shared:
		fmt.Fprintln(w, "no two nodes at the same index")
	case agree(hashes[index]):
		fmt.Fprintf(w, "nodes agree at index %d\n", index)
	default:
		fmt.Fprintf(w, "nodes DIFFER at index %d\n", index)
		code = 1
	}
	if answered == 0 {
		return 3
	}
	return code
}

func agree(hs [][32]byte) bool {
	for _, h := range hs[1:] {
		if h != hs[0] {
			return false
		}
	}
	return true
}

func table(w io.Writer, rows [][]string) {
	width := make([]int, len(rows[0]))
	for _, r := range rows {
		for i, cell := range r[:len(r)-1] {
			width[i] = max(width[i], len(cell))
		}
	}
	for _, r := range rows {
		var line strings.Builder
		for i, cell := range r[:len(r)-1] {
			fmt.Fprintf(&line, "%-*s", width[i]+2, cell)
		}
		line.WriteString(r[len(r)-1])
		fmt.Fprintln(w, line.String())
	}
}
