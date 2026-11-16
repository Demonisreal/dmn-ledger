package replica

import (
	"bytes"
	"encoding/binary"
	"math"
	"slices"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/audit"
	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/raft"
	"github.com/Demonisreal/dmn-ledger/internal/state"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
)

func proofOf(kind byte, id uint64) []byte {
	return append([]byte{kind}, ids(id)...)
}

func batchAt(time uint64) []byte {
	return binary.BigEndian.AppendUint64([]byte{audit.KindBatch}, time)
}

func span(from, to uint64) []byte {
	return binary.BigEndian.AppendUint64(binary.BigEndian.AppendUint64(nil, from), to)
}

func decode(t *testing.T, proof []byte) (audit.Receipt, audit.Head) {
	t.Helper()
	r, h, rest, err := audit.Decode(proof)
	if err != nil || len(rest) != 0 {
		t.Fatalf("proof %x: %v with %d bytes left", proof, err, len(rest))
	}
	return r, h
}

func linksOf(t *testing.T, body []byte) []audit.Link {
	t.Helper()
	if len(body)%audit.LinkSize != 0 {
		t.Fatalf("links of %d bytes", len(body))
	}
	var links []audit.Link
	for ; len(body) > 0; body = body[audit.LinkSize:] {
		links = append(links, audit.DecodeLink(body))
	}
	return links
}

func TestProveGoesThroughReadIndex(t *testing.T) {
	tn := newTestNet(t, 3, nil)
	tn.elect(0)
	c := tn.register(0, 1)
	ok(t, c.do(client.CreateAccounts, accounts(1, 2)))
	ok(t, c.do(client.CreateTransfers, transfer(10, 1, 2, 5)))
	if r := ok(t, c.do(client.Prove, proofOf(audit.KindTransfer, 10))); len(r.Body) == 0 {
		t.Fatal("no proof of a confirmed transfer")
	}

	reads := []client.Request{
		c.request(client.Prove, proofOf(audit.KindTransfer, 10)),
		c.request(client.Head, nil),
		c.request(client.Links, span(0, math.MaxUint64)),
	}
	for _, req := range reads {
		tn.send(1, req)
		if r := tn.take(); len(r) != 1 || r[0].Status != client.NotLeader || r[0].Leader != 1 {
			t.Fatalf("follower answered %s with %+v", req.Op, r)
		}
	}

	tn.cut[2], tn.cut[3] = true, true
	for _, req := range reads {
		tn.send(0, req)
	}
	for range 5 {
		tn.tick(0)
	}
	if r := tn.take(); len(r) != 0 {
		t.Fatalf("answered without a majority: %+v", r)
	}
	term := tn.nodes[0].Status().Term
	tn.nodes[0].Receive(raft.Message{Type: raft.MsgApp, From: 2, To: 1, Term: term + 1}, nil)
	tn.run(0)
	got := tn.take()
	if len(got) != len(reads) {
		t.Fatalf("replies %+v, want %d", got, len(reads))
	}
	for _, r := range got {
		if r.Status != client.NotLeader || r.Leader != 2 {
			t.Fatalf("deposed leader answered %+v, want NotLeader pointing at n2", r)
		}
	}
}

func TestBusyWhenTheWaitingReadsFillTheBudget(t *testing.T) {
	tn := newTestNet(t, 3, nil)
	tn.elect(0)
	c := tn.register(0, 1)
	tn.cut[2], tn.cut[3] = true, true
	widest := make([]byte, client.MaxBody/state.TransferSize*client.IDSize)
	var waiting []client.Request
	for range readBudget / (client.MaxBody / state.TransferSize * state.TransferSize) {
		waiting = append(waiting, c.request(client.LookupTransfers, widest))
		tn.send(0, waiting[len(waiting)-1])
	}
	if len(waiting) != 8 {
		t.Fatalf("%d of the widest lookups fit the budget, want 8", len(waiting))
	}
	for _, tc := range []struct {
		op   client.Op
		body []byte
		busy bool
	}{
		{client.Prove, proofOf(audit.KindAccount, 1), true},
		{client.Links, span(0, math.MaxUint64), true},
		{client.Links, span(0, 6), true},
		{client.Links, span(0, 5), false},
		{client.Head, nil, false},
		{client.Head, nil, true},
	} {
		req := c.request(tc.op, tc.body)
		tn.send(0, req)
		r := tn.take()
		switch {
		case tc.busy && (len(r) != 1 || r[0].Status != client.Busy || r[0].Number != req.Number):
			t.Fatalf("%s %x: replies %+v, want Busy", tc.op, tc.body, r)
		case !tc.busy && len(r) != 0:
			t.Fatalf("%s %x: replies %+v, want it to wait", tc.op, tc.body, r)
		case !tc.busy:
			waiting = append(waiting, req)
		}
	}
	tn.cut[2], tn.cut[3] = false, false
	for range 10 {
		tn.tick(0)
	}
	got := tn.take()
	if len(got) != len(waiting) {
		t.Fatalf("%d replies, want all %d waiting reads answered", len(got), len(waiting))
	}
	for i, r := range got {
		if r.Number != waiting[i].Number || r.Status != client.OK {
			t.Fatalf("reply %d: %s %s, want %s OK", i, r.Op, r.Status, waiting[i].Op)
		}
	}
	ok(t, tn.call(0, c.request(client.LookupAccounts, ids(4))))
}

func TestProofsOfOneRoundStayWithinTheBudget(t *testing.T) {
	const each, batches, asked = 10_000, 3, 32
	tn := newTestNet(t, 1, func(cfg *Config) { cfg.SnapshotEvery = 1 << 40 })
	tn.elect(0)
	c := tn.register(0, 1)
	ok(t, c.do(client.CreateAccounts, accounts(1, 2)))
	for b := range uint64(batches) {
		var body []byte
		for i := range uint64(each) {
			body = append(body, hold(1000+b*each+i, 1)...)
		}
		ok(t, c.do(client.CreateTransfers, body))
	}
	for range 150 {
		tn.tick(0)
	}
	ok(t, c.do(client.CreateAccounts, nil))
	n := tn.nodes[0]
	if l := n.sm.Links(n.sm.Head().Seq-1, n.sm.Head().Seq, nil)[0]; l.Leaves != batches*each {
		t.Fatalf("the sweep has %d leaves, want %d", l.Leaves, batches*each)
	}

	var reqs []client.Request
	for i := range uint64(asked) {
		reqs = append(reqs, c.request(client.Prove, proofOf(audit.KindExpiry, 1000+i)))
		n.Request(reqs[i])
	}
	var rounds []int
	for left := asked; left > 0; {
		tn.clocks[0] += tickNs
		n.Tick()
		out, err := n.Process()
		if err != nil {
			t.Fatal(err)
		}
		answered := 0
		for _, r := range out.Replies {
			switch {
			case r.Status == client.OK && len(r.Body) > 0:
				answered++
			case r.Status == client.Busy:
				n.Request(reqs[r.Number-reqs[0].Number])
			default:
				t.Fatalf("reply %+v", r)
			}
		}
		rounds = append(rounds, answered)
		left -= answered
		if answered == 0 {
			t.Fatalf("proofs answered per round %v", rounds)
		}
	}
	if len(rounds) != (asked+2)/3 || slices.Max(rounds) != 3 {
		t.Fatalf("proofs answered per round %v, want three: two proofs of %d leaves stay below the budget, the third crosses it", rounds, batches*each)
	}
}

func TestProofRequestsAreChecked(t *testing.T) {
	tn := newTestNet(t, 1, nil)
	tn.elect(0)
	c := tn.register(0, 1)
	id := ids(1)
	for _, tc := range []struct {
		op   client.Op
		body []byte
		want client.Status
	}{
		{client.Prove, batchAt(5), client.OK},
		{client.Prove, proofOf(audit.KindAccount, 1), client.OK},
		{client.Prove, proofOf(audit.KindTransfer, 1), client.OK},
		{client.Prove, proofOf(audit.KindExpiry, 1), client.OK},
		{client.Prove, nil, client.Malformed},
		{client.Prove, []byte{audit.KindBatch}, client.Malformed},
		{client.Prove, batchAt(5)[:8], client.Malformed},
		{client.Prove, append(batchAt(5), 0), client.Malformed},
		{client.Prove, append([]byte{audit.KindBatch}, id...), client.Malformed},
		{client.Prove, append([]byte{audit.KindAccount}, id[:8]...), client.Malformed},
		{client.Prove, append(proofOf(audit.KindTransfer, 1), 0), client.Malformed},
		{client.Prove, proofOf(audit.KindTransfer, 1)[:16], client.Malformed},
		{client.Prove, proofOf(audit.KindHead, 1), client.Malformed},
		{client.Prove, proofOf(audit.KindHead+1, 1), client.Malformed},
		{client.Prove, proofOf(0xff, 1), client.Malformed},
		{client.Head, nil, client.OK},
		{client.Head, []byte{0}, client.Malformed},
		{client.Head, id, client.Malformed},
		{client.Links, span(0, 0), client.OK},
		{client.Links, span(3, 9), client.OK},
		{client.Links, span(math.MaxUint64, math.MaxUint64), client.OK},
		{client.Links, span(9, 3), client.Malformed},
		{client.Links, span(1, 0), client.Malformed},
		{client.Links, nil, client.Malformed},
		{client.Links, span(0, 9)[:15], client.Malformed},
		{client.Links, append(span(0, 9), 0), client.Malformed},
		{client.Links, span(0, 9)[:8], client.Malformed},
	} {
		if r := c.do(tc.op, tc.body); r.Status != tc.want {
			t.Errorf("%s with body %x: %s, want %s", tc.op, tc.body, r.Status, tc.want)
		}
	}
}

func TestReplyHashIsOverTheReplyBody(t *testing.T) {
	tn := newTestNet(t, 1, nil)
	tn.elect(0)
	c := tn.register(0, 1)
	for _, w := range []struct {
		op     client.Op
		body   []byte
		events uint32
		fails  int
	}{
		{client.CreateAccounts, accounts(1, 2), 2, 0},
		{client.CreateAccounts, accounts(2, 3), 2, 1},
		{client.CreateTransfers, append(transfer(10, 1, 2, 5), transfer(11, 1, 9, 5)...), 2, 1},
		{client.CreateTransfers, transfer(12, 1, 2, 7), 1, 0},
		{client.CreateTransfers, nil, 0, 0},
	} {
		r := ok(t, c.do(w.op, w.body))
		if len(r.Body) != w.fails*client.FailureSize {
			t.Fatalf("%s: failures %x, want %d", w.op, r.Body, w.fails)
		}
		proof, _ := decode(t, ok(t, c.do(client.Prove, batchAt(r.Time))).Body)
		l := proof.Link
		switch {
		case l.Time != r.Time:
			t.Errorf("%s at %d: the link has time %d", w.op, r.Time, l.Time)
		case l.Body != audit.BodyHash(w.body):
			t.Errorf("%s at %d: the link holds %x for the body, the request hashes to %x", w.op, r.Time, l.Body[:4], audit.BodyHash(w.body))
		case l.Reply != audit.ReplyHash(r.Body):
			t.Errorf("%s at %d: the link holds %x for the reply, the reply %x hashes to %x", w.op, r.Time, l.Reply[:4], r.Body, audit.ReplyHash(r.Body))
		case l.Events != w.events:
			t.Errorf("%s at %d: %d events in the link, want %d", w.op, r.Time, l.Events, w.events)
		}
	}
}

func TestProofsConnectToTheHead(t *testing.T) {
	tn := newTestNet(t, 3, nil)
	tn.elect(0)
	c := tn.register(0, 1)
	made := ok(t, c.do(client.CreateAccounts, accounts(1, 2)))
	sent := ok(t, c.do(client.CreateTransfers, append(transfer(10, 1, 2, 5), hold(11, 1)...)))
	for range 150 {
		tn.tick(0)
	}
	swept := ok(t, c.do(client.CreateTransfers, nil))
	head := ok(t, c.do(client.Head, nil))
	_, now := decode(t, head.Body)
	if now.Seq != 3 {
		t.Fatalf("head %d after three batches", now.Seq)
	}

	sm := tn.nodes[0].sm
	a := sm.LookupAccounts([]u128.Uint128{u128.From64(2)}, nil)[0]
	a.DebitsPending, a.DebitsPosted, a.CreditsPending, a.CreditsPosted = u128.Uint128{}, u128.Uint128{}, u128.Uint128{}, u128.Uint128{}
	opened := state.AppendAccount(nil, a)
	stored := func(id uint64) []byte {
		return state.AppendTransfer(nil, sm.LookupTransfers([]u128.Uint128{u128.From64(id)}, nil)[0])
	}
	for _, tc := range []struct {
		name   string
		body   []byte
		kind   byte
		record []byte
		time   uint64
	}{
		{"account", proofOf(audit.KindAccount, 2), audit.KindAccount, opened, made.Time},
		{"transfer", proofOf(audit.KindTransfer, 10), audit.KindTransfer, stored(10), sent.Time},
		{"pending transfer", proofOf(audit.KindTransfer, 11), audit.KindTransfer, stored(11), sent.Time},
		{"expiry", proofOf(audit.KindExpiry, 11), audit.KindExpiry, stored(11), swept.Time},
		{"batch", batchAt(sent.Time), audit.KindBatch, nil, sent.Time},
	} {
		r := ok(t, c.do(client.Prove, tc.body))
		proof, h := decode(t, r.Body)
		switch {
		case proof.Kind != tc.kind || !bytes.Equal(proof.Record, tc.record):
			t.Fatalf("%s: kind %d with record %x, want %d with %x", tc.name, proof.Kind, proof.Record, tc.kind, tc.record)
		case proof.Link.Time != tc.time:
			t.Fatalf("%s: link at %d, want the batch at %d", tc.name, proof.Link.Time, tc.time)
		}
		links := linksOf(t, ok(t, c.do(client.Links, span(h.Seq, now.Seq))).Body)
		if got := audit.Connect(h, links); got != now {
			t.Fatalf("%s: head %d %x connects to %d %x, the ledger is at %d %x", tc.name, h.Seq, h.Hash[:4], got.Seq, got.Hash[:4], now.Seq, now.Hash[:4])
		}
	}
	for _, body := range [][]byte{proofOf(audit.KindAccount, 9), proofOf(audit.KindTransfer, 9), proofOf(audit.KindExpiry, 10), batchAt(sent.Time + 1)} {
		if r := ok(t, c.do(client.Prove, body)); len(r.Body) != 0 {
			t.Fatalf("proof of %x for nothing: %x", body, r.Body)
		}
	}
}

func TestStatusShowsTheHead(t *testing.T) {
	tn := newTestNet(t, 3, nil)
	tn.elect(0)
	c := tn.register(0, 1)
	for i, n := range tn.nodes {
		if h := n.Status().Head; h != (audit.Head{}) {
			t.Fatalf("n%d shows head %d %x before the first batch", i+1, h.Seq, h.Hash[:4])
		}
	}
	for k, w := range []struct {
		op   client.Op
		body []byte
	}{
		{client.CreateAccounts, accounts(1, 2)},
		{client.CreateAccounts, accounts(2)},
		{client.CreateTransfers, nil},
		{client.CreateTransfers, transfer(10, 1, 2, 5)},
	} {
		ok(t, c.do(w.op, w.body))
		tn.tick(tn.all()...)
		_, want := decode(t, ok(t, c.do(client.Head, nil)).Body)
		if want.Seq != uint64(k)+1 {
			t.Fatalf("head %d after %d batches", want.Seq, k+1)
		}
		for i, n := range tn.nodes {
			if h := n.Status().Head; h != want {
				t.Fatalf("n%d shows head %d %x, the leader serves %d %x", i+1, h.Seq, h.Hash[:4], want.Seq, want.Hash[:4])
			}
		}
	}
	tn.register(0, 2)
	tn.tick(tn.all()...)
	if h := tn.nodes[1].Status().Head; h.Seq != 4 {
		t.Fatalf("a register moved the head to %d", h.Seq)
	}
}

func TestSameReceiptOnEveryNode(t *testing.T) {
	tn := newTestNet(t, 3, nil)
	tn.elect(0)
	c := tn.register(0, 1)
	ok(t, c.do(client.CreateAccounts, accounts(1, 2)))
	sent := ok(t, c.do(client.CreateTransfers, append(transfer(10, 1, 2, 5), transfer(11, 2, 1, 3)...)))
	ok(t, c.do(client.CreateTransfers, transfer(12, 1, 9, 5)))
	reads := []struct {
		op   client.Op
		body []byte
	}{
		{client.Prove, proofOf(audit.KindAccount, 1)},
		{client.Prove, proofOf(audit.KindTransfer, 11)},
		{client.Prove, batchAt(sent.Time)},
		{client.Head, nil},
		{client.Links, span(0, math.MaxUint64)},
	}
	served := func() [][]byte {
		var bodies [][]byte
		for _, rd := range reads {
			bodies = append(bodies, ok(t, c.do(rd.op, rd.body)).Body)
		}
		return bodies
	}
	first := served()
	lead := 0
	for range 2 {
		prev := lead
		var others []int
		for i := range tn.nodes {
			if i != prev {
				others = append(others, i)
			}
		}
		tn.cut[uint64(prev)+1] = true
		lead = tn.choose(others...)
		tn.cut[uint64(prev)+1] = false
		c.node = lead
		again := served()
		for k := range reads {
			if !bytes.Equal(again[k], first[k]) {
				t.Fatalf("%s %x from n%d: %x, n1 served %x", reads[k].op, reads[k].body, lead+1, again[k], first[k])
			}
		}
	}
}

func TestLinksRange(t *testing.T) {
	tn := newTestNet(t, 1, nil)
	n := tn.nodes[0]
	const batches = client.MaxLinks + 21
	for i := range uint64(batches) {
		if _, err := n.sm.Apply(state.CreateTransfers, i+1, nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	at := func(seq uint64) audit.Head {
		h, _ := n.sm.HeadAt(seq)
		return h
	}
	for _, tc := range []struct {
		from, to uint64
		want     int
	}{
		{0, math.MaxUint64, client.MaxLinks},
		{0, client.MaxLinks, client.MaxLinks},
		{0, client.MaxLinks + 1, client.MaxLinks},
		{1, client.MaxLinks + 1, client.MaxLinks},
		{client.MaxLinks, math.MaxUint64, 21},
		{5, 6, 1},
		{5, 5, 0},
		{0, 0, 0},
		{batches - 1, batches, 1},
		{batches, math.MaxUint64, 0},
		{batches + 1, math.MaxUint64, 0},
		{math.MaxUint64, math.MaxUint64, 0},
	} {
		r, _ := n.lookup(client.Request{Op: client.Links, Body: span(tc.from, tc.to)})
		links := linksOf(t, r.Body)
		if len(links) != tc.want || len(r.Body) > 128<<10 {
			t.Fatalf("links from %d to %d: %d in %d bytes, want %d", tc.from, tc.to, len(links), len(r.Body), tc.want)
		}
		if got, want := audit.Connect(at(tc.from), links), at(tc.from+uint64(tc.want)); tc.want > 0 && got != want {
			t.Fatalf("links from %d to %d connect to %d %x, want %d %x", tc.from, tc.to, got.Seq, got.Hash[:4], want.Seq, want.Hash[:4])
		}
	}
}
