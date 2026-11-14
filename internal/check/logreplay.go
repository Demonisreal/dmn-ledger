package check

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"math"
	"slices"

	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/oracle"
	"github.com/Demonisreal/dmn-ledger/internal/raft"
	"github.com/Demonisreal/dmn-ledger/internal/replica"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
)

const (
	entryRegister uint8 = 1
	entryRequest  uint8 = 2

	requestHead = client.IDSize + 8 + 8 + 1 + 8
)

type session struct {
	client u128.Uint128
	since  uint64
	last   uint64
	access uint64
}

type request struct {
	client  u128.Uint128
	session uint64
	number  uint64
}

type execution struct {
	index  uint64
	op     client.Op
	recs   []byte
	status client.Status
	time   uint64
	fails  []byte
}

type shadow struct {
	ledger   *oracle.Ledger
	limit    int
	sessions []session
	opened   map[uint64]u128.Uint128
	ran      map[request]execution
}

func newShadow(limit int) *shadow {
	return &shadow{
		ledger: oracle.New(),
		limit:  limit,
		opened: make(map[uint64]u128.Uint128),
		ran:    make(map[request]execution),
	}
}

func (w *watch) logged(id uint64, ents []raft.Entry) error {
	for _, e := range ents {
		key := [2]uint64{e.Index, e.Term}
		was, seen := w.entries[key]
		switch {
		case !seen:
			e.Body = slices.Clone(e.Body)
			w.entries[key] = e
		case was.Kind != e.Kind || !bytes.Equal(was.Body, e.Body):
			return w.fail("S3", id, "sent entry %d of term %d with another body than an earlier MsgApp", e.Index, e.Term)
		}
	}
	return nil
}

func (w *watch) replayLog(nodes []*replica.Node) error {
	sh, err := w.answers(nodes[0].Status().Applied)
	if err != nil {
		return err
	}
	return w.agrees(sh, nodes)
}

func (w *watch) answers(upTo uint64) (*shadow, error) {
	sh, err := w.shadow(upTo)
	if err != nil {
		return nil, err
	}
	if err := w.executed(sh); err != nil {
		return nil, err
	}
	return sh, w.stale()
}

func (w *watch) reach() uint64 {
	var top uint64
	for i := range w.committed {
		top = max(top, i)
	}
	return top
}

func (w *watch) shadow(upTo uint64) (*shadow, error) {
	calls := w.c.Calls()
	var reads []int
	for i, call := range calls {
		if call.Op.Lookup() && call.Reply.Status == client.OK {
			reads = append(reads, i)
		}
	}
	slices.SortStableFunc(reads, func(a, b int) int { return cmp.Compare(calls[a].Reply.Time, calls[b].Reply.Time) })

	sh := newShadow(w.c.MaxSessions())
	for i := uint64(1); i <= upTo; i++ {
		term, ok := w.committed[i]
		if !ok {
			return nil, w.fail("S11", 0, "no node showed the term of entry %d when it committed", i)
		}
		e, ok := w.entries[[2]uint64{i, term}]
		if !ok {
			return nil, w.fail("S11", 0, "entry %d of term %d is committed, no MsgApp carried it", i, term)
		}
		rest, err := w.served(sh, reads, sh.ledger.Last())
		if err != nil {
			return nil, err
		}
		reads = rest
		sh.apply(e)
	}
	if _, err := w.served(sh, reads, math.MaxUint64); err != nil {
		return nil, err
	}
	return sh, nil
}

func (w *watch) served(sh *shadow, reads []int, until uint64) ([]int, error) {
	calls := w.c.Calls()
	for len(reads) > 0 && calls[reads[0]].Reply.Time <= until {
		i := reads[0]
		call := &calls[i]
		switch {
		case call.Reply.Time != sh.ledger.Last():
			return nil, w.fail("S11", 0, "c%d call %d (%s) was read at time %d, no prefix of the log ends with the ledger at that time", call.Client, i, call.Op, call.Reply.Time)
		case !bytes.Equal(lookup(sh.ledger, call), call.Reply.Body):
			return nil, w.fail("S11", 0, "c%d call %d (%s at %d) does not show what the log holds at that time", call.Client, i, call.Op, call.Reply.Time)
		}
		reads = reads[1:]
	}
	return reads, nil
}

func (w *watch) executed(sh *shadow) error {
	numbers := make(map[u128.Uint128]uint64)
	sent := 0
	for i, call := range w.c.Calls() {
		numbers[call.ID]++
		number := numbers[call.ID]
		exec, ran := sh.ran[request{client: call.ID, session: call.Session, number: number}]
		confirmed := call.Reply.Status == client.OK && !call.Op.Lookup()
		switch {
		case sh.opened[call.Session] != call.ID:
			return w.fail("S11", 0, "c%d call %d (%s #%d) names session %d, the log never opens it for this client", call.Client, i, call.Op, number, call.Session)
		case !ran && confirmed:
			return w.fail("S11", 0, "c%d call %d (%s #%d of session %d) ended OK, the log never executes it", call.Client, i, call.Op, number, call.Session)
		case !ran:
			continue
		case exec.op != call.Op || !bytes.Equal(exec.recs, call.Body):
			return w.fail("S11", 0, "c%d call %d (%s #%d of session %d) is not the request the log executes under that number at %d", call.Client, i, call.Op, number, call.Session, exec.index)
		case confirmed && (exec.status != client.OK || exec.time != call.Reply.Time || !bytes.Equal(exec.fails, call.Reply.Body)):
			return w.fail("S11", 0, "c%d call %d (%s) got time %d and failures %x, the log executes it at %d with %s, time %d and failures %x", call.Client, i, call.Op, call.Reply.Time, call.Reply.Body, exec.index, exec.status, exec.time, exec.fails)
		}
		sent++
	}
	if sent != len(sh.ran) {
		return w.fail("S11", 0, "the log executes %d requests, the clients sent %d of them", len(sh.ran), sent)
	}
	return nil
}

func (w *watch) stale() error {
	calls := w.c.Calls()
	var ended []int
	for i, call := range calls {
		if call.Reply.Status == client.OK {
			ended = append(ended, i)
		}
	}
	slices.SortStableFunc(ended, func(a, b int) int { return cmp.Compare(calls[a].End, calls[b].End) })
	over, newest := 0, -1
	for i, call := range calls {
		for ; over < len(ended) && calls[ended[over]].End < call.Start; over++ {
			if newest < 0 || calls[ended[over]].Reply.Time > calls[newest].Reply.Time {
				newest = ended[over]
			}
		}
		if newest < 0 || !call.Op.Lookup() || call.Reply.Status != client.OK || call.Reply.Time >= calls[newest].Reply.Time {
			continue
		}
		seen := calls[newest]
		return w.fail("S11", 0, "c%d call %d (%s) began at %d and shows time %d, c%d call %d (%s) had ended at %d with time %d", call.Client, i, call.Op, call.Start, call.Reply.Time, seen.Client, newest, seen.Op, seen.End, seen.Reply.Time)
	}
	return nil
}

func (w *watch) agrees(sh *shadow, nodes []*replica.Node) error {
	want := sh.ledger.Digest()
	for k, n := range nodes {
		id := uint64(k) + 1
		if d := n.LedgerDigest(); d != want {
			return w.fail("S11", id, "ledger digest %x, the log gives %x", d[:4], want[:4])
		}
		if held := n.Status().Sessions; held != len(sh.sessions) {
			return w.fail("S11", id, "%d sessions, the log leaves %d", held, len(sh.sessions))
		}
		for _, s := range sh.sessions {
			since, held := n.Session(s.client)
			switch {
			case !held:
				return w.fail("S11", id, "no session of %v, the log leaves session %d", s.client, s.since)
			case since != s.since:
				return w.fail("S11", id, "holds session %d of %v, the log leaves session %d", since, s.client, s.since)
			}
		}
	}
	return nil
}

func (sh *shadow) apply(e raft.Entry) {
	switch e.Kind {
	case entryRegister:
		sh.register(e.Index, e.Body)
	case entryRequest:
		sh.request(e.Index, e.Body)
	}
}

func (sh *shadow) register(index uint64, body []byte) {
	if len(body) != client.IDSize {
		return
	}
	id := u128.FromBytes(body)
	if sh.find(id) >= 0 {
		return
	}
	if len(sh.sessions) == sh.limit {
		oldest := 0
		for k, s := range sh.sessions {
			if s.access < sh.sessions[oldest].access {
				oldest = k
			}
		}
		sh.sessions = slices.Delete(sh.sessions, oldest, oldest+1)
	}
	sh.sessions = append(sh.sessions, session{client: id, since: index, access: index})
	sh.opened[index] = id
}

func (sh *shadow) request(index uint64, body []byte) {
	if len(body) < requestHead {
		return
	}
	req := request{
		client:  u128.FromBytes(body),
		session: binary.BigEndian.Uint64(body[client.IDSize:]),
		number:  binary.BigEndian.Uint64(body[client.IDSize+8:]),
	}
	k := sh.find(req.client)
	if k < 0 || sh.sessions[k].since != req.session || req.number <= sh.sessions[k].last {
		return
	}
	sh.sessions[k].last, sh.sessions[k].access = req.number, index
	op, ts := client.Op(body[client.IDSize+16]), binary.BigEndian.Uint64(body[client.IDSize+17:])
	sh.ran[req] = sh.execute(index, op, ts, body[requestHead:])
}

func (sh *shadow) execute(index uint64, op client.Op, ts uint64, recs []byte) execution {
	exec := execution{index: index, op: op, recs: recs, status: client.Malformed}
	if op != client.CreateAccounts && op != client.CreateTransfers {
		return exec
	}
	start := max(ts, sh.ledger.Last()+1)
	fails, err := sh.ledger.Apply(stateOp(op), ts, recs)
	if err != nil {
		return exec
	}
	exec.status, exec.time, exec.fails = client.OK, start, client.AppendFailures(nil, fails)
	return exec
}

func (sh *shadow) find(id u128.Uint128) int {
	return slices.IndexFunc(sh.sessions, func(s session) bool { return s.client == id })
}
