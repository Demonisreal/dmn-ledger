package demo

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

const testOrigin = "https://demo.example.org"

var testProxies = []netip.Prefix{netip.MustParsePrefix("10.0.0.0/24"), netip.MustParsePrefix("fd00::/64")}

type actCall struct {
	kind   Kind
	target int
	from   netip.Prefix
}

type fakeShow struct {
	mu       sync.Mutex
	st       State
	lines    []Line
	moving   bool
	actErr   error
	until    time.Time
	acts     []actCall
	states   int
	receipt  Proof
	proofErr error
	proofs   int
	held     chan struct{}
	watching int
	alive    bool
}

func newFake() *fakeShow {
	f := &fakeShow{st: sampleState(), receipt: sampleProof(), alive: true}
	for range 100 {
		f.write("node 2 of 3 in cluster 13911521510627622913 started")
	}
	return f
}

func (f *fakeShow) Act(kind Kind, target int, from netip.Prefix) (time.Time, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acts = append(f.acts, actCall{kind, target, from})
	if f.actErr != nil {
		return f.until, f.actErr
	}
	f.st.Action = &Action{Kind: kind, Target: target, Phase: "struck", Began: time.Now().UnixMilli()}
	f.st.Rev++
	return time.Time{}, nil
}

func (f *fakeShow) State() State {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.states++
	if f.moving {
		f.st.Rev++
	}
	st := f.st
	st.Now = time.Now().UnixMilli()
	st.Recent = slices.Clone(f.st.Recent)
	st.Totals.Findings = slices.Clone(f.st.Totals.Findings)
	if f.st.Action != nil {
		a := *f.st.Action
		st.Action = &a
	}
	return st
}

func (f *fakeShow) Lines(after uint64) []Line {
	f.mu.Lock()
	defer f.mu.Unlock()
	var newer []Line
	for _, l := range f.lines {
		if l.N > after {
			newer = append(newer, l)
		}
	}
	return newer
}

func (f *fakeShow) Proof(context.Context) (Proof, error) {
	f.mu.Lock()
	f.proofs++
	held := f.held
	f.mu.Unlock()
	if held != nil {
		<-held
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.receipt, f.proofErr
}

func (f *fakeShow) Watching(streams int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.watching = streams
}

func (f *fakeShow) Alive() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.alive
}

func (f *fakeShow) write(text string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lines = append(f.lines, Line{N: uint64(len(f.lines) + 1), At: time.Now().UnixMilli(), Src: "n2", Text: text})
}

func (f *fakeShow) change(edit func(st *State)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	edit(&f.st)
	f.st.Rev++
}

func (f *fakeShow) calls() []actCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.acts)
}

func (f *fakeShow) counted() (states, proofs, watching int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.states, f.proofs, f.watching
}

func sampleState() State {
	st := State{
		Rev: 1, Now: 1795036492202, Started: 1795030000000, Phase: "steady", Load: "running",
		Rest:    Rest{Since: 1795035000000, Until: 1795035030000},
		Viewers: 12,
		Cluster: Cluster{N: 3, ID: "c1057e1200000001", Since: 1795034000000, Why: "limit", Limit: 20000},
		Books: Books{
			Confirmed: 18734, Refused: 211, Open: 2, OpenSince: 1795036492100,
			Sum: Sum{At: 1795036491000, Value: "6000000", Want: "6000000"},
			Recount: Recount{
				At: 1795036480000, After: Kill, Confirmed: 18650, Found: 18650, Refused: 210, Strays: 0,
				Index: 4521, Hash: "a594ed26ac5edbed",
			},
			Judged: Judged{Windows: 1170, Calls: 18720, Upto: "18712", At: 1795036490000},
		},
		Action: &Action{
			Kind: Flip, Target: 2, Node: 2, Role: "follower", Term: 7, Phase: "done", Began: 1795036400000,
			LiftAt: 1795036400500, Ended: 1795036402300, File: "wal/00000000000000000001.seg", Size: 1048576,
			Offset: 393216, Bit: 5, Before: 0x3c, After: 0x1c, Repair: "yes", NonVoter: 77,
		},
		Next: 1795036410000,
		Totals: Totals{
			Clusters: 3, Confirmed: 58734, Windows: 3671, Actions: Actions{Kill: 14, Freeze: 9, Cut: 11, Flip: 6},
			Healed: 40, Found: 1,
			Findings: []Finding{{At: 1795032000000, Check: "F1", Reason: "node 3 exited: signal: killed", ResetAt: 1795032030000, Evidence: "finding-1795032000000"}},
		},
	}
	for i := range st.Nodes {
		st.Nodes[i] = Node{
			ID: i + 1, Pid: 41230 + i, Since: 1795034000000, Proc: "up", Answers: true, Role: "follower", Term: 7, Leader: 1,
			Commit: 4521, Applied: 4521, Last: 4521, Apply: "a594ed26ac5edbed", HeadHash: "671c96d80dee258f", Head: 4519,
			Key:  "244358c694263d7bbb801427b3c4bd819134051291e3b3da8df63ce5f27b8fd5",
			Note: Note{At: 1795036402300, Text: "Bit gekippt, 77 ms Nicht-Wähler"},
		}
	}
	st.Nodes[0].Role = "leader"
	i := 0
	for from := 1; from <= 3; from++ {
		for to := 1; to <= 3; to++ {
			if from != to {
				st.Links[i] = Link{From: from, To: to}
				i++
			}
		}
	}
	for i := range st.Books.Sum.Accounts {
		st.Books.Sum.Accounts[i] = Account{ID: i + 1, Balance: "1000000"}
	}
	for i := range 5 {
		st.Recent = append(st.Recent, Booking{ID: "18734", Debit: 1 + i, Credit: 6 - i, Amount: "12345", At: 1795036492000, Refused: "exceeds_credits"})
	}
	return st
}

func sampleProof() Proof {
	return Proof{
		At: 1795036492202, Cluster: "c1057e1200000001",
		Keys:     [3]string{strings.Repeat("ab", 32), strings.Repeat("cd", 32), strings.Repeat("ef", 32)},
		Transfer: Booking{ID: "18734", Debit: 1, Credit: 2, Amount: "12345", At: 1795036492000},
		Receipt:  "0102", Base64: "AQI=",
		Head: Head{Seq: 4521, Hash: "a594ed26ac5edbedafd69fbe842ad84d643c76fd7234eef0b3bd0b3622397386", Signer: 2},
	}
}

func hit(web *Web, method, target string, headers ...string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, target, nil)
	for i := 0; i+1 < len(headers); i += 2 {
		r.Header.Add(headers[i], headers[i+1])
	}
	w := httptest.NewRecorder()
	web.ServeHTTP(w, r)
	return w
}

type pipeListener struct {
	conns chan net.Conn
	done  chan struct{}
	once  sync.Once
}

type visitorConn struct {
	net.Conn
	from net.Addr
}

func (c visitorConn) RemoteAddr() net.Addr { return c.from }

func (l *pipeListener) Accept() (net.Conn, error) {
	select {
	case conn := <-l.conns:
		return conn, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *pipeListener) Close() error {
	l.once.Do(func() { close(l.done) })
	return nil
}

func (l *pipeListener) Addr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 8080}
}

func serveWeb(t *testing.T, show *fakeShow) *pipeListener {
	l := &pipeListener{conns: make(chan net.Conn), done: make(chan struct{})}
	web := NewWeb(show, testOrigin, testProxies)
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- web.Serve(ctx, l) }()
	t.Cleanup(func() {
		cancel()
		if err := <-served; err != nil {
			t.Errorf("serve: %v", err)
		}
	})
	return l
}

func (l *pipeListener) connect(from string) (net.Conn, bool) {
	near, far := net.Pipe()
	select {
	case l.conns <- visitorConn{Conn: far, from: net.TCPAddrFromAddrPort(netip.MustParseAddrPort(from))}:
		return near, true
	case <-l.done:
		_ = near.Close()
		return nil, false
	}
}

type pipeClient struct {
	conn net.Conn
	br   *bufio.Reader
}

func (l *pipeListener) open(t *testing.T, from string) *pipeClient {
	t.Helper()
	conn, ok := l.connect(from)
	if !ok {
		t.Fatal("the listener is closed")
	}
	t.Cleanup(func() { _ = conn.Close() })
	return &pipeClient{conn: conn, br: bufio.NewReader(conn)}
}

func request(method, target string, headers ...string) string {
	var b strings.Builder
	b.WriteString(method + " " + target + " HTTP/1.1\r\nHost: demo.example.org\r\n")
	for i := 0; i+1 < len(headers); i += 2 {
		b.WriteString(headers[i] + ": " + headers[i+1] + "\r\n")
	}
	b.WriteString("\r\n")
	return b.String()
}

func (c *pipeClient) send(raw string) {
	go func() { _, _ = io.WriteString(c.conn, raw) }()
}

func (c *pipeClient) read(t *testing.T) *http.Response {
	t.Helper()
	res, err := http.ReadResponse(c.br, nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return res
}

func (c *pipeClient) do(t *testing.T, raw string) (*http.Response, string) {
	t.Helper()
	c.send(raw)
	res := c.read(t)
	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return res, string(body)
}

func (c *pipeClient) ended() bool {
	_, err := c.br.ReadByte()
	return err != nil
}

type stream struct {
	*pipeClient
	res  *http.Response
	body *bufio.Reader
}

type sse struct {
	name   string
	data   string
	fields []string
}

func (l *pipeListener) watch(t *testing.T, from string, headers ...string) *stream {
	t.Helper()
	c := l.open(t, from)
	c.send(request("GET", "/events", headers...))
	res := c.read(t)
	return &stream{pipeClient: c, res: res, body: bufio.NewReader(res.Body)}
}

func (s *stream) next() (sse, error) {
	var ev sse
	for {
		line, err := s.body.ReadString('\n')
		if err != nil {
			return ev, err
		}
		line = strings.TrimSuffix(line, "\n")
		if line == "" {
			return ev, nil
		}
		ev.fields = append(ev.fields, line)
		if name, ok := strings.CutPrefix(line, "event: "); ok {
			ev.name = name
		}
		if js, ok := strings.CutPrefix(line, "data: "); ok {
			ev.data = js
		}
	}
}

func (s *stream) event(t *testing.T) sse {
	t.Helper()
	ev, err := s.next()
	if err != nil {
		t.Fatalf("read event: %v", err)
	}
	return ev
}

func (s *stream) drain() <-chan time.Time {
	ended := make(chan time.Time, 1)
	go func() {
		_, _ = io.Copy(io.Discard, s.body)
		ended <- time.Now()
	}()
	return ended
}
