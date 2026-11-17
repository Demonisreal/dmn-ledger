package report

import (
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Demonisreal/dmn-ledger/bench/raw"
)

func TestProfileShares(t *testing.T) {
	text, err := os.ReadFile("testdata/ledgerd.traces")
	if err != nil {
		t.Fatal(err)
	}
	s, err := countTraces(text)
	if err != nil {
		t.Fatal(err)
	}
	if s.total != 58*time.Millisecond {
		t.Fatalf("total %v, pprof says 58ms", s.total)
	}
	var sum float64
	var by time.Duration
	for _, p := range posts {
		sum += s.of(p)
		by += s.by[p]
	}
	if by != s.total || sum < 0.9999 || sum > 1.0001 {
		t.Fatalf("posts add up to %v and %.4f of %v", by, sum, s.total)
	}
	if rest := s.of(postRest); rest >= 0.02 {
		t.Errorf("%.1f %% of the samples are rest: %v", 100*rest, s.by)
	}
	want := map[string]time.Duration{postKernel: 16, postLog: 14, postWire: 12, postNode: 8, postRuntime: 8}
	for _, p := range posts {
		if s.by[p] != want[p]*time.Millisecond {
			t.Errorf("%s %v, want %dms", p, s.by[p], want[p])
		}
	}
	if top := s.top(3); strings.Join(top, ",") != postKernel+","+postLog+","+postWire {
		t.Errorf("largest three %q", top)
	}
}

func TestEachSampleHasOnePost(t *testing.T) {
	const m = module + "internal/"
	for _, c := range []struct {
		want  string
		stack []string
	}{
		{postChain, []string{"crypto/internal/fips140/sha256.blockAMD64", "crypto/sha256.(*Digest).Write", m + "state.(*Machine).seal", m + "state.(*Machine).Apply", m + "replica.(*Node).apply"}},
		{postChain, []string{"crypto/sha256.Sum256", m + "audit.Leaf", m + "state.(*Machine).link"}},
		{postHash, []string{"crypto/internal/fips140/sha256.blockAMD64", "hash.Hash.Write", m + "replica.(*Node).apply", m + "replica.(*Node).applyTo"}},
		{postNode, []string{"crypto/sha256.(*Digest).Sum", m + "replica.(*Node).Digest"}},
		{postApply, []string{"runtime.mallocgc", m + "state.(*Machine).createTransfer", m + "state.(*Machine).Apply", m + "replica.(*Node).apply"}},
		{postLog, []string{"syscall.Fsync", "os.(*File).Sync", "main.(*syncs).timed", "main.(*countedFile).Sync", m + "wal.(*WAL).Sync", m + "replica.(*Node).persist"}},
		{postLog, []string{"hash/crc32.ieeeCLMUL", "hash/crc32.Update", m + "server.(*Server).fill"}},
		{postWire, []string{"syscall.write", "net.(*conn).Write", m + "server.(*clientConn).write"}},
		{postWire, []string{m + "wire.ReadFrame", m + "server.(*Server).readClient"}},
		{postWire, []string{"runtime.memmove", m + "server.closeOnStop.func2", m + "server.(*Server).serveClient"}},
		{postRaft, []string{"runtime.growslice", m + "raft.(*Raft).Step", m + "replica.(*Node).Process"}},
		{postNode, []string{"runtime.selectgo", m + "server.(*Server).loop", m + "server.(*Server).Run", "main.serve.func1"}},
		{postNode, []string{m + "server.(*Server).answer", m + "server.(*Server).loop"}},
		{postRuntime, []string{"runtime.scanobject", "runtime.gcDrain", "runtime.gcBgMarkWorker"}},
		{postKernel, []string{"internal/runtime/syscall/linux.Syscall6", "internal/runtime/syscall/linux.EpollWait", "runtime.netpoll", "runtime.findRunnable"}},
		{postKernel, []string{"runtime.futex", "runtime.futexsleep", "runtime.stopm"}},
		{postRest, []string{"main.statsLine", "main.serve"}},
	} {
		if got := postOf(c.stack); got != c.want {
			t.Errorf("%s: %s, want %s", c.stack[0], got, c.want)
		}
	}
}

func TestTracesAreRead(t *testing.T) {
	text := "File: ledgerd\nType: cpu\n-----------+-------\n" +
		"     1.50s   " + module + "internal/state.(*Machine).Apply\n" +
		"             " + module + "internal/replica.(*Node).apply (inline)\n" +
		"-----------+-------\n" +
		"    thread:  main\n" +
		"     500ms   runtime.gcBgMarkWorker\n" +
		"-----------+-------\n"
	s, err := countTraces([]byte(text))
	if err != nil {
		t.Fatal(err)
	}
	if s.total != 2*time.Second || s.by[postApply] != 1500*time.Millisecond || s.by[postRuntime] != 500*time.Millisecond {
		t.Fatalf("%v of %v", s.by, s.total)
	}
	for _, broken := range []string{"", "File: ledgerd\n", "-----------+---\n     lots   runtime.main\n", "-----------+---\nruntime.main\n"} {
		if _, err := countTraces([]byte(broken)); !errors.Is(err, ErrTraces) {
			t.Errorf("%q: %v", broken, err)
		}
	}
}

func TestLoopShares(t *testing.T) {
	part := func(role string, up, wait, process, logNs, metaNs uint64) raw.Part {
		return raw.Part{Name: role, Role: role, Counters: map[string]uint64{"up": up, "wait": wait, "process": process, "logsync_ns": logNs, "metasync_ns": metaNs, "othersync_ns": 0}}
	}
	f := &raw.File{
		Before: []raw.Part{part("leader", 0, 0, 0, 0, 0)},
		Seconds: []raw.Second{
			{Parts: []raw.Part{part("leader", 1e9, 5e8, 3e8, 1e8, 1e8)}},
			{Parts: []raw.Part{part("leader", 2e9, 6e8, 9e8, 4e8, 3e8)}},
			{Parts: []raw.Part{part("leader", 3e9, 7e8, 1e9, 4e8, 3e8)}},
		},
	}
	loops := loopsOf(f, func(i int) bool { return i == 1 })
	s := loops["leader"].shares()
	want := map[string]float64{loopLog: 0.3, loopMeta: 0.2, loopOther: 0, loopCompute: 0.1, loopIO: 0.3, loopWait: 0.1}
	var sum float64
	for _, p := range loopPosts {
		sum += s[p]
		if d := s[p] - want[p]; d > 1e-9 || d < -1e-9 {
			t.Errorf("%s %.3f, want %.3f", p, s[p], want[p])
		}
	}
	if sum < 0.9999 || sum > 1.0001 {
		t.Errorf("shares add up to %.4f", sum)
	}
	f.Seconds[1].Parts[0].Counters = map[string]uint64{"up": 2e9}
	if l := loopsOf(f, func(i int) bool { return i == 1 }); len(l) != 0 {
		t.Errorf("a part without loop counters counted: %v", l)
	}
}
