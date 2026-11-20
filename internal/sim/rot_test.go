package sim

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"maps"
	"math/bits"
	"math/rand/v2"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/disk"
	"github.com/Demonisreal/dmn-ledger/internal/wal"
)

const (
	vaultDir     = "/vault"
	vaultLog     = "/vault/wal"
	vaultSnaps   = "/vault/snap"
	vaultSegment = 2048
	vaultSnap    = 32
)

type vault struct {
	fsys disk.FS
	meta *wal.MetaFile
	log  *wal.WAL
	last uint64
	goal uint64
	due  bool
}

func startVault(env NodeEnv) (Process, error) {
	meta, st, err := wal.CreateMeta(env.FS, vaultDir, wal.Identity{Cluster: clusterID, Node: env.ID, Nodes: uint64(len(env.Peers))})
	if err != nil {
		return nil, err
	}
	if err := meta.Settle(); err != nil {
		return nil, err
	}
	log, err := wal.Open(env.FS, vaultLog, wal.Options{SegmentSize: vaultSegment, Nonces: env.Nonces, Salvage: true})
	if err != nil {
		return nil, err
	}
	last, err := log.LastIndex()
	if err != nil {
		return nil, err
	}
	return &vault{fsys: env.FS, meta: meta, log: log, last: last, goal: st.Sealed}, meta.Seal(last)
}

func (v *vault) Tick()       { v.due = true }
func (v *vault) Receive(Msg) {}

func (v *vault) Run() ([]Msg, error) {
	if !v.due {
		return nil, nil
	}
	v.due = false
	v.last++
	fill := byte(v.last)
	err := v.log.Append(wal.Entry{Index: v.last, Term: 1, Kind: 1, Body: bytes.Repeat([]byte{fill}, 100)})
	if err == nil {
		err = v.log.Sync()
	}
	if err == nil && v.last%vaultSnap == 0 {
		err = wal.SaveSnapshot(v.fsys, vaultSnaps, wal.SnapshotMeta{Index: v.last, Term: 1}, func(w io.Writer) error {
			_, err := w.Write(bytes.Repeat([]byte{fill}, 1500))
			return err
		})
	}
	if err == nil {
		err = v.meta.Seal(v.last)
	}
	return nil, err
}

func (v *vault) repairing() bool {
	return v.last < v.goal
}

func vaultWorld(t *testing.T, pl plan, seed uint64) (*World, *bytes.Buffer) {
	t.Helper()
	return world(t, Config{Nodes: 3, Node: startVault, Rot: rotTarget}, pl, seed)
}

func traced(trace *bytes.Buffer, what string) []string {
	var lines []string
	for line := range strings.Lines(trace.String()) {
		line = strings.TrimSuffix(line, "\n")
		if _, event, _ := strings.Cut(line, " "); strings.HasPrefix(event, what) {
			lines = append(lines, line)
		}
	}
	return lines
}

func platter(t *testing.T, d *disk.Sim) map[string][]byte {
	t.Helper()
	c := d.Clone(rand.New(rand.NewPCG(1, 1)))
	c.Crash()
	files := make(map[string][]byte)
	for _, f := range c.Durable() {
		files[f.Name] = contents(t, c, f.Name)
	}
	return files
}

func damage(t *testing.T, before, after map[string][]byte) (string, []int64) {
	t.Helper()
	if len(before) != len(after) {
		t.Fatalf("%d files on the platter, %d before the fault", len(after), len(before))
	}
	var name string
	var offs []int64
	for _, file := range slices.Sorted(maps.Keys(before)) {
		was, is := before[file], after[file]
		if len(was) != len(is) {
			t.Fatalf("%s has %d bytes, %d before the fault", file, len(is), len(was))
		}
		for off := range was {
			if was[off] == is[off] {
				continue
			}
			if name != "" && name != file {
				t.Fatalf("the fault changed %s and %s", name, file)
			}
			name = file
			offs = append(offs, int64(off))
		}
	}
	return name, offs
}

func TestRotDamagesTheNamedFile(t *testing.T) {
	cases := []struct {
		name  string
		win   window
		words string
		kill  string
	}{
		{"latent bit", window{}, "", ""},
		{"latent sector", window{sector: true}, " sector", ""},
		{"down bit", window{down: true}, " down", "power"},
		{"down sector", window{down: true, sector: true}, " down sector", "power"},
		{"meta bit", window{down: true, meta: true}, " down meta", "process"},
		{"meta sector", window{down: true, meta: true, sector: true}, " down meta sector", "process"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			win := tc.win
			win.kind, win.node, win.start, win.end = rot, 2, second, 1500*millisecond
			pl := testPlan(3, 4*second, 4*second)
			pl.windows = []window{win}
			w, trace := vaultWorld(t, pl, 11)
			for w.events[0].kind != evFault {
				w.step()
			}
			h := w.hosts[1]
			before := platter(t, h.disk)
			w.step()
			after := platter(t, h.disk)
			file, offs := damage(t, before, after)

			rots := traced(trace, "rot ")
			if len(rots) != 1 || len(offs) == 0 {
				t.Fatalf("rot lines %q, %d bytes changed", rots, len(offs))
			}
			f := strings.Fields(rots[0])
			off, _ := strconv.ParseInt(f[4], 10, 64)
			if f[0] != "1000000000" || f[2] != "n2" || f[3] != file {
				t.Fatalf("trace says %q, the platter changed in %s", rots[0], file)
			}
			switch {
			case win.sector:
				sector := off - off%disk.SectorSize
				if f[5] != "sector" || offs[0] < sector || offs[len(offs)-1] >= sector+disk.SectorSize || len(offs) < disk.SectorSize/2 {
					t.Fatalf("trace says %q, bytes %d to %d changed, %d in all", rots[0], offs[0], offs[len(offs)-1], len(offs))
				}
			default:
				if f[5] != "bit" || len(offs) != 1 || offs[0] != off || bits.OnesCount8(before[file][off]^after[file][off]) != 1 {
					t.Fatalf("trace says %q, bytes %v changed", rots[0], offs)
				}
			}

			if isMeta := filepath.Base(file) == wal.MetaName; isMeta != win.meta || isMeta && off%wal.MetaStride >= wal.MetaSlotLen {
				t.Fatalf("variant meta %v hit %s at %d", win.meta, file, off)
			}
			crashed := h.disk.Clone(rand.New(rand.NewPCG(1, 1)))
			crashed.Crash()
			m, _, err := wal.OpenMeta(crashed, vaultDir)
			if err != nil || m.Intact() == win.meta {
				t.Fatalf("variant meta %v: meta opens with %v, both slots valid %v", win.meta, err, err == nil && m.Intact())
			}

			if !h.latent || w.sick != 2 || !w.Rotted(2) || w.Rotted(1) || w.Rotted(3) {
				t.Fatalf("after the fault: latent %v, sick n%d, rotted %v %v %v", h.latent, w.sick, w.Rotted(1), w.Rotted(2), w.Rotted(3))
			}
			kills := traced(trace, "kill ")
			switch {
			case win.down && (h.proc != nil || !slices.Equal(kills, []string{"1000000000 kill n2 " + tc.kill})):
				t.Fatalf("n2 runs %v, kills %q, want one by %s", h.proc != nil, kills, tc.kill)
			case !win.down && (h.proc == nil || len(kills) != 0):
				t.Fatalf("a latent fault stopped the node: kills %q", kills)
			}

			if err := w.Run(); err != nil {
				t.Fatal(err)
			}
			span := " rot 1000000000-1500000000 n2" + tc.words + "\n"
			for _, line := range []string{"0 plan" + span, "1000000000 fault" + span, "1500000000 heal" + span} {
				if !strings.Contains(trace.String(), line) {
					t.Fatalf("no %q in the trace", line)
				}
			}
			back := "4000000000"
			if win.down {
				back = "1500000000"
			}
			starts, cures := traced(trace, "start n2"), traced(trace, "cured ")
			if !slices.Equal(starts, []string{"0 start n2", back + " start n2"}) || !slices.Equal(cures, []string{back + " cured n2"}) {
				t.Fatalf("starts %q, cures %q, want n2 back and cured at %s", starts, cures, back)
			}
			if !w.Rotted(2) || w.sick != 0 || h.latent {
				t.Fatalf("at the end: rotted %v, sick n%d, latent %v", w.Rotted(2), w.sick, h.latent)
			}
		})
	}
}

func outcomes(trace *bytes.Buffer) []string {
	var out []string
	for _, line := range traced(trace, "rot ") {
		f := strings.Fields(line)
		if strings.HasPrefix(f[3], "/") {
			f[3] = "hit"
		}
		out = append(out, f[2]+" "+f[3])
	}
	return out
}

func TestRotWaitsForTheSickNode(t *testing.T) {
	cases := []struct {
		name   string
		beyond bool
		rots   []string
		kills  []string
		cures  []string
	}{
		{
			"one at a time", false,
			[]string{"n1 hit", "n2 skipped", "n1 skipped", "n2 hit"},
			[]string{"2500000000 kill n2 power"},
			[]string{"2200000000 cured n1", "2800000000 cured n2"},
		},
		{
			"beyond the model", true,
			[]string{"n1 hit", "n2 hit", "n1 hit", "n2 hit"},
			[]string{"1300000000 kill n2 power", "2500000000 kill n2 power"},
			nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pl := testPlan(3, 4*second, 4*second)
			pl.beyond = tc.beyond
			pl.windows = []window{
				{kind: rot, node: 1, start: 1000 * millisecond, end: 1200 * millisecond},
				{kind: rot, node: 2, start: 1300 * millisecond, end: 1600 * millisecond, down: true},
				{kind: rot, node: 1, start: 1700 * millisecond, end: 1800 * millisecond},
				{kind: crash, node: 1, start: 2000 * millisecond, end: 2200 * millisecond},
				{kind: rot, node: 2, start: 2500 * millisecond, end: 2800 * millisecond, down: true},
			}
			w, trace := vaultWorld(t, pl, 21)
			runUntil(w, 3*second)
			if w.err != nil {
				t.Fatal(w.err)
			}
			if got := outcomes(trace); !slices.Equal(got, tc.rots) {
				t.Fatalf("faults %q, want %q", got, tc.rots)
			}
			if kills := traced(trace, "kill n2"); !slices.Equal(kills, tc.kills) {
				t.Fatalf("kills %q, want %q", kills, tc.kills)
			}
			if cures := traced(trace, "cured "); !slices.Equal(cures, tc.cures) {
				t.Fatalf("cures %q, want %q", cures, tc.cures)
			}
			if !w.Rotted(1) || !w.Rotted(2) || w.Rotted(3) || w.sick != 0 {
				t.Fatalf("rotted %v %v %v, sick n%d", w.Rotted(1), w.Rotted(2), w.Rotted(3), w.sick)
			}
		})
	}
}

func TestRotHitsMetaOnlyOnARunningNode(t *testing.T) {
	cases := []struct {
		name            string
		crashed, beyond bool
		meta            bool
		kill            string
	}{
		{"running", false, false, true, "process"},
		{"down", true, false, false, "power"},
		{"running, beyond the model", false, true, true, "power"},
		{"down, beyond the model", true, true, true, "power"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pl := testPlan(3, 4*second, 4*second)
			pl.beyond = tc.beyond
			pl.windows = []window{{kind: rot, node: 2, start: second, end: 1200 * millisecond, down: true, meta: true}}
			if tc.crashed {
				pl.windows = append(pl.windows, window{kind: crash, node: 2, start: 500 * millisecond, end: 1500 * millisecond})
			}
			w, trace := vaultWorld(t, pl, 8)
			runUntil(w, second)
			rots, kills := traced(trace, "rot "), traced(trace, "kill n2")
			if len(rots) != 1 || len(kills) == 0 {
				t.Fatalf("faults %q, kills %q", rots, kills)
			}
			file := strings.Fields(rots[0])[3]
			if isMeta := filepath.Base(file) == wal.MetaName; isMeta != tc.meta || kills[len(kills)-1] != "1000000000 kill n2 "+tc.kill {
				t.Fatalf("hit %s after %q, want meta %v after a kill by %s", file, kills[len(kills)-1], tc.meta, tc.kill)
			}
		})
	}
}

func TestRotHoldsTheNodeDownLikeACrash(t *testing.T) {
	pl := testPlan(3, 4*second, 4*second)
	pl.windows = []window{
		{kind: rot, node: 2, start: second, end: 1500 * millisecond, down: true},
		{kind: crash, node: 2, start: 1200 * millisecond, end: 2 * second},
		{kind: crash, node: 3, start: second, end: 2 * second},
		{kind: rot, node: 3, start: 1100 * millisecond, end: 1300 * millisecond, down: true},
	}
	w, trace := vaultWorld(t, pl, 6)
	runUntil(w, 3*second)
	if got := outcomes(trace); !slices.Equal(got, []string{"n2 hit", "n3 skipped"}) {
		t.Fatalf("faults %q", got)
	}
	for _, name := range []string{"n2", "n3"} {
		if starts := traced(trace, "start "+name); !slices.Equal(starts, []string{"0 start " + name, "2000000000 start " + name}) {
			t.Fatalf("%s started at %q, want it down until the crash window ends", name, starts)
		}
	}
}

func TestQuietScrubsLatentNodes(t *testing.T) {
	pl := testPlan(3, 2*second, 3*second)
	pl.windows = []window{{kind: rot, node: 1, start: second, end: 1100 * millisecond}}
	w, trace := world(t, Config{Nodes: 3, Node: startScribe, Rot: rotTarget}, pl, 5)
	if err := w.Run(); err != nil {
		t.Fatal(err)
	}
	_, after, _ := strings.Cut(trace.String(), " quiet\n")
	want := []string{"2000000000 kill n1 scrub", "2000000000 start n1", "2000000000 cured n1"}
	if lines := strings.SplitN(after, "\n", 4); !slices.Equal(lines[:3], want) {
		t.Fatalf("the quiet phase began with %q, want %q", lines[:3], want)
	}
	for _, h := range w.nodes()[1:] {
		if h.epoch != 0 {
			t.Fatalf("%s was restarted without a fault", h.name)
		}
	}
	rots := traced(trace, "rot ")
	if len(rots) != 1 {
		t.Fatalf("faults %q", rots)
	}
	off, _ := strconv.Atoi(strings.Fields(rots[0])[4])
	read, wrote := w.hosts[0].proc.(*scribe).before, written(16)
	if len(read) != len(wrote) {
		t.Fatalf("the scrub left %d of %d bytes: it cut the power", len(read), len(wrote))
	}
	for i := range wrote {
		if (read[i] != wrote[i]) != (i == off) {
			t.Fatalf("after the scrub byte %d differs %v, the fault sits at %d", i, read[i] != wrote[i], off)
		}
	}
}

func TestCuredOnlyAfterStartAndRepair(t *testing.T) {
	var refuse, repair bool
	node := func(env NodeEnv) (Process, error) {
		if refuse && env.ID == 2 {
			return nil, errors.New("refused")
		}
		return startVault(env)
	}
	pl := testPlan(3, 6*second, 6*second)
	pl.windows = []window{
		{kind: rot, node: 2, start: second, end: 1200 * millisecond, down: true},
		{kind: rot, node: 3, start: 3 * second, end: 3100 * millisecond},
		{kind: rot, node: 3, start: 4 * second, end: 4100 * millisecond},
	}
	cfg := Config{Nodes: 3, Node: node, Repairing: func(id uint64) bool { return repair && id == 2 }, Rot: rotTarget}
	w, trace := world(t, cfg, pl, 13)
	h := w.hosts[1]
	state := func(when string) string {
		return fmt.Sprintf("%s: n2 runs %v, latent %v, sick n%d, cures %q, faults %q", when, h.proc != nil, h.latent, w.sick, traced(trace, "cured "), outcomes(trace))
	}

	refuse = true
	runUntil(w, 1500*millisecond)
	if h.proc != nil || !h.latent || w.sick != 2 || len(traced(trace, "fail n2")) == 0 {
		t.Fatal(state("start refused"))
	}

	refuse, repair = false, true
	runUntil(w, 3500*millisecond)
	if h.proc == nil || h.latent || w.sick != 2 || len(traced(trace, "cured ")) != 0 || !slices.Equal(outcomes(trace), []string{"n2 hit", "n3 skipped"}) {
		t.Fatal(state("in repair"))
	}

	repair = false
	runUntil(w, 3500*millisecond+2*tick)
	if cures := traced(trace, "cured "); w.sick != 0 || len(cures) != 1 || !strings.HasSuffix(cures[0], " cured n2") {
		t.Fatal(state("repaired"))
	}

	runUntil(w, 5*second)
	if !slices.Equal(outcomes(trace), []string{"n2 hit", "n3 skipped", "n3 hit"}) {
		t.Fatal(state("after the cure"))
	}
}

func clutter(names ...string) func(NodeEnv) (Process, error) {
	return func(env NodeEnv) (Process, error) {
		if err := env.FS.MkdirAll("/c"); err != nil {
			return nil, err
		}
		for _, name := range names {
			f, err := env.FS.Create("/c/" + name)
			if err != nil {
				return nil, err
			}
			if name == "empty" {
				continue
			}
			if _, err := f.WriteAt(make([]byte, wal.MetaStride+wal.MetaSlotLen), 0); err != nil {
				return nil, err
			}
			if err := f.Sync(); err != nil {
				return nil, err
			}
		}
		return &sender{}, env.FS.SyncDir("/c")
	}
}

func TestRotSparesWhatIsNoData(t *testing.T) {
	all := []string{"snap.tmp", "empty", "meta", "plain"}
	cases := []struct {
		name  string
		files []string
		win   window
		want  string
	}{
		{"temporary file", []string{"snap.tmp"}, window{}, "nothing"},
		{"empty file", []string{"empty"}, window{}, "nothing"},
		{"meta outside its variant", []string{"meta"}, window{down: true}, "nothing"},
		{"meta variant without meta", []string{"plain"}, window{down: true, meta: true}, "nothing"},
		{"no file", nil, window{}, "nothing"},
		{"plain file among the others", all, window{}, "/c/plain"},
		{"meta among the others", all, window{down: true, meta: true}, "/c/meta"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for seed := range uint64(20) {
				win := tc.win
				win.kind, win.node, win.start, win.end = rot, 1, second, 1500*millisecond
				pl := testPlan(1, 4*second, 4*second)
				pl.windows = []window{win}
				w, trace := world(t, Config{Nodes: 1, Node: clutter(tc.files...), Rot: rotTarget}, pl, seed)
				runUntil(w, second)
				rots := traced(trace, "rot ")
				if len(rots) != 1 || strings.Fields(rots[0])[3] != tc.want {
					t.Fatalf("seed %d: faults %q, want %s", seed, rots, tc.want)
				}
				if hit := tc.want != "nothing"; w.Rotted(1) != hit || w.hosts[0].latent != hit || (w.sick == 1) != hit {
					t.Fatalf("seed %d: rotted %v, latent %v, sick n%d after %q", seed, w.Rotted(1), w.hosts[0].latent, w.sick, rots[0])
				}
				runUntil(w, 2*second)
				if w.hosts[0].proc == nil {
					t.Fatalf("seed %d: the node is still down after the window", seed)
				}
			}
		})
	}
}

func TestRotAimsAtTheLogTail(t *testing.T) {
	w, _ := vaultWorld(t, testPlan(3, 10*second, 10*second), 17)
	h := w.hosts[0]
	tail := func() disk.DurableFile {
		var seg disk.DurableFile
		for _, f := range h.disk.Durable() {
			if strings.HasPrefix(filepath.Base(f.Name), wal.SegmentPrefix) {
				seg = f
			}
		}
		return seg
	}
	for h.proc.(*vault).last < 100 || tail().Size < 1500 {
		w.step()
	}
	seg := tail()
	sizes := make(map[string]int64)
	for _, f := range h.disk.Durable() {
		sizes[f.Name] = f.Size
	}

	const draws = 4000
	var inSeg, inTail int
	for range draws {
		name, off, ok := rotTarget(h.disk.Durable(), false, h.rng)
		size, known := sizes[name]
		if !ok || !known || off < 0 || off >= size || filepath.Base(name) == wal.MetaName {
			t.Fatalf("target %s at %d, found %v, file has %d bytes", name, off, ok, size)
		}
		if name == seg.Name {
			inSeg++
			if off >= seg.Size-rotTail {
				inTail++
			}
		}
	}
	if inSeg < draws*45/100 || inSeg > draws*65/100 {
		t.Fatalf("%d of %d faults in the last segment of %d files, want about half", inSeg, draws, len(sizes))
	}
	if inTail < inSeg*45/100 || inTail > inSeg*70/100 {
		t.Fatalf("%d of %d faults in the last segment hit its last %d of %d bytes, want about half", inTail, inSeg, rotTail, seg.Size)
	}

	var slots [2]int
	for range draws / 10 {
		name, off, ok := rotTarget(h.disk.Durable(), true, h.rng)
		if !ok || filepath.Base(name) != wal.MetaName || off%wal.MetaStride >= wal.MetaSlotLen || off >= sizes[name] {
			t.Fatalf("meta target %s at %d, found %v", name, off, ok)
		}
		slots[off/wal.MetaStride]++
	}
	if slots[0] < draws/40 || slots[1] < draws/40 {
		t.Fatalf("meta faults per slot %v, want both hit", slots)
	}
}

func rotRun(t *testing.T, seed uint64, p Profile) (*World, string) {
	t.Helper()
	var trace bytes.Buffer
	var w *World
	repairing := func(id uint64) bool {
		v, ok := w.hosts[id-1].proc.(*vault)
		return ok && v.repairing()
	}
	nodes := uint64(3)
	if seed%5 == 4 {
		nodes = 5
	}
	w, err := New(Config{
		Seed:      seed,
		Profile:   p,
		Nodes:     nodes,
		Node:      startVault,
		Repairing: repairing,
		Rot:       rotTarget,
		Trace:     &trace,
		Check: func() error {
			if w.sick == 0 {
				return nil
			}
			if h := w.hosts[w.sick-1]; h.proc != nil && !h.latent && !repairing(h.id) {
				return fmt.Errorf("%s runs repaired and still counts as sick", h.name)
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Run(); err != nil {
		t.Fatal(err)
	}
	return w, trace.String()
}

func TestRotRunsReplay(t *testing.T) {
	for seed := range seedCount(100) {
		for _, p := range []Profile{ProfileRot, ProfileBeyond} {
			t.Run(fmt.Sprintf("%s-%d", p, seed), func(t *testing.T) {
				t.Parallel()
				w, trace := rotRun(t, seed, p)
				again, replay := rotRun(t, seed, p)
				if replay == trace && again.TraceHash() == w.TraceHash() {
					return
				}
				a, b := strings.Split(trace, "\n"), strings.Split(replay, "\n")
				for i := range min(len(a), len(b)) {
					if a[i] != b[i] {
						t.Fatalf("line %d is %q, then %q", i+1, a[i], b[i])
					}
				}
				t.Fatalf("%d lines, then %d", len(a), len(b))
			})
		}
	}
}

func TestRotKeepsItsRulesInPlannedRuns(t *testing.T) {
	t.Parallel()
	counts := make(tally)
	targets := make(map[string]bool)
	for seed := range seedCount(200) {
		_, trace := rotRun(t, seed, ProfileRot)
		var sick, last string
		quiet := false
		for line := range strings.Lines(trace) {
			_, event, _ := strings.Cut(strings.TrimSuffix(line, "\n"), " ")
			f := strings.Fields(event)
			switch {
			case f[0] == "quiet":
				quiet = true
			case f[0] == "kill" && f[2] == "scrub":
				counts["scrub"]++
			case f[0] == "cured" && f[1] != sick:
				t.Fatalf("seed %d: %q while %q is sick", seed, event, sick)
			case f[0] == "cured" && (strings.HasPrefix(last, "start ") || strings.HasPrefix(last, "arm ")):
				sick = ""
				counts["cured at the start"]++
			case f[0] == "cured":
				sick = ""
				counts["cured after a repair"]++
			case f[0] == "rot" && len(f) == 3:
				counts[f[2]]++
			case f[0] == "rot":
				meta := filepath.Base(f[2]) == wal.MetaName
				switch {
				case quiet || sick != "":
					t.Fatalf("seed %d: %q with the quiet phase begun %v and %q sick", seed, event, quiet, sick)
				case meta && last != "kill "+f[1]+" process":
					t.Fatalf("seed %d: %q after %q, want a process stop", seed, event, last)
				case meta:
					counts["meta"]++
				}
				sick = f[1]
				counts[f[4]]++
				targets[f[2]+" "+f[3]] = true
			}
			last = event
		}
	}
	for _, kind := range []string{"bit", "sector", "meta", "skipped", "nothing", "scrub", "cured at the start", "cured after a repair"} {
		if counts[kind] == 0 {
			t.Errorf("no %q in the runs: %v", kind, counts)
		}
	}
	if faults := counts["bit"] + counts["sector"]; len(targets) < faults*9/10 {
		t.Errorf("%d faults hit only %d different places", faults, len(targets))
	}
}

func TestBeyondDropsTheRules(t *testing.T) {
	t.Parallel()
	var together, metas int
	for seed := range seedCount(100) {
		_, trace := rotRun(t, seed, ProfileBeyond)
		unstarted := make(map[string]bool)
		var last string
		for line := range strings.Lines(trace) {
			_, event, _ := strings.Cut(strings.TrimSuffix(line, "\n"), " ")
			f := strings.Fields(event)
			switch {
			case f[0] == "cured" || f[0] == "rot" && f[2] == "skipped":
				t.Fatalf("seed %d: %q beyond the model", seed, event)
			case f[0] == "start":
				delete(unstarted, f[1])
			case f[0] == "rot" && len(f) == 5:
				if filepath.Base(f[2]) == wal.MetaName {
					if last != "kill "+f[1]+" power" {
						t.Fatalf("seed %d: %q after %q, want a power loss", seed, event, last)
					}
					metas++
				}
				unstarted[f[1]] = true
				if len(unstarted) > 1 {
					together++
				}
			}
			last = event
		}
	}
	if together == 0 || metas == 0 {
		t.Fatalf("%d faults while another damaged node had not restarted, %d in meta; want both", together, metas)
	}
}

func TestRotPlans(t *testing.T) {
	const seeds = 2000
	var windows, rots, downs, sectors, metas int
	inAll, inRot := make(map[fault]bool), make(map[fault]bool)
	for seed := range uint64(seeds) {
		pl, err := newPlan(rand.New(rand.NewPCG(seed, 1)), ProfileRot, 3, 6)
		if err != nil {
			t.Fatal(err)
		}
		beyond, err := newPlan(rand.New(rand.NewPCG(seed, 1)), ProfileBeyond, 3, 6)
		if err != nil {
			t.Fatal(err)
		}
		if pl.beyond || !beyond.beyond || !pl.rearm {
			t.Fatalf("seed %d: rot beyond %v rearm %v, beyond %v", seed, pl.beyond, pl.rearm, beyond.beyond)
		}
		beyond.beyond = false
		if !reflect.DeepEqual(pl, beyond) {
			t.Fatalf("seed %d: beyond plans other faults than rot", seed)
		}
		all, err := newPlan(rand.New(rand.NewPCG(seed, 1)), ProfileAll, 3, 6)
		if err != nil {
			t.Fatal(err)
		}
		if pl.quiet != all.quiet || pl.loss != all.loss || pl.dup != all.dup || !slices.Equal(pl.journal, all.journal) {
			t.Fatalf("seed %d: rot starts from another world than all", seed)
		}
		for _, win := range all.windows {
			inAll[win.kind] = true
		}
		for _, win := range pl.windows {
			windows++
			inRot[win.kind] = true
			switch {
			case win.end >= pl.quiet:
				t.Fatalf("seed %d: %s window open until %d, the quiet phase begins at %d", seed, win.kind, win.end, pl.quiet)
			case win.kind != rot && (win.down || win.sector || win.meta):
				t.Fatalf("seed %d: %s window with a rot variant", seed, win.kind)
			case win.kind != rot:
				continue
			case win.meta && !win.down:
				t.Fatalf("seed %d: meta without down", seed)
			}
			rots++
			if win.down {
				downs++
			}
			if win.sector {
				sectors++
			}
			if win.meta {
				metas++
			}
		}
	}
	for kind := range inAll {
		if !inRot[kind] {
			t.Errorf("rot never plans %s, all does", kind)
		}
	}
	for _, share := range []struct {
		what        string
		part, whole int
		least, most int
	}{
		{"rot windows", rots, windows, 22, 28},
		{"down", downs, rots, 45, 55},
		{"sector", sectors, rots, 20, 30},
		{"meta of down", metas, downs, 28, 39},
	} {
		if pct := share.part * 100 / share.whole; pct < share.least || pct > share.most {
			t.Errorf("%s: %d of %d, %d%%, want %d%% to %d%%", share.what, share.part, share.whole, pct, share.least, share.most)
		}
	}

	for _, p := range []Profile{ProfileCalm, ProfileNet, ProfileCrash, ProfileClock, ProfileAll, ProfileLeaders, ProfileSplit} {
		for seed := range uint64(200) {
			pl, err := newPlan(rand.New(rand.NewPCG(seed, 1)), p, 3, 6)
			if err != nil {
				t.Fatal(err)
			}
			if pl.beyond || slices.ContainsFunc(pl.windows, func(win window) bool { return win.kind == rot }) {
				t.Fatalf("profile %s seed %d plans rot", p, seed)
			}
		}
	}
}

func TestRotNeedsThreeNodes(t *testing.T) {
	for _, p := range []Profile{ProfileRot, ProfileBeyond} {
		for nodes := uint64(1); nodes <= 3; nodes++ {
			_, err := New(Config{Profile: p, Nodes: nodes, Node: startVault, Rot: rotTarget})
			if (err == nil) != (nodes == 3) {
				t.Errorf("profile %s with %d nodes: %v", p, nodes, err)
			}
		}
	}
}

func TestRotNeedsATarget(t *testing.T) {
	for _, p := range Profiles() {
		_, err := New(Config{Profile: p, Nodes: 3, Node: startVault})
		if (err == nil) != (p != ProfileRot && p != ProfileBeyond) {
			t.Errorf("profile %s without Config.Rot: %v", p, err)
		}
	}
}

func TestBeyondShredsEveryDisk(t *testing.T) {
	for _, tc := range []struct {
		p    Profile
		rate float64
	}{{ProfileBeyond, shredRate}, {ProfileRot, 0}, {ProfileAll, 0}} {
		w, err := New(Config{Seed: 3, Profile: tc.p, Nodes: 3, Node: startVault, Rot: rotTarget})
		if err != nil {
			t.Fatal(err)
		}
		for _, h := range w.nodes() {
			if got := w.Disk(h.id).ShredRate; got != tc.rate {
				t.Errorf("profile %s: %s shreds at %v, want %v", tc.p, h.name, got, tc.rate)
			}
		}
	}
}
