package report

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"slices"
	"strings"

	"github.com/Demonisreal/dmn-ledger/bench/raw"
)

const (
	PlanFile   = "plan.json"
	PlanSchema = 1

	WhyUnplanned = "not in " + PlanFile
)

var ErrPlanSchema = errors.New("report: unknown schema of " + PlanFile)

type Planned struct {
	Schema int      `json:"schema"`
	Expect []Expect `json:"expect"`
}

type Expect struct {
	Measurement string            `json:"measurement"`
	Kind        string            `json:"kind"`
	Nodes       int               `json:"nodes"`
	Variant     string            `json:"variant"`
	Lanes       int               `json:"lanes"`
	Workload    string            `json:"workload"`
	Rate        int               `json:"rate"`
	Passes      int               `json:"passes"`
	Env         map[string]string `json:"env"`
	Tags        []string          `json:"tags"`
}

func WritePlan(w io.Writer, p Planned) error {
	p.Schema = PlanSchema
	b, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return err
	}
	_, err = w.Write(append(b, '\n'))
	return err
}

func readPlan(fsys fs.FS) (Planned, error) {
	var p Planned
	b, err := fs.ReadFile(fsys, PlanFile)
	if err != nil {
		return p, err
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&p); err != nil {
		return p, fmt.Errorf("report: %s: %w", PlanFile, err)
	}
	if p.Schema != PlanSchema {
		return p, fmt.Errorf("%w: %d", ErrPlanSchema, p.Schema)
	}
	return p, nil
}

func (e Expect) fits(g *group) (int, bool) {
	t := g.target
	if e.Measurement != g.m || e.Kind != t.Kind || e.Nodes != t.Nodes || !strings.EqualFold(e.Variant, t.Variant) ||
		e.Lanes != t.Lanes || e.Workload != g.workload || e.Rate != 0 && e.Rate != g.rate || !slices.Equal(e.Tags, g.tags) {
		return 0, false
	}
	for k, v := range e.Env {
		if t.Env[k] != v {
			return 0, false
		}
	}
	return len(e.Env), true
}

func (e Expect) subject() string {
	t := raw.Target{Kind: e.Kind, Nodes: e.Nodes, Variant: e.Variant}
	s := fmt.Sprintf("%s %s, %d lanes, %s", e.Measurement, name(t), e.Lanes, e.Workload)
	if e.Rate > 0 {
		s += " at " + grouped(float64(e.Rate)) + "/s"
	}
	for _, k := range slices.Sorted(maps.Keys(e.Env)) {
		s += ", " + k + "=" + e.Env[k]
	}
	return s
}

func complete(gs []*group, written fs.FS) []Verdict {
	p, err := readPlan(written)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return []Verdict{{Subject: PlanFile, Why: []string{WhyMissing}}}
	case err != nil:
		return []Verdict{{Subject: PlanFile, Why: []string{err.Error()}}}
	}
	passes := make([]map[int]bool, len(p.Expect))
	for i := range passes {
		passes[i] = make(map[int]bool)
	}
	var verdicts []Verdict
	for _, g := range gs {
		best, most := -1, -1
		for i, e := range p.Expect {
			if n, ok := e.fits(g); ok && n > most {
				best, most = i, n
			}
		}
		if best < 0 {
			verdicts = append(verdicts, Verdict{Subject: g.subject(), Why: []string{WhyUnplanned}})
			continue
		}
		for _, r := range g.runs {
			passes[best][r.f.Pass] = true
		}
	}
	for i, e := range p.Expect {
		for pass := 1; pass <= e.Passes; pass++ {
			if !passes[i][pass] {
				verdicts = append(verdicts, Verdict{Subject: e.subject(), Why: []string{fmt.Sprintf("%s: no run in pass %d", WhyMissing, pass)}})
			}
		}
	}
	if len(verdicts) == 0 {
		verdicts = append(verdicts, Verdict{Subject: PlanFile, Passed: true, Why: []string{fmt.Sprintf("%d entries, every pass has its runs", len(p.Expect))}})
	}
	return verdicts
}
