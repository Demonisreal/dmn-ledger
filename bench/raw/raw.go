package raw

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/Demonisreal/dmn-ledger/bench/control"
	"github.com/Demonisreal/dmn-ledger/bench/load"
)

const Schema = 2

var ErrSchema = errors.New("raw: unknown schema")

type Build struct {
	Commit string   `json:"commit"`
	Dirty  bool     `json:"dirty"`
	Tags   []string `json:"tags"`
	Go     string   `json:"go"`
}

type Target struct {
	Kind     string            `json:"kind"`
	Nodes    int               `json:"nodes"`
	Variant  string            `json:"variant"`
	Lanes    int               `json:"lanes"`
	Cores    string            `json:"cores"`
	Env      map[string]string `json:"env"`
	Version  string            `json:"version"`
	Settings map[string]string `json:"settings"`
}

type Load struct {
	Workload  string        `json:"workload"`
	Accounts  int           `json:"accounts"`
	Rate      int           `json:"rate"`
	Stage     int           `json:"stage"`
	Stages    []int         `json:"stages"`
	Warmup    time.Duration `json:"warmup_ns"`
	Window    time.Duration `json:"window_ns"`
	MaxWindow time.Duration `json:"max_window_ns"`
	Drain     time.Duration `json:"drain_ns"`
	Need      int           `json:"need"`
	Seed      uint64        `json:"seed"`
}

type Head struct {
	Schema      int                `json:"schema"`
	ID          string             `json:"id"`
	Measurement string             `json:"measurement"`
	Pass        int                `json:"pass"`
	Started     time.Time          `json:"started"`
	Build       Build              `json:"build"`
	Machine     control.Machine    `json:"machine"`
	Target      Target             `json:"target"`
	Load        Load               `json:"load"`
	Probe       control.Probe      `json:"probe"`
	Thresholds  control.Thresholds `json:"thresholds"`
	AllowStalls bool               `json:"allow_stalls"`
}

type CPU struct {
	Generator float64 `json:"generator"`
	Target    float64 `json:"target"`
	Foreign   float64 `json:"foreign"`
}

type Part struct {
	Name     string            `json:"name"`
	Role     string            `json:"role"`
	Term     uint64            `json:"term"`
	Syncs    uint64            `json:"syncs"`
	SyncTime time.Duration     `json:"sync_ns"`
	Requests uint64            `json:"requests"`
	Counters map[string]uint64 `json:"counters"`
	Stale    bool              `json:"stale"`
}

type Second struct {
	Phase      string        `json:"phase"`
	Due        uint64        `json:"due"`
	Booked     uint64        `json:"booked"`
	Open       uint64        `json:"open"`
	Retries    uint64        `json:"retries"`
	Failed     uint64        `json:"failed"`
	Count      uint64        `json:"count"`
	P50        time.Duration `json:"p50_ns"`
	P99        time.Duration `json:"p99_ns"`
	LateP99    time.Duration `json:"late_p99_ns"`
	Flushes    uint64        `json:"flushes"`
	FlushTime  time.Duration `json:"flush_ns"`
	Writes     uint64        `json:"writes"`
	WriteTime  time.Duration `json:"write_ns"`
	CPU        CPU           `json:"cpu"`
	MHz        []int         `json:"mhz"`
	Throttled  uint64        `json:"throttled"`
	Available  uint64        `json:"available"`
	SwappedOut uint64        `json:"swapped_out"`
	Speed      control.Speed `json:"speed"`
	Valid      bool          `json:"valid"`
	Why        []string      `json:"why"`
	Parts      []Part        `json:"parts"`
}

type Histogram struct {
	Buckets [][2]int64 `json:"buckets"`
	Never   uint64     `json:"never"`
}

type Band struct {
	Seconds    int                `json:"seconds"`
	Throughput float64            `json:"throughput"`
	Histogram  Histogram          `json:"histogram"`
	P50        time.Duration      `json:"p50_ns"`
	P99        time.Duration      `json:"p99_ns"`
	P999       time.Duration      `json:"p999_ns"`
	Max        time.Duration      `json:"max_ns"`
	Unfinished uint64             `json:"unfinished"`
	Reserve    *Histogram         `json:"reserve"`
	ReserveP50 time.Duration      `json:"reserve_p50_ns"`
	ReserveP99 time.Duration      `json:"reserve_p99_ns"`
	Syncs      float64            `json:"syncs_per_transfer"`
	PartSyncs  map[string]float64 `json:"part_syncs_per_transfer"`
	PerRequest float64            `json:"transfers_per_request"`
	Flush      time.Duration      `json:"flush_ns"`
	P50Flushes float64            `json:"p50_flushes"`
}

type Bands struct {
	Slow Band `json:"slow"`
	Fast Band `json:"fast"`
}

type Verdict struct {
	Valid          bool          `json:"valid"`
	Speed          control.Speed `json:"speed"`
	Saturated      bool          `json:"saturated"`
	Capped         bool          `json:"capped"`
	Limit          string        `json:"limit"`
	OpenAtEnd      uint64        `json:"open_at_end"`
	Horizon        time.Duration `json:"horizon_ns"`
	Throughput     float64       `json:"throughput"`
	LateP99        time.Duration `json:"late_p99_ns"`
	GeneratorShare float64       `json:"generator_share"`
	TargetShare    float64       `json:"target_share"`
	Excluded       int           `json:"excluded_seconds"`
	ExcludedP99    time.Duration `json:"excluded_p99_ns"`
	Reasons        []string      `json:"reasons"`
	Problems       []string      `json:"problems"`
}

type Books struct {
	Confirmed      uint64      `json:"confirmed"`
	Existed        uint64      `json:"existed"`
	Posted         uint64      `json:"posted"`
	Pending        uint64      `json:"pending"`
	Unsure         load.Unsure `json:"unsure"`
	Sampled        int         `json:"sampled"`
	Verified       bool        `json:"verified"`
	Found          int         `json:"found"`
	DebitsPosted   uint64      `json:"debits_posted"`
	CreditsPosted  uint64      `json:"credits_posted"`
	DebitsPending  uint64      `json:"debits_pending"`
	CreditsPending uint64      `json:"credits_pending"`
}

type File struct {
	Head
	Before  []Part   `json:"before"`
	Seconds []Second `json:"seconds"`
	Bands   Bands    `json:"bands"`
	Verdict Verdict  `json:"verdict"`
	Books   Books    `json:"books"`
}

func Write(w io.Writer, f *File) error {
	b, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return err
	}
	_, err = w.Write(append(b, '\n'))
	return err
}

func Read(r io.Reader) (*File, error) {
	b, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	var version struct {
		Schema int `json:"schema"`
	}
	if err := json.Unmarshal(b, &version); err != nil {
		return nil, fmt.Errorf("raw: %w", err)
	}
	if version.Schema != Schema {
		return nil, fmt.Errorf("%w %d", ErrSchema, version.Schema)
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	f := new(File)
	if err := dec.Decode(f); err != nil {
		return nil, fmt.Errorf("raw: %w", err)
	}
	return f, nil
}
