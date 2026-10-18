package apptrace

import (
	"cmp"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

// Defaults for [Config] fields left zero.
const (
	DefaultCapacity = 2000
	DefaultMaxBody  = 4096
)

// Config configures a [Tracer].
type Config struct {
	// Capacity is the ring size in spans.
	Capacity int
	// MaxBody caps each captured body in bytes.
	MaxBody int
	// Enabled turns metadata recording on.
	Enabled bool
	// Detail additionally captures redacted headers and bodies.
	Detail bool
}

// Tracer is a fixed-capacity, lock-free ring of committed spans.
type Tracer struct {
	red      *redactor
	slots    []atomic.Pointer[Span]
	next     atomic.Uint64
	floor    atomic.Uint64
	children atomic.Uint64
	maxBody  int
	enabled  atomic.Bool
	detail   atomic.Bool
}

// New builds a Tracer; zero Capacity and MaxBody take their defaults.
func New(cfg Config) *Tracer {
	if cfg.Capacity <= 0 {
		cfg.Capacity = DefaultCapacity
	}

	if cfg.MaxBody <= 0 {
		cfg.MaxBody = DefaultMaxBody
	}

	t := &Tracer{
		slots:   make([]atomic.Pointer[Span], cfg.Capacity),
		maxBody: cfg.MaxBody,
		red:     newRedactor(),
	}
	t.enabled.Store(cfg.Enabled)
	t.detail.Store(cfg.Detail)

	return t
}

// Enabled reports whether spans are being recorded.
func (t *Tracer) Enabled() bool { return t.enabled.Load() }

// DetailEnabled reports whether headers and bodies are captured.
func (t *Tracer) DetailEnabled() bool { return t.detail.Load() }

// SetEnabled toggles recording at runtime.
func (t *Tracer) SetEnabled(v bool) { t.enabled.Store(v) }

// SetDetail toggles header and body capture at runtime.
func (t *Tracer) SetDetail(v bool) { t.detail.Store(v) }

// Config returns the current settings.
func (t *Tracer) Config() Config {
	return Config{
		Capacity: len(t.slots),
		MaxBody:  t.maxBody,
		Enabled:  t.enabled.Load(),
		Detail:   t.detail.Load(),
	}
}

// Record commits s to the ring, overwriting the oldest span when full.
func (t *Tracer) Record(s *Span) {
	seq := t.next.Add(1)
	s.Seq = seq
	s.HasDetail = s.Detail != nil
	t.slots[(seq-1)%uint64(len(t.slots))].Store(s)
}

func (t *Tracer) childID(traceID string) string {
	return traceID + "." + strconv.FormatUint(t.children.Add(1), 36)
}

// Recorded is the total number of spans ever committed, including evicted ones.
func (t *Tracer) Recorded() uint64 { return t.next.Load() }

// Clear drops every retained span.
func (t *Tracer) Clear() { t.floor.Store(t.next.Load()) }

// Filter selects spans for [Tracer.List]. Zero fields match everything.
type Filter struct {
	Service    string
	Operation  string
	Region     string
	Account    string
	Principal  string
	ErrorCode  string
	TraceID    string
	ParentID   string
	Kind       string
	Query      string
	AfterSeq   uint64
	Limit      int
	Offset     int
	MinStatus  int
	MaxStatus  int
	ErrorsOnly bool
}

// ListResult is a page of spans, newest first.
type ListResult struct {
	Items    []Span `json:"items"`
	Total    int    `json:"total"`
	Capacity int    `json:"capacity"`
	Recorded uint64 `json:"recorded"`
}

func contains(hay, needle string) bool {
	return needle == "" || strings.Contains(strs.Fold(hay), strs.Fold(needle))
}

func (f *Filter) matchIdentity(s *Span) bool {
	switch {
	case f.Service != "" && !strs.Equal(s.Service, f.Service),
		f.Region != "" && !strs.Equal(s.Region, f.Region),
		f.Account != "" && s.Account != f.Account,
		f.TraceID != "" && s.TraceID != f.TraceID,
		f.ParentID != "" && s.ParentID != f.ParentID,
		f.Kind != "" && s.Kind != f.Kind:
		return false
	}

	return true
}

func (f *Filter) matchStatus(s *Span) bool {
	switch {
	case f.ErrorsOnly && s.Status < 400,
		f.MinStatus > 0 && s.Status < f.MinStatus,
		f.MaxStatus > 0 && s.Status > f.MaxStatus,
		s.Seq <= f.AfterSeq:
		return false
	}

	return true
}

func (f *Filter) matchText(s *Span) bool {
	if !contains(s.Operation, f.Operation) || !contains(s.Principal, f.Principal) ||
		!contains(s.ErrorCode, f.ErrorCode) {
		return false
	}

	if f.Query == "" {
		return true
	}

	return contains(s.Service, f.Query) || contains(s.Operation, f.Query) || contains(s.Resource, f.Query) ||
		contains(s.Path, f.Query) || contains(s.ID, f.Query) || contains(s.ErrorCode, f.Query) ||
		contains(s.Principal, f.Query)
}

func (f *Filter) match(s *Span) bool {
	return f.matchIdentity(s) && f.matchStatus(s) && f.matchText(s)
}

// snapshot returns every retained span, newest first.
func (t *Tracer) snapshot() []*Span {
	floor := t.floor.Load()
	out := make([]*Span, 0, len(t.slots))

	for i := range t.slots {
		if s := t.slots[i].Load(); s != nil && s.Seq > floor {
			out = append(out, s)
		}
	}

	slices.SortFunc(out, func(a, b *Span) int { return cmp.Compare(b.Seq, a.Seq) })

	return out
}

// List returns spans matching f, newest first, without their Detail.
func (t *Tracer) List(f Filter) ListResult {
	res := ListResult{Items: []Span{}, Capacity: len(t.slots), Recorded: t.next.Load()}
	skip := f.Offset

	for _, s := range t.snapshot() {
		if !f.match(s) {
			continue
		}

		res.Total++

		if skip > 0 {
			skip--

			continue
		}

		if f.Limit <= 0 || len(res.Items) < f.Limit {
			cp := *s
			cp.Detail = nil
			res.Items = append(res.Items, cp)
		}
	}

	return res
}

// Get returns the newest span with the given id, including its Detail.
func (t *Tracer) Get(id string) (Span, bool) {
	for _, s := range t.snapshot() {
		if s.ID == id {
			return *s, true
		}
	}

	return Span{}, false
}

// Trace returns every retained span of a trace, oldest first, without Detail.
func (t *Tracer) Trace(traceID string) []Span {
	all := t.List(Filter{TraceID: traceID})
	for i, j := 0, len(all.Items)-1; i < j; i, j = i+1, j-1 {
		all.Items[i], all.Items[j] = all.Items[j], all.Items[i]
	}

	return all.Items
}
