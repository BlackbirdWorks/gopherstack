package xray

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

var (
	// ErrGroupNotFound is returned when an X-Ray group is not found.
	ErrGroupNotFound = awserr.New("InvalidRequestException", awserr.ErrNotFound)
	// ErrGroupAlreadyExists is returned when an X-Ray group already exists.
	ErrGroupAlreadyExists = awserr.New("GroupAlreadyExistsException", awserr.ErrConflict)
	// ErrSamplingRuleNotFound is returned when a sampling rule is not found.
	ErrSamplingRuleNotFound = awserr.New("InvalidRequestException", awserr.ErrNotFound)
	// ErrSamplingRuleAlreadyExists is returned when a sampling rule already exists.
	ErrSamplingRuleAlreadyExists = awserr.New("RuleAlreadyExistsException", awserr.ErrConflict)
)

// Group represents an X-Ray group used to filter trace data.
type Group struct {
	CreatedAt        time.Time
	GroupARN         string
	GroupName        string
	FilterExpression string
}

// SamplingRule represents an X-Ray sampling rule that controls the rate of data collection.
type SamplingRule struct {
	CreatedAt     time.Time
	RuleARN       string
	RuleName      string
	ResourceARN   string
	ServiceName   string
	ServiceType   string
	Host          string
	HTTPMethod    string
	URLPath       string
	FixedRate     float64
	Priority      int32
	ReservoirSize int32
}

// Trace represents a collected X-Ray trace with its constituent segments.
type Trace struct {
	StartTime time.Time
	TraceID   string
	Segments  []string
}

// InMemoryBackend is the in-memory store for X-Ray resources.
type InMemoryBackend struct {
	groups        map[string]*Group
	samplingRules map[string]*SamplingRule
	traces        map[string]*Trace
	mu            *lockmetrics.RWMutex
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		groups:        make(map[string]*Group),
		samplingRules: make(map[string]*SamplingRule),
		traces:        make(map[string]*Trace),
		mu:            lockmetrics.New("xray"),
	}
}

func groupARN(name string) string {
	return "arn:aws:xray:" + config.DefaultRegion + ":" + config.DefaultAccountID + ":group/default/" + name
}

func samplingRuleARN(name string) string {
	return "arn:aws:xray:" + config.DefaultRegion + ":" + config.DefaultAccountID + ":sampling-rule/" + name
}

func cloneGroup(g *Group) *Group {
	cp := *g
	return &cp
}

func cloneRule(r *SamplingRule) *SamplingRule {
	cp := *r
	return &cp
}

// CreateGroup creates a new X-Ray group with the given name and filter expression.
func (b *InMemoryBackend) CreateGroup(name, filterExpr string) (*Group, error) {
	b.mu.Lock("CreateGroup")
	defer b.mu.Unlock()

	if _, ok := b.groups[name]; ok {
		return nil, fmt.Errorf("%w: group %s already exists", ErrGroupAlreadyExists, name)
	}

	g := &Group{
		GroupARN:         groupARN(name),
		GroupName:        name,
		FilterExpression: filterExpr,
		CreatedAt:        time.Now(),
	}
	b.groups[name] = g

	return cloneGroup(g), nil
}

// GetGroup returns the group with the given name.
func (b *InMemoryBackend) GetGroup(name string) (*Group, error) {
	b.mu.RLock("GetGroup")
	defer b.mu.RUnlock()

	g, ok := b.groups[name]
	if !ok {
		return nil, fmt.Errorf("%w: group %s not found", ErrGroupNotFound, name)
	}

	return cloneGroup(g), nil
}

// GetGroups returns all groups sorted by name.
func (b *InMemoryBackend) GetGroups() []Group {
	b.mu.RLock("GetGroups")
	defer b.mu.RUnlock()

	out := make([]Group, 0, len(b.groups))
	for _, g := range b.groups {
		out = append(out, *cloneGroup(g))
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].GroupName < out[j].GroupName
	})

	return out
}

// UpdateGroup updates the filter expression for the group with the given name.
func (b *InMemoryBackend) UpdateGroup(name, filterExpr string) (*Group, error) {
	b.mu.Lock("UpdateGroup")
	defer b.mu.Unlock()

	g, ok := b.groups[name]
	if !ok {
		return nil, fmt.Errorf("%w: group %s not found", ErrGroupNotFound, name)
	}

	g.FilterExpression = filterExpr

	return cloneGroup(g), nil
}

// DeleteGroup removes the group with the given name.
func (b *InMemoryBackend) DeleteGroup(name string) error {
	b.mu.Lock("DeleteGroup")
	defer b.mu.Unlock()

	if _, ok := b.groups[name]; !ok {
		return fmt.Errorf("%w: group %s not found", ErrGroupNotFound, name)
	}

	delete(b.groups, name)

	return nil
}

// CreateSamplingRule creates a new sampling rule.
func (b *InMemoryBackend) CreateSamplingRule(rule SamplingRule) (*SamplingRule, error) {
	b.mu.Lock("CreateSamplingRule")
	defer b.mu.Unlock()

	if _, ok := b.samplingRules[rule.RuleName]; ok {
		return nil, fmt.Errorf("%w: sampling rule %s already exists", ErrSamplingRuleAlreadyExists, rule.RuleName)
	}

	rule.RuleARN = samplingRuleARN(rule.RuleName)
	rule.CreatedAt = time.Now()
	b.samplingRules[rule.RuleName] = &rule

	return cloneRule(&rule), nil
}

// GetSamplingRules returns all sampling rules sorted by name.
func (b *InMemoryBackend) GetSamplingRules() []SamplingRule {
	b.mu.RLock("GetSamplingRules")
	defer b.mu.RUnlock()

	out := make([]SamplingRule, 0, len(b.samplingRules))
	for _, r := range b.samplingRules {
		out = append(out, *cloneRule(r))
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].RuleName < out[j].RuleName
	})

	return out
}

// UpdateSamplingRule updates the mutable fields of an existing sampling rule.
func (b *InMemoryBackend) UpdateSamplingRule(ruleName string, updates SamplingRule) (*SamplingRule, error) {
	b.mu.Lock("UpdateSamplingRule")
	defer b.mu.Unlock()

	r, ok := b.samplingRules[ruleName]
	if !ok {
		return nil, fmt.Errorf("%w: sampling rule %s not found", ErrSamplingRuleNotFound, ruleName)
	}

	if updates.FixedRate >= 0 {
		r.FixedRate = updates.FixedRate
	}

	if updates.ReservoirSize >= 0 {
		r.ReservoirSize = updates.ReservoirSize
	}

	if updates.ResourceARN != "" {
		r.ResourceARN = updates.ResourceARN
	}

	if updates.ServiceName != "" {
		r.ServiceName = updates.ServiceName
	}

	if updates.ServiceType != "" {
		r.ServiceType = updates.ServiceType
	}

	if updates.Host != "" {
		r.Host = updates.Host
	}

	if updates.HTTPMethod != "" {
		r.HTTPMethod = updates.HTTPMethod
	}

	if updates.URLPath != "" {
		r.URLPath = updates.URLPath
	}

	if updates.Priority > 0 {
		r.Priority = updates.Priority
	}

	return cloneRule(r), nil
}

// DeleteSamplingRule removes the sampling rule with the given name and returns it.
func (b *InMemoryBackend) DeleteSamplingRule(ruleName string) (*SamplingRule, error) {
	b.mu.Lock("DeleteSamplingRule")
	defer b.mu.Unlock()

	r, ok := b.samplingRules[ruleName]
	if !ok {
		return nil, fmt.Errorf("%w: sampling rule %s not found", ErrSamplingRuleNotFound, ruleName)
	}

	deleted := cloneRule(r)
	delete(b.samplingRules, ruleName)

	return deleted, nil
}

// segmentHeader is used to extract the trace_id from a raw segment JSON.
type segmentHeader struct {
	TraceID string `json:"trace_id"`
}

// PutTraceSegments stores raw segment JSON strings and returns the list of
// unprocessed segment IDs (empty slice means all segments were accepted).
func (b *InMemoryBackend) PutTraceSegments(segments []string) []string {
	b.mu.Lock("PutTraceSegments")
	defer b.mu.Unlock()

	unprocessed := make([]string, 0)

	for _, seg := range segments {
		var hdr segmentHeader
		if err := json.Unmarshal([]byte(seg), &hdr); err != nil || hdr.TraceID == "" {
			unprocessed = append(unprocessed, uuid.NewString())

			continue
		}

		t, ok := b.traces[hdr.TraceID]
		if !ok {
			t = &Trace{
				TraceID:   hdr.TraceID,
				StartTime: time.Now(),
				Segments:  []string{},
			}
			b.traces[hdr.TraceID] = t
		}

		t.Segments = append(t.Segments, seg)
	}

	return unprocessed
}

// GetTraceSummaries returns all trace summaries sorted by start time (newest first).
func (b *InMemoryBackend) GetTraceSummaries() []Trace {
	b.mu.RLock("GetTraceSummaries")
	defer b.mu.RUnlock()

	out := make([]Trace, 0, len(b.traces))
	for _, t := range b.traces {
		cp := *t
		cp.Segments = make([]string, len(t.Segments))
		copy(cp.Segments, t.Segments)
		out = append(out, cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].StartTime.After(out[j].StartTime)
	})

	return out
}

// GetTrace returns the trace with the given ID, or nil if not found.
func (b *InMemoryBackend) GetTrace(traceID string) *Trace {
	b.mu.RLock("GetTrace")
	defer b.mu.RUnlock()

	t, ok := b.traces[traceID]
	if !ok {
		return nil
	}

	cp := *t
	cp.Segments = make([]string, len(t.Segments))
	copy(cp.Segments, t.Segments)

	return &cp
}
