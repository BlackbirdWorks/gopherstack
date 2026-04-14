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
	// ErrInsightNotFound is returned when an X-Ray insight is not found.
	ErrInsightNotFound = awserr.New("InvalidRequestException", awserr.ErrNotFound)
	// ErrResourcePolicyNotFound is returned when a resource policy is not found.
	ErrResourcePolicyNotFound = awserr.New("InvalidRequestException", awserr.ErrNotFound)
)

// Group represents an X-Ray group used to filter trace data.
type Group struct {
	CreatedAt        time.Time `json:"createdAt"`
	GroupARN         string    `json:"groupARN"`
	GroupName        string    `json:"groupName"`
	FilterExpression string    `json:"filterExpression"`
}

// SamplingRule represents an X-Ray sampling rule that controls the rate of data collection.
type SamplingRule struct {
	CreatedAt     time.Time `json:"createdAt"`
	RuleARN       string    `json:"ruleARN"`
	RuleName      string    `json:"ruleName"`
	ResourceARN   string    `json:"resourceARN"`
	ServiceName   string    `json:"serviceName"`
	ServiceType   string    `json:"serviceType"`
	Host          string    `json:"host"`
	HTTPMethod    string    `json:"httpMethod"`
	URLPath       string    `json:"urlPath"`
	FixedRate     float64   `json:"fixedRate"`
	Priority      int32     `json:"priority"`
	ReservoirSize int32     `json:"reservoirSize"`
}

// Trace represents a collected X-Ray trace with its constituent segments.
type Trace struct {
	StartTime time.Time `json:"startTime"`
	TraceID   string    `json:"traceID"`
	Segments  []string  `json:"segments"`
}

// EncryptionConfig represents X-Ray encryption configuration.
type EncryptionConfig struct {
	KeyID  string `json:"KeyId,omitempty"`
	Status string `json:"Status"`
	Type   string `json:"Type"`
}

// Insight represents an X-Ray insight.
type Insight struct {
	StartTime time.Time `json:"startTime"`
	InsightID string    `json:"insightId"`
	GroupARN  string    `json:"groupARN"`
	GroupName string    `json:"groupName"`
	State     string    `json:"state"`
	Summary   string    `json:"summary"`
}

// InsightEvent represents an event within an X-Ray insight.
type InsightEvent struct {
	EventTime time.Time `json:"eventTime"`
	InsightID string    `json:"insightId"`
	Summary   string    `json:"summary"`
}

// ResourcePolicy represents a resource-based policy attached to the X-Ray account.
type ResourcePolicy struct {
	PolicyName       string `json:"policyName"`
	PolicyDocument   string `json:"policyDocument"`
	PolicyRevisionID string `json:"policyRevisionId"`
}

// TraceRetrieval represents an ongoing trace retrieval operation.
type TraceRetrieval struct {
	StartTime      time.Time `json:"startTime"`
	RetrievalToken string    `json:"retrievalToken"`
	Status         string    `json:"status"`
}

// IndexingRule represents an X-Ray CloudWatch Logs indexing rule.
type IndexingRule struct {
	ModifiedAt time.Time `json:"modifiedAt"`
	Name       string    `json:"name"`
}

// SamplingStatisticSummary holds aggregated request sampling data for a rule.
type SamplingStatisticSummary struct {
	Timestamp    time.Time `json:"timestamp"`
	RuleName     string    `json:"ruleName"`
	RequestCount int32     `json:"requestCount"`
	SampledCount int32     `json:"sampledCount"`
	BorrowCount  int32     `json:"borrowCount"`
}

const (
	// traceRetrievalStatusComplete is the retrieval status returned for unknown tokens.
	traceRetrievalStatusComplete = "COMPLETE"
	// samplingTargetInterval is the recommended polling interval for sampling targets.
	samplingTargetInterval = 10
)

// InMemoryBackend is the in-memory store for X-Ray resources.
type InMemoryBackend struct {
	groups           map[string]*Group
	samplingRules    map[string]*SamplingRule
	traces           map[string]*Trace
	insights         map[string]*Insight
	insightEvents    map[string][]*InsightEvent
	resourcePolicies map[string]*ResourcePolicy
	traceRetrievals  map[string]*TraceRetrieval
	encryptionConfig *EncryptionConfig
	mu               *lockmetrics.RWMutex
	indexingRules    []*IndexingRule
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		groups:           make(map[string]*Group),
		samplingRules:    make(map[string]*SamplingRule),
		traces:           make(map[string]*Trace),
		insights:         make(map[string]*Insight),
		insightEvents:    make(map[string][]*InsightEvent),
		resourcePolicies: make(map[string]*ResourcePolicy),
		traceRetrievals:  make(map[string]*TraceRetrieval),
		indexingRules:    defaultIndexingRules(),
		mu:               lockmetrics.New("xray"),
		encryptionConfig: &EncryptionConfig{
			Type:   "NONE",
			Status: "ACTIVE",
		},
	}
}

// defaultIndexingRules returns the built-in X-Ray indexing rules.
func defaultIndexingRules() []*IndexingRule {
	now := time.Now()

	return []*IndexingRule{
		{Name: "Default", ModifiedAt: now},
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

// Reset clears all backend state, resetting to an empty store.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.groups = make(map[string]*Group)
	b.samplingRules = make(map[string]*SamplingRule)
	b.traces = make(map[string]*Trace)
	b.insights = make(map[string]*Insight)
	b.insightEvents = make(map[string][]*InsightEvent)
	b.resourcePolicies = make(map[string]*ResourcePolicy)
	b.traceRetrievals = make(map[string]*TraceRetrieval)
	b.indexingRules = defaultIndexingRules()
	b.encryptionConfig = &EncryptionConfig{Type: "NONE", Status: "ACTIVE"}
}

// GetEncryptionConfig returns the current X-Ray encryption configuration.
func (b *InMemoryBackend) GetEncryptionConfig() *EncryptionConfig {
	b.mu.RLock("GetEncryptionConfig")
	defer b.mu.RUnlock()

	cp := *b.encryptionConfig

	return &cp
}

// PutEncryptionConfig updates the X-Ray encryption configuration.
// encType must be one of "NONE" or "KMS". keyID is only used when encType is "KMS".
func (b *InMemoryBackend) PutEncryptionConfig(encType, keyID string) *EncryptionConfig {
	b.mu.Lock("PutEncryptionConfig")
	defer b.mu.Unlock()

	b.encryptionConfig = &EncryptionConfig{
		Type:   encType,
		KeyID:  keyID,
		Status: "ACTIVE",
	}

	cp := *b.encryptionConfig

	return &cp
}

// --- Insight operations ---

func cloneInsight(i *Insight) *Insight {
	cp := *i

	return &cp
}

// AddInsightInternal seeds an insight directly for testing.
func (b *InMemoryBackend) AddInsightInternal(insight Insight) {
	b.mu.Lock("AddInsightInternal")
	defer b.mu.Unlock()

	b.insights[insight.InsightID] = &insight
}

// GetInsight returns the insight with the given ID.
func (b *InMemoryBackend) GetInsight(insightID string) (*Insight, error) {
	b.mu.RLock("GetInsight")
	defer b.mu.RUnlock()

	i, ok := b.insights[insightID]
	if !ok {
		return nil, fmt.Errorf("%w: insight %s not found", ErrInsightNotFound, insightID)
	}

	return cloneInsight(i), nil
}

// GetInsightEvents returns all events for the given insight ID.
func (b *InMemoryBackend) GetInsightEvents(insightID string) ([]*InsightEvent, error) {
	b.mu.RLock("GetInsightEvents")
	defer b.mu.RUnlock()

	if _, ok := b.insights[insightID]; !ok {
		return nil, fmt.Errorf("%w: insight %s not found", ErrInsightNotFound, insightID)
	}

	events := b.insightEvents[insightID]
	out := make([]*InsightEvent, len(events))

	for idx, e := range events {
		cp := *e
		out[idx] = &cp
	}

	return out, nil
}

// GetInsightSummaries returns all insights as summaries.
func (b *InMemoryBackend) GetInsightSummaries() []Insight {
	b.mu.RLock("GetInsightSummaries")
	defer b.mu.RUnlock()

	out := make([]Insight, 0, len(b.insights))
	for _, i := range b.insights {
		out = append(out, *cloneInsight(i))
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].InsightID < out[j].InsightID
	})

	return out
}

// --- Resource policy operations ---

func cloneResourcePolicy(p *ResourcePolicy) *ResourcePolicy {
	cp := *p

	return &cp
}

// DeleteResourcePolicy removes the resource policy with the given name.
func (b *InMemoryBackend) DeleteResourcePolicy(policyName string) error {
	b.mu.Lock("DeleteResourcePolicy")
	defer b.mu.Unlock()

	if _, ok := b.resourcePolicies[policyName]; !ok {
		return fmt.Errorf("%w: resource policy %s not found", ErrResourcePolicyNotFound, policyName)
	}

	delete(b.resourcePolicies, policyName)

	return nil
}

// AddResourcePolicyInternal seeds a resource policy directly for testing.
func (b *InMemoryBackend) AddResourcePolicyInternal(policy ResourcePolicy) {
	b.mu.Lock("AddResourcePolicyInternal")
	defer b.mu.Unlock()

	b.resourcePolicies[policy.PolicyName] = cloneResourcePolicy(&policy)
}

// --- Indexing rule operations ---

// GetIndexingRules returns all indexing rules.
func (b *InMemoryBackend) GetIndexingRules() []*IndexingRule {
	b.mu.RLock("GetIndexingRules")
	defer b.mu.RUnlock()

	out := make([]*IndexingRule, len(b.indexingRules))
	for i, r := range b.indexingRules {
		cp := *r
		out[i] = &cp
	}

	return out
}

// --- Trace retrieval operations ---

// CancelTraceRetrieval marks a trace retrieval as cancelled.
// If the token is not found the operation is a no-op (idempotent).
func (b *InMemoryBackend) CancelTraceRetrieval(retrievalToken string) {
	b.mu.Lock("CancelTraceRetrieval")
	defer b.mu.Unlock()

	delete(b.traceRetrievals, retrievalToken)
}

// GetRetrievedTracesGraph returns the status and services for a retrieval token.
// If the token is not found a COMPLETE status is returned.
func (b *InMemoryBackend) GetRetrievedTracesGraph(retrievalToken string) (string, []*Trace) {
	b.mu.RLock("GetRetrievedTracesGraph")
	defer b.mu.RUnlock()

	if _, ok := b.traceRetrievals[retrievalToken]; !ok {
		return traceRetrievalStatusComplete, nil
	}

	return b.traceRetrievals[retrievalToken].Status, nil
}

// --- Sampling statistic operations ---

// GetSamplingStatisticSummaries returns an empty list of sampling statistic summaries.
// In a production implementation statistics would be accumulated per sampling rule.
func (b *InMemoryBackend) GetSamplingStatisticSummaries() []SamplingStatisticSummary {
	b.mu.RLock("GetSamplingStatisticSummaries")
	defer b.mu.RUnlock()

	return []SamplingStatisticSummary{}
}

// SamplingTargetResult holds the per-document results of GetSamplingTargets.
type SamplingTargetResult struct {
	RuleName      string
	FixedRate     float64
	ReservoirSize int32
	Found         bool
}

// GetSamplingTargets returns target documents for the provided rule names.
// Rules that do not exist are returned in the unprocessed list.
func (b *InMemoryBackend) GetSamplingTargets(ruleNames []string) ([]SamplingTargetResult, []string) {
	b.mu.RLock("GetSamplingTargets")
	defer b.mu.RUnlock()

	targets := make([]SamplingTargetResult, 0, len(ruleNames))
	unprocessed := make([]string, 0)

	for _, name := range ruleNames {
		r, ok := b.samplingRules[name]
		if !ok {
			unprocessed = append(unprocessed, name)

			continue
		}

		targets = append(targets, SamplingTargetResult{
			RuleName:      r.RuleName,
			FixedRate:     r.FixedRate,
			ReservoirSize: r.ReservoirSize,
			Found:         true,
		})
	}

	return targets, unprocessed
}
