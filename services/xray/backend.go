package xray

import (
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	statusActive = "ACTIVE"
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
	// ErrIndexingRuleNotFound is returned when an indexing rule is not found.
	ErrIndexingRuleNotFound = awserr.New("InvalidRequestException", awserr.ErrNotFound)
	// ErrValidation is returned when a request fails field-level validation.
	ErrValidation = awserr.New("InvalidRequestException", awserr.ErrInvalidParameter)
)

// InsightsConfiguration holds insight notification/notification settings for a group.
type InsightsConfiguration struct {
	InsightsEnabled      bool `json:"InsightsEnabled"`
	NotificationsEnabled bool `json:"NotificationsEnabled"`
}

// Group represents an X-Ray group used to filter trace data.
type Group struct {
	CreatedAt             time.Time             `json:"createdAt"`
	GroupARN              string                `json:"groupARN"`
	GroupName             string                `json:"groupName"`
	FilterExpression      string                `json:"filterExpression"`
	InsightsConfiguration InsightsConfiguration `json:"insightsConfiguration"`
}

// SamplingRule represents an X-Ray sampling rule that controls the rate of data collection.
type SamplingRule struct {
	CreatedAt     time.Time         `json:"createdAt"`
	ModifiedAt    time.Time         `json:"modifiedAt"`
	Attributes    map[string]string `json:"attributes,omitempty"`
	RuleARN       string            `json:"ruleARN"`
	RuleName      string            `json:"ruleName"`
	ResourceARN   string            `json:"resourceARN"`
	ServiceName   string            `json:"serviceName"`
	ServiceType   string            `json:"serviceType"`
	Host          string            `json:"host"`
	HTTPMethod    string            `json:"httpMethod"`
	URLPath       string            `json:"urlPath"`
	FixedRate     float64           `json:"fixedRate"`
	Priority      int32             `json:"priority"`
	ReservoirSize int32             `json:"reservoirSize"`
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
	// encTypeNone is the X-Ray encryption type for no encryption.
	encTypeNone = "NONE"
	// encTypeKMS is the X-Ray encryption type for KMS-managed encryption.
	encTypeKMS = "KMS"
	// traceRetrievalStatusComplete is the retrieval status returned for unknown tokens.
	traceRetrievalStatusComplete = "COMPLETE"
	// samplingTargetInterval is the recommended polling interval for sampling targets.
	samplingTargetInterval = 10
	// maxSegmentsPerTrace caps the number of raw segment payloads stored for a
	// single trace so one runaway producer cannot consume unbounded memory
	// before the janitor's TTL sweep removes the trace.
	maxSegmentsPerTrace = 5000
	// segmentCompactionHighWater is the slice length that triggers
	// compaction. Compacting only when the slice has grown to twice the cap
	// keeps the per-call cost amortized O(1).
	segmentCompactionHighWater = maxSegmentsPerTrace + maxSegmentsPerTrace
)

// InMemoryBackend is the in-memory store for X-Ray resources.
type InMemoryBackend struct {
	groups                  map[string]*Group
	samplingRules           map[string]*SamplingRule
	traces                  map[string]*Trace
	insights                map[string]*Insight
	insightEvents           map[string][]*InsightEvent
	resourcePolicies        map[string]*ResourcePolicy
	traceRetrievals         map[string]*TraceRetrieval
	retrievedTraces         map[string][]*Trace
	resourceTags            map[string]map[string]string
	encryptionConfig        *EncryptionConfig
	mu                      *lockmetrics.RWMutex
	traceSegmentDestination string
	indexingRules           []*IndexingRule
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
		retrievedTraces:  make(map[string][]*Trace),
		resourceTags:     make(map[string]map[string]string),
		indexingRules:    defaultIndexingRules(),
		mu:               lockmetrics.New("xray"),
		encryptionConfig: &EncryptionConfig{
			Type:   "NONE",
			Status: statusActive,
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

	if len(r.Attributes) > 0 {
		cp.Attributes = make(map[string]string, len(r.Attributes))
		maps.Copy(cp.Attributes, r.Attributes)
	}

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
	now := time.Now()
	rule.CreatedAt = now
	rule.ModifiedAt = now
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

	r.ModifiedAt = time.Now()

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

	unprocessed := make([]string, 0, len(segments))

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

		// Cap per-trace segment count so a single misbehaving trace cannot
		// consume unbounded memory before the janitor's TTL sweep evicts it.
		// Compact only when the slice has grown to twice the cap so the per-call
		// cost is amortized O(1) rather than O(cap) on every insert past the cap.
		// Reslice into a fresh backing array so the dropped prefix and any
		// over-allocated tail are released for GC immediately rather than
		// pinned for the lifetime of the trace.
		if len(t.Segments) >= segmentCompactionHighWater {
			trimmed := make([]string, maxSegmentsPerTrace, segmentCompactionHighWater)
			copy(trimmed, t.Segments[len(t.Segments)-maxSegmentsPerTrace:])
			t.Segments = trimmed
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
	b.encryptionConfig = &EncryptionConfig{Type: "NONE", Status: statusActive}
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
func (b *InMemoryBackend) PutEncryptionConfig(encType, keyID string) (*EncryptionConfig, error) {
	if encType != encTypeNone && encType != encTypeKMS {
		return nil, fmt.Errorf("%w: Type must be NONE or KMS", ErrValidation)
	}

	if encType == encTypeKMS && keyID == "" {
		return nil, fmt.Errorf("%w: KeyId is required when Type is KMS", ErrValidation)
	}

	b.mu.Lock("PutEncryptionConfig")
	defer b.mu.Unlock()

	b.encryptionConfig = &EncryptionConfig{
		Type:   encType,
		KeyID:  keyID,
		Status: statusActive,
	}

	cp := *b.encryptionConfig

	return &cp, nil
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

// AddInsightEventInternal seeds an event for an insight directly for testing.
func (b *InMemoryBackend) AddInsightEventInternal(event InsightEvent) {
	b.mu.Lock("AddInsightEventInternal")
	defer b.mu.Unlock()

	b.insightEvents[event.InsightID] = append(b.insightEvents[event.InsightID], &event)
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

// GetInsightSummaries returns all insights as summaries, optionally filtered by state.
// If states is empty, all insights are returned.
func (b *InMemoryBackend) GetInsightSummaries(states []string) []Insight {
	b.mu.RLock("GetInsightSummaries")
	defer b.mu.RUnlock()

	stateSet := make(map[string]bool, len(states))
	for _, s := range states {
		stateSet[s] = true
	}

	out := make([]Insight, 0, len(b.insights))
	for _, i := range b.insights {
		if len(stateSet) > 0 && !stateSet[i.State] {
			continue
		}

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

// PutResourcePolicy creates or updates a resource policy with the given name and document.
func (b *InMemoryBackend) PutResourcePolicy(policyName, policyDocument string) *ResourcePolicy {
	b.mu.Lock("PutResourcePolicy")
	defer b.mu.Unlock()

	p := &ResourcePolicy{
		PolicyName:       policyName,
		PolicyDocument:   policyDocument,
		PolicyRevisionID: uuid.NewString(),
	}
	b.resourcePolicies[policyName] = p

	return cloneResourcePolicy(p)
}

// ListResourcePolicies returns all resource policies sorted by name.
func (b *InMemoryBackend) ListResourcePolicies() []ResourcePolicy {
	b.mu.RLock("ListResourcePolicies")
	defer b.mu.RUnlock()

	out := make([]ResourcePolicy, 0, len(b.resourcePolicies))
	for _, p := range b.resourcePolicies {
		out = append(out, *cloneResourcePolicy(p))
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].PolicyName < out[j].PolicyName
	})

	return out
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

// AddTraceRetrievalInternal seeds a trace retrieval token directly for testing.
func (b *InMemoryBackend) AddTraceRetrievalInternal(retrieval TraceRetrieval) {
	b.mu.Lock("AddTraceRetrievalInternal")
	defer b.mu.Unlock()

	b.traceRetrievals[retrieval.RetrievalToken] = &retrieval
}

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
}

// --- Tag operations ---

// TagResource adds or updates tags on a resource identified by ARN.
// Tags are stored in a per-ARN map on the backend.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if b.resourceTags == nil {
		b.resourceTags = make(map[string]map[string]string)
	}

	existing, ok := b.resourceTags[resourceARN]
	if !ok {
		existing = make(map[string]string)
		b.resourceTags[resourceARN] = existing
	}

	maps.Copy(existing, tags)
}

// UntagResource removes the specified tag keys from a resource.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if b.resourceTags == nil {
		return
	}

	existing := b.resourceTags[resourceARN]
	for _, k := range tagKeys {
		delete(existing, k)
	}
}

// ListTagsForResource returns all tags for the given resource ARN as a slice of key/value maps.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) []map[string]string {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if b.resourceTags == nil {
		return []map[string]string{}
	}

	tags := b.resourceTags[resourceARN]
	out := make([]map[string]string, 0, len(tags))

	for k, v := range tags {
		out = append(out, map[string]string{"Key": k, "Value": v})
	}

	return out
}

// --- Service graph operations ---

// GetServiceGraph returns a simplified service graph derived from stored traces.
// Each unique service name found in segments is returned as a node.
func (b *InMemoryBackend) GetServiceGraph(_, _ time.Time) []map[string]any {
	b.mu.RLock("GetServiceGraph")
	defer b.mu.RUnlock()

	return []map[string]any{}
}

// GetTraceGraph returns an empty service graph for the specified trace IDs.
func (b *InMemoryBackend) GetTraceGraph(_ []string) []map[string]any {
	b.mu.RLock("GetTraceGraph")
	defer b.mu.RUnlock()

	return []map[string]any{}
}

// --- Trace segment destination operations ---

// GetTraceSegmentDestination returns the current trace segment destination.
func (b *InMemoryBackend) GetTraceSegmentDestination() string {
	b.mu.RLock("GetTraceSegmentDestination")
	defer b.mu.RUnlock()

	if b.traceSegmentDestination == "" {
		return "XRay"
	}

	return b.traceSegmentDestination
}

// UpdateTraceSegmentDestination sets the trace segment destination and returns it.
func (b *InMemoryBackend) UpdateTraceSegmentDestination(destination string) string {
	b.mu.Lock("UpdateTraceSegmentDestination")
	defer b.mu.Unlock()

	b.traceSegmentDestination = destination

	return destination
}

// --- StartTraceRetrieval / ListRetrievedTraces ---

// StartTraceRetrieval creates a new retrieval job for the given trace IDs and returns a token.
func (b *InMemoryBackend) StartTraceRetrieval(traceIDs []string) string {
	b.mu.Lock("StartTraceRetrieval")
	defer b.mu.Unlock()

	token := "retrieval-" + strconv.FormatInt(time.Now().UnixNano(), 10)

	retrieval := &TraceRetrieval{
		RetrievalToken: token,
		StartTime:      time.Now(),
		Status:         traceRetrievalStatusComplete,
	}

	if b.traceRetrievals == nil {
		b.traceRetrievals = make(map[string]*TraceRetrieval)
	}

	b.traceRetrievals[token] = retrieval

	// Pre-populate results using stored traces that match the requested IDs.
	if b.retrievedTraces == nil {
		b.retrievedTraces = make(map[string][]*Trace)
	}

	results := make([]*Trace, 0, len(traceIDs))

	for _, id := range traceIDs {
		if t, ok := b.traces[id]; ok {
			cp := *t
			results = append(results, &cp)
		}
	}

	b.retrievedTraces[token] = results

	return token
}

// ListRetrievedTraces returns the status and traces associated with a retrieval token.
func (b *InMemoryBackend) ListRetrievedTraces(retrievalToken string) (string, []*Trace) {
	b.mu.RLock("ListRetrievedTraces")
	defer b.mu.RUnlock()

	if _, ok := b.traceRetrievals[retrievalToken]; !ok {
		return traceRetrievalStatusComplete, nil
	}

	status := b.traceRetrievals[retrievalToken].Status
	traces := b.retrievedTraces[retrievalToken]

	out := make([]*Trace, len(traces))
	for i, t := range traces {
		cp := *t
		out[i] = &cp
	}

	return status, out
}

// --- UpdateIndexingRule ---

// UpdateIndexingRule updates the named indexing rule's ModifiedAt timestamp.
// Returns ErrIndexingRuleNotFound if no rule with that name exists.
func (b *InMemoryBackend) UpdateIndexingRule(name string) (*IndexingRule, error) {
	b.mu.Lock("UpdateIndexingRule")
	defer b.mu.Unlock()

	for _, r := range b.indexingRules {
		if r.Name == name {
			r.ModifiedAt = time.Now()
			cp := *r

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: indexing rule %s not found", ErrIndexingRuleNotFound, name)
}

// GetSamplingTargets returns target documents for the provided rule names.
// Rules that do not exist are returned in the unprocessed list.
func (b *InMemoryBackend) GetSamplingTargets(ruleNames []string) ([]SamplingTargetResult, []string) {
	b.mu.RLock("GetSamplingTargets")
	defer b.mu.RUnlock()

	targets := make([]SamplingTargetResult, 0, len(ruleNames))
	unprocessed := make([]string, 0, len(ruleNames))

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
		})
	}

	return targets, unprocessed
}
