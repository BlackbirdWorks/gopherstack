package xray

import (
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	statusActive   = "ACTIVE"
	statusUpdating = "UPDATING"
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
	// ErrInvalidSamplingRule is returned when sampling rule fields fail validation.
	ErrInvalidSamplingRule = awserr.New("InvalidSamplingRuleException", awserr.ErrInvalidParameter)
	// ErrInvalidPolicyRevisionID is returned when a policy revision ID does not match.
	ErrInvalidPolicyRevisionID = awserr.New("InvalidPolicyRevisionIdException", awserr.ErrConflict)
	// ErrMalformedPolicyDocument is returned when a policy document is not valid JSON.
	ErrMalformedPolicyDocument = awserr.New("MalformedPolicyDocumentException", awserr.ErrInvalidParameter)
	// ErrTooManyPolicies is returned when the max policy count is exceeded.
	ErrTooManyPolicies = awserr.New("InvalidRequestException", awserr.ErrInvalidParameter)
	// ErrBatchGetTracesLimit is returned when more than 5 trace IDs are requested.
	ErrBatchGetTracesLimit = awserr.New("InvalidRequestException", awserr.ErrInvalidParameter)
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

// SamplingRuleUpdate holds pointer-semantic updates for UpdateSamplingRule.
// A nil pointer means "no change"; a non-nil pointer (even to zero/empty) means "apply".
type SamplingRuleUpdate struct {
	ResourceARN   *string
	ServiceName   *string
	ServiceType   *string
	Host          *string
	HTTPMethod    *string
	URLPath       *string
	FixedRate     *float64
	Priority      *int32
	ReservoirSize *int32
}

// Segment is a parsed X-Ray segment document.
type Segment struct {
	AWS         map[string]any `json:"aws,omitempty"`
	Annotations map[string]any `json:"annotations,omitempty"`
	Metadata    map[string]any `json:"metadata,omitempty"`
	HTTP        *SegmentHTTP   `json:"http,omitempty"`
	Namespace   string         `json:"namespace,omitempty"`
	Document    string         `json:"-"`
	TraceID     string         `json:"trace_id"`
	ID          string         `json:"id"`
	ParentID    string         `json:"parent_id,omitempty"`
	Name        string         `json:"name"`
	Origin      string         `json:"origin,omitempty"`
	Subsegments []Segment      `json:"subsegments,omitempty"`
	StartTime   float64        `json:"start_time"`
	EndTime     float64        `json:"end_time,omitempty"`
	Error       bool           `json:"error"`
	Fault       bool           `json:"fault"`
	Throttle    bool           `json:"throttle"`
}

// SegmentHTTP holds HTTP request/response data from a segment.
type SegmentHTTP struct {
	Request  *SegmentHTTPRequest  `json:"request,omitempty"`
	Response *SegmentHTTPResponse `json:"response,omitempty"`
}

// SegmentHTTPRequest holds HTTP request fields from a segment.
type SegmentHTTPRequest struct {
	URL       string `json:"url,omitempty"`
	Method    string `json:"method,omitempty"`
	UserAgent string `json:"user_agent,omitempty"`
	ClientIP  string `json:"client_ip,omitempty"`
}

// SegmentHTTPResponse holds HTTP response fields from a segment.
type SegmentHTTPResponse struct {
	Status        int `json:"status,omitempty"`
	ContentLength int `json:"content_length,omitempty"`
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

// TelemetryRecord holds a single telemetry data point.
type TelemetryRecord struct {
	Timestamp              time.Time `json:"timestamp"`
	SegmentsReceivedCount  int32     `json:"segmentsReceivedCount"`
	SegmentsSentCount      int32     `json:"segmentsSentCount"`
	SegmentsSpilloverCount int32     `json:"segmentsSpilloverCount"`
	SegmentsRejectedCount  int32     `json:"segmentsRejectedCount"`
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
	// maxResourcePolicies is the maximum number of resource policies per account.
	maxResourcePolicies = 5
	// maxBatchGetTraces is the maximum number of trace IDs in a BatchGetTraces call.
	maxBatchGetTraces = 5
	// telemetryRingSize is the capacity of the telemetry ring buffer.
	telemetryRingSize = 100
	// maxServiceNameLen is the maximum length of a sampling rule ServiceName.
	maxServiceNameLen = 64
	// nanosPerSecond is the number of nanoseconds in a second.
	nanosPerSecond = 1e9
	// filterParts3 is the expected length of a 3-part filter expression.
	filterParts3 = 3
	// filterParts2 is the expected length of a 2-part filter expression.
	filterParts2 = 2
	// serviceGraphTotalCount is the key for the TotalCount stat in service graph nodes.
	serviceGraphTotalCount = "TotalCount"
)

// validKMSKeyID checks whether a KMS KeyId is in an acceptable format:
// alias/... | arn:aws:kms:... | UUID (hex with dashes, 36 chars).
//
//nolint:lll // regex is intentionally long; splitting would harm readability
var validKMSKeyID = regexp.MustCompile(
	`^(alias/[a-zA-Z0-9/_-]+|arn:aws:kms:[a-z0-9-]+:\d+:key/[a-zA-Z0-9/_-]+|[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})$`,
)

// InMemoryBackend is the in-memory store for X-Ray resources.
type InMemoryBackend struct {
	groups        map[string]*Group
	samplingRules map[string]*SamplingRule
	traces        map[string]*Trace
	// parsedSegments indexes segments by traceID+":"+segID
	parsedSegments map[string]*Segment
	// traceSegments maps traceID → list of segments (pointers into parsedSegments)
	traceSegments        map[string][]*Segment
	insights             map[string]*Insight
	insightEvents        map[string][]*InsightEvent
	resourcePolicies     map[string]*ResourcePolicy
	traceRetrievals      map[string]*TraceRetrieval
	retrievedTraces      map[string][]*Trace
	resourceTags         map[string]map[string]string
	encryptionConfig     *EncryptionConfig
	mu                   *lockmetrics.RWMutex
	traceSegmentDest     string
	indexingRules        []*IndexingRule
	lastRuleModification time.Time
	// samplingStats accumulates per-rule statistics from PutSamplingTargets docs.
	samplingStats map[string]*SamplingStatisticSummary
	// telemetry is a ring buffer of the last telemetryRingSize records.
	telemetry    []*TelemetryRecord
	telemetryIdx int
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		groups:           make(map[string]*Group),
		samplingRules:    make(map[string]*SamplingRule),
		traces:           make(map[string]*Trace),
		parsedSegments:   make(map[string]*Segment),
		traceSegments:    make(map[string][]*Segment),
		insights:         make(map[string]*Insight),
		insightEvents:    make(map[string][]*InsightEvent),
		resourcePolicies: make(map[string]*ResourcePolicy),
		traceRetrievals:  make(map[string]*TraceRetrieval),
		retrievedTraces:  make(map[string][]*Trace),
		resourceTags:     make(map[string]map[string]string),
		indexingRules:    defaultIndexingRules(),
		samplingStats:    make(map[string]*SamplingStatisticSummary),
		telemetry:        make([]*TelemetryRecord, telemetryRingSize),
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

// CreateGroupWithInsights creates a new group with full InsightsConfiguration.
func (b *InMemoryBackend) CreateGroupWithInsights(name, filterExpr string, ic InsightsConfiguration) (*Group, error) {
	b.mu.Lock("CreateGroupWithInsights")
	defer b.mu.Unlock()

	if _, ok := b.groups[name]; ok {
		return nil, fmt.Errorf("%w: group %s already exists", ErrGroupAlreadyExists, name)
	}

	g := &Group{
		GroupARN:              groupARN(name),
		GroupName:             name,
		FilterExpression:      filterExpr,
		InsightsConfiguration: ic,
		CreatedAt:             time.Now(),
	}
	b.groups[name] = g

	return cloneGroup(g), nil
}

// GetGroup returns the group with the given name, or by ARN if name is empty.
func (b *InMemoryBackend) GetGroup(name string) (*Group, error) {
	b.mu.RLock("GetGroup")
	defer b.mu.RUnlock()

	g, ok := b.groups[name]
	if !ok {
		return nil, fmt.Errorf("%w: group %s not found", ErrGroupNotFound, name)
	}

	return cloneGroup(g), nil
}

// GetGroupByARN returns the group with the given ARN.
func (b *InMemoryBackend) GetGroupByARN(arn string) (*Group, error) {
	b.mu.RLock("GetGroupByARN")
	defer b.mu.RUnlock()

	for _, g := range b.groups {
		if g.GroupARN == arn {
			return cloneGroup(g), nil
		}
	}

	return nil, fmt.Errorf("%w: group with ARN %s not found", ErrGroupNotFound, arn)
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

// UpdateGroupByARN updates a group by ARN or name.
func (b *InMemoryBackend) UpdateGroupByARN(name, arn, filterExpr string) (*Group, error) {
	b.mu.Lock("UpdateGroupByARN")
	defer b.mu.Unlock()

	var g *Group

	if arn != "" {
		for _, grp := range b.groups {
			if grp.GroupARN == arn {
				g = grp

				break
			}
		}
	} else {
		g = b.groups[name]
	}

	if g == nil {
		key := name
		if arn != "" {
			key = arn
		}

		return nil, fmt.Errorf("%w: group %s not found", ErrGroupNotFound, key)
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

// DeleteGroupByARN removes the group with the given ARN or name.
func (b *InMemoryBackend) DeleteGroupByARN(name, arn string) error {
	b.mu.Lock("DeleteGroupByARN")
	defer b.mu.Unlock()

	if arn != "" {
		for n, g := range b.groups {
			if g.GroupARN == arn {
				delete(b.groups, n)

				return nil
			}
		}

		return fmt.Errorf("%w: group with ARN %s not found", ErrGroupNotFound, arn)
	}

	if _, ok := b.groups[name]; !ok {
		return fmt.Errorf("%w: group %s not found", ErrGroupNotFound, name)
	}

	delete(b.groups, name)

	return nil
}

// ValidateSamplingRule validates sampling rule fields per AWS constraints.
func ValidateSamplingRule(rule SamplingRule) error {
	if rule.RuleName == "" || len(rule.RuleName) > 32 {
		return fmt.Errorf("%w: RuleName must be 1-32 characters", ErrInvalidSamplingRule)
	}

	if len(rule.ServiceName) > maxServiceNameLen {
		return fmt.Errorf("%w: ServiceName must be at most %d characters", ErrInvalidSamplingRule, maxServiceNameLen)
	}

	if rule.Priority < 1 || rule.Priority > 9999 {
		return fmt.Errorf("%w: Priority must be between 1 and 9999", ErrInvalidSamplingRule)
	}

	if rule.FixedRate < 0 || rule.FixedRate > 1.0 {
		return fmt.Errorf("%w: FixedRate must be between 0.0 and 1.0", ErrInvalidSamplingRule)
	}

	if rule.ReservoirSize < 0 {
		return fmt.Errorf("%w: ReservoirSize must be >= 0", ErrInvalidSamplingRule)
	}

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
	b.lastRuleModification = now

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
// It accepts a SamplingRule struct where non-zero values are applied (legacy API).
func (b *InMemoryBackend) UpdateSamplingRule(ruleName string, updates SamplingRule) (*SamplingRule, error) {
	b.mu.Lock("UpdateSamplingRule")
	defer b.mu.Unlock()

	r, ok := b.samplingRules[ruleName]
	if !ok {
		return nil, fmt.Errorf("%w: sampling rule %s not found", ErrSamplingRuleNotFound, ruleName)
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
	b.lastRuleModification = r.ModifiedAt

	return cloneRule(r), nil
}

// UpdateSamplingRuleWithPointers applies pointer-semantic updates so zero values apply.
func (b *InMemoryBackend) UpdateSamplingRuleWithPointers(
	ruleName string,
	updates SamplingRuleUpdate,
) (*SamplingRule, error) {
	b.mu.Lock("UpdateSamplingRuleWithPointers")
	defer b.mu.Unlock()

	r, ok := b.samplingRules[ruleName]
	if !ok {
		return nil, fmt.Errorf("%w: sampling rule %s not found", ErrSamplingRuleNotFound, ruleName)
	}

	if updates.FixedRate != nil {
		r.FixedRate = *updates.FixedRate
	}

	if updates.ReservoirSize != nil {
		r.ReservoirSize = *updates.ReservoirSize
	}

	if updates.ResourceARN != nil {
		r.ResourceARN = *updates.ResourceARN
	}

	if updates.ServiceName != nil {
		r.ServiceName = *updates.ServiceName
	}

	if updates.ServiceType != nil {
		r.ServiceType = *updates.ServiceType
	}

	if updates.Host != nil {
		r.Host = *updates.Host
	}

	if updates.HTTPMethod != nil {
		r.HTTPMethod = *updates.HTTPMethod
	}

	if updates.URLPath != nil {
		r.URLPath = *updates.URLPath
	}

	if updates.Priority != nil {
		r.Priority = *updates.Priority
	}

	r.ModifiedAt = time.Now()
	b.lastRuleModification = r.ModifiedAt

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
	b.lastRuleModification = time.Now()

	return deleted, nil
}

// segmentHeader is used to extract the trace_id from a raw segment JSON.
type segmentHeader struct {
	TraceID string `json:"trace_id"`
	ID      string `json:"id"`
}

// PutTraceSegments stores raw segment JSON strings, parses them into typed Segment structs,
// and returns the list of unprocessed segment IDs (empty slice means all segments were accepted).
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

		// Cap per-trace segment count.
		if len(t.Segments) >= segmentCompactionHighWater {
			trimmed := make([]string, maxSegmentsPerTrace, segmentCompactionHighWater)
			copy(trimmed, t.Segments[len(t.Segments)-maxSegmentsPerTrace:])
			t.Segments = trimmed
		}

		t.Segments = append(t.Segments, seg)

		// Parse into typed Segment struct and index it.
		var parsed Segment
		if err := json.Unmarshal([]byte(seg), &parsed); err == nil {
			parsed.Document = seg

			segKey := hdr.TraceID + ":" + parsed.ID
			b.parsedSegments[segKey] = &parsed
			b.traceSegments[hdr.TraceID] = append(b.traceSegments[hdr.TraceID], &parsed)

			// Update trace StartTime from the earliest segment start_time.
			if parsed.StartTime > 0 {
				segStart := time.Unix(
					int64(parsed.StartTime),
					int64((parsed.StartTime-math.Floor(parsed.StartTime))*nanosPerSecond),
				)
				if segStart.Before(t.StartTime) {
					t.StartTime = segStart
				}
			}
		}
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

// GetParsedSegments returns a copy of the parsed segments for a given trace ID.
func (b *InMemoryBackend) GetParsedSegments(traceID string) []*Segment {
	b.mu.RLock("GetParsedSegments")
	defer b.mu.RUnlock()

	segs := b.traceSegments[traceID]
	out := make([]*Segment, len(segs))
	copy(out, segs)

	return out
}

// GetAllParsedSegments returns all parsed segments.
func (b *InMemoryBackend) GetAllParsedSegments() map[string][]*Segment {
	b.mu.RLock("GetAllParsedSegments")
	defer b.mu.RUnlock()

	out := make(map[string][]*Segment, len(b.traceSegments))
	for k, v := range b.traceSegments {
		cp := make([]*Segment, len(v))
		copy(cp, v)
		out[k] = cp
	}

	return out
}

// Reset clears all backend state, resetting to an empty store.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.groups = make(map[string]*Group)
	b.samplingRules = make(map[string]*SamplingRule)
	b.traces = make(map[string]*Trace)
	b.parsedSegments = make(map[string]*Segment)
	b.traceSegments = make(map[string][]*Segment)
	b.insights = make(map[string]*Insight)
	b.insightEvents = make(map[string][]*InsightEvent)
	b.resourcePolicies = make(map[string]*ResourcePolicy)
	b.traceRetrievals = make(map[string]*TraceRetrieval)
	b.samplingStats = make(map[string]*SamplingStatisticSummary)
	b.telemetry = make([]*TelemetryRecord, telemetryRingSize)
	b.telemetryIdx = 0
	b.indexingRules = defaultIndexingRules()
	b.encryptionConfig = &EncryptionConfig{Type: "NONE", Status: statusActive}
	b.lastRuleModification = time.Time{}
}

// GetEncryptionConfig returns the current X-Ray encryption configuration.
// If the current status is UPDATING, this call advances it to ACTIVE.
func (b *InMemoryBackend) GetEncryptionConfig() *EncryptionConfig {
	b.mu.Lock("GetEncryptionConfig")
	defer b.mu.Unlock()

	if b.encryptionConfig.Status == statusUpdating {
		b.encryptionConfig.Status = statusActive
	}

	cp := *b.encryptionConfig

	return &cp
}

// PutEncryptionConfig updates the X-Ray encryption configuration.
// encType must be one of "NONE" or "KMS". keyID is only used when encType is "KMS".
// When encType is KMS the keyID must match alias/..., ARN, or UUID format.
// The status is initially set to UPDATING; the next GET will advance it to ACTIVE.
func (b *InMemoryBackend) PutEncryptionConfig(encType, keyID string) (*EncryptionConfig, error) {
	if encType != encTypeNone && encType != encTypeKMS {
		return nil, fmt.Errorf("%w: Type must be NONE or KMS", ErrValidation)
	}

	if encType == encTypeKMS {
		if keyID == "" {
			return nil, fmt.Errorf("%w: KeyId is required when Type is KMS", ErrValidation)
		}

		if !validKMSKeyID.MatchString(keyID) {
			return nil, fmt.Errorf("%w: KeyId must be alias/..., key ARN, or UUID", ErrValidation)
		}
	}

	b.mu.Lock("PutEncryptionConfig")
	defer b.mu.Unlock()

	status := statusActive

	if encType == encTypeKMS {
		status = statusUpdating
	}

	b.encryptionConfig = &EncryptionConfig{
		Type:   encType,
		KeyID:  keyID,
		Status: status,
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

// isValidInsightState returns true if s is a recognised insight state name.
func isValidInsightState(s string) bool {
	return s == statusActive || s == "CLOSED"
}

// GetInsightSummaries returns all insights as summaries, optionally filtered by state.
// If states is empty, all insights are returned. "ALL" matches both ACTIVE and CLOSED.
// Unknown states return ErrValidation.
func (b *InMemoryBackend) GetInsightSummaries(states []string) ([]Insight, error) {
	b.mu.RLock("GetInsightSummaries")
	defer b.mu.RUnlock()

	// Validate states and resolve ALL.
	wantAll := len(states) == 0
	stateSet := make(map[string]bool, len(states))

	for _, s := range states {
		if s == "ALL" {
			wantAll = true

			continue
		}

		if !isValidInsightState(s) {
			return nil, fmt.Errorf("%w: unknown insight state %q", ErrValidation, s)
		}

		stateSet[s] = true
	}

	out := make([]Insight, 0, len(b.insights))

	for _, i := range b.insights {
		if !wantAll && !stateSet[i.State] {
			continue
		}

		out = append(out, *cloneInsight(i))
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].InsightID < out[j].InsightID
	})

	return out, nil
}

// --- Resource policy operations ---

func cloneResourcePolicy(p *ResourcePolicy) *ResourcePolicy {
	cp := *p

	return &cp
}

// PutResourcePolicy creates or updates a resource policy with the given name and document.
// Returns ErrTooManyPolicies if the account already has maxResourcePolicies.
// Returns ErrInvalidPolicyRevisionID if revisionID doesn't match the stored one.
// Returns ErrMalformedPolicyDocument if policyDocument is not valid JSON.
func (b *InMemoryBackend) PutResourcePolicy(policyName, policyDocument, revisionID string) (*ResourcePolicy, error) {
	// Validate JSON.
	var js json.RawMessage
	if err := json.Unmarshal([]byte(policyDocument), &js); err != nil {
		return nil, fmt.Errorf("%w: policy document is not valid JSON: %w", ErrMalformedPolicyDocument, err)
	}

	b.mu.Lock("PutResourcePolicy")
	defer b.mu.Unlock()

	existing, exists := b.resourcePolicies[policyName]

	if !exists && len(b.resourcePolicies) >= maxResourcePolicies {
		return nil, fmt.Errorf(
			"%w: maximum of %d resource policies per account",
			ErrTooManyPolicies,
			maxResourcePolicies,
		)
	}

	// Revision ID check: if a revision is provided it must match the stored one.
	if revisionID != "" && exists && existing.PolicyRevisionID != revisionID {
		return nil, fmt.Errorf("%w: policy revision ID does not match", ErrInvalidPolicyRevisionID)
	}

	p := &ResourcePolicy{
		PolicyName:       policyName,
		PolicyDocument:   policyDocument,
		PolicyRevisionID: uuid.NewString(),
	}
	b.resourcePolicies[policyName] = p

	return cloneResourcePolicy(p), nil
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

// GetSamplingStatisticSummaries returns accumulated sampling statistic summaries.
func (b *InMemoryBackend) GetSamplingStatisticSummaries() []SamplingStatisticSummary {
	b.mu.RLock("GetSamplingStatisticSummaries")
	defer b.mu.RUnlock()

	out := make([]SamplingStatisticSummary, 0, len(b.samplingStats))
	for _, s := range b.samplingStats {
		cp := *s
		out = append(out, cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].RuleName < out[j].RuleName
	})

	return out
}

// SamplingStatisticsDocument is a single document submitted in GetSamplingTargets.
type SamplingStatisticsDocument struct {
	RuleName     string
	ClientID     string
	RequestCount int32
	SampledCount int32
	BorrowCount  int32
}

// SamplingTargetResult holds the per-document results of GetSamplingTargets.
type SamplingTargetResult struct {
	ReservoirQuotaTTL time.Time
	RuleName          string
	FixedRate         float64
	ReservoirSize     int32
}

// UnprocessedStatisticsResult holds results for unknown rule names.
type UnprocessedStatisticsResult struct {
	RuleName  string
	ErrorCode string
	Message   string
}

// GetSamplingTargets returns target documents for the provided stat documents.
// Rules that do not exist are returned in the unprocessed list.
// Statistics from known rules are accumulated for GetSamplingStatisticSummaries.
func (b *InMemoryBackend) GetSamplingTargets(
	docs []SamplingStatisticsDocument,
) ([]SamplingTargetResult, []UnprocessedStatisticsResult) {
	b.mu.Lock("GetSamplingTargets")
	defer b.mu.Unlock()

	targets := make([]SamplingTargetResult, 0, len(docs))
	unprocessed := make([]UnprocessedStatisticsResult, 0)

	for _, d := range docs {
		r, ok := b.samplingRules[d.RuleName]
		if !ok {
			unprocessed = append(unprocessed, UnprocessedStatisticsResult{
				RuleName:  d.RuleName,
				ErrorCode: "404",
				Message:   "Rule not found",
			})

			continue
		}

		// Accumulate statistics.
		if existing := b.samplingStats[d.RuleName]; existing != nil {
			existing.RequestCount += d.RequestCount
			existing.SampledCount += d.SampledCount
			existing.BorrowCount += d.BorrowCount
			existing.Timestamp = time.Now()
		} else {
			b.samplingStats[d.RuleName] = &SamplingStatisticSummary{
				RuleName:     d.RuleName,
				RequestCount: d.RequestCount,
				SampledCount: d.SampledCount,
				BorrowCount:  d.BorrowCount,
				Timestamp:    time.Now(),
			}
		}

		targets = append(targets, SamplingTargetResult{
			RuleName:          r.RuleName,
			FixedRate:         r.FixedRate,
			ReservoirSize:     r.ReservoirSize,
			ReservoirQuotaTTL: time.Now().Add(samplingTargetInterval * time.Second),
		})
	}

	return targets, unprocessed
}

// LastRuleModification returns the timestamp of the last sampling rule modification.
func (b *InMemoryBackend) LastRuleModification() time.Time {
	b.mu.RLock("LastRuleModification")
	defer b.mu.RUnlock()

	return b.lastRuleModification
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

// --- serviceNode is used to build the service graph ---

type serviceNode struct {
	Name          string
	Type          string
	ReferenceID   int
	OkCount       int64
	ErrorCount    int64
	ThrottleCount int64
	FaultCount    int64
	TotalCount    int64
	TotalRespTime float64
	StartTime     float64
	EndTime       float64
	IsRoot        bool
}

// serviceKey identifies a unique service node in the service graph.
type serviceKey struct{ Name, Type string }

// edgeKey identifies a directed edge between two service nodes.
type edgeKey struct{ From, To serviceKey }

// accumulateServiceNodes builds nodeMap and segToService from the trace segments.
func accumulateServiceNodes(
	traceSegs map[string][]*Segment,
) (map[serviceKey]*serviceNode, map[string]serviceKey) {
	nodeMap := map[serviceKey]*serviceNode{}
	segToService := map[string]serviceKey{}
	refID := 0

	for _, segs := range traceSegs {
		for _, seg := range segs {
			svcType := seg.Origin
			if svcType == "" {
				svcType = seg.Namespace
			}

			key := serviceKey{Name: seg.Name, Type: svcType}

			node, ok := nodeMap[key]
			if !ok {
				node = &serviceNode{Name: seg.Name, Type: svcType, ReferenceID: refID}
				refID++
				nodeMap[key] = node
			}

			accumulateNodeStats(node, seg)
			segToService[seg.ID] = key
		}
	}

	return nodeMap, segToService
}

// accumulateNodeStats updates a service node with stats from a single segment.
func accumulateNodeStats(node *serviceNode, seg *Segment) {
	if seg.StartTime > 0 && (node.StartTime == 0 || seg.StartTime < node.StartTime) {
		node.StartTime = seg.StartTime
	}

	if seg.EndTime > node.EndTime {
		node.EndTime = seg.EndTime
	}

	node.TotalCount++

	switch {
	case seg.Fault:
		node.FaultCount++
	case seg.Error && seg.Throttle:
		node.ThrottleCount++
	case seg.Error:
		node.ErrorCount++
	default:
		node.OkCount++
	}

	if seg.EndTime > 0 && seg.StartTime > 0 {
		node.TotalRespTime += seg.EndTime - seg.StartTime
	}

	if seg.ParentID == "" {
		node.IsRoot = true
	}
}

// buildEdgeSet returns the set of directed edges between service nodes.
func buildEdgeSet(
	traceSegs map[string][]*Segment,
	segToService map[string]serviceKey,
) map[edgeKey]bool {
	edgeSet := map[edgeKey]bool{}

	for _, segs := range traceSegs {
		for _, seg := range segs {
			if seg.ParentID == "" {
				continue
			}

			parentKey, ok := segToService[seg.ParentID]
			if !ok {
				continue
			}

			childKey := segToService[seg.ID]
			if childKey != parentKey {
				edgeSet[edgeKey{From: childKey, To: parentKey}] = true
			}
		}
	}

	return edgeSet
}

// nodeToView converts a service node to its JSON output representation.
func nodeToView(
	key serviceKey,
	node *serviceNode,
	edgeSet map[edgeKey]bool,
	nodeMap map[serviceKey]*serviceNode,
) map[string]any {
	nodeEdges := make([]map[string]any, 0)

	for e := range edgeSet {
		if e.From == key {
			to := nodeMap[e.To]
			nodeEdges = append(nodeEdges, map[string]any{
				"ReferenceId": to.ReferenceID,
			})
		}
	}

	return map[string]any{
		"ReferenceId": node.ReferenceID,
		"Name":        node.Name,
		"Type":        node.Type,
		"State":       "active",
		"Root":        node.IsRoot,
		"StartTime":   node.StartTime,
		"EndTime":     node.EndTime,
		"Edges":       nodeEdges,
		"SummaryStatistics": map[string]any{
			"OkCount": node.OkCount,
			"ErrorStatistics": map[string]any{
				"ThrottleCount":        node.ThrottleCount,
				"OtherCount":           node.ErrorCount,
				serviceGraphTotalCount: node.ThrottleCount + node.ErrorCount,
			},
			"FaultStatistics": map[string]any{
				serviceGraphTotalCount: node.FaultCount,
			},
			serviceGraphTotalCount: node.TotalCount,
			"TotalResponseTime":    node.TotalRespTime,
			"DurationHistogram":    []any{},
		},
	}
}

// buildServiceGraph builds service nodes from a map of traceID → segments.
func buildServiceGraph(traceSegs map[string][]*Segment) []map[string]any {
	nodeMap, segToService := accumulateServiceNodes(traceSegs)
	edgeSet := buildEdgeSet(traceSegs, segToService)

	nodes := make([]map[string]any, 0, len(nodeMap))

	for key, node := range nodeMap {
		nodes = append(nodes, nodeToView(key, node, edgeSet, nodeMap))
	}

	sort.Slice(nodes, func(i, j int) bool {
		ri, _ := nodes[i]["ReferenceId"].(int)
		rj, _ := nodes[j]["ReferenceId"].(int)

		return ri < rj
	})

	return nodes
}

// GetServiceGraph returns a service graph derived from stored traces in the time window.
func (b *InMemoryBackend) GetServiceGraph(startTime, endTime time.Time) []map[string]any {
	b.mu.RLock("GetServiceGraph")
	defer b.mu.RUnlock()

	// Filter segments to those within the time window.
	filtered := map[string][]*Segment{}

	for traceID, segs := range b.traceSegments {
		var inWindow []*Segment

		for _, seg := range segs {
			if seg.StartTime == 0 {
				inWindow = append(inWindow, seg)

				continue
			}

			t := time.Unix(int64(seg.StartTime), 0)
			if !t.Before(startTime) && !t.After(endTime) {
				inWindow = append(inWindow, seg)
			}
		}

		if len(inWindow) > 0 {
			filtered[traceID] = inWindow
		}
	}

	if len(filtered) == 0 {
		return []map[string]any{}
	}

	return buildServiceGraph(filtered)
}

// GetTraceGraph returns a service graph scoped to the given trace IDs.
func (b *InMemoryBackend) GetTraceGraph(traceIDs []string) []map[string]any {
	b.mu.RLock("GetTraceGraph")
	defer b.mu.RUnlock()

	filtered := map[string][]*Segment{}

	for _, id := range traceIDs {
		if segs, ok := b.traceSegments[id]; ok {
			filtered[id] = segs
		}
	}

	if len(filtered) == 0 {
		return []map[string]any{}
	}

	return buildServiceGraph(filtered)
}

// GetTimeSeriesServiceStatistics returns per-period bucketed statistics.
// tsBucket accumulates time-series statistics for one time period.
type tsBucket struct {
	OkCount       int64
	ErrorCount    int64
	ThrottleCount int64
	FaultCount    int64
	TotalCount    int64
	TotalRespTime float64
}

// accumulateToBucket adds one segment's stats into the appropriate time bucket.
func accumulateToBucket(buckets map[int64]*tsBucket, seg *Segment, period int) {
	bk := (int64(seg.StartTime) / int64(period)) * int64(period)

	bkt := buckets[bk]
	if bkt == nil {
		bkt = &tsBucket{}
		buckets[bk] = bkt
	}

	bkt.TotalCount++

	switch {
	case seg.Fault:
		bkt.FaultCount++
	case seg.Error && seg.Throttle:
		bkt.ThrottleCount++
	case seg.Error:
		bkt.ErrorCount++
	default:
		bkt.OkCount++
	}

	if seg.EndTime > seg.StartTime {
		bkt.TotalRespTime += seg.EndTime - seg.StartTime
	}
}

// tsBucketToView converts a tsBucket to its JSON output map.
func tsBucketToView(k int64, bkt *tsBucket) map[string]any {
	return map[string]any{
		"Timestamp": float64(k),
		"ServiceSummaryStatistics": map[string]any{
			"OkCount": bkt.OkCount,
			"ErrorStatistics": map[string]any{
				"ThrottleCount":        bkt.ThrottleCount,
				"OtherCount":           bkt.ErrorCount,
				serviceGraphTotalCount: bkt.ThrottleCount + bkt.ErrorCount,
			},
			"FaultStatistics": map[string]any{
				serviceGraphTotalCount: bkt.FaultCount,
			},
			serviceGraphTotalCount: bkt.TotalCount,
			"TotalResponseTime":    bkt.TotalRespTime,
		},
	}
}

// GetTimeSeriesServiceStatistics returns per-period bucketed statistics for segments in the time window.
func (b *InMemoryBackend) GetTimeSeriesServiceStatistics(startTime, endTime time.Time, period int) []map[string]any {
	b.mu.RLock("GetTimeSeriesServiceStatistics")
	defer b.mu.RUnlock()

	if period <= 0 {
		period = 60
	}

	buckets := map[int64]*tsBucket{}

	for _, segs := range b.traceSegments {
		for _, seg := range segs {
			if seg.StartTime == 0 {
				continue
			}

			t := time.Unix(int64(seg.StartTime), 0)
			if t.Before(startTime) || t.After(endTime) {
				continue
			}

			accumulateToBucket(buckets, seg, period)
		}
	}

	keys := make([]int64, 0, len(buckets))
	for k := range buckets {
		keys = append(keys, k)
	}

	slices.Sort(keys)

	out := make([]map[string]any, 0, len(keys))
	for _, k := range keys {
		out = append(out, tsBucketToView(k, buckets[k]))
	}

	return out
}

// --- Trace segment destination operations ---

// GetTraceSegmentDestination returns the current trace segment destination.
func (b *InMemoryBackend) GetTraceSegmentDestination() string {
	b.mu.RLock("GetTraceSegmentDestination")
	defer b.mu.RUnlock()

	if b.traceSegmentDest == "" {
		return "XRay"
	}

	return b.traceSegmentDest
}

// UpdateTraceSegmentDestination sets the trace segment destination and returns it.
func (b *InMemoryBackend) UpdateTraceSegmentDestination(destination string) string {
	b.mu.Lock("UpdateTraceSegmentDestination")
	defer b.mu.Unlock()

	b.traceSegmentDest = destination

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

// PutTelemetryRecords stores telemetry records in a ring buffer.
func (b *InMemoryBackend) PutTelemetryRecords(records []TelemetryRecord) {
	b.mu.Lock("PutTelemetryRecords")
	defer b.mu.Unlock()

	for i := range records {
		b.telemetry[b.telemetryIdx%telemetryRingSize] = &records[i]
		b.telemetryIdx++
	}
}

// TraceSummaryData holds derived data for GetTraceSummaries response.
type TraceSummaryData struct {
	Annotations  map[string]any
	HTTP         *TraceSummaryHTTP
	TraceID      string
	EntryPoint   string
	ServiceIDs   []TraceSummaryServiceID
	Duration     float64
	ResponseTime float64
	ApproxTime   float64
	HasFault     bool
	HasError     bool
	HasThrottle  bool
	IsPartial    bool
}

// TraceSummaryHTTP holds HTTP fields for a trace summary.
type TraceSummaryHTTP struct {
	HTTPURL    string
	HTTPMethod string
	ClientIP   string
	UserAgent  string
	HTTPStatus int
}

// TraceSummaryServiceID is a service identifier in a trace summary.
type TraceSummaryServiceID struct {
	Name string
	Type string
}

// extractRootHTTP populates HTTP fields from the root segment's HTTP data.
// Returns the updated (or newly created) TraceSummaryHTTP pointer.
func extractRootHTTP(segHTTP *SegmentHTTP, existing *TraceSummaryHTTP) *TraceSummaryHTTP {
	if segHTTP == nil {
		return existing
	}

	if segHTTP.Request != nil {
		if existing == nil {
			existing = &TraceSummaryHTTP{}
		}

		existing.HTTPURL = segHTTP.Request.URL
		existing.HTTPMethod = segHTTP.Request.Method
		existing.ClientIP = segHTTP.Request.ClientIP
		existing.UserAgent = segHTTP.Request.UserAgent
	}

	if segHTTP.Response != nil {
		if existing == nil {
			existing = &TraceSummaryHTTP{}
		}

		existing.HTTPStatus = segHTTP.Response.Status
	}

	return existing
}

// BuildTraceSummary derives TraceSummaryData from parsed segments.
func BuildTraceSummary(traceID string, segs []*Segment) TraceSummaryData {
	summary := TraceSummaryData{
		TraceID:     traceID,
		ApproxTime:  float64(time.Now().Unix()),
		Annotations: map[string]any{},
	}

	if len(segs) == 0 {
		summary.IsPartial = true

		return summary
	}

	var minStart, maxEnd float64

	seen := map[serviceKey]bool{}
	hasRoot := false

	for _, seg := range segs {
		accumulateTraceSummaryFlags(&summary, seg)

		if seg.StartTime > 0 && (minStart == 0 || seg.StartTime < minStart) {
			minStart = seg.StartTime
		}

		if seg.EndTime > maxEnd {
			maxEnd = seg.EndTime
		}

		maps.Copy(summary.Annotations, seg.Annotations)

		svcType := seg.Origin
		if svcType == "" {
			svcType = seg.Namespace
		}

		key := serviceKey{Name: seg.Name, Type: svcType}
		if !seen[key] {
			seen[key] = true
			summary.ServiceIDs = append(summary.ServiceIDs, TraceSummaryServiceID{Name: seg.Name, Type: svcType})
		}

		// Root segment has no parent.
		if seg.ParentID == "" {
			hasRoot = true
			summary.EntryPoint = seg.Name
			summary.HTTP = extractRootHTTP(seg.HTTP, summary.HTTP)
		}
	}

	if !hasRoot {
		summary.IsPartial = true
	}

	if maxEnd > 0 && minStart > 0 {
		summary.Duration = maxEnd - minStart
	}

	// ResponseTime: root segment duration or overall duration.
	summary.ResponseTime = summary.Duration

	return summary
}

// accumulateTraceSummaryFlags copies boolean fault/error/throttle flags from a segment.
func accumulateTraceSummaryFlags(summary *TraceSummaryData, seg *Segment) {
	if seg.Fault {
		summary.HasFault = true
	}

	if seg.Error {
		summary.HasError = true
	}

	if seg.Throttle {
		summary.HasThrottle = true
	}
}

// evaluateHTTPStatusFilter matches `http.status = N` expressions.
func evaluateHTTPStatusFilter(expr string, http *TraceSummaryHTTP) bool {
	parts := strings.Fields(expr)
	if len(parts) != filterParts3 || parts[1] != "=" {
		return false
	}

	n, err := strconv.Atoi(parts[2])
	if err != nil || http == nil {
		return false
	}

	return http.HTTPStatus == n
}

// compareResponseTime applies a comparison operator to response time.
func compareResponseTime(op string, rt, n float64) bool {
	switch op {
	case ">":
		return rt > n
	case ">=":
		return rt >= n
	case "<":
		return rt < n
	case "<=":
		return rt <= n
	case "=":
		return rt == n
	}

	return false
}

// evaluateResponseTimeFilter matches `responsetime OP N.N` expressions.
func evaluateResponseTimeFilter(expr string, rt float64) bool {
	parts := strings.Fields(expr)
	if len(parts) != filterParts3 {
		return false
	}

	n, err := strconv.ParseFloat(parts[2], 64)
	if err != nil {
		return false
	}

	return compareResponseTime(parts[1], rt, n)
}

// evaluateAnnotationFilter matches `annotation.KEY = "VALUE"` expressions.
func evaluateAnnotationFilter(expr string, annotations map[string]any) bool {
	parts := strings.SplitN(expr, "=", filterParts2)
	if len(parts) != filterParts2 {
		return false
	}

	key := strings.TrimSpace(parts[0])
	key = key[len("annotation."):]
	val := strings.TrimSpace(parts[1])
	val = strings.Trim(val, `"`)

	v, ok := annotations[key]

	return ok && fmt.Sprintf("%v", v) == val
}

// evaluateFilter checks a trace summary against a simple filter expression.
// Supported tokens:
//   - `fault`                      — trace has fault
//   - `error`                      — trace has error
//   - `http.status = N`            — HTTP status equals N
//   - `responsetime > N.N`         — response time comparison (also >=, <, <=, =)
//   - `annotation.KEY = "VALUE"`   — annotation match
//
// Empty expression always returns true.
func evaluateFilter(expr string, summary TraceSummaryData) bool {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return true
	}

	lower := strings.ToLower(expr)

	switch lower {
	case "fault":
		return summary.HasFault
	case "error":
		return summary.HasError
	case "throttle":
		return summary.HasThrottle
	}

	if strings.HasPrefix(lower, "http.status") {
		return evaluateHTTPStatusFilter(expr, summary.HTTP)
	}

	if strings.HasPrefix(lower, "responsetime") {
		return evaluateResponseTimeFilter(expr, summary.ResponseTime)
	}

	if strings.HasPrefix(lower, "annotation.") {
		return evaluateAnnotationFilter(expr, summary.Annotations)
	}

	return false
}

// EvaluateFilter is exported for tests.
func EvaluateFilter(expr string, summary TraceSummaryData) bool {
	return evaluateFilter(expr, summary)
}
