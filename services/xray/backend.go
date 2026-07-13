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
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	statusActive   = "ACTIVE"
	statusUpdating = "UPDATING"

	// defaultSamplingRuleName is the name of the built-in default sampling rule.
	// AWS X-Ray always maintains this rule and it cannot be deleted.
	defaultSamplingRuleName = "Default"
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
	// ErrDefaultRuleUndeletable is returned when the built-in Default sampling rule is deleted.
	ErrDefaultRuleUndeletable = awserr.New("InvalidRequestException", awserr.ErrInvalidParameter)
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
	EndTime   time.Time `json:"endTime,omitzero"`
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
	// defaultFixedRate is the FixedRate of the built-in Default sampling rule.
	defaultFixedRate = 0.05
	// defaultSamplingPriority is the priority of the built-in Default sampling rule.
	defaultSamplingPriority = int32(10000)
	// insightFaultThreshold is the fault rate that triggers an insight (5%).
	insightFaultThreshold = 0.05
	// insightMinRequests is the minimum number of requests before an insight fires.
	insightMinRequests = int64(10)
	// insightWindowDuration is the rolling window for fault rate tracking.
	insightWindowDuration = 60 * time.Second
	// pctMultiplier converts a 0-1 fraction to a percentage for display.
	pctMultiplier = 100.0
)

// validKMSKeyID checks whether a KMS KeyId is in an acceptable format:
// alias/... | arn:aws:kms:... | UUID (hex with dashes, 36 chars).
//
//nolint:lll // regex is intentionally long; splitting would harm readability
var validKMSKeyID = regexp.MustCompile(
	`^(alias/[a-zA-Z0-9/_-]+|arn:aws:kms:[a-z0-9-]+:\d+:key/[a-zA-Z0-9/_-]+|[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})$`,
)

// serviceInsightWindow tracks fault/error rates per service for insight detection.
type serviceInsightWindow struct {
	WindowStart time.Time
	InsightID   string
	// Name is the service name this window tracks. It is the store.Table
	// identity key; serviceWindows is ephemeral insight-detection state that
	// is never persisted (reset fresh on Restore), so Name is tagged json:"-"
	// defensively even though it is never actually marshaled.
	Name       string `json:"-"`
	Total      int64
	FaultCount int64
}

// InMemoryBackend is the in-memory store for X-Ray resources.
type InMemoryBackend struct {
	lastRuleModification time.Time
	// registry is the lifecycle registry for every store.Table below. It
	// collapses Reset() to one registry.ResetAll() call instead of one
	// hand-written make() per map. Snapshot/Restore deliberately do NOT use
	// registry.SnapshotAll()/RestoreAll() wholesale -- parsedSegments and
	// serviceWindows are excluded from persistence (see persistence.go) so
	// their JSON round-trip is driven by hand, table by table.
	registry *store.Registry
	// groupsByARN is a secondary index on groups, keyed by GroupARN.
	groupsByARN *store.Index[Group]
	// retrievedTraces is keyed by retrieval token; its values are slices,
	// not *T, so it is left as a plain map (see store_setup.go).
	retrievedTraces map[string][]*Trace
	parsedSegments  *store.Table[Segment]
	// traceSegments is a secondary index on parsedSegments, keyed by TraceID
	// ("segments of a trace").
	traceSegments *store.Index[Segment]
	insights      *store.Table[Insight]
	// insightEvents is keyed by insight ID; its values are slices, not *T,
	// so it is left as a plain map (see store_setup.go).
	insightEvents    map[string][]*InsightEvent
	resourcePolicies *store.Table[ResourcePolicy]
	// retrievalTimes is map[string]time.Time -- not a *T map -- so it is
	// left as a plain map (see store_setup.go).
	retrievalTimes map[string]time.Time
	groups         *store.Table[Group]
	// resourceTags is map[string]map[string]string -- not a *T map -- so it
	// is left as a plain map (see store_setup.go).
	resourceTags     map[string]map[string]string
	traces           *store.Table[Trace]
	samplingStats    *store.Table[SamplingStatisticSummary]
	traceRetrievals  *store.Table[TraceRetrieval]
	serviceWindows   *store.Table[serviceInsightWindow]
	encryptionConfig *EncryptionConfig
	mu               *lockmetrics.RWMutex
	samplingRules    *store.Table[SamplingRule]
	traceSegmentDest string
	region           string
	accountID        string
	telemetry        []*TelemetryRecord
	indexingRules    []*IndexingRule
	telemetryIdx     int
}

// defaultSamplingRule returns the built-in X-Ray sampling rule that is always
// present. The "Default" rule matches all requests and has the lowest
// priority (10000).
func (b *InMemoryBackend) defaultSamplingRule() *SamplingRule {
	now := time.Now()

	return &SamplingRule{
		RuleName:      defaultSamplingRuleName,
		RuleARN:       b.samplingRuleARN(defaultSamplingRuleName),
		ResourceARN:   "*",
		ServiceName:   "*",
		ServiceType:   "*",
		Host:          "*",
		HTTPMethod:    "*",
		URLPath:       "*",
		FixedRate:     defaultFixedRate,
		Priority:      defaultSamplingPriority,
		ReservoirSize: 1,
		CreatedAt:     now,
		ModifiedAt:    now,
	}
}

// NewInMemoryBackend creates a new InMemoryBackend with the given accountID and region.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:        store.NewRegistry(),
		insightEvents:   make(map[string][]*InsightEvent),
		retrievedTraces: make(map[string][]*Trace),
		resourceTags:    make(map[string]map[string]string),
		indexingRules:   defaultIndexingRules(),
		telemetry:       make([]*TelemetryRecord, telemetryRingSize),
		mu:              lockmetrics.New("xray"),
		encryptionConfig: &EncryptionConfig{
			Type:   "NONE",
			Status: statusActive,
		},
		region:         region,
		accountID:      accountID,
		retrievalTimes: make(map[string]time.Time),
	}
	registerAllTables(b)
	b.samplingRules.Put(b.defaultSamplingRule())

	return b
}

// defaultIndexingRules returns the built-in X-Ray indexing rules.
func defaultIndexingRules() []*IndexingRule {
	now := time.Now()

	return []*IndexingRule{
		{Name: "Default", ModifiedAt: now},
	}
}

func (b *InMemoryBackend) groupARN(name string) string {
	return "arn:aws:xray:" + b.region + ":" + b.accountID + ":group/default/" + name
}

func (b *InMemoryBackend) samplingRuleARN(name string) string {
	return "arn:aws:xray:" + b.region + ":" + b.accountID + ":sampling-rule/" + name
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

	if b.groups.Has(name) {
		return nil, fmt.Errorf("%w: group %s already exists", ErrGroupAlreadyExists, name)
	}

	g := &Group{
		GroupARN:         b.groupARN(name),
		GroupName:        name,
		FilterExpression: filterExpr,
		CreatedAt:        time.Now(),
	}
	b.groups.Put(g)

	return cloneGroup(g), nil
}

// CreateGroupWithInsights creates a new group with full InsightsConfiguration.
func (b *InMemoryBackend) CreateGroupWithInsights(name, filterExpr string, ic InsightsConfiguration) (*Group, error) {
	b.mu.Lock("CreateGroupWithInsights")
	defer b.mu.Unlock()

	if b.groups.Has(name) {
		return nil, fmt.Errorf("%w: group %s already exists", ErrGroupAlreadyExists, name)
	}

	g := &Group{
		GroupARN:              b.groupARN(name),
		GroupName:             name,
		FilterExpression:      filterExpr,
		InsightsConfiguration: ic,
		CreatedAt:             time.Now(),
	}
	b.groups.Put(g)

	return cloneGroup(g), nil
}

// GetGroup returns the group with the given name, or by ARN if name is empty.
func (b *InMemoryBackend) GetGroup(name string) (*Group, error) {
	b.mu.RLock("GetGroup")
	defer b.mu.RUnlock()

	g, ok := b.groups.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: group %s not found", ErrGroupNotFound, name)
	}

	return cloneGroup(g), nil
}

// GetGroupByARN returns the group with the given ARN.
func (b *InMemoryBackend) GetGroupByARN(arn string) (*Group, error) {
	b.mu.RLock("GetGroupByARN")
	defer b.mu.RUnlock()

	if list := b.groupsByARN.Get(arn); len(list) > 0 {
		return cloneGroup(list[0]), nil
	}

	return nil, fmt.Errorf("%w: group with ARN %s not found", ErrGroupNotFound, arn)
}

// GetGroups returns all groups sorted by name.
func (b *InMemoryBackend) GetGroups() []Group {
	b.mu.RLock("GetGroups")
	defer b.mu.RUnlock()

	all := b.groups.All()
	out := make([]Group, 0, len(all))

	for _, g := range all {
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

	g, ok := b.groups.Get(name)
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
		if list := b.groupsByARN.Get(arn); len(list) > 0 {
			g = list[0]
		}
	} else {
		g, _ = b.groups.Get(name)
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

	if !b.groups.Delete(name) {
		return fmt.Errorf("%w: group %s not found", ErrGroupNotFound, name)
	}

	return nil
}

// DeleteGroupByARN removes the group with the given ARN or name.
func (b *InMemoryBackend) DeleteGroupByARN(name, arn string) error {
	b.mu.Lock("DeleteGroupByARN")
	defer b.mu.Unlock()

	if arn != "" {
		list := b.groupsByARN.Get(arn)
		if len(list) == 0 {
			return fmt.Errorf("%w: group with ARN %s not found", ErrGroupNotFound, arn)
		}

		b.groups.Delete(list[0].GroupName)

		return nil
	}

	if !b.groups.Delete(name) {
		return fmt.Errorf("%w: group %s not found", ErrGroupNotFound, name)
	}

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

	if b.samplingRules.Has(rule.RuleName) {
		return nil, fmt.Errorf("%w: sampling rule %s already exists", ErrSamplingRuleAlreadyExists, rule.RuleName)
	}

	rule.RuleARN = b.samplingRuleARN(rule.RuleName)
	now := time.Now()
	rule.CreatedAt = now
	rule.ModifiedAt = now
	b.samplingRules.Put(&rule)
	b.lastRuleModification = now

	return cloneRule(&rule), nil
}

// GetSamplingRules returns all sampling rules sorted by priority (ascending), then by name for stability.
func (b *InMemoryBackend) GetSamplingRules() []SamplingRule {
	b.mu.RLock("GetSamplingRules")
	defer b.mu.RUnlock()

	all := b.samplingRules.All()
	out := make([]SamplingRule, 0, len(all))

	for _, r := range all {
		out = append(out, *cloneRule(r))
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Priority != out[j].Priority {
			return out[i].Priority < out[j].Priority
		}

		return out[i].RuleName < out[j].RuleName
	})

	return out
}

// UpdateSamplingRule updates the mutable fields of an existing sampling rule.
// It accepts a SamplingRule struct where non-zero values are applied (legacy API).
func (b *InMemoryBackend) UpdateSamplingRule(ruleName string, updates SamplingRule) (*SamplingRule, error) {
	b.mu.Lock("UpdateSamplingRule")
	defer b.mu.Unlock()

	r, ok := b.samplingRules.Get(ruleName)
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

	r, ok := b.samplingRules.Get(ruleName)
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
// The built-in "Default" rule cannot be deleted; attempting to do so returns ErrDefaultRuleUndeletable.
func (b *InMemoryBackend) DeleteSamplingRule(ruleName string) (*SamplingRule, error) {
	if ruleName == defaultSamplingRuleName {
		return nil, fmt.Errorf(
			"%w: the %s sampling rule cannot be deleted",
			ErrDefaultRuleUndeletable,
			defaultSamplingRuleName,
		)
	}

	b.mu.Lock("DeleteSamplingRule")
	defer b.mu.Unlock()

	r, ok := b.samplingRules.Get(ruleName)
	if !ok {
		return nil, fmt.Errorf("%w: sampling rule %s not found", ErrSamplingRuleNotFound, ruleName)
	}

	deleted := cloneRule(r)
	b.samplingRules.Delete(ruleName)
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
	newlyParsed := make([]*Segment, 0, len(segments))

	for _, seg := range segments {
		var hdr segmentHeader
		if err := json.Unmarshal([]byte(seg), &hdr); err != nil || hdr.TraceID == "" {
			unprocessed = append(unprocessed, uuid.NewString())

			continue
		}

		t, ok := b.traces.Get(hdr.TraceID)
		if !ok {
			t = &Trace{
				TraceID:   hdr.TraceID,
				StartTime: time.Now(),
				Segments:  []string{},
			}
			b.traces.Put(t)
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

			b.parsedSegments.Put(&parsed)
			newlyParsed = append(newlyParsed, &parsed)

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

	b.detectInsights(newlyParsed)

	return unprocessed
}

// maybeResetInsightWindow resets the window when it has expired, closing any
// active insight whose rate has normalised. Must be called with mu held.
func (b *InMemoryBackend) maybeResetInsightWindow(w *serviceInsightWindow, now time.Time) {
	if now.Sub(w.WindowStart) <= insightWindowDuration {
		return
	}

	if w.InsightID != "" && w.Total > 0 {
		rate := float64(w.FaultCount) / float64(w.Total)
		if rate < insightFaultThreshold {
			if ins, exists := b.insights.Get(w.InsightID); exists {
				ins.State = "CLOSED"
				ins.EndTime = now
			}

			w.InsightID = ""
		}
	}

	w.Total = 0
	w.FaultCount = 0
	w.WindowStart = now
}

// maybeOpenInsight creates a new ACTIVE insight when the window has enough
// data and the fault rate exceeds the threshold. Must be called with mu held.
func (b *InMemoryBackend) maybeOpenInsight(w *serviceInsightWindow, svcName string, now time.Time) {
	if w.Total < insightMinRequests || w.InsightID != "" {
		return
	}

	rate := float64(w.FaultCount) / float64(w.Total)
	if rate < insightFaultThreshold {
		return
	}

	insightID := uuid.NewString()
	b.insights.Put(&Insight{
		InsightID: insightID,
		GroupARN:  b.groupARN("default"),
		GroupName: "default",
		State:     statusActive,
		StartTime: now,
		Summary: fmt.Sprintf(
			"Elevated fault rate detected for service %q (%.0f%%)",
			svcName, rate*pctMultiplier,
		),
	})
	b.insightEvents[insightID] = []*InsightEvent{{
		InsightID: insightID,
		EventTime: now,
		Summary: fmt.Sprintf(
			"Fault rate %.0f%% exceeded threshold for %q",
			rate*pctMultiplier, svcName,
		),
	}}
	w.InsightID = insightID
}

// detectInsights checks per-service fault rates and creates/closes insights as needed.
// Must be called while the backend mutex is held.
func (b *InMemoryBackend) detectInsights(newSegs []*Segment) {
	now := time.Now()

	byService := map[string][]*Segment{}
	for _, seg := range newSegs {
		byService[seg.Name] = append(byService[seg.Name], seg)
	}

	for svcName, segs := range byService {
		w, ok := b.serviceWindows.Get(svcName)
		if !ok {
			w = &serviceInsightWindow{Name: svcName, WindowStart: now}
			b.serviceWindows.Put(w)
		}

		b.maybeResetInsightWindow(w, now)

		for _, seg := range segs {
			w.Total++
			if seg.Fault || seg.Error {
				w.FaultCount++
			}
		}

		b.maybeOpenInsight(w, svcName, now)
	}
}

// GetTraceSummaries returns all trace summaries sorted by start time (newest first).
func (b *InMemoryBackend) GetTraceSummaries() []Trace {
	b.mu.RLock("GetTraceSummaries")
	defer b.mu.RUnlock()

	all := b.traces.All()
	out := make([]Trace, 0, len(all))

	for _, t := range all {
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

	t, ok := b.traces.Get(traceID)
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

	segs := b.traceSegments.Get(traceID)
	out := make([]*Segment, len(segs))
	copy(out, segs)

	return out
}

// GetAllParsedSegments returns all parsed segments, keyed by trace ID.
func (b *InMemoryBackend) GetAllParsedSegments() map[string][]*Segment {
	b.mu.RLock("GetAllParsedSegments")
	defer b.mu.RUnlock()

	out := make(map[string][]*Segment)

	for _, t := range b.traces.All() {
		segs := b.traceSegments.Get(t.TraceID)
		if len(segs) == 0 {
			continue
		}

		cp := make([]*Segment, len(segs))
		copy(cp, segs)
		out[t.TraceID] = cp
	}

	return out
}

// Reset clears all backend state, resetting to an empty store.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	// Resets every store.Table-backed resource map in one call instead of
	// the per-map make() calls this used to be (Phase 3.3 pkgs/store
	// conversion). See registerAllTables in store_setup.go for the full
	// list of tables this covers.
	b.registry.ResetAll()
	b.samplingRules.Put(b.defaultSamplingRule())

	b.insightEvents = make(map[string][]*InsightEvent)
	b.retrievedTraces = make(map[string][]*Trace)
	b.retrievalTimes = make(map[string]time.Time)
	b.telemetry = make([]*TelemetryRecord, telemetryRingSize)
	b.telemetryIdx = 0
	b.indexingRules = defaultIndexingRules()
	b.encryptionConfig = &EncryptionConfig{Type: "NONE", Status: statusActive}
	b.lastRuleModification = time.Time{}
	b.traceSegmentDest = ""
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

	b.insights.Put(&insight)
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

	i, ok := b.insights.Get(insightID)
	if !ok {
		return nil, fmt.Errorf("%w: insight %s not found", ErrInsightNotFound, insightID)
	}

	return cloneInsight(i), nil
}

// GetInsightEvents returns all events for the given insight ID.
func (b *InMemoryBackend) GetInsightEvents(insightID string) ([]*InsightEvent, error) {
	b.mu.RLock("GetInsightEvents")
	defer b.mu.RUnlock()

	if !b.insights.Has(insightID) {
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

	all := b.insights.All()
	out := make([]Insight, 0, len(all))

	for _, i := range all {
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

	existing, exists := b.resourcePolicies.Get(policyName)

	if !exists && b.resourcePolicies.Len() >= maxResourcePolicies {
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
	b.resourcePolicies.Put(p)

	return cloneResourcePolicy(p), nil
}

// ListResourcePolicies returns all resource policies sorted by name.
func (b *InMemoryBackend) ListResourcePolicies() []ResourcePolicy {
	b.mu.RLock("ListResourcePolicies")
	defer b.mu.RUnlock()

	all := b.resourcePolicies.All()
	out := make([]ResourcePolicy, 0, len(all))

	for _, p := range all {
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

	if !b.resourcePolicies.Delete(policyName) {
		return fmt.Errorf("%w: resource policy %s not found", ErrResourcePolicyNotFound, policyName)
	}

	return nil
}

// AddResourcePolicyInternal seeds a resource policy directly for testing.
func (b *InMemoryBackend) AddResourcePolicyInternal(policy ResourcePolicy) {
	b.mu.Lock("AddResourcePolicyInternal")
	defer b.mu.Unlock()

	b.resourcePolicies.Put(cloneResourcePolicy(&policy))
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

	b.traceRetrievals.Put(&retrieval)
}

// CancelTraceRetrieval marks a trace retrieval as cancelled.
// If the token is not found the operation is a no-op (idempotent).
func (b *InMemoryBackend) CancelTraceRetrieval(retrievalToken string) {
	b.mu.Lock("CancelTraceRetrieval")
	defer b.mu.Unlock()

	b.traceRetrievals.Delete(retrievalToken)
}

// GetRetrievedTracesGraph returns the status and services for a retrieval token.
// If the token is not found a COMPLETE status is returned.
func (b *InMemoryBackend) GetRetrievedTracesGraph(retrievalToken string) (string, []*Trace) {
	b.mu.RLock("GetRetrievedTracesGraph")
	defer b.mu.RUnlock()

	tr, ok := b.traceRetrievals.Get(retrievalToken)
	if !ok {
		return traceRetrievalStatusComplete, nil
	}

	return tr.Status, nil
}

// --- Sampling statistic operations ---

// GetSamplingStatisticSummaries returns accumulated sampling statistic summaries.
func (b *InMemoryBackend) GetSamplingStatisticSummaries() []SamplingStatisticSummary {
	b.mu.RLock("GetSamplingStatisticSummaries")
	defer b.mu.RUnlock()

	all := b.samplingStats.All()
	out := make([]SamplingStatisticSummary, 0, len(all))

	for _, s := range all {
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
// Documents with an empty ClientID are returned in the unprocessed list.
// Statistics from known rules are accumulated for GetSamplingStatisticSummaries.
func (b *InMemoryBackend) GetSamplingTargets(
	docs []SamplingStatisticsDocument,
) ([]SamplingTargetResult, []UnprocessedStatisticsResult) {
	b.mu.Lock("GetSamplingTargets")
	defer b.mu.Unlock()

	targets := make([]SamplingTargetResult, 0, len(docs))
	unprocessed := make([]UnprocessedStatisticsResult, 0)

	for _, d := range docs {
		if d.ClientID == "" {
			unprocessed = append(unprocessed, UnprocessedStatisticsResult{
				RuleName:  d.RuleName,
				ErrorCode: "400",
				Message:   "ClientID is required",
			})

			continue
		}

		r, ok := b.samplingRules.Get(d.RuleName)
		if !ok {
			unprocessed = append(unprocessed, UnprocessedStatisticsResult{
				RuleName:  d.RuleName,
				ErrorCode: "404",
				Message:   "Rule not found",
			})

			continue
		}

		// Accumulate statistics.
		if existing, exists := b.samplingStats.Get(d.RuleName); exists {
			existing.RequestCount += d.RequestCount
			existing.SampledCount += d.SampledCount
			existing.BorrowCount += d.BorrowCount
			existing.Timestamp = time.Now()
		} else {
			b.samplingStats.Put(&SamplingStatisticSummary{
				RuleName:     d.RuleName,
				RequestCount: d.RequestCount,
				SampledCount: d.SampledCount,
				BorrowCount:  d.BorrowCount,
				Timestamp:    time.Now(),
			})
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

	for _, t := range b.traces.All() {
		segs := b.traceSegments.Get(t.TraceID)

		var inWindow []*Segment

		for _, seg := range segs {
			if seg.StartTime == 0 {
				inWindow = append(inWindow, seg)

				continue
			}

			segTime := time.Unix(int64(seg.StartTime), 0)
			if !segTime.Before(startTime) && !segTime.After(endTime) {
				inWindow = append(inWindow, seg)
			}
		}

		if len(inWindow) > 0 {
			filtered[t.TraceID] = inWindow
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
		if segs := b.traceSegments.Get(id); len(segs) > 0 {
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

	for _, t := range b.traces.All() {
		segs := b.traceSegments.Get(t.TraceID)

		for _, seg := range segs {
			if seg.StartTime == 0 {
				continue
			}

			segTime := time.Unix(int64(seg.StartTime), 0)
			if segTime.Before(startTime) || segTime.After(endTime) {
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

	now := time.Now()
	token := "retrieval-" + strconv.FormatInt(now.UnixNano(), 10)

	retrieval := &TraceRetrieval{
		RetrievalToken: token,
		StartTime:      now,
		Status:         traceRetrievalStatusComplete,
	}

	b.traceRetrievals.Put(retrieval)
	b.retrievalTimes[token] = now

	// Pre-populate results using stored traces that match the requested IDs.
	if b.retrievedTraces == nil {
		b.retrievedTraces = make(map[string][]*Trace)
	}

	results := make([]*Trace, 0, len(traceIDs))

	for _, id := range traceIDs {
		if t, ok := b.traces.Get(id); ok {
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

	tr, ok := b.traceRetrievals.Get(retrievalToken)
	if !ok {
		return traceRetrievalStatusComplete, nil
	}

	traces := b.retrievedTraces[retrievalToken]

	out := make([]*Trace, len(traces))
	for i, t := range traces {
		cp := *t
		out[i] = &cp
	}

	return tr.Status, out
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
	Users        []string
	ServiceIDs   []TraceSummaryServiceID
	Duration     float64
	ResponseTime float64
	ApproxTime   float64
	Revision     int
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
// accumulateUserFromAnnotations checks segment annotations for a "user" key and
// appends the value to summary.Users when not already present.
func accumulateUserFromAnnotations(summary *TraceSummaryData, seg *Segment, seenUsers map[string]bool) {
	userVal, ok := seg.Annotations["user"]
	if !ok {
		return
	}

	userStr, isStr := userVal.(string)
	if !isStr || userStr == "" || seenUsers[userStr] {
		return
	}

	seenUsers[userStr] = true
	summary.Users = append(summary.Users, userStr)
}

// accumulateServiceID records the service identity from seg into summary.ServiceIDs when not yet seen.
func accumulateServiceID(summary *TraceSummaryData, seg *Segment, seen map[serviceKey]bool) {
	svcType := seg.Origin
	if svcType == "" {
		svcType = seg.Namespace
	}

	key := serviceKey{Name: seg.Name, Type: svcType}
	if !seen[key] {
		seen[key] = true
		summary.ServiceIDs = append(summary.ServiceIDs, TraceSummaryServiceID{Name: seg.Name, Type: svcType})
	}
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
	seenUsers := map[string]bool{}
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
		accumulateUserFromAnnotations(&summary, seg, seenUsers)
		accumulateServiceID(&summary, seg, seen)

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
