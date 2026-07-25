package xray

import (
	"time"
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
	CreatedAt         time.Time          `json:"createdAt"`
	ModifiedAt        time.Time          `json:"modifiedAt"`
	SamplingRateBoost *SamplingRateBoost `json:"samplingRateBoost,omitempty"`
	Attributes        map[string]string  `json:"attributes,omitempty"`
	RuleARN           string             `json:"ruleARN"`
	RuleName          string             `json:"ruleName"`
	ResourceARN       string             `json:"resourceARN"`
	ServiceName       string             `json:"serviceName"`
	ServiceType       string             `json:"serviceType"`
	Host              string             `json:"host"`
	HTTPMethod        string             `json:"httpMethod"`
	URLPath           string             `json:"urlPath"`
	FixedRate         float64            `json:"fixedRate"`
	Priority          int32              `json:"priority"`
	ReservoirSize     int32              `json:"reservoirSize"`
}

// SamplingRateBoost holds the configuration for temporary sampling-rate boosts.
type SamplingRateBoost struct {
	MaxRate               float64 `json:"maxRate"`
	CooldownWindowMinutes int32   `json:"cooldownWindowMinutes"`
}

// SamplingRuleUpdate holds pointer-semantic updates for UpdateSamplingRule.
// A nil pointer means "no change"; a non-nil pointer (even to zero/empty) means "apply".
type SamplingRuleUpdate struct {
	ResourceARN       *string
	ServiceName       *string
	ServiceType       *string
	Host              *string
	HTTPMethod        *string
	URLPath           *string
	FixedRate         *float64
	Priority          *int32
	ReservoirSize     *int32
	SamplingRateBoost *SamplingRateBoost
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
	StartTime      time.Time `json:"startTime"`
	EndTime        time.Time `json:"endTime,omitzero"`
	LastUpdateTime time.Time `json:"lastUpdateTime"`
	InsightID      string    `json:"insightId"`
	GroupARN       string    `json:"groupARN"`
	GroupName      string    `json:"groupName"`
	State          string    `json:"state"`
	Summary        string    `json:"summary"`
}

// InsightEvent represents an event within an X-Ray insight.
type InsightEvent struct {
	EventTime time.Time `json:"eventTime"`
	InsightID string    `json:"insightId"`
	Summary   string    `json:"summary"`
}

// ResourcePolicy represents a resource-based policy attached to the X-Ray account.
type ResourcePolicy struct {
	LastUpdatedTime  time.Time `json:"lastUpdatedTime"`
	PolicyName       string    `json:"policyName"`
	PolicyDocument   string    `json:"policyDocument"`
	PolicyRevisionID string    `json:"policyRevisionId"`
}

// TraceRetrieval represents an ongoing trace retrieval operation.
type TraceRetrieval struct {
	StartTime      time.Time `json:"startTime"`
	RetrievalToken string    `json:"retrievalToken"`
	Status         string    `json:"status"`
}

// IndexingRule represents an X-Ray CloudWatch Logs indexing rule.
type IndexingRule struct {
	ModifiedAt time.Time               `json:"modifiedAt"`
	Rule       *ProbabilisticRuleValue `json:"rule,omitempty"`
	Name       string                  `json:"name"`
}

// ProbabilisticRuleValue holds the probabilistic sampling percentage configuration
// for an indexing rule.
type ProbabilisticRuleValue struct {
	DesiredSamplingPercentage float64 `json:"desiredSamplingPercentage"`
	ActualSamplingPercentage  float64 `json:"actualSamplingPercentage"`
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

// segmentHeader is used to extract the trace_id from a raw segment JSON.
type segmentHeader struct {
	TraceID string `json:"trace_id"`
	ID      string `json:"id"`
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

// TraceSummaryData holds derived data for GetTraceSummaries response.
type TraceSummaryData struct {
	Annotations  map[string]any
	HTTP         *TraceSummaryHTTP
	EntryPoint   *TraceSummaryServiceID
	TraceID      string
	Users        []string
	ServiceIDs   []TraceSummaryServiceID
	Duration     float64
	ResponseTime float64
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
