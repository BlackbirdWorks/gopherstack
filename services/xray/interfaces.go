package xray

import (
	"context"
	"time"
)

// StorageBackend defines the interface for X-Ray backend implementations.
// All mutating methods must be safe for concurrent use.
type StorageBackend interface {
	CreateGroup(name, filterExpr string) (*Group, error)
	CreateGroupWithInsights(name, filterExpr string, ic InsightsConfiguration) (*Group, error)
	GetGroup(name string) (*Group, error)
	GetGroupByARN(arn string) (*Group, error)
	GetGroups() []Group
	UpdateGroup(name, filterExpr string) (*Group, error)
	UpdateGroupByARN(name, arn string, filterExpr *string, insights *InsightsConfiguration) (*Group, error)
	DeleteGroup(name string) error
	DeleteGroupByARN(name, arn string) error
	CreateSamplingRule(rule SamplingRule) (*SamplingRule, error)
	GetSamplingRules() []SamplingRule
	UpdateSamplingRule(ruleName string, updates SamplingRule) (*SamplingRule, error)
	UpdateSamplingRuleWithPointers(ruleName, ruleARN string, updates SamplingRuleUpdate) (*SamplingRule, error)
	DeleteSamplingRule(ruleName, ruleARN string) (*SamplingRule, error)
	PutTraceSegments(segments []string) []string
	GetTraceSummaries() []Trace
	GetTrace(traceID string) *Trace
	GetParsedSegments(traceID string) []*Segment
	GetAllParsedSegments() map[string][]*Segment
	Reset()
	GetEncryptionConfig() *EncryptionConfig
	PutEncryptionConfig(encType, keyID string) (*EncryptionConfig, error)
	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error
	// Insight operations
	GetInsight(insightID string) (*Insight, error)
	GetInsightEvents(insightID string) ([]*InsightEvent, error)
	GetInsightSummaries(states []string) ([]Insight, error)
	// Resource policy operations
	CancelTraceRetrieval(retrievalToken string) error
	DeleteResourcePolicy(policyName, policyRevisionID string) error
	ListResourcePolicies() []ResourcePolicy
	PutResourcePolicy(policyName, policyDocument, revisionID string) (*ResourcePolicy, error)
	// Indexing rules
	GetIndexingRules() []*IndexingRule
	// Retrieval
	GetRetrievedTracesGraph(retrievalToken string) (string, []*Trace, error)
	// Sampling statistics
	GetSamplingStatisticSummaries() []SamplingStatisticSummary
	GetSamplingTargets(docs []SamplingStatisticsDocument) ([]SamplingTargetResult, []UnprocessedStatisticsResult)
	LastRuleModification() time.Time
	// Service graph operations
	GetServiceGraph(startTime, endTime time.Time) []map[string]any
	GetTraceGraph(traceIDs []string) []map[string]any
	GetTimeSeriesServiceStatistics(startTime, endTime time.Time, period int) []map[string]any
	// Destination
	GetTraceSegmentDestination() string
	UpdateTraceSegmentDestination(destination string) string
	// Retrieval list
	ListRetrievedTraces(retrievalToken string) (string, []*Trace, error)
	// Tags
	ListTagsForResource(resourceARN string) ([]map[string]string, error)
	StartTraceRetrieval(traceIDs []string) string
	TagResource(resourceARN string, tags map[string]string) error
	UntagResource(resourceARN string, tagKeys []string) error
	// Indexing rule update
	UpdateIndexingRule(name string, rule *ProbabilisticRuleValue) (*IndexingRule, error)
	// Telemetry
	PutTelemetryRecords(records []TelemetryRecord)
}

// Compile-time assertion: InMemoryBackend must implement StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
