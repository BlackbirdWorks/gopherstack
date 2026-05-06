package xray

import "time"

// StorageBackend defines the interface for X-Ray backend implementations.
// All mutating methods must be safe for concurrent use.
type StorageBackend interface {
	CreateGroup(name, filterExpr string) (*Group, error)
	GetGroup(name string) (*Group, error)
	GetGroups() []Group
	UpdateGroup(name, filterExpr string) (*Group, error)
	DeleteGroup(name string) error
	CreateSamplingRule(rule SamplingRule) (*SamplingRule, error)
	GetSamplingRules() []SamplingRule
	UpdateSamplingRule(ruleName string, updates SamplingRule) (*SamplingRule, error)
	DeleteSamplingRule(ruleName string) (*SamplingRule, error)
	PutTraceSegments(segments []string) []string
	GetTraceSummaries() []Trace
	GetTrace(traceID string) *Trace
	Reset()
	GetEncryptionConfig() *EncryptionConfig
	PutEncryptionConfig(encType, keyID string) (*EncryptionConfig, error)
	Snapshot() []byte
	Restore(data []byte) error
	// New operations
	CancelTraceRetrieval(retrievalToken string)
	DeleteResourcePolicy(policyName string) error
	ListResourcePolicies() []ResourcePolicy
	PutResourcePolicy(policyName, policyDocument string) *ResourcePolicy
	GetIndexingRules() []*IndexingRule
	GetInsight(insightID string) (*Insight, error)
	GetInsightEvents(insightID string) ([]*InsightEvent, error)
	GetInsightSummaries(states []string) []Insight
	GetRetrievedTracesGraph(retrievalToken string) (string, []*Trace)
	GetSamplingStatisticSummaries() []SamplingStatisticSummary
	GetSamplingTargets(ruleNames []string) ([]SamplingTargetResult, []string)
	// New ops (gh-1185)
	GetServiceGraph(startTime, endTime time.Time) []map[string]any
	GetTraceGraph(traceIDs []string) []map[string]any
	GetTraceSegmentDestination() string
	ListRetrievedTraces(retrievalToken string) (string, []*Trace)
	ListTagsForResource(resourceARN string) []map[string]string
	StartTraceRetrieval(traceIDs []string) string
	TagResource(resourceARN string, tags map[string]string)
	UntagResource(resourceARN string, tagKeys []string)
	UpdateIndexingRule(name string) (*IndexingRule, error)
	UpdateTraceSegmentDestination(destination string) string
}

// Compile-time assertion: InMemoryBackend must implement StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
