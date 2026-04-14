package xray

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
	Reset()
	GetEncryptionConfig() *EncryptionConfig
	PutEncryptionConfig(encType, keyID string) *EncryptionConfig
	// New operations
	CancelTraceRetrieval(retrievalToken string)
	DeleteResourcePolicy(policyName string) error
	GetIndexingRules() []*IndexingRule
	GetInsight(insightID string) (*Insight, error)
	GetInsightEvents(insightID string) ([]*InsightEvent, error)
	GetInsightSummaries() []Insight
	GetRetrievedTracesGraph(retrievalToken string) (string, []*Trace)
	GetSamplingStatisticSummaries() []SamplingStatisticSummary
	GetSamplingTargets(ruleNames []string) ([]SamplingTargetResult, []string)
}

// Compile-time assertion: InMemoryBackend must implement StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
