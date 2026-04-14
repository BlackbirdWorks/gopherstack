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
}

// Compile-time assertion: InMemoryBackend must implement StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
