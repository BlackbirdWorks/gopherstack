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
}

// Compile-time assertion: InMemoryBackend must implement StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
