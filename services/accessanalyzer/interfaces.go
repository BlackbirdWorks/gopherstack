package accessanalyzer

// StorageBackend defines the interface for Access Analyzer backend implementations.
// All mutating methods must be safe for concurrent use.
type StorageBackend interface {
	// Analyzer operations
	CreateAnalyzer(name string, analyzerType AnalyzerType, tags map[string]string) (*Analyzer, error)
	GetAnalyzer(name string) (*Analyzer, error)
	ListAnalyzers(analyzerType string) ([]*Analyzer, error)
	DeleteAnalyzer(name string) error

	// Archive rule operations
	CreateArchiveRule(analyzerName, ruleName string, filter map[string]FilterCriterion) (*ArchiveRule, error)
	GetArchiveRule(analyzerName, ruleName string) (*ArchiveRule, error)
	ListArchiveRules(analyzerName string) ([]*ArchiveRule, error)
	DeleteArchiveRule(analyzerName, ruleName string) error
	UpdateArchiveRule(analyzerName, ruleName string, filter map[string]FilterCriterion) (*ArchiveRule, error)

	// Finding operations
	AddFinding(
		analyzerName, resourceType, resourceArn string,
		action []string,
		principal map[string]string,
		isPublic *bool,
	) (*Finding, error)
	GetFinding(analyzerName, findingID string) (*Finding, error)
	ListFindings(
		analyzerName string,
		filter map[string]FilterCriterion,
		status string,
		maxResults int,
		nextToken string,
	) ([]*Finding, string, error)
	UpdateFindings(analyzerName string, findingIDs []string, status FindingStatus) error

	// Scan
	StartResourceScan(analyzerARN, resourceARN string) error

	// Tag operations
	TagResource(resourceARN string, kv map[string]string) error
	UntagResource(resourceARN string, tagKeys []string) error
	ListTagsForResource(resourceARN string) (map[string]string, error)

	// Lifecycle
	Reset()
	Region() string
	AccountID() string
	Snapshot() []byte
	Restore(data []byte) error
}

// compile-time assertion.
var _ StorageBackend = (*InMemoryBackend)(nil)
