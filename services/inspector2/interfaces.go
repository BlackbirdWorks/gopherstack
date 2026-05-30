package inspector2

// StorageBackend is the interface for Inspector2 storage operations.
type StorageBackend interface {
	Enable(resourceTypes []string) error
	Disable(resourceTypes []string) error
	IsEnabled() bool
	GetStatus() *AccountStatusResponse

	CreateFilter(
		name, action, description, reason string,
		criteria map[string]any,
		tags map[string]string,
	) (*Filter, error)
	UpdateFilter(arn, action, description, reason string, criteria map[string]any) (*Filter, error)
	DeleteFilter(arn string) error
	ListFilters(arns []string, action string) ([]*Filter, error)

	ListFindings(maxResults int32, nextToken string) ([]*Finding, string, error)

	GetConfiguration() *Configuration
	UpdateConfiguration(ec2ScanMode, ecrRescanDuration string) error

	TagResource(resourceARN string, tags map[string]string) error
	UntagResource(resourceARN string, tagKeys []string) error
	ListTagsForResource(resourceARN string) (map[string]string, error)

	AccountID() string
	Region() string
	Reset()
	Snapshot() []byte
	Restore(data []byte) error
}

var _ StorageBackend = (*InMemoryBackend)(nil)
