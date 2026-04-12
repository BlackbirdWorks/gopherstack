package shield

// StorageBackend is the interface for Shield Advanced storage operations.
type StorageBackend interface {
	CreateSubscription() error
	DescribeSubscription() (*Subscription, error)
	GetSubscriptionState() string
	CreateProtection(name, resourceARN string, tags map[string]string) (*Protection, error)
	DescribeProtection(protectionID, resourceARN string) (*Protection, error)
	DeleteProtection(protectionID string) error
	ListProtections() []*Protection
	TagResource(resourceARN string, tags map[string]string) error
	ListTagsForResource(resourceARN string) (map[string]string, error)
	UntagResource(resourceARN string, tagKeys []string) error
	AccountID() string
	Region() string
	Reset()
	Snapshot() []byte
	Restore(data []byte) error
}

var _ StorageBackend = (*InMemoryBackend)(nil)
