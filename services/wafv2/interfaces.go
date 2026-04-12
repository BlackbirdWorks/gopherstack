package wafv2

// StorageBackend is the interface for WAFv2 storage operations.
type StorageBackend interface {
	Region() string
	WebACLARN(name, id, scope string) string
	IPSetARN(name, id, scope string) string
	CreateWebACL(name, description, defaultAction, scope, visibilityConfig string, tags map[string]string) (*WebACL, error)
	GetWebACL(id string) (*WebACL, error)
	UpdateWebACL(id, description, defaultAction, visibilityConfig string) (*WebACL, error)
	DeleteWebACL(id string) error
	ListWebACLs() []*WebACL
	CreateIPSet(name, description, scope, ipVersion string, addresses []string, tags map[string]string) (*IPSet, error)
	GetIPSet(id string) (*IPSet, error)
	UpdateIPSet(id, description string, addresses []string) (*IPSet, error)
	DeleteIPSet(id string) error
	ListIPSets() []*IPSet
	TagResource(resourceARN string, tags map[string]string) error
	ListTagsForResource(resourceARN string) (map[string]string, error)
	UntagResource(resourceARN string, tagKeys []string) error
	Reset()
	AssociateWebACL(webACLARN, resourceARN string) error
	DisassociateWebACL(resourceARN string) error
	GetWebACLForResource(resourceARN string) (*WebACL, error)
	Snapshot() []byte
	Restore(data []byte) error
}

var _ StorageBackend = (*InMemoryBackend)(nil)
