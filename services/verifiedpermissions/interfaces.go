package verifiedpermissions

// StorageBackend is the interface for Verified Permissions storage operations.
type StorageBackend interface {
	CreatePolicyStore(description string, tags map[string]string) (*PolicyStore, error)
	GetPolicyStore(policyStoreID string) (*PolicyStore, error)
	ListPolicyStores() []PolicyStore
	UpdatePolicyStore(policyStoreID, description string) (*PolicyStore, error)
	DeletePolicyStore(policyStoreID string) error
	CreatePolicy(policyStoreID, policyType, statement string) (*Policy, error)
	GetPolicy(policyStoreID, policyID string) (*Policy, error)
	ListPolicies(policyStoreID string) ([]Policy, error)
	UpdatePolicy(policyStoreID, policyID, statement string) (*Policy, error)
	DeletePolicy(policyStoreID, policyID string) error
	CreatePolicyTemplate(policyStoreID, description, statement string) (*PolicyTemplate, error)
	GetPolicyTemplate(policyStoreID, policyTemplateID string) (*PolicyTemplate, error)
	ListPolicyTemplates(policyStoreID string) ([]PolicyTemplate, error)
	UpdatePolicyTemplate(policyStoreID, policyTemplateID, description, statement string) (*PolicyTemplate, error)
	DeletePolicyTemplate(policyStoreID, policyTemplateID string) error
	Reset()
	TagResource(resourceARN string, tags map[string]string) error
	UntagResource(resourceARN string, tagKeys []string) error
	ListTagsForResource(resourceARN string) (map[string]string, error)
	Snapshot() []byte
	Restore(data []byte) error
}

var _ StorageBackend = (*InMemoryBackend)(nil)
