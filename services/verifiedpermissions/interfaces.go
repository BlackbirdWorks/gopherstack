package verifiedpermissions

// StorageBackend is the interface for Verified Permissions storage operations.
type StorageBackend interface {
	AccountID() string
	CreatePolicyStore(
		description string,
		tags map[string]string,
		validationMode, deletionProtection string,
	) (*PolicyStore, error)
	GetPolicyStore(policyStoreID string) (*PolicyStore, error)
	ListPolicyStores(nextToken string, maxResults int) ([]PolicyStore, string)
	UpdatePolicyStore(policyStoreID, description, validationMode, deletionProtection string) (*PolicyStore, error)
	DeletePolicyStore(policyStoreID string) error
	CreatePolicy(policyStoreID string, params CreatePolicyParams) (*Policy, error)
	GetPolicy(policyStoreID, policyID string) (*Policy, error)
	ListPolicies(
		policyStoreID string,
		filter ListPoliciesFilter,
		nextToken string,
		maxResults int,
	) ([]Policy, string, error)
	UpdatePolicy(policyStoreID, policyID string, params UpdatePolicyParams) (*Policy, error)
	DeletePolicy(policyStoreID, policyID string) error
	CreatePolicyTemplate(policyStoreID, description, statement string) (*PolicyTemplate, error)
	GetPolicyTemplate(policyStoreID, policyTemplateID string) (*PolicyTemplate, error)
	ListPolicyTemplates(policyStoreID, nextToken string, maxResults int) ([]PolicyTemplate, string, error)
	UpdatePolicyTemplate(policyStoreID, policyTemplateID, description, statement string) (*PolicyTemplate, error)
	DeletePolicyTemplate(policyStoreID, policyTemplateID string) error
	Reset()
	TagResource(resourceARN string, tags map[string]string) error
	UntagResource(resourceARN string, tagKeys []string) error
	ListTagsForResource(resourceARN string) (map[string]string, error)
	IsAuthorized(policyStoreID string, req AuthorizationRequest) (*AuthDecision, error)
	IsAuthorizedWithToken(policyStoreID string, req AuthorizationRequest) (*AuthDecision, error)
	BatchGetPolicy(items []BatchGetPolicyItem) BatchGetPolicyResult
	BatchIsAuthorized(policyStoreID string, requests []AuthorizationRequest) ([]AuthDecision, error)
	BatchIsAuthorizedWithToken(policyStoreID string, requests []AuthorizationRequest) ([]AuthDecision, error)
	CreateIdentitySource(policyStoreID, principalEntityType string, cfg IdentitySourceConfig) (*IdentitySource, error)
	GetIdentitySource(policyStoreID, identitySourceID string) (*IdentitySource, error)
	DeleteIdentitySource(policyStoreID, identitySourceID string) error
	ListIdentitySources(policyStoreID, nextToken string, maxResults int) ([]IdentitySource, string, error)
	UpdateIdentitySource(
		policyStoreID, identitySourceID, principalEntityType string,
		cfg IdentitySourceConfig,
	) (*IdentitySource, error)
	PutSchema(policyStoreID, schema string) ([]string, error)
	GetSchema(policyStoreID string) (*PolicyStoreSchema, error)
	Snapshot() []byte
	Restore(data []byte) error
}

var _ StorageBackend = (*InMemoryBackend)(nil)
