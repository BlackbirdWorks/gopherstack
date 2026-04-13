package verifiedpermissions

// StorageBackend is the interface for Verified Permissions storage operations.
type StorageBackend interface {
	AccountID() string
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
	IsAuthorized(policyStoreID string, req AuthorizationRequest) (*AuthDecision, error)
	IsAuthorizedWithToken(policyStoreID string, req AuthorizationRequest) (*AuthDecision, error)
	BatchGetPolicy(items []BatchGetPolicyItem) BatchGetPolicyResult
	BatchIsAuthorized(policyStoreID string, requests []AuthorizationRequest) ([]AuthDecision, error)
	BatchIsAuthorizedWithToken(policyStoreID string, requests []AuthorizationRequest) ([]AuthDecision, error)
	CreateIdentitySource(
		policyStoreID, userPoolArn, openIDIssuer, principalEntityType string,
		clientIDs []string,
	) (*IdentitySource, error)
	GetIdentitySource(policyStoreID, identitySourceID string) (*IdentitySource, error)
	DeleteIdentitySource(policyStoreID, identitySourceID string) error
	ListIdentitySources(policyStoreID string) ([]IdentitySource, error)
	UpdateIdentitySource(
		policyStoreID, identitySourceID, userPoolArn, openIDIssuer, principalEntityType string,
		clientIDs []string,
	) (*IdentitySource, error)
	PutSchema(policyStoreID, schema string) error
	GetSchema(policyStoreID string) (*PolicyStoreSchema, error)
	Snapshot() []byte
	Restore(data []byte) error
}

var _ StorageBackend = (*InMemoryBackend)(nil)
