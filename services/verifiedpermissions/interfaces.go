package verifiedpermissions

import "context"

// StorageBackend is the interface for Verified Permissions storage operations.
type StorageBackend interface {
	AccountID() string
	CreatePolicyStore(
		description string,
		tags map[string]string,
		validationMode, deletionProtection, clientToken string,
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
	PolicyScope(p *Policy) *PolicyScopeResult
	CreatePolicyTemplate(policyStoreID, description, statement, clientToken string) (*PolicyTemplate, error)
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
	CreateIdentitySource(
		policyStoreID, principalEntityType string,
		cfg IdentitySourceConfig,
		clientToken string,
	) (*IdentitySource, error)
	GetIdentitySource(policyStoreID, identitySourceID string) (*IdentitySource, error)
	DeleteIdentitySource(policyStoreID, identitySourceID string) error
	ListIdentitySources(
		policyStoreID, nextToken string,
		maxResults int,
		principalEntityTypes []string,
	) ([]IdentitySource, string, error)
	UpdateIdentitySource(
		policyStoreID, identitySourceID, principalEntityType string,
		cfg IdentitySourceConfig,
	) (*IdentitySource, error)
	PutSchema(policyStoreID, schema string) ([]string, error)
	GetSchema(policyStoreID string) (*PolicyStoreSchema, error)
	CreatePolicyStoreAlias(aliasName, policyStoreID string) (*PolicyStoreAlias, error)
	GetPolicyStoreAlias(aliasName string) (*PolicyStoreAlias, error)
	ListPolicyStoreAliases(policyStoreID, nextToken string, maxResults int) ([]PolicyStoreAlias, string)
	DeletePolicyStoreAlias(aliasName string, hardDelete bool) error
	ResolvePolicyStoreAlias(aliasName string) (string, error)
	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error
}

var _ StorageBackend = (*InMemoryBackend)(nil)
