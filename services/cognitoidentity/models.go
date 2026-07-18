package cognitoidentity

import (
	"maps"
	"time"
)

// IdentityProvider represents a linked Cognito User Pool provider.
type IdentityProvider struct {
	ProviderName         string `json:"providerName"`
	ClientID             string `json:"clientID"`
	ServerSideTokenCheck bool   `json:"serverSideTokenCheck"`
}

// MappingRule defines a single claim-based role assignment rule.
type MappingRule struct {
	Claim     string `json:"claim"`
	MatchType string `json:"matchType"`
	Value     string `json:"value"`
	RoleARN   string `json:"roleARN"`
}

// RulesConfiguration holds an ordered list of claim-based mapping rules.
type RulesConfiguration struct {
	Rules []MappingRule `json:"rules"`
}

// RoleMapping configures how an identity pool assigns IAM roles for a provider.
type RoleMapping struct {
	RulesConfiguration      *RulesConfiguration `json:"rulesConfiguration,omitempty"`
	Type                    string              `json:"type"`
	AmbiguousRoleResolution string              `json:"ambiguousRoleResolution,omitempty"`
}

// PoolExtendedConfig carries optional fields added in later API versions that
// would otherwise require touching every existing CreateIdentityPool/UpdateIdentityPool
// call site. Pass as a trailing variadic arg; callers that omit it get zero values.
type PoolExtendedConfig struct {
	OpenIDConnectProviderARNs []string
	SamlProviderARNs          []string
}

// IdentityPool represents an Amazon Cognito Identity Pool.
type IdentityPool struct {
	CreatedAt                 time.Time         `json:"createdAt"`
	SupportedLoginProviders   map[string]string `json:"supportedLoginProviders,omitempty"`
	Tags                      map[string]string `json:"tags,omitempty"`
	OpenIDConnectProviderARNs []string          `json:"openIdConnectProviderARNs,omitempty"`
	SamlProviderARNs          []string          `json:"samlProviderARNs,omitempty"`
	IdentityPoolID            string            `json:"identityPoolID"`
	IdentityPoolName          string            `json:"identityPoolName"`
	ARN                       string            `json:"arn"`
	DeveloperProviderName     string            `json:"developerProviderName,omitempty"`
	// region is the store.Table composite-key qualifier (see regionKey); it is never
	// serialised on IdentityPool itself, only carried through persistence.go's DTO.
	region                         string
	IdentityProviders              []IdentityProvider `json:"identityProviders,omitempty"`
	AllowUnauthenticatedIdentities bool               `json:"allowUnauthenticatedIdentities"`
	AllowClassicFlow               bool               `json:"allowClassicFlow"`
}

// IdentityRoles holds IAM role mappings for an identity pool.
type IdentityRoles struct {
	RoleMappings           map[string]RoleMapping `json:"roleMappings,omitempty"`
	AuthenticatedRoleARN   string                 `json:"authenticatedRoleARN"`
	UnauthenticatedRoleARN string                 `json:"unauthenticatedRoleARN"`
	// region and poolID are the store.Table composite-key components (see regionKey);
	// neither is serialised on IdentityRoles itself, only via persistence.go's DTO.
	region string
	poolID string
}

// Identity represents a federated identity.
type Identity struct {
	CreatedAt        time.Time         `json:"createdAt"`
	LastModifiedDate time.Time         `json:"lastModifiedDate"`
	Logins           map[string]string `json:"logins,omitempty"`
	IdentityID       string            `json:"identityID"`
	IdentityPoolID   string            `json:"identityPoolID"`
	// region is the store.Table composite-key qualifier (see regionKey); it is never
	// serialised on Identity itself, only carried through persistence.go's DTO.
	region  string
	Enabled bool `json:"enabled"`
}

// PrincipalTagMapping stores the principal tag attribute map for a pool and provider.
type PrincipalTagMapping struct {
	PrincipalTags map[string]string `json:"principalTags,omitempty"`
	// region, poolID and providerName are the store.Table composite-key components (see
	// regionKey and principalTagKey); none is serialised on PrincipalTagMapping itself,
	// only via persistence.go's DTO.
	region       string
	poolID       string
	providerName string
	UseDefaults  bool `json:"useDefaults"`
}

// UnprocessedIdentityID represents a Cognito identity that could not be deleted.
type UnprocessedIdentityID struct {
	ErrorCode  string `json:"errorCode"`
	IdentityID string `json:"identityId"`
}

// IdentityDescription describes a federated identity, including its login providers.
type IdentityDescription struct {
	CreationDate     time.Time `json:"creationDate"`
	LastModifiedDate time.Time `json:"lastModifiedDate"`
	IdentityID       string    `json:"identityId"`
	Logins           []string  `json:"logins,omitempty"`
}

// ListIdentitiesResult holds the output for ListIdentities.
type ListIdentitiesResult struct {
	IdentityPoolID string                `json:"identityPoolId"`
	NextToken      string                `json:"nextToken,omitempty"`
	Identities     []IdentityDescription `json:"identities"`
}

// DeveloperOpenIDToken is the result of GetOpenIDTokenForDeveloperIdentity.
type DeveloperOpenIDToken struct {
	IdentityID string
	Token      string
}

// LookupDeveloperIdentityResult holds the output of LookupDeveloperIdentity.
type LookupDeveloperIdentityResult struct {
	IdentityID                  string
	DeveloperUserIdentifierList []string
}

// Credentials holds temporary AWS credentials returned by GetCredentialsForIdentity.
type Credentials struct {
	Expiration      time.Time
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	IdentityID      string
}

// OpenIDToken holds the result of GetOpenIdToken.
type OpenIDToken struct {
	IdentityID string
	Token      string
}

// cloneProviders returns a deep copy of the given provider slice.
func cloneProviders(providers []IdentityProvider) []IdentityProvider {
	if providers == nil {
		return nil
	}

	cp := make([]IdentityProvider, len(providers))
	copy(cp, providers)

	return cp
}

// cloneStringMap returns a copy of a string map.
func cloneStringMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}

	out := make(map[string]string, len(m))
	maps.Copy(out, m)

	return out
}

// cloneStringSlice returns a shallow copy of a string slice.
func cloneStringSlice(s []string) []string {
	if s == nil {
		return nil
	}

	cp := make([]string, len(s))
	copy(cp, s)

	return cp
}

// clonePool returns a deep copy of an IdentityPool to prevent callers from
// mutating the backend's internal maps and slices.
func clonePool(pool *IdentityPool) *IdentityPool {
	cp := *pool
	cp.IdentityProviders = cloneProviders(pool.IdentityProviders)
	cp.SupportedLoginProviders = cloneStringMap(pool.SupportedLoginProviders)
	cp.Tags = cloneStringMap(pool.Tags)
	cp.OpenIDConnectProviderARNs = cloneStringSlice(pool.OpenIDConnectProviderARNs)
	cp.SamlProviderARNs = cloneStringSlice(pool.SamlProviderARNs)

	return &cp
}

// cloneIdentity returns a deep copy of an Identity to prevent callers from
// mutating the backend's internal Logins map.
func cloneIdentity(identity *Identity) *Identity {
	cp := *identity
	cp.Logins = cloneStringMap(identity.Logins)

	return &cp
}
