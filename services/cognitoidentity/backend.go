package cognitoidentity

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"maps"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

// getRegion extracts the region from ctx, falling back to defaultRegion when unset.
// Cognito Identity resources are isolated per region: every backend operation resolves
// the caller's region from the request context and operates only on that region's
// nested store.
func getRegion(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
}

// regionFromARN extracts the region component (index 3) from an AWS ARN
// (arn:partition:service:region:account:resource), falling back to defaultRegion.
func regionFromARN(resourceARN, defaultRegion string) string {
	parts := strings.SplitN(resourceARN, ":", arnSplitParts)
	if len(parts) >= 4 && parts[3] != "" {
		return parts[3]
	}

	return defaultRegion
}

const (
	// deleteIdentitiesMaxBatch is the AWS-imposed limit on identities per DeleteIdentities call.
	deleteIdentitiesMaxBatch = 60

	// listIdentitiesMaxResults is the AWS-imposed upper limit on MaxResults for ListIdentities.
	listIdentitiesMaxResults = 60

	// arnSplitParts is the maximum number of colon-delimited fields to split from an AWS ARN.
	arnSplitParts = 6

	// identityIDParts is the expected number of colon-delimited parts in a Cognito identity ID
	// (format: "region:uuid").
	identityIDParts = 2
)

const (
	// alphanumChars contains characters used for random ID generation.
	alphanumChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	// credentialsExpirySeconds is how many seconds temporary credentials are valid.
	credentialsExpirySeconds = 3600

	// accessKeyIDLen is the length suffix of the synthetic access key ID.
	accessKeyIDLen = 16

	// secretKeyLen is the length of the synthetic secret access key.
	secretKeyLen = 40

	// tokenLen is the length of synthetic session and OpenID tokens.
	tokenLen = 64
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
	CreatedAt                      time.Time          `json:"createdAt"`
	SupportedLoginProviders        map[string]string  `json:"supportedLoginProviders,omitempty"`
	Tags                           map[string]string  `json:"tags,omitempty"`
	OpenIDConnectProviderARNs      []string           `json:"openIdConnectProviderARNs,omitempty"`
	SamlProviderARNs               []string           `json:"samlProviderARNs,omitempty"`
	IdentityPoolID                 string             `json:"identityPoolID"`
	IdentityPoolName               string             `json:"identityPoolName"`
	ARN                            string             `json:"arn"`
	DeveloperProviderName          string             `json:"developerProviderName,omitempty"`
	IdentityProviders              []IdentityProvider `json:"identityProviders,omitempty"`
	AllowUnauthenticatedIdentities bool               `json:"allowUnauthenticatedIdentities"`
	AllowClassicFlow               bool               `json:"allowClassicFlow"`
}

// IdentityRoles holds IAM role mappings for an identity pool.
type IdentityRoles struct {
	RoleMappings           map[string]RoleMapping `json:"roleMappings,omitempty"`
	AuthenticatedRoleARN   string                 `json:"authenticatedRoleARN"`
	UnauthenticatedRoleARN string                 `json:"unauthenticatedRoleARN"`
}

// Identity represents a federated identity.
type Identity struct {
	CreatedAt        time.Time         `json:"createdAt"`
	LastModifiedDate time.Time         `json:"lastModifiedDate"`
	Logins           map[string]string `json:"logins,omitempty"`
	IdentityID       string            `json:"identityID"`
	IdentityPoolID   string            `json:"identityPoolID"`
	Enabled          bool              `json:"enabled"`
}

// PrincipalTagMapping stores the principal tag attribute map for a pool and provider.
type PrincipalTagMapping struct {
	PrincipalTags map[string]string `json:"principalTags,omitempty"`
	UseDefaults   bool              `json:"useDefaults"`
}

// UnprocessedIdentityID represents a Cognito identity that could not be deleted.
type UnprocessedIdentityID struct {
	ErrorCode  string `json:"errorCode"`
	IdentityID string `json:"identityId"`
}

// InMemoryBackend is the in-memory store for Cognito Identity Pool resources.
//
// All resource maps are nested by region (outer key = region) so that
// same-named resources are isolated across regions. The per-region inner maps
// are created lazily via the *Store helpers. Callers must hold b.mu while
// accessing the inner maps.
type InMemoryBackend struct {
	mu               *lockmetrics.RWMutex
	pools            map[string]map[string]*IdentityPool        // region → poolID → pool
	poolsByName      map[string]map[string]*IdentityPool        // region → name → pool
	poolsByARN       map[string]map[string]*IdentityPool        // region → arn → pool
	identities       map[string]map[string]*Identity            // region → identityID → identity
	identitiesByPool map[string]map[string][]*Identity          // region → poolID → identities
	roles            map[string]map[string]*IdentityRoles       // region → poolID → roles
	principalTags    map[string]map[string]*PrincipalTagMapping // region → key → mapping
	accountID        string
	region           string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		mu:               lockmetrics.New("cognitoidentity"),
		pools:            make(map[string]map[string]*IdentityPool),
		poolsByName:      make(map[string]map[string]*IdentityPool),
		poolsByARN:       make(map[string]map[string]*IdentityPool),
		identities:       make(map[string]map[string]*Identity),
		identitiesByPool: make(map[string]map[string][]*Identity),
		roles:            make(map[string]map[string]*IdentityRoles),
		principalTags:    make(map[string]map[string]*PrincipalTagMapping),
		accountID:        accountID,
		region:           region,
	}
}

// The *Store helpers return the per-region inner map, lazily creating it.
// Callers must hold b.mu.

func (b *InMemoryBackend) poolsStore(region string) map[string]*IdentityPool {
	if b.pools[region] == nil {
		b.pools[region] = make(map[string]*IdentityPool)
	}

	return b.pools[region]
}

func (b *InMemoryBackend) poolsByNameStore(region string) map[string]*IdentityPool {
	if b.poolsByName[region] == nil {
		b.poolsByName[region] = make(map[string]*IdentityPool)
	}

	return b.poolsByName[region]
}

func (b *InMemoryBackend) poolsByARNStore(region string) map[string]*IdentityPool {
	if b.poolsByARN[region] == nil {
		b.poolsByARN[region] = make(map[string]*IdentityPool)
	}

	return b.poolsByARN[region]
}

func (b *InMemoryBackend) identitiesStore(region string) map[string]*Identity {
	if b.identities[region] == nil {
		b.identities[region] = make(map[string]*Identity)
	}

	return b.identities[region]
}

func (b *InMemoryBackend) identitiesByPoolStore(region string) map[string][]*Identity {
	if b.identitiesByPool[region] == nil {
		b.identitiesByPool[region] = make(map[string][]*Identity)
	}

	return b.identitiesByPool[region]
}

func (b *InMemoryBackend) rolesStore(region string) map[string]*IdentityRoles {
	if b.roles[region] == nil {
		b.roles[region] = make(map[string]*IdentityRoles)
	}

	return b.roles[region]
}

func (b *InMemoryBackend) principalTagsStore(region string) map[string]*PrincipalTagMapping {
	if b.principalTags[region] == nil {
		b.principalTags[region] = make(map[string]*PrincipalTagMapping)
	}

	return b.principalTags[region]
}

// Region returns the region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// CreateIdentityPool creates a new identity pool.
func (b *InMemoryBackend) CreateIdentityPool(
	ctx context.Context,
	name string,
	allowUnauthenticated bool,
	allowClassicFlow bool,
	developerProviderName string,
	providers []IdentityProvider,
	supportedLoginProviders map[string]string,
	tags map[string]string,
	opts ...*PoolExtendedConfig,
) (*IdentityPool, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateIdentityPool")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: IdentityPoolName is required", ErrInvalidParameter)
	}

	if _, ok := b.poolsByNameStore(region)[name]; ok {
		return nil, fmt.Errorf(
			"%w: identity pool %q already exists",
			ErrIdentityPoolAlreadyExists,
			name,
		)
	}

	poolID := region + ":" + uuid.New().String()
	arn := fmt.Sprintf(
		"arn:aws:cognito-identity:%s:%s:identitypool/%s",
		region,
		b.accountID,
		poolID,
	)

	var cfg PoolExtendedConfig
	if len(opts) > 0 && opts[0] != nil {
		cfg = *opts[0]
	}

	pool := &IdentityPool{
		IdentityPoolID:                 poolID,
		IdentityPoolName:               name,
		ARN:                            arn,
		DeveloperProviderName:          developerProviderName,
		AllowUnauthenticatedIdentities: allowUnauthenticated,
		AllowClassicFlow:               allowClassicFlow,
		IdentityProviders:              cloneProviders(providers),
		SupportedLoginProviders:        cloneStringMap(supportedLoginProviders),
		Tags:                           cloneStringMap(tags),
		OpenIDConnectProviderARNs:      cloneStringSlice(cfg.OpenIDConnectProviderARNs),
		SamlProviderARNs:               cloneStringSlice(cfg.SamlProviderARNs),
		CreatedAt:                      time.Now(),
	}

	b.poolsStore(region)[poolID] = pool
	b.poolsByNameStore(region)[name] = pool
	b.poolsByARNStore(region)[arn] = pool

	return clonePool(pool), nil
}

// DeleteIdentityPool removes an identity pool and all associated identities, roles,
// and principal-tag configurations.
func (b *InMemoryBackend) DeleteIdentityPool(ctx context.Context, poolID string) error {
	if poolID == "" {
		return fmt.Errorf("%w: IdentityPoolId is required", ErrInvalidParameter)
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteIdentityPool")
	defer b.mu.Unlock()

	pools := b.poolsStore(region)

	pool, ok := pools[poolID]
	if !ok {
		return fmt.Errorf("%w: identity pool %q not found", ErrIdentityPoolNotFound, poolID)
	}

	delete(b.poolsByNameStore(region), pool.IdentityPoolName)
	delete(b.poolsByARNStore(region), pool.ARN)
	delete(pools, poolID)
	delete(b.rolesStore(region), poolID)

	for _, identity := range b.identitiesByPoolStore(region)[poolID] {
		delete(b.identitiesStore(region), identity.IdentityID)
	}

	delete(b.identitiesByPoolStore(region), poolID)

	// Purge all principal-tag mappings that belong to this pool.
	prefix := poolID + ":"
	pt := b.principalTagsStore(region)

	for key := range pt {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			delete(pt, key)
		}
	}

	return nil
}

// DescribeIdentityPool returns the identity pool with the given ID.
func (b *InMemoryBackend) DescribeIdentityPool(
	ctx context.Context,
	poolID string,
) (*IdentityPool, error) {
	if poolID == "" {
		return nil, fmt.Errorf("%w: IdentityPoolId is required", ErrInvalidParameter)
	}

	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeIdentityPool")
	defer b.mu.RUnlock()

	pool, ok := b.poolsStore(region)[poolID]
	if !ok {
		return nil, fmt.Errorf("%w: identity pool %q not found", ErrIdentityPoolNotFound, poolID)
	}

	return clonePool(pool), nil
}

// ListIdentityPools returns all identity pools sorted by name, up to maxResults (0 = all).
// nextToken is an opaque cursor that encodes the last-returned pool name for pagination.
func (b *InMemoryBackend) ListIdentityPools(
	ctx context.Context,
	maxResults int,
	nextToken string,
) ([]*IdentityPool, string) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListIdentityPools")
	defer b.mu.RUnlock()

	regionPools := b.poolsStore(region)

	keys := make([]string, 0, len(regionPools))
	for id := range regionPools {
		keys = append(keys, id)
	}

	sort.Slice(keys, func(i, j int) bool {
		return regionPools[keys[i]].IdentityPoolName < regionPools[keys[j]].IdentityPoolName
	})

	// Apply cursor: skip all pools up to and including the one named by nextToken.
	// Default to len(keys) so an unrecognised / past-end token returns an empty page.
	startIdx := 0
	if nextToken != "" {
		startIdx = len(keys)

		for i, id := range keys {
			if regionPools[id].IdentityPoolName == nextToken {
				startIdx = i + 1

				break
			}
		}
	}

	keys = keys[startIdx:]

	limit := len(keys)
	if maxResults > 0 && maxResults < limit {
		limit = maxResults
	}

	out := make([]*IdentityPool, 0, limit)

	for i, id := range keys {
		out = append(out, clonePool(regionPools[id]))

		if maxResults > 0 && len(out) >= maxResults {
			// Return the name of the last item as the cursor for the next page.
			if i+1 < len(keys) {
				return out, regionPools[id].IdentityPoolName
			}

			break
		}
	}

	return out, ""
}

// UpdateIdentityPool updates the settings of an existing identity pool.
func (b *InMemoryBackend) UpdateIdentityPool(
	ctx context.Context,
	poolID string,
	name string,
	allowUnauthenticated bool,
	allowClassicFlow bool,
	developerProviderName string,
	providers []IdentityProvider,
	supportedLoginProviders map[string]string,
	tags map[string]string,
	opts ...*PoolExtendedConfig,
) (*IdentityPool, error) {
	if poolID == "" {
		return nil, fmt.Errorf("%w: IdentityPoolId is required", ErrInvalidParameter)
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("UpdateIdentityPool")
	defer b.mu.Unlock()

	pools := b.poolsStore(region)

	pool, ok := pools[poolID]
	if !ok {
		return nil, fmt.Errorf("%w: identity pool %q not found", ErrIdentityPoolNotFound, poolID)
	}

	if name != "" && name != pool.IdentityPoolName {
		if _, exists := b.poolsByNameStore(region)[name]; exists {
			return nil, fmt.Errorf(
				"%w: identity pool %q already exists",
				ErrIdentityPoolAlreadyExists,
				name,
			)
		}

		delete(b.poolsByNameStore(region), pool.IdentityPoolName)
		pool.IdentityPoolName = name
		b.poolsByNameStore(region)[name] = pool
	}

	pool.AllowUnauthenticatedIdentities = allowUnauthenticated
	pool.AllowClassicFlow = allowClassicFlow
	if developerProviderName != "" {
		pool.DeveloperProviderName = developerProviderName
	}
	pool.IdentityProviders = cloneProviders(providers)
	pool.SupportedLoginProviders = cloneStringMap(supportedLoginProviders)
	if tags != nil {
		pool.Tags = cloneStringMap(tags)
	}

	if len(opts) > 0 && opts[0] != nil {
		pool.OpenIDConnectProviderARNs = cloneStringSlice(opts[0].OpenIDConnectProviderARNs)
		pool.SamlProviderARNs = cloneStringSlice(opts[0].SamlProviderARNs)
	}

	return clonePool(pool), nil
}

// GetID returns an existing identity or creates a new one for the given pool and logins.
func (b *InMemoryBackend) GetID(
	ctx context.Context,
	poolID string,
	_ string,
	logins map[string]string,
) (*Identity, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("GetID")
	defer b.mu.Unlock()

	if poolID == "" {
		return nil, fmt.Errorf("%w: IdentityPoolId is required", ErrInvalidParameter)
	}

	pool, ok := b.poolsStore(region)[poolID]
	if !ok {
		return nil, fmt.Errorf("%w: identity pool %q not found", ErrIdentityPoolNotFound, poolID)
	}

	if len(logins) == 0 && !pool.AllowUnauthenticatedIdentities {
		return nil, fmt.Errorf(
			"%w: unauthenticated access is not supported for this identity pool",
			ErrNotAuthorized,
		)
	}

	// AWS GetId matches an existing identity if any of the provided (provider, token) pairs
	// already appear in the identity's logins. On a match the identity is updated with any
	// new login providers from the current request (provider-account linking).
	if existing := b.mergeExistingIdentity(region, poolID, logins); existing != nil {
		return existing, nil
	}

	// Create a new identity.
	identityID := region + ":" + uuid.New().String()
	now := time.Now()
	identity := &Identity{
		IdentityID:       identityID,
		IdentityPoolID:   poolID,
		Logins:           cloneStringMap(logins),
		CreatedAt:        now,
		LastModifiedDate: now,
		Enabled:          true,
	}

	b.identitiesStore(region)[identityID] = identity
	b.identitiesByPoolStore(region)[poolID] = append(
		b.identitiesByPoolStore(region)[poolID],
		identity,
	)

	return cloneIdentity(identity), nil
}

// mergeExistingIdentity searches identitiesByPool[region][poolID] for an identity that shares any
// (provider, token) pair with logins, merges new providers into it, and returns a clone.
// Returns nil if no match is found. Must be called with b.mu held.
func (b *InMemoryBackend) mergeExistingIdentity(
	region, poolID string,
	logins map[string]string,
) *Identity {
	for _, identity := range b.identitiesByPoolStore(region)[poolID] {
		if !anyLoginMatches(identity.Logins, logins) {
			continue
		}

		updated := false

		for provider, token := range logins {
			if identity.Logins[provider] != token {
				if identity.Logins == nil {
					identity.Logins = make(map[string]string)
				}

				identity.Logins[provider] = token
				updated = true
			}
		}

		if updated {
			identity.LastModifiedDate = time.Now()
		}

		return cloneIdentity(identity)
	}

	return nil
}

// GetCredentialsForIdentity returns synthetic temporary AWS credentials for an identity.
func (b *InMemoryBackend) GetCredentialsForIdentity(
	ctx context.Context,
	identityID string,
	logins map[string]string,
) (*Credentials, error) {
	if identityID == "" {
		return nil, fmt.Errorf("%w: IdentityId is required", ErrInvalidParameter)
	}

	region := getRegion(ctx, b.region)

	b.mu.RLock("GetCredentialsForIdentity")
	defer b.mu.RUnlock()

	identity, ok := b.identitiesStore(region)[identityID]
	if !ok {
		return nil, fmt.Errorf("%w: identity %q not found", ErrIdentityPoolNotFound, identityID)
	}

	// An authenticated identity (one that has logins on record) must present a
	// matching login token. An empty request Logins map would otherwise skip
	// the validation loop entirely and hand out credentials with no token,
	// bypassing authentication.
	if len(logins) == 0 && len(identity.Logins) > 0 {
		return nil, fmt.Errorf(
			"%w: Logins is required for an authenticated identity",
			ErrNotAuthorized,
		)
	}

	for provider, token := range logins {
		stored, exists := identity.Logins[provider]
		if !exists || stored != token {
			return nil, fmt.Errorf(
				"%w: login token for provider %q does not match",
				ErrNotAuthorized,
				provider,
			)
		}
	}

	expiry := time.Now().Add(credentialsExpirySeconds * time.Second)

	keyID, err := randomAlphanumeric(accessKeyIDLen)
	if err != nil {
		return nil, fmt.Errorf("generate access key ID: %w", err)
	}

	secretKey, err := randomAlphanumeric(secretKeyLen)
	if err != nil {
		return nil, fmt.Errorf("generate secret key: %w", err)
	}

	sessionToken, err := randomAlphanumeric(tokenLen)
	if err != nil {
		return nil, fmt.Errorf("generate session token: %w", err)
	}

	return &Credentials{
		AccessKeyID:     "ASIA" + keyID,
		SecretAccessKey: secretKey,
		SessionToken:    sessionToken,
		Expiration:      expiry,
		IdentityID:      identityID,
	}, nil
}

// GetOpenIDToken returns a synthetic OpenID Connect token for an identity.
func (b *InMemoryBackend) GetOpenIDToken(
	ctx context.Context,
	identityID string,
	_ map[string]string,
) (*OpenIDToken, error) {
	if identityID == "" {
		return nil, fmt.Errorf("%w: IdentityId is required", ErrInvalidParameter)
	}

	region := getRegion(ctx, b.region)

	b.mu.RLock("GetOpenIDToken")
	defer b.mu.RUnlock()

	if _, ok := b.identitiesStore(region)[identityID]; !ok {
		return nil, fmt.Errorf("%w: identity %q not found", ErrIdentityPoolNotFound, identityID)
	}

	// Return a synthetic token.
	payload, err := randomAlphanumeric(tokenLen)
	if err != nil {
		return nil, fmt.Errorf("generate token: %w", err)
	}

	token := fmt.Sprintf("eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.%s.signature", payload)

	return &OpenIDToken{
		IdentityID: identityID,
		Token:      token,
	}, nil
}

// SetIdentityPoolRoles configures IAM roles for an identity pool.
// Only the roles that are present (non-empty) in the provided map are updated;
// existing roles for omitted keys are preserved.
func (b *InMemoryBackend) SetIdentityPoolRoles(
	ctx context.Context,
	poolID, authenticatedARN, unauthenticatedARN string,
	roleMappings map[string]RoleMapping,
) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("SetIdentityPoolRoles")
	defer b.mu.Unlock()

	if _, ok := b.poolsStore(region)[poolID]; !ok {
		return fmt.Errorf("%w: identity pool %q not found", ErrIdentityPoolNotFound, poolID)
	}

	rs := b.rolesStore(region)
	existing, ok := rs[poolID]

	if !ok {
		existing = &IdentityRoles{}
		rs[poolID] = existing
	}

	// Only update the roles that the caller provided (non-empty value = explicitly set).
	if authenticatedARN != "" {
		existing.AuthenticatedRoleARN = authenticatedARN
	}

	if unauthenticatedARN != "" {
		existing.UnauthenticatedRoleARN = unauthenticatedARN
	}

	if roleMappings != nil {
		existing.RoleMappings = cloneRoleMappings(roleMappings)
	}

	return nil
}

// GetIdentityPoolRoles returns the IAM roles configured for an identity pool.
func (b *InMemoryBackend) GetIdentityPoolRoles(
	ctx context.Context,
	poolID string,
) (*IdentityRoles, error) {
	if poolID == "" {
		return nil, fmt.Errorf("%w: IdentityPoolId is required", ErrInvalidParameter)
	}

	region := getRegion(ctx, b.region)

	b.mu.RLock("GetIdentityPoolRoles")
	defer b.mu.RUnlock()

	if _, ok := b.poolsStore(region)[poolID]; !ok {
		return nil, fmt.Errorf("%w: identity pool %q not found", ErrIdentityPoolNotFound, poolID)
	}

	roles, ok := b.rolesStore(region)[poolID]
	if !ok {
		return &IdentityRoles{}, nil
	}

	cp := *roles

	return &cp, nil
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

// DeleteIdentities deletes the given identity IDs from the backend.
// Identities that do not exist are silently skipped.
// Returns a (possibly empty) list of IDs that could not be processed.
func (b *InMemoryBackend) DeleteIdentities(
	ctx context.Context,
	identityIDs []string,
) ([]UnprocessedIdentityID, error) {
	if len(identityIDs) > deleteIdentitiesMaxBatch {
		return nil, fmt.Errorf(
			"%w: DeleteIdentities accepts at most %d identities per call, got %d",
			ErrInvalidParameter, deleteIdentitiesMaxBatch, len(identityIDs),
		)
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteIdentities")
	defer b.mu.Unlock()

	var unprocessed []UnprocessedIdentityID

	ids := b.identitiesStore(region)
	idsByPool := b.identitiesByPoolStore(region)

	for _, id := range identityIDs {
		identity, ok := ids[id]
		if !ok {
			continue
		}

		poolID := identity.IdentityPoolID

		delete(ids, id)

		// Remove from identitiesByPool slice.
		existing := idsByPool[poolID]
		updated := make([]*Identity, 0, len(existing))

		for _, i := range existing {
			if i.IdentityID != id {
				updated = append(updated, i)
			}
		}

		idsByPool[poolID] = updated
	}

	return unprocessed, nil
}

// DescribeIdentity returns metadata about a specific federated identity.
func (b *InMemoryBackend) DescribeIdentity(
	ctx context.Context,
	identityID string,
) (*IdentityDescription, error) {
	if identityID == "" {
		return nil, fmt.Errorf("%w: IdentityId is required", ErrInvalidParameter)
	}

	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeIdentity")
	defer b.mu.RUnlock()

	identity, ok := b.identitiesStore(region)[identityID]
	if !ok {
		return nil, fmt.Errorf("%w: identity %q not found", ErrIdentityPoolNotFound, identityID)
	}

	logins := make([]string, 0, len(identity.Logins))
	for provider := range identity.Logins {
		logins = append(logins, provider)
	}

	slices.Sort(logins)

	return &IdentityDescription{
		IdentityID:       identity.IdentityID,
		Logins:           logins,
		CreationDate:     identity.CreatedAt,
		LastModifiedDate: identity.LastModifiedDate,
	}, nil
}

// DeveloperOpenIDToken is the result of GetOpenIDTokenForDeveloperIdentity.
type DeveloperOpenIDToken struct {
	IdentityID string
	Token      string
}

// lookupOrCreateDeveloperIdentity finds an existing identity in poolID whose logins
// overlap with logins, merging any new providers into it.  If none is found, a new
// identity is created.  Must be called with b.mu held.
func (b *InMemoryBackend) lookupOrCreateDeveloperIdentity(
	region, poolID string,
	logins map[string]string,
) string {
	for _, identity := range b.identitiesByPoolStore(region)[poolID] {
		if anyLoginMatches(identity.Logins, logins) {
			if identity.Logins == nil {
				identity.Logins = make(map[string]string)
			}

			maps.Copy(identity.Logins, logins)

			identity.LastModifiedDate = time.Now()

			return identity.IdentityID
		}
	}

	newID := region + ":" + uuid.New().String()
	now := time.Now()
	identity := &Identity{
		IdentityID:       newID,
		IdentityPoolID:   poolID,
		Logins:           cloneStringMap(logins),
		CreatedAt:        now,
		LastModifiedDate: now,
		Enabled:          true,
	}

	b.identitiesStore(region)[newID] = identity
	b.identitiesByPoolStore(region)[poolID] = append(
		b.identitiesByPoolStore(region)[poolID],
		identity,
	)

	return newID
}

// GetOpenIDTokenForDeveloperIdentity registers or retrieves an identity for a developer
// authenticated user, then returns a synthetic OpenID token.
func (b *InMemoryBackend) GetOpenIDTokenForDeveloperIdentity(
	ctx context.Context,
	poolID string,
	identityID string,
	logins map[string]string,
	tokenDuration int64,
) (*DeveloperOpenIDToken, error) {
	const maxTokenDuration = 86400

	if tokenDuration < 0 || tokenDuration > maxTokenDuration {
		return nil, fmt.Errorf(
			"%w: TokenDuration must be between 0 and %d seconds",
			ErrInvalidParameter, maxTokenDuration,
		)
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("GetOpenIDTokenForDeveloperIdentity")
	defer b.mu.Unlock()

	if _, ok := b.poolsStore(region)[poolID]; !ok {
		return nil, fmt.Errorf("%w: identity pool %q not found", ErrIdentityPoolNotFound, poolID)
	}

	if identityID != "" {
		if _, ok := b.identitiesStore(region)[identityID]; !ok {
			return nil, fmt.Errorf("%w: identity %q not found", ErrIdentityPoolNotFound, identityID)
		}
	}

	if identityID == "" {
		identityID = b.lookupOrCreateDeveloperIdentity(region, poolID, logins)
	}

	payload, err := randomAlphanumeric(tokenLen)
	if err != nil {
		return nil, fmt.Errorf("generate token: %w", err)
	}

	token := fmt.Sprintf("eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.%s.signature", payload)

	return &DeveloperOpenIDToken{
		IdentityID: identityID,
		Token:      token,
	}, nil
}

// principalTagKey returns the composite map key for a pool+provider combination.
// Format: "<poolID>:<providerName>".
func principalTagKey(poolID, providerName string) string {
	return poolID + ":" + providerName
}

// GetPrincipalTagAttributeMap returns the principal tag attribute map for a pool and provider.
func (b *InMemoryBackend) GetPrincipalTagAttributeMap(
	ctx context.Context,
	poolID, providerName string,
) (*PrincipalTagMapping, error) {
	if providerName == "" {
		return nil, fmt.Errorf("%w: IdentityProviderName is required", ErrInvalidParameter)
	}

	region := getRegion(ctx, b.region)

	b.mu.RLock("GetPrincipalTagAttributeMap")
	defer b.mu.RUnlock()

	if _, ok := b.poolsStore(region)[poolID]; !ok {
		return nil, fmt.Errorf("%w: identity pool %q not found", ErrIdentityPoolNotFound, poolID)
	}

	key := principalTagKey(poolID, providerName)

	if m, ok := b.principalTagsStore(region)[key]; ok {
		return clonePrincipalTagMapping(m), nil
	}

	return &PrincipalTagMapping{UseDefaults: true}, nil
}

// ListIdentities returns identities associated with an identity pool, sorted by IdentityId.
// nextToken is an opaque cursor encoding the last-returned IdentityId for pagination.
func (b *InMemoryBackend) ListIdentities(
	ctx context.Context,
	poolID string,
	maxResults int,
	hideDisabled bool,
	nextToken string,
) (*ListIdentitiesResult, error) {
	if poolID == "" {
		return nil, fmt.Errorf("%w: IdentityPoolId is required", ErrInvalidParameter)
	}

	if maxResults < 1 || maxResults > listIdentitiesMaxResults {
		return nil, fmt.Errorf(
			"%w: MaxResults must be between 1 and %d",
			ErrInvalidParameter, listIdentitiesMaxResults,
		)
	}

	region := getRegion(ctx, b.region)

	b.mu.RLock("ListIdentities")
	defer b.mu.RUnlock()

	if _, ok := b.poolsStore(region)[poolID]; !ok {
		return nil, fmt.Errorf("%w: identity pool %q not found", ErrIdentityPoolNotFound, poolID)
	}

	poolIdentities := b.identitiesByPoolStore(region)[poolID]

	// Filter disabled identities (when requested) and sort by IdentityId.
	sorted := filterAndSortIdentities(poolIdentities, hideDisabled)

	// Apply cursor: skip items up to and including the one with the cursor ID.
	startIdx := 0
	if nextToken != "" {
		for i, id := range sorted {
			if id.IdentityID == nextToken {
				startIdx = i + 1

				break
			}
		}
	}

	sorted = sorted[startIdx:]

	limit := len(sorted)
	if maxResults > 0 && maxResults < limit {
		limit = maxResults
	}

	descriptions := make([]IdentityDescription, 0, limit)

	for i := range limit {
		identity := sorted[i]
		logins := make([]string, 0, len(identity.Logins))

		for provider := range identity.Logins {
			logins = append(logins, provider)
		}

		slices.Sort(logins)

		descriptions = append(descriptions, IdentityDescription{
			IdentityID:       identity.IdentityID,
			Logins:           logins,
			CreationDate:     identity.CreatedAt,
			LastModifiedDate: identity.LastModifiedDate,
		})
	}

	// Emit a cursor if there are more pages.
	var returnNextToken string
	if maxResults > 0 && len(sorted) > maxResults {
		returnNextToken = sorted[maxResults-1].IdentityID
	}

	return &ListIdentitiesResult{
		IdentityPoolID: poolID,
		Identities:     descriptions,
		NextToken:      returnNextToken,
	}, nil
}

// ListTagsForResource returns the tags for an identity pool resource by its ARN.
func (b *InMemoryBackend) ListTagsForResource(
	ctx context.Context,
	resourceARN string,
) (map[string]string, error) {
	region := regionFromARN(resourceARN, getRegion(ctx, b.region))

	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	pool, ok := b.poolsByARNStore(region)[resourceARN]
	if !ok {
		return nil, fmt.Errorf("%w: resource %q not found", ErrIdentityPoolNotFound, resourceARN)
	}

	return cloneStringMap(pool.Tags), nil
}

// LookupDeveloperIdentityResult holds the output of LookupDeveloperIdentity.
type LookupDeveloperIdentityResult struct {
	IdentityID                  string
	DeveloperUserIdentifierList []string
}

// LookupDeveloperIdentity retrieves the identity associated with a developer user identifier
// or the list of developer user identifiers associated with an identity.
func (b *InMemoryBackend) LookupDeveloperIdentity(
	ctx context.Context,
	poolID string,
	identityID string,
	developerUserIdentifier string,
	developerProviderName string,
) (*LookupDeveloperIdentityResult, error) {
	if poolID == "" {
		return nil, fmt.Errorf("%w: IdentityPoolId is required", ErrInvalidParameter)
	}

	region := getRegion(ctx, b.region)

	b.mu.RLock("LookupDeveloperIdentity")
	defer b.mu.RUnlock()

	if _, ok := b.poolsStore(region)[poolID]; !ok {
		return nil, fmt.Errorf("%w: identity pool %q not found", ErrIdentityPoolNotFound, poolID)
	}

	if identityID != "" {
		identity, ok := b.identitiesStore(region)[identityID]
		if !ok {
			return nil, fmt.Errorf("%w: identity %q not found", ErrIdentityPoolNotFound, identityID)
		}

		devIDs := developerLoginsFrom(identity.Logins, developerProviderName)

		return &LookupDeveloperIdentityResult{
			IdentityID:                  identity.IdentityID,
			DeveloperUserIdentifierList: devIDs,
		}, nil
	}

	if developerUserIdentifier != "" {
		for _, identity := range b.identitiesByPoolStore(region)[poolID] {
			if v, ok := identity.Logins[developerProviderName]; ok && v == developerUserIdentifier {
				devIDs := developerLoginsFrom(identity.Logins, developerProviderName)

				return &LookupDeveloperIdentityResult{
					IdentityID:                  identity.IdentityID,
					DeveloperUserIdentifierList: devIDs,
				}, nil
			}
		}

		return nil, fmt.Errorf(
			"%w: developer user identifier %q not found",
			ErrIdentityPoolNotFound,
			developerUserIdentifier,
		)
	}

	return nil, fmt.Errorf(
		"%w: either IdentityId or DeveloperUserIdentifier must be provided",
		ErrInvalidParameter,
	)
}

// developerLoginsFrom extracts developer user identifiers from a logins map.
// A developer login key is non-standard (not a well-known provider prefix).
// The result is always sorted for deterministic output.
func developerLoginsFrom(logins map[string]string, developerProviderName string) []string {
	ids := make([]string, 0, len(logins))

	if developerProviderName != "" {
		if v, ok := logins[developerProviderName]; ok {
			ids = append(ids, v)
		}

		return ids
	}

	for _, v := range logins {
		ids = append(ids, v)
	}

	slices.Sort(ids)

	return ids
}

// MergeDeveloperIdentities merges the source identity into the destination identity.
func (b *InMemoryBackend) MergeDeveloperIdentities(
	ctx context.Context,
	sourceUserID string,
	destUserID string,
	developerProviderName string,
	poolID string,
) (*Identity, error) {
	if poolID == "" {
		return nil, fmt.Errorf("%w: IdentityPoolId is required", ErrInvalidParameter)
	}

	if developerProviderName == "" {
		return nil, fmt.Errorf("%w: DeveloperProviderName is required", ErrInvalidParameter)
	}

	if sourceUserID == "" {
		return nil, fmt.Errorf("%w: SourceUserIdentifier is required", ErrInvalidParameter)
	}

	if destUserID == "" {
		return nil, fmt.Errorf("%w: DestinationUserIdentifier is required", ErrInvalidParameter)
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("MergeDeveloperIdentities")
	defer b.mu.Unlock()

	if _, ok := b.poolsStore(region)[poolID]; !ok {
		return nil, fmt.Errorf("%w: identity pool %q not found", ErrIdentityPoolNotFound, poolID)
	}

	var sourceIdentity, destIdentity *Identity

	for _, identity := range b.identitiesByPoolStore(region)[poolID] {
		if v, ok := identity.Logins[developerProviderName]; ok {
			switch v {
			case sourceUserID:
				sourceIdentity = identity
			case destUserID:
				destIdentity = identity
			}
		}
	}

	if sourceIdentity == nil {
		return nil, fmt.Errorf(
			"%w: source developer user %q not found",
			ErrIdentityPoolNotFound,
			sourceUserID,
		)
	}

	if destIdentity == nil {
		return nil, fmt.Errorf(
			"%w: destination developer user %q not found",
			ErrIdentityPoolNotFound,
			destUserID,
		)
	}

	// Merge logins from source into destination.
	maps.Copy(destIdentity.Logins, sourceIdentity.Logins)
	destIdentity.LastModifiedDate = time.Now()

	// Remove source identity.
	ids := b.identitiesStore(region)
	delete(ids, sourceIdentity.IdentityID)

	idsByPool := b.identitiesByPoolStore(region)
	updated := make([]*Identity, 0, len(idsByPool[poolID])-1)

	for _, i := range idsByPool[poolID] {
		if i.IdentityID != sourceIdentity.IdentityID {
			updated = append(updated, i)
		}
	}

	idsByPool[poolID] = updated

	return cloneIdentity(destIdentity), nil
}

// UnlinkIdentity removes login providers from an identity after validating
// the supplied login tokens.
func (b *InMemoryBackend) UnlinkIdentity(
	ctx context.Context,
	identityID string,
	logins map[string]string,
	loginsToRemove []string,
) error {
	if identityID == "" {
		return fmt.Errorf("%w: IdentityId is required", ErrInvalidParameter)
	}

	if len(loginsToRemove) == 0 {
		return fmt.Errorf("%w: LoginsToRemove must not be empty", ErrInvalidParameter)
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("UnlinkIdentity")
	defer b.mu.Unlock()

	identity, ok := b.identitiesStore(region)[identityID]
	if !ok {
		return fmt.Errorf("%w: identity %q not found", ErrIdentityPoolNotFound, identityID)
	}

	for _, providerName := range loginsToRemove {
		loginToken, hasLoginToken := logins[providerName]
		if !hasLoginToken {
			return fmt.Errorf(
				"%w: login token for provider %q is required",
				ErrInvalidParameter,
				providerName,
			)
		}

		identityToken, exists := identity.Logins[providerName]
		if !exists {
			return fmt.Errorf(
				"%w: provider %q is not linked to identity",
				ErrNotAuthorized,
				providerName,
			)
		}

		if identityToken != loginToken {
			return fmt.Errorf(
				"%w: invalid login token for provider %q",
				ErrNotAuthorized,
				providerName,
			)
		}

		delete(identity.Logins, providerName)
	}

	identity.LastModifiedDate = time.Now()

	return nil
}

// UnlinkDeveloperIdentity removes a developer-provider association from an identity.
func (b *InMemoryBackend) UnlinkDeveloperIdentity(
	ctx context.Context,
	identityID string,
	poolID string,
	developerProviderName string,
	developerUserIdentifier string,
) error {
	if identityID == "" {
		return fmt.Errorf("%w: IdentityId is required", ErrInvalidParameter)
	}

	if poolID == "" {
		return fmt.Errorf("%w: IdentityPoolId is required", ErrInvalidParameter)
	}

	if developerProviderName == "" {
		return fmt.Errorf("%w: DeveloperProviderName is required", ErrInvalidParameter)
	}

	if developerUserIdentifier == "" {
		return fmt.Errorf("%w: DeveloperUserIdentifier is required", ErrInvalidParameter)
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("UnlinkDeveloperIdentity")
	defer b.mu.Unlock()

	if _, ok := b.poolsStore(region)[poolID]; !ok {
		return fmt.Errorf("%w: identity pool %q not found", ErrIdentityPoolNotFound, poolID)
	}

	identity, ok := b.identitiesStore(region)[identityID]
	if !ok {
		return fmt.Errorf("%w: identity %q not found", ErrIdentityPoolNotFound, identityID)
	}

	if identity.IdentityPoolID != poolID {
		return fmt.Errorf(
			"%w: identity %q not found in pool %q",
			ErrIdentityPoolNotFound,
			identityID,
			poolID,
		)
	}

	existingUserIdentifier, ok := identity.Logins[developerProviderName]
	if !ok {
		return fmt.Errorf(
			"%w: provider %q is not linked to identity",
			ErrNotAuthorized,
			developerProviderName,
		)
	}

	if existingUserIdentifier != developerUserIdentifier {
		return fmt.Errorf(
			"%w: developer user identifier %q not linked to provider %q",
			ErrNotAuthorized,
			developerUserIdentifier,
			developerProviderName,
		)
	}

	delete(identity.Logins, developerProviderName)
	identity.LastModifiedDate = time.Now()

	return nil
}

// SetPrincipalTagAttributeMap configures principal tag attribute mappings for a pool and provider.
func (b *InMemoryBackend) SetPrincipalTagAttributeMap(
	ctx context.Context,
	poolID string,
	providerName string,
	useDefaults bool,
	principalTags map[string]string,
) (*PrincipalTagMapping, error) {
	if providerName == "" {
		return nil, fmt.Errorf("%w: IdentityProviderName is required", ErrInvalidParameter)
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("SetPrincipalTagAttributeMap")
	defer b.mu.Unlock()

	if _, ok := b.poolsStore(region)[poolID]; !ok {
		return nil, fmt.Errorf("%w: identity pool %q not found", ErrIdentityPoolNotFound, poolID)
	}

	mapping := &PrincipalTagMapping{
		UseDefaults:   useDefaults,
		PrincipalTags: cloneStringMap(principalTags),
	}

	b.principalTagsStore(region)[principalTagKey(poolID, providerName)] = mapping

	return clonePrincipalTagMapping(mapping), nil
}

// TagResource adds or updates tags on an identity pool resource by ARN.
func (b *InMemoryBackend) TagResource(
	ctx context.Context,
	resourceARN string,
	tags map[string]string,
) error {
	if resourceARN == "" {
		return fmt.Errorf("%w: ResourceArn is required", ErrInvalidParameter)
	}

	region := regionFromARN(resourceARN, getRegion(ctx, b.region))

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	pool, ok := b.poolsByARNStore(region)[resourceARN]
	if !ok {
		return fmt.Errorf("%w: resource %q not found", ErrIdentityPoolNotFound, resourceARN)
	}

	if pool.Tags == nil {
		pool.Tags = make(map[string]string)
	}

	maps.Copy(pool.Tags, tags)

	return nil
}

// UntagResource removes the given tag keys from an identity pool resource by ARN.
func (b *InMemoryBackend) UntagResource(
	ctx context.Context,
	resourceARN string,
	tagKeys []string,
) error {
	if resourceARN == "" {
		return fmt.Errorf("%w: ResourceArn is required", ErrInvalidParameter)
	}

	if len(tagKeys) == 0 {
		return fmt.Errorf("%w: TagKeys must not be empty", ErrInvalidParameter)
	}

	region := regionFromARN(resourceARN, getRegion(ctx, b.region))

	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	pool, ok := b.poolsByARNStore(region)[resourceARN]
	if !ok {
		return fmt.Errorf("%w: resource %q not found", ErrIdentityPoolNotFound, resourceARN)
	}

	for _, k := range tagKeys {
		delete(pool.Tags, k)
	}

	return nil
}

// clonePrincipalTagMapping returns a deep copy of a PrincipalTagMapping.
func clonePrincipalTagMapping(m *PrincipalTagMapping) *PrincipalTagMapping {
	return &PrincipalTagMapping{
		UseDefaults:   m.UseDefaults,
		PrincipalTags: cloneStringMap(m.PrincipalTags),
	}
}

// cloneRoleMappings returns a deep copy of a RoleMapping map.
func cloneRoleMappings(m map[string]RoleMapping) map[string]RoleMapping {
	if m == nil {
		return nil
	}

	out := make(map[string]RoleMapping, len(m))

	for k, v := range m {
		rm := RoleMapping{
			Type:                    v.Type,
			AmbiguousRoleResolution: v.AmbiguousRoleResolution,
		}

		if v.RulesConfiguration != nil {
			rules := make([]MappingRule, len(v.RulesConfiguration.Rules))
			copy(rules, v.RulesConfiguration.Rules)
			rm.RulesConfiguration = &RulesConfiguration{Rules: rules}
		}

		out[k] = rm
	}

	return out
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

// filterAndSortIdentities returns a new slice containing identities from src, optionally
// excluding disabled ones, sorted by IdentityID for deterministic output.
func filterAndSortIdentities(src []*Identity, hideDisabled bool) []*Identity {
	out := make([]*Identity, 0, len(src))

	for _, id := range src {
		if hideDisabled && !id.Enabled {
			continue
		}

		out = append(out, id)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].IdentityID < out[j].IdentityID
	})

	return out
}

// anyLoginMatches returns true if any (provider, token) pair in req also exists in stored.
// This implements AWS GetId semantics: an existing identity is returned when any of the
// requested login providers is already linked to it.
func anyLoginMatches(stored, req map[string]string) bool {
	for provider, token := range req {
		if stored[provider] == token {
			return true
		}
	}

	return false
}

// randomAlphanumeric returns a random alphanumeric string of length n.
// It reads a single batch of random bytes from crypto/rand and uses rejection
// sampling to avoid modulo bias, falling back to additional reads only when
// the initial batch has insufficient usable bytes (~3 % rejection rate).
func randomAlphanumeric(n int) (string, error) {
	const (
		charsLen = len(alphanumChars) // 62
		byteMax  = 256                // total number of distinct byte values
		// cutoff is the largest multiple of charsLen that fits in a byte (0–255).
		// Bytes in [0, cutoff) map uniformly; bytes in [cutoff, byteMax) are rejected.
		cutoff = (byteMax / charsLen) * charsLen // 248
		// batchOversize is extra bytes read per iteration to reduce retry probability.
		batchOversize = 16
	)

	result := make([]byte, 0, n)

	for len(result) < n {
		// Over-read to reduce the chance of needing more than one batch.
		raw := make([]byte, n+batchOversize)
		if _, err := io.ReadFull(rand.Reader, raw); err != nil {
			return "", fmt.Errorf("crypto/rand failure: %w", err)
		}

		for _, b := range raw {
			if int(b) < cutoff {
				result = append(result, alphanumChars[int(b)%charsLen])
				if len(result) == n {
					break
				}
			}
		}
	}

	return string(result), nil
}

// Reset clears all identity pool, identity and role state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.pools = make(map[string]map[string]*IdentityPool)
	b.poolsByName = make(map[string]map[string]*IdentityPool)
	b.poolsByARN = make(map[string]map[string]*IdentityPool)
	b.identities = make(map[string]map[string]*Identity)
	b.identitiesByPool = make(map[string]map[string][]*Identity)
	b.roles = make(map[string]map[string]*IdentityRoles)
	b.principalTags = make(map[string]map[string]*PrincipalTagMapping)
}

// AddPoolInternal seeds an identity pool directly into the backend for testing purposes.
// It bypasses the normal CreateIdentityPool validation. The region is derived from the pool's ARN.
func (b *InMemoryBackend) AddPoolInternal(pool *IdentityPool) {
	b.mu.Lock("AddPoolInternal")
	defer b.mu.Unlock()

	region := regionFromARN(pool.ARN, b.region)
	cp := clonePool(pool)
	b.poolsStore(region)[cp.IdentityPoolID] = cp
	b.poolsByNameStore(region)[cp.IdentityPoolName] = cp
	b.poolsByARNStore(region)[cp.ARN] = cp
}

// AddIdentityInternal seeds an identity directly into the backend for testing purposes.
// The region is derived from the identity's IdentityPoolID prefix (format: "region:uuid").
func (b *InMemoryBackend) AddIdentityInternal(identity *Identity) {
	b.mu.Lock("AddIdentityInternal")
	defer b.mu.Unlock()

	region := b.region
	if parts := strings.SplitN(identity.IdentityPoolID, ":", identityIDParts); len(parts) == identityIDParts {
		region = parts[0]
	}

	cp := cloneIdentity(identity)
	b.identitiesStore(region)[cp.IdentityID] = cp
	b.identitiesByPoolStore(region)[cp.IdentityPoolID] = append(
		b.identitiesByPoolStore(region)[cp.IdentityPoolID],
		cp,
	)
}
