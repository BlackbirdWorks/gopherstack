package cognitoidentity

import (
	"crypto/rand"
	"fmt"
	"maps"
	"math/big"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
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

// IdentityPool represents an Amazon Cognito Identity Pool.
type IdentityPool struct {
	CreatedAt                      time.Time          `json:"createdAt"`
	SupportedLoginProviders        map[string]string  `json:"supportedLoginProviders,omitempty"`
	Tags                           map[string]string  `json:"tags,omitempty"`
	IdentityPoolID                 string             `json:"identityPoolID"`
	IdentityPoolName               string             `json:"identityPoolName"`
	ARN                            string             `json:"arn"`
	IdentityProviders              []IdentityProvider `json:"identityProviders,omitempty"`
	AllowUnauthenticatedIdentities bool               `json:"allowUnauthenticatedIdentities"`
	AllowClassicFlow               bool               `json:"allowClassicFlow"`
}

// IdentityRoles holds IAM role mappings for an identity pool.
type IdentityRoles struct {
	AuthenticatedRoleARN   string `json:"authenticatedRoleARN"`
	UnauthenticatedRoleARN string `json:"unauthenticatedRoleARN"`
}

// Identity represents a federated identity.
type Identity struct {
	CreatedAt      time.Time         `json:"createdAt"`
	Logins         map[string]string `json:"logins,omitempty"`
	IdentityID     string            `json:"identityID"`
	IdentityPoolID string            `json:"identityPoolID"`
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
type InMemoryBackend struct {
	mu               *lockmetrics.RWMutex
	pools            map[string]*IdentityPool
	poolsByName      map[string]*IdentityPool
	poolsByARN       map[string]*IdentityPool // ARN → pool (for tag/resource ops)
	identities       map[string]*Identity
	identitiesByPool map[string][]*Identity // poolID → identities (O(1) GetID lookup)
	roles            map[string]*IdentityRoles
	principalTags    map[string]*PrincipalTagMapping // key: poolID:providerName
	accountID        string
	region           string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		mu:               lockmetrics.New("cognitoidentity"),
		pools:            make(map[string]*IdentityPool),
		poolsByName:      make(map[string]*IdentityPool),
		poolsByARN:       make(map[string]*IdentityPool),
		identities:       make(map[string]*Identity),
		identitiesByPool: make(map[string][]*Identity),
		roles:            make(map[string]*IdentityRoles),
		principalTags:    make(map[string]*PrincipalTagMapping),
		accountID:        accountID,
		region:           region,
	}
}

// Region returns the region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// CreateIdentityPool creates a new identity pool.
func (b *InMemoryBackend) CreateIdentityPool(
	name string,
	allowUnauthenticated bool,
	allowClassicFlow bool,
	providers []IdentityProvider,
	supportedLoginProviders map[string]string,
	tags map[string]string,
) (*IdentityPool, error) {
	b.mu.Lock("CreateIdentityPool")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: IdentityPoolName is required", ErrInvalidParameter)
	}

	if _, ok := b.poolsByName[name]; ok {
		return nil, fmt.Errorf("%w: identity pool %q already exists", ErrIdentityPoolAlreadyExists, name)
	}

	poolID := b.region + ":" + uuid.New().String()
	arn := fmt.Sprintf("arn:aws:cognito-identity:%s:%s:identitypool/%s", b.region, b.accountID, poolID)

	pool := &IdentityPool{
		IdentityPoolID:                 poolID,
		IdentityPoolName:               name,
		ARN:                            arn,
		AllowUnauthenticatedIdentities: allowUnauthenticated,
		AllowClassicFlow:               allowClassicFlow,
		IdentityProviders:              cloneProviders(providers),
		SupportedLoginProviders:        cloneStringMap(supportedLoginProviders),
		Tags:                           cloneStringMap(tags),
		CreatedAt:                      time.Now(),
	}

	b.pools[poolID] = pool
	b.poolsByName[name] = pool
	b.poolsByARN[arn] = pool

	return clonePool(pool), nil
}

// DeleteIdentityPool removes an identity pool and all associated identities and roles.
func (b *InMemoryBackend) DeleteIdentityPool(poolID string) error {
	b.mu.Lock("DeleteIdentityPool")
	defer b.mu.Unlock()

	pool, ok := b.pools[poolID]
	if !ok {
		return fmt.Errorf("%w: identity pool %q not found", ErrIdentityPoolNotFound, poolID)
	}

	delete(b.poolsByName, pool.IdentityPoolName)
	delete(b.poolsByARN, pool.ARN)
	delete(b.pools, poolID)
	delete(b.roles, poolID)

	for id, identity := range b.identities {
		if identity.IdentityPoolID == poolID {
			delete(b.identities, id)
		}
	}

	delete(b.identitiesByPool, poolID)

	return nil
}

// DescribeIdentityPool returns the identity pool with the given ID.
func (b *InMemoryBackend) DescribeIdentityPool(poolID string) (*IdentityPool, error) {
	b.mu.RLock("DescribeIdentityPool")
	defer b.mu.RUnlock()

	pool, ok := b.pools[poolID]
	if !ok {
		return nil, fmt.Errorf("%w: identity pool %q not found", ErrIdentityPoolNotFound, poolID)
	}

	return clonePool(pool), nil
}

// ListIdentityPools returns all identity pools, up to maxResults (0 = all).
func (b *InMemoryBackend) ListIdentityPools(maxResults int) []*IdentityPool {
	b.mu.RLock("ListIdentityPools")
	defer b.mu.RUnlock()

	out := make([]*IdentityPool, 0, len(b.pools))
	for _, p := range b.pools {
		out = append(out, clonePool(p))

		if maxResults > 0 && len(out) >= maxResults {
			break
		}
	}

	return out
}

// UpdateIdentityPool updates the settings of an existing identity pool.
func (b *InMemoryBackend) UpdateIdentityPool(
	poolID string,
	name string,
	allowUnauthenticated bool,
	allowClassicFlow bool,
	providers []IdentityProvider,
	supportedLoginProviders map[string]string,
) (*IdentityPool, error) {
	b.mu.Lock("UpdateIdentityPool")
	defer b.mu.Unlock()

	pool, ok := b.pools[poolID]
	if !ok {
		return nil, fmt.Errorf("%w: identity pool %q not found", ErrIdentityPoolNotFound, poolID)
	}

	if name != "" && name != pool.IdentityPoolName {
		if _, exists := b.poolsByName[name]; exists {
			return nil, fmt.Errorf("%w: identity pool %q already exists", ErrIdentityPoolAlreadyExists, name)
		}

		delete(b.poolsByName, pool.IdentityPoolName)
		pool.IdentityPoolName = name
		b.poolsByName[name] = pool
	}

	pool.AllowUnauthenticatedIdentities = allowUnauthenticated
	pool.AllowClassicFlow = allowClassicFlow
	pool.IdentityProviders = cloneProviders(providers)
	pool.SupportedLoginProviders = cloneStringMap(supportedLoginProviders)

	return clonePool(pool), nil
}

// GetID returns an existing identity or creates a new one for the given pool and logins.
func (b *InMemoryBackend) GetID(poolID string, _ string, logins map[string]string) (*Identity, error) {
	b.mu.Lock("GetID")
	defer b.mu.Unlock()

	if _, ok := b.pools[poolID]; !ok {
		return nil, fmt.Errorf("%w: identity pool %q not found", ErrIdentityPoolNotFound, poolID)
	}

	// O(n pool size) but bounded to the specific pool, not all identities.
	for _, identity := range b.identitiesByPool[poolID] {
		if mapsEqual(identity.Logins, logins) {
			return cloneIdentity(identity), nil
		}
	}

	// Create a new identity.
	identityID := b.region + ":" + uuid.New().String()
	identity := &Identity{
		IdentityID:     identityID,
		IdentityPoolID: poolID,
		Logins:         cloneStringMap(logins),
		CreatedAt:      time.Now(),
	}

	b.identities[identityID] = identity
	b.identitiesByPool[poolID] = append(b.identitiesByPool[poolID], identity)

	return cloneIdentity(identity), nil
}

// GetCredentialsForIdentity returns synthetic temporary AWS credentials for an identity.
func (b *InMemoryBackend) GetCredentialsForIdentity(identityID string, _ map[string]string) (*Credentials, error) {
	b.mu.RLock("GetCredentialsForIdentity")
	defer b.mu.RUnlock()

	if _, ok := b.identities[identityID]; !ok {
		return nil, fmt.Errorf("%w: identity %q not found", ErrIdentityPoolNotFound, identityID)
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
func (b *InMemoryBackend) GetOpenIDToken(identityID string, _ map[string]string) (*OpenIDToken, error) {
	b.mu.RLock("GetOpenIDToken")
	defer b.mu.RUnlock()

	if _, ok := b.identities[identityID]; !ok {
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
func (b *InMemoryBackend) SetIdentityPoolRoles(poolID, authenticatedARN, unauthenticatedARN string) error {
	b.mu.Lock("SetIdentityPoolRoles")
	defer b.mu.Unlock()

	if _, ok := b.pools[poolID]; !ok {
		return fmt.Errorf("%w: identity pool %q not found", ErrIdentityPoolNotFound, poolID)
	}

	b.roles[poolID] = &IdentityRoles{
		AuthenticatedRoleARN:   authenticatedARN,
		UnauthenticatedRoleARN: unauthenticatedARN,
	}

	return nil
}

// GetIdentityPoolRoles returns the IAM roles configured for an identity pool.
func (b *InMemoryBackend) GetIdentityPoolRoles(poolID string) (*IdentityRoles, error) {
	b.mu.RLock("GetIdentityPoolRoles")
	defer b.mu.RUnlock()

	if _, ok := b.pools[poolID]; !ok {
		return nil, fmt.Errorf("%w: identity pool %q not found", ErrIdentityPoolNotFound, poolID)
	}

	roles, ok := b.roles[poolID]
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
func (b *InMemoryBackend) DeleteIdentities(identityIDs []string) []UnprocessedIdentityID {
	b.mu.Lock("DeleteIdentities")
	defer b.mu.Unlock()

	var unprocessed []UnprocessedIdentityID

	for _, id := range identityIDs {
		identity, ok := b.identities[id]
		if !ok {
			continue
		}

		poolID := identity.IdentityPoolID

		delete(b.identities, id)

		// Remove from identitiesByPool slice.
		existing := b.identitiesByPool[poolID]
		updated := make([]*Identity, 0, len(existing))

		for _, i := range existing {
			if i.IdentityID != id {
				updated = append(updated, i)
			}
		}

		b.identitiesByPool[poolID] = updated
	}

	return unprocessed
}

// DescribeIdentity returns metadata about a specific federated identity.
func (b *InMemoryBackend) DescribeIdentity(identityID string) (*IdentityDescription, error) {
	b.mu.RLock("DescribeIdentity")
	defer b.mu.RUnlock()

	identity, ok := b.identities[identityID]
	if !ok {
		return nil, fmt.Errorf("%w: identity %q not found", ErrIdentityPoolNotFound, identityID)
	}

	logins := make([]string, 0, len(identity.Logins))
	for provider := range identity.Logins {
		logins = append(logins, provider)
	}

	return &IdentityDescription{
		IdentityID:       identity.IdentityID,
		Logins:           logins,
		CreationDate:     identity.CreatedAt,
		LastModifiedDate: identity.CreatedAt,
	}, nil
}

// DeveloperOpenIDToken is the result of GetOpenIDTokenForDeveloperIdentity.
type DeveloperOpenIDToken struct {
	IdentityID string
	Token      string
}

// GetOpenIDTokenForDeveloperIdentity registers or retrieves an identity for a developer
// authenticated user, then returns a synthetic OpenID token.
func (b *InMemoryBackend) GetOpenIDTokenForDeveloperIdentity(
	poolID string,
	identityID string,
	logins map[string]string,
	_ int64,
) (*DeveloperOpenIDToken, error) {
	b.mu.Lock("GetOpenIDTokenForDeveloperIdentity")
	defer b.mu.Unlock()

	if _, ok := b.pools[poolID]; !ok {
		return nil, fmt.Errorf("%w: identity pool %q not found", ErrIdentityPoolNotFound, poolID)
	}

	if identityID != "" {
		if _, ok := b.identities[identityID]; !ok {
			return nil, fmt.Errorf("%w: identity %q not found", ErrIdentityPoolNotFound, identityID)
		}
	} else {
		// Look up by developer login tokens or create new.
		for _, identity := range b.identitiesByPool[poolID] {
			if mapsEqual(identity.Logins, logins) {
				identityID = identity.IdentityID
			}
		}

		if identityID == "" {
			newID := b.region + ":" + uuid.New().String()
			identity := &Identity{
				IdentityID:     newID,
				IdentityPoolID: poolID,
				Logins:         cloneStringMap(logins),
				CreatedAt:      time.Now(),
			}

			b.identities[newID] = identity
			b.identitiesByPool[poolID] = append(b.identitiesByPool[poolID], identity)
			identityID = newID
		}
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
func principalTagKey(poolID, providerName string) string {
	return poolID + ":" + providerName
}

// GetPrincipalTagAttributeMap returns the principal tag attribute map for a pool and provider.
func (b *InMemoryBackend) GetPrincipalTagAttributeMap(poolID, providerName string) (*PrincipalTagMapping, error) {
	b.mu.RLock("GetPrincipalTagAttributeMap")
	defer b.mu.RUnlock()

	if _, ok := b.pools[poolID]; !ok {
		return nil, fmt.Errorf("%w: identity pool %q not found", ErrIdentityPoolNotFound, poolID)
	}

	key := principalTagKey(poolID, providerName)

	if m, ok := b.principalTags[key]; ok {
		return clonePrincipalTagMapping(m), nil
	}

	return &PrincipalTagMapping{UseDefaults: true}, nil
}

// ListIdentities returns identities associated with an identity pool.
func (b *InMemoryBackend) ListIdentities(poolID string, maxResults int, _ bool) (*ListIdentitiesResult, error) {
	b.mu.RLock("ListIdentities")
	defer b.mu.RUnlock()

	if _, ok := b.pools[poolID]; !ok {
		return nil, fmt.Errorf("%w: identity pool %q not found", ErrIdentityPoolNotFound, poolID)
	}

	poolIdentities := b.identitiesByPool[poolID]

	limit := len(poolIdentities)
	if maxResults > 0 && maxResults < limit {
		limit = maxResults
	}

	descriptions := make([]IdentityDescription, 0, limit)

	for i := range limit {
		identity := poolIdentities[i]
		logins := make([]string, 0, len(identity.Logins))

		for provider := range identity.Logins {
			logins = append(logins, provider)
		}

		descriptions = append(descriptions, IdentityDescription{
			IdentityID:       identity.IdentityID,
			Logins:           logins,
			CreationDate:     identity.CreatedAt,
			LastModifiedDate: identity.CreatedAt,
		})
	}

	return &ListIdentitiesResult{
		IdentityPoolID: poolID,
		Identities:     descriptions,
	}, nil
}

// ListTagsForResource returns the tags for an identity pool resource by its ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	pool, ok := b.poolsByARN[resourceARN]
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
	poolID string,
	identityID string,
	developerUserIdentifier string,
	developerProviderName string,
) (*LookupDeveloperIdentityResult, error) {
	b.mu.RLock("LookupDeveloperIdentity")
	defer b.mu.RUnlock()

	if _, ok := b.pools[poolID]; !ok {
		return nil, fmt.Errorf("%w: identity pool %q not found", ErrIdentityPoolNotFound, poolID)
	}

	if identityID != "" {
		identity, ok := b.identities[identityID]
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
		for _, identity := range b.identitiesByPool[poolID] {
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

	return nil, fmt.Errorf("%w: either IdentityId or DeveloperUserIdentifier must be provided", ErrInvalidParameter)
}

// developerLoginsFrom extracts developer user identifiers from a logins map.
// A developer login key is non-standard (not a well-known provider prefix).
func developerLoginsFrom(logins map[string]string, developerProviderName string) []string {
	ids := make([]string, 0)

	if developerProviderName != "" {
		if v, ok := logins[developerProviderName]; ok {
			ids = append(ids, v)
		}

		return ids
	}

	for _, v := range logins {
		ids = append(ids, v)
	}

	return ids
}

// MergeDeveloperIdentities merges the source identity into the destination identity.
func (b *InMemoryBackend) MergeDeveloperIdentities(
	sourceUserID string,
	destUserID string,
	developerProviderName string,
	poolID string,
) (*Identity, error) {
	b.mu.Lock("MergeDeveloperIdentities")
	defer b.mu.Unlock()

	if _, ok := b.pools[poolID]; !ok {
		return nil, fmt.Errorf("%w: identity pool %q not found", ErrIdentityPoolNotFound, poolID)
	}

	var sourceIdentity, destIdentity *Identity

	for _, identity := range b.identitiesByPool[poolID] {
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
		return nil, fmt.Errorf("%w: source developer user %q not found", ErrIdentityPoolNotFound, sourceUserID)
	}

	if destIdentity == nil {
		return nil, fmt.Errorf("%w: destination developer user %q not found", ErrIdentityPoolNotFound, destUserID)
	}

	// Merge logins from source into destination.
	maps.Copy(destIdentity.Logins, sourceIdentity.Logins)

	// Remove source identity.
	delete(b.identities, sourceIdentity.IdentityID)

	updated := make([]*Identity, 0, len(b.identitiesByPool[poolID])-1)

	for _, i := range b.identitiesByPool[poolID] {
		if i.IdentityID != sourceIdentity.IdentityID {
			updated = append(updated, i)
		}
	}

	b.identitiesByPool[poolID] = updated

	return cloneIdentity(destIdentity), nil
}

// SetPrincipalTagAttributeMap configures principal tag attribute mappings for a pool and provider.
func (b *InMemoryBackend) SetPrincipalTagAttributeMap(
	poolID string,
	providerName string,
	useDefaults bool,
	principalTags map[string]string,
) (*PrincipalTagMapping, error) {
	b.mu.Lock("SetPrincipalTagAttributeMap")
	defer b.mu.Unlock()

	if _, ok := b.pools[poolID]; !ok {
		return nil, fmt.Errorf("%w: identity pool %q not found", ErrIdentityPoolNotFound, poolID)
	}

	mapping := &PrincipalTagMapping{
		UseDefaults:   useDefaults,
		PrincipalTags: cloneStringMap(principalTags),
	}

	b.principalTags[principalTagKey(poolID, providerName)] = mapping

	return clonePrincipalTagMapping(mapping), nil
}

// TagResource adds or updates tags on an identity pool resource by ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	pool, ok := b.poolsByARN[resourceARN]
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
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	pool, ok := b.poolsByARN[resourceARN]
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

// clonePool returns a deep copy of an IdentityPool to prevent callers from
// mutating the backend's internal maps and slices.
func clonePool(pool *IdentityPool) *IdentityPool {
	cp := *pool
	cp.IdentityProviders = cloneProviders(pool.IdentityProviders)
	cp.SupportedLoginProviders = cloneStringMap(pool.SupportedLoginProviders)
	cp.Tags = cloneStringMap(pool.Tags)

	return &cp
}

// cloneIdentity returns a deep copy of an Identity to prevent callers from
// mutating the backend's internal Logins map.
func cloneIdentity(identity *Identity) *Identity {
	cp := *identity
	cp.Logins = cloneStringMap(identity.Logins)

	return &cp
}

// mapsEqual returns true if both maps have the same key-value pairs.
func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}

	for k, v := range a {
		if b[k] != v {
			return false
		}
	}

	return true
}

// randomAlphanumeric returns a random alphanumeric string of length n.
func randomAlphanumeric(n int) (string, error) {
	buf := make([]byte, n)
	for i := range buf {
		idx, err := rand.Int(rand.Reader, big.NewInt(int64(len(alphanumChars))))
		if err != nil {
			return "", fmt.Errorf("crypto/rand failure: %w", err)
		}

		buf[i] = alphanumChars[idx.Int64()]
	}

	return string(buf), nil
}

// Reset clears all identity pool, identity and role state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.pools = make(map[string]*IdentityPool)
	b.poolsByName = make(map[string]*IdentityPool)
	b.poolsByARN = make(map[string]*IdentityPool)
	b.identities = make(map[string]*Identity)
	b.identitiesByPool = make(map[string][]*Identity)
	b.roles = make(map[string]*IdentityRoles)
	b.principalTags = make(map[string]*PrincipalTagMapping)
}
