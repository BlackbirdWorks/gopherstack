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

// InMemoryBackend is the in-memory store for Cognito Identity Pool resources.
type InMemoryBackend struct {
	mu               *lockmetrics.RWMutex
	pools            map[string]*IdentityPool
	poolsByName      map[string]*IdentityPool
	identities       map[string]*Identity
	identitiesByPool map[string][]*Identity // poolID → identities (O(1) GetID lookup)
	roles            map[string]*IdentityRoles
	accountID        string
	region           string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		mu:               lockmetrics.New("cognitoidentity"),
		pools:            make(map[string]*IdentityPool),
		poolsByName:      make(map[string]*IdentityPool),
		identities:       make(map[string]*Identity),
		identitiesByPool: make(map[string][]*Identity),
		roles:            make(map[string]*IdentityRoles),
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
	b.identities = make(map[string]*Identity)
	b.identitiesByPool = make(map[string][]*Identity)
	b.roles = make(map[string]*IdentityRoles)
}
