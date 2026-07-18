package cognitoidentity

import (
	"context"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
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

// regionKey builds the composite store.Table primary (or index) key "region|id" shared
// by every resource collection below, which was previously nested by region (outer key =
// region, e.g. map[string]map[string]*IdentityPool). See store_setup.go.
func regionKey(region, id string) string { return region + "|" + id }

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

// InMemoryBackend is the in-memory store for Cognito Identity Pool resources.
//
// The four resource collections below were previously nested by region (outer key =
// region, e.g. map[string]map[string]*IdentityPool) so that same-named resources are
// isolated across regions. Each is now a flat *store.Table keyed by the composite
// "region|id" string (see regionKey), with companion *store.Index values replacing the
// old reverse-lookup maps (poolsByName, poolsByARN, identitiesByPool) and the
// prefix-scan used to purge a pool's principal-tag mappings. Callers must hold b.mu
// while accessing any table or index, exactly as they held it for the old maps --
// store.Table and store.Index perform no locking of their own.
type InMemoryBackend struct {
	pools               *store.Table[IdentityPool]
	poolsByRegion       *store.Index[IdentityPool]
	poolsByName         *store.Index[IdentityPool]
	poolsByARN          *store.Index[IdentityPool]
	identities          *store.Table[Identity]
	identitiesByPool    *store.Index[Identity]
	roles               *store.Table[IdentityRoles]
	principalTags       *store.Table[PrincipalTagMapping]
	principalTagsByPool *store.Index[PrincipalTagMapping]
	registry            *store.Registry
	mu                  *lockmetrics.RWMutex
	accountID           string
	region              string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		mu:        lockmetrics.New("cognitoidentity"),
		registry:  store.NewRegistry(),
		accountID: accountID,
		region:    region,
	}

	registerAllTables(b)

	return b
}

// The following Get/Put/Delete/In* helpers replace the old lazy per-region map
// accessors (poolsStore(region) etc.) with store.Table / store.Index operations.
// Callers must still hold b.mu, exactly as before.

func (b *InMemoryBackend) poolGet(region, poolID string) (*IdentityPool, bool) {
	return b.pools.Get(regionKey(region, poolID))
}

func (b *InMemoryBackend) poolPut(v *IdentityPool) { b.pools.Put(v) }

func (b *InMemoryBackend) poolDelete(region, poolID string) {
	b.pools.Delete(regionKey(region, poolID))
}

func (b *InMemoryBackend) poolsInRegion(region string) []*IdentityPool {
	return b.poolsByRegion.Get(region)
}

// poolNameTaken reports whether an identity pool named name already exists in region.
// AWS enforces pool-name uniqueness per region+account, so the index group holds at
// most one entry.
func (b *InMemoryBackend) poolNameTaken(region, name string) bool {
	return len(b.poolsByName.Get(regionKey(region, name))) > 0
}

// poolByARN returns the identity pool with the given ARN in region, if any. An ARN is
// unique per pool, so the index group holds at most one entry.
func (b *InMemoryBackend) poolByARN(region, arn string) (*IdentityPool, bool) {
	matches := b.poolsByARN.Get(regionKey(region, arn))
	if len(matches) == 0 {
		return nil, false
	}

	return matches[0], true
}

func (b *InMemoryBackend) identityGet(region, identityID string) (*Identity, bool) {
	return b.identities.Get(regionKey(region, identityID))
}

func (b *InMemoryBackend) identityPut(v *Identity) { b.identities.Put(v) }

func (b *InMemoryBackend) identityDelete(region, identityID string) {
	b.identities.Delete(regionKey(region, identityID))
}

func (b *InMemoryBackend) identitiesInPool(region, poolID string) []*Identity {
	return b.identitiesByPool.Get(regionKey(region, poolID))
}

func (b *InMemoryBackend) rolesGet(region, poolID string) (*IdentityRoles, bool) {
	return b.roles.Get(regionKey(region, poolID))
}

func (b *InMemoryBackend) rolesPut(v *IdentityRoles) { b.roles.Put(v) }

func (b *InMemoryBackend) rolesDelete(region, poolID string) bool {
	return b.roles.Delete(regionKey(region, poolID))
}

// principalTagKey returns the composite map key for a pool+provider combination.
// Format: "<poolID>:<providerName>".
func principalTagKey(poolID, providerName string) string {
	return poolID + ":" + providerName
}

func (b *InMemoryBackend) principalTagGet(region, poolID, providerName string) (*PrincipalTagMapping, bool) {
	return b.principalTags.Get(regionKey(region, principalTagKey(poolID, providerName)))
}

func (b *InMemoryBackend) principalTagPut(v *PrincipalTagMapping) { b.principalTags.Put(v) }

func (b *InMemoryBackend) principalTagDelete(region, poolID, providerName string) bool {
	return b.principalTags.Delete(regionKey(region, principalTagKey(poolID, providerName)))
}

func (b *InMemoryBackend) principalTagsInPool(region, poolID string) []*PrincipalTagMapping {
	return b.principalTagsByPool.Get(regionKey(region, poolID))
}

// Region returns the region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all identity pool, identity and role state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
}

// AddPoolInternal seeds an identity pool directly into the backend for testing purposes.
// It bypasses the normal CreateIdentityPool validation. The region is derived from the pool's ARN.
func (b *InMemoryBackend) AddPoolInternal(pool *IdentityPool) {
	b.mu.Lock("AddPoolInternal")
	defer b.mu.Unlock()

	region := regionFromARN(pool.ARN, b.region)
	cp := clonePool(pool)
	cp.region = region
	b.poolPut(cp)
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
	cp.region = region
	b.identityPut(cp)
}
