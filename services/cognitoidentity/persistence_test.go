// Package cognitoidentity: this file needs access to the unexported region context key
// (for multi-region persistence coverage) and to backendSnapshot (for the
// version-mismatch test), so it lives in the internal package like isolation_test.go.
package cognitoidentity //nolint:testpackage // see comment above.

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testRegionEast and testRegionWest are the two AWS regions exercised by the
// multi-region persistence round-trip test below.
const (
	testRegionEast = "us-east-1"
	testRegionWest = "us-west-2"
)

// regionFixture holds every resource created for one region by newPersistenceFixture,
// so assertPersistenceFixture can verify each collection survived a Snapshot/Restore
// round-trip independently per region.
type regionFixture struct {
	pool          *IdentityPool
	identity      *Identity
	providerName  string
	preRenameName string
}

// newPersistenceFixture populates b with one identity pool, one identity, one IAM role
// mapping, and one principal-tag mapping in region, then renames the pool -- exercising
// the byName index rebuild UpdateIdentityPool performs (see renamePoolIfNeeded in
// identity_pools.go). Returns the fixture describing what was created, for later assertion.
func newPersistenceFixture(t *testing.T, b *InMemoryBackend, region string) regionFixture {
	t.Helper()

	ctx := ctxRegion(region)

	preRenameName := "fixture-pool-" + region

	pool, err := b.CreateIdentityPool(ctx, preRenameName, true, false, "", nil, nil,
		map[string]string{"region": region})
	require.NoError(t, err)

	renamed, err := b.UpdateIdentityPool(ctx, pool.IdentityPoolID, preRenameName+"-renamed",
		true, false, "", nil, nil, nil)
	require.NoError(t, err)

	identity, err := b.GetID(ctx, renamed.IdentityPoolID, "000000000000",
		map[string]string{"accounts.google.com": "token-" + region})
	require.NoError(t, err)

	require.NoError(t, b.SetIdentityPoolRoles(ctx, renamed.IdentityPoolID,
		"arn:aws:iam::000000000000:role/auth-"+region,
		"arn:aws:iam::000000000000:role/unauth-"+region,
		nil))

	providerName := "cognito-idp." + region + ".amazonaws.com/pool"

	_, err = b.SetPrincipalTagAttributeMap(ctx, renamed.IdentityPoolID, providerName, false,
		map[string]string{"sub": "user_id"})
	require.NoError(t, err)

	return regionFixture{pool: renamed, identity: identity, providerName: providerName, preRenameName: preRenameName}
}

// assertPersistenceFixture verifies that every resource newPersistenceFixture created in
// region is present, correct, and still region-isolated on b.
func assertPersistenceFixture(t *testing.T, b *InMemoryBackend, region string, want regionFixture) {
	t.Helper()

	ctx := ctxRegion(region)

	pool, err := b.DescribeIdentityPool(ctx, want.pool.IdentityPoolID)
	require.NoError(t, err)
	assert.Equal(t, want.pool.IdentityPoolName, pool.IdentityPoolName)
	assert.Equal(t, region, pool.Tags["region"])

	// The renamed (current) name must still be taken -- proves the byName index
	// reflects the post-rename state, not a stale pre-rename entry.
	_, err = b.CreateIdentityPool(ctx, want.pool.IdentityPoolName, true, false, "", nil, nil, nil)
	require.ErrorIs(t, err, ErrIdentityPoolAlreadyExists)

	// The pre-rename name must be free again -- proves the byName index no longer
	// carries the stale pre-rename entry after a rename survives a restore.
	_, err = b.CreateIdentityPool(ctx, want.preRenameName, true, false, "", nil, nil, nil)
	require.NoError(t, err, "pre-rename name should be free to reuse after restore")

	identity, err := b.DescribeIdentity(ctx, want.identity.IdentityID)
	require.NoError(t, err)
	assert.Equal(t, want.identity.IdentityID, identity.IdentityID)

	roles, err := b.GetIdentityPoolRoles(ctx, want.pool.IdentityPoolID)
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:iam::000000000000:role/auth-"+region, roles.AuthenticatedRoleARN)
	assert.Equal(t, "arn:aws:iam::000000000000:role/unauth-"+region, roles.UnauthenticatedRoleARN)

	mapping, err := b.GetPrincipalTagAttributeMap(ctx, want.pool.IdentityPoolID, want.providerName)
	require.NoError(t, err)
	assert.Equal(t, "user_id", mapping.PrincipalTags["sub"])
}

// Test_PersistenceRoundTrip proves that Snapshot/Restore round-trips every converted
// resource collection (pools, identities, roles, principalTags) across multiple regions,
// including a pool rename (byName index rebuild) and a pool deletion (its identities,
// roles and principal-tag mappings must not resurrect on restore).
func Test_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		regions []string
	}{
		{name: "single_region", regions: []string{testRegionEast}},
		{name: "two_regions", regions: []string{testRegionEast, testRegionWest}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend("000000000000", tt.regions[0])

			fixtures := make(map[string]regionFixture, len(tt.regions))
			for _, region := range tt.regions {
				fixtures[region] = newPersistenceFixture(t, b, region)
			}

			deletedPoolID := createAndDeleteFixture(t, b, tt.regions[0])

			snap := b.Snapshot(context.Background())
			require.NotEmpty(t, snap)

			restored := NewInMemoryBackend("000000000000", tt.regions[0])
			require.NoError(t, restored.Restore(context.Background(), snap))

			for region, want := range fixtures {
				assertPersistenceFixture(t, restored, region, want)
			}

			_, err := restored.DescribeIdentityPool(ctxRegion(tt.regions[0]), deletedPoolID)
			require.ErrorIs(t, err, ErrIdentityPoolNotFound)
		})
	}
}

// createAndDeleteFixture creates a pool with an identity, role mapping and principal-tag
// mapping in region, then deletes the pool. It returns the deleted pool's ID so the
// caller can assert none of its resources resurrect after a Snapshot/Restore round-trip
// -- proving DeleteIdentityPool's cleanup (identities, roles, principal tags) is durable
// across persistence, not just an in-memory side effect.
func createAndDeleteFixture(t *testing.T, b *InMemoryBackend, region string) string {
	t.Helper()

	ctx := ctxRegion(region)

	pool, err := b.CreateIdentityPool(ctx, "doomed-pool-"+region, true, false, "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.GetID(ctx, pool.IdentityPoolID, "000000000000",
		map[string]string{"accounts.google.com": "doomed-token"})
	require.NoError(t, err)

	require.NoError(t, b.SetIdentityPoolRoles(ctx, pool.IdentityPoolID,
		"arn:aws:iam::000000000000:role/doomed-auth", "", nil))

	_, err = b.SetPrincipalTagAttributeMap(ctx, pool.IdentityPoolID, "doomed-provider", false, nil)
	require.NoError(t, err)

	require.NoError(t, b.DeleteIdentityPool(ctx, pool.IdentityPoolID))

	return pool.IdentityPoolID
}

// Test_PersistenceVersionMismatch proves that Restore discards a snapshot whose Version
// does not match cognitoidentitySnapshotVersion -- rather than partially decoding it --
// and leaves the backend empty instead of returning an error, matching the recoverable
// (e.g. cross-build-upgrade) condition this guards against.
func Test_PersistenceVersionMismatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		newVersion int
	}{
		{name: "future_version", newVersion: cognitoidentitySnapshotVersion + 1},
		{name: "zero_version", newVersion: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend("000000000000", testRegionEast)
			ctx := ctxRegion(testRegionEast)

			_, err := b.CreateIdentityPool(ctx, "version-mismatch-pool", true, false, "", nil, nil, nil)
			require.NoError(t, err)

			snap := b.Snapshot(context.Background())
			require.NotEmpty(t, snap)

			tampered := tamperSnapshotVersion(t, snap, tt.newVersion)

			restored := NewInMemoryBackend("000000000000", testRegionEast)
			require.NoError(t, restored.Restore(context.Background(), tampered))

			assert.Equal(t, 0, restored.PoolCount())
			assert.Equal(t, 0, restored.IdentityCount())
			assert.Equal(t, 0, restored.PrincipalTagCount())
		})
	}
}

// tamperSnapshotVersion decodes data as a backendSnapshot, overwrites its Version field,
// and re-encodes it, for feeding an incompatible-version snapshot into Restore.
func tamperSnapshotVersion(t *testing.T, data []byte, version int) []byte {
	t.Helper()

	var snap backendSnapshot
	require.NoError(t, json.Unmarshal(data, &snap))

	snap.Version = version

	out, err := json.Marshal(snap)
	require.NoError(t, err)

	return out
}

// Test_PersistenceMalformedSnapshot proves that a Restore call fed syntactically invalid
// JSON returns an error via persistence.UnmarshalSnapshot rather than silently resetting
// or panicking.
func Test_PersistenceMalformedSnapshot(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("000000000000", testRegionEast)

	err := b.Restore(context.Background(), []byte("{not valid json"))
	require.Error(t, err)
}
