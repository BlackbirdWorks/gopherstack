package shield_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/shield"
)

// newFullPersistenceTestBackend creates a backend with one populated entry in
// every store.Table (protections plus its two secondary indexes,
// protectionGroups, attacks, and the "dirty" alarConfigs table) plus every
// raw field left un-converted (subscription, drtAccess, emergencyContacts,
// proactiveEngagementStatus), so a Snapshot from it exercises the entire
// persisted surface of the backend.
func newFullPersistenceTestBackend(t *testing.T) (*shield.InMemoryBackend, *shield.Protection) {
	t.Helper()

	b := shield.NewInMemoryBackend(testAccountID, testRegion)

	require.NoError(t, b.CreateSubscription())

	const resourceARN = "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/full"

	p, err := b.CreateProtection("full-protection", resourceARN, map[string]string{"env": "test"})
	require.NoError(t, err)

	require.NoError(t, b.AssociateHealthCheck(p.ID, "arn:aws:route53:::healthcheck/hc1"))

	require.NoError(t, b.EnableApplicationLayerAutomaticResponse(resourceARN, "BLOCK"))

	_, err = b.CreateProtectionGroup("grp1", shield.AggregationSum, shield.PatternArbitrary, "", []string{resourceARN})
	require.NoError(t, err)

	b.AddAttackInternal("attack1", resourceARN)

	require.NoError(t, b.AssociateDRTLogBucket("shield-drt-bucket"))
	require.NoError(t, b.AssociateDRTRole("arn:aws:iam::000000000000:role/drt-role"))

	require.NoError(t, b.UpdateEmergencyContactSettings([]shield.EmergencyContact{
		{EmailAddress: "ops@example.com", ContactNotes: "primary"},
	}))
	require.NoError(t, b.AssociateProactiveEngagementDetails([]shield.EmergencyContact{
		{EmailAddress: "ops@example.com"},
	}))

	return b, p
}

// TestInMemoryBackend_SnapshotRestore_FullState round-trips every store.Table
// (protections + its byResourceARN/byName indexes, protectionGroups,
// attacks, alarConfigs) and every raw field left un-converted (subscription,
// drtAccess, emergencyContacts, proactiveEngagementStatus) through
// Snapshot -> Restore into a fresh backend.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original, p := newFullPersistenceTestBackend(t)

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := shield.NewInMemoryBackend(testAccountID, testRegion)
	require.NoError(t, fresh.Restore(t.Context(), snap))

	// protections table, keyed by ID.
	restored, err := fresh.DescribeProtection(p.ID, "")
	require.NoError(t, err)
	assert.Equal(t, "full-protection", restored.Name)
	assert.Equal(t, "test", restored.Tags["env"])
	assert.Equal(t, []string{"arn:aws:route53:::healthcheck/hc1"}, restored.HealthCheckIDs)

	// protectionsByResourceARN index.
	byARN, err := fresh.DescribeProtection("", p.ResourceARN)
	require.NoError(t, err)
	assert.Equal(t, p.ID, byARN.ID)

	// protectionsByName index, exercised indirectly via the duplicate-name
	// rejection CreateProtection performs against it.
	const otherARN = "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/other"

	_, err = fresh.CreateProtection("full-protection", otherARN, nil)
	require.ErrorIs(t, err, shield.ErrProtectionAlreadyExists)

	// alarConfigs "dirty" table (hidden ResourceARN id round-tripped via DTO).
	alarCfg := fresh.GetALARConfig(p.ResourceARN)
	require.NotNil(t, alarCfg)
	assert.Equal(t, "BLOCK", alarCfg.Action)
	assert.True(t, alarCfg.Enabled)

	// protectionGroups table.
	group, err := fresh.DescribeProtectionGroup("grp1")
	require.NoError(t, err)
	assert.Equal(t, shield.PatternArbitrary, group.Pattern)
	assert.Equal(t, []string{p.ResourceARN}, group.Members)

	// attacks table.
	attack, err := fresh.DescribeAttack("attack1")
	require.NoError(t, err)
	assert.Equal(t, p.ResourceARN, attack.ResourceARN)

	// subscription (raw field).
	sub, err := fresh.DescribeSubscription()
	require.NoError(t, err)
	assert.Equal(t, "ENABLED", sub.AutoRenew)

	// drtAccess (raw field).
	drt := fresh.DescribeDRTAccess()
	assert.Equal(t, "arn:aws:iam::000000000000:role/drt-role", drt.RoleArn)
	assert.Equal(t, []string{"shield-drt-bucket"}, drt.LogBucketList)

	// emergencyContacts + proactiveEngagementStatus (raw fields).
	contacts := fresh.DescribeEmergencyContactSettings()
	require.Len(t, contacts, 1)
	assert.Equal(t, "ops@example.com", contacts[0].EmailAddress)
	assert.Equal(t, shield.ProactiveEngagementPending, fresh.GetProactiveEngagementStatus())

	// Sanity: account/region carried through too.
	assert.Equal(t, testRegion, fresh.Region())
	assert.Equal(t, testAccountID, fresh.AccountID())
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that a snapshot whose
// version doesn't match the current backend (including the pre-Phase-3.3
// format, which decodes with Version == 0) is discarded cleanly rather than
// partially decoded: the backend resets to empty state and Restore returns
// no error.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	b, p := newFullPersistenceTestBackend(t)

	// A syntactically valid but version-less/mismatched snapshot.
	err := b.Restore(t.Context(), []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	assert.Empty(t, b.ListProtections())
	assert.Equal(t, "INACTIVE", b.GetSubscriptionState())
	assert.Nil(t, b.GetALARConfig(p.ResourceARN))
	assert.Empty(t, b.ListProtectionGroups())
	assert.Empty(t, b.DescribeEmergencyContactSettings())
	assert.Empty(t, b.GetProactiveEngagementStatus())

	drt := b.DescribeDRTAccess()
	assert.Empty(t, drt.LogBucketList)
}

// TestHandler_SnapshotRestoreDelegate verifies Handler.Snapshot/Restore
// delegate to the backend (the wiring cli.go's generic setupPersistence
// relies on).
func TestHandler_SnapshotRestoreDelegate(t *testing.T) {
	t.Parallel()

	backend, p := newFullPersistenceTestBackend(t)
	h := shield.NewHandler(backend)

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	h2 := shield.NewHandler(shield.NewInMemoryBackend(testAccountID, testRegion))
	require.NoError(t, h2.Restore(t.Context(), snap))

	restored, err := h2.Backend.DescribeProtection(p.ID, "")
	require.NoError(t, err)
	assert.Equal(t, "full-protection", restored.Name)
}
