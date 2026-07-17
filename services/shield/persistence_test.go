package shield_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/shield"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *shield.InMemoryBackend) string
		verify func(t *testing.T, b *shield.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "protection_round_trip",
			setup: func(b *shield.InMemoryBackend) string {
				require.NoError(t, b.CreateSubscription())

				const webARN = "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/web"

				p, err := b.CreateProtection("web-app", webARN, nil)
				require.NoError(t, err)

				return p.ID
			},
			verify: func(t *testing.T, b *shield.InMemoryBackend, id string) {
				t.Helper()

				const webARN = "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/web"

				p, err := b.DescribeProtection(id, "")
				require.NoError(t, err)
				assert.Equal(t, "web-app", p.Name)
				assert.Equal(t, id, p.ID)

				// Indexes must be rebuilt.
				p2, err := b.DescribeProtection("", webARN)
				require.NoError(t, err)
				assert.Equal(t, id, p2.ID)
			},
		},
		{
			name: "subscription_round_trip",
			setup: func(b *shield.InMemoryBackend) string {
				require.NoError(t, b.CreateSubscription())

				return ""
			},
			verify: func(t *testing.T, b *shield.InMemoryBackend, _ string) {
				t.Helper()

				sub, err := b.DescribeSubscription()
				require.NoError(t, err)
				assert.Equal(t, "ENABLED", sub.AutoRenew)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *shield.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *shield.InMemoryBackend, _ string) {
				t.Helper()

				assert.Empty(t, b.ListProtections())
				assert.Equal(t, "INACTIVE", b.GetSubscriptionState())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := shield.NewInMemoryBackend(testAccountID, testRegion)
			id := tt.setup(original)

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := shield.NewInMemoryBackend(testAccountID, testRegion)
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend(testAccountID, testRegion)
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

func TestShieldHandler_Persistence(t *testing.T) {
	t.Parallel()

	backend := shield.NewInMemoryBackend(testAccountID, testRegion)
	h := shield.NewHandler(backend)

	require.NoError(t, backend.CreateSubscription())

	const testARN = "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/t"

	_, err := backend.CreateProtection("test", testARN, nil)
	require.NoError(t, err)

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := shield.NewInMemoryBackend(testAccountID, testRegion)
	freshH := shield.NewHandler(fresh)
	require.NoError(t, freshH.Restore(t.Context(), snap))

	assert.Len(t, fresh.ListProtections(), 1)
}

// TestBackend_SnapshotRestoreNewFields verifies new fields are persisted across snapshot/restore.
func TestBackend_SnapshotRestoreNewFields(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")

	require.NoError(t, b.CreateSubscription())
	require.NoError(t, b.AssociateDRTRole("arn:aws:iam::123:role/DRTRole"))
	require.NoError(t, b.AssociateDRTLogBucket("my-bucket"))

	_, err := b.CreateProtectionGroup("grp-1", "MAX", "ALL", "", nil)
	require.NoError(t, err)

	b.AddAttackInternal("atk-1", "arn:aws:ec2:us-east-1:123:eip/eipalloc-1")

	require.NoError(t, b.AssociateProactiveEngagementDetails([]shield.EmergencyContact{
		{EmailAddress: "sec@example.com"},
	}))

	snap := b.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	b2 := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	access := b2.DescribeDRTAccess()
	assert.Equal(t, "arn:aws:iam::123:role/DRTRole", access.RoleArn)
	assert.Len(t, access.LogBucketList, 1)

	assert.Equal(t, 1, shield.ProtectionGroupCount(b2))
	assert.Equal(t, 1, shield.AttackCount(b2))
}

// TestRefinement1_SnapshotRestore verifies snapshot and restore.
func TestInMemoryBackend_SnapshotRestoreBasic(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddProtectionInternal("prot-1", "arn:aws:ec2:us-east-1::eip-allocation/eipalloc-111")
	require.NoError(t, b.CreateSubscription())

	snap := b.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	b2 := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	assert.Equal(t, 1, shield.ProtectionCount(b2))

	sub, err := b2.DescribeSubscription()
	require.NoError(t, err)
	assert.NotNil(t, sub)
}

// TestRefinement1_SnapshotDeepCopy verifies snapshot is isolated from live mutations.
func TestInMemoryBackend_SnapshotDeepCopy(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	p := b.AddProtectionInternal("prot-1", "arn:aws:ec2:us-east-1::eip-allocation/eipalloc-1")

	snap := b.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	// Mutate backend after snapshot.
	require.NoError(t, b.DeleteProtection(p.ID))
	assert.Equal(t, 0, shield.ProtectionCount(b))

	// Restore should see the pre-mutation state.
	b2 := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))
	assert.Equal(t, 1, shield.ProtectionCount(b2))
}

// TestRefinement1_ProactiveEngagementStatusPersistedInSnapshot tests persistence.
func TestInMemoryBackend_ProactiveEngagementStatusPersistedInSnapshot(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.CreateSubscription())
	require.NoError(t, b.AssociateProactiveEngagementDetails([]shield.EmergencyContact{
		{EmailAddress: "sec@example.com"},
	}))
	require.NoError(t, b.EnableProactiveEngagement())

	snap := b.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	b2 := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))
	assert.Equal(t, shield.ProactiveEngagementEnabled, shield.GetProactiveEngagementStatus(b2))
}

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
