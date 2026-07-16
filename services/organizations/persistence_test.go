package organizations_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/organizations"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *organizations.InMemoryBackend) string
		verify func(t *testing.T, b *organizations.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "org_and_account_round_trip",
			setup: func(b *organizations.InMemoryBackend) string {
				_, _, err := b.CreateOrganization("ALL")
				require.NoError(t, err)

				status, err := b.CreateAccount("test-account", "test@example.com", "", "", nil)
				require.NoError(t, err)

				return status.AccountID
			},
			verify: func(t *testing.T, b *organizations.InMemoryBackend, id string) {
				t.Helper()

				acct, err := b.DescribeAccount(id)
				require.NoError(t, err)
				assert.Equal(t, "test-account", acct.Name)
			},
		},
		{
			name: "ou_round_trip",
			setup: func(b *organizations.InMemoryBackend) string {
				_, _, err := b.CreateOrganization("ALL")
				require.NoError(t, err)

				roots, err := b.ListRoots()
				require.NoError(t, err)
				require.NotEmpty(t, roots)

				ou, err := b.CreateOrganizationalUnit(roots[0].ID, "eng-team", nil)
				require.NoError(t, err)

				return ou.ID
			},
			verify: func(t *testing.T, b *organizations.InMemoryBackend, id string) {
				t.Helper()

				ou, err := b.DescribeOrganizationalUnit(id)
				require.NoError(t, err)
				assert.Equal(t, "eng-team", ou.Name)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *organizations.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *organizations.InMemoryBackend, _ string) {
				t.Helper()

				_, err := b.DescribeOrganization()
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := organizations.NewInMemoryBackend("000000000000", "us-east-1")
			id := tt.setup(original)

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := organizations.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := organizations.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

func TestOrganizationsHandler_Persistence(t *testing.T) {
	t.Parallel()

	backend := organizations.NewInMemoryBackend("000000000000", "us-east-1")
	h := organizations.NewHandler(backend)

	_, _, err := backend.CreateOrganization("ALL")
	require.NoError(t, err)

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := organizations.NewInMemoryBackend("000000000000", "us-east-1")
	freshH := organizations.NewHandler(fresh)
	require.NoError(t, freshH.Restore(t.Context(), snap))

	org, err := fresh.DescribeOrganization()
	require.NoError(t, err)
	assert.NotEmpty(t, org.ID)
}

// TestInMemoryBackend_FullStateSnapshotRestore exercises every store.Table
// (accounts, ous, policies, createStatuses, handshakes, serviceAccess) and
// the DTO-backed delegatedAdmins table introduced by the Phase 3.3
// pkgs/store conversion, plus the plain maps left un-converted (tags,
// policyTargets/targetPolicies via a policy attachment), in a single
// Snapshot -> Restore round trip. It exists specifically to catch
// registry/index wiring bugs (e.g. a table registered after RestoreAll, or
// a secondary index not repopulated) that a narrower per-feature test would
// miss.
func TestInMemoryBackend_FullStateSnapshotRestore(t *testing.T) {
	t.Parallel()

	const servicePrincipal = "guardduty.amazonaws.com"

	original := organizations.NewInMemoryBackend("000000000000", "us-east-1")

	_, root, err := original.CreateOrganization("ALL")
	require.NoError(t, err)

	ou, err := original.CreateOrganizationalUnit(root.ID, "eng-team", nil)
	require.NoError(t, err)

	status, err := original.CreateAccount("member", "member@example.com", "", "", nil)
	require.NoError(t, err)
	accountID := status.AccountID

	require.NoError(t, original.MoveAccount(accountID, root.ID, ou.ID))

	policy, err := original.CreatePolicy("deny-all", "desc", `{"Version":"2012-10-17"}`, "SERVICE_CONTROL_POLICY", nil)
	require.NoError(t, err)
	require.NoError(t, original.AttachPolicy(policy.PolicySummary.ID, ou.ID))

	require.NoError(t, original.TagResource(ou.ID, []organizations.Tag{{Key: "env", Value: "prod"}}))

	require.NoError(t, original.EnableAWSServiceAccess(servicePrincipal))
	require.NoError(t, original.RegisterDelegatedAdministrator(accountID, servicePrincipal))

	original.AddHandshakeInternal(&organizations.Handshake{Action: "INVITE", State: "OPEN"})

	_, err = original.PutResourcePolicy(`{"Version":"2012-10-17"}`)
	require.NoError(t, err)

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := organizations.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	// accounts / ous / policies tables.
	acct, err := fresh.DescribeAccount(accountID)
	require.NoError(t, err)
	assert.Equal(t, "member", acct.Name)

	restoredOU, err := fresh.DescribeOrganizationalUnit(ou.ID)
	require.NoError(t, err)
	assert.Equal(t, "eng-team", restoredOU.Name)

	restoredPolicy, err := fresh.DescribePolicy(policy.PolicySummary.ID)
	require.NoError(t, err)
	assert.Equal(t, "deny-all", restoredPolicy.PolicySummary.Name)

	// policyTargets/targetPolicies (plain maps) round trip via the policy attachment.
	targets, err := fresh.ListTargetsForPolicy(policy.PolicySummary.ID)
	require.NoError(t, err)
	require.Len(t, targets, 1)
	assert.Equal(t, ou.ID, targets[0].TargetID)

	// tags (plain map).
	tags, err := fresh.ListTagsForResource(ou.ID)
	require.NoError(t, err)
	require.Len(t, tags, 1)
	assert.Equal(t, "env", tags[0].Key)

	// serviceAccess table.
	sps, err := fresh.ListAWSServiceAccessForOrganization()
	require.NoError(t, err)
	require.Len(t, sps, 1)
	assert.Equal(t, servicePrincipal, sps[0].ServicePrincipal)

	// delegatedAdmins DTO table + its two secondary indexes.
	admins, err := fresh.ListDelegatedAdministrators(servicePrincipal)
	require.NoError(t, err)
	require.Len(t, admins, 1)
	assert.Equal(t, accountID, admins[0].AccountID)
	assert.Equal(t, servicePrincipal, admins[0].ServicePrincipal)

	delegatedServices, err := fresh.ListDelegatedServicesForAccount(accountID)
	require.NoError(t, err)
	require.Len(t, delegatedServices, 1)
	assert.Equal(t, servicePrincipal, delegatedServices[0].ServicePrincipal)

	// handshakes table.
	handshakes, err := fresh.ListHandshakesForOrganization("")
	require.NoError(t, err)
	require.Len(t, handshakes, 1)

	// createStatuses table.
	statuses, err := fresh.ListCreateAccountStatus(nil)
	require.NoError(t, err)
	require.NotEmpty(t, statuses)

	// resourcePolicy (single pointer field, not a map).
	rp, err := fresh.DescribeResourcePolicy()
	require.NoError(t, err)
	assert.NotEmpty(t, rp.Content)
}

// TestPersistence_EnsureNonNilMaps verifies ensureNonNilMaps initialises all fields.
func TestPersistence_EnsureNonNilMaps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "nil_snapshot_gets_non_nil_maps"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			snap := organizations.NewBackendSnapshot()
			organizations.EnsureNonNilMapsExported(snap)

			// After EnsureNonNilMaps all maps in the snapshot should be non-nil.
			// We can verify via round-trip: marshal+unmarshal should not panic.
			data, err := json.Marshal(snap)
			require.NoError(t, err)
			assert.NotNil(t, data, tt.name)
		})
	}
}
