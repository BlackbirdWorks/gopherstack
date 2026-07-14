package ram_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ram"
)

func TestRAM_PersistenceSnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(t *testing.T, b *ram.InMemoryBackend)
		verify func(t *testing.T, b *ram.InMemoryBackend)
		name   string
	}{
		{
			name:  "empty_backend",
			setup: func(_ *testing.T, _ *ram.InMemoryBackend) {},
			verify: func(t *testing.T, b *ram.InMemoryBackend) {
				t.Helper()

				shares := b.ListResourceShares("SELF", "")
				assert.Empty(t, shares)

				// Built-in permissions are always seeded, even into an
				// otherwise-empty restored backend.
				assert.Equal(t, ram.BuiltInPermissionCount, ram.PermissionCount(b))
			},
		},
		{
			name: "resource_share_preserved",
			setup: func(t *testing.T, b *ram.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateResourceShare(
					"my-share",
					true,
					map[string]string{"env": "test"},
					[]string{"arn:aws:iam::999:root"},
					[]string{"arn:aws:ec2:us-east-1:123:subnet/abc"},
				)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *ram.InMemoryBackend) {
				t.Helper()

				shares := b.ListResourceShares("SELF", "")
				require.Len(t, shares, 1)
				assert.Equal(t, "my-share", shares[0].Name)
				assert.True(t, shares[0].AllowExternalPrincipals)
				assert.Equal(t, "test", shares[0].Tags["env"])

				// ARN-based lookup must work.
				got, err := b.GetResourceShare(shares[0].ARN)
				require.NoError(t, err)
				assert.Equal(t, "my-share", got.Name)
			},
		},
		{
			name: "associations_preserved",
			setup: func(t *testing.T, b *ram.InMemoryBackend) {
				t.Helper()

				rs, err := b.CreateResourceShare("shared", true, nil, nil, nil)
				require.NoError(t, err)

				_, err = b.AssociateResourceShare(
					rs.ARN,
					[]string{"arn:aws:iam::777:root"},
					nil,
				)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *ram.InMemoryBackend) {
				t.Helper()

				shares := b.ListResourceShares("SELF", "")
				require.Len(t, shares, 1)

				assocs := b.GetResourceShareAssociations("PRINCIPAL", []string{shares[0].ARN})
				require.Len(t, assocs, 1)
				assert.Equal(t, "arn:aws:iam::777:root", assocs[0].AssociatedEntity)
			},
		},
		{
			name: "customer_permission_preserved",
			setup: func(t *testing.T, b *ram.InMemoryBackend) {
				t.Helper()

				_, err := b.CreatePermission(
					"my-perm", "custom:Widget", `{"Effect":"Allow"}`, map[string]string{"k": "v"},
				)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *ram.InMemoryBackend) {
				t.Helper()

				perms := b.ListPermissions("custom:Widget")
				require.Len(t, perms, 1)
				assert.Equal(t, "my-perm", perms[0].Name)
				assert.Equal(t, "v", perms[0].Tags["k"])
			},
		},
		{
			name: "permission_version_and_share_permission_preserved",
			setup: func(t *testing.T, b *ram.InMemoryBackend) {
				t.Helper()

				rs, err := b.CreateResourceShare("shared", false, nil, nil, nil)
				require.NoError(t, err)

				perm, err := b.CreatePermission("my-perm", "ec2:Subnet", `{"v":1}`, nil)
				require.NoError(t, err)

				_, err = b.CreatePermissionVersion(perm.ARN, `{"v":2}`)
				require.NoError(t, err)

				require.NoError(t, b.AssociateResourceSharePermission(rs.ARN, perm.ARN, false, nil))
			},
			verify: func(t *testing.T, b *ram.InMemoryBackend) {
				t.Helper()

				shares := b.ListResourceShares("SELF", "")
				require.Len(t, shares, 1)

				versions, err := b.ListPermissionVersions(
					b.ListResourceSharePermissions(shares[0].ARN)[0].Permission.ARN,
				)
				require.NoError(t, err)
				assert.Len(t, versions, 2)

				sharePerms := b.ListResourceSharePermissions(shares[0].ARN)
				require.Len(t, sharePerms, 1)
			},
		},
		{
			name: "invitation_preserved",
			setup: func(t *testing.T, b *ram.InMemoryBackend) {
				t.Helper()

				rs, err := b.CreateResourceShare(
					"shared", true, nil, []string{"555566667777"}, nil,
				)
				require.NoError(t, err)

				invs := b.GetResourceShareInvitations(nil, []string{rs.ARN})
				require.Len(t, invs, 1)
			},
			verify: func(t *testing.T, b *ram.InMemoryBackend) {
				t.Helper()

				shares := b.ListResourceShares("SELF", "")
				require.Len(t, shares, 1)

				invs := b.GetResourceShareInvitations(nil, []string{shares[0].ARN})
				require.Len(t, invs, 1)
				assert.Equal(t, "555566667777", invs[0].ReceiverAccountID)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ram.NewInMemoryBackend("123456789012", "us-east-1")
			tt.setup(t, b)

			snap := b.Snapshot(t.Context())
			require.NotNil(t, snap)

			b2 := ram.NewInMemoryBackend("123456789012", "us-east-1")
			err := b2.Restore(t.Context(), snap)
			require.NoError(t, err)

			tt.verify(t, b2)
		})
	}
}

// TestRAM_PersistenceFullStateRoundTrip exercises a Snapshot->Restore round
// trip across every resource family the Phase 3.3 pkgs/store conversion
// touched (resourceShares, permissions, invitations), plus every raw field
// left un-converted (sharePermissions, associations, accountID, region), all
// populated at once so cross-table consistency (e.g. a permission version
// referenced by a sharePermissions entry) survives the round trip together.
func TestRAM_PersistenceFullStateRoundTrip(t *testing.T) {
	t.Parallel()

	original := ram.NewInMemoryBackend("111122223333", "eu-west-1")

	rs, err := original.CreateResourceShare(
		"full-share",
		true,
		map[string]string{"team": "platform"},
		[]string{"arn:aws:iam::444455556666:root"},
		[]string{"arn:aws:ec2:eu-west-1:111122223333:subnet/subnet-1"},
	)
	require.NoError(t, err)

	perm, err := original.CreatePermission(
		"full-perm", "ec2:Subnet", `{"Effect":"Allow","Action":"*"}`, map[string]string{"env": "prod"},
	)
	require.NoError(t, err)

	_, err = original.CreatePermissionVersion(perm.ARN, `{"Effect":"Allow","Action":"ec2:Describe*"}`)
	require.NoError(t, err)

	require.NoError(t, original.AssociateResourceSharePermission(rs.ARN, perm.ARN, false, nil))

	invs := original.GetResourceShareInvitations(nil, []string{rs.ARN})
	require.Len(t, invs, 1)

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := ram.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	// accountID/region restored from the snapshot, not the fresh backend's
	// construction-time values.
	assert.Equal(t, "111122223333", fresh.AccountID())
	assert.Equal(t, "eu-west-1", fresh.Region())

	shares := fresh.ListResourceShares("SELF", "")
	require.Len(t, shares, 1)
	assert.Equal(t, "full-share", shares[0].Name)
	assert.Equal(t, "platform", shares[0].Tags["team"])

	assocs := fresh.GetResourceShareAssociations("", []string{shares[0].ARN})
	require.Len(t, assocs, 2)

	perms := fresh.ListPermissions("")
	// Built-ins plus the one customer-managed permission.
	require.Len(t, perms, ram.BuiltInPermissionCount+1)

	sharePerms := fresh.ListResourceSharePermissions(shares[0].ARN)
	require.Len(t, sharePerms, 1)
	assert.Equal(t, perm.ARN, sharePerms[0].Permission.ARN)

	versions, err := fresh.ListPermissionVersions(perm.ARN)
	require.NoError(t, err)
	assert.Len(t, versions, 2)

	restoredInvs := fresh.GetResourceShareInvitations(nil, []string{shares[0].ARN})
	require.Len(t, restoredInvs, 1)
	assert.Equal(t, invs[0].InvitationARN, restoredInvs[0].InvitationARN)

	assert.Equal(t, ram.AssociationCount(original), ram.AssociationCount(fresh))
	assert.Equal(t, ram.SharePermissionCount(original), ram.SharePermissionCount(fresh))
}

// TestRAM_RestoreVersionMismatch verifies that a snapshot whose version
// doesn't match the current backend (including the pre-Phase-3.3 format,
// which decodes with Version == 0 since it never had a version field) is
// discarded cleanly rather than partially decoded: the backend resets to its
// freshly-constructed state (built-in permissions re-seeded, everything else
// empty) and Restore returns no error.
func TestRAM_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	b := ram.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.CreateResourceShare("seed-share", true, nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreatePermission("seed-perm", "ec2:Subnet", `{}`, nil)
	require.NoError(t, err)

	// A syntactically valid but version-less/mismatched snapshot.
	err = b.Restore(t.Context(), []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	assert.Empty(t, b.ListResourceShares("SELF", ""))
	assert.Equal(t, ram.BuiltInPermissionCount, ram.PermissionCount(b))
	assert.Equal(t, 0, ram.AssociationCount(b))
	assert.Equal(t, 0, ram.SharePermissionCount(b))
}

// TestRAM_RestoreMalformedJSON verifies that malformed JSON surfaces as an
// error from Restore rather than silently discarding state.
func TestRAM_RestoreMalformedJSON(t *testing.T) {
	t.Parallel()

	b := ram.NewInMemoryBackend("123456789012", "us-east-1")

	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestRAM_HandlerSnapshotRestoreDelegate verifies Handler.Snapshot/Restore
// delegate to the backend correctly.
func TestRAM_HandlerSnapshotRestoreDelegate(t *testing.T) {
	t.Parallel()

	h := ram.NewHandler(ram.NewInMemoryBackend("123456789012", "us-east-1"))

	_, err := h.Backend.CreateResourceShare("delegate-share", true, nil, nil, nil)
	require.NoError(t, err)

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	h2 := ram.NewHandler(ram.NewInMemoryBackend("123456789012", "us-east-1"))
	require.NoError(t, h2.Restore(t.Context(), snap))

	shares := h2.Backend.ListResourceShares("SELF", "")
	require.Len(t, shares, 1)
	assert.Equal(t, "delegate-share", shares[0].Name)
}
