package ec2_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestIpamPrefixListResolver_RestoreRecomputesCurrentVersion covers gopherstack-a73ak:
// CurrentVersion is json:"-", so restore used to leave it at 0, restarting Modify's
// numbering from 1 (colliding with the already-persisted version history) and stamping
// new targets LastSyncedVersion=0 (falsely never-synced).
func TestIpamPrefixListResolver_RestoreRecomputesCurrentVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{name: "modify_after_restore_continues_numbering", run: testModifyAfterRestoreContinuesNumbering},
		{name: "target_after_restore_sees_synced_version", run: testTargetAfterRestoreSeesSyncedVersion},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

// restoredResolverBackend creates a resolver (CurrentVersion=1 at creation, per
// ipam_prefix_list_resolvers.go:49) and modifies it once (CurrentVersion=2, per :137),
// snapshots it, and restores that snapshot into a fresh backend.
func restoredResolverBackend(t *testing.T) (*ec2.InMemoryBackend, string) {
	t.Helper()

	original := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	ipam, err := original.CreateIpam()
	require.NoError(t, err)

	resolver, err := original.CreateIpamPrefixListResolver(ipam.IpamID, "", "test resolver", nil)
	require.NoError(t, err)

	modified, err := original.ModifyIpamPrefixListResolver(resolver.IpamPrefixListResolverID, "", nil, true)
	require.NoError(t, err)
	require.EqualValues(t, 2, modified.CurrentVersion)

	snap := original.Snapshot(t.Context())

	restored := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, restored.Restore(t.Context(), snap))

	return restored, resolver.IpamPrefixListResolverID
}

func testModifyAfterRestoreContinuesNumbering(t *testing.T) {
	t.Helper()

	restored, resolverID := restoredResolverBackend(t)

	modified, err := restored.ModifyIpamPrefixListResolver(resolverID, "", nil, true)
	require.NoError(t, err)
	assert.EqualValues(t, 3, modified.CurrentVersion)
}

func testTargetAfterRestoreSeesSyncedVersion(t *testing.T) {
	t.Helper()

	restored, resolverID := restoredResolverBackend(t)

	target, err := restored.CreateIpamPrefixListResolverTarget(resolverID, "pl-test", "", false, nil)
	require.NoError(t, err)
	require.NotNil(t, target.LastSyncedVersion)
	assert.EqualValues(t, 2, *target.LastSyncedVersion)
}
