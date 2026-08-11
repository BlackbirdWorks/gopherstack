package workspaces

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func directoryIPGroupIDs(b *InMemoryBackend, directoryID string) []string {
	b.mu.RLock("test.directoryIPGroupIDs")
	defer b.mu.RUnlock()

	groups := b.directoryIpGroups[directoryID]
	ids := make([]string, 0, len(groups))

	for id := range groups {
		ids = append(ids, id)
	}

	return ids
}

// TestInMemoryBackend_SnapshotRestore_DirectoryIpGroupsPersisted documents
// that directoryIpGroups (unlike imagePermissions, clientProperties, and
// appAssociations, which remain ephemeral) now survives a Snapshot -> Restore
// round trip -- fixed alongside the AssociateIpGroups/DisassociateIpGroups
// persistence gap (see PARITY.md gaps history; previously all four raw maps
// were ephemeral, matching pre-Phase-3.3 behavior).
func TestInMemoryBackend_SnapshotRestore_DirectoryIpGroupsPersisted(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("000000000000", "us-east-1")
	ctx := t.Context()

	require.NoError(t, b.RegisterWorkspaceDirectory("d-1234567890", []string{"subnet-1"}))

	_, err := b.CreateIpGroup("grp1", "desc", nil, nil)
	require.NoError(t, err)
	require.NoError(t, b.AssociateIpGroups("d-1234567890", []string{"grp1"}))

	snap := b.Snapshot(ctx)
	require.NotNil(t, snap)

	fresh := NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(ctx, snap))

	dirs, _, err := fresh.DescribeWorkspaceDirectories(ctx, nil, "")
	require.NoError(t, err)
	require.Len(t, dirs, 1)

	groups, _, err := fresh.DescribeIpGroups(nil, 0, "")
	require.NoError(t, err)
	require.Len(t, groups, 1)

	assert.Equal(t, []string{"grp1"}, directoryIPGroupIDs(fresh, "d-1234567890"))
}
