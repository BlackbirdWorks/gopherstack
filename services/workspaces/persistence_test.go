package workspaces_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/workspaces"
)

// newPersistenceTestBackend creates a backend with one populated entry in
// every store.Table (see store_setup.go's registerAllTables) plus the one
// raw map that was already persisted pre-Phase-3.3 (tags), so a Snapshot from
// it exercises the entire persisted surface of the backend. poolSessions and
// applications have no Create path anywhere in the backend (dead maps, both
// pre- and post-refactor), so they are exercised only as empty tables here.
func newPersistenceTestBackend(t *testing.T) *workspaces.InMemoryBackend {
	t.Helper()

	b := workspaces.NewInMemoryBackend("000000000000", "us-east-1")
	ctx := t.Context()

	require.NoError(t, b.RegisterWorkspaceDirectory("d-1234567890", []string{"subnet-1"}))

	ws, err := b.CreateWorkspace(ctx, &workspaces.WorkspaceCreationSpec{
		DirectoryID: "d-1234567890",
		UserName:    "alice",
		BundleID:    "wsb-bh8rsxt14",
		Tags:        map[string]string{"env": "test"},
	})
	require.NoError(t, err)

	require.NoError(t, b.CreateTags(ws.WorkspaceID, map[string]string{"extra": "tag"}))

	_, err = b.CreateIpGroup("grp1", "desc", nil, map[string]string{"k": "v"})
	require.NoError(t, err)

	_, err = b.CreateConnectionAlias("conn.example.com", map[string]string{"k": "v"})
	require.NoError(t, err)

	_, err = b.CreateWorkspaceBundle("custom-bundle", "desc", "wsi-00000001", "STANDARD", map[string]string{"k": "v"})
	require.NoError(t, err)

	_, err = b.CreateWorkspaceImage("img1", "desc", ws.WorkspaceID, map[string]string{"k": "v"})
	require.NoError(t, err)

	_, err = b.CreateWorkspacesPool(
		"pool1", "wsb-bh8rsxt14", "d-1234567890", "desc", "ALWAYS_ON", 5,
		map[string]string{"k": "v"},
	)
	require.NoError(t, err)

	_, err = b.CreateConnectClientAddIn("addin1", ws.WorkspaceID, "https://example.com")
	require.NoError(t, err)

	require.NoError(t, b.ImportClientBranding(
		ws.WorkspaceID, map[string]map[string]any{"WINDOWS": {"logo": "abc"}},
	))

	_, err = b.CreateAccountLinkInvitation("111111111111")
	require.NoError(t, err)

	return b
}

// TestInMemoryBackend_SnapshotRestore_FullState round-trips every store.Table
// (workspaces, ipGroups, connAliases, customBundles, images, pools,
// poolSessions, connectAddIns, clientBranding, accountLinks, applications,
// dirSettings) and the one raw map left un-converted-but-persisted (tags)
// through Snapshot -> Restore into a fresh backend.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original := newPersistenceTestBackend(t)
	ctx := t.Context()

	snap := original.Snapshot(ctx)
	require.NotNil(t, snap)

	fresh := workspaces.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(ctx, snap))

	// workspaces table.
	wsList, _, err := fresh.DescribeWorkspaces(ctx, nil, nil, nil, nil, 0, "")
	require.NoError(t, err)
	require.Len(t, wsList, 1)
	wsID := wsList[0].WorkspaceID
	assert.Equal(t, "alice", wsList[0].UserName)

	// tags raw map, persisted alongside the tables; CreateTags merged
	// "extra" into the workspace's own tag set too.
	tags, err := fresh.DescribeTags(wsID)
	require.NoError(t, err)
	assert.Equal(t, "tag", tags["extra"])
	assert.Equal(t, "test", tags["env"])

	// ipGroups table.
	groups, _, err := fresh.DescribeIpGroups(nil, 0, "")
	require.NoError(t, err)
	require.Len(t, groups, 1)
	assert.Equal(t, "grp1", groups[0].GroupName)

	// connAliases table.
	aliases, _, err := fresh.DescribeConnectionAliases(nil, "", 0, "")
	require.NoError(t, err)
	require.Len(t, aliases, 1)
	assert.Equal(t, "conn.example.com", aliases[0].ConnectionString)

	// customBundles table.
	bundles, _, err := fresh.DescribeWorkspaceBundles(ctx, nil, "000000000000", "")
	require.NoError(t, err)
	require.Len(t, bundles, 1)
	assert.Equal(t, "custom-bundle", bundles[0].Name)

	// images table.
	images, _, err := fresh.DescribeWorkspaceImages(nil, "", 0, "")
	require.NoError(t, err)
	require.Len(t, images, 1)
	assert.Equal(t, "img1", images[0].Name)

	// pools table.
	pools, _, err := fresh.DescribeWorkspacesPools(nil, 0, "")
	require.NoError(t, err)
	require.Len(t, pools, 1)
	assert.Equal(t, "pool1", pools[0].PoolName)

	// connectAddIns table.
	addIns, _, err := fresh.DescribeConnectClientAddIns(wsID, 0, "")
	require.NoError(t, err)
	require.Len(t, addIns, 1)
	assert.Equal(t, "addin1", addIns[0].Name)

	// clientBranding table.
	branding, err := fresh.DescribeClientBranding(wsID)
	require.NoError(t, err)
	require.Contains(t, branding, "WINDOWS")
	assert.Equal(t, "abc", branding["WINDOWS"]["logo"])

	// accountLinks table.
	links, _, err := fresh.ListAccountLinks("", 0, "")
	require.NoError(t, err)
	require.Len(t, links, 1)
	assert.Equal(t, "111111111111", links[0].TargetAccountID)

	// dirSettings table.
	dirs, _, err := fresh.DescribeWorkspaceDirectories(ctx, nil, "")
	require.NoError(t, err)
	require.Len(t, dirs, 1)
	assert.Equal(t, "d-1234567890", dirs[0].DirectoryID)
	assert.Equal(t, []string{"subnet-1"}, dirs[0].SubnetIDs)

	// poolSessions and applications tables have no Create path anywhere in
	// the backend, so they round-trip as empty rather than populated.
	sessions, _, err := fresh.DescribeWorkspacesPoolSessions(pools[0].PoolID, "", 0, "")
	require.NoError(t, err)
	assert.Empty(t, sessions)

	apps, _, err := fresh.DescribeApplications(nil, 0, "")
	require.NoError(t, err)
	assert.Empty(t, apps)

	// Sanity: account/region carried through the constructor, not the
	// snapshot (matching pre-Phase-3.3 behavior -- see persistence.go).
	assert.Equal(t, "us-east-1", fresh.Region())
	assert.Equal(t, "000000000000", fresh.AccountID())
}

// TestInMemoryBackend_SnapshotRestore_DirectoryIpGroupsPersisted documents
// that directoryIpGroups (unlike imagePermissions, clientProperties, and
// appAssociations, which remain ephemeral) now survives a Snapshot -> Restore
// round trip -- fixed alongside the AssociateIpGroups/DisassociateIpGroups
// persistence gap (see PARITY.md gaps history; previously all four raw maps
// were ephemeral, matching pre-Phase-3.3 behavior).
func TestInMemoryBackend_SnapshotRestore_DirectoryIpGroupsPersisted(t *testing.T) {
	t.Parallel()

	b := workspaces.NewInMemoryBackend("000000000000", "us-east-1")
	ctx := t.Context()

	require.NoError(t, b.RegisterWorkspaceDirectory("d-1234567890", []string{"subnet-1"}))

	_, err := b.CreateIpGroup("grp1", "desc", nil, nil)
	require.NoError(t, err)
	require.NoError(t, b.AssociateIpGroups("d-1234567890", []string{"grp1"}))

	snap := b.Snapshot(ctx)
	require.NotNil(t, snap)

	fresh := workspaces.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(ctx, snap))

	dirs, _, err := fresh.DescribeWorkspaceDirectories(ctx, nil, "")
	require.NoError(t, err)
	require.Len(t, dirs, 1)

	groups, _, err := fresh.DescribeIpGroups(nil, 0, "")
	require.NoError(t, err)
	require.Len(t, groups, 1)

	assert.Equal(t, []string{"grp1"}, workspaces.DirectoryIPGroupIDs(fresh, "d-1234567890"))
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that a snapshot whose
// version doesn't match the current backend (including the pre-Phase-3.3
// format, which decodes with Version == 0) is discarded cleanly rather than
// partially decoded: the backend resets to empty state and Restore returns
// no error.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	b := newPersistenceTestBackend(t)
	ctx := t.Context()

	err := b.Restore(ctx, []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	assert.Equal(t, 0, workspaces.WorkspaceCount(b))

	wsList, _, err := b.DescribeWorkspaces(ctx, nil, nil, nil, nil, 0, "")
	require.NoError(t, err)
	assert.Empty(t, wsList)

	groups, _, err := b.DescribeIpGroups(nil, 0, "")
	require.NoError(t, err)
	assert.Empty(t, groups)

	tags, err := b.DescribeTags("ws-00000001")
	require.NoError(t, err)
	assert.Empty(t, tags)
}

// TestInMemoryBackend_RestoreInvalidData verifies malformed JSON surfaces as
// an error rather than being silently discarded (that path is reserved for a
// syntactically valid but version-mismatched snapshot; see
// TestInMemoryBackend_RestoreVersionMismatch).
func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := workspaces.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestHandler_SnapshotRestoreDelegate verifies Handler.Snapshot/Restore
// delegate to the backend (the wiring cli.go's generic setupPersistence
// relies on -- WorkSpaces previously had no Handler.Snapshot/Restore at all,
// so it was never actually registered with the persistence manager despite
// InMemoryBackend implementing persistence.Persistable; see persistence.go).
func TestHandler_SnapshotRestoreDelegate(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	h := workspaces.NewHandler(newPersistenceTestBackend(t))

	snap := h.Snapshot(ctx)
	require.NotNil(t, snap)

	h2 := workspaces.NewHandler(workspaces.NewInMemoryBackend("000000000000", "us-east-1"))
	require.NoError(t, h2.Restore(ctx, snap))

	wsList, _, err := h2.Backend.DescribeWorkspaces(ctx, nil, nil, nil, nil, 0, "")
	require.NoError(t, err)
	require.Len(t, wsList, 1)
	assert.Equal(t, "alice", wsList[0].UserName)
}
