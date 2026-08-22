package codeconnections_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codeconnections"
)

// TestInMemoryBackend_RestoreInvalidData verifies that malformed JSON is
// reported as an error rather than silently discarded or partially applied.
func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := codeconnections.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that a snapshot whose
// version doesn't match the current backend is discarded cleanly rather than
// partially decoded: the backend resets to empty state and Restore returns
// no error.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	b := codeconnections.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateConnection(t.Context(), "seed-conn", "GitHub", "", nil)
	require.NoError(t, err)

	err = b.Restore(t.Context(), []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	assert.Equal(t, 0, b.ConnectionCount())
}

// TestInMemoryBackend_RestoreOldSnapshotDecodesAsZero verifies that a
// snapshot with no version field at all (the pre-Phase-3.3 shape -- plain
// region-nested resource maps) decodes with Version == 0, which mismatches
// codeconnectionsSnapshotVersion and is discarded the same way any other
// incompatible version is -- not partially applied.
func TestInMemoryBackend_RestoreOldSnapshotDecodesAsZero(t *testing.T) {
	t.Parallel()

	b := codeconnections.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateConnection(t.Context(), "seed-conn", "GitHub", "", nil)
	require.NoError(t, err)

	oldShape := `{"connections":{"us-east-1":{"arn:aws:codeconnections:us-east-1:000000000000:connection/old":` +
		`{"connectionName":"old"}}}}`
	err = b.Restore(t.Context(), []byte(oldShape))
	require.NoError(t, err)

	assert.Equal(t, 0, b.ConnectionCount())
}

// seededState holds every resource seedFullState creates, so the caller can
// re-fetch each by identity after a restore.
type seededState struct {
	eastConn *codeconnections.Connection
	westConn *codeconnections.Connection
	eastHost *codeconnections.Host
	eastLink *codeconnections.RepositoryLink
	eastCfg  *codeconnections.SyncConfiguration
}

// seedFullState populates b across two regions with one of every resource
// family the Phase 3.3 conversion touched: connections, hosts, repository
// links, and sync configurations.
func seedFullState(t *testing.T, b *codeconnections.InMemoryBackend) seededState {
	t.Helper()

	ctxEast := codeconnections.CtxRegion("us-east-1")
	ctxWest := codeconnections.CtxRegion("us-west-2")

	eastConn, err := b.CreateConnection(ctxEast, "shared-conn", "GitHub", "", map[string]string{"env": "prod"})
	require.NoError(t, err)

	westConn, err := b.CreateConnection(ctxWest, "shared-conn", "Bitbucket", "", map[string]string{"env": "staging"})
	require.NoError(t, err)

	eastHost, err := b.CreateHost(
		ctxEast, "shared-host", "GitHubEnterpriseServer", "https://east.example.com", nil, map[string]string{"k": "v"},
	)
	require.NoError(t, err)

	eastLink, err := b.CreateRepositoryLink(
		ctxEast, eastConn.ConnectionArn, "east-owner", "east-repo", "kms-arn", map[string]string{"link": "east"},
	)
	require.NoError(t, err)

	_, err = b.CreateRepositoryLink(ctxWest, westConn.ConnectionArn, "west-owner", "west-repo", "", nil)
	require.NoError(t, err)

	eastCfg, err := b.CreateSyncConfiguration(
		ctxEast, "main", "cfg.yaml", eastLink.RepositoryLinkID, "east-stack", "arn:role", "CFN_STACK_SYNC",
		"ENABLED", "ANY_CHANGE", "ENABLED",
	)
	require.NoError(t, err)

	return seededState{
		eastConn: eastConn, westConn: westConn, eastHost: eastHost, eastLink: eastLink, eastCfg: eastCfg,
	}
}

// TestInMemoryBackend_SnapshotRestore_FullState exercises a Snapshot->Restore
// round trip across every store.Table-backed resource family the Phase 3.3
// conversion touched: connections, hosts (clean, ARN-keyed), repositoryLinks,
// and syncConfigurations (dirty, region-composite-keyed). It also proves the
// byRegion secondary indexes survive the round trip (region-scoped listings
// are correct, cross-region lookups still fail, and duplicate names remain
// accepted) rather than merely checking the primary key lookups.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original := codeconnections.NewInMemoryBackend("111122223333", "us-east-1")
	seeded := seedFullState(t, original)
	eastConn, westConn, eastHost, eastLink, eastCfg := seeded.eastConn, seeded.westConn,
		seeded.eastHost, seeded.eastLink, seeded.eastCfg

	data := original.Snapshot(t.Context())
	require.NotNil(t, data)

	restored := codeconnections.NewInMemoryBackend("999999999999", "eu-west-1")
	require.NoError(t, restored.Restore(t.Context(), data))

	ctxEast := codeconnections.CtxRegion("us-east-1")
	ctxWest := codeconnections.CtxRegion("us-west-2")

	// connections: primary lookup (region-scoped) + byRegion.
	gotEastConn, err := restored.GetConnection(ctxEast, eastConn.ConnectionArn)
	require.NoError(t, err)
	assert.Equal(t, "GitHub", gotEastConn.ProviderType)
	assert.Equal(t, "prod", gotEastConn.Tags["env"])

	gotWestConn, err := restored.GetConnection(ctxWest, westConn.ConnectionArn)
	require.NoError(t, err)
	assert.Equal(t, "Bitbucket", gotWestConn.ProviderType)

	// Cross-region GetConnection must still fail after restore (region
	// isolation must survive the round trip, not just the primary lookup).
	_, err = restored.GetConnection(ctxWest, eastConn.ConnectionArn)
	require.ErrorIs(t, err, codeconnections.ErrNotFound,
		"east ARN must not resolve from west region after restore")

	assert.Len(t, restored.ListConnections(ctxEast, "", ""), 1)
	assert.Len(t, restored.ListConnections(ctxWest, "", ""), 1)

	// CreateConnection has no ResourceAlreadyExistsException in its real
	// error list (see TestConnectionNameNotUnique), so a same-named
	// connection created after restore must succeed, not be rejected.
	_, err = restored.CreateConnection(ctxEast, "shared-conn", "GitHub", "", nil)
	require.NoError(t, err, "duplicate connection names are accepted, including after restore")

	// hosts.
	gotHost, err := restored.GetHost(ctxEast, eastHost.HostArn)
	require.NoError(t, err)
	assert.Equal(t, "https://east.example.com", gotHost.ProviderEndpoint)
	assert.Equal(t, "v", gotHost.Tags["k"])

	// CreateHost has no ResourceAlreadyExistsException in its real error
	// list either (see TestCreateHostNameNotUnique).
	_, err = restored.CreateHost(
		ctxEast, "shared-host", "GitHubEnterpriseServer", "https://other.example.com", nil, nil,
	)
	require.NoError(t, err, "duplicate host names are accepted, including after restore")

	// repositoryLinks: ctx-region-scoped lookup (not ARN-derived).
	gotLink, err := restored.GetRepositoryLink(ctxEast, eastLink.RepositoryLinkID)
	require.NoError(t, err)
	assert.Equal(t, "east-owner", gotLink.OwnerID)
	assert.Equal(t, "kms-arn", gotLink.EncryptionKeyArn)
	assert.Equal(t, "east", gotLink.Tags["link"])

	_, err = restored.GetRepositoryLink(ctxWest, eastLink.RepositoryLinkID)
	require.ErrorIs(t, err, codeconnections.ErrNotFound,
		"repository link region-scoping must survive restore: wrong-region ctx must not find it")

	assert.Len(t, restored.ListRepositoryLinks(ctxEast), 1)
	assert.Len(t, restored.ListRepositoryLinks(ctxWest), 1)

	// syncConfigurations.
	gotCfg, err := restored.GetSyncConfiguration(ctxEast, "east-stack", "CFN_STACK_SYNC")
	require.NoError(t, err)
	assert.Equal(t, eastCfg.RoleArn, gotCfg.RoleArn)
	assert.Equal(t, "ENABLED", gotCfg.PublishDeploymentStatus)
	assert.Equal(t, "ANY_CHANGE", gotCfg.TriggerResourceUpdateOn)
	assert.Equal(t, "ENABLED", gotCfg.PullRequestComment,
		"PullRequestComment must survive the syncConfigurations DTO round trip")

	_, err = restored.GetSyncConfiguration(ctxWest, "east-stack", "CFN_STACK_SYNC")
	require.ErrorIs(t, err, codeconnections.ErrNotFound,
		"sync configuration region-scoping must survive restore")

	assert.Len(t, restored.ListSyncConfigurations(ctxEast, eastLink.RepositoryLinkID, ""), 1)

	// region (accountID has no exported getter on this backend).
	assert.Equal(t, "us-east-1", restored.Region())
}

// TestInMemoryBackend_SnapshotRestore_EmptyState verifies that an empty
// backend round-trips to another empty backend, rather than panicking or
// leaving stray entries from nil-map handling.
func TestInMemoryBackend_SnapshotRestore_EmptyState(t *testing.T) {
	t.Parallel()

	original := codeconnections.NewInMemoryBackend("000000000000", "us-east-1")

	data := original.Snapshot(t.Context())
	require.NotNil(t, data)

	restored := codeconnections.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, restored.Restore(t.Context(), data))

	ctx := codeconnections.CtxRegion("us-east-1")
	assert.Empty(t, restored.ListConnections(ctx, "", ""))
	assert.Empty(t, restored.ListHosts(ctx))
	assert.Empty(t, restored.ListRepositoryLinks(ctx))
	assert.Equal(t, 0, restored.ConnectionCount())
	assert.Equal(t, 0, restored.HostCount())
	assert.Equal(t, 0, restored.RepositoryLinkCount())
	assert.Equal(t, 0, restored.SyncConfigurationCount())
}

// TestSnapshotRestore verifies Snapshot/Restore round-trip preserves state
// across every resource family, exercised through the handler.
func TestSnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "snapshot_restore_round_trip"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()

			createConn(t, h, "conn-snap", "GitHub")
			createHost(t, h, "host-snap", "GitHubEnterpriseServer", "https://ghe.example.com")
			connArn := createConn(t, h, "conn-snap2", "GitLab")
			linkID := createRepositoryLink(t, h, connArn, "my-org", "my-repo")

			snap := h.Backend.Snapshot(t.Context())
			require.NotNil(t, snap)

			newBackend := codeconnections.NewInMemoryBackend("123456789012", "us-east-1")
			require.NoError(t, newBackend.Restore(t.Context(), snap))

			conns := newBackend.ListConnections(context.Background(), "", "")
			assert.Len(t, conns, 2)

			_, err := newBackend.GetRepositoryLink(context.Background(), linkID)
			require.NoError(t, err)
		})
	}
}

// TestSnapshotRestoreHostNameNotUnique verifies a post-restore host is fully
// usable and that same-named hosts are still accepted after restore (see
// TestCreateHostNameNotUnique).
func TestSnapshotRestoreHostNameNotUnique(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "hosts_restored"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			hostArn1 := createHost(t, h, "snap-host", "GitHubEnterpriseServer", "https://ghe.example.com")

			snap := h.Backend.Snapshot(t.Context())
			require.NotNil(t, snap)

			newBackend := codeconnections.NewInMemoryBackend("123456789012", "us-east-1")
			require.NoError(t, newBackend.Restore(t.Context(), snap))

			got, err := newBackend.GetHost(context.Background(), hostArn1)
			require.NoError(t, err)
			assert.Equal(t, "snap-host", got.Name)

			host2, err := newBackend.CreateHost(
				context.Background(),
				"snap-host",
				"GitHubEnterpriseServer",
				"https://new.example.com",
				nil,
				nil,
			)
			require.NoError(t, err, "duplicate host name must succeed after restore")
			assert.NotEqual(t, hostArn1, host2.HostArn)
		})
	}
}

// TestHandler_SnapshotRestore proves the dead-wiring fix: Handler.Snapshot/
// Handler.Restore must delegate to the backend so cli.go's setupPersistence
// (which type-asserts the service.Registerable value returned by
// Provider.Init -- the Handler, not InMemoryBackend -- against a
// Snapshot/Restore interface) actually picks this service up.
func TestHandler_SnapshotRestore(t *testing.T) {
	t.Parallel()

	backend := codeconnections.NewInMemoryBackend("000000000000", "us-east-1")
	h := codeconnections.NewHandler(backend)

	_, err := backend.CreateConnection(t.Context(), "via-handler", "GitHub", "", nil)
	require.NoError(t, err)

	data := h.Snapshot(t.Context())
	require.NotNil(t, data)

	restoredBackend := codeconnections.NewInMemoryBackend("000000000000", "us-east-1")
	restoredHandler := codeconnections.NewHandler(restoredBackend)
	require.NoError(t, restoredHandler.Restore(t.Context(), data))

	assert.Equal(t, 1, restoredBackend.ConnectionCount())
}
