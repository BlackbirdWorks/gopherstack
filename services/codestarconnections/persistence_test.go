package codestarconnections_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codestarconnections"
)

// TestInMemoryBackend_RestoreInvalidData verifies that malformed JSON is
// reported as an error rather than silently discarded or partially applied.
func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := codestarconnections.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that a snapshot whose
// version doesn't match the current backend is discarded cleanly rather than
// partially decoded: the backend resets to empty state and Restore returns
// no error.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	b := codestarconnections.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateConnection(t.Context(), "seed-conn", "GitHub", "", nil)
	require.NoError(t, err)

	err = b.Restore(t.Context(), []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	assert.Equal(t, 0, b.ConnectionCount())
}

// TestInMemoryBackend_RestoreOldSnapshotDecodesAsZero verifies that a
// snapshot with no version field at all (the pre-Phase-3.3 shape -- plain
// region-nested resource maps) decodes with Version == 0, which mismatches
// codestarconnectionsSnapshotVersion and is discarded the same way any other
// incompatible version is -- not partially applied.
func TestInMemoryBackend_RestoreOldSnapshotDecodesAsZero(t *testing.T) {
	t.Parallel()

	b := codestarconnections.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateConnection(t.Context(), "seed-conn", "GitHub", "", nil)
	require.NoError(t, err)

	oldShape := `{"connections":{"us-east-1":{"arn:aws:codestar-connections:us-east-1:000000000000:connection/old":` +
		`{"connectionName":"old"}}}}`
	err = b.Restore(t.Context(), []byte(oldShape))
	require.NoError(t, err)

	assert.Equal(t, 0, b.ConnectionCount())
}

// seededState holds every resource seedFullState creates, so the caller can
// re-fetch each by identity after a restore.
type seededState struct {
	eastConn *codestarconnections.Connection
	westConn *codestarconnections.Connection
	eastHost *codestarconnections.Host
	eastLink *codestarconnections.RepositoryLink
	eastCfg  *codestarconnections.SyncConfiguration
}

// seedFullState populates b across two regions with one of every resource
// family the Phase 3.3 conversion touched: connections, hosts, repository
// links, sync configurations (plus the resource sync status seeded
// automatically by CreateSyncConfigurationFull), an explicit
// RepositorySyncStatus (via the test helper), and a sync blocker.
func seedFullState(t *testing.T, b *codestarconnections.InMemoryBackend) seededState {
	t.Helper()

	ctxEast := codestarconnections.CtxRegion("us-east-1")
	ctxWest := codestarconnections.CtxRegion("us-west-2")

	eastConn, err := b.CreateConnection(ctxEast, "shared-conn", "GitHub", "", map[string]string{"env": "prod"})
	require.NoError(t, err)

	westConn, err := b.CreateConnection(ctxWest, "shared-conn", "Bitbucket", "", map[string]string{"env": "staging"})
	require.NoError(t, err)

	eastHost, err := b.CreateHost(
		ctxEast, "shared-host", "GitHubEnterpriseServer", "https://east.example.com",
		&codestarconnections.VpcConfiguration{VpcID: "vpc-1", SubnetIDs: []string{"subnet-1"}}, nil,
	)
	require.NoError(t, err)

	eastLink, err := b.CreateRepositoryLink(ctxEast, eastConn.ConnectionArn, "east-owner", "east-repo", "kms-arn")
	require.NoError(t, err)

	_, err = b.CreateRepositoryLink(ctxWest, westConn.ConnectionArn, "west-owner", "west-repo", "")
	require.NoError(t, err)

	eastCfg, err := b.CreateSyncConfigurationFull(
		ctxEast, "main", "cfg.yaml", eastLink.RepositoryLinkID, "east-stack", "arn:role", "CFN_STACK_SYNC",
		"ENABLED", "ANY_CHANGE",
	)
	require.NoError(t, err)

	b.SetRepositorySyncStatus(ctxEast, eastLink.RepositoryLinkID, "main", "CFN_STACK_SYNC", "SUCCEEDED",
		[]codestarconnections.SyncEvent{{Event: "seeded", Type: "INFO", ExternalID: "ext-1"}})

	b.SetResourceSyncStatus(ctxEast, "east-stack", "CFN_STACK_SYNC", "FAILED",
		[]codestarconnections.SyncEvent{{Event: "resource-seeded", Type: "ERROR", ExternalID: "ext-2"}})

	_, err = b.CreateSyncBlocker(ctxEast, "east-stack", "CFN_STACK_SYNC", "AUTOMATED", "blocked for testing")
	require.NoError(t, err)

	return seededState{
		eastConn: eastConn, westConn: westConn, eastHost: eastHost, eastLink: eastLink, eastCfg: eastCfg,
	}
}

// TestInMemoryBackend_SnapshotRestore_FullState exercises a Snapshot->Restore
// round trip across every store.Table-backed resource family the Phase 3.3
// conversion touched: connections, hosts (clean, ARN-keyed), repositoryLinks,
// syncConfigurations, repositorySyncStatuses, resourceSyncStatuses, and
// syncBlockers (dirty, region-composite-keyed). It also proves the byName/
// byRegion/byResource secondary indexes survive the round trip (duplicate
// creates are rejected, region-scoped listings are correct, sync blocker
// resolution still finds its target) rather than merely checking the primary
// key lookups.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original := codestarconnections.NewInMemoryBackend("111122223333", "us-east-1")
	seeded := seedFullState(t, original)
	eastConn, westConn, eastHost, eastLink, eastCfg := seeded.eastConn, seeded.westConn,
		seeded.eastHost, seeded.eastLink, seeded.eastCfg

	data := original.Snapshot(t.Context())
	require.NotNil(t, data)

	restored := codestarconnections.NewInMemoryBackend("999999999999", "eu-west-1")
	require.NoError(t, restored.Restore(t.Context(), data))

	ctxEast := codestarconnections.CtxRegion("us-east-1")
	ctxWest := codestarconnections.CtxRegion("us-west-2")

	// accountID/region.
	assert.Equal(t, "111122223333", restored.AccountID())
	assert.Equal(t, "us-east-1", restored.Region())

	// connections: primary lookup + byRegion + byName (duplicate-name reject).
	gotEastConn, err := restored.GetConnection(ctxEast, eastConn.ConnectionArn)
	require.NoError(t, err)
	assert.Equal(t, "GitHub", gotEastConn.ProviderType)
	assert.Equal(t, "prod", gotEastConn.Tags["env"])

	gotWestConn, err := restored.GetConnection(ctxWest, westConn.ConnectionArn)
	require.NoError(t, err)
	assert.Equal(t, "Bitbucket", gotWestConn.ProviderType)

	assert.Len(t, restored.ListConnections(ctxEast, "", ""), 1)
	assert.Len(t, restored.ListConnections(ctxWest, "", ""), 1)

	_, err = restored.CreateConnection(ctxEast, "shared-conn", "GitHub", "", nil)
	require.ErrorIs(t, err, codestarconnections.ErrAlreadyExists,
		"byName index must be rebuilt so the duplicate-name check still fires after restore")

	// hosts.
	gotHost, err := restored.GetHost(ctxEast, eastHost.HostArn)
	require.NoError(t, err)
	assert.Equal(t, "https://east.example.com", gotHost.ProviderEndpoint)
	require.NotNil(t, gotHost.VpcConfiguration)
	assert.Equal(t, "vpc-1", gotHost.VpcConfiguration.VpcID)

	// repositoryLinks: ctx-region-scoped lookup (not ARN-derived).
	gotLink, err := restored.GetRepositoryLink(ctxEast, eastLink.RepositoryLinkID)
	require.NoError(t, err)
	assert.Equal(t, "east-owner", gotLink.OwnerID)
	assert.Equal(t, "kms-arn", gotLink.EncryptionKeyArn)

	_, err = restored.GetRepositoryLink(ctxWest, eastLink.RepositoryLinkID)
	require.ErrorIs(t, err, codestarconnections.ErrNotFound,
		"repository link region-scoping must survive restore: wrong-region ctx must not find it")

	assert.Len(t, restored.ListRepositoryLinks(ctxEast), 1)
	assert.Len(t, restored.ListRepositoryLinks(ctxWest), 1)

	// syncConfigurations.
	gotCfg, err := restored.GetSyncConfiguration(ctxEast, "east-stack", "CFN_STACK_SYNC")
	require.NoError(t, err)
	assert.Equal(t, eastCfg.RoleArn, gotCfg.RoleArn)
	assert.Equal(t, "ENABLED", gotCfg.PublishDeploymentStatus)
	assert.Equal(t, "ANY_CHANGE", gotCfg.TriggerResourceUpdateOn)

	// repositorySyncStatuses (dirty: 4 hidden composite-key fields).
	rss, err := restored.GetRepositorySyncStatus(ctxEast, eastLink.RepositoryLinkID, "main", "CFN_STACK_SYNC")
	require.NoError(t, err)
	assert.Equal(t, "SUCCEEDED", rss.Status)
	require.Len(t, rss.Events, 1)
	assert.Equal(t, "seeded", rss.Events[0].Event)

	// resourceSyncStatuses: the explicit SetResourceSyncStatus overwrite must
	// win over the SUCCEEDED status CreateSyncConfigurationFull auto-seeds.
	res, err := restored.GetResourceSyncStatus(ctxEast, "east-stack", "CFN_STACK_SYNC")
	require.NoError(t, err)
	assert.Equal(t, "FAILED", res.Status)
	require.Len(t, res.Events, 1)
	assert.Equal(t, "resource-seeded", res.Events[0].Event)

	// syncBlockers + the byResource index (GetSyncBlockerSummary).
	summary, err := restored.GetSyncBlockerSummary(ctxEast, "east-stack", "CFN_STACK_SYNC")
	require.NoError(t, err)
	require.Len(t, summary.LatestBlockers, 1)
	assert.Equal(t, "blocked for testing", summary.LatestBlockers[0].CreatedReason)
	assert.Equal(t, codestarconnections.SyncBlockerStatusActive, summary.LatestBlockers[0].Status)

	// UpdateSyncBlocker's region-scoped ID lookup must also survive restore.
	resolvedSummary, err := restored.UpdateSyncBlocker(ctxEast, summary.LatestBlockers[0].ID, "fixed")
	require.NoError(t, err)
	require.Len(t, resolvedSummary.LatestBlockers, 1)
	assert.Equal(t, codestarconnections.SyncBlockerStatusResolved, resolvedSummary.LatestBlockers[0].Status)
}

// TestInMemoryBackend_SnapshotRestore_EmptyState verifies that an empty
// backend round-trips to another empty backend, rather than panicking or
// leaving stray entries from nil-map handling.
func TestInMemoryBackend_SnapshotRestore_EmptyState(t *testing.T) {
	t.Parallel()

	original := codestarconnections.NewInMemoryBackend("000000000000", "us-east-1")

	data := original.Snapshot(t.Context())
	require.NotNil(t, data)

	restored := codestarconnections.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, restored.Restore(t.Context(), data))

	ctx := codestarconnections.CtxRegion("us-east-1")
	assert.Empty(t, restored.ListConnections(ctx, "", ""))
	assert.Empty(t, restored.ListHosts(ctx))
	assert.Empty(t, restored.ListRepositoryLinks(ctx))
}

// TestHandler_SnapshotRestore proves the dead-wiring fix: Handler.Snapshot/
// Handler.Restore must delegate to the backend so cli.go's setupPersistence
// (which type-asserts the service.Registerable value returned by
// Provider.Init -- the Handler, not InMemoryBackend -- against a
// Snapshot/Restore interface) actually picks this service up.
func TestHandler_SnapshotRestore(t *testing.T) {
	t.Parallel()

	backend := codestarconnections.NewInMemoryBackend("000000000000", "us-east-1")
	h := codestarconnections.NewHandler(backend)

	_, err := backend.CreateConnection(t.Context(), "via-handler", "GitHub", "", nil)
	require.NoError(t, err)

	data := h.Snapshot(t.Context())
	require.NotNil(t, data)

	restoredBackend := codestarconnections.NewInMemoryBackend("000000000000", "us-east-1")
	restoredHandler := codestarconnections.NewHandler(restoredBackend)
	require.NoError(t, restoredHandler.Restore(t.Context(), data))

	assert.Equal(t, 1, restoredBackend.ConnectionCount())
}
