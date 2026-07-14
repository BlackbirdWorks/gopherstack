package codeconnections_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/codeconnections"
)

// newTestBackend creates a fresh in-memory backend for direct backend-level tests.
func newTestBackend() *codeconnections.InMemoryBackend {
	return codeconnections.NewInMemoryBackend("123456789012", config.DefaultRegion)
}

// TestDeleteHost_InUse verifies that a host cannot be deleted while a connection
// still references it. Real AWS documents that all connections associated to a
// host must be deleted before the host itself can be deleted.
func TestDeleteHost_InUse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr    error
		name       string
		attachConn bool
	}{
		{name: "host_with_active_connection_rejected", attachConn: true, wantErr: codeconnections.ErrResourceInUse},
		{name: "host_with_no_connections_deletes", attachConn: false, wantErr: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			b := newTestBackend()

			host, err := b.CreateHost(ctx, "my-host", "GitHubEnterpriseServer", "https://ghe.example.com", nil)
			require.NoError(t, err)

			if tt.attachConn {
				_, connErr := b.CreateConnection(ctx, "my-conn", "GitHubEnterpriseServer", host.HostArn, nil)
				require.NoError(t, connErr)
			}

			err = b.DeleteHost(ctx, host.HostArn)

			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestDeleteRepositoryLink_InUse verifies that a repository link cannot be deleted
// while a sync configuration still references it. Real DeleteRepositoryLink
// documents SyncConfigurationStillExistsException for this case.
func TestDeleteRepositoryLink_InUse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr    error
		name       string
		attachSync bool
	}{
		{
			name:       "link_with_active_sync_config_rejected",
			attachSync: true,
			wantErr:    codeconnections.ErrSyncConfigStillExists,
		},
		{name: "link_with_no_sync_configs_deletes", attachSync: false, wantErr: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			b := newTestBackend()

			conn, err := b.CreateConnection(ctx, "my-conn", "GitHub", "", nil)
			require.NoError(t, err)

			link, err := b.CreateRepositoryLink(ctx, conn.ConnectionArn, "my-org", "my-repo", "", nil)
			require.NoError(t, err)

			if tt.attachSync {
				_, syncErr := b.CreateSyncConfiguration(
					ctx, "main", "sync.yaml", link.RepositoryLinkID, "my-stack",
					"arn:aws:iam::123456789012:role/r", "CFN_STACK_SYNC", "", "",
				)
				require.NoError(t, syncErr)
			}

			err = b.DeleteRepositoryLink(ctx, link.RepositoryLinkID)

			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestCreateRepositoryLink_Duplicate verifies that creating a repository link for the
// same connection+owner+repo combination twice is rejected. Real CreateRepositoryLink
// registers a dedicated ResourceAlreadyExistsException.
func TestCreateRepositoryLink_Duplicate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newTestBackend()

	conn, err := b.CreateConnection(ctx, "my-conn", "GitHub", "", nil)
	require.NoError(t, err)

	_, err = b.CreateRepositoryLink(ctx, conn.ConnectionArn, "my-org", "my-repo", "", nil)
	require.NoError(t, err)

	_, err = b.CreateRepositoryLink(ctx, conn.ConnectionArn, "my-org", "my-repo", "", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, codeconnections.ErrAlreadyExists)
}

// TestCreateSyncConfiguration_Duplicate verifies that creating a sync configuration for
// the same ResourceName+SyncType twice is rejected instead of silently overwriting the
// existing configuration. Real CreateSyncConfiguration registers a dedicated
// ResourceAlreadyExistsException.
func TestCreateSyncConfiguration_Duplicate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newTestBackend()

	conn, err := b.CreateConnection(ctx, "my-conn", "GitHub", "", nil)
	require.NoError(t, err)

	link, err := b.CreateRepositoryLink(ctx, conn.ConnectionArn, "my-org", "my-repo", "", nil)
	require.NoError(t, err)

	_, err = b.CreateSyncConfiguration(
		ctx, "main", "sync.yaml", link.RepositoryLinkID, "my-stack",
		"arn:aws:iam::123456789012:role/r", "CFN_STACK_SYNC", "", "",
	)
	require.NoError(t, err)

	_, err = b.CreateSyncConfiguration(
		ctx, "develop", "other.yaml", link.RepositoryLinkID, "my-stack",
		"arn:aws:iam::123456789012:role/r2", "CFN_STACK_SYNC", "", "",
	)
	require.ErrorIs(t, err, codeconnections.ErrAlreadyExists)

	// The original configuration must be untouched by the rejected duplicate.
	cfg, err := b.GetSyncConfiguration(ctx, "my-stack", "CFN_STACK_SYNC")
	require.NoError(t, err)
	assert.Equal(t, "main", cfg.Branch)
}

// TestListRepositorySyncDefinitions_DerivedFromSyncConfig verifies real sync definitions
// are derived from the repository link's sync configurations (Branch/ConfigFile-as-
// Directory/ResourceName-as-Target+Parent), not a hardcoded empty stub.
func TestListRepositorySyncDefinitions_DerivedFromSyncConfig(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newTestBackend()

	conn, err := b.CreateConnection(ctx, "my-conn", "GitHub", "", nil)
	require.NoError(t, err)

	link, err := b.CreateRepositoryLink(ctx, conn.ConnectionArn, "my-org", "my-repo", "", nil)
	require.NoError(t, err)

	_, err = b.CreateSyncConfiguration(
		ctx, "main", "template.yaml", link.RepositoryLinkID, "my-stack",
		"arn:aws:iam::123456789012:role/r", "CFN_STACK_SYNC", "", "",
	)
	require.NoError(t, err)

	defs, err := b.ListRepositorySyncDefinitions(ctx, link.RepositoryLinkID, "CFN_STACK_SYNC")
	require.NoError(t, err)
	require.Len(t, defs, 1)
	assert.Equal(t, "main", defs[0].Branch)
	assert.Equal(t, "template.yaml", defs[0].Directory)
	// Real AWS docs: "for CFN_STACK_SYNC the parent and target resource are the same".
	assert.Equal(t, "my-stack", defs[0].Parent)
	assert.Equal(t, "my-stack", defs[0].Target)
}

// TestSyncBlocker_CreateGetResolve exercises the real sync blocker lifecycle: creating a
// blocker, reading it back via GetSyncBlockerSummary, and resolving it via
// UpdateSyncBlocker.
func TestSyncBlocker_CreateGetResolve(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newTestBackend()

	conn, err := b.CreateConnection(ctx, "my-conn", "GitHub", "", nil)
	require.NoError(t, err)

	link, err := b.CreateRepositoryLink(ctx, conn.ConnectionArn, "my-org", "my-repo", "", nil)
	require.NoError(t, err)

	_, err = b.CreateSyncConfiguration(
		ctx, "main", "sync.yaml", link.RepositoryLinkID, "my-stack",
		"arn:aws:iam::123456789012:role/r", "CFN_STACK_SYNC", "", "",
	)
	require.NoError(t, err)

	blocker, err := b.CreateSyncBlocker(ctx, "my-stack", "CFN_STACK_SYNC", "AUTOMATED", "drift detected")
	require.NoError(t, err)
	assert.Equal(t, codeconnections.SyncBlockerStatusActive, blocker.Status)

	summary, err := b.GetSyncBlockerSummary(ctx, "my-stack", "CFN_STACK_SYNC")
	require.NoError(t, err)
	require.Len(t, summary.LatestBlockers, 1)
	assert.Equal(t, blocker.ID, summary.LatestBlockers[0].ID)
	assert.Equal(t, codeconnections.SyncBlockerStatusActive, summary.LatestBlockers[0].Status)

	resolved, err := b.UpdateSyncBlocker(ctx, blocker.ID, "fixed")
	require.NoError(t, err)
	require.Len(t, resolved.LatestBlockers, 1)
	assert.Equal(t, codeconnections.SyncBlockerStatusResolved, resolved.LatestBlockers[0].Status)
	assert.Equal(t, "fixed", resolved.LatestBlockers[0].ResolvedReason)

	_, err = b.UpdateSyncBlocker(ctx, "nonexistent-id", "fixed")
	require.Error(t, err)
	assert.ErrorIs(t, err, codeconnections.ErrSyncBlockerNotFound)
}

// TestValidProviderTypes_AzureDevOps verifies AzureDevOps is accepted as a real
// CodeConnections provider type (types.ProviderTypeAzureDevOps in
// aws-sdk-go-v2/service/codeconnections/types/enums.go) -- unlike the older
// CodeStarConnections service predating it.
func TestValidProviderTypes_AzureDevOps(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newTestBackend()

	conn, err := b.CreateConnection(ctx, "azdo-conn", "AzureDevOps", "", nil)
	require.NoError(t, err)
	assert.Equal(t, "AzureDevOps", conn.ProviderType)
}
