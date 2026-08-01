package grafana_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	grafanasdk "github.com/aws/aws-sdk-go-v2/service/grafana"
	"github.com/aws/aws-sdk-go-v2/service/grafana/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/grafana"
)

// TestPersistence_SnapshotRestoreRoundTrip proves Handler.Snapshot/Restore
// (persistence.go) round-trips every collection this backend owns,
// including a workspace's tags.Tags field, which deserializes with a
// generic Prometheus lock name after a JSON round-trip and must be rebuilt
// under its real per-workspace name (see restoreWorkspaceTagsLocked) -- plus
// its child API key, service account, token, and permission grant.
func TestPersistence_SnapshotRestoreRoundTrip(t *testing.T) {
	t.Parallel()

	h, client := newTestHandlerAndClient(t)

	in := minimalCreateWorkspaceInput()
	in.Tags = map[string]string{"env": "staging"}

	created, err := client.CreateWorkspace(t.Context(), in)
	require.NoError(t, err)

	id := aws.ToString(created.Workspace.Id)

	_, err = client.CreateWorkspaceApiKey(t.Context(), &grafanasdk.CreateWorkspaceApiKeyInput{
		WorkspaceId:   aws.String(id),
		KeyName:       aws.String("persisted-key"),
		KeyRole:       aws.String("VIEWER"),
		SecondsToLive: aws.Int32(3600),
	})
	require.NoError(t, err)

	sa, err := client.CreateWorkspaceServiceAccount(t.Context(), &grafanasdk.CreateWorkspaceServiceAccountInput{
		WorkspaceId: aws.String(id),
		Name:        aws.String("persisted-sa"),
		GrafanaRole: types.RoleEditor,
	})
	require.NoError(t, err)

	_, err = client.CreateWorkspaceServiceAccountToken(t.Context(), &grafanasdk.CreateWorkspaceServiceAccountTokenInput{
		WorkspaceId:      aws.String(id),
		ServiceAccountId: sa.Id,
		Name:             aws.String("persisted-token"),
		SecondsToLive:    aws.Int32(3600),
	})
	require.NoError(t, err)

	const grantedUserID = "11111111-2222-3333-4444-555555555555"

	_, err = client.UpdatePermissions(t.Context(), &grafanasdk.UpdatePermissionsInput{
		WorkspaceId: aws.String(id),
		UpdateInstructionBatch: []types.UpdateInstruction{
			{
				Action: types.UpdateActionAdd,
				Role:   types.RoleAdmin,
				Users:  []types.User{{Id: aws.String(grantedUserID), Type: types.UserTypeSsoUser}},
			},
		},
	})
	require.NoError(t, err)

	// Snapshot the populated backend, then restore the bytes into a
	// completely fresh backend -- only the snapshot carries the state across.
	snapshot := h.Snapshot(t.Context())
	require.NotEmpty(t, snapshot)

	restoredBackend := grafana.NewInMemoryBackend(t.Context(), "000000000000", rtTestRegion)
	t.Cleanup(restoredBackend.Close)
	require.NoError(t, restoredBackend.Restore(t.Context(), snapshot))

	restoredHandler := grafana.NewHandler(restoredBackend)
	restoredClient := newRoundTripClient(t, restoredHandler)

	desc, err := restoredClient.DescribeWorkspace(t.Context(), &grafanasdk.DescribeWorkspaceInput{
		WorkspaceId: aws.String(id),
	})
	require.NoError(t, err)
	require.Equal(t, "staging", desc.Workspace.Tags["env"])

	// Prove the restored workspace's tags.Tags is genuinely usable post-
	// restore (not just a JSON echo) by mutating it through the native API.
	resourceArn := restoredHandler.Backend.WorkspaceARN(id)
	_, err = restoredClient.TagResource(t.Context(), &grafanasdk.TagResourceInput{
		ResourceArn: aws.String(resourceArn),
		Tags:        map[string]string{"post-restore": "true"},
	})
	require.NoError(t, err)

	keys, err := restoredClient.ListWorkspaceServiceAccounts(t.Context(), &grafanasdk.ListWorkspaceServiceAccountsInput{
		WorkspaceId: aws.String(id),
	})
	require.NoError(t, err)
	require.Len(t, keys.ServiceAccounts, 1)
	require.Equal(t, "persisted-sa", aws.ToString(keys.ServiceAccounts[0].Name))

	tokens, err := restoredClient.ListWorkspaceServiceAccountTokens(
		t.Context(),
		&grafanasdk.ListWorkspaceServiceAccountTokensInput{
			WorkspaceId:      aws.String(id),
			ServiceAccountId: keys.ServiceAccounts[0].Id,
		},
	)
	require.NoError(t, err)
	require.Len(t, tokens.ServiceAccountTokens, 1)
	require.Equal(t, "persisted-token", aws.ToString(tokens.ServiceAccountTokens[0].Name))

	perms, err := restoredClient.ListPermissions(t.Context(), &grafanasdk.ListPermissionsInput{
		WorkspaceId: aws.String(id),
	})
	require.NoError(t, err)
	require.Len(t, perms.Permissions, 1)
	require.Equal(t, grantedUserID, aws.ToString(perms.Permissions[0].User.Id))
}
