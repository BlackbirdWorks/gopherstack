package grafana_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	grafanasdk "github.com/aws/aws-sdk-go-v2/service/grafana"
	"github.com/aws/aws-sdk-go-v2/service/grafana/types"
	"github.com/stretchr/testify/require"
)

// workspaceTransitionWait is long enough for the backend's 100ms simulated
// CREATING/UPDATING/etc. -> ACTIVE transition to have fired.
const workspaceTransitionWait = 200 * time.Millisecond

// waitForWorkspaceActive polls DescribeWorkspace until the workspace reaches
// ACTIVE. These tests drive the backend over a real httptest.Server (a real
// loopback HTTP connection), so the production transition timer fires from a
// goroutine outside any test-local synctest bubble -- synctest can't durably
// block on real network I/O, so this can't be converted to a fake clock and
// falls back to require.Eventually instead.
func waitForWorkspaceActive(t *testing.T, client *grafanasdk.Client, id string) *grafanasdk.DescribeWorkspaceOutput {
	t.Helper()

	var desc *grafanasdk.DescribeWorkspaceOutput

	require.Eventually(t, func() bool {
		out, err := client.DescribeWorkspace(
			t.Context(), &grafanasdk.DescribeWorkspaceInput{WorkspaceId: aws.String(id)},
		)
		if err != nil || out.Workspace.Status != types.WorkspaceStatusActive {
			return false
		}
		desc = out

		return true
	}, workspaceTransitionWait*10, 10*time.Millisecond, "workspace never reached ACTIVE")

	return desc
}

func minimalCreateWorkspaceInput() *grafanasdk.CreateWorkspaceInput {
	return &grafanasdk.CreateWorkspaceInput{
		AccountAccessType:       types.AccountAccessTypeCurrentAccount,
		AuthenticationProviders: []types.AuthenticationProviderTypes{types.AuthenticationProviderTypesAwsSso},
		PermissionType:          types.PermissionTypeCustomerManaged,
	}
}

func TestCreateWorkspace_Lifecycle(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)

	out, err := client.CreateWorkspace(t.Context(), minimalCreateWorkspaceInput())
	require.NoError(t, err)
	require.NotNil(t, out.Workspace)
	require.Equal(t, types.WorkspaceStatusCreating, out.Workspace.Status)
	require.NotEmpty(t, aws.ToString(out.Workspace.Id))
	require.Contains(t, aws.ToString(out.Workspace.Endpoint), aws.ToString(out.Workspace.Id))
	require.False(t, out.Workspace.Created.IsZero())
	require.Equal(t, types.AccountAccessTypeCurrentAccount, out.Workspace.AccountAccessType)
	require.Equal(t, types.PermissionTypeCustomerManaged, out.Workspace.PermissionType)
	require.NotEmpty(t, out.Workspace.GrafanaVersion, "defaults to latest supported version")

	id := aws.ToString(out.Workspace.Id)

	desc := waitForWorkspaceActive(t, client, id)
	require.Equal(t, id, aws.ToString(desc.Workspace.Id))
}

func TestCreateWorkspace_Validation(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)

	tests := []struct {
		mutate func(*grafanasdk.CreateWorkspaceInput)
		name   string
	}{
		{
			name: "invalid accountAccessType",
			mutate: func(in *grafanasdk.CreateWorkspaceInput) {
				in.AccountAccessType = "BOGUS"
			},
		},
		{
			name: "invalid authenticationProviders entry",
			mutate: func(in *grafanasdk.CreateWorkspaceInput) {
				in.AuthenticationProviders = []types.AuthenticationProviderTypes{"BOGUS_PROVIDER"}
			},
		},
		{
			name: "ORGANIZATION without organizational units",
			mutate: func(in *grafanasdk.CreateWorkspaceInput) {
				in.AccountAccessType = types.AccountAccessTypeOrganization
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			in := minimalCreateWorkspaceInput()
			tc.mutate(in)

			_, err := client.CreateWorkspace(t.Context(), in)
			require.Error(t, err)

			var ve *types.ValidationException
			require.ErrorAs(t, err, &ve, "expected a real ValidationException from the SDK deserializer")
		})
	}
}

func TestDescribeWorkspace_NotFound(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)

	_, err := client.DescribeWorkspace(t.Context(), &grafanasdk.DescribeWorkspaceInput{
		WorkspaceId: aws.String("g-does-not-exist"),
	})
	require.Error(t, err)

	var nfe *types.ResourceNotFoundException
	require.ErrorAs(t, err, &nfe)
	require.Equal(t, "g-does-not-exist", aws.ToString(nfe.ResourceId))
	require.Equal(t, "workspace", aws.ToString(nfe.ResourceType))
}

func TestUpdateWorkspace_MergesNotOverwrites(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)

	in := minimalCreateWorkspaceInput()
	in.WorkspaceName = aws.String("original-name")
	in.WorkspaceDescription = aws.String("original description")

	created, err := client.CreateWorkspace(t.Context(), in)
	require.NoError(t, err)

	id := aws.ToString(created.Workspace.Id)
	waitForWorkspaceActive(t, client, id)

	updated, err := client.UpdateWorkspace(t.Context(), &grafanasdk.UpdateWorkspaceInput{
		WorkspaceId:          aws.String(id),
		WorkspaceDescription: aws.String("new description"),
	})
	require.NoError(t, err)
	require.Equal(t, "new description", aws.ToString(updated.Workspace.Description))
	require.Equal(t, "original-name", aws.ToString(updated.Workspace.Name),
		"omitted WorkspaceName must be left unchanged, not overwritten with empty")
	require.Equal(t, types.WorkspaceStatusUpdating, updated.Workspace.Status)

	waitForWorkspaceActive(t, client, id)
}

func TestUpdateWorkspace_NetworkAccessRemoveAndSetConflict(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)

	created, err := client.CreateWorkspace(t.Context(), minimalCreateWorkspaceInput())
	require.NoError(t, err)

	id := aws.ToString(created.Workspace.Id)
	waitForWorkspaceActive(t, client, id)

	_, err = client.UpdateWorkspace(t.Context(), &grafanasdk.UpdateWorkspaceInput{
		WorkspaceId:                      aws.String(id),
		RemoveNetworkAccessConfiguration: aws.Bool(true),
		NetworkAccessControl: &types.NetworkAccessConfiguration{
			PrefixListIds: []string{"pl-1a2b3c4d"},
			VpceIds:       []string{},
		},
	})
	require.Error(t, err)

	var ve *types.ValidationException
	require.ErrorAs(t, err, &ve)
}

func TestUpdateWorkspace_NotActive_Conflict(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)

	created, err := client.CreateWorkspace(t.Context(), minimalCreateWorkspaceInput())
	require.NoError(t, err)

	id := aws.ToString(created.Workspace.Id)
	// Do NOT sleep -- the workspace is still CREATING.

	_, err = client.UpdateWorkspace(t.Context(), &grafanasdk.UpdateWorkspaceInput{
		WorkspaceId:          aws.String(id),
		WorkspaceDescription: aws.String("x"),
	})
	require.Error(t, err)

	var ce *types.ConflictException
	require.ErrorAs(t, err, &ce)
}

func TestDeleteWorkspace_CascadesChildren(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)

	created, err := client.CreateWorkspace(t.Context(), minimalCreateWorkspaceInput())
	require.NoError(t, err)

	id := aws.ToString(created.Workspace.Id)
	waitForWorkspaceActive(t, client, id)

	_, err = client.CreateWorkspaceApiKey(t.Context(), &grafanasdk.CreateWorkspaceApiKeyInput{
		WorkspaceId:   aws.String(id),
		KeyName:       aws.String("k1"),
		KeyRole:       aws.String("VIEWER"),
		SecondsToLive: aws.Int32(3600),
	})
	require.NoError(t, err)

	sa, err := client.CreateWorkspaceServiceAccount(t.Context(), &grafanasdk.CreateWorkspaceServiceAccountInput{
		WorkspaceId: aws.String(id),
		Name:        aws.String("sa1"),
		GrafanaRole: types.RoleViewer,
	})
	require.NoError(t, err)

	_, err = client.CreateWorkspaceServiceAccountToken(t.Context(), &grafanasdk.CreateWorkspaceServiceAccountTokenInput{
		WorkspaceId:      aws.String(id),
		ServiceAccountId: sa.Id,
		Name:             aws.String("tok1"),
		SecondsToLive:    aws.Int32(3600),
	})
	require.NoError(t, err)

	del, err := client.DeleteWorkspace(t.Context(), &grafanasdk.DeleteWorkspaceInput{WorkspaceId: aws.String(id)})
	require.NoError(t, err)
	require.Equal(t, types.WorkspaceStatusDeleting, del.Workspace.Status)

	_, err = client.DescribeWorkspace(t.Context(), &grafanasdk.DescribeWorkspaceInput{WorkspaceId: aws.String(id)})
	require.Error(t, err)

	var nfe *types.ResourceNotFoundException
	require.ErrorAs(t, err, &nfe)

	// The service account and its token must have cascaded away too --
	// listing them (against the now-deleted workspace) 404s.
	_, err = client.ListWorkspaceServiceAccounts(t.Context(), &grafanasdk.ListWorkspaceServiceAccountsInput{
		WorkspaceId: aws.String(id),
	})
	require.Error(t, err)
	require.ErrorAs(t, err, &nfe)
}

func TestListWorkspaces_Pagination(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)

	const numWorkspaces = 3

	for range numWorkspaces {
		_, err := client.CreateWorkspace(t.Context(), minimalCreateWorkspaceInput())
		require.NoError(t, err)
	}

	page1, err := client.ListWorkspaces(t.Context(), &grafanasdk.ListWorkspacesInput{MaxResults: aws.Int32(2)})
	require.NoError(t, err)
	require.Len(t, page1.Workspaces, 2)
	require.NotNil(t, page1.NextToken)

	page2, err := client.ListWorkspaces(t.Context(), &grafanasdk.ListWorkspacesInput{
		MaxResults: aws.Int32(2),
		NextToken:  page1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.Workspaces, 1)
}
