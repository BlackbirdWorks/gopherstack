package grafana_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	grafanasdk "github.com/aws/aws-sdk-go-v2/service/grafana"
	"github.com/aws/aws-sdk-go-v2/service/grafana/types"
	"github.com/stretchr/testify/require"
)

func TestUpdateAndListPermissions(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	id := createActiveWorkspace(t, client, minimalCreateWorkspaceInput())

	userID := "10a20b30-4c5d-4e6f-8a9b-0c1d2e3f4a5b"

	upd, err := client.UpdatePermissions(t.Context(), &grafanasdk.UpdatePermissionsInput{
		WorkspaceId: aws.String(id),
		UpdateInstructionBatch: []types.UpdateInstruction{
			{
				Action: types.UpdateActionAdd,
				Role:   types.RoleAdmin,
				Users:  []types.User{{Id: aws.String(userID), Type: types.UserTypeSsoUser}},
			},
		},
	})
	require.NoError(t, err)
	require.Empty(t, upd.Errors)

	list, err := client.ListPermissions(t.Context(), &grafanasdk.ListPermissionsInput{WorkspaceId: aws.String(id)})
	require.NoError(t, err)
	require.Len(t, list.Permissions, 1)
	require.Equal(t, types.RoleAdmin, list.Permissions[0].Role)
	require.Equal(t, userID, aws.ToString(list.Permissions[0].User.Id))
	require.Equal(t, types.UserTypeSsoUser, list.Permissions[0].User.Type)

	// Filter by userId.
	filtered, err := client.ListPermissions(t.Context(), &grafanasdk.ListPermissionsInput{
		WorkspaceId: aws.String(id),
		UserId:      aws.String(userID),
	})
	require.NoError(t, err)
	require.Len(t, filtered.Permissions, 1)

	// REVOKE removes the grant.
	_, err = client.UpdatePermissions(t.Context(), &grafanasdk.UpdatePermissionsInput{
		WorkspaceId: aws.String(id),
		UpdateInstructionBatch: []types.UpdateInstruction{
			{
				Action: types.UpdateActionRevoke,
				Role:   types.RoleAdmin,
				Users:  []types.User{{Id: aws.String(userID), Type: types.UserTypeSsoUser}},
			},
		},
	})
	require.NoError(t, err)

	after, err := client.ListPermissions(t.Context(), &grafanasdk.ListPermissionsInput{WorkspaceId: aws.String(id)})
	require.NoError(t, err)
	require.Empty(t, after.Permissions)
}

func TestUpdatePermissions_PartialFailure(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	id := createActiveWorkspace(t, client, minimalCreateWorkspaceInput())

	goodUserID := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

	out, err := client.UpdatePermissions(t.Context(), &grafanasdk.UpdatePermissionsInput{
		WorkspaceId: aws.String(id),
		UpdateInstructionBatch: []types.UpdateInstruction{
			{
				Action: types.UpdateActionAdd,
				Role:   types.RoleEditor,
				Users:  []types.User{{Id: aws.String(goodUserID), Type: types.UserTypeSsoUser}},
			},
			{
				// Malformed: no users at all.
				Action: types.UpdateActionAdd,
				Role:   types.RoleViewer,
				Users:  []types.User{},
			},
		},
	})
	require.NoError(t, err, "the batch call itself succeeds even though one instruction failed")
	require.Len(t, out.Errors, 1, "UpdatePermissionsOutput.Errors is a partial-failure surface, not all-or-nothing")
	require.NotNil(t, out.Errors[0].CausedBy)
	require.Equal(t, types.RoleViewer, out.Errors[0].CausedBy.Role)

	list, err := client.ListPermissions(t.Context(), &grafanasdk.ListPermissionsInput{WorkspaceId: aws.String(id)})
	require.NoError(t, err)
	require.Len(t, list.Permissions, 1, "the well-formed instruction in the same batch must still have applied")
	require.Equal(t, types.RoleEditor, list.Permissions[0].Role)
}
