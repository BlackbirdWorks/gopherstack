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

// TestListPermissions_FiltersExcludeNonMatching seeds a user, a second user,
// and a group so that each filter has a wrong answer to return: a test with
// only one grant on the workspace cannot tell "filtered correctly" apart
// from "returned everything".
func TestListPermissions_FiltersExcludeNonMatching(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	id := createActiveWorkspace(t, client, minimalCreateWorkspaceInput())

	const (
		userA  = "10a20b30-4c5d-4e6f-8a9b-0c1d2e3f4a5b"
		userB  = "20a20b30-4c5d-4e6f-8a9b-0c1d2e3f4a5c"
		groupG = "30a20b30-4c5d-4e6f-8a9b-0c1d2e3f4a5d"
	)

	upd, err := client.UpdatePermissions(t.Context(), &grafanasdk.UpdatePermissionsInput{
		WorkspaceId: aws.String(id),
		UpdateInstructionBatch: []types.UpdateInstruction{
			{
				Action: types.UpdateActionAdd, Role: types.RoleAdmin,
				Users: []types.User{{Id: aws.String(userA), Type: types.UserTypeSsoUser}},
			},
			{
				Action: types.UpdateActionAdd, Role: types.RoleEditor,
				Users: []types.User{{Id: aws.String(userB), Type: types.UserTypeSsoUser}},
			},
			{
				Action: types.UpdateActionAdd, Role: types.RoleAdmin,
				Users: []types.User{{Id: aws.String(groupG), Type: types.UserTypeSsoGroup}},
			},
		},
	})
	require.NoError(t, err)
	require.Empty(t, upd.Errors)

	byUser, err := client.ListPermissions(t.Context(), &grafanasdk.ListPermissionsInput{
		WorkspaceId: aws.String(id), UserId: aws.String(userA),
	})
	require.NoError(t, err)
	require.Len(t, byUser.Permissions, 1, "userId filter must exclude userB and groupG")
	require.Equal(t, userA, aws.ToString(byUser.Permissions[0].User.Id))

	byGroup, err := client.ListPermissions(t.Context(), &grafanasdk.ListPermissionsInput{
		WorkspaceId: aws.String(id), GroupId: aws.String(groupG),
	})
	require.NoError(t, err)
	require.Len(t, byGroup.Permissions, 1, "groupId filter must exclude userA and userB")
	require.Equal(t, groupG, aws.ToString(byGroup.Permissions[0].User.Id))
	require.Equal(t, types.UserTypeSsoGroup, byGroup.Permissions[0].User.Type)

	byGroupType, err := client.ListPermissions(t.Context(), &grafanasdk.ListPermissionsInput{
		WorkspaceId: aws.String(id), UserType: types.UserTypeSsoGroup,
	})
	require.NoError(t, err)
	require.Len(t, byGroupType.Permissions, 1, "userType=SSO_GROUP must exclude both SSO_USER grants")

	byUserType, err := client.ListPermissions(t.Context(), &grafanasdk.ListPermissionsInput{
		WorkspaceId: aws.String(id), UserType: types.UserTypeSsoUser,
	})
	require.NoError(t, err)
	require.Len(t, byUserType.Permissions, 2, "userType=SSO_USER must exclude the SSO_GROUP grant")
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
