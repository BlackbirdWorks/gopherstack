package quicksight_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/quicksight"
)

// TestQuickSight_DeleteUser_RemovesGroupMemberships verifies that deleting a
// user (via DeleteUser or DeleteUserByPrincipalId) removes it from every
// group it belonged to. Without this cleanup, ListGroupMemberships keeps
// surfacing a deleted user as a live member indefinitely -- a state leak,
// since group-membership rows for a since-deleted user can never be reached
// again through DeleteGroupMembership once the user resource itself is gone.
func TestQuickSight_DeleteUser_RemovesGroupMemberships(t *testing.T) {
	t.Parallel()

	tests := []struct {
		deleteUser func(t *testing.T, h *quicksight.Handler, userName, principalID string)
		name       string
	}{
		{
			name: "DeleteUser cleans up memberships",
			deleteUser: func(t *testing.T, h *quicksight.Handler, userName, _ string) {
				t.Helper()
				rec := doRequest(
					t,
					h,
					http.MethodDelete,
					accountPath("/namespaces/"+testNamespace+"/users/"+userName),
					nil,
				)
				require.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "DeleteUserByPrincipalId cleans up memberships",
			deleteUser: func(t *testing.T, h *quicksight.Handler, _, principalID string) {
				t.Helper()
				rec := doRequest(
					t,
					h,
					http.MethodDelete,
					accountPath("/namespaces/"+testNamespace+"/user-principals/"+principalID),
					nil,
				)
				require.Equal(t, http.StatusOK, rec.Code)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			const groupName = "g1"
			const userName = "u1"

			doRequest(t, h, http.MethodPost, accountPath("/namespaces/"+testNamespace+"/groups"), map[string]any{
				"GroupName": groupName,
			})
			regRec := doRequest(
				t,
				h,
				http.MethodPost,
				accountPath("/namespaces/"+testNamespace+"/users"),
				map[string]any{"UserName": userName, "Email": "u1@example.com", "IdentityType": "QUICKSIGHT"},
			)
			require.Equal(t, http.StatusOK, regRec.Code)
			user, ok := parseBody(t, regRec)["User"].(map[string]any)
			require.True(t, ok)
			principalID, _ := user["PrincipalId"].(string)
			require.NotEmpty(t, principalID)

			memRec := doRequest(
				t,
				h,
				http.MethodPut,
				accountPath("/namespaces/"+testNamespace+"/groups/"+groupName+"/members/"+userName),
				nil,
			)
			require.Equal(t, http.StatusOK, memRec.Code)

			tc.deleteUser(t, h, userName, principalID)

			listRec := doRequest(
				t,
				h,
				http.MethodGet,
				accountPath("/namespaces/"+testNamespace+"/groups/"+groupName+"/members"),
				nil,
			)
			require.Equal(t, http.StatusOK, listRec.Code)
			members, ok := parseBody(t, listRec)["GroupMemberList"].([]any)
			require.True(t, ok)
			assert.Empty(t, members, "deleted user must not remain a group member")
		})
	}
}
