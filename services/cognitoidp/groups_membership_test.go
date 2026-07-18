package cognitoidp_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_UpdateGroup_Via_HTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "success", wantCode: http.StatusOK},
		{name: "group_not_found", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID, _ := setupHandlerPoolAndClient(t, h, "upd-grp-pool")

			groupName := "my-group"

			if tt.name == "success" {
				doCognitoRequest(t, h, "CreateGroup", map[string]any{
					"UserPoolId": poolID,
					"GroupName":  groupName,
				})
			}

			rec := doCognitoRequest(t, h, "UpdateGroup", map[string]any{
				"UserPoolId":  poolID,
				"GroupName":   groupName,
				"Description": "updated",
				"Precedence":  int32(10),
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestHandler_GetGroup covers the HTTP handler for GetGroup.
func TestHandler_GetGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "success", wantCode: http.StatusOK},
		{name: "not_found", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID, _ := setupHandlerPoolAndClient(t, h, "get-grp-pool")

			groupName := "target-group"

			if tt.name == "success" {
				doCognitoRequest(t, h, "CreateGroup", map[string]any{
					"UserPoolId": poolID,
					"GroupName":  groupName,
				})
			}

			rec := doCognitoRequest(t, h, "GetGroup", map[string]any{
				"UserPoolId": poolID,
				"GroupName":  groupName,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestBackend_UpdateGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget    error
		name         string
		wantErr      bool
		poolMissing  bool
		groupMissing bool
	}{
		{name: "success"},
		{
			name:        "pool_not_found",
			wantErr:     true,
			errTarget:   cognitoidp.ErrUserPoolNotFound,
			poolMissing: true,
		},
		{
			name:         "group_not_found",
			wantErr:      true,
			errTarget:    cognitoidp.ErrGroupNotFound,
			groupMissing: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			if tt.poolMissing {
				_, err := b.UpdateGroup("bad-pool", "grp", "desc", 0)
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			pool, err := b.CreateUserPool("grp-pool")
			require.NoError(t, err)

			if tt.groupMissing {
				_, err = b.UpdateGroup(pool.ID, "no-grp", "desc", 0)
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			_, err = b.CreateGroup(pool.ID, "my-group", "original desc", 1)
			require.NoError(t, err)

			updated, err := b.UpdateGroup(pool.ID, "my-group", "new desc", 5)
			require.NoError(t, err)
			require.NotNil(t, updated)
			assert.Equal(t, "new desc", updated.Description)
			assert.Equal(t, int32(5), updated.Precedence)
		})
	}
}

func TestIDToken_IncludesGroups(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPool("groups-pool")
	require.NoError(t, err)

	client, err := b.CreateUserPoolClient(pool.ID, "gc")
	require.NoError(t, err)

	user, err := b.SignUp(client.ClientID, "groups-user", "Pass1234!", map[string]string{})
	require.NoError(t, err)
	require.NoError(t, b.ConfirmSignUp(client.ClientID, "groups-user", user.ConfirmCode))

	// Create a group and add the user.
	_, err = b.CreateGroup(pool.ID, "admins", "admin group", 1)
	require.NoError(t, err)
	require.NoError(t, b.AdminAddUserToGroup(pool.ID, "groups-user", "admins"))

	result, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "groups-user", "Pass1234!")
	require.NoError(t, err)
	require.NotNil(t, result.Tokens)

	// cognito:groups appears in the ID token.
	claims := decodeJWTPayload(t, result.Tokens.IDToken)
	groups, ok := claims["cognito:groups"]
	require.True(t, ok, "cognito:groups should be in ID token when user is in a group")
	_ = groups
}

func TestCognito_ListUsersInGroup(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{
		"PoolName": "group-pool",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, unmarshalBody(t, rec, &resp))
	poolID := resp["UserPool"].(map[string]any)["Id"].(string)

	doCognitoRequest(t, h, "CreateGroup", map[string]any{
		"UserPoolId": poolID,
		"GroupName":  "my-group",
	})

	rec = doCognitoRequest(t, h, "ListUsersInGroup", map[string]any{
		"UserPoolId": poolID,
		"GroupName":  "my-group",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_Groups_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		wantCreateErr bool
		wantDeleteErr bool
	}{
		{
			name:          "create_list_delete_success",
			wantCreateErr: false,
			wantDeleteErr: false,
		},
		{
			name:          "create_duplicate_fails",
			wantCreateErr: true,
			wantDeleteErr: false,
		},
		{
			name:          "delete_nonexistent_fails",
			wantCreateErr: false,
			wantDeleteErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "grp-pool"})
			var poolResp map[string]map[string]any
			_ = json.Unmarshal(poolRec.Body.Bytes(), &poolResp)
			poolID := poolResp["UserPool"]["Id"].(string)

			// Create a group.
			createRec := doCognitoRequest(t, h, "CreateGroup", map[string]any{
				"UserPoolId":  poolID,
				"GroupName":   "admins",
				"Description": "Admin users",
				"Precedence":  int32(1),
			})
			require.Equal(t, http.StatusOK, createRec.Code)
			assert.Contains(t, createRec.Body.String(), "admins")

			if tt.wantCreateErr {
				// Create duplicate group — should fail.
				dupRec := doCognitoRequest(t, h, "CreateGroup", map[string]any{
					"UserPoolId": poolID,
					"GroupName":  "admins",
				})
				assert.Equal(t, http.StatusBadRequest, dupRec.Code)
			}

			// List groups — should contain the created group.
			listRec := doCognitoRequest(t, h, "ListGroups", map[string]any{"UserPoolId": poolID})
			assert.Equal(t, http.StatusOK, listRec.Code)
			assert.Contains(t, listRec.Body.String(), "admins")

			if tt.wantDeleteErr {
				// Delete nonexistent group — should fail.
				delRec := doCognitoRequest(t, h, "DeleteGroup", map[string]any{
					"UserPoolId": poolID,
					"GroupName":  "nonexistent",
				})
				assert.Equal(t, http.StatusBadRequest, delRec.Code)
			} else {
				// Delete the group successfully.
				delRec := doCognitoRequest(t, h, "DeleteGroup", map[string]any{
					"UserPoolId": poolID,
					"GroupName":  "admins",
				})
				assert.Equal(t, http.StatusOK, delRec.Code)

				// List groups — should now be empty.
				listRec2 := doCognitoRequest(t, h, "ListGroups", map[string]any{"UserPoolId": poolID})
				assert.Equal(t, http.StatusOK, listRec2.Code)

				var listResp map[string]any
				require.NoError(t, json.Unmarshal(listRec2.Body.Bytes(), &listResp))
				groups := listResp["Groups"].([]any)
				assert.Empty(t, groups)
			}
		})
	}
}

func TestHandler_AdminGroupMembership(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		errCase  string
		wantCode int
	}{
		{
			name:     "add_remove_list",
			wantCode: http.StatusOK,
		},
		{
			name:     "add_unknown_group",
			wantCode: http.StatusBadRequest,
			errCase:  "unknown_group",
		},
		{
			name:     "add_unknown_user",
			wantCode: http.StatusBadRequest,
			errCase:  "unknown_user",
		},
		{
			name:     "remove_unknown_group",
			wantCode: http.StatusBadRequest,
			errCase:  "remove_unknown_group",
		},
		{
			name:     "list_groups_unknown_user",
			wantCode: http.StatusBadRequest,
			errCase:  "list_unknown_user",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Set up pool, user, and group.
			poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "grp-pool"})
			var poolResp map[string]map[string]any
			_ = json.Unmarshal(poolRec.Body.Bytes(), &poolResp)
			poolID := poolResp["UserPool"]["Id"].(string)

			doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
				"UserPoolId":        poolID,
				"Username":          "grpuser",
				"TemporaryPassword": "Temp123!",
			})

			doCognitoRequest(t, h, "CreateGroup", map[string]any{
				"UserPoolId": poolID,
				"GroupName":  "mygroup",
			})

			switch tt.errCase {
			case "unknown_group":
				rec := doCognitoRequest(t, h, "AdminAddUserToGroup", map[string]any{
					"UserPoolId": poolID,
					"Username":   "grpuser",
					"GroupName":  "nogroup",
				})
				assert.Equal(t, tt.wantCode, rec.Code)
			case "unknown_user":
				rec := doCognitoRequest(t, h, "AdminAddUserToGroup", map[string]any{
					"UserPoolId": poolID,
					"Username":   "nobody",
					"GroupName":  "mygroup",
				})
				assert.Equal(t, tt.wantCode, rec.Code)
			case "remove_unknown_group":
				rec := doCognitoRequest(t, h, "AdminRemoveUserFromGroup", map[string]any{
					"UserPoolId": poolID,
					"Username":   "grpuser",
					"GroupName":  "nogroup",
				})
				assert.Equal(t, tt.wantCode, rec.Code)
			case "list_unknown_user":
				rec := doCognitoRequest(t, h, "AdminListGroupsForUser", map[string]any{
					"UserPoolId": poolID,
					"Username":   "nobody",
				})
				assert.Equal(t, tt.wantCode, rec.Code)
			default:
				// Happy path: add user to group, list groups for user, remove from group.
				addRec := doCognitoRequest(t, h, "AdminAddUserToGroup", map[string]any{
					"UserPoolId": poolID,
					"Username":   "grpuser",
					"GroupName":  "mygroup",
				})
				assert.Equal(t, http.StatusOK, addRec.Code)

				listRec := doCognitoRequest(t, h, "AdminListGroupsForUser", map[string]any{
					"UserPoolId": poolID,
					"Username":   "grpuser",
				})
				assert.Equal(t, http.StatusOK, listRec.Code)
				assert.Contains(t, listRec.Body.String(), "mygroup")

				removeRec := doCognitoRequest(t, h, "AdminRemoveUserFromGroup", map[string]any{
					"UserPoolId": poolID,
					"Username":   "grpuser",
					"GroupName":  "mygroup",
				})
				assert.Equal(t, http.StatusOK, removeRec.Code)

				listRec2 := doCognitoRequest(t, h, "AdminListGroupsForUser", map[string]any{
					"UserPoolId": poolID,
					"Username":   "grpuser",
				})
				assert.Equal(t, http.StatusOK, listRec2.Code)
				var listResp map[string]any
				require.NoError(t, json.Unmarshal(listRec2.Body.Bytes(), &listResp))
				groups := listResp["Groups"].([]any)
				assert.Empty(t, groups)
			}
		})
	}
}

func TestSortedListGroups(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "sorted-groups-pool"})
	var poolResp map[string]any
	require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
	poolID := poolResp["UserPool"].(map[string]any)["Id"].(string)

	for _, name := range []string{"zeta", "admin", "moderator"} {
		doCognitoRequest(t, h, "CreateGroup", map[string]any{
			"UserPoolId": poolID,
			"GroupName":  name,
		})
	}

	rec := doCognitoRequest(t, h, "ListGroups", map[string]any{"UserPoolId": poolID})
	assert.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	groups := listResp["Groups"].([]any)
	require.Len(t, groups, 3)
	assert.Equal(t, "admin", groups[0].(map[string]any)["GroupName"])
	assert.Equal(t, "moderator", groups[1].(map[string]any)["GroupName"])
	assert.Equal(t, "zeta", groups[2].(map[string]any)["GroupName"])
}

func TestSortedAdminListGroupsForUser(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "algfu-pool"})
	var poolResp map[string]any
	require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
	poolID := poolResp["UserPool"].(map[string]any)["Id"].(string)

	for _, g := range []string{"zeta", "alpha", "beta"} {
		doCognitoRequest(t, h, "CreateGroup", map[string]any{
			"UserPoolId": poolID,
			"GroupName":  g,
		})
	}

	doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
		"UserPoolId":        poolID,
		"Username":          "group-user",
		"TemporaryPassword": "TempPass123!",
	})

	for _, g := range []string{"zeta", "alpha", "beta"} {
		doCognitoRequest(t, h, "AdminAddUserToGroup", map[string]any{
			"UserPoolId": poolID,
			"Username":   "group-user",
			"GroupName":  g,
		})
	}

	rec := doCognitoRequest(t, h, "AdminListGroupsForUser", map[string]any{
		"UserPoolId": poolID,
		"Username":   "group-user",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	groups := resp["Groups"].([]any)
	require.Len(t, groups, 3)
	assert.Equal(t, "alpha", groups[0].(map[string]any)["GroupName"])
	assert.Equal(t, "beta", groups[1].(map[string]any)["GroupName"])
	assert.Equal(t, "zeta", groups[2].(map[string]any)["GroupName"])
}

// TestParityB_AccessTokenGroupsClaim verifies that cognito:groups appears in access tokens.
func TestAccessTokenGroupsClaim(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name       string
		addToGroup bool
		wantGroups bool
	}

	tests := []testCase{
		{name: "user_in_group_has_claim", addToGroup: true, wantGroups: true},
		{name: "user_not_in_group_no_claim", addToGroup: false, wantGroups: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID, clientID := setupPoolAndClientNamed(t, h, "grp-pool-"+tt.name, "grp-client-"+tt.name)

			signUpAndAdminConfirm(t, h, clientID, poolID, "grpuser")

			if tt.addToGroup {
				rec := doCognitoRequest(t, h, "CreateGroup", map[string]any{
					"UserPoolId": poolID,
					"GroupName":  "admins",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				rec = doCognitoRequest(t, h, "AdminAddUserToGroup", map[string]any{
					"UserPoolId": poolID,
					"Username":   "grpuser",
					"GroupName":  "admins",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doCognitoRequest(t, h, "AdminInitiateAuth", map[string]any{
				"UserPoolId": poolID,
				"ClientId":   clientID,
				"AuthFlow":   "ADMIN_USER_PASSWORD_AUTH",
				"AuthParameters": map[string]string{
					"USERNAME": "grpuser",
					"PASSWORD": "Pass1234!",
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			authResult := resp["AuthenticationResult"].(map[string]any)
			accessToken := authResult["AccessToken"].(string)

			claims := decodeJWTPayload(t, accessToken)
			_, hasGroups := claims["cognito:groups"]
			assert.Equal(t, tt.wantGroups, hasGroups)
		})
	}
}
