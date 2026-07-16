package cognitoidp_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIDToken_CognitoGroups(t *testing.T) {
	t.Parallel()

	b, pool, client := setupTestPoolAndClient(t)

	tokens := signUpConfirmAndLogin(t, b, client.ClientID, "alice")

	_, err := b.CreateGroup(pool.ID, "admins", "", 1)
	require.NoError(t, err)

	result2, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "alice", "Pass1234!")
	require.NoError(t, err)
	_ = result2

	err = b.AdminAddUserToGroup(pool.ID, "alice", "admins")
	require.NoError(t, err)

	result3, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "alice", "Pass1234!")
	require.NoError(t, err)
	_ = tokens

	idClaims := decodeJWTPayload(t, result3.Tokens.IDToken)
	groups, ok := idClaims["cognito:groups"].([]any)
	require.True(t, ok, "cognito:groups must be present in ID token after group membership")
	require.Len(t, groups, 1)
	assert.Equal(t, "admins", groups[0])
}

func TestIDToken_NoCognitoGroupsWhenNone(t *testing.T) {
	t.Parallel()

	b, _, client := setupTestPoolAndClient(t)
	tokens := signUpConfirmAndLogin(t, b, client.ClientID, "bob")

	idClaims := decodeJWTPayload(t, tokens.IDToken)
	_, hasClaims := idClaims["cognito:groups"]
	assert.False(t, hasClaims, "cognito:groups must be absent when user has no groups")
}

func TestGroups(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := setupHandlerPoolAndClient(t, h, "groups-pool")

	// Create group.
	rec := doCognitoRequest(t, h, "CreateGroup", map[string]any{
		"UserPoolId":  poolID,
		"GroupName":   "admins",
		"Description": "Admin group",
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	// Create user + confirm.
	signUpRec := doCognitoRequest(t, h, "SignUp", map[string]any{
		"ClientId": clientID,
		"Username": "groupuser",
		"Password": "Passw0rd!",
	})
	require.Equal(t, http.StatusOK, signUpRec.Code)

	confRec := doCognitoRequest(t, h, "AdminConfirmSignUp", map[string]any{
		"UserPoolId": poolID,
		"Username":   "groupuser",
	})
	require.Equal(t, http.StatusOK, confRec.Code)

	// Add user to group.
	addRec := doCognitoRequest(t, h, "AdminAddUserToGroup", map[string]any{
		"UserPoolId": poolID,
		"Username":   "groupuser",
		"GroupName":  "admins",
	})
	require.Equal(t, http.StatusOK, addRec.Code, addRec.Body.String())

	// Auth — token should contain cognito:groups.
	authResp := initiateAuth(t, h, clientID, "groupuser")
	authResult, ok := authResp["AuthenticationResult"].(map[string]any)
	require.True(t, ok)

	accessClaims := jwtClaims(t, authResult["AccessToken"].(string))
	groups, ok := accessClaims["cognito:groups"].([]any)
	require.True(t, ok, "expected cognito:groups claim")
	require.Len(t, groups, 1)
	assert.Equal(t, "admins", groups[0])
}

func TestInMemoryBackend_GetGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget error
		setup     func(b *cognitoidp.InMemoryBackend) (string, string)
		name      string
		wantErr   bool
	}{
		{
			name: "success",
			setup: func(b *cognitoidp.InMemoryBackend) (string, string) {
				p, _ := b.CreateUserPool("pool")
				g, _ := b.CreateGroup(p.ID, "admins", "admin group", 0)

				return p.ID, g.GroupName
			},
		},
		{
			name: "group_not_found",
			setup: func(b *cognitoidp.InMemoryBackend) (string, string) {
				p, _ := b.CreateUserPool("pool")

				return p.ID, "nonexistent"
			},
			wantErr:   true,
			errTarget: cognitoidp.ErrGroupNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			poolID, groupName := tt.setup(b)

			g, err := b.GetGroup(poolID, groupName)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, groupName, g.GroupName)
		})
	}
}

func TestGroup_LastModifiedDate_Present(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		op   string
	}{
		{name: "create_group", op: "CreateGroup"},
		{name: "get_group", op: "GetGroup"},
		{name: "update_group", op: "UpdateGroup"},
		{name: "list_groups", op: "ListGroups"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID, _ := setupHandlerPoolAndClient(t, h, "group-lmd-"+tt.name+"-pool")

			createRec := doCognitoRequest(t, h, "CreateGroup", map[string]any{
				"UserPoolId":  poolID,
				"GroupName":   "admins",
				"Description": "Admin group",
				"Precedence":  1,
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			switch tt.op {
			case "CreateGroup":
				var out struct {
					Group *struct {
						LastModifiedDate float64 `json:"LastModifiedDate,omitempty"`
					} `json:"Group"`
				}
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &out))
				require.NotNil(t, out.Group)
				assert.Greater(t, out.Group.LastModifiedDate, float64(0), "CreateGroup must include LastModifiedDate")

			case "GetGroup":
				rec := doCognitoRequest(t, h, "GetGroup", map[string]any{
					"UserPoolId": poolID,
					"GroupName":  "admins",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var out struct {
					Group *struct {
						LastModifiedDate float64 `json:"LastModifiedDate,omitempty"`
					} `json:"Group"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				require.NotNil(t, out.Group)
				assert.Greater(t, out.Group.LastModifiedDate, float64(0), "GetGroup must include LastModifiedDate")

			case "UpdateGroup":
				rec := doCognitoRequest(t, h, "UpdateGroup", map[string]any{
					"UserPoolId":  poolID,
					"GroupName":   "admins",
					"Description": "Updated admin group",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var out struct {
					Group *struct {
						LastModifiedDate float64 `json:"LastModifiedDate,omitempty"`
					} `json:"Group"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				require.NotNil(t, out.Group)
				assert.Greater(t, out.Group.LastModifiedDate, float64(0), "UpdateGroup must include LastModifiedDate")

			case "ListGroups":
				rec := doCognitoRequest(t, h, "ListGroups", map[string]any{
					"UserPoolId": poolID,
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var out struct {
					Groups []struct {
						GroupName        string  `json:"GroupName,omitempty"`
						LastModifiedDate float64 `json:"LastModifiedDate,omitempty"`
					} `json:"Groups"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				require.Len(t, out.Groups, 1)
				assert.Greater(
					t,
					out.Groups[0].LastModifiedDate,
					float64(0),
					"ListGroups must include LastModifiedDate",
				)
			}
		})
	}
}

func TestGroup_Create_WithRoleArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "group-role-pool")

	roleArn := "arn:aws:iam::123456789:role/CognitoAdminRole"

	rec := doCognitoRequest(t, h, "CreateGroup", map[string]any{
		"UserPoolId":  poolID,
		"GroupName":   "admins",
		"Description": "Admin users",
		"RoleArn":     roleArn,
		"Precedence":  int32(1),
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Group *struct {
			GroupName    string  `json:"GroupName,omitempty"`
			RoleArn      string  `json:"RoleArn,omitempty"`
			Precedence   int32   `json:"Precedence,omitempty"`
			CreationDate float64 `json:"CreationDate,omitempty"`
		} `json:"Group"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.NotNil(t, out.Group)
	assert.Equal(t, "admins", out.Group.GroupName)
	assert.Equal(t, roleArn, out.Group.RoleArn)
	assert.Equal(t, int32(1), out.Group.Precedence)
	assert.Greater(t, out.Group.CreationDate, float64(0))
}

func TestGroup_Update_WithRoleArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "group-upd-role-pool")

	// Create without role.
	rec := doCognitoRequest(t, h, "CreateGroup", map[string]any{
		"UserPoolId": poolID,
		"GroupName":  "editors",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Update with role.
	roleArn := "arn:aws:iam::123:role/EditorRole"
	rec = doCognitoRequest(t, h, "UpdateGroup", map[string]any{
		"UserPoolId": poolID,
		"GroupName":  "editors",
		"RoleArn":    roleArn,
		"Precedence": int32(10),
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Group *struct {
			RoleArn    string `json:"RoleArn,omitempty"`
			Precedence int32  `json:"Precedence,omitempty"`
		} `json:"Group"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.NotNil(t, out.Group)
	assert.Equal(t, roleArn, out.Group.RoleArn)
	assert.Equal(t, int32(10), out.Group.Precedence)
}

func TestGroup_GetGroup_WithRoleArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "group-get-role-pool")

	roleArn := "arn:aws:iam::123:role/MyRole"
	rec := doCognitoRequest(t, h, "CreateGroup", map[string]any{
		"UserPoolId": poolID,
		"GroupName":  "viewers",
		"RoleArn":    roleArn,
		"Precedence": int32(5),
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doCognitoRequest(t, h, "GetGroup", map[string]any{
		"UserPoolId": poolID,
		"GroupName":  "viewers",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Group *struct {
			GroupName  string `json:"GroupName,omitempty"`
			RoleArn    string `json:"RoleArn,omitempty"`
			Precedence int32  `json:"Precedence,omitempty"`
		} `json:"Group"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.NotNil(t, out.Group)
	assert.Equal(t, "viewers", out.Group.GroupName)
	assert.Equal(t, roleArn, out.Group.RoleArn)
}

func TestGroup_ListGroups_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "group-page-pool")

	// Create 5 groups.
	for i := range 5 {
		rec := doCognitoRequest(t, h, "CreateGroup", map[string]any{
			"UserPoolId": poolID,
			"GroupName":  fmt.Sprintf("group-%02d", i),
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Page 1 — 3 items.
	rec := doCognitoRequest(t, h, "ListGroups", map[string]any{
		"UserPoolId": poolID,
		"Limit":      3,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var page1 struct {
		NextToken string `json:"NextToken,omitempty"`
		Groups    []struct {
			GroupName string `json:"GroupName,omitempty"`
		} `json:"Groups"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page1))
	assert.Len(t, page1.Groups, 3)
	assert.NotEmpty(t, page1.NextToken)

	// Page 2 — remaining items.
	rec = doCognitoRequest(t, h, "ListGroups", map[string]any{
		"UserPoolId": poolID,
		"Limit":      3,
		"NextToken":  page1.NextToken,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var page2 struct {
		NextToken string `json:"NextToken,omitempty"`
		Groups    []struct {
			GroupName string `json:"GroupName,omitempty"`
		} `json:"Groups"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page2))
	assert.Len(t, page2.Groups, 2)
	assert.Empty(t, page2.NextToken)
}

func TestGroup_ListUsersInGroup_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := setupHandlerPoolAndClient(t, h, "group-users-page-pool")

	rec := doCognitoRequest(t, h, "CreateGroup", map[string]any{
		"UserPoolId": poolID,
		"GroupName":  "members",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Create and add 4 users to group.
	for i := range 4 {
		username := fmt.Sprintf("member-%02d", i)
		signUpAndConfirmViaHandler(t, h, clientID, username)
		addRec := doCognitoRequest(t, h, "AdminAddUserToGroup", map[string]any{
			"UserPoolId": poolID,
			"Username":   username,
			"GroupName":  "members",
		})
		require.Equal(t, http.StatusOK, addRec.Code)
	}

	// Page 1 — 2 users.
	rec = doCognitoRequest(t, h, "ListUsersInGroup", map[string]any{
		"UserPoolId": poolID,
		"GroupName":  "members",
		"Limit":      2,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var page1 struct {
		NextToken string `json:"NextToken,omitempty"`
		Users     []struct {
			Username string `json:"Username,omitempty"`
		} `json:"Users"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page1))
	assert.Len(t, page1.Users, 2)
	assert.NotEmpty(t, page1.NextToken)

	// Page 2.
	rec = doCognitoRequest(t, h, "ListUsersInGroup", map[string]any{
		"UserPoolId": poolID,
		"GroupName":  "members",
		"Limit":      2,
		"NextToken":  page1.NextToken,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var page2 struct {
		NextToken string `json:"NextToken,omitempty"`
		Users     []struct {
			Username string `json:"Username,omitempty"`
		} `json:"Users"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page2))
	assert.Len(t, page2.Users, 2)
	assert.Empty(t, page2.NextToken)
}

func TestGroup_Backend_Direct(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPool("group-backend-pool")
	require.NoError(t, err)

	roleArn := "arn:aws:iam::123:role/TestRole"
	g, err := b.CreateGroupFull(pool.ID, "testers", "Test users", roleArn, 5)
	require.NoError(t, err)
	assert.Equal(t, roleArn, g.RoleArn)
	assert.Equal(t, int32(5), g.Precedence)

	// Duplicate should fail.
	_, err = b.CreateGroupFull(pool.ID, "testers", "", "", 0)
	require.Error(t, err)

	// Update.
	newRole := "arn:aws:iam::123:role/UpdatedRole"
	updated, err := b.UpdateGroupFull(pool.ID, "testers", "Updated desc", newRole, 10)
	require.NoError(t, err)
	assert.Equal(t, newRole, updated.RoleArn)
	assert.Equal(t, "Updated desc", updated.Description)
}

func TestGroup_ListGroupsPage_Backend(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPool("group-page-backend-pool")
	require.NoError(t, err)

	for i := range 6 {
		_, err = b.CreateGroupFull(pool.ID, fmt.Sprintf("grp-%02d", i), "", "", int32(i))
		require.NoError(t, err)
	}

	// First page.
	page1, tok1, err := b.ListGroupsPage(pool.ID, 3, "")
	require.NoError(t, err)
	assert.Len(t, page1, 3)
	assert.NotEmpty(t, tok1)

	// Second page.
	page2, tok2, err := b.ListGroupsPage(pool.ID, 3, tok1)
	require.NoError(t, err)
	assert.Len(t, page2, 3)
	assert.Empty(t, tok2)

	// All groups — no limit.
	all, tok3, err := b.ListGroupsPage(pool.ID, 0, "")
	require.NoError(t, err)
	assert.Len(t, all, 6)
	assert.Empty(t, tok3)
}

// TestHandler_UpdateGroup_Via_HTTP covers the HTTP handler for UpdateGroup.
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
