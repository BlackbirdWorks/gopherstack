package cognitoidp_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
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
