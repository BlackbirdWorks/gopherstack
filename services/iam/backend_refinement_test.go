package iam_test

import (
	"maps"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

// TestBackendRefinement_UpdateAccessKey tests UpdateAccessKey.
func TestBackendRefinement_UpdateAccessKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*iam.InMemoryBackend)
		name       string
		userName   string
		keyID      string
		status     string
		wantErrMsg string
		wantErr    bool
	}{
		{
			name: "set_inactive",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
			},
			userName: "alice",
			status:   "Inactive",
		},
		{
			name: "set_active",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
			},
			userName: "alice",
			status:   "Active",
		},
		{
			name: "invalid_status",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
			},
			userName:   "alice",
			status:     "Suspended",
			wantErr:    true,
			wantErrMsg: "InvalidAction",
		},
		{
			name:       "key_not_found",
			setup:      func(_ *iam.InMemoryBackend) {},
			userName:   "alice",
			keyID:      "AKIANONEXIST",
			status:     "Inactive",
			wantErr:    true,
			wantErrMsg: "NoSuchEntity",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			tt.setup(b)

			keyID := tt.keyID
			if keyID == "" {
				ak, err := b.CreateAccessKey("alice")
				if tt.userName == "alice" && err == nil {
					keyID = ak.AccessKeyID
				}
			}

			err := b.UpdateAccessKey(tt.userName, keyID, tt.status)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrMsg)

				return
			}

			require.NoError(t, err)
		})
	}
}

// TestBackendRefinement_GetAccessKeyLastUsed tests GetAccessKeyLastUsed.
func TestBackendRefinement_GetAccessKeyLastUsed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		keyIDFn  func(*iam.InMemoryBackend) string
		name     string
		wantUser string
		wantErr  bool
	}{
		{
			name: "existing_key",
			keyIDFn: func(b *iam.InMemoryBackend) string {
				_, _ = b.CreateUser("bob", "/", "")
				ak, _ := b.CreateAccessKey("bob")

				return ak.AccessKeyID
			},
			wantUser: "bob",
		},
		{
			name: "nonexistent_key",
			keyIDFn: func(_ *iam.InMemoryBackend) string {
				return "AKIANONEXIST"
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			keyID := tt.keyIDFn(b)

			info, err := b.GetAccessKeyLastUsed(keyID)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantUser, info.UserName)
			assert.Equal(t, keyID, info.AccessKeyID)
			assert.Equal(t, "N/A", info.ServiceName)
		})
	}
}

// TestBackendRefinement_ListDeleteAccountAliases tests ListAccountAliases and DeleteAccountAlias.
func TestBackendRefinement_ListDeleteAccountAliases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		setup       func(*iam.InMemoryBackend)
		deleteAlias string
		wantLen     int
		wantErr     bool
	}{
		{
			name:    "list_empty",
			setup:   func(_ *iam.InMemoryBackend) {},
			wantLen: 0,
		},
		{
			name: "list_with_alias",
			setup: func(b *iam.InMemoryBackend) {
				_ = b.CreateAccountAlias("mycompany")
			},
			wantLen: 1,
		},
		{
			name: "delete_alias",
			setup: func(b *iam.InMemoryBackend) {
				_ = b.CreateAccountAlias("mycompany")
			},
			deleteAlias: "mycompany",
			wantLen:     0,
		},
		{
			name:        "delete_not_found",
			setup:       func(_ *iam.InMemoryBackend) {},
			deleteAlias: "ghost",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			tt.setup(b)

			if tt.deleteAlias != "" {
				err := b.DeleteAccountAlias(tt.deleteAlias)
				if tt.wantErr {
					require.Error(t, err)

					return
				}

				require.NoError(t, err)
			}

			aliases := b.ListAccountAliases()
			assert.Len(t, aliases, tt.wantLen)
		})
	}
}

// TestBackendRefinement_ListGroupsForUser tests ListGroupsForUser.
func TestBackendRefinement_ListGroupsForUser(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(*iam.InMemoryBackend)
		userName   string
		wantGroups int
		wantErr    bool
	}{
		{
			name: "no_groups",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
			},
			userName:   "alice",
			wantGroups: 0,
		},
		{
			name: "one_group",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
				_, _ = b.CreateGroup("Admins", "/")
				_ = b.AddUserToGroup("Admins", "alice")
			},
			userName:   "alice",
			wantGroups: 1,
		},
		{
			name:     "user_not_found",
			setup:    func(_ *iam.InMemoryBackend) {},
			userName: "ghost",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			tt.setup(b)

			groups, err := b.ListGroupsForUser(tt.userName)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, groups, tt.wantGroups)
		})
	}
}

// TestBackendRefinement_PasswordPolicy tests password policy CRUD.
func TestBackendRefinement_PasswordPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*iam.InMemoryBackend)
		name       string
		action     string
		wantMinLen int
		wantErr    bool
	}{
		{
			name:       "get_default",
			action:     "get",
			setup:      func(_ *iam.InMemoryBackend) {},
			wantMinLen: 8,
		},
		{
			name:   "update_and_get",
			action: "update",
			setup: func(b *iam.InMemoryBackend) {
				_ = b.UpdateAccountPasswordPolicy(iam.PasswordPolicy{
					MinimumPasswordLength: 12,
					RequireNumbers:        true,
				})
			},
			wantMinLen: 12,
		},
		{
			name:   "delete_existing",
			action: "delete",
			setup: func(b *iam.InMemoryBackend) {
				_ = b.UpdateAccountPasswordPolicy(iam.PasswordPolicy{MinimumPasswordLength: 14})
			},
		},
		{
			name:    "delete_not_set",
			action:  "delete",
			setup:   func(_ *iam.InMemoryBackend) {},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			tt.setup(b)

			switch tt.action {
			case "get":
				pp := b.GetAccountPasswordPolicy()
				require.NotNil(t, pp)
				assert.Equal(t, tt.wantMinLen, pp.MinimumPasswordLength)
			case "update":
				pp := b.GetAccountPasswordPolicy()
				require.NotNil(t, pp)
				assert.Equal(t, tt.wantMinLen, pp.MinimumPasswordLength)
			case "delete":
				err := b.DeleteAccountPasswordPolicy()
				if tt.wantErr {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
			}
		})
	}
}

// TestBackendRefinement_PolicyVersionMgmt tests SetDefaultPolicyVersion and DeletePolicyVersion.
func TestBackendRefinement_PolicyVersionMgmt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		action    string // "set_default", "delete"
		versionID string
		wantErr   bool
	}{
		{
			name:      "set_default_v2",
			action:    "set_default",
			versionID: "v2",
		},
		{
			name:      "delete_non_default_v2",
			action:    "delete",
			versionID: "v2",
		},
		{
			name:      "delete_default_v1_fails",
			action:    "delete",
			versionID: "v1",
			wantErr:   true,
		},
		{
			name:      "set_default_nonexistent",
			action:    "set_default",
			versionID: "v99",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			pol, err := b.CreatePolicy(
				"MyPolicy",
				"/",
				`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
			)
			require.NoError(t, err)

			_, err = b.CreatePolicyVersion(
				pol.Arn,
				`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
				false,
			)
			require.NoError(t, err)

			switch tt.action {
			case "set_default":
				err = b.SetDefaultPolicyVersion(pol.Arn, tt.versionID)
			case "delete":
				err = b.DeletePolicyVersion(pol.Arn, tt.versionID)
			}

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestBackendRefinement_ListEntitiesForPolicy tests ListEntitiesForPolicy.
func TestBackendRefinement_ListEntitiesForPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(*iam.InMemoryBackend, string)
		filter     string
		wantUsers  int
		wantGroups int
		wantRoles  int
		wantErr    bool
	}{
		{
			name: "all_entities",
			setup: func(b *iam.InMemoryBackend, policyArn string) {
				_, _ = b.CreateUser("alice", "/", "")
				_, _ = b.CreateGroup("Admins", "/")
				_, _ = b.CreateRole(
					"DevRole",
					"/",
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
					"",
				)
				_ = b.AttachUserPolicy("alice", policyArn)
				_ = b.AttachGroupPolicy("Admins", policyArn)
				_ = b.AttachRolePolicy("DevRole", policyArn)
			},
			wantUsers:  1,
			wantGroups: 1,
			wantRoles:  1,
		},
		{
			name: "filter_user",
			setup: func(b *iam.InMemoryBackend, policyArn string) {
				_, _ = b.CreateUser("alice", "/", "")
				_ = b.AttachUserPolicy("alice", policyArn)
			},
			filter:    "User",
			wantUsers: 1,
		},
		{
			name:    "nonexistent_policy",
			setup:   func(_ *iam.InMemoryBackend, _ string) {},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			var policyArn string

			if !tt.wantErr {
				pol, err := b.CreatePolicy(
					"TestPolicy",
					"/",
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
				)
				require.NoError(t, err)
				policyArn = pol.Arn
			} else {
				policyArn = "arn:aws:iam::000000000000:policy/ghost"
			}

			tt.setup(b, policyArn)

			entities, err := b.ListEntitiesForPolicy(policyArn, tt.filter)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, entities.PolicyUsers, tt.wantUsers)
			assert.Len(t, entities.PolicyGroups, tt.wantGroups)
			assert.Len(t, entities.PolicyRoles, tt.wantRoles)
		})
	}
}

// TestBackendRefinement_UpdateUser tests UpdateUser.
func TestBackendRefinement_UpdateUser(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		setup       func(*iam.InMemoryBackend)
		userName    string
		newPath     string
		newUserName string
		wantErr     bool
	}{
		{
			name: "rename_user",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
			},
			userName:    "alice",
			newUserName: "alice2",
		},
		{
			name: "update_path",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
			},
			userName: "alice",
			newPath:  "/team/",
		},
		{
			name:     "user_not_found",
			setup:    func(_ *iam.InMemoryBackend) {},
			userName: "ghost",
			wantErr:  true,
		},
		{
			name: "rename_to_existing",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
				_, _ = b.CreateUser("bob", "/", "")
			},
			userName:    "alice",
			newUserName: "bob",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			tt.setup(b)

			err := b.UpdateUser(tt.userName, tt.newPath, tt.newUserName)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

// TestBackendRefinement_UpdateRole tests UpdateRole.
func TestBackendRefinement_UpdateRole(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		roleName    string
		description string
		wantErr     bool
	}{
		{
			name:        "update_description",
			roleName:    "MyRole",
			description: "A new description",
		},
		{
			name:     "role_not_found",
			roleName: "ghost",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			_, _ = b.CreateRole(
				"MyRole",
				"/",
				`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
				"",
			)

			err := b.UpdateRole(tt.roleName, tt.description)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			r, err := b.GetRole("MyRole")
			require.NoError(t, err)
			assert.Equal(t, tt.description, r.Description)
		})
	}
}

// TestBackendRefinement_UpdateGroup tests UpdateGroup.
func TestBackendRefinement_UpdateGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		setup        func(*iam.InMemoryBackend)
		groupName    string
		newPath      string
		newGroupName string
		wantErr      bool
	}{
		{
			name: "rename_group",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateGroup("Admins", "/")
			},
			groupName:    "Admins",
			newGroupName: "Operators",
		},
		{
			name: "update_path",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateGroup("Admins", "/")
			},
			groupName: "Admins",
			newPath:   "/teams/",
		},
		{
			name:      "group_not_found",
			setup:     func(_ *iam.InMemoryBackend) {},
			groupName: "ghost",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			tt.setup(b)

			err := b.UpdateGroup(tt.groupName, tt.newPath, tt.newGroupName)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

// TestBackendRefinement_VirtualMFA tests ListVirtualMFADevices and DeleteVirtualMFADevice.
func TestBackendRefinement_VirtualMFA(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(*iam.InMemoryBackend) string
		name      string
		action    string
		wantCount int
		wantErr   bool
	}{
		{
			name:   "list_empty",
			action: "list",
			setup:  func(_ *iam.InMemoryBackend) string { return "" },
		},
		{
			name:   "list_one",
			action: "list",
			setup: func(b *iam.InMemoryBackend) string {
				_, _ = b.CreateVirtualMFADevice("MyMFA", "/")

				return ""
			},
			wantCount: 1,
		},
		{
			name:   "delete_existing",
			action: "delete",
			setup: func(b *iam.InMemoryBackend) string {
				d, _ := b.CreateVirtualMFADevice("MyMFA", "/")

				return d.SerialNumber
			},
		},
		{
			name:    "delete_not_found",
			action:  "delete",
			setup:   func(_ *iam.InMemoryBackend) string { return "arn:aws:iam::000000000000:mfa/ghost" },
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			serial := tt.setup(b)

			switch tt.action {
			case "list":
				p, err := b.ListVirtualMFADevices("", 0)
				require.NoError(t, err)
				assert.Len(t, p.Data, tt.wantCount)
			case "delete":
				err := b.DeleteVirtualMFADevice(serial)
				if tt.wantErr {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
			}
		})
	}
}

// TestBackendRefinement_GetInstanceProfile tests GetInstanceProfile.
func TestBackendRefinement_GetInstanceProfile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setup   func(*iam.InMemoryBackend)
		ipName  string
		wantErr bool
	}{
		{
			name: "found",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateInstanceProfile("MyProfile", "/")
			},
			ipName: "MyProfile",
		},
		{
			name:    "not_found",
			setup:   func(_ *iam.InMemoryBackend) {},
			ipName:  "ghost",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			tt.setup(b)

			ip, err := b.GetInstanceProfile(tt.ipName)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.ipName, ip.InstanceProfileName)
		})
	}
}

// TestBackendRefinement_SimulateCustomPolicy tests SimulateCustomPolicy.
func TestBackendRefinement_SimulateCustomPolicy(t *testing.T) {
	t.Parallel()

	allowAllPolicy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	denyAllPolicy := `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"*","Resource":"*"}]}`

	tests := []struct {
		name          string
		policies      []string
		actions       []string
		resources     []string
		wantDecisions []string
		wantErr       bool
	}{
		{
			name:          "allow_all",
			policies:      []string{allowAllPolicy},
			actions:       []string{"s3:GetObject"},
			resources:     []string{"arn:aws:s3:::my-bucket/key"},
			wantDecisions: []string{"allowed"},
		},
		{
			name: "implicit_deny",
			policies: []string{
				`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"ec2:DescribeInstances","Resource":"*"}]}`,
			},
			actions:       []string{"s3:GetObject"},
			resources:     []string{"*"},
			wantDecisions: []string{"implicitDeny"},
		},
		{
			name:          "explicit_deny",
			policies:      []string{denyAllPolicy},
			actions:       []string{"s3:GetObject"},
			resources:     []string{"*"},
			wantDecisions: []string{"explicitDeny"},
		},
		{
			name:    "no_actions",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()

			results, err := b.SimulateCustomPolicy(tt.policies, nil, tt.actions, tt.resources, iam.ConditionContext{})
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.Len(t, results, len(tt.wantDecisions))

			for i, want := range tt.wantDecisions {
				assert.Equal(t, want, results[i].Decision)
			}
		})
	}
}

// TestHandlerRefinement_AccessKey tests the UpdateAccessKey and GetAccessKeyLastUsed handlers.
func TestHandlerRefinement_AccessKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(*iam.InMemoryBackend) string
		params      map[string]string
		name        string
		action      string
		wantContain string
		wantCode    int
	}{
		{
			name:   "UpdateAccessKey_inactive",
			action: "UpdateAccessKey",
			setup: func(b *iam.InMemoryBackend) string {
				_, _ = b.CreateUser("alice", "/", "")
				ak, _ := b.CreateAccessKey("alice")

				return ak.AccessKeyID
			},
			params: map[string]string{
				"UserName": "alice",
				"Status":   "Inactive",
			},
			wantCode:    http.StatusOK,
			wantContain: "UpdateAccessKeyResponse",
		},
		{
			name:   "GetAccessKeyLastUsed",
			action: "GetAccessKeyLastUsed",
			setup: func(b *iam.InMemoryBackend) string {
				_, _ = b.CreateUser("alice", "/", "")
				ak, _ := b.CreateAccessKey("alice")

				return ak.AccessKeyID
			},
			wantCode:    http.StatusOK,
			wantContain: "GetAccessKeyLastUsedResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			keyID := tt.setup(b)
			h := iam.NewHandler(b)

			params := make(map[string]string)
			maps.Copy(params, tt.params)

			params["AccessKeyId"] = keyID

			e := echo.New()
			req := iamRequest(tt.action, params)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContain)
		})
	}
}

// TestHandlerRefinement_PasswordPolicy tests the password policy handlers.
func TestHandlerRefinement_PasswordPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(*iam.InMemoryBackend)
		params      map[string]string
		name        string
		action      string
		wantContain string
		wantCode    int
	}{
		{
			name:        "GetAccountPasswordPolicy",
			action:      "GetAccountPasswordPolicy",
			setup:       func(_ *iam.InMemoryBackend) {},
			wantCode:    http.StatusOK,
			wantContain: "GetAccountPasswordPolicyResponse",
		},
		{
			name:   "UpdateAccountPasswordPolicy",
			action: "UpdateAccountPasswordPolicy",
			setup:  func(_ *iam.InMemoryBackend) {},
			params: map[string]string{
				"MinimumPasswordLength": "12",
				"RequireNumbers":        "true",
			},
			wantCode:    http.StatusOK,
			wantContain: "UpdateAccountPasswordPolicyResponse",
		},
		{
			name:   "DeleteAccountPasswordPolicy",
			action: "DeleteAccountPasswordPolicy",
			setup: func(b *iam.InMemoryBackend) {
				_ = b.UpdateAccountPasswordPolicy(iam.PasswordPolicy{MinimumPasswordLength: 10})
			},
			wantCode:    http.StatusOK,
			wantContain: "DeleteAccountPasswordPolicyResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			tt.setup(b)
			h := iam.NewHandler(b)

			e := echo.New()
			req := iamRequest(tt.action, tt.params)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContain)
		})
	}
}

// TestHandlerRefinement_ListAccountAliases tests the account alias handlers.
func TestHandlerRefinement_ListAccountAliases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(*iam.InMemoryBackend)
		params      map[string]string
		name        string
		action      string
		wantContain string
		wantCode    int
	}{
		{
			name:        "ListAccountAliases_empty",
			action:      "ListAccountAliases",
			setup:       func(_ *iam.InMemoryBackend) {},
			wantCode:    http.StatusOK,
			wantContain: "ListAccountAliasesResponse",
		},
		{
			name:   "ListAccountAliases_with_alias",
			action: "ListAccountAliases",
			setup: func(b *iam.InMemoryBackend) {
				_ = b.CreateAccountAlias("mycompany")
			},
			wantCode:    http.StatusOK,
			wantContain: "mycompany",
		},
		{
			name:   "DeleteAccountAlias",
			action: "DeleteAccountAlias",
			setup: func(b *iam.InMemoryBackend) {
				_ = b.CreateAccountAlias("mycompany")
			},
			params:      map[string]string{"AccountAlias": "mycompany"},
			wantCode:    http.StatusOK,
			wantContain: "DeleteAccountAliasResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			tt.setup(b)
			h := iam.NewHandler(b)

			e := echo.New()
			req := iamRequest(tt.action, tt.params)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContain)
		})
	}
}
