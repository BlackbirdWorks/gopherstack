package iam_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

func TestInMemoryBackend_UserInlinePolicies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*iam.InMemoryBackend)
		name    string
		wantDoc string
		wantErr error
		action  string
	}{
		{
			name: "PutAndGetUserPolicy",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
			},
			action:  "put_get",
			wantDoc: `{"Version":"2012-10-17"}`,
		},
		{
			name:    "PutUserPolicy_UserNotFound",
			setup:   func(_ *iam.InMemoryBackend) {},
			action:  "put_notfound",
			wantErr: iam.ErrUserNotFound,
		},
		{
			name: "GetUserPolicy_PolicyNotFound",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
			},
			action:  "get_notfound",
			wantErr: iam.ErrInlinePolicyNotFound,
		},
		{
			name: "DeleteUserPolicy",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
				_ = b.PutUserPolicy("alice", "MyPolicy", `{"Version":"2012-10-17"}`)
			},
			action: "delete",
		},
		{
			name: "DeleteUserPolicy_NotFound",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
			},
			action:  "delete_notfound",
			wantErr: iam.ErrInlinePolicyNotFound,
		},
		{
			name: "ListUserPolicies_Sorted",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
				_ = b.PutUserPolicy("alice", "ZPolicy", "{}")
				_ = b.PutUserPolicy("alice", "APolicy", "{}")
			},
			action: "list",
		},
		{
			name:    "ListUserPolicies_UserNotFound",
			setup:   func(_ *iam.InMemoryBackend) {},
			action:  "list_notfound",
			wantErr: iam.ErrUserNotFound,
		},
		{
			name: "PutUserPolicy_InvalidJSON",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
			},
			action:  "put_invalid_json",
			wantErr: iam.ErrMalformedPolicyDocument,
		},
		{
			name: "DeleteUser_WithInlinePolicy_Conflict",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
				_ = b.PutUserPolicy("alice", "MyPolicy", "{}")
			},
			action:  "delete_user_conflict",
			wantErr: iam.ErrDeleteConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			tt.setup(b)

			switch tt.action {
			case "put_get":
				err := b.PutUserPolicy("alice", "MyPolicy", tt.wantDoc)
				require.NoError(t, err)

				doc, err := b.GetUserPolicy("alice", "MyPolicy")
				require.NoError(t, err)
				assert.Equal(t, tt.wantDoc, doc)

			case "put_notfound":
				err := b.PutUserPolicy("nobody", "MyPolicy", "{}")
				require.ErrorIs(t, err, tt.wantErr)

			case "get_notfound":
				_, err := b.GetUserPolicy("alice", "Ghost")
				require.ErrorIs(t, err, tt.wantErr)

			case "delete":
				err := b.DeleteUserPolicy("alice", "MyPolicy")
				require.NoError(t, err)

				names, err := b.ListUserPolicies("alice")
				require.NoError(t, err)
				assert.Empty(t, names)

			case "delete_notfound":
				err := b.DeleteUserPolicy("alice", "Ghost")
				require.ErrorIs(t, err, tt.wantErr)

			case "list":
				names, err := b.ListUserPolicies("alice")
				require.NoError(t, err)
				require.Len(t, names, 2)
				assert.Equal(t, "APolicy", names[0])
				assert.Equal(t, "ZPolicy", names[1])

			case "list_notfound":
				_, err := b.ListUserPolicies("nobody")
				require.ErrorIs(t, err, tt.wantErr)

			case "put_invalid_json":
				err := b.PutUserPolicy("alice", "MyPolicy", "not-json")
				require.ErrorIs(t, err, tt.wantErr)

			case "delete_user_conflict":
				err := b.DeleteUser("alice")
				require.ErrorIs(t, err, tt.wantErr)
			}
		})
	}
}

func TestInMemoryBackend_RoleInlinePolicies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*iam.InMemoryBackend)
		name    string
		wantDoc string
		wantErr error
		action  string
	}{
		{
			name: "PutAndGetRolePolicy",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateRole("MyRole", "/", "{}", "")
			},
			action:  "put_get",
			wantDoc: `{"Version":"2012-10-17"}`,
		},
		{
			name:    "PutRolePolicy_RoleNotFound",
			setup:   func(_ *iam.InMemoryBackend) {},
			action:  "put_notfound",
			wantErr: iam.ErrRoleNotFound,
		},
		{
			name: "GetRolePolicy_PolicyNotFound",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateRole("MyRole", "/", "{}", "")
			},
			action:  "get_notfound",
			wantErr: iam.ErrInlinePolicyNotFound,
		},
		{
			name: "DeleteRolePolicy",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateRole("MyRole", "/", "{}", "")
				_ = b.PutRolePolicy("MyRole", "InlinePolicy", "{}")
			},
			action: "delete",
		},
		{
			name: "ListRolePolicies_Sorted",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateRole("MyRole", "/", "{}", "")
				_ = b.PutRolePolicy("MyRole", "ZPolicy", "{}")
				_ = b.PutRolePolicy("MyRole", "APolicy", "{}")
			},
			action: "list",
		},
		{
			name: "DeleteRole_WithInlinePolicy_Conflict",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateRole("MyRole", "/", "{}", "")
				_ = b.PutRolePolicy("MyRole", "InlinePolicy", "{}")
			},
			action:  "delete_role_conflict",
			wantErr: iam.ErrDeleteConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			tt.setup(b)

			switch tt.action {
			case "put_get":
				err := b.PutRolePolicy("MyRole", "MyPolicy", tt.wantDoc)
				require.NoError(t, err)

				doc, err := b.GetRolePolicy("MyRole", "MyPolicy")
				require.NoError(t, err)
				assert.Equal(t, tt.wantDoc, doc)

			case "put_notfound":
				err := b.PutRolePolicy("Ghost", "MyPolicy", "{}")
				require.ErrorIs(t, err, tt.wantErr)

			case "get_notfound":
				_, err := b.GetRolePolicy("MyRole", "Ghost")
				require.ErrorIs(t, err, tt.wantErr)

			case "delete":
				err := b.DeleteRolePolicy("MyRole", "InlinePolicy")
				require.NoError(t, err)

				names, err := b.ListRolePolicies("MyRole")
				require.NoError(t, err)
				assert.Empty(t, names)

			case "list":
				names, err := b.ListRolePolicies("MyRole")
				require.NoError(t, err)
				require.Len(t, names, 2)
				assert.Equal(t, "APolicy", names[0])
				assert.Equal(t, "ZPolicy", names[1])

			case "delete_role_conflict":
				err := b.DeleteRole("MyRole")
				require.ErrorIs(t, err, tt.wantErr)
			}
		})
	}
}

func TestInMemoryBackend_GroupInlinePolicies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*iam.InMemoryBackend)
		name    string
		wantDoc string
		wantErr error
		action  string
	}{
		{
			name: "PutAndGetGroupPolicy",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateGroup("Admins", "/")
			},
			action:  "put_get",
			wantDoc: `{"Version":"2012-10-17"}`,
		},
		{
			name:    "PutGroupPolicy_GroupNotFound",
			setup:   func(_ *iam.InMemoryBackend) {},
			action:  "put_notfound",
			wantErr: iam.ErrGroupNotFound,
		},
		{
			name: "GetGroupPolicy_PolicyNotFound",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateGroup("Admins", "/")
			},
			action:  "get_notfound",
			wantErr: iam.ErrInlinePolicyNotFound,
		},
		{
			name: "DeleteGroupPolicy",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateGroup("Admins", "/")
				_ = b.PutGroupPolicy("Admins", "InlinePolicy", "{}")
			},
			action: "delete",
		},
		{
			name: "ListGroupPolicies_Sorted",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateGroup("Admins", "/")
				_ = b.PutGroupPolicy("Admins", "ZPolicy", "{}")
				_ = b.PutGroupPolicy("Admins", "APolicy", "{}")
			},
			action: "list",
		},
		{
			name: "DeleteGroup_WithInlinePolicy_Conflict",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateGroup("Admins", "/")
				_ = b.PutGroupPolicy("Admins", "InlinePolicy", "{}")
			},
			action:  "delete_group_conflict",
			wantErr: iam.ErrDeleteConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			tt.setup(b)

			switch tt.action {
			case "put_get":
				err := b.PutGroupPolicy("Admins", "MyPolicy", tt.wantDoc)
				require.NoError(t, err)

				doc, err := b.GetGroupPolicy("Admins", "MyPolicy")
				require.NoError(t, err)
				assert.Equal(t, tt.wantDoc, doc)

			case "put_notfound":
				err := b.PutGroupPolicy("Ghost", "MyPolicy", "{}")
				require.ErrorIs(t, err, tt.wantErr)

			case "get_notfound":
				_, err := b.GetGroupPolicy("Admins", "Ghost")
				require.ErrorIs(t, err, tt.wantErr)

			case "delete":
				err := b.DeleteGroupPolicy("Admins", "InlinePolicy")
				require.NoError(t, err)

				names, err := b.ListGroupPolicies("Admins")
				require.NoError(t, err)
				assert.Empty(t, names)

			case "list":
				names, err := b.ListGroupPolicies("Admins")
				require.NoError(t, err)
				require.Len(t, names, 2)
				assert.Equal(t, "APolicy", names[0])
				assert.Equal(t, "ZPolicy", names[1])

			case "delete_group_conflict":
				err := b.DeleteGroup("Admins")
				require.ErrorIs(t, err, tt.wantErr)
			}
		})
	}
}

func TestInMemoryBackend_PermissionsBoundaries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*iam.InMemoryBackend)
		name         string
		wantBoundary string
		wantErr      error
		action       string
	}{
		{
			name: "PutUserPermissionsBoundary",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
			},
			action:       "put_user",
			wantBoundary: "arn:aws:iam::000000000000:policy/Boundary",
		},
		{
			name:    "PutUserPermissionsBoundary_UserNotFound",
			setup:   func(_ *iam.InMemoryBackend) {},
			action:  "put_user_notfound",
			wantErr: iam.ErrUserNotFound,
		},
		{
			name: "DeleteUserPermissionsBoundary",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "arn:aws:iam::000000000000:policy/Boundary")
			},
			action: "delete_user",
		},
		{
			name: "PutRolePermissionsBoundary",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateRole("MyRole", "/", "{}", "")
			},
			action:       "put_role",
			wantBoundary: "arn:aws:iam::000000000000:policy/Boundary",
		},
		{
			name:    "PutRolePermissionsBoundary_RoleNotFound",
			setup:   func(_ *iam.InMemoryBackend) {},
			action:  "put_role_notfound",
			wantErr: iam.ErrRoleNotFound,
		},
		{
			name: "DeleteRolePermissionsBoundary",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateRole("MyRole", "/", "{}", "arn:aws:iam::000000000000:policy/Boundary")
			},
			action: "delete_role",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			tt.setup(b)

			switch tt.action {
			case "put_user":
				err := b.PutUserPermissionsBoundary("alice", tt.wantBoundary)
				require.NoError(t, err)

				u, err := b.GetUser("alice")
				require.NoError(t, err)
				assert.Equal(t, tt.wantBoundary, u.PermissionsBoundary)

			case "put_user_notfound":
				err := b.PutUserPermissionsBoundary("nobody", "arn:aws:iam::000000000000:policy/Boundary")
				require.ErrorIs(t, err, tt.wantErr)

			case "delete_user":
				err := b.DeleteUserPermissionsBoundary("alice")
				require.NoError(t, err)

				u, err := b.GetUser("alice")
				require.NoError(t, err)
				assert.Empty(t, u.PermissionsBoundary)

			case "put_role":
				err := b.PutRolePermissionsBoundary("MyRole", tt.wantBoundary)
				require.NoError(t, err)

				r, err := b.GetRole("MyRole")
				require.NoError(t, err)
				assert.Equal(t, tt.wantBoundary, r.PermissionsBoundary)

			case "put_role_notfound":
				err := b.PutRolePermissionsBoundary("Ghost", "arn:aws:iam::000000000000:policy/Boundary")
				require.ErrorIs(t, err, tt.wantErr)

			case "delete_role":
				err := b.DeleteRolePermissionsBoundary("MyRole")
				require.NoError(t, err)

				r, err := b.GetRole("MyRole")
				require.NoError(t, err)
				assert.Empty(t, r.PermissionsBoundary)
			}
		})
	}
}

func TestInMemoryBackend_UpdateAssumeRolePolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		setup   func(*iam.InMemoryBackend)
		name    string
		doc     string
	}{
		{
			name: "UpdateAssumeRolePolicy_Success",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateRole("MyRole", "/", "{}", "")
			},
			doc: `{"Version":"2012-10-17","Statement":[` +
				`{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}`,
		},
		{
			name:    "UpdateAssumeRolePolicy_RoleNotFound",
			setup:   func(_ *iam.InMemoryBackend) {},
			doc:     "{}",
			wantErr: iam.ErrRoleNotFound,
		},
		{
			name: "UpdateAssumeRolePolicy_InvalidJSON",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateRole("MyRole", "/", "{}", "")
			},
			doc:     "not-json",
			wantErr: iam.ErrMalformedPolicyDocument,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			tt.setup(b)

			err := b.UpdateAssumeRolePolicy("MyRole", tt.doc)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			r, err := b.GetRole("MyRole")
			require.NoError(t, err)
			assert.Equal(t, tt.doc, r.AssumeRolePolicyDocument)
		})
	}
}

// ---- Handler HTTP tests for new operations ----

func TestIAMHandler_UserInlinePolicies(t *testing.T) {
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
			name: "PutUserPolicy",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
			},
			action: "PutUserPolicy",
			params: map[string]string{
				"UserName":       "alice",
				"PolicyName":     "MyPolicy",
				"PolicyDocument": `{"Version":"2012-10-17"}`,
			},
			wantCode:    http.StatusOK,
			wantContain: "PutUserPolicyResponse",
		},
		{
			name: "GetUserPolicy",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
				_ = b.PutUserPolicy("alice", "MyPolicy", `{"Version":"2012-10-17"}`)
			},
			action:      "GetUserPolicy",
			params:      map[string]string{"UserName": "alice", "PolicyName": "MyPolicy"},
			wantCode:    http.StatusOK,
			wantContain: "GetUserPolicyResponse",
		},
		{
			name: "DeleteUserPolicy",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
				_ = b.PutUserPolicy("alice", "MyPolicy", "{}")
			},
			action:      "DeleteUserPolicy",
			params:      map[string]string{"UserName": "alice", "PolicyName": "MyPolicy"},
			wantCode:    http.StatusOK,
			wantContain: "DeleteUserPolicyResponse",
		},
		{
			name: "ListUserPolicies",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
				_ = b.PutUserPolicy("alice", "MyPolicy", "{}")
			},
			action:      "ListUserPolicies",
			params:      map[string]string{"UserName": "alice"},
			wantCode:    http.StatusOK,
			wantContain: "ListUserPoliciesResponse",
		},
		{
			name:        "GetUserPolicy_NotFound",
			setup:       func(_ *iam.InMemoryBackend) {},
			action:      "GetUserPolicy",
			params:      map[string]string{"UserName": "nobody", "PolicyName": "Ghost"},
			wantCode:    http.StatusBadRequest,
			wantContain: "NoSuchEntity",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h, b := newTestHandler(t)
			tt.setup(b)

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

func TestIAMHandler_RoleInlinePolicies(t *testing.T) {
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
			name: "PutRolePolicy",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateRole("MyRole", "/", "{}", "")
			},
			action: "PutRolePolicy",
			params: map[string]string{
				"RoleName":       "MyRole",
				"PolicyName":     "InlineP",
				"PolicyDocument": `{"Version":"2012-10-17"}`,
			},
			wantCode:    http.StatusOK,
			wantContain: "PutRolePolicyResponse",
		},
		{
			name: "GetRolePolicy",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateRole("MyRole", "/", "{}", "")
				_ = b.PutRolePolicy("MyRole", "InlineP", `{"Version":"2012-10-17"}`)
			},
			action:      "GetRolePolicy",
			params:      map[string]string{"RoleName": "MyRole", "PolicyName": "InlineP"},
			wantCode:    http.StatusOK,
			wantContain: "GetRolePolicyResponse",
		},
		{
			name: "DeleteRolePolicy",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateRole("MyRole", "/", "{}", "")
				_ = b.PutRolePolicy("MyRole", "InlineP", "{}")
			},
			action:      "DeleteRolePolicy",
			params:      map[string]string{"RoleName": "MyRole", "PolicyName": "InlineP"},
			wantCode:    http.StatusOK,
			wantContain: "DeleteRolePolicyResponse",
		},
		{
			name: "ListRolePolicies",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateRole("MyRole", "/", "{}", "")
				_ = b.PutRolePolicy("MyRole", "InlineP", "{}")
			},
			action:      "ListRolePolicies",
			params:      map[string]string{"RoleName": "MyRole"},
			wantCode:    http.StatusOK,
			wantContain: "ListRolePoliciesResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h, b := newTestHandler(t)
			tt.setup(b)

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

func TestIAMHandler_PermissionBoundaries(t *testing.T) {
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
			name: "PutUserPermissionsBoundary",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
			},
			action: "PutUserPermissionsBoundary",
			params: map[string]string{
				"UserName":            "alice",
				"PermissionsBoundary": "arn:aws:iam::000000000000:policy/Boundary",
			},
			wantCode:    http.StatusOK,
			wantContain: "PutUserPermissionsBoundaryResponse",
		},
		{
			name: "DeleteUserPermissionsBoundary",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "arn:aws:iam::000000000000:policy/Boundary")
			},
			action:      "DeleteUserPermissionsBoundary",
			params:      map[string]string{"UserName": "alice"},
			wantCode:    http.StatusOK,
			wantContain: "DeleteUserPermissionsBoundaryResponse",
		},
		{
			name: "PutRolePermissionsBoundary",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateRole("MyRole", "/", "{}", "")
			},
			action: "PutRolePermissionsBoundary",
			params: map[string]string{
				"RoleName":            "MyRole",
				"PermissionsBoundary": "arn:aws:iam::000000000000:policy/Boundary",
			},
			wantCode:    http.StatusOK,
			wantContain: "PutRolePermissionsBoundaryResponse",
		},
		{
			name: "DeleteRolePermissionsBoundary",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateRole("MyRole", "/", "{}", "arn:aws:iam::000000000000:policy/Boundary")
			},
			action:      "DeleteRolePermissionsBoundary",
			params:      map[string]string{"RoleName": "MyRole"},
			wantCode:    http.StatusOK,
			wantContain: "DeleteRolePermissionsBoundaryResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h, b := newTestHandler(t)
			tt.setup(b)

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

func TestIAMHandler_UpdateAssumeRolePolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(*iam.InMemoryBackend)
		params      map[string]string
		name        string
		wantContain string
		wantCode    int
	}{
		{
			name: "UpdateAssumeRolePolicy_Success",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateRole("MyRole", "/", "{}", "")
			},
			params: map[string]string{
				"RoleName":       "MyRole",
				"PolicyDocument": `{"Version":"2012-10-17","Statement":[]}`,
			},
			wantCode:    http.StatusOK,
			wantContain: "UpdateAssumeRolePolicyResponse",
		},
		{
			name:  "UpdateAssumeRolePolicy_RoleNotFound",
			setup: func(_ *iam.InMemoryBackend) {},
			params: map[string]string{
				"RoleName":       "Ghost",
				"PolicyDocument": "{}",
			},
			wantCode:    http.StatusBadRequest,
			wantContain: "NoSuchEntity",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h, b := newTestHandler(t)
			tt.setup(b)

			req := iamRequest("UpdateAssumeRolePolicy", tt.params)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContain)
		})
	}
}

func TestIAMHandler_CreateUserWithPermissionsBoundary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		params              map[string]string
		name                string
		wantBoundaryPresent bool
	}{
		{
			name: "WithPermissionsBoundary",
			params: map[string]string{
				"UserName":            "alice",
				"PermissionsBoundary": "arn:aws:iam::000000000000:policy/Boundary",
			},
			wantBoundaryPresent: true,
		},
		{
			name:                "WithoutPermissionsBoundary",
			params:              map[string]string{"UserName": "bob"},
			wantBoundaryPresent: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h, _ := newTestHandler(t)

			req := iamRequest("CreateUser", tt.params)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, http.StatusOK, rec.Code)

			var resp iam.CreateUserResponse
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

			if tt.wantBoundaryPresent {
				require.NotNil(t, resp.CreateUserResult.User.PermissionsBoundary)
				assert.Equal(
					t,
					"arn:aws:iam::000000000000:policy/Boundary",
					resp.CreateUserResult.User.PermissionsBoundary.PermissionsBoundaryArn,
				)
				assert.Equal(t, "Policy", resp.CreateUserResult.User.PermissionsBoundary.PermissionsBoundaryType)
			} else {
				assert.Nil(t, resp.CreateUserResult.User.PermissionsBoundary)
			}
		})
	}
}

func TestIAMHandler_CreateRoleWithPermissionsBoundary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		params              map[string]string
		name                string
		wantBoundaryPresent bool
	}{
		{
			name: "WithPermissionsBoundary",
			params: map[string]string{
				"RoleName":            "BoundedRole",
				"PermissionsBoundary": "arn:aws:iam::000000000000:policy/Boundary",
			},
			wantBoundaryPresent: true,
		},
		{
			name:                "WithoutPermissionsBoundary",
			params:              map[string]string{"RoleName": "FreeRole"},
			wantBoundaryPresent: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h, _ := newTestHandler(t)

			req := iamRequest("CreateRole", tt.params)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, http.StatusOK, rec.Code)

			var resp iam.CreateRoleResponse
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

			if tt.wantBoundaryPresent {
				require.NotNil(t, resp.CreateRoleResult.Role.PermissionsBoundary)
				assert.Equal(
					t,
					"arn:aws:iam::000000000000:policy/Boundary",
					resp.CreateRoleResult.Role.PermissionsBoundary.PermissionsBoundaryArn,
				)
			} else {
				assert.Nil(t, resp.CreateRoleResult.Role.PermissionsBoundary)
			}
		})
	}
}
