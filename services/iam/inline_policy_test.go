package iam_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

const (
	// allowS3GetObjectPolicy is a minimal allow-S3-GetObject IAM policy for use in tests.
	allowS3GetObjectPolicy = `{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`

	// allowS3WildcardPolicy is a minimal allow-all-S3 IAM policy for use in tests.
	allowS3WildcardPolicy = `{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`

	// denyAllPolicy is a minimal deny-all IAM policy for use in tests.
	denyAllPolicy = `{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Deny","Action":"*","Resource":"*"}]}`
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

func TestGetAccountAuthorizationDetails(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*iam.InMemoryBackend)
		name         string
		wantUsers    int
		wantGroups   int
		wantRoles    int
		wantPolicies int
	}{
		{
			name:  "empty_account",
			setup: func(_ *iam.InMemoryBackend) {},
		},
		{
			name: "populated_account",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
				_, _ = b.CreateUser("bob", "/", "")
				_, _ = b.CreateGroup("admins", "/")
				_, _ = b.CreateRole("my-role", "/", `{"Version":"2012-10-17"}`, "")
				pol, _ := b.CreatePolicy("MyPolicy", "/", `{"Version":"2012-10-17"}`)
				_ = b.AttachUserPolicy("alice", pol.Arn)
				_ = b.PutUserPolicy("alice", "InlineP", `{"Version":"2012-10-17"}`)
				_ = b.PutRolePolicy("my-role", "InlineR", `{"Version":"2012-10-17"}`)
			},
			wantUsers:    2,
			wantGroups:   1,
			wantRoles:    1,
			wantPolicies: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h, b := newTestHandler(t)
			tt.setup(b)

			req := iamRequest("GetAccountAuthorizationDetails", nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, http.StatusOK, rec.Code)

			var resp iam.GetAccountAuthorizationDetailsResponse
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

			assert.Len(t, resp.GetAccountAuthorizationDetailsResult.UserDetailList, tt.wantUsers)
			assert.Len(t, resp.GetAccountAuthorizationDetailsResult.GroupDetailList, tt.wantGroups)
			assert.Len(t, resp.GetAccountAuthorizationDetailsResult.RoleDetailList, tt.wantRoles)
			assert.Len(t, resp.GetAccountAuthorizationDetailsResult.Policies, tt.wantPolicies)
			assert.False(t, resp.GetAccountAuthorizationDetailsResult.IsTruncated)
		})
	}
}

func TestGetAccountAuthorizationDetails_InlinePoliciesIncluded(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)

	_, _ = b.CreateUser("alice", "/", "")
	_ = b.PutUserPolicy("alice", "MyInline", `{"Version":"2012-10-17"}`)

	req := iamRequest("GetAccountAuthorizationDetails", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp iam.GetAccountAuthorizationDetailsResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

	require.Len(t, resp.GetAccountAuthorizationDetailsResult.UserDetailList, 1)
	user := resp.GetAccountAuthorizationDetailsResult.UserDetailList[0]
	assert.Equal(t, "alice", user.UserName)
	require.Len(t, user.UserPolicyList, 1)
	assert.Equal(t, "MyInline", user.UserPolicyList[0].PolicyName)
}

func TestSimulatePrincipalPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		setup          func(*iam.InMemoryBackend)
		params         map[string]string
		wantDecision   string
		wantHTTPStatus int
		wantErr        bool
	}{
		{
			name: "user_with_allow_policy",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
				pol, _ := b.CreatePolicy("AllowS3", "/", allowS3GetObjectPolicy)
				_ = b.AttachUserPolicy("alice", pol.Arn)
			},
			params: map[string]string{
				"PolicySourceArn":       "arn:aws:iam::000000000000:user/alice",
				"ActionNames.member.1":  "s3:GetObject",
				"ResourceArns.member.1": "arn:aws:s3:::my-bucket/key",
			},
			wantDecision:   "allowed",
			wantHTTPStatus: http.StatusOK,
		},
		{
			name: "user_implicit_deny",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("bob", "/", "")
			},
			params: map[string]string{
				"PolicySourceArn":      "arn:aws:iam::000000000000:user/bob",
				"ActionNames.member.1": "s3:DeleteObject",
			},
			wantDecision:   "implicitDeny",
			wantHTTPStatus: http.StatusOK,
		},
		{
			name: "role_with_inline_deny",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateRole("my-role", "/", `{}`, "")
				_ = b.PutRolePolicy("my-role", "DenyAll", denyAllPolicy)
			},
			params: map[string]string{
				"PolicySourceArn":      "arn:aws:iam::000000000000:role/my-role",
				"ActionNames.member.1": "s3:GetObject",
			},
			wantDecision:   "explicitDeny",
			wantHTTPStatus: http.StatusOK,
		},
		{
			name:  "user_not_found",
			setup: func(_ *iam.InMemoryBackend) {},
			params: map[string]string{
				"PolicySourceArn":      "arn:aws:iam::000000000000:user/nonexistent",
				"ActionNames.member.1": "s3:GetObject",
			},
			wantHTTPStatus: http.StatusBadRequest,
			wantErr:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h, b := newTestHandler(t)
			tt.setup(b)

			req := iamRequest("SimulatePrincipalPolicy", tt.params)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tt.wantHTTPStatus, rec.Code)

			if !tt.wantErr {
				var resp iam.SimulatePrincipalPolicyResponse
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

				require.NotEmpty(t, resp.SimulatePrincipalPolicyResult.EvaluationResults)
				assert.Equal(t, tt.wantDecision, resp.SimulatePrincipalPolicyResult.EvaluationResults[0].EvalDecision)
			}
		})
	}
}

func TestGenerateAndGetCredentialReport(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(*iam.InMemoryBackend)
		check  func(*testing.T, *httptest.ResponseRecorder)
		name   string
		action string
	}{
		{
			name:   "GenerateCredentialReport_returns_complete",
			setup:  func(_ *iam.InMemoryBackend) {},
			action: "GenerateCredentialReport",
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()

				var resp iam.GenerateCredentialReportResponse
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, "COMPLETE", resp.GenerateCredentialReportResult.State)
			},
		},
		{
			name:   "GetCredentialReport_empty",
			setup:  func(_ *iam.InMemoryBackend) {},
			action: "GetCredentialReport",
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()

				var resp iam.GetCredentialReportResponse
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

				result := resp.GetCredentialReportResult
				assert.Equal(t, "text/csv", result.ReportFormat)
				assert.NotEmpty(t, result.Content)
				assert.NotEmpty(t, result.GeneratedTime)
			},
		},
		{
			name: "GetCredentialReport_with_users",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
				_, _ = b.CreateUser("bob", "/", "")
			},
			action: "GetCredentialReport",
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()

				var resp iam.GetCredentialReportResponse
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

				content := resp.GetCredentialReportResult.Content
				assert.NotEmpty(t, content)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h, b := newTestHandler(t)
			tt.setup(b)

			req := iamRequest(tt.action, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, http.StatusOK, rec.Code)

			tt.check(t, rec)
		})
	}
}

func TestGetAccountAuthorizationDetails_Backend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup         func(*iam.InMemoryBackend)
		name          string
		wantUserCount int
		wantRoleCount int
	}{
		{
			name:  "empty",
			setup: func(_ *iam.InMemoryBackend) {},
		},
		{
			name: "users_and_roles",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
				_, _ = b.CreateRole("exec", "/", "{}", "")
			},
			wantUserCount: 1,
			wantRoleCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			tt.setup(b)

			details := b.GetAccountAuthorizationDetails()
			assert.Len(t, details.Users, tt.wantUserCount)
			assert.Len(t, details.Roles, tt.wantRoleCount)
		})
	}
}

func TestSimulatePrincipalPolicy_Backend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr      error
		setup        func(*iam.InMemoryBackend)
		name         string
		principalArn string
		wantDecision string
		actions      []string
		resources    []string
	}{
		{
			name: "allow_via_managed_policy",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
				pol, _ := b.CreatePolicy("AllowS3", "/", allowS3WildcardPolicy)
				_ = b.AttachUserPolicy("alice", pol.Arn)
			},
			principalArn: "arn:aws:iam::000000000000:user/alice",
			actions:      []string{"s3:GetObject"},
			resources:    []string{"*"},
			wantDecision: "allowed",
		},
		{
			name: "implicit_deny_no_policy",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("bob", "/", "")
			},
			principalArn: "arn:aws:iam::000000000000:user/bob",
			actions:      []string{"ec2:DescribeInstances"},
			wantDecision: "implicitDeny",
		},
		{
			name:         "user_not_found_error",
			setup:        func(_ *iam.InMemoryBackend) {},
			principalArn: "arn:aws:iam::000000000000:user/nobody",
			actions:      []string{"s3:GetObject"},
			wantErr:      iam.ErrUserNotFound,
		},
		{
			name:         "role_not_found_error",
			setup:        func(_ *iam.InMemoryBackend) {},
			principalArn: "arn:aws:iam::000000000000:role/nobody",
			actions:      []string{"s3:GetObject"},
			wantErr:      iam.ErrRoleNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			tt.setup(b)

			results, err := b.SimulatePrincipalPolicy(tt.principalArn, tt.actions, tt.resources)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			require.NotEmpty(t, results)
			assert.Equal(t, tt.wantDecision, results[0].Decision)
		})
	}
}

func TestGetCredentialReport_Backend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup         func(*iam.InMemoryBackend)
		name          string
		wantUserLines int
	}{
		{
			name:          "no_users",
			setup:         func(_ *iam.InMemoryBackend) {},
			wantUserLines: 2, // header + root
		},
		{
			name: "with_users",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
				_, _ = b.CreateUser("bob", "/", "")
			},
			wantUserLines: 4, // header + root + 2 users
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			tt.setup(b)

			report := b.GetCredentialReport()
			assert.NotEmpty(t, report)

			lines := len(splitLines(report))
			assert.Equal(t, tt.wantUserLines, lines)
		})
	}
}

// splitLines splits a string on newlines, filtering empty lines.
func splitLines(s string) []string {
	var lines []string

	for l := range strings.SplitSeq(s, "\n") {
		if l != "" {
			lines = append(lines, l)
		}
	}

	return lines
}
