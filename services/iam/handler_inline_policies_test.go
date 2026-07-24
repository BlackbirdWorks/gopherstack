package iam_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/iam"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_SimulatePrincipalPolicy_WithPermissionsBoundary(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)

	boundary, _ := b.CreatePolicy("S3OnlyBoundary", "/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`)

	_, _ = b.CreateUser("boundary-user", "/", boundary.Arn)

	identityPol, _ := b.CreatePolicy("AllowAll", "/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`)
	_ = b.AttachUserPolicy("boundary-user", identityPol.Arn)

	req := iamRequest("SimulatePrincipalPolicy", map[string]string{
		"PolicySourceArn":       "arn:aws:iam::000000000000:user/boundary-user",
		"ActionNames.member.1":  "ec2:DescribeInstances",
		"ResourceArns.member.1": "*",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	// Boundary blocks ec2, so overall decision is implicitDeny.
	assert.Contains(t, body, "implicitDeny")
	// PermissionsBoundaryDecisionDetail must be present.
	assert.Contains(t, body, "PermissionsBoundaryDecisionDetail")
	assert.Contains(t, body, "AllowedByPermissionsBoundary")
}

func TestHandler_UserInlinePolicy_RoundTrip(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	_, _ = b.CreateUser("alice", "/", "")
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`

	req := iamRequest("PutUserPolicy", map[string]string{
		"UserName":       "alice",
		"PolicyName":     "MyPolicy",
		"PolicyDocument": doc,
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	req2 := iamRequest("GetUserPolicy", map[string]string{
		"UserName":   "alice",
		"PolicyName": "MyPolicy",
	})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Equal(t, http.StatusOK, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "MyPolicy")

	req3 := iamRequest("ListUserPolicies", map[string]string{"UserName": "alice"})
	rec3 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req3, rec3)))
	assert.Contains(t, rec3.Body.String(), "MyPolicy")

	req4 := iamRequest("DeleteUserPolicy", map[string]string{
		"UserName":   "alice",
		"PolicyName": "MyPolicy",
	})
	rec4 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req4, rec4)))
	assert.Equal(t, http.StatusOK, rec4.Code)
}

func TestHandler_RoleInlinePolicy_RoundTrip(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	_, _ = b.CreateRole("MyRole", "/", doc, "")

	req := iamRequest("PutRolePolicy", map[string]string{
		"RoleName":       "MyRole",
		"PolicyName":     "RolePolicy",
		"PolicyDocument": doc,
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	req2 := iamRequest("GetRolePolicy", map[string]string{
		"RoleName":   "MyRole",
		"PolicyName": "RolePolicy",
	})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Contains(t, rec2.Body.String(), "RolePolicy")

	req3 := iamRequest("ListRolePolicies", map[string]string{"RoleName": "MyRole"})
	rec3 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req3, rec3)))
	assert.Contains(t, rec3.Body.String(), "RolePolicy")

	req4 := iamRequest("DeleteRolePolicy", map[string]string{
		"RoleName":   "MyRole",
		"PolicyName": "RolePolicy",
	})
	rec4 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req4, rec4)))
	assert.Equal(t, http.StatusOK, rec4.Code)
}

func TestHandler_GroupInlinePolicy_RoundTrip(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	_, _ = b.CreateGroup("Ops", "/")
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`

	req := iamRequest("PutGroupPolicy", map[string]string{
		"GroupName":      "Ops",
		"PolicyName":     "OpsPolicy",
		"PolicyDocument": doc,
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	req2 := iamRequest("ListGroupPolicies", map[string]string{"GroupName": "Ops"})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Contains(t, rec2.Body.String(), "OpsPolicy")

	req3 := iamRequest("DeleteGroupPolicy", map[string]string{
		"GroupName":  "Ops",
		"PolicyName": "OpsPolicy",
	})
	rec3 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req3, rec3)))
	assert.Equal(t, http.StatusOK, rec3.Code)
}

func TestHandler_PermissionsBoundary_UserRoundTrip(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	_, _ = b.CreateUser("alice", "/", "")
	p, _ := b.CreatePolicy(
		"Boundary",
		"/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
	)

	req := iamRequest("PutUserPermissionsBoundary", map[string]string{
		"UserName":            "alice",
		"PermissionsBoundary": p.Arn,
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	// Real IAM has no GetUserPermissionsBoundary action; the boundary is
	// surfaced via GetUser's PermissionsBoundary field instead.
	req2 := iamRequest("GetUser", map[string]string{"UserName": "alice"})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Equal(t, http.StatusOK, rec2.Code)
	assert.Contains(t, rec2.Body.String(), p.Arn)

	req3 := iamRequest("DeleteUserPermissionsBoundary", map[string]string{"UserName": "alice"})
	rec3 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req3, rec3)))
	assert.Equal(t, http.StatusOK, rec3.Code)
}

func TestHandler_PermissionsBoundary_RoleRoundTrip(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	_, _ = b.CreateRole("MyRole", "/", doc, "")
	p, _ := b.CreatePolicy("Boundary", "/", doc)

	req := iamRequest("PutRolePermissionsBoundary", map[string]string{
		"RoleName":            "MyRole",
		"PermissionsBoundary": p.Arn,
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	// Real IAM has no GetRolePermissionsBoundary action; the boundary is
	// surfaced via GetRole's PermissionsBoundary field instead.
	req2 := iamRequest("GetRole", map[string]string{"RoleName": "MyRole"})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Equal(t, http.StatusOK, rec2.Code)
	assert.Contains(t, rec2.Body.String(), p.Arn)

	req3 := iamRequest("DeleteRolePermissionsBoundary", map[string]string{"RoleName": "MyRole"})
	rec3 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req3, rec3)))
	assert.Equal(t, http.StatusOK, rec3.Code)
}

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
				"PolicyDocument": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
			},
			wantCode:    http.StatusOK,
			wantContain: "PutUserPolicyResponse",
		},
		{
			name: "GetUserPolicy",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
				_ = b.PutUserPolicy(
					"alice",
					"MyPolicy",
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
				)
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
				_ = b.PutUserPolicy(
					"alice",
					"MyPolicy",
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
				)
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
				_ = b.PutUserPolicy(
					"alice",
					"MyPolicy",
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
				)
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
			wantCode:    http.StatusNotFound, // NoSuchEntity is 404 on real AWS IAM.
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
				"PolicyDocument": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
			},
			wantCode:    http.StatusOK,
			wantContain: "PutRolePolicyResponse",
		},
		{
			name: "GetRolePolicy",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateRole("MyRole", "/", "{}", "")
				_ = b.PutRolePolicy(
					"MyRole",
					"InlineP",
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
				)
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
				_ = b.PutRolePolicy(
					"MyRole",
					"InlineP",
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
				)
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
				_ = b.PutRolePolicy(
					"MyRole",
					"InlineP",
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
				)
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
