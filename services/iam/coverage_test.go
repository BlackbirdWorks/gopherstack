package iam_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/iam"
)

// ---- normPath coverage ----

func TestNormPath_Coverage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		wantPath string
	}{
		{
			name:     "empty_path_defaults_to_root",
			input:    "",
			wantPath: "/",
		},
		{
			name:     "path_without_trailing_slash_gets_one",
			input:    "/engineering",
			wantPath: "/engineering/",
		},
		{
			name:     "path_with_trailing_slash_unchanged",
			input:    "/engineering/",
			wantPath: "/engineering/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			u, err := b.CreateUser("normpath-user-"+tt.name, tt.input, "")
			require.NoError(t, err)
			assert.Equal(t, tt.wantPath, u.Path)
		})
	}
}

// ---- AttachRolePolicy idempotency ----

func TestAttachRolePolicy_Idempotent(t *testing.T) {
	t.Parallel()

	b := iam.NewInMemoryBackend()
	_, err := b.CreateRole("myrole", "/", "{}", "")
	require.NoError(t, err)

	policyArn := "arn:aws:iam::000000000000:policy/MyPolicy"

	// First attach
	require.NoError(t, b.AttachRolePolicy("myrole", policyArn))

	// Second attach to same role with same ARN – should be a no-op (idempotent)
	require.NoError(t, b.AttachRolePolicy("myrole", policyArn))

	policies, err := b.ListAttachedRolePolicies("myrole")
	require.NoError(t, err)
	assert.Len(t, policies, 1, "policy should appear only once")
}

// ---- policyNameFromARN coverage ----

func TestPolicyNameFromARN_Coverage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		policyArn string
		wantName  string
	}{
		{
			name:      "arn_with_policy_prefix",
			policyArn: "arn:aws:iam::000000000000:policy/MyManagedPolicy",
			wantName:  "MyManagedPolicy",
		},
		{
			name:      "arn_without_policy_prefix_returns_full_string",
			policyArn: "SomeArbitraryString",
			wantName:  "SomeArbitraryString",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()

			// Create a policy then look it up by ARN to trigger policyNameFromARN
			_, err := b.CreatePolicy(tt.wantName, "/", "{}")
			require.NoError(t, err)

			pol, err := b.GetPolicy(tt.policyArn)
			if tt.policyArn == "SomeArbitraryString" {
				// Won't be found, but function is exercised
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantName, pol.PolicyName)
			}
		})
	}
}

// ---- getTags: resource with existing tags ----

func TestHandler_GetTags_WithExistingTags(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)

	_, err := b.CreateRole("tagged-role", "/", "{}", "")
	require.NoError(t, err)

	// Tag the role via HTTP
	rec := httptest.NewRecorder()
	params := map[string]string{
		"RoleName":            "tagged-role",
		"Tags.member.1.Key":   "env",
		"Tags.member.1.Value": "prod",
	}
	req := iamRequest("TagRole", params)
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListRoleTags should return the tag (exercises getTags non-nil branch)
	rec2 := httptest.NewRecorder()
	req2 := iamRequest("ListRoleTags", map[string]string{"RoleName": "tagged-role"})
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Equal(t, http.StatusOK, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "env")
}

// ---- RouteMatcher edge cases ----

func TestRouteMatcher_EdgeCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func() *http.Request
		name  string
		want  bool
	}{
		{
			name: "GET_request_returns_false",
			setup: func() *http.Request {
				r := httptest.NewRequest(http.MethodGet, "/", nil)
				r.Header.Set("Content-Type", "application/x-www-form-urlencoded")

				return r
			},
			want: false,
		},
		{
			name: "dashboard_path_returns_false",
			setup: func() *http.Request {
				r := httptest.NewRequest(http.MethodPost, "/dashboard/iam", nil)
				r.Header.Set("Content-Type", "application/x-www-form-urlencoded")

				return r
			},
			want: false,
		},
		{
			name: "non_form_content_type_returns_false",
			setup: func() *http.Request {
				r := httptest.NewRequest(http.MethodPost, "/",
					strings.NewReader(`{"Action":"ListUsers"}`))
				r.Header.Set("Content-Type", "application/json")

				return r
			},
			want: false,
		},
		{
			name: "valid_iam_request_returns_true",
			setup: func() *http.Request {
				vals := url.Values{}
				vals.Set("Action", "ListUsers")
				vals.Set("Version", "2010-05-08")
				r := httptest.NewRequest(http.MethodPost, "/",
					strings.NewReader(vals.Encode()))
				r.Header.Set("Content-Type", "application/x-www-form-urlencoded")

				return r
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			e := echo.New()
			matcher := h.RouteMatcher()
			req := tt.setup()
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.want, matcher(c))
		})
	}
}

// ---- ExtractOperation edge cases ----

func TestExtractOperation_EdgeCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupReq func() *http.Request
		name     string
		want     string
	}{
		{
			name: "valid_action_extracted",
			setupReq: func() *http.Request {
				return iamRequest("CreateUser", nil)
			},
			want: "CreateUser",
		},
		{
			name: "no_action_returns_unknown",
			setupReq: func() *http.Request {
				r := httptest.NewRequest(http.MethodPost, "/",
					strings.NewReader("Version=2010-05-08"))
				r.Header.Set("Content-Type", "application/x-www-form-urlencoded")

				return r
			},
			want: "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			e := echo.New()
			req := tt.setupReq()
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.want, h.ExtractOperation(c))
		})
	}
}

// ---- ExtractResource edge cases ----

func TestExtractResource_EdgeCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupReq func() *http.Request
		name     string
		want     string
	}{
		{
			name: "username_extracted",
			setupReq: func() *http.Request {
				return iamRequest("CreateUser", map[string]string{"UserName": "alice"})
			},
			want: "alice",
		},
		{
			name: "rolename_extracted",
			setupReq: func() *http.Request {
				return iamRequest("CreateRole", map[string]string{"RoleName": "MyRole"})
			},
			want: "MyRole",
		},
		{
			name: "policyname_extracted",
			setupReq: func() *http.Request {
				return iamRequest("CreatePolicy", map[string]string{"PolicyName": "MyPolicy"})
			},
			want: "MyPolicy",
		},
		{
			name: "groupname_extracted",
			setupReq: func() *http.Request {
				return iamRequest("CreateGroup", map[string]string{"GroupName": "Admins"})
			},
			want: "Admins",
		},
		{
			name: "instance_profile_name_extracted",
			setupReq: func() *http.Request {
				return iamRequest("CreateInstanceProfile",
					map[string]string{"InstanceProfileName": "MyProfile"})
			},
			want: "MyProfile",
		},
		{
			name: "no_resource_key_returns_empty",
			setupReq: func() *http.Request {
				return iamRequest("ListUsers", nil)
			},
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			e := echo.New()
			req := tt.setupReq()
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.want, h.ExtractResource(c))
		})
	}
}

// ---- Handler() entry-point coverage ----

func TestHandler_EntryPoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupReq    func() *http.Request
		name        string
		wantContain string
		wantCode    int
	}{
		{
			name: "GET_root_returns_supported_operations",
			setupReq: func() *http.Request {
				return httptest.NewRequest(http.MethodGet, "/", nil)
			},
			wantCode:    http.StatusOK,
			wantContain: "CreateUser",
		},
		{
			name: "non_POST_non_root_GET_returns_405",
			setupReq: func() *http.Request {
				return httptest.NewRequest(http.MethodGet, "/some/path", nil)
			},
			wantCode:    http.StatusMethodNotAllowed,
			wantContain: "Method not allowed",
		},
		{
			name: "PUT_returns_405",
			setupReq: func() *http.Request {
				return httptest.NewRequest(http.MethodPut, "/", nil)
			},
			wantCode:    http.StatusMethodNotAllowed,
			wantContain: "Method not allowed",
		},
		{
			name: "POST_with_invalid_body_returns_400",
			setupReq: func() *http.Request {
				r := httptest.NewRequest(http.MethodPost, "/",
					strings.NewReader("%ZZ")) // invalid URL encoding
				r.Header.Set("Content-Type", "application/x-www-form-urlencoded")

				return r
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "POST_missing_action_returns_400",
			setupReq: func() *http.Request {
				r := httptest.NewRequest(http.MethodPost, "/",
					strings.NewReader("Version=2010-05-08"))
				r.Header.Set("Content-Type", "application/x-www-form-urlencoded")

				return r
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			e := echo.New()
			req := tt.setupReq()
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantContain != "" {
				assert.Contains(t, rec.Body.String(), tt.wantContain)
			}
		})
	}
}

// ---- Policy dispatch table coverage ----

func TestIAMHandler_PolicyDispatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(b *iam.InMemoryBackend)
		params      map[string]string
		name        string
		action      string
		wantContain string
		wantCode    int
	}{
		{
			name:   "GetPolicy_success",
			action: "GetPolicy",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreatePolicy("ReadOnlyPolicy", "/", "{}")
			},
			params:      map[string]string{"PolicyArn": "arn:aws:iam::000000000000:policy/ReadOnlyPolicy"},
			wantCode:    http.StatusOK,
			wantContain: "GetPolicyResponse",
		},
		{
			name:     "GetPolicy_not_found",
			action:   "GetPolicy",
			params:   map[string]string{"PolicyArn": "arn:aws:iam::000000000000:policy/Ghost"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "GetPolicyVersion_success",
			action: "GetPolicyVersion",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreatePolicy("VersionedPolicy", "/", "{}")
			},
			params: map[string]string{
				"PolicyArn": "arn:aws:iam::000000000000:policy/VersionedPolicy",
				"VersionId": "v1",
			},
			wantCode:    http.StatusOK,
			wantContain: "GetPolicyVersionResponse",
		},
		{
			name:   "ListPolicyVersions_success",
			action: "ListPolicyVersions",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreatePolicy("AnyPolicy", "/", "{}")
			},
			params:      map[string]string{"PolicyArn": "arn:aws:iam::000000000000:policy/AnyPolicy"},
			wantCode:    http.StatusOK,
			wantContain: "ListPolicyVersionsResponse",
		},
		{
			name:   "AttachRolePolicy_success",
			action: "AttachRolePolicy",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateRole("svc-role", "/", "{}", "")
			},
			params: map[string]string{
				"RoleName":  "svc-role",
				"PolicyArn": "arn:aws:iam::aws:policy/ReadOnlyAccess",
			},
			wantCode:    http.StatusOK,
			wantContain: "AttachRolePolicyResponse",
		},
		{
			name:   "DetachRolePolicy_success",
			action: "DetachRolePolicy",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateRole("detach-role", "/", "{}", "")
				_ = b.AttachRolePolicy("detach-role", "arn:aws:iam::aws:policy/Policy1")
			},
			params: map[string]string{
				"RoleName":  "detach-role",
				"PolicyArn": "arn:aws:iam::aws:policy/Policy1",
			},
			wantCode:    http.StatusOK,
			wantContain: "DetachRolePolicyResponse",
		},
		{
			name:   "ListAttachedRolePolicies_success",
			action: "ListAttachedRolePolicies",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateRole("list-role", "/", "{}", "")
				_ = b.AttachRolePolicy("list-role", "arn:aws:iam::aws:policy/Policy1")
			},
			params:      map[string]string{"RoleName": "list-role"},
			wantCode:    http.StatusOK,
			wantContain: "ListAttachedRolePoliciesResponse",
		},
		{
			name:   "ListRolePolicies_success",
			action: "ListRolePolicies",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateRole("any-role", "/", "{}", "")
			},
			params:      map[string]string{"RoleName": "any-role"},
			wantCode:    http.StatusOK,
			wantContain: "ListRolePoliciesResponse",
		},
		{
			name:        "ListInstanceProfilesForRole_success",
			action:      "ListInstanceProfilesForRole",
			params:      map[string]string{"RoleName": "any-role"},
			wantCode:    http.StatusOK,
			wantContain: "ListInstanceProfilesForRoleResponse",
		},
		{
			name:   "ListAttachedUserPolicies_success",
			action: "ListAttachedUserPolicies",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("policy-user", "/", "")
				_ = b.AttachUserPolicy("policy-user", "arn:aws:iam::aws:policy/ReadOnly")
			},
			params:      map[string]string{"UserName": "policy-user"},
			wantCode:    http.StatusOK,
			wantContain: "ListAttachedUserPoliciesResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h, b := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(b)
			}

			req := iamRequest(tt.action, tt.params)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantContain != "" {
				assert.Contains(t, rec.Body.String(), tt.wantContain)
			}
		})
	}
}

// ---- Persistence handler delegation ----

func TestHandler_SnapshotRestore_Delegation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(b *iam.InMemoryBackend)
		name  string
	}{
		{
			name: "snapshot_and_restore_via_handler",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("snap-user", "/", "")
			},
		},
		{
			name:  "empty_backend_snapshot_and_restore",
			setup: func(_ *iam.InMemoryBackend) {},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			h := iam.NewHandler(b)
			tt.setup(b)

			snap := h.Snapshot()
			require.NotNil(t, snap)

			freshB := iam.NewInMemoryBackend()
			freshH := iam.NewHandler(freshB)
			require.NoError(t, freshH.Restore(snap))

			if tt.name == "snapshot_and_restore_via_handler" {
				u, err := freshB.GetUser("snap-user")
				require.NoError(t, err)
				assert.Equal(t, "snap-user", u.UserName)
			}
		})
	}
}

// ---- Provider Init with config ----

func TestIAMProvider_InitWithConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		config any
		name   string
	}{
		{
			name:   "nil_config_uses_defaults",
			config: nil,
		},
		{
			name:   "with_config_provider",
			config: &mockIAMConfig{accountID: "123456789012"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &iam.Provider{}
			appCtx := &service.AppContext{
				Logger: logger.NewTestLogger(),
				Config: tt.config,
			}

			svc, err := p.Init(appCtx)
			require.NoError(t, err)
			require.NotNil(t, svc)
		})
	}
}

type mockIAMConfig struct {
	accountID string
}

func (m *mockIAMConfig) GetGlobalConfig() config.GlobalConfig {
	return config.GlobalConfig{AccountID: m.accountID, Region: "us-east-1"}
}

// ---- marshalXML error coverage via invalid input ----

func TestMarshalXML_ErrorPath(t *testing.T) {
	t.Parallel()

	// Create a handler and attempt to write an error that triggers the XML marshal path
	// by sending an unrecognized action - the handler will call writeError which uses marshalXML
	h, _ := newTestHandler(t)
	e := echo.New()

	req := iamRequest("UnknownAction", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	// Should get a 400 for unknown action with proper XML
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidAction")
}

// ---- GetSupportedOperations ----

func TestIAMHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)
	ops := h.GetSupportedOperations()

	assert.Contains(t, ops, "CreateUser")
	assert.Contains(t, ops, "CreateRole")
	assert.Contains(t, ops, "AttachRolePolicy")
	assert.NotEmpty(t, ops)
}

// ---- ListPolicies ----

func TestIAMHandler_ListPolicies(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)
	e := echo.New()

	_, _ = b.CreatePolicy("APolicy", "/", "{}")
	_, _ = b.CreatePolicy("BPolicy", "/", "{}")

	req := iamRequest("ListPolicies", nil)
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"ListPoliciesResponse"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
}

// ---- AttachUserPolicy success ----

func TestIAMHandler_AttachUserPolicy_Success(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)
	e := echo.New()

	_, _ = b.CreateUser("policy-attach-user", "/", "")

	req := iamRequest("AttachUserPolicy", map[string]string{
		"UserName":  "policy-attach-user",
		"PolicyArn": "arn:aws:iam::aws:policy/ReadOnlyAccess",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "AttachUserPolicyResponse")
}
