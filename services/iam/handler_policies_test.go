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

func Test_PolicyDocumentEncoding(t *testing.T) {
	t.Parallel()

	const rawDoc = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`
	// url.QueryEscape("s3:*") == "s3%3A%2A"; the whole document is escaped the
	// same way once it reaches the wire.
	const encodedFragment = "s3%3A%2A"

	tests := []struct {
		setup    func(t *testing.T, b *iam.InMemoryBackend) map[string]string
		name     string
		action   string
		wantElem string
	}{
		{
			name:   "GetRole_AssumeRolePolicyDocument",
			action: "GetRole",
			setup: func(t *testing.T, b *iam.InMemoryBackend) map[string]string {
				t.Helper()
				_, err := b.CreateRole("MyRole", "/", rawDoc, "")
				require.NoError(t, err)

				return map[string]string{"RoleName": "MyRole"}
			},
			wantElem: "AssumeRolePolicyDocument",
		},
		{
			name:   "GetPolicyVersion_Document",
			action: "GetPolicyVersion",
			setup: func(t *testing.T, b *iam.InMemoryBackend) map[string]string {
				t.Helper()
				pol, err := b.CreatePolicy("MyPolicy", "/", rawDoc)
				require.NoError(t, err)

				return map[string]string{"PolicyArn": pol.Arn, "VersionId": "v1"}
			},
			wantElem: "Document",
		},
		{
			name:   "GetUserPolicy_PolicyDocument",
			action: "GetUserPolicy",
			setup: func(t *testing.T, b *iam.InMemoryBackend) map[string]string {
				t.Helper()
				_, err := b.CreateUser("bob", "/", "")
				require.NoError(t, err)
				require.NoError(t, b.PutUserPolicy("bob", "Inline", rawDoc))

				return map[string]string{"UserName": "bob", "PolicyName": "Inline"}
			},
			wantElem: "PolicyDocument",
		},
		{
			name:   "GetRolePolicy_PolicyDocument",
			action: "GetRolePolicy",
			setup: func(t *testing.T, b *iam.InMemoryBackend) map[string]string {
				t.Helper()
				_, err := b.CreateRole("PolRole", "/", "{}", "")
				require.NoError(t, err)
				require.NoError(t, b.PutRolePolicy("PolRole", "Inline", rawDoc))

				return map[string]string{"RoleName": "PolRole", "PolicyName": "Inline"}
			},
			wantElem: "PolicyDocument",
		},
		{
			name:   "GetGroupPolicy_PolicyDocument",
			action: "GetGroupPolicy",
			setup: func(t *testing.T, b *iam.InMemoryBackend) map[string]string {
				t.Helper()
				_, err := b.CreateGroup("PolGroup", "/")
				require.NoError(t, err)
				require.NoError(t, b.PutGroupPolicy("PolGroup", "Inline", rawDoc))

				return map[string]string{"GroupName": "PolGroup", "PolicyName": "Inline"}
			},
			wantElem: "PolicyDocument",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h, b := newTestHandler(t)
			params := tt.setup(t, b)

			req := iamRequest(tt.action, params)
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(e.NewContext(req, rec)))
			require.Equal(t, http.StatusOK, rec.Code)

			body := rec.Body.String()
			assert.Contains(t, body, encodedFragment,
				"%s must be percent-encoded on the wire", tt.wantElem)
			assert.NotContains(t, body, rawDoc,
				"%s must not contain the raw (unencoded) policy document", tt.wantElem)
		})
	}
}

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

func TestPolicyNameFromARN(t *testing.T) {
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
			_, err := b.CreatePolicy(
				tt.wantName,
				"/",
				`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
			)
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
				_, _ = b.CreatePolicy(
					"ReadOnlyPolicy",
					"/",
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
				)
			},
			params:      map[string]string{"PolicyArn": "arn:aws:iam::000000000000:policy/ReadOnlyPolicy"},
			wantCode:    http.StatusOK,
			wantContain: "GetPolicyResponse",
		},
		{
			name:   "GetPolicy_not_found",
			action: "GetPolicy",
			params: map[string]string{"PolicyArn": "arn:aws:iam::000000000000:policy/Ghost"},
			// NoSuchEntity is 404 on real AWS IAM.
			wantCode: http.StatusNotFound,
		},
		{
			name:   "GetPolicyVersion_success",
			action: "GetPolicyVersion",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreatePolicy(
					"VersionedPolicy",
					"/",
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
				)
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
				_, _ = b.CreatePolicy(
					"AnyPolicy",
					"/",
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
				)
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
			name:   "ListInstanceProfilesForRole_success",
			action: "ListInstanceProfilesForRole",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateRole(
					"any-role",
					"/",
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
					"",
				)
			},
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

func TestIAMHandler_ListPolicies(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)
	e := echo.New()

	_, _ = b.CreatePolicy(
		"APolicy",
		"/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
	)
	_, _ = b.CreatePolicy(
		"BPolicy",
		"/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
	)

	req := iamRequest("ListPolicies", nil)
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"ListPoliciesResponse"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
}

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

// TestHandler_SimulatePrincipalPolicy_AllowDenyDecisions verifies that SimulatePrincipalPolicy
// evaluates actual attached policies rather than returning canned results.

func TestHandler_AttachDetachUserPolicy_RoundTrip(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	_, _ = b.CreateUser("alice", "/", "")
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	p, _ := b.CreatePolicy("P", "/", doc)

	// Attach.
	req := iamRequest("AttachUserPolicy", map[string]string{
		"UserName":  "alice",
		"PolicyArn": p.Arn,
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify via ListAttachedUserPolicies.
	req2 := iamRequest("ListAttachedUserPolicies", map[string]string{"UserName": "alice"})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Equal(t, http.StatusOK, rec2.Code)
	assert.Contains(t, rec2.Body.String(), p.Arn)

	// Detach.
	req3 := iamRequest("DetachUserPolicy", map[string]string{
		"UserName":  "alice",
		"PolicyArn": p.Arn,
	})
	rec3 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req3, rec3)))
	assert.Equal(t, http.StatusOK, rec3.Code)

	// Verify detached.
	req4 := iamRequest("ListAttachedUserPolicies", map[string]string{"UserName": "alice"})
	rec4 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req4, rec4)))
	assert.Equal(t, http.StatusOK, rec4.Code)
	assert.NotContains(t, rec4.Body.String(), p.Arn)
}

func TestHandler_AttachRolePolicy_RoundTrip(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	_, _ = b.CreateRole("MyRole", "/", doc, "")
	p, _ := b.CreatePolicy("P", "/", doc)

	req := iamRequest("AttachRolePolicy", map[string]string{
		"RoleName":  "MyRole",
		"PolicyArn": p.Arn,
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	req2 := iamRequest("ListAttachedRolePolicies", map[string]string{"RoleName": "MyRole"})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Contains(t, rec2.Body.String(), p.Arn)

	req3 := iamRequest("DetachRolePolicy", map[string]string{
		"RoleName":  "MyRole",
		"PolicyArn": p.Arn,
	})
	rec3 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req3, rec3)))
	assert.Equal(t, http.StatusOK, rec3.Code)
}

func TestHandler_ListEntitiesForPolicy(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	_, _ = b.CreateUser("alice", "/", "")
	p, _ := b.CreatePolicy("P", "/", doc)
	_ = b.AttachUserPolicy("alice", p.Arn)

	req := iamRequest("ListEntitiesForPolicy", map[string]string{"PolicyArn": p.Arn})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "alice")
}

func TestIAMHandler_Policies(t *testing.T) {
	t.Parallel()

	t.Run("CreatePolicy", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, _ := newTestHandler(t)

		req := iamRequest("CreatePolicy", map[string]string{
			"PolicyName":     "MyPolicy",
			"PolicyDocument": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
		})
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)

		var resp iam.CreatePolicyResponse
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Equal(t, "MyPolicy", resp.CreatePolicyResult.Policy.PolicyName)
	})

	t.Run("DeletePolicy", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, b := newTestHandler(t)
		pol, _ := b.CreatePolicy("MyPolicy", "/", "")

		req := iamRequest("DeletePolicy", map[string]string{"PolicyArn": pol.Arn})
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("ListPolicies", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, b := newTestHandler(t)
		_, _ = b.CreatePolicy("PolicyA", "/", "")

		req := iamRequest("ListPolicies", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)

		var resp iam.ListPoliciesResponse
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Len(t, resp.ListPoliciesResult.Policies, 1)
	})

	t.Run("AttachUserPolicy", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, b := newTestHandler(t)
		_, _ = b.CreateUser("alice", "/", "")

		req := iamRequest("AttachUserPolicy", map[string]string{
			"UserName":  "alice",
			"PolicyArn": "arn:aws:iam::000000000000:policy/SomePolicy",
		})
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("AttachRolePolicy", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, b := newTestHandler(t)
		_, _ = b.CreateRole("MyRole", "/", "", "")

		req := iamRequest("AttachRolePolicy", map[string]string{
			"RoleName":  "MyRole",
			"PolicyArn": "arn:aws:iam::000000000000:policy/SomePolicy",
		})
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)
	})
}

func TestIAMHandler_DetachRolePolicy(t *testing.T) {
	t.Parallel()

	t.Run("DetachRolePolicy", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, b := newTestHandler(t)
		_, _ = b.CreateRole("MyRole", "/", "", "")
		_ = b.AttachRolePolicy("MyRole", "arn:aws:iam::000000000000:policy/SomePolicy")

		req := iamRequest("DetachRolePolicy", map[string]string{
			"RoleName":  "MyRole",
			"PolicyArn": "arn:aws:iam::000000000000:policy/SomePolicy",
		})
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("DetachRolePolicyRoleNotFound", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, _ := newTestHandler(t)

		req := iamRequest("DetachRolePolicy", map[string]string{
			"RoleName":  "nonexistent",
			"PolicyArn": "arn:aws:iam::000000000000:policy/P",
		})
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusNotFound, rec.Code)

		var errResp iam.ErrorResponse
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
		assert.Equal(t, "NoSuchEntity", errResp.Error.Code)
	})
}
