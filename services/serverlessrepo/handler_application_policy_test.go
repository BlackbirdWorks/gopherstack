package serverlessrepo_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/serverlessrepo"
)

func TestHandler_GetApplicationPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*serverlessrepo.Handler)
		name     string
		appName  string
		wantLen  int
		wantCode int
	}{
		{
			name:     "returns empty policy for new application",
			appName:  "my-app",
			wantLen:  0,
			wantCode: http.StatusOK,
		},
		{
			name:    "returns existing policy statements",
			appName: "my-app",
			setup: func(h *serverlessrepo.Handler) {
				_, _ = h.Backend.PutApplicationPolicy("my-app", []*serverlessrepo.ApplicationPolicyStatement{
					{Actions: []string{"deploy"}, Principals: []string{"*"}},
				})
			},
			wantLen:  1,
			wantCode: http.StatusOK,
		},
		{
			name:     "app not found returns 404",
			appName:  "not-found",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "1.0.0", nil, "", "", "")
			require.NoError(t, err)

			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doServerlessRepoRequest(t, h, http.MethodGet, "/applications/"+tt.appName+"/policy", nil)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				stmts, ok := resp["statements"].([]any)
				require.True(t, ok)
				assert.Len(t, stmts, tt.wantLen)
			}
		})
	}
}

func TestGetApplicationPolicy_EmptyForNewApp(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodGet, "/applications/my-app/policy", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	stmts, ok := resp["statements"].([]any)
	require.True(t, ok, "statements must be an array")
	assert.Empty(t, stmts, "new app should have empty policy statements")
}

func TestGetApplicationPolicy_EmptyPolicy_ReturnsEmptyStatements(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("no-policy-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodGet, "/applications/no-policy-app/policy", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	stmts := resp["statements"].([]any)
	assert.Empty(t, stmts)
}

func TestGetApplicationPolicy_ARNForm_Routing(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("policy-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	// PUT policy using ARN form
	path := arnPathFor("policy-app") + "/policy"
	rec := doServerlessRepoRequestEncoded(t, h, http.MethodPut, path, map[string]any{
		"statements": []map[string]any{
			{"actions": []string{"Deploy"}, "principals": []string{"*"}},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code, "PUT with ARN path should route to PutApplicationPolicy")

	// GET policy using ARN form
	rec = doServerlessRepoRequestEncoded(t, h, http.MethodGet, path, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	stmts, ok := resp["statements"].([]any)
	require.True(t, ok)
	assert.Len(t, stmts, 1)
}

func TestHandler_PutApplicationPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		appName  string
		wantLen  int
		wantCode int
	}{
		{
			name:    "sets policy statements successfully",
			appName: "my-app",
			body: map[string]any{
				"statements": []map[string]any{
					{
						"actions":    []string{"deploy"},
						"principals": []string{"*"},
					},
				},
			},
			wantLen:  1,
			wantCode: http.StatusOK,
		},
		{
			name:    "sets multiple policy statements",
			appName: "my-app",
			body: map[string]any{
				"statements": []map[string]any{
					{
						"actions":     []string{"deploy"},
						"principals":  []string{"111111111111"},
						"statementId": "stmt-1",
					},
					{
						"actions":     []string{"deploy"},
						"principals":  []string{"222222222222"},
						"statementId": "stmt-2",
					},
				},
			},
			wantLen:  2,
			wantCode: http.StatusOK,
		},
		{
			name:    "app not found returns 404",
			appName: "not-found",
			body: map[string]any{
				"statements": []map[string]any{},
			},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "1.0.0", nil, "", "", "")
			require.NoError(t, err)

			rec := doServerlessRepoRequest(t, h, http.MethodPut, "/applications/"+tt.appName+"/policy", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				stmts, ok := resp["statements"].([]any)
				require.True(t, ok)
				assert.Len(t, stmts, tt.wantLen)
			}
		})
	}
}

func TestPutApplicationPolicy_AWSActions(t *testing.T) {
	t.Parallel()

	tests := []string{
		"Deploy",
		"UnshareApplication",
		"GetApplication",
		"CreateCloudFormationChangeSet",
		"CreateCloudFormationTemplate",
		"ListApplicationVersions",
		"ListApplicationDependencies",
		"SearchApplications",
	}
	for _, action := range tests {
		t.Run(action, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_, err := h.Backend.CreateApplication("policy-app", "desc", "author", "", "", nil, "", "", "")
			require.NoError(t, err)

			rec := doServerlessRepoRequest(t, h, http.MethodPut, "/applications/policy-app/policy", map[string]any{
				"statements": []map[string]any{{"actions": []string{action}, "principals": []string{"*"}}},
			})
			assert.Equal(t, http.StatusOK, rec.Code)
		})
	}
}

func TestPutApplicationPolicy_InvalidAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodPut, "/applications/my-app/policy", map[string]any{
		"statements": []map[string]any{
			{"actions": []string{"s3:GetObject"}, "principals": []string{"*"}},
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "unsupported policy action should return 400")
}

func TestPutApplicationPolicy_CaseSensitive_Deploy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		action string
	}{
		{name: "lowercase_deploy", action: "deploy"},
		{name: "mixed_case_Deploy", action: "Deploy"},
		{name: "lowercase_GetApplication", action: "getapplication"},
		{name: "mixed_case_GetApplication", action: "GetApplication"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "", nil, "", "", "")
			require.NoError(t, err)

			rec := doServerlessRepoRequest(t, h, http.MethodPut, "/applications/my-app/policy", map[string]any{
				"statements": []map[string]any{
					{"actions": []string{tt.action}, "principals": []string{"*"}},
				},
			})
			assert.Equal(t, http.StatusOK, rec.Code, "action %q should be accepted", tt.action)
		})
	}
}

func TestPutApplicationPolicy_ValidationError_EmptyActions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "1.0.0", nil, "", "", "")
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodPut, "/applications/my-app/policy", map[string]any{
		"statements": []map[string]any{
			{
				"actions":    []string{},
				"principals": []string{"*"},
			},
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestPutApplicationPolicy_ValidationError_EmptyPrincipals(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "1.0.0", nil, "", "", "")
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodPut, "/applications/my-app/policy", map[string]any{
		"statements": []map[string]any{
			{
				"actions":    []string{"deploy"},
				"principals": []string{},
			},
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestErrValidation_MapsTo400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "1.0.0", nil, "", "", "")
	require.NoError(t, err)

	// PutApplicationPolicy with no actions triggers ErrValidation
	rec := doServerlessRepoRequest(t, h, http.MethodPut, "/applications/my-app/policy", map[string]any{
		"statements": []map[string]any{
			{"actions": []string{}, "principals": []string{"*"}},
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "BadRequestException", resp["__type"])
}

func TestPutApplicationPolicy_AutoGeneratesStatementID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "1.0.0", nil, "", "", "")
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodPut, "/applications/my-app/policy", map[string]any{
		"statements": []map[string]any{
			{
				"actions":    []string{"deploy"},
				"principals": []string{"*"},
				// no statementId
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	stmts, ok := resp["statements"].([]any)
	require.True(t, ok)
	require.Len(t, stmts, 1)

	stmt := stmts[0].(map[string]any)
	assert.NotEmpty(t, stmt["statementId"], "statementId should be auto-generated")
}

func TestPolicyResponse_NonNilSlices(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "1.0.0", nil, "", "", "")
	require.NoError(t, err)

	_, err = h.Backend.PutApplicationPolicy("my-app", []*serverlessrepo.ApplicationPolicyStatement{
		{Actions: []string{"deploy"}, Principals: []string{"*"}},
	})
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodGet, "/applications/my-app/policy", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	stmts, ok := resp["statements"].([]any)
	require.True(t, ok)
	require.Len(t, stmts, 1)

	stmt := stmts[0].(map[string]any)
	actions, ok := stmt["actions"].([]any)
	require.True(t, ok, "actions should be non-nil array")
	assert.NotNil(t, actions)
	principals, ok := stmt["principals"].([]any)
	require.True(t, ok, "principals should be non-nil array")
	assert.NotNil(t, principals)
	principalOrgIDs := stmt["principalOrgIDs"]
	assert.IsType(t, []any{}, principalOrgIDs, "principalOrgIDs should be an empty array, not nil")
}

func TestPutApplicationPolicy_PrincipalOrgIDs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("org-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodPut, "/applications/org-app/policy", map[string]any{
		"statements": []map[string]any{
			{
				"actions":         []string{"Deploy"},
				"principals":      []string{"*"},
				"principalOrgIDs": []string{"o-abc123", "o-def456"},
				"statementId":     "stmt-1",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var putResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &putResp))
	stmts := putResp["statements"].([]any)
	stmt := stmts[0].(map[string]any)
	orgIDs := stmt["principalOrgIDs"].([]any)
	assert.Len(t, orgIDs, 2)
	assert.Equal(t, "o-abc123", orgIDs[0])

	// GET should return orgIDs too
	rec2 := doServerlessRepoRequest(t, h, http.MethodGet, "/applications/org-app/policy", nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &getResp))
	stmts2 := getResp["statements"].([]any)
	stmt2 := stmts2[0].(map[string]any)
	orgIDs2 := stmt2["principalOrgIDs"].([]any)
	assert.Len(t, orgIDs2, 2)
}

func TestPutApplicationPolicy_ReplacesExistingStatements(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("policy-replace-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	// Set initial policy with 2 statements
	rec := doServerlessRepoRequest(t, h, http.MethodPut, "/applications/policy-replace-app/policy", map[string]any{
		"statements": []map[string]any{
			{"actions": []string{"Deploy"}, "principals": []string{"111111111111"}, "statementId": "s1"},
			{"actions": []string{"Deploy"}, "principals": []string{"222222222222"}, "statementId": "s2"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Replace with 1 statement
	rec = doServerlessRepoRequest(t, h, http.MethodPut, "/applications/policy-replace-app/policy", map[string]any{
		"statements": []map[string]any{
			{"actions": []string{"SearchApplications"}, "principals": []string{"*"}, "statementId": "s3"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doServerlessRepoRequest(t, h, http.MethodGet, "/applications/policy-replace-app/policy", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	stmts := resp["statements"].([]any)
	require.Len(t, stmts, 1, "policy should have exactly 1 statement after replacement")
	assert.Equal(t, "s3", stmts[0].(map[string]any)["statementId"])
}
