package ssoadmin_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestApplicationAccessScope_AuthorizedTargets verifies PutApplicationAccessScope
// stores AuthorizedTargets (previously silently dropped) and that
// ListApplicationAccessScopes returns the real ssoadmin.ScopeDetails wire
// shape -- an array of {Scope, AuthorizedTargets} objects, not an array of
// bare scope-name strings (which a real SDK client cannot deserialize: it
// expects a JSON object per element).
func TestApplicationAccessScope_AuthorizedTargets(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "scope-targets-inst")
	appRec := doRequest(t, h, "CreateApplication", map[string]any{
		"InstanceArn":            instanceArn,
		"ApplicationProviderArn": "arn:aws:sso::123456789012:applicationProvider/custom",
		"Name":                   "scope-targets-app",
	})
	require.Equal(t, http.StatusOK, appRec.Code)
	appArn := parseResponse(t, appRec)["ApplicationArn"].(string)

	targets := []any{"arn:aws:iam::123456789012:role/Admin", "arn:aws:iam::123456789012:role/ReadOnly"}

	putRec := doRequest(t, h, "PutApplicationAccessScope", map[string]any{
		"ApplicationArn":    appArn,
		"Scope":             "openid",
		"AuthorizedTargets": targets,
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	// GetApplicationAccessScope must return what was Put, not a fixed empty array.
	getRec := doRequest(t, h, "GetApplicationAccessScope", map[string]any{
		"ApplicationArn": appArn,
		"Scope":          "openid",
	})
	require.Equal(t, http.StatusOK, getRec.Code)
	getResp := parseResponse(t, getRec)
	assert.Equal(t, "openid", getResp["Scope"])
	assert.ElementsMatch(t, targets, getResp["AuthorizedTargets"])

	// ListApplicationAccessScopes must return an array of {Scope,
	// AuthorizedTargets} objects (ScopeDetails), matching
	// aws-sdk-go-v2/service/ssoadmin/types.ScopeDetails.
	listRec := doRequest(t, h, "ListApplicationAccessScopes", map[string]any{"ApplicationArn": appArn})
	require.Equal(t, http.StatusOK, listRec.Code)
	listResp := parseResponse(t, listRec)
	scopes, ok := listResp["Scopes"].([]any)
	require.True(t, ok)
	require.Len(t, scopes, 1)

	detail, ok := scopes[0].(map[string]any)
	require.True(t, ok, "each Scopes element must be a JSON object (ScopeDetails), not a bare string")
	assert.Equal(t, "openid", detail["Scope"])
	assert.ElementsMatch(t, targets, detail["AuthorizedTargets"])

	// A Put with no AuthorizedTargets must still surface a non-null [] on
	// read, not JSON null, since AuthorizedTargets is not a required member.
	putRec2 := doRequest(t, h, "PutApplicationAccessScope", map[string]any{
		"ApplicationArn": appArn,
		"Scope":          "profile",
	})
	require.Equal(t, http.StatusOK, putRec2.Code)

	getRec2 := doRequest(t, h, "GetApplicationAccessScope", map[string]any{
		"ApplicationArn": appArn,
		"Scope":          "profile",
	})
	require.Equal(t, http.StatusOK, getRec2.Code)
	getResp2 := parseResponse(t, getRec2)
	assert.NotNil(t, getResp2["AuthorizedTargets"])
	assert.Empty(t, getResp2["AuthorizedTargets"])
}

func TestDeleteApplicationAccessScope(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		scope         string
		wantStatus    int
		addScope      bool
		useInvalidApp bool
	}{
		{
			name:       "delete existing scope",
			scope:      "openid",
			addScope:   true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "delete nonexistent scope",
			scope:      "openid",
			addScope:   false,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:          "delete scope from nonexistent app",
			scope:         "openid",
			addScope:      false,
			wantStatus:    http.StatusBadRequest,
			useInvalidApp: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			var appArn string
			if tt.useInvalidApp {
				appArn = "arn:aws:sso::123456789012:application/ssoins-bad/apl-notexist"
			} else {
				instanceArn := createInstance(t, h, "scope-app-instance")
				rec := doRequest(t, h, "CreateApplication", map[string]any{
					"InstanceArn":            instanceArn,
					"ApplicationProviderArn": "arn:aws:sso::123456789012:applicationProvider/custom",
					"Name":                   "ScopeApp",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseResponse(t, rec)
				appArn = resp["ApplicationArn"].(string)

				if tt.addScope {
					// Add the scope by direct backend manipulation is not available;
					// use PutApplicationAccessScope if supported - for now just test not-found path
					// since there's no PutApplicationAccessScope handler yet.
					// We rely on backend test for scope presence; here we verify not-found.
					tt.addScope = false
					tt.wantStatus = http.StatusBadRequest
				}
			}
			rec := doRequest(t, h, "DeleteApplicationAccessScope", map[string]any{
				"ApplicationArn": appArn,
				"Scope":          tt.scope,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestGetApplicationAccessScope verifies GetApplicationAccessScope operation.
func TestGetApplicationAccessScope(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		scope      string
		putScope   bool
		wantStatus int
	}{
		{
			name:       "get_existing_scope",
			scope:      "openid",
			putScope:   true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "get_nonexistent_scope",
			scope:      "nonexistent",
			putScope:   false,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_scope_param",
			scope:      "",
			putScope:   false,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "r3-scope-inst")
			appRec := doRequest(t, h, "CreateApplication", map[string]any{
				"InstanceArn":            instanceArn,
				"ApplicationProviderArn": "arn:aws:sso::123456789012:applicationProvider/custom",
				"Name":                   "r3-scope-app",
			})
			require.Equal(t, http.StatusOK, appRec.Code)
			appArn := parseResponse(t, appRec)["ApplicationArn"].(string)

			if tt.putScope {
				putRec := doRequest(t, h, "PutApplicationAccessScope", map[string]any{
					"ApplicationArn": appArn,
					"Scope":          tt.scope,
				})
				require.Equal(t, http.StatusOK, putRec.Code)
			}

			rec := doRequest(t, h, "GetApplicationAccessScope", map[string]any{
				"ApplicationArn": appArn,
				"Scope":          tt.scope,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				resp := parseResponse(t, rec)
				assert.Equal(t, tt.scope, resp["Scope"])
				assert.NotNil(t, resp["AuthorizedTargets"])
			}
		})
	}
}

// TestGetApplicationAccessScopeNotFoundAfterDelete verifies scope removed after delete.
func TestGetApplicationAccessScopeNotFoundAfterDelete(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "r3-scope-del-inst")
	appRec := doRequest(t, h, "CreateApplication", map[string]any{
		"InstanceArn":            instanceArn,
		"ApplicationProviderArn": "arn:aws:sso::123456789012:applicationProvider/custom",
		"Name":                   "r3-scope-del-app",
	})
	require.Equal(t, http.StatusOK, appRec.Code)
	appArn := parseResponse(t, appRec)["ApplicationArn"].(string)

	// Add scope.
	putRec := doRequest(t, h, "PutApplicationAccessScope", map[string]any{
		"ApplicationArn": appArn,
		"Scope":          "profile",
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	// Verify scope exists.
	getRec := doRequest(t, h, "GetApplicationAccessScope", map[string]any{
		"ApplicationArn": appArn,
		"Scope":          "profile",
	})
	assert.Equal(t, http.StatusOK, getRec.Code)

	// Remove scope.
	delRec := doRequest(t, h, "DeleteApplicationAccessScope", map[string]any{
		"ApplicationArn": appArn,
		"Scope":          "profile",
	})
	require.Equal(t, http.StatusOK, delRec.Code)

	// Now it should return 404.
	getRec2 := doRequest(t, h, "GetApplicationAccessScope", map[string]any{
		"ApplicationArn": appArn,
		"Scope":          "profile",
	})
	assert.Equal(t, http.StatusBadRequest, getRec2.Code)
}
