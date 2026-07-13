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
