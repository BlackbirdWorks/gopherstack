package azurearm_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	exAPI       = "?api-version=2022-07-01-preview"
	exBuiltinID = "/providers/Microsoft.Authorization/policyDefinitions/e56962a6-4747-49cd-b67b-bf8b01975c4c"
	exSetID     = "/providers/Microsoft.Authorization/policySetDefinitions/0a2ebd47-3fb9-4735-a006-b7f31ddadd9f"
)

func (s *authStack) putAssignmentAt(t *testing.T, scope, name, defID string) string {
	t.Helper()

	path := scope + "/providers/Microsoft.Authorization/policyAssignments/" + name
	status, out := s.raw(t, http.MethodPut, path+"?api-version=2022-06-01",
		`{"properties":{"policyDefinitionId":"`+defID+`"}}`)
	require.Less(t, status, http.StatusMultipleChoices, out)

	return path
}

func exemptionJSON(assignmentID, extra string) string {
	return `{"properties":{"policyAssignmentId":"` + assignmentID + `","exemptionCategory":"Waiver"` + extra + `}}`
}

func TestAuthorizationPolicyExemptions(t *testing.T) {
	t.Parallel()

	s := newAuthStack(t)
	assign := s.putAssignmentAt(t, authSubScope, "exassign", exBuiltinID)
	setAssign := s.putAssignmentAt(t, authSubScope, "exset", exSetID)
	path := authRGScope + "/providers/Microsoft.Authorization/policyExemptions/"

	tests := []struct {
		name   string
		scope  string
		body   string
		status int
	}{
		{name: "create", scope: path + "ex1", body: exemptionJSON(assign, `,"displayName":"d","metadata":{"a":"b"}`),
			status: http.StatusCreated},
		{name: "mitigated", scope: path + "ex2", status: http.StatusCreated,
			body: `{"properties":{"policyAssignmentId":"` + assign + `","exemptionCategory":"Mitigated"}}`},
		{name: "missing assignment id", scope: path + "ex3", body: `{"properties":{"exemptionCategory":"Waiver"}}`,
			status: http.StatusBadRequest},
		{name: "bad category", scope: path + "ex4", status: http.StatusBadRequest,
			body: `{"properties":{"policyAssignmentId":"` + assign + `","exemptionCategory":"Other"}}`},
		{name: "unknown assignment", scope: path + "ex5", status: http.StatusBadRequest,
			body: exemptionJSON(authSubScope+"/providers/Microsoft.Authorization/policyAssignments/ghost", "")},
		{name: "bad expiry", scope: path + "ex6", body: exemptionJSON(assign, `,"expiresOn":"tomorrow"`),
			status: http.StatusBadRequest},
		{name: "refs on a non-set assignment", scope: path + "ex7",
			body: exemptionJSON(assign, `,"policyDefinitionReferenceIds":["x"]`), status: http.StatusBadRequest},
		{name: "unknown ref on a set assignment", scope: path + "ex8",
			body: exemptionJSON(setAssign, `,"policyDefinitionReferenceIds":["nope"]`), status: http.StatusBadRequest},
		{name: "bad scope validation", scope: path + "ex9", status: http.StatusBadRequest,
			body: exemptionJSON(assign, `,"assignmentScopeValidation":"x"`)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			status, out := s.raw(t, http.MethodPut, tt.scope+exAPI, tt.body)
			require.Equal(t, tt.status, status, out)

			if tt.status != http.StatusCreated {
				return
			}

			assert.Equal(t, "Microsoft.Authorization/policyExemptions", out["type"])
			assert.Equal(t, tt.scope, out["id"])
			assert.NotNil(t, out["systemData"])

			status, _ = s.raw(t, http.MethodPut, tt.scope+exAPI, tt.body)
			assert.Equal(t, http.StatusOK, status, "an update answers 200")
		})
	}
}

func TestAuthorizationPolicyExemptionScope(t *testing.T) {
	t.Parallel()

	s := newAuthStack(t)
	s.putAccount(t)

	rgAssign := s.putAssignmentAt(t, authRGScope, "rgassign", exBuiltinID)

	subPath := authSubScope + "/providers/Microsoft.Authorization/policyExemptions/wide"
	status, out := s.raw(t, http.MethodPut, subPath+exAPI, exemptionJSON(rgAssign, ""))
	require.Equal(t, http.StatusBadRequest, status, out)

	status, out = s.raw(t, http.MethodPut, subPath+exAPI,
		exemptionJSON(rgAssign, `,"assignmentScopeValidation":"DoNotValidate"`))
	require.Equal(t, http.StatusCreated, status, out)

	acctPath := authAcctScope + "/providers/Microsoft.Authorization/policyExemptions/narrow"
	status, out = s.raw(t, http.MethodPut, acctPath+exAPI, exemptionJSON(rgAssign, ""))
	require.Equal(t, http.StatusCreated, status, out)
}

func TestAuthorizationPolicyExemptionLifecycle(t *testing.T) {
	t.Parallel()

	s := newAuthStack(t)
	assign := s.putAssignmentAt(t, authSubScope, "lifeassign", exBuiltinID)
	base := authRGScope + "/providers/Microsoft.Authorization/policyExemptions"

	status, out := s.raw(t, http.MethodPut, base+"/e1"+exAPI,
		exemptionJSON(assign, `,"expiresOn":"2000-01-01T00:00:00Z"`))
	require.Equal(t, http.StatusCreated, status, out)

	status, out = s.raw(t, http.MethodPut, base+"/e2"+exAPI, exemptionJSON(assign, ""))
	require.Equal(t, http.StatusCreated, status, out)

	list := func(scope, filter string) []any {
		t.Helper()

		q := exAPI
		if filter != "" {
			q += "&$filter=" + url.QueryEscape(filter)
		}

		code, body := s.raw(t, http.MethodGet, scope+"/providers/Microsoft.Authorization/policyExemptions"+q, "")
		require.Equal(t, http.StatusOK, code, body)

		v, _ := body["value"].([]any)

		return v
	}

	assert.Len(t, list(authRGScope, ""), 2)
	assert.Len(t, list(authRGScope, "excludeExpired()"), 1)
	assert.Len(t, list(authRGScope, "atExactScope()"), 2)
	assert.Empty(t, list(authSubScope, "atExactScope()"))
	assert.Len(t, list(authRGScope, "policyAssignmentId eq '"+assign+"'"), 2)
	assert.Empty(t, list(authRGScope, "policyAssignmentId eq '/nope'"))

	status, got := s.raw(t, http.MethodGet, base+"/e2"+exAPI, "")
	require.Equal(t, http.StatusOK, status, got)

	status, got = s.raw(t, http.MethodPatch, base+"/e2"+exAPI,
		`{"properties":{"assignmentScopeValidation":"DoNotValidate","resourceSelectors":[{"name":"r"}]}}`)
	require.Equal(t, http.StatusOK, status, got)
	assert.Equal(t, "DoNotValidate", props(got)["assignmentScopeValidation"])

	status, _ = s.raw(t, http.MethodPatch, base+"/nope"+exAPI, `{"properties":{}}`)
	assert.Equal(t, http.StatusNotFound, status)

	status, _ = s.raw(t, http.MethodDelete, base+"/e2"+exAPI, "")
	assert.Equal(t, http.StatusOK, status)

	status, _ = s.raw(t, http.MethodDelete, base+"/e2"+exAPI, "")
	assert.Equal(t, http.StatusNoContent, status)

	status, got = s.raw(t, http.MethodGet, base+"/e2"+exAPI, "")
	require.Equal(t, http.StatusNotFound, status)
	assert.Equal(t, "PolicyExemptionNotFound", authErrCode(t, got))
}
