package dlm_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dlm"
)

// createPolicyWithDetails is like createPolicy (handler_audit1_test.go) but
// lets the caller supply PolicyDetails, needed to exercise the
// resourceTypes/targetTags/tagsToAdd GetLifecyclePolicies filters.
func createPolicyWithDetails(t *testing.T, h *dlm.Handler, description string, details map[string]any) string {
	t.Helper()

	rec := doRequest(t, h, http.MethodPost, "/policies", map[string]any{
		"Description":      description,
		"ExecutionRoleArn": "arn:aws:iam::000000000000:role/dlm-role",
		"State":            "ENABLED",
		"PolicyDetails":    details,
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	policyID, ok := resp["PolicyId"].(string)
	require.True(t, ok, "PolicyId missing from response")

	return policyID
}

func getPoliciesResp(t *testing.T, rec *httptest.ResponseRecorder) []map[string]any {
	t.Helper()

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	raw, ok := resp["Policies"].([]any)
	require.True(t, ok)

	out := make([]map[string]any, len(raw))
	for i, p := range raw {
		out[i] = p.(map[string]any)
	}

	return out
}

// TestHandler_GetLifecyclePolicies_MultiValuePolicyIDs verifies a real SDK
// client's wire format for the policyIds filter: the same query key repeated
// once per ID (policyIds=a&policyIds=b), never comma-joined. A prior version
// of handleGetLifecyclePolicies used url.Values.Get, which only observes the
// FIRST occurrence of a repeated key, silently dropping every id but the
// first from the filter.
func TestHandler_GetLifecyclePolicies_MultiValuePolicyIDs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	id1 := createPolicy(t, h)
	id2 := createPolicy(t, h)
	_ = createPolicy(t, h) // a third, unfiltered policy

	path := fmt.Sprintf("/policies?policyIds=%s&policyIds=%s", id1, id2)

	rec := doRequest(t, h, http.MethodGet, path, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	policies := getPoliciesResp(t, rec)
	require.Len(t, policies, 2, "both repeated policyIds query values must be honored")

	gotIDs := map[string]bool{}
	for _, p := range policies {
		gotIDs[p["PolicyId"].(string)] = true
	}

	assert.True(t, gotIDs[id1])
	assert.True(t, gotIDs[id2])
}

// TestHandler_GetLifecyclePolicies_PolicyTypeInResponse verifies the
// LifecyclePolicySummary wire shape includes PolicyType (previously absent
// from this handler's summary JSON).
func TestHandler_GetLifecyclePolicies_PolicyTypeInResponse(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createPolicyWithDetails(t, h, "img", map[string]any{"PolicyType": "IMAGE_MANAGEMENT"})

	rec := doRequest(t, h, http.MethodGet, "/policies", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	policies := getPoliciesResp(t, rec)
	require.Len(t, policies, 1)
	assert.Equal(t, "IMAGE_MANAGEMENT", policies[0]["PolicyType"])
}

// TestHandler_GetLifecyclePolicies_ResourceTypesQueryFilter drives the
// resourceTypes filter through the full HTTP path (query string -> handler
// -> backend), matching the real client's repeated-query-key wire format.
func TestHandler_GetLifecyclePolicies_ResourceTypesQueryFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createPolicyWithDetails(t, h, "volume", map[string]any{"ResourceTypes": []any{"VOLUME"}})
	createPolicyWithDetails(t, h, "instance", map[string]any{"ResourceTypes": []any{"INSTANCE"}})

	rec := doRequest(t, h, http.MethodGet, "/policies?resourceTypes=INSTANCE", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	policies := getPoliciesResp(t, rec)
	require.Len(t, policies, 1)
	assert.Equal(t, "instance", policies[0]["Description"])
}

// TestHandler_GetLifecyclePolicies_TargetTagsQueryFilter drives the
// targetTags filter (format key=value) through the full HTTP path.
func TestHandler_GetLifecyclePolicies_TargetTagsQueryFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createPolicyWithDetails(t, h, "prod", map[string]any{
		"TargetTags": []any{map[string]any{"Key": "env", "Value": "prod"}},
	})
	createPolicyWithDetails(t, h, "dev", map[string]any{
		"TargetTags": []any{map[string]any{"Key": "env", "Value": "dev"}},
	})

	rec := doRequest(t, h, http.MethodGet, "/policies?targetTags=env%3Dprod", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	policies := getPoliciesResp(t, rec)
	require.Len(t, policies, 1)
	assert.Equal(t, "prod", policies[0]["Description"])
}

// TestHandler_GetLifecyclePolicies_MatcherRouted proves the RouteMatcher and
// the query-parameter parsing work together end-to-end: a request is first
// accepted by RouteMatcher (the way the real service router would decide
// whether DLM should handle it), then dispatched through Handler(), so the
// policyIds fix above is proven under the same routing path a real request
// takes rather than only via a direct Handler() call.
func TestHandler_GetLifecyclePolicies_MatcherRouted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	id1 := createPolicy(t, h)
	id2 := createPolicy(t, h)

	path := fmt.Sprintf("/policies?policyIds=%s&policyIds=%s", id1, id2)
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)

	require.True(t, h.RouteMatcher()(c), "RouteMatcher must accept GET /policies?policyIds=...")
	require.Equal(t, "GetLifecyclePolicies", h.ExtractOperation(c))

	require.NoError(t, h.Handler()(c))
	require.Equal(t, http.StatusOK, rec.Code)

	policies := getPoliciesResp(t, rec)
	assert.Len(t, policies, 2)
}

// ---------------------------------------------------------------------------
// CreateLifecyclePolicy: required-field validation over HTTP
// ---------------------------------------------------------------------------

// TestHandler_CreateLifecyclePolicy_MissingRequiredFields verifies that
// omitting ExecutionRoleArn or Description -- both required members of the
// real CreateLifecyclePolicyInput -- surfaces as a 400 InvalidRequestException
// rather than silently fabricating a policy the real API could never produce.
func TestHandler_CreateLifecyclePolicy_MissingRequiredFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
	}{
		{
			name: "missing Description",
			body: map[string]any{
				"ExecutionRoleArn": "arn:aws:iam::000000000000:role/dlm-role",
				"State":            "ENABLED",
			},
		},
		{
			name: "missing ExecutionRoleArn",
			body: map[string]any{
				"Description": "a policy",
				"State":       "ENABLED",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/policies", tc.body)

			require.Equal(t, http.StatusBadRequest, rec.Code)

			var resp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, "InvalidRequestException", resp["__type"])
		})
	}
}
