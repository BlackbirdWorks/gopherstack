package vpclattice_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAuthPolicy tests put/get/delete auth policy.
func TestAuthPolicy(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)

	// create service
	recSvc := doRequest(t, h, http.MethodPost, "/services", map[string]any{"name": "svc-auth"})
	require.Equal(t, http.StatusCreated, recSvc.Code)
	svcID, _ := parseBody(t, recSvc)["id"].(string)

	policy := `{"Version":"2012-10-17","Statement":[]}`

	// put
	rec := doRequest(t, h, http.MethodPut, "/authpolicy/"+svcID, map[string]any{"policy": policy})
	assert.Equal(t, http.StatusOK, rec.Code)
	resp := parseBody(t, rec)
	assert.Equal(t, policy, resp["policy"])

	// get
	rec = doRequest(t, h, http.MethodGet, "/authpolicy/"+svcID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	resp = parseBody(t, rec)
	assert.Equal(t, policy, resp["policy"])

	// delete
	rec = doRequest(t, h, http.MethodDelete, "/authpolicy/"+svcID, nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

// TestGetAuthPolicyNotFoundReturns404 verifies that GetAuthPolicy returns
// 404 when no policy has been set on the resource. Real AWS returns
// ResourceNotFoundException; the emulator previously returned a 200 with an
// empty policy string, making it impossible to distinguish "not set" from
// "policy is empty string".
func TestGetAuthPolicyNotFoundReturns404(t *testing.T) {
	t.Parallel()

	tests := []struct {
		resourceID string
		name       string
	}{
		{
			name:       "unknown_service_id",
			resourceID: "svc-abc123notexist",
		},
		{
			name:       "existing_service_without_policy",
			resourceID: "", // populated after creating a service below
		},
	}

	h := newTestHandler(t)
	svcRec := doRequest(t, h, http.MethodPost, "/services", map[string]any{"name": "parity-auth-svc"})
	require.Equal(t, http.StatusCreated, svcRec.Code)
	svcID, _ := parseBody(t, svcRec)["id"].(string)
	require.NotEmpty(t, svcID)
	tests[1].resourceID = svcID

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, http.MethodGet, "/authpolicy/"+tt.resourceID, nil)
			assert.Equal(t, http.StatusNotFound, rec.Code,
				"GetAuthPolicy on resource with no policy must return 404")
		})
	}
}

// TestGetAuthPolicyAfterPutReturns200 verifies the happy-path still works
// after the not-found fix: setting a policy and then getting it returns 200.
func TestGetAuthPolicyAfterPutReturns200(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	svcRec := doRequest(t, h, http.MethodPost, "/services", map[string]any{"name": "parity-auth-set"})
	require.Equal(t, http.StatusCreated, svcRec.Code)
	svcID, _ := parseBody(t, svcRec)["id"].(string)
	require.NotEmpty(t, svcID)

	policy := `{"Version":"2012-10-17","Statement":[]}`
	putRec := doRequest(t, h, http.MethodPut, "/authpolicy/"+svcID, map[string]any{"policy": policy})
	require.Equal(t, http.StatusOK, putRec.Code, "PutAuthPolicy must succeed")

	getRec := doRequest(t, h, http.MethodGet, "/authpolicy/"+svcID, nil)
	assert.Equal(t, http.StatusOK, getRec.Code, "GetAuthPolicy after Put must return 200")

	resp := parseBody(t, getRec)
	assert.Equal(t, policy, resp["policy"])
}
