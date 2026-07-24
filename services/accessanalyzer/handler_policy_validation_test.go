package accessanalyzer_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCheckPolicyOps verifies Check* operations return PASS.
func TestCheckPolicyOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
		path string
	}{
		{
			name: "check_access_not_granted",
			path: "/policy/check-access-not-granted",
			body: map[string]any{
				"access":         []any{},
				"policyDocument": `{"Version":"2012-10-17","Statement":[]}`,
				"policyType":     "IDENTITY_POLICY",
			},
		},
		{
			name: "check_no_new_access",
			path: "/policy/check-no-new-access",
			body: map[string]any{
				"existingPolicyDocument": `{"Version":"2012-10-17","Statement":[]}`,
				"newPolicyDocument":      `{"Version":"2012-10-17","Statement":[]}`,
				"policyType":             "IDENTITY_POLICY",
			},
		},
		{
			name: "check_no_public_access",
			path: "/policy/check-no-public-access",
			body: map[string]any{
				"policyDocument": `{"Version":"2012-10-17","Statement":[]}`,
				"resourceType":   "AWS::S3::Bucket",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, tt.path, tt.body)
			assert.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, "PASS", resp["result"])
		})
	}
}

// TestValidatePolicy verifies POST /policy/validation returns empty findings.
func TestValidatePolicy(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/policy/validation", map[string]any{
		"policyDocument": `{"Version":"2012-10-17","Statement":[]}`,
		"policyType":     "IDENTITY_POLICY",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp["findings"])
}

// TestValidatePolicy_FindingDetailsPopulated verifies that every finding
// ValidatePolicy produces carries a non-empty "findingDetails" message --
// a required member of types.ValidatePolicyFinding ("a localized message
// that explains the finding and provides guidance on how to address it"),
// previously always omitted.
func TestValidatePolicy_FindingDetailsPopulated(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	// No "Version" element triggers the MISSING_VERSION suggestion finding.
	rec := doRequest(t, h, http.MethodPost, "/policy/validation", map[string]any{
		"policyDocument": `{"Statement":[]}`,
		"policyType":     "IDENTITY_POLICY",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	findings, ok := resp["findings"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, findings)

	for _, raw := range findings {
		f, isMap := raw.(map[string]any)
		require.True(t, isMap)
		details, _ := f["findingDetails"].(string)
		assert.NotEmpty(t, details, "findingDetails is required and must not be empty for issueCode %v", f["issueCode"])
	}
}
