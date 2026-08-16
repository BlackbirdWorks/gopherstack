package kinesis_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesis"
)

// TestResourcePolicyCount verifies ResourcePolicyCount export helper.
func TestResourcePolicyCount(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	assert.Equal(t, 0, b.ResourcePolicyCount())

	rec := doRequest(t, h, "PutResourcePolicy", map[string]any{
		"ResourceARN": "arn:aws:kinesis:us-east-1:123:stream/my-stream",
		"Policy":      `{"Version":"2012-10-17"}`,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	assert.Equal(t, 1, b.ResourcePolicyCount())
}

// testPolicy is a sample resource policy for testing.
const testPolicy = `{"Version":"2012-10-17","Statement":[` +
	`{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"kinesis:*"}]}`

// TestResourcePolicyLifecycle verifies Put/Get/Delete of resource policies.
func TestResourcePolicyLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		resourceARN string
		policy      string
	}{
		{
			name:        "stream_resource_policy",
			resourceARN: "arn:aws:kinesis:us-east-1:123456789012:stream/my-stream",
			policy:      testPolicy,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// GetResourcePolicy on non-existent policy should 400.
			rec := doRequest(t, h, "GetResourcePolicy", map[string]any{
				"ResourceARN": tt.resourceARN,
			})
			assert.Equal(t, http.StatusBadRequest, rec.Code)

			// PutResourcePolicy.
			rec = doRequest(t, h, "PutResourcePolicy", map[string]any{
				"ResourceARN": tt.resourceARN,
				"Policy":      tt.policy,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			// GetResourcePolicy should return the stored policy.
			rec = doRequest(t, h, "GetResourcePolicy", map[string]any{
				"ResourceARN": tt.resourceARN,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var policyResp struct {
				Policy string `json:"Policy"`
			}

			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &policyResp))
			assert.Equal(t, tt.policy, policyResp.Policy)

			// DeleteResourcePolicy.
			rec = doRequest(t, h, "DeleteResourcePolicy", map[string]any{
				"ResourceARN": tt.resourceARN,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			// GetResourcePolicy after deletion should 400.
			rec = doRequest(t, h, "GetResourcePolicy", map[string]any{
				"ResourceARN": tt.resourceARN,
			})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// TestDeleteResourcePolicy_NotFound verifies 400 when deleting a non-existent policy.
func TestDeleteResourcePolicy_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "DeleteResourcePolicy", map[string]any{
		"ResourceARN": "arn:aws:kinesis:us-east-1:123:stream/no-policy",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp struct {
		Type string `json:"__type"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "ResourceNotFoundException", errResp.Type)
}
