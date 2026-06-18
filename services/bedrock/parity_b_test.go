package bedrock_test

// parity_b_test.go — §B parity: ValidationException returns HTTP 400 (not 500)

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// ValidationException HTTP status
// ---------------------------------------------------------------------------

// TestParity_ValidationException_Returns400 verifies that invalid request
// bodies to Bedrock endpoints return HTTP 400 with a ValidationException error
// code rather than HTTP 500 InternalFailure.
func TestParity_ValidationException_Returns400(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   any
		name   string
		method string
		path   string
	}{
		{
			name:   "CreateModelCustomizationJob_null_body",
			method: http.MethodPost,
			path:   "/model-customization-jobs",
			body:   nil,
		},
		{
			name:   "CreateModelCustomizationJob_empty_object",
			method: http.MethodPost,
			path:   "/model-customization-jobs",
			body:   map[string]any{},
		},
		{
			name:   "CreateModelCopyJob_null_body",
			method: http.MethodPost,
			path:   "/model-copy-jobs",
			body:   nil,
		},
		{
			name:   "CreateModelCopyJob_empty_object",
			method: http.MethodPost,
			path:   "/model-copy-jobs",
			body:   map[string]any{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tc.method, tc.path, tc.body)

			assert.Equal(t, http.StatusBadRequest, rec.Code,
				"ValidationException must return 400, not 500")

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			errType, _ := resp["__type"].(string)
			assert.Contains(t, errType, "ValidationException",
				"error type must be ValidationException")
		})
	}
}

// TestParity_ValidationException_NotInternalError verifies that
// ValidationException is never confused with InternalFailure (HTTP 500).
func TestParity_ValidationException_NotInternalError(t *testing.T) {
	t.Parallel()

	endpoints := []struct {
		method string
		path   string
	}{
		{http.MethodPost, "/model-customization-jobs"},
		{http.MethodPost, "/model-copy-jobs"},
	}

	for _, ep := range endpoints {
		t.Run(ep.method+"_"+ep.path, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, ep.method, ep.path, nil)

			assert.NotEqual(t, http.StatusInternalServerError, rec.Code,
				"validation failure must not return 500")
		})
	}
}

// TestParity_ValidModelCopyJob_Returns201 confirms that a well-formed
// CreateModelCopyJob request succeeds — distinguishing it from invalid requests.
func TestParity_ValidModelCopyJob_Returns201(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model-copy-jobs", map[string]any{
		"sourceModelArn": "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-v2",
	})

	assert.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp, "jobArn", "successful response must include jobArn")
}

// TestParity_ValidationException_ErrorShape verifies the response body shape
// for ValidationException matches the Bedrock error envelope.
func TestParity_ValidationException_ErrorShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
	}{
		{"model-customization-jobs", "/model-customization-jobs"},
		{"model-copy-jobs", "/model-copy-jobs"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, tc.path, nil)

			require.Equal(t, http.StatusBadRequest, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			_, hasType := resp["__type"]
			assert.True(t, hasType, "error response must have '__type' field")

			_, hasMsg := resp["message"]
			assert.True(t, hasMsg, "error response must have 'message' field")
		})
	}
}
