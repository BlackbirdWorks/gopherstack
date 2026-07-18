package sagemakerruntime_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- InvokeEndpointWithResponseStream tests ---

func TestHandler_InvokeEndpointWithResponseStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		headers         map[string]string
		wantContentType string
		wantVariant     string
	}{
		{name: "defaults", wantContentType: "application/octet-stream", wantVariant: "AllTraffic"},
		{
			name: "sdk_bound_response_headers",
			headers: map[string]string{
				"X-Amzn-Sagemaker-Accept":            "application/json",
				"X-Amzn-Sagemaker-Custom-Attributes": "trace=stream",
				"X-Amzn-Sagemaker-Target-Variant":    "green",
			},
			wantContentType: "application/json",
			wantVariant:     "green",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequestWithHeaders(t, h, http.MethodPost,
				"/endpoints/my-endpoint/invocations-response-stream",
				map[string]any{"data": "stream input"}, tt.headers)

			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, "application/vnd.amazon.eventstream", rec.Header().Get("Content-Type"))
			assert.Equal(t, tt.wantContentType, rec.Header().Get("X-Amzn-Sagemaker-Content-Type"))
			assert.Equal(t, tt.wantVariant, rec.Header().Get("X-Amzn-Invoked-Production-Variant"))
			assert.Equal(t, tt.headers["X-Amzn-Sagemaker-Custom-Attributes"],
				rec.Header().Get("X-Amzn-Sagemaker-Custom-Attributes"))
			assert.Greater(t, len(rec.Body.Bytes()), 12, "response should contain event stream prelude")

			invocations := h.Backend.ListInvocations()
			require.Len(t, invocations, 1)
			assert.Equal(t, "InvokeEndpointWithResponseStream", invocations[0].Operation)
		})
	}
}

// TestResponseStreamContentType verifies that InvokeEndpointWithResponseStream
// uses the event-stream content type and echoes X-Amzn-Sagemaker-Content-Type from
// the Accept header.
func TestResponseStreamContentType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		headers         map[string]string
		wantContentType string
	}{
		{
			name:            "default_octet_stream",
			headers:         nil,
			wantContentType: "application/octet-stream",
		},
		{
			name: "accept_json_reflected",
			headers: map[string]string{
				"X-Amzn-Sagemaker-Accept": "application/json",
			},
			wantContentType: "application/json",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequestWithHeaders(
				t, h, http.MethodPost,
				"/endpoints/ep/invocations-response-stream",
				nil,
				tt.headers,
			)

			require.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, "application/vnd.amazon.eventstream", rec.Header().Get("Content-Type"))
			assert.Equal(t, tt.wantContentType, rec.Header().Get("X-Amzn-Sagemaker-Content-Type"))
		})
	}
}
