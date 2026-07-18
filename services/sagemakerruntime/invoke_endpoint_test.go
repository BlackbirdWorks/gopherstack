package sagemakerruntime_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- InvokeEndpoint tests ---

func TestHandler_InvokeEndpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body            any
		headers         map[string]string
		name            string
		endpointName    string
		wantContentType string
		wantVariant     string
		wantSession     bool
	}{
		{
			name:            "default_opaque_response",
			endpointName:    "my-endpoint",
			body:            map[string]any{"data": "test input"},
			wantContentType: "application/octet-stream",
			wantVariant:     "AllTraffic",
		},
		{
			name:         "bound_headers_and_new_session",
			endpointName: "session-endpoint",
			body:         nil,
			headers: map[string]string{
				"Accept":                             "application/json",
				"X-Amzn-Sagemaker-Custom-Attributes": "trace=123",
				"X-Amzn-Sagemaker-Session-Id":        "NEW_SESSION",
				"X-Amzn-Sagemaker-Target-Variant":    "blue",
			},
			wantContentType: "application/json",
			wantVariant:     "blue",
			wantSession:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequestWithHeaders(
				t, h, http.MethodPost, "/endpoints/"+tt.endpointName+"/invocations", tt.body, tt.headers,
			)

			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, tt.wantContentType, rec.Header().Get("Content-Type"))
			assert.Equal(t, tt.wantVariant, rec.Header().Get("X-Amzn-Invoked-Production-Variant"))
			assert.Equal(t, "mock response from Gopherstack", rec.Body.String())
			assert.Equal(t, tt.headers["X-Amzn-Sagemaker-Custom-Attributes"],
				rec.Header().Get("X-Amzn-Sagemaker-Custom-Attributes"))
			assert.Equal(t, tt.wantSession, rec.Header().Get("X-Amzn-Sagemaker-New-Session-Id") != "")
			assert.Len(t, h.Backend.ListSessions(), boolToInt(tt.wantSession))

			invocations := h.Backend.ListInvocations()
			require.Len(t, invocations, 1)
			assert.Equal(t, "InvokeEndpoint", invocations[0].Operation)
			assert.Equal(t, tt.endpointName, invocations[0].EndpointName)
		})
	}
}

// TestSessionLifecycle verifies NEW_SESSION creation and subsequent
// session-touch behaviour match AWS semantics.
func TestSessionLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // fieldalignment: readability over micro-optimization
		name         string
		headers      map[string]string
		wantSessions int
		wantNewSessH bool
	}{
		{
			name:         "no_session_header_creates_nothing",
			headers:      nil,
			wantSessions: 0,
			wantNewSessH: false,
		},
		{
			name: "new_session_header_creates_session",
			headers: map[string]string{
				"X-Amzn-Sagemaker-Session-Id": "NEW_SESSION",
			},
			wantSessions: 1,
			wantNewSessH: true,
		},
		{
			name: "existing_session_id_touches_without_creating",
			headers: map[string]string{
				"X-Amzn-Sagemaker-Session-Id": "some-pre-existing-id",
			},
			wantSessions: 0,
			wantNewSessH: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequestWithHeaders(
				t, h, http.MethodPost,
				"/endpoints/ep/invocations",
				nil,
				tt.headers,
			)

			require.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, tt.wantNewSessH, rec.Header().Get("X-Amzn-Sagemaker-New-Session-Id") != "")
			assert.Len(t, h.Backend.ListSessions(), tt.wantSessions)
		})
	}
}

// TestNewSessionHeaderFormat checks that the new-session response header
// contains both the session ID and an Expires attribute.
func TestNewSessionHeaderFormat(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequestWithHeaders(
		t, h, http.MethodPost,
		"/endpoints/ep/invocations",
		nil,
		map[string]string{"X-Amzn-Sagemaker-Session-Id": "NEW_SESSION"},
	)

	require.Equal(t, http.StatusOK, rec.Code)

	hdr := rec.Header().Get("X-Amzn-Sagemaker-New-Session-Id")
	require.NotEmpty(t, hdr)
	assert.Contains(t, hdr, "Expires=", "new session header must contain Expires attribute")
}

// TestTargetVariantForwarding verifies that the X-Amzn-Invoked-Production-Variant
// response header reflects the X-Amzn-Sagemaker-Target-Variant request header,
// and defaults to "AllTraffic" when absent.
func TestTargetVariantForwarding(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		headers     map[string]string
		wantVariant string
	}{
		{
			name:        "default_all_traffic",
			headers:     nil,
			wantVariant: "AllTraffic",
		},
		{
			name: "explicit_variant",
			headers: map[string]string{
				"X-Amzn-Sagemaker-Target-Variant": "blue",
			},
			wantVariant: "blue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequestWithHeaders(
				t, h, http.MethodPost, "/endpoints/ep/invocations", nil, tt.headers,
			)

			assert.Equal(t, tt.wantVariant, rec.Header().Get("X-Amzn-Invoked-Production-Variant"))
		})
	}
}
