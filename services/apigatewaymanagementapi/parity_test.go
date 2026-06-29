package apigatewaymanagementapi_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigatewaymanagementapi"
)

// TestParity_GetConnection_IdentityShape verifies that GetConnection returns
// sourceIp and userAgent nested under an "identity" object, matching the real
// AWS API Gateway Management API wire format.
func TestParity_GetConnection_IdentityShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		sourceIP  string
		userAgent string
	}{
		{name: "browser client", sourceIP: "203.0.113.1", userAgent: "Mozilla/5.0"},
		{name: "sdk client", sourceIP: "10.0.0.5", userAgent: "aws-sdk-go/1.44.0"},
		{name: "empty user agent", sourceIP: "192.168.1.1", userAgent: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_, err := h.Backend.CreateConnection("conn-identity", tt.sourceIP, tt.userAgent, nil)
			require.NoError(t, err)

			rec := doRequest(t, h, http.MethodGet, "/@connections/conn-identity", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var body map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))

			// AWS shape: identity must be a nested object
			identityRaw, ok := body["identity"]
			require.True(t, ok, "response must include 'identity' field")

			var identity map[string]string
			require.NoError(t, json.Unmarshal(identityRaw, &identity))
			assert.Equal(t, tt.sourceIP, identity["sourceIp"], "identity.sourceIp must match")
			assert.Equal(t, tt.userAgent, identity["userAgent"], "identity.userAgent must match")

			// Flat fields must NOT appear at top level
			_, hasSourceIP := body["sourceIp"]
			assert.False(t, hasSourceIP, "sourceIp must be nested under identity, not at top level")
			_, hasUserAgent := body["userAgent"]
			assert.False(t, hasUserAgent, "userAgent must be nested under identity, not at top level")

			// connectedAt and lastActiveAt must be present at top level
			assert.Contains(t, body, "connectedAt")
			assert.Contains(t, body, "lastActiveAt")
		})
	}
}

// TestParity_PostToConnection_Returns200EmptyBody verifies PostToConnection
// returns HTTP 200 with an empty body on success, matching the real AWS shape.
func TestParity_PostToConnection_Returns200EmptyBody(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		payload []byte
	}{
		{name: "text message", payload: []byte(`{"action":"message","data":"hello"}`)},
		{name: "binary message", payload: []byte{0x01, 0x02, 0x03}},
		{name: "empty body", payload: []byte{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_, err := h.Backend.CreateConnection("conn-post", "127.0.0.1", "test", nil)
			require.NoError(t, err)

			rec := doRequest(t, h, http.MethodPost, "/@connections/conn-post", tt.payload)
			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Empty(t, rec.Body.Bytes(), "PostToConnection success must return empty body")
		})
	}
}

// TestParity_DeleteConnection_Returns204EmptyBody verifies DeleteConnection
// returns HTTP 204 with an empty body on success, matching the real AWS shape.
func TestParity_DeleteConnection_Returns204EmptyBody(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateConnection("conn-del-parity", "127.0.0.1", "test", nil)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodDelete, "/@connections/conn-del-parity", nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)
	assert.Empty(t, rec.Body.Bytes(), "DeleteConnection success must return empty body")
}

// TestParity_PostToConnection_GoneException verifies that posting to a missing
// connection returns the full GoneException shape (status, header, body fields).
func TestParity_PostToConnection_GoneException(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/@connections/gone-conn", []byte("data"))

	require.Equal(t, http.StatusGone, rec.Code)
	assert.Equal(t, "GoneException", rec.Header().Get("X-Amzn-Errortype"))

	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "GoneException", body["__type"])
	assert.NotEmpty(t, body["message"])
}

// TestParity_GetConnection_GoneException verifies GetConnection on a missing
// connection returns the full GoneException shape.
func TestParity_GetConnection_GoneException(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/@connections/gone-conn", nil)

	require.Equal(t, http.StatusGone, rec.Code)
	assert.Equal(t, "GoneException", rec.Header().Get("X-Amzn-Errortype"))

	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "GoneException", body["__type"])
	assert.NotEmpty(t, body["message"])
}

// TestParity_DeleteConnection_GoneException verifies DeleteConnection on a
// missing connection returns the full GoneException shape.
func TestParity_DeleteConnection_GoneException(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodDelete, "/@connections/gone-conn", nil)

	require.Equal(t, http.StatusGone, rec.Code)
	assert.Equal(t, "GoneException", rec.Header().Get("X-Amzn-Errortype"))

	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "GoneException", body["__type"])
	assert.NotEmpty(t, body["message"])
}

// TestParity_PostToConnection_PayloadLimit verifies that payloads exceeding
// 128 KB return HTTP 413, matching real AWS behavior.
func TestParity_PostToConnection_PayloadLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		size       int
		wantStatus int
	}{
		{name: "at limit 128KB", size: 128 * 1024, wantStatus: http.StatusOK},
		{name: "over limit 128KB+1", size: 128*1024 + 1, wantStatus: http.StatusRequestEntityTooLarge},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_, err := h.Backend.CreateConnection("conn-limit", "127.0.0.1", "test", nil)
			require.NoError(t, err)

			rec := doRequest(t, h, http.MethodPost, "/@connections/conn-limit", make([]byte, tt.size))
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestParity_GetConnection_TimestampFields verifies that connectedAt and
// lastActiveAt are non-zero timestamps in the response.
func TestParity_GetConnection_TimestampFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	conn, err := h.Backend.CreateConnection("conn-ts", "1.2.3.4", "ua", nil)
	require.NoError(t, err)
	require.NotZero(t, conn.ConnectedAt)

	rec := doRequest(t, h, http.MethodGet, "/@connections/conn-ts", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var body struct {
		ConnectedAt  string `json:"connectedAt"`
		LastActiveAt string `json:"lastActiveAt"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.NotEmpty(t, body.ConnectedAt, "connectedAt must be non-empty")
	assert.NotEmpty(t, body.LastActiveAt, "lastActiveAt must be non-empty")
}

// TestParity_GetConnection_ExistingHandlerTest_StillPasses verifies the
// parity fix does not regress the existing handler test expectation.
func TestParity_GetConnection_ExistingHandlerTest_StillPasses(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateConnection("conn-compat", "10.0.0.1", "compat-agent", nil)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodGet, "/@connections/conn-compat", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	// The old handler_test.go unmarshal into Connection; verify our response
	// body fields that it checked still work via the identity path.
	var body map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))

	var identity map[string]string
	require.NoError(t, json.Unmarshal(body["identity"], &identity))
	assert.Equal(t, "10.0.0.1", identity["sourceIp"])

	_ = apigatewaymanagementapi.NewInMemoryBackend() // ensure import used
}
