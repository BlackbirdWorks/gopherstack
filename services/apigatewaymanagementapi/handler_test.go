package apigatewaymanagementapi_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/services/apigatewaymanagementapi"
)

func newTestHandler(t *testing.T) *apigatewaymanagementapi.Handler {
	t.Helper()

	return apigatewaymanagementapi.NewHandler(apigatewaymanagementapi.NewInMemoryBackend())
}

func doRequest(
	t *testing.T,
	h *apigatewaymanagementapi.Handler,
	method, path string,
	body []byte,
) *httptest.ResponseRecorder {
	t.Helper()

	var reqBody *bytes.Reader
	if body != nil {
		reqBody = bytes.NewReader(body)
	} else {
		reqBody = bytes.NewReader(nil)
	}

	e := echo.New()
	req := httptest.NewRequest(method, path, reqBody)
	req = req.WithContext(logger.Save(t.Context(), slog.Default()))

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "APIGatewayManagementAPI", h.Name())
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "PostToConnection")
	assert.Contains(t, ops, "GetConnection")
	assert.Contains(t, ops, "DeleteConnection")
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		want   string
	}{
		{name: "post", method: http.MethodPost, want: "PostToConnection"},
		{name: "get", method: http.MethodGet, want: "GetConnection"},
		{name: "delete", method: http.MethodDelete, want: "DeleteConnection"},
		{name: "put", method: http.MethodPut, want: "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(tt.method, "/@connections/conn-1", nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.want, h.ExtractOperation(c))
		})
	}
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/@connections/my-conn-id", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	assert.Equal(t, "my-conn-id", h.ExtractResource(c))
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		want bool
	}{
		{name: "connections prefix", path: "/@connections/conn-1", want: true},
		{name: "not connections", path: "/restapis/something", want: false},
		{name: "dashboard", path: "/dashboard/apigatewaymanagementapi", want: false},
		// Real connectionIds are base64url-ish and can contain '=' padding;
		// the literal "@connections" prefix must still match with such an id.
		{name: "connectionId with base64 padding", path: "/@connections/L0Xc123=", want: true},
		{name: "connectionId with plus and equals", path: "/@connections/AbC+d==", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.want, h.RouteMatcher()(c))
		})
	}
}

// TestHandler_RouteMatcher_SameConnectionIDDifferentMethods verifies that
// PostToConnection, GetConnection, and DeleteConnection -- which all share
// the exact same /@connections/{connectionId} path -- are distinguished
// purely by HTTP method, and that the matcher claims the path regardless of
// method.
func TestHandler_RouteMatcher_SameConnectionIDDifferentMethods(t *testing.T) {
	t.Parallel()

	tests := []struct {
		method  string
		wantOp  string
		matches bool
	}{
		{method: http.MethodPost, wantOp: "PostToConnection", matches: true},
		{method: http.MethodGet, wantOp: "GetConnection", matches: true},
		{method: http.MethodDelete, wantOp: "DeleteConnection", matches: true},
	}

	const path = "/@connections/L0Xc123="

	for _, tt := range tests {
		t.Run(tt.method, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(tt.method, path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.matches, h.RouteMatcher()(c))
			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
			assert.Equal(t, "L0Xc123=", h.ExtractResource(c))
		})
	}
}

func TestHandler_PostToConnection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		connectionID string
		payload      []byte
		wantStatus   int
		preCreate    bool
	}{
		{
			name:         "success",
			connectionID: "conn-abc",
			preCreate:    true,
			payload:      []byte(`{"message":"hello"}`),
			wantStatus:   http.StatusOK,
		},
		{
			name:         "connection not found returns 410",
			connectionID: "conn-missing",
			preCreate:    false,
			payload:      []byte(`{"message":"hello"}`),
			wantStatus:   http.StatusGone,
		},
		{
			name:         "oversized payload returns 413",
			connectionID: "conn-large",
			preCreate:    true,
			payload:      make([]byte, 129*1024),
			wantStatus:   http.StatusRequestEntityTooLarge,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.preCreate {
				_, err := h.Backend.CreateConnection(tt.connectionID, "127.0.0.1", "test-agent", nil)
				require.NoError(t, err)
			}

			rec := doRequest(t, h, http.MethodPost, "/@connections/"+tt.connectionID, tt.payload)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_PostToConnection_EmptyBodyOnSuccess verifies PostToConnection
// returns HTTP 200 with an empty body on success, matching the real AWS shape,
// across text, binary, and empty payloads.
func TestHandler_PostToConnection_EmptyBodyOnSuccess(t *testing.T) {
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

// TestHandler_PostToConnection_PayloadLimitBoundary verifies that payloads
// exceeding 128 KB return HTTP 413, matching real AWS behavior, and that the
// boundary itself (exactly 128 KB) is still accepted.
func TestHandler_PostToConnection_PayloadLimitBoundary(t *testing.T) {
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

func TestHandler_GetConnection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		connectionID string
		preCreate    bool
		wantStatus   int
	}{
		{
			name:         "found",
			connectionID: "conn-get-1",
			preCreate:    true,
			wantStatus:   http.StatusOK,
		},
		{
			name:         "not found returns 410",
			connectionID: "conn-missing",
			preCreate:    false,
			wantStatus:   http.StatusGone,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.preCreate {
				_, err := h.Backend.CreateConnection(tt.connectionID, "10.0.0.1", "test-agent/1.0", nil)
				require.NoError(t, err)
			}

			rec := doRequest(t, h, http.MethodGet, "/@connections/"+tt.connectionID, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var conn apigatewaymanagementapi.Connection
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &conn))
				assert.Equal(t, tt.connectionID, conn.ConnectionID)
			}
		})
	}
}

// TestHandler_GetConnection_IdentityShape verifies that GetConnection returns
// sourceIp and userAgent nested under an "identity" object, matching the real
// AWS API Gateway Management API wire format -- not as flat top-level fields.
func TestHandler_GetConnection_IdentityShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		sourceIP  string
		userAgent string
	}{
		{name: "browser client", sourceIP: "203.0.113.1", userAgent: "Mozilla/5.0"},
		{name: "sdk client", sourceIP: "10.0.0.5", userAgent: "aws-sdk-go/1.44.0"},
		{name: "empty user agent", sourceIP: "192.168.1.1", userAgent: ""},
		{name: "compat agent", sourceIP: "10.0.0.1", userAgent: "compat-agent"},
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

			// AWS shape: identity must be a nested object.
			identityRaw, ok := body["identity"]
			require.True(t, ok, "response must include 'identity' field")

			var identity map[string]string
			require.NoError(t, json.Unmarshal(identityRaw, &identity))
			assert.Equal(t, tt.sourceIP, identity["sourceIp"], "identity.sourceIp must match")
			assert.Equal(t, tt.userAgent, identity["userAgent"], "identity.userAgent must match")

			// Flat fields must NOT appear at top level.
			_, hasSourceIP := body["sourceIp"]
			assert.False(t, hasSourceIP, "sourceIp must be nested under identity, not at top level")
			_, hasUserAgent := body["userAgent"]
			assert.False(t, hasUserAgent, "userAgent must be nested under identity, not at top level")

			// connectedAt and lastActiveAt must be present at top level.
			assert.Contains(t, body, "connectedAt")
			assert.Contains(t, body, "lastActiveAt")
		})
	}
}

// TestHandler_GetConnection_TimestampFields verifies that connectedAt and
// lastActiveAt are non-zero timestamps in the response.
func TestHandler_GetConnection_TimestampFields(t *testing.T) {
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

func TestHandler_DeleteConnection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		connectionID string
		preCreate    bool
		wantStatus   int
	}{
		{
			name:         "success",
			connectionID: "conn-del-1",
			preCreate:    true,
			wantStatus:   http.StatusNoContent,
		},
		{
			name:         "not found returns 410",
			connectionID: "conn-missing",
			preCreate:    false,
			wantStatus:   http.StatusGone,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.preCreate {
				_, err := h.Backend.CreateConnection(tt.connectionID, "10.0.0.2", "test-agent/2.0", nil)
				require.NoError(t, err)
			}

			rec := doRequest(t, h, http.MethodDelete, "/@connections/"+tt.connectionID, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_DeleteConnection_EmptyBodyOnSuccess verifies DeleteConnection
// returns HTTP 204 with an empty body on success, matching the real AWS shape.
func TestHandler_DeleteConnection_EmptyBodyOnSuccess(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateConnection("conn-del-parity", "127.0.0.1", "test", nil)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodDelete, "/@connections/conn-del-parity", nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)
	assert.Empty(t, rec.Body.Bytes(), "DeleteConnection success must return empty body")
}

// TestHandler_GoneExceptionWireShape verifies that PostToConnection,
// GetConnection, and DeleteConnection against a missing connection all return
// the full GoneException wire shape: HTTP 410, the modeled type in both the
// X-Amzn-Errortype header and the body's __type field, and a human-readable
// (not the type name) "message".
func TestHandler_GoneExceptionWireShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		method       string
		connectionID string
		payload      []byte
	}{
		{name: "post", method: http.MethodPost, connectionID: "gone-conn", payload: []byte("data")},
		{
			name: "post with message body", method: http.MethodPost,
			connectionID: "conn-missing", payload: []byte(`{"message":"hi"}`),
		},
		{name: "get", method: http.MethodGet, connectionID: "gone-conn"},
		{name: "delete", method: http.MethodDelete, connectionID: "gone-conn"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.method, "/@connections/"+tt.connectionID, tt.payload)

			require.Equal(t, http.StatusGone, rec.Code)
			assert.Equal(t, "GoneException", rec.Header().Get("X-Amzn-Errortype"))

			var body map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			assert.Equal(t, "GoneException", body["__type"])
			assert.NotEqual(t, "GoneException", body["message"],
				"message must be a human-readable string, not the error type")
			assert.NotEmpty(t, body["message"])
		})
	}
}

// TestHandler_PostToConnection_LimitExceeded verifies that a full WebSocket
// client-side buffer surfaces as a LimitExceededException (429) in the AWS
// rest-json shape, matching real AWS's documented behavior for this exact
// condition -- not a silently-dropped message reported as success.
func TestHandler_PostToConnection_LimitExceeded(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	downstream := make(chan []byte, 1)
	_, err := h.Backend.CreateConnection("conn-full", "127.0.0.1", "test", downstream)
	require.NoError(t, err)

	// Fill the bounded downstream buffer so the next post cannot be queued.
	downstream <- []byte("filler")

	rec := doRequest(t, h, http.MethodPost, "/@connections/conn-full", []byte("data"))
	require.Equal(t, http.StatusTooManyRequests, rec.Code)
	assert.Equal(t, "LimitExceededException", rec.Header().Get("X-Amzn-Errortype"))

	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "LimitExceededException", body["__type"])
	assert.NotEmpty(t, body["message"])

	// The rejected message must not be recorded as delivered.
	assert.Empty(t, h.Backend.GetMessages("conn-full"))
}

// TestHandler_PostToConnection_PayloadTooLarge_WireShape verifies that a
// PayloadTooLargeException carries the modeled type in both the
// X-Amzn-Errortype header and the body's __type field -- without these the
// AWS SDK cannot resolve the response as a PayloadTooLargeException and
// instead surfaces a generic/unknown error.
func TestHandler_PostToConnection_PayloadTooLarge_WireShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateConnection("conn-big", "127.0.0.1", "test", nil)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodPost, "/@connections/conn-big", make([]byte, 129*1024))
	require.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
	assert.Equal(t, "PayloadTooLargeException", rec.Header().Get("X-Amzn-Errortype"))

	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "PayloadTooLargeException", body["__type"])
	assert.NotEmpty(t, body["message"])
}

// TestHandler_DeleteConnection_ClosesDownstream verifies that DeleteConnection
// forcibly disconnects the connection by closing its downstream delivery
// channel, matching real AWS's "forcibly disconnects" semantics rather than
// merely forgetting the connection while leaving the transport open.
func TestHandler_DeleteConnection_ClosesDownstream(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	downstream := make(chan []byte, 1)
	_, err := h.Backend.CreateConnection("conn-close", "127.0.0.1", "test", downstream)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodDelete, "/@connections/conn-close", nil)
	require.Equal(t, http.StatusNoContent, rec.Code)

	select {
	case _, open := <-downstream:
		assert.False(t, open, "downstream channel must be closed after DeleteConnection")
	default:
		t.Fatal("downstream channel must be closed (readable as closed) after DeleteConnection")
	}
}

func TestHandler_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_, err := h.Backend.CreateConnection("conn-1", "127.0.0.1", "test", nil)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodPut, "/@connections/conn-1", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandler_EmptyConnectionID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/@connections/", nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ChaosInterface(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	assert.Equal(t, "apigatewaymanagementapi", h.ChaosServiceName())
	assert.Equal(t, []string{"PostToConnection", "GetConnection", "DeleteConnection"}, h.ChaosOperations())
	assert.Equal(t, []string{"us-east-1"}, h.ChaosRegions())
	assert.Equal(t, 87, h.MatchPriority())
}

func TestBackend_ListConnections(t *testing.T) {
	t.Parallel()

	b := apigatewaymanagementapi.NewInMemoryBackend()

	conns := b.ListConnections()
	assert.Empty(t, conns)

	_, err := b.CreateConnection("list-conn-1", "1.2.3.4", "ua1", nil)
	require.NoError(t, err)

	_, err = b.CreateConnection("list-conn-2", "5.6.7.8", "ua2", nil)
	require.NoError(t, err)

	conns = b.ListConnections()
	assert.Len(t, conns, 2)
}

func TestProvider_NameAndInit(t *testing.T) {
	t.Parallel()

	p := apigatewaymanagementapi.Provider{}
	assert.Equal(t, "APIGatewayManagementAPI", p.Name())

	h, err := p.Init(nil)
	require.NoError(t, err)
	assert.NotNil(t, h)
}

func TestHandler_UnknownPathPrefix(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/restapis/something", nil)
	req = req.WithContext(logger.Save(t.Context(), slog.Default()))
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestBackend_PostToConnection_AfterDelete(t *testing.T) {
	t.Parallel()

	b := apigatewaymanagementapi.NewInMemoryBackend()

	_, err := b.CreateConnection("c1", "1.2.3.4", "ua", nil)
	require.NoError(t, err)

	require.NoError(t, b.DeleteConnection("c1"))

	err = b.PostToConnection("c1", []byte("data"))
	require.Error(t, err)
}

func TestBackend_GetMessages(t *testing.T) {
	t.Parallel()

	b := apigatewaymanagementapi.NewInMemoryBackend()

	_, err := b.CreateConnection("conn-msg", "1.2.3.4", "ua", nil)
	require.NoError(t, err)

	require.NoError(t, b.PostToConnection("conn-msg", []byte("first")))
	require.NoError(t, b.PostToConnection("conn-msg", []byte("second")))

	msgs := b.GetMessages("conn-msg")
	assert.Len(t, msgs, 2)
	assert.Equal(t, []byte("first"), msgs[0].Data)
	assert.Equal(t, []byte("second"), msgs[1].Data)
}

func TestBackend_PostToConnection_MessageCap(t *testing.T) {
	t.Parallel()

	b := apigatewaymanagementapi.NewInMemoryBackend()

	_, err := b.CreateConnection("capped", "1.2.3.4", "ua", nil)
	require.NoError(t, err)

	total := apigatewaymanagementapi.MaxMessagesPerConnection + 10
	for i := range total {
		require.NoError(t, b.PostToConnection("capped", fmt.Appendf(nil, "msg-%d", i)))
	}

	msgs := b.GetMessages("capped")
	assert.Len(t, msgs, apigatewaymanagementapi.MaxMessagesPerConnection)
	// The oldest messages should have been dropped; the last message should be the most recent.
	assert.Equal(t, fmt.Appendf(nil, "msg-%d", total-1), msgs[len(msgs)-1].Data)
	// The first retained message should be message #10 (the 11th posted).
	assert.Equal(t, []byte("msg-10"), msgs[0].Data)
}
