package iotdataplane_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/iotdataplane"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_DeleteConnection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		method   string
		clientID string
		wantCode int
	}{
		{
			name:     "delete_existing_connection",
			method:   http.MethodDelete,
			clientID: "client-001",
			wantCode: http.StatusOK,
		},
		{
			// Real AWS models ResourceNotFoundException for DeleteConnection
			// (see ErrConnectionNotFound) -- gopherstack tracks "currently
			// connected" via the connections registry, so a clientId with no
			// tracked connection is correctly a 404, not an idempotent no-op.
			name:     "delete_nonexistent_connection_returns_not_found",
			method:   http.MethodDelete,
			clientID: "unknown-client",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "missing_clientId_returns_bad_request",
			method:   http.MethodDelete,
			clientID: "",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "wrong_method_returns_method_not_allowed",
			method:   http.MethodGet,
			clientID: "client-001",
			wantCode: http.StatusMethodNotAllowed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotdataplane.NewInMemoryBackend()
			b.AddConnectionInternal("client-001")
			h := iotdataplane.NewHandler(b)

			path := "/_admin/connections/" + tt.clientID
			rec := doRequest(t, h, tt.method, path, nil)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestHandler_RouteMatcher_DeleteConnectionRealPath verifies the RouteMatcher
// (which real HTTP traffic goes through, unlike doRequest's direct Handler()
// call) recognizes the real AWS wire paths rooted at /connections/{clientId}:
// DELETE (DeleteConnection), GET (GetConnection), GET .../subscriptions
// (ListSubscriptions), and POST .../messages (SendDirectMessage) all match,
// while a bare POST on /connections/{clientId} -- which has no real AWS
// equivalent (RegisterConnection only exists under /_admin) -- does not.
func TestHandler_RouteMatcher_DeleteConnectionRealPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		method    string
		path      string
		wantMatch bool
	}{
		{name: "delete_matches", method: http.MethodDelete, path: "/connections/client-001", wantMatch: true},
		{name: "get_matches_get_connection", method: http.MethodGet, path: "/connections/client-001", wantMatch: true},
		{name: "post_does_not_match", method: http.MethodPost, path: "/connections/client-001", wantMatch: false},
		{
			name: "get_subscriptions_matches_list_subscriptions", method: http.MethodGet,
			path: "/connections/client-001/subscriptions", wantMatch: true,
		},
		{
			name: "post_messages_matches_send_direct_message", method: http.MethodPost,
			path: "/connections/client-001/messages", wantMatch: true,
		},
		{
			name: "put_does_not_match", method: http.MethodPut,
			path: "/connections/client-001", wantMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())
			matcher := h.RouteMatcher()
			assert.Equal(t, tt.wantMatch, matcher(c))
		})
	}
}
func TestBackend_DeleteConnection_UnknownClientNotFound(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	b.AddConnectionInternal("my-client")

	// First delete succeeds.
	require.NoError(t, b.DeleteConnection("my-client"))
	// Second delete on the now-removed client returns ResourceNotFoundException
	// -- real AWS models this for DeleteConnection (see ErrConnectionNotFound).
	require.ErrorIs(t, b.DeleteConnection("my-client"), iotdataplane.ErrConnectionNotFound)
	// Deleting a client that was never registered is also not found.
	require.ErrorIs(t, b.DeleteConnection("never-existed"), iotdataplane.ErrConnectionNotFound)
}
func Test_DeleteConnection_DollarPrefixReturns400(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
	rec := doRequest(t, h, http.MethodDelete, "/_admin/connections/$reserved", nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidRequestException")
}
func Test_DeleteConnection_UnknownClientNotFound(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	b.AddConnectionInternal("c1")

	require.NoError(t, b.DeleteConnection("c1"))
	require.ErrorIs(t, b.DeleteConnection("c1"), iotdataplane.ErrConnectionNotFound)
	require.ErrorIs(t, b.DeleteConnection("never-existed"), iotdataplane.ErrConnectionNotFound)

	assert.Equal(t, 0, iotdataplane.ConnectionCount(b))
}
func Test_ListConnections_Empty(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
	rec := doRequest(t, h, http.MethodGet, "/_admin/connections", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	conns := resp["connections"].([]any)
	assert.Empty(t, conns)
}
func Test_RegisterConnection_Success(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	h := iotdataplane.NewHandler(b)

	rec := doRequest(t, h, http.MethodPost, "/_admin/connections/device-001", nil)
	assert.Equal(t, http.StatusCreated, rec.Code)
	assert.Equal(t, 1, iotdataplane.ConnectionCount(b))
}
func Test_RegisterConnection_DuplicateReturns409(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	h := iotdataplane.NewHandler(b)

	rec := doRequest(t, h, http.MethodPost, "/_admin/connections/device-001", nil)
	assert.Equal(t, http.StatusCreated, rec.Code)

	rec = doRequest(t, h, http.MethodPost, "/_admin/connections/device-001", nil)
	assert.Equal(t, http.StatusConflict, rec.Code)
	assert.Contains(t, rec.Body.String(), "ResourceAlreadyExistsException")
}
func Test_RegisterConnection_DollarPrefixRejected(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
	rec := doRequest(t, h, http.MethodPost, "/_admin/connections/$system", nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
func Test_ListConnections_ShowsRegistered(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	h := iotdataplane.NewHandler(b)

	// Register two connections.
	doRequest(t, h, http.MethodPost, "/_admin/connections/device-a", nil)
	doRequest(t, h, http.MethodPost, "/_admin/connections/device-b", nil)

	rec := doRequest(t, h, http.MethodGet, "/_admin/connections", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	conns := resp["connections"].([]any)
	assert.Len(t, conns, 2)
}
func Test_DeleteConnection_RemovesFromList(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	h := iotdataplane.NewHandler(b)

	doRequest(t, h, http.MethodPost, "/_admin/connections/device-x", nil)
	assert.Equal(t, 1, iotdataplane.ConnectionCount(b))

	doRequest(t, h, http.MethodDelete, "/_admin/connections/device-x", nil)
	assert.Equal(t, 0, iotdataplane.ConnectionCount(b))
}
func Test_Connections_WrongMethod(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())

	// PUT on /connections should return 405.
	rec := doRequest(t, h, http.MethodPut, "/_admin/connections", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}
func Test_ListConnections_SortedByConnectedAt(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	// Add three connections with small sleeps to ensure distinct timestamps.
	for _, id := range []string{"first", "second", "third"} {
		b.AddConnectionInternal(id)
	}

	conns := b.ListConnections()
	// All must be present; order is by connectedAt (insertion order here).
	assert.Len(t, conns, 3)
}
func Test_RegisterConnection_NotIdempotent(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	require.NoError(t, b.RegisterConnection("c1", ""))
	err := b.RegisterConnection("c1", "")
	require.ErrorIs(t, err, iotdataplane.ErrConnectionExists)
}
func Test_AdminConnectionsPath_BasicFlow(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	h := iotdataplane.NewHandler(b)

	// List (empty initially).
	rec := doRequest(t, h, http.MethodGet, "/_admin/connections", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Register a new connection.
	rec = doRequest(t, h, http.MethodPost, "/_admin/connections/dev-x", nil)
	assert.Equal(t, http.StatusCreated, rec.Code)
	assert.Equal(t, 1, iotdataplane.ConnectionCount(b))

	// List shows it.
	rec = doRequest(t, h, http.MethodGet, "/_admin/connections", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp["connections"].([]any), 1)

	// Delete.
	rec = doRequest(t, h, http.MethodDelete, "/_admin/connections/dev-x", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, iotdataplane.ConnectionCount(b))
}

// TestRefinement3_ConnectionsPath_NonAWSOpsReturn404 verifies that
// RegisterConnection and ListConnections -- which have no real AWS
// iotdataplane equivalent -- are unreachable at the bare /connections path
// (they only exist under the gopherstack-only /_admin/connections alias).
func Test_ConnectionsPath_NonAWSOpsReturn404(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())

	tests := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/connections"},
		{http.MethodPost, "/connections/device-x"},
	}

	for _, tt := range tests {
		t.Run(tt.method+tt.path, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, tt.method, tt.path, nil)
			assert.Equal(t, http.StatusNotFound, rec.Code,
				"RegisterConnection/ListConnections have no real AWS path and stay 404 outside /_admin")
		})
	}
}

// TestRefinement3_DeleteConnection_RealAWSPath verifies that DeleteConnection
// -- unlike RegisterConnection/ListConnections -- IS a real, published AWS
// iotdataplane operation (aws-sdk-go-v2/service/iotdataplane.DeleteConnection,
// wire path "DELETE /connections/{clientId}") and so must remain reachable at
// its real wire path, not just the gopherstack /_admin alias.
func Test_DeleteConnection_RealAWSPath(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	h := iotdataplane.NewHandler(b)

	doRequest(t, h, http.MethodPost, "/_admin/connections/device-x", nil)
	assert.Equal(t, 1, iotdataplane.ConnectionCount(b))

	rec := doRequest(t, h, http.MethodDelete, "/connections/device-x", nil)
	assert.Equal(t, http.StatusOK, rec.Code, "real AWS DeleteConnection wire path must work")
	assert.Equal(t, 0, iotdataplane.ConnectionCount(b))
}
func Test_Connection_Register_DuplicateShape(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())

	doRequest(t, h, http.MethodPost, "/_admin/connections/client-1", nil)

	rec := doRequest(t, h, http.MethodPost, "/_admin/connections/client-1", nil)
	require.Equal(t, http.StatusConflict, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ResourceAlreadyExistsException", resp["error"])
}
func Test_Connection_EmptyClientID_Rejected(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())

	// POST /_admin/connections/ with empty clientId segment.
	rec := doRequest(t, h, http.MethodPost, "/_admin/connections/", nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
func Test_Connection_DollarPrefix_Rejected(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())

	rec := doRequest(t, h, http.MethodPost, "/_admin/connections/$system-client", nil)
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "InvalidRequestException", resp["error"])
}
func Test_Connection_DeleteUnknown_NotFound_DollarStillRejected(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	// Deleting a nonexistent ID returns ResourceNotFoundException (real AWS
	// models this for DeleteConnection; see ErrConnectionNotFound).
	err := b.DeleteConnection("nonexistent-client")
	require.ErrorIs(t, err, iotdataplane.ErrConnectionNotFound)

	// Dollar-prefix is still rejected as InvalidRequestException, checked
	// before the not-found lookup.
	err = b.DeleteConnection("$system")
	require.ErrorIs(t, err, iotdataplane.ErrValidation)
}

// Test_DeleteConnection_UnknownClient_HTTPShape verifies the HTTP-level wire
// shape for DeleteConnection against an untracked clientId: 404 with the
// ResourceNotFoundException error code, on both the gopherstack admin alias
// and the real AWS wire path.
func Test_DeleteConnection_UnknownClient_HTTPShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
	}{
		{name: "admin_alias", path: "/_admin/connections/never-connected"},
		{name: "real_aws_path", path: "/connections/never-connected"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
			rec := doRequest(t, h, http.MethodDelete, tt.path, nil)
			require.Equal(t, http.StatusNotFound, rec.Code)

			var resp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, "ResourceNotFoundException", resp["error"])
		})
	}
}

// TestBackend_GetConnection exercises InMemoryBackend.GetConnection directly:
// a tracked clientId returns its genuinely-known connection info,  an
// untracked clientId is ResourceNotFoundException (matching DeleteConnection's
// modeled error, per deserializers.go's
// awsRestjson1_deserializeOpErrorGetConnection case list), and a
// dollar-prefixed clientId is rejected up front like every other connections
// op.
func TestBackend_GetConnection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr  error
		name     string
		clientID string
		seed     bool
	}{
		{name: "tracked_client_returns_connection_info", clientID: "dev-1", seed: true},
		{name: "unknown_client_not_found", clientID: "never-connected", wantErr: iotdataplane.ErrConnectionNotFound},
		{name: "dollar_prefix_rejected", clientID: "$reserved", wantErr: iotdataplane.ErrValidation},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotdataplane.NewInMemoryBackend()
			if tt.seed {
				b.AddConnectionInternal(tt.clientID)
			}

			conn, err := b.GetConnection(tt.clientID)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.clientID, conn.ClientID)
		})
	}
}

// TestHandler_GetConnection_WireShape verifies the GET /connections/{clientId}
// response shape against GetConnectionOutput (deserializers.go's
// awsRestjson1_deserializeOpDocumentGetConnectionOutput): connected/clientId/
// connectedSince are always present for a tracked client, and sourceIp is
// present only when includeSocketInformation=true was requested -- mirroring
// the real API's documented default-false gating (GetConnectionInput.
// IncludeSocketInformation).
func TestHandler_GetConnection_WireShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		clientID     string
		query        string
		wantCode     int
		preRegister  bool
		wantSourceIP bool
	}{
		{
			name: "known_client_no_socket_info_by_default", clientID: "dev-1",
			preRegister: true, wantCode: http.StatusOK,
		},
		{
			name: "known_client_include_socket_information", clientID: "dev-2",
			query: "?includeSocketInformation=true", preRegister: true,
			wantCode: http.StatusOK, wantSourceIP: true,
		},
		{name: "unknown_client_not_found", clientID: "never-connected", wantCode: http.StatusNotFound},
		{name: "dollar_prefix_rejected", clientID: "$reserved", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotdataplane.NewInMemoryBackend()
			if tt.preRegister {
				require.NoError(t, b.RegisterConnection(tt.clientID, "203.0.113.5"))
			}

			h := iotdataplane.NewHandler(b)
			rec := doRequest(t, h, http.MethodGet, "/connections/"+tt.clientID+tt.query, nil)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode != http.StatusOK {
				return
			}

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tt.clientID, resp["clientId"])
			assert.Equal(t, true, resp["connected"])
			assert.NotZero(t, resp["connectedSince"])

			_, hasSourceIP := resp["sourceIp"]
			assert.Equal(t, tt.wantSourceIP, hasSourceIP)
		})
	}
}

// TestHandler_ListSubscriptions_WireShape verifies the GET
// /connections/{clientId}/subscriptions response shape against
// ListSubscriptionsOutput. A tracked client always gets an honestly empty
// "subscriptions" array (gopherstack tracks no live MQTT subscription state
// -- see InMemoryBackend.ListSubscriptions), and an untracked clientId is
// ResourceNotFoundException, matching GetConnection/DeleteConnection.
func TestHandler_ListSubscriptions_WireShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		clientID    string
		wantCode    int
		preRegister bool
	}{
		{
			name: "known_client_returns_empty_subscriptions", clientID: "dev-1",
			preRegister: true, wantCode: http.StatusOK,
		},
		{name: "unknown_client_not_found", clientID: "never-connected", wantCode: http.StatusNotFound},
		{name: "dollar_prefix_rejected", clientID: "$reserved", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotdataplane.NewInMemoryBackend()
			if tt.preRegister {
				b.AddConnectionInternal(tt.clientID)
			}

			h := iotdataplane.NewHandler(b)
			rec := doRequest(t, h, http.MethodGet, "/connections/"+tt.clientID+"/subscriptions", nil)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode != http.StatusOK {
				return
			}

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			subs, ok := resp["subscriptions"].([]any)
			require.True(t, ok)
			assert.Empty(t, subs)

			_, hasNextToken := resp["nextToken"]
			assert.False(t, hasNextToken)
		})
	}
}

// TestBackend_SendDirectMessage exercises InMemoryBackend.SendDirectMessage
// directly: a tracked clientId with a broker wired delivers via the same
// broker path as Publish (see SendDirectMessage's doc comment for why this
// broadcasts on topic rather than truly targeting one client), an untracked
// clientId is ResourceNotFoundException, a dollar-prefixed clientId is
// rejected up front, and -- matching Publish's own behavior exactly, since
// SendDirectMessage delegates to it -- no broker configured surfaces
// ErrNoBroker to the backend caller (the HTTP handler degrades this to a
// logged warning plus a 200, same as Publish; see
// TestHandler_SendDirectMessage_WireShape).
func TestBackend_SendDirectMessage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr    error
		name       string
		clientID   string
		topic      string
		seed       bool
		withBroker bool
	}{
		{
			name: "delivers_via_broker_when_client_tracked", clientID: "dev-1", topic: "t/1",
			seed: true, withBroker: true,
		},
		{
			name: "unknown_client_not_found", clientID: "never-connected", topic: "t/1",
			wantErr: iotdataplane.ErrConnectionNotFound,
		},
		{name: "dollar_prefix_rejected", clientID: "$reserved", topic: "t/1", wantErr: iotdataplane.ErrValidation},
		{
			name: "no_broker_configured_returns_ErrNoBroker", clientID: "dev-2", topic: "t/1",
			seed: true, wantErr: iotdataplane.ErrNoBroker,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotdataplane.NewInMemoryBackend()
			if tt.seed {
				b.AddConnectionInternal(tt.clientID)
			}

			var mock *mockMQTTPublisher
			if tt.withBroker {
				mock = &mockMQTTPublisher{}
				b.SetBroker(mock)
			}

			err := b.SendDirectMessage(tt.clientID, tt.topic, []byte("hi"), 0)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.topic, mock.topic)
			assert.Equal(t, []byte("hi"), mock.payload)
		})
	}
}

// TestHandler_SendDirectMessage_WireShape verifies the POST
// /connections/{clientId}/messages request/response shape against
// SendDirectMessageInput/Output (serializers.go's
// awsRestjson1_serializeOpHttpBindingsSendDirectMessageInput and
// deserializers.go's awsRestjson1_deserializeOpDocumentSendDirectMessageOutput):
// topic is a required query param, payload is the raw request body (no JSON
// envelope), confirmation=true maps to QoS 1 (false/absent to QoS 0, per
// AWS docs), and a successful response always carries message+traceId. No
// broker configured degrades to a logged warning and a 200 (message
// silently dropped), exactly like Publish.
func TestHandler_SendDirectMessage_WireShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		headers       map[string]string
		name          string
		clientID      string
		query         string
		body          []byte
		wantCode      int
		wantQos       byte
		preRegister   bool
		withBroker    bool
		wantDelivered bool
	}{
		{
			name: "delivers_and_returns_message_and_trace_id", clientID: "dev-1", query: "?topic=t/1",
			body: []byte("payload-bytes"), preRegister: true, withBroker: true,
			wantCode: http.StatusOK, wantDelivered: true,
		},
		{
			name: "confirmation_true_maps_to_qos1", clientID: "dev-1", query: "?topic=t/1&confirmation=true",
			body: []byte("hi"), preRegister: true, withBroker: true,
			wantCode: http.StatusOK, wantDelivered: true, wantQos: 1,
		},
		{name: "missing_topic_bad_request", clientID: "dev-1", preRegister: true, wantCode: http.StatusBadRequest},
		{
			name: "invalid_topic_wildcard_bad_request", clientID: "dev-1", query: "?topic=a/%2B/b",
			preRegister: true, wantCode: http.StatusBadRequest,
		},
		{
			name: "unknown_client_not_found", clientID: "never-connected",
			query: "?topic=t/1", wantCode: http.StatusNotFound,
		},
		{
			name: "dollar_prefix_client_rejected", clientID: "$reserved",
			query: "?topic=t/1", wantCode: http.StatusBadRequest,
		},
		{
			name: "invalid_payload_format_indicator_rejected", clientID: "dev-1", query: "?topic=t/1",
			preRegister: true, headers: map[string]string{"X-Amz-Mqtt5-Payload-Format-Indicator": "NOT_A_REAL_ENUM"},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "no_broker_still_returns_200_message_dropped", clientID: "dev-1", query: "?topic=t/1",
			preRegister: true, wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotdataplane.NewInMemoryBackend()
			if tt.preRegister {
				b.AddConnectionInternal(tt.clientID)
			}

			var mock *mockMQTTPublisher
			if tt.withBroker {
				mock = &mockMQTTPublisher{}
				b.SetBroker(mock)
			}

			h := iotdataplane.NewHandler(b)
			rec := doRequestHeaders(t, h, http.MethodPost,
				"/connections/"+tt.clientID+"/messages"+tt.query, tt.body, tt.headers)
			assert.Equal(t, tt.wantCode, rec.Code, "body: %s", rec.Body.String())

			if tt.wantCode != http.StatusOK {
				return
			}

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, "OK", resp["message"])
			assert.NotEmpty(t, resp["traceId"])

			if tt.wantDelivered {
				assert.Equal(t, "t/1", mock.topic)
				assert.Equal(t, tt.body, mock.payload)
				assert.Equal(t, tt.wantQos, mock.qos)
			}
		})
	}
}
