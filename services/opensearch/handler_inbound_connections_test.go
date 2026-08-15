package opensearch_test

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/opensearch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAcceptInboundConnection_EmptyID(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.AcceptInboundConnection("")
	require.Error(t, err)
	assert.ErrorIs(t, err, opensearch.ErrInvalidParameter)
}

func TestInboundConnections_DescribeRejectDelete(t *testing.T) {
	t.Parallel()

	b, h := newTestHandlerAndBackend()

	// Seed connections (AWS creates inbound connections via remote outbound requests).
	opensearch.SeedInboundConnection(b, "conn-inb-1")
	opensearch.SeedInboundConnection(b, "conn-inb-2")

	// Accept conn-inb-1.
	ar := doRequest(t, h, http.MethodPut,
		"/2021-01-01/opensearch/cc/inboundConnection/conn-inb-1/accept", nil)
	ar.Body.Close()

	// Describe returns it.
	dr := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/cc/inboundConnection/search", nil)
	defer dr.Body.Close()
	require.Equal(t, http.StatusOK, dr.StatusCode)

	var dOut map[string]any
	require.NoError(t, json.NewDecoder(dr.Body).Decode(&dOut))
	conns, ok := dOut["Connections"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, conns)

	// Accept conn-inb-2, then reject it.
	ar2 := doRequest(t, h, http.MethodPut,
		"/2021-01-01/opensearch/cc/inboundConnection/conn-inb-2/accept", nil)
	ar2.Body.Close()

	rr := doRequest(t, h, http.MethodPut,
		"/2021-01-01/opensearch/cc/inboundConnection/conn-inb-2/reject", nil)
	defer rr.Body.Close()
	require.Equal(t, http.StatusOK, rr.StatusCode)

	var rOut map[string]any
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&rOut))
	rConn := rOut["Connection"].(map[string]any)
	assert.Equal(t, "REJECTED", rConn["ConnectionStatus"].(map[string]any)["StatusCode"])

	// Delete the first connection.
	del := doRequest(t, h, http.MethodDelete,
		"/2021-01-01/opensearch/cc/inboundConnection/conn-inb-1", nil)
	defer del.Body.Close()
	require.Equal(t, http.StatusOK, del.StatusCode)

	var delOut map[string]any
	require.NoError(t, json.NewDecoder(del.Body).Decode(&delOut))
	delConn := delOut["Connection"].(map[string]any)
	assert.Equal(t, "conn-inb-1", delConn["ConnectionId"])
}

func TestAcceptInboundConnection_UnknownIDReturnsError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		connectionID string
		wantCode     int
	}{
		{
			name:         "unknown connection id",
			connectionID: "nonexistent-conn-id",
			wantCode:     http.StatusNotFound,
		},
		{
			name:         "another unknown connection",
			connectionID: "ci-00000000",
			wantCode:     http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			resp := doRequest(
				t,
				h,
				http.MethodPut,
				"/2021-01-01/opensearch/cc/inboundConnection/"+tt.connectionID+"/accept",
				nil,
			)
			defer resp.Body.Close()

			assert.Equal(t, tt.wantCode, resp.StatusCode)
		})
	}
}

func TestInboundConnections_RejectAndDeleteUnknownReturnNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{
			name:   "reject unknown",
			method: http.MethodPut,
			path:   "/2021-01-01/opensearch/cc/inboundConnection/nonexistent-conn/reject",
		},
		{
			name:   "delete unknown",
			method: http.MethodDelete,
			path:   "/2021-01-01/opensearch/cc/inboundConnection/nonexistent-conn",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			resp := doRequest(t, h, tt.method, tt.path, nil)
			defer resp.Body.Close()

			assert.Equal(t, http.StatusNotFound, resp.StatusCode)
		})
	}
}

func TestOutboundConnection_CreateThenAcceptMirrorsBothSides(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	cr := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/cc/outboundConnection",
		map[string]any{
			"ConnectionAlias": "mirror-alias",
			"LocalDomainInfo": map[string]any{
				"AWSDomainInformation": map[string]any{"DomainName": "local-dom"},
			},
			"RemoteDomainInfo": map[string]any{
				"AWSDomainInformation": map[string]any{"DomainName": "remote-dom"},
			},
		})
	defer cr.Body.Close()
	require.Equal(t, http.StatusOK, cr.StatusCode)

	var cOut map[string]any
	require.NoError(t, json.NewDecoder(cr.Body).Decode(&cOut))
	connID := cOut["ConnectionId"].(string)

	// Accepting the mirrored inbound connection also activates the outbound side.
	ar := doRequest(t, h, http.MethodPut,
		"/2021-01-01/opensearch/cc/inboundConnection/"+connID+"/accept", nil)
	defer ar.Body.Close()
	require.Equal(t, http.StatusOK, ar.StatusCode)

	var aOut map[string]any
	require.NoError(t, json.NewDecoder(ar.Body).Decode(&aOut))
	aConn := aOut["Connection"].(map[string]any)
	assert.Equal(t, "ACTIVE", aConn["ConnectionStatus"].(map[string]any)["StatusCode"])

	dr := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/cc/outboundConnection/search", nil)
	defer dr.Body.Close()

	var dOut map[string]any
	require.NoError(t, json.NewDecoder(dr.Body).Decode(&dOut))
	conns := dOut["Connections"].([]any)
	require.Len(t, conns, 1)
	outConn := conns[0].(map[string]any)
	assert.Equal(t, connID, outConn["ConnectionId"])
	assert.Equal(t, "ACTIVE", outConn["ConnectionStatus"].(map[string]any)["StatusCode"])
}

func TestOpenSearchHandler_AcceptInboundConnection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		connectionID string
		wantContains []string
		wantCode     int
		seedConn     bool
	}{
		{
			name:         "success",
			connectionID: "conn-123",
			wantCode:     http.StatusOK,
			wantContains: []string{"conn-123", "ACTIVE"},
			seedConn:     true,
		},
		{
			name:         "not_found",
			connectionID: "conn-nonexistent",
			wantCode:     http.StatusNotFound,
		},
		{
			name:         "empty_id",
			connectionID: "",
			wantCode:     http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := opensearch.NewInMemoryBackend("123456789012", "us-east-1")
			if tt.seedConn {
				opensearch.SeedInboundConnection(b, tt.connectionID)
			}
			h := opensearch.NewHandler(b)

			path := "/2021-01-01/opensearch/cc/inboundConnection/" + tt.connectionID + "/accept"
			resp := doRequest(t, h, http.MethodPut, path, nil)
			defer resp.Body.Close()

			assert.Equal(t, tt.wantCode, resp.StatusCode)

			if len(tt.wantContains) > 0 {
				bodyBytes, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				for _, s := range tt.wantContains {
					assert.Contains(t, string(bodyBytes), s)
				}
			}
		})
	}
}
