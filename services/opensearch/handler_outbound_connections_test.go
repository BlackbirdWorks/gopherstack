package opensearch_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOutboundConnections_CreateDescribeDelete(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// Create outbound connection.
	cr := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/cc/outboundConnection",
		map[string]any{
			"ConnectionAlias": "my-alias",
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
	assert.NotEmpty(t, connID)
	assert.Equal(t, "my-alias", cOut["ConnectionAlias"])
	assert.Equal(t, "DIRECT", cOut["ConnectionMode"])
	assert.Equal(t, "PENDING_ACCEPTANCE", cOut["ConnectionStatus"].(map[string]any)["StatusCode"])
	localInfo := cOut["LocalDomainInfo"].(map[string]any)["AWSDomainInformation"].(map[string]any)
	assert.Equal(t, "local-dom", localInfo["DomainName"])

	// Describe returns the connection.
	dr := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/cc/outboundConnection", nil)
	defer dr.Body.Close()
	require.Equal(t, http.StatusOK, dr.StatusCode)

	var dOut map[string]any
	require.NoError(t, json.NewDecoder(dr.Body).Decode(&dOut))
	conns, ok := dOut["Connections"].([]any)
	require.True(t, ok)
	assert.Len(t, conns, 1)

	// The mirrored inbound connection is discoverable on the remote side.
	ir := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/cc/inboundConnection", nil)
	defer ir.Body.Close()
	require.Equal(t, http.StatusOK, ir.StatusCode)

	var iOut map[string]any
	require.NoError(t, json.NewDecoder(ir.Body).Decode(&iOut))
	inConns, ok := iOut["Connections"].([]any)
	require.True(t, ok)
	require.Len(t, inConns, 1)
	inConn := inConns[0].(map[string]any)
	assert.Equal(t, connID, inConn["ConnectionId"])
	remoteInfo := inConn["LocalDomainInfo"].(map[string]any)["AWSDomainInformation"].(map[string]any)
	assert.Equal(t, "remote-dom", remoteInfo["DomainName"])

	// Delete the connection.
	del := doRequest(t, h, http.MethodDelete,
		"/2021-01-01/opensearch/cc/outboundConnection/"+connID, nil)
	defer del.Body.Close()
	require.Equal(t, http.StatusOK, del.StatusCode)

	var delOut map[string]any
	require.NoError(t, json.NewDecoder(del.Body).Decode(&delOut))
	conn := delOut["Connection"].(map[string]any)
	assert.Equal(t, connID, conn["ConnectionId"])
}

func TestOutboundConnections_MissingRequiredFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
	}{
		{
			name: "missing connection alias",
			body: map[string]any{
				"LocalDomainInfo": map[string]any{
					"AWSDomainInformation": map[string]any{"DomainName": "local-dom"},
				},
				"RemoteDomainInfo": map[string]any{
					"AWSDomainInformation": map[string]any{"DomainName": "remote-dom"},
				},
			},
		},
		{
			name: "missing local domain name",
			body: map[string]any{
				"ConnectionAlias": "my-alias",
				"RemoteDomainInfo": map[string]any{
					"AWSDomainInformation": map[string]any{"DomainName": "remote-dom"},
				},
			},
		},
		{
			name: "missing remote domain name",
			body: map[string]any{
				"ConnectionAlias": "my-alias",
				"LocalDomainInfo": map[string]any{
					"AWSDomainInformation": map[string]any{"DomainName": "local-dom"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/cc/outboundConnection", tt.body)
			defer resp.Body.Close()

			assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		})
	}
}

func TestOutboundConnections_DeleteUnknownReturnsNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	resp := doRequest(t, h, http.MethodDelete,
		"/2021-01-01/opensearch/cc/outboundConnection/nonexistent-conn", nil)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}
