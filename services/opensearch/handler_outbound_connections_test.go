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
			"ConnectionAlias":  "my-alias",
			"LocalDomainInfo":  map[string]any{"DomainName": "local-dom"},
			"RemoteDomainInfo": map[string]any{"DomainName": "remote-dom"},
		})
	defer cr.Body.Close()
	require.Equal(t, http.StatusOK, cr.StatusCode)

	var cOut map[string]any
	require.NoError(t, json.NewDecoder(cr.Body).Decode(&cOut))
	connID := cOut["ConnectionId"].(string)
	assert.NotEmpty(t, connID)
	assert.Equal(t, "my-alias", cOut["ConnectionAlias"])

	// Describe returns the connection.
	dr := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/cc/outboundConnection", nil)
	defer dr.Body.Close()
	require.Equal(t, http.StatusOK, dr.StatusCode)

	var dOut map[string]any
	require.NoError(t, json.NewDecoder(dr.Body).Decode(&dOut))
	conns, ok := dOut["Connections"].([]any)
	require.True(t, ok)
	assert.Len(t, conns, 1)

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
