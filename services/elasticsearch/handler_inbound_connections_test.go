package elasticsearch_test

import (
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticsearch"
)

func TestElasticsearchHandler_AcceptInboundCrossClusterSearchConnection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		seed         *elasticsearch.InboundConnection
		name         string
		connectionID string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "success",
			connectionID: "conn-001",
			seed: &elasticsearch.InboundConnection{
				ConnectionID:     "conn-001",
				ConnectionStatus: "PENDING_ACCEPTANCE",
				SourceDomainInfo: elasticsearch.CrossClusterDomainInfo{
					OwnerID:    "111111111111",
					DomainName: "source-domain",
					Region:     "us-west-2",
				},
				DestDomainInfo: elasticsearch.CrossClusterDomainInfo{
					OwnerID:    "123456789012",
					DomainName: "dest-domain",
					Region:     "us-east-1",
				},
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"CrossClusterSearchConnectionId", "conn-001", "ACTIVE"},
		},
		{
			name:         "not_found",
			connectionID: "nonexistent-conn",
			wantCode:     http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := elasticsearch.NewInMemoryBackend("123456789012", "us-east-1")
			h := elasticsearch.NewHandler(b)

			if tt.seed != nil {
				b.AddInboundConnectionInternal(context.Background(), *tt.seed)
			}

			resp := doRequest(t, h, http.MethodPut,
				"/2015-01-01/es/ccs/inboundConnection/"+tt.connectionID+"/accept", nil)
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

// TestElasticsearchHandler_InboundConnections_Lifecycle drives reject,
// delete, and describe (search) for inbound cross-cluster connections through
// the HTTP handler.
func TestElasticsearchHandler_InboundConnections_Lifecycle(t *testing.T) {
	t.Parallel()

	backend := elasticsearch.NewInMemoryBackend("123456789012", "us-east-1")
	backend.AddInboundConnectionInternal(context.Background(), elasticsearch.InboundConnection{
		ConnectionID: "connection-state", ConnectionStatus: "PENDING_ACCEPTANCE",
	})
	h := elasticsearch.NewHandler(backend)

	resp := doRequest(t, h, http.MethodPut,
		"/2015-01-01/es/ccs/inboundConnection/connection-state/reject", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body := readJSONBody(t, resp)
	connection := body["CrossClusterSearchConnection"].(map[string]any)
	assert.Equal(t, "REJECTED", connection["ConnectionStatus"].(map[string]any)["StatusCode"])

	resp = doRequest(t, h, http.MethodDelete, "/2015-01-01/es/ccs/inboundConnection/connection-state", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	resp = doRequest(t, h, http.MethodPost, "/2015-01-01/es/ccs/inboundConnection/search", nil)
	assert.Empty(t, readJSONBody(t, resp)["CrossClusterSearchConnections"])
}
