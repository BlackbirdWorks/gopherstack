package elasticsearch_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticsearch"
)

func TestElasticsearchHandler_CreateOutboundCrossClusterSearchConnection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body         map[string]any
		name         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			body: map[string]any{
				"ConnectionAlias": "my-connection",
				"SourceDomainInfo": map[string]any{
					"OwnerId":    "123456789012",
					"DomainName": "local-domain",
					"Region":     "us-east-1",
				},
				"DestinationDomainInfo": map[string]any{
					"OwnerId":    "999999999999",
					"DomainName": "remote-domain",
					"Region":     "eu-west-1",
				},
			},
			wantCode: http.StatusOK,
			wantContains: []string{
				"CrossClusterSearchConnectionId", "my-connection", "VALIDATING",
				"SourceDomainInfo", "local-domain", "DestinationDomainInfo", "remote-domain",
			},
		},
		{
			name: "no_alias",
			body: map[string]any{
				"SourceDomainInfo":      map[string]any{"DomainName": "local"},
				"DestinationDomainInfo": map[string]any{"DomainName": "remote"},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "invalid_json",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			if tt.name == "invalid_json" {
				req := httptest.NewRequest(http.MethodPost, "/2015-01-01/es/ccs/outboundConnection",
					strings.NewReader("not-json"))
				req.Header.Set("Content-Type", "application/json")
				rw := httptest.NewRecorder()
				h.ServeHTTP(rw, req)
				assert.Equal(t, tt.wantCode, rw.Code)

				return
			}

			resp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/ccs/outboundConnection", tt.body)
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

// TestElasticsearchHandler_OutboundConnectionValidation verifies empty alias
// returns ErrValidation.
func TestElasticsearchHandler_OutboundConnectionValidation(t *testing.T) {
	t.Parallel()

	b := elasticsearch.NewInMemoryBackend("123456789012", "us-east-1")
	_, err := b.CreateOutboundCrossClusterSearchConnection(
		context.Background(),
		elasticsearch.CrossClusterDomainInfo{},
		elasticsearch.CrossClusterDomainInfo{},
		"",
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, elasticsearch.ErrValidation)
}
