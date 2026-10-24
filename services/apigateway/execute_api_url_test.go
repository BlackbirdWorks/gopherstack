package apigateway_test

import (
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExecuteAPIInvokeURLs(t *testing.T) {
	t.Parallel()

	srv := newProxyRoutingServer(t)
	client := srv.Client()
	apiID := setupDeployedMockAPI(t, client, srv.URL)

	tests := []struct {
		name       string
		host       string
		path       string
		wantStatus int
		notS3      bool
	}{
		{
			name:       "host_style",
			host:       apiID + ".execute-api.localhost.localstack.cloud:4566",
			path:       "/prod",
			wantStatus: http.StatusOK,
			notS3:      true,
		},
		{
			name:       "host_style_aws_domain",
			host:       apiID + ".execute-api.us-east-1.amazonaws.com",
			path:       "/prod/",
			wantStatus: http.StatusOK,
			notS3:      true,
		},
		{
			name:       "internal_path",
			path:       "/_aws/execute-api/" + apiID + "/prod",
			wantStatus: http.StatusOK,
			notS3:      true,
		},
		{
			name:       "internal_path_trailing",
			path:       "/_aws/execute-api/" + apiID + "/prod/",
			wantStatus: http.StatusOK,
			notS3:      true,
		},
		{
			name:       "unknown_stage",
			path:       "/_aws/execute-api/" + apiID + "/nope",
			wantStatus: http.StatusForbidden,
			notS3:      true,
		},
		{
			name:       "unknown_api_host",
			host:       "zzzzzzzz.execute-api.localhost.localstack.cloud",
			path:       "/prod",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req, err := http.NewRequest(http.MethodGet, srv.URL+tt.path, nil)
			require.NoError(t, err)

			if tt.host != "" {
				req.Host = tt.host
			}

			resp, err := client.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			assert.Equal(t, tt.wantStatus, resp.StatusCode, string(body))

			if tt.notS3 {
				assert.NotContains(t, string(body), "NoSuchBucket")
			}
		})
	}
}
