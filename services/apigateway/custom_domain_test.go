package apigateway_test

import (
	"io"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	apigwsdk "github.com/aws/aws-sdk-go-v2/service/apigateway"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCustomDomainRouting(t *testing.T) {
	t.Parallel()

	srv := newProxyRoutingServer(t)
	apiID := setupDeployedMockAPI(t, srv.Client(), srv.URL)

	cfg, err := awscfg.LoadDefaultConfig(t.Context(), awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")))
	require.NoError(t, err)

	client := apigwsdk.NewFromConfig(cfg, func(o *apigwsdk.Options) { o.BaseEndpoint = aws.String(srv.URL) })

	for _, d := range []string{"api.example.com", "rooted.example.com", "bare.example.com"} {
		_, err = client.CreateDomainName(t.Context(), &apigwsdk.CreateDomainNameInput{DomainName: aws.String(d)})
		require.NoError(t, err)
	}

	mappings := []struct {
		domain, path, stage string
	}{
		{"api.example.com", "v1", "prod"},
		{"api.example.com", "v1/deep", "ghost"},
		{"api.example.com", "nostage", ""},
		{"rooted.example.com", "", "prod"},
	}

	for _, m := range mappings {
		in := &apigwsdk.CreateBasePathMappingInput{DomainName: aws.String(m.domain), RestApiId: aws.String(apiID)}
		if m.path != "" {
			in.BasePath = aws.String(m.path)
		}

		if m.stage != "" {
			in.Stage = aws.String(m.stage)
		}

		_, err = client.CreateBasePathMapping(t.Context(), in)
		require.NoError(t, err)
	}

	tests := []struct {
		name       string
		host       string
		path       string
		wantBody   string
		wantStatus int
	}{
		{"base path and stage", "api.example.com:4566", "/v1", "", http.StatusOK},
		{"base path trailing slash", "api.example.com", "/v1/", "", http.StatusOK},
		{"host case-insensitive", "API.example.com", "/v1", "", http.StatusOK},
		{"stage taken from path", "api.example.com", "/nostage/prod", "", http.StatusOK},
		{"longest path wins", "api.example.com", "/v1/deep", "", http.StatusForbidden},
		{"stage-less mapping without stage", "api.example.com", "/nostage", "Forbidden", http.StatusForbidden},
		{"unmatched base path", "api.example.com", "/other", "Forbidden", http.StatusForbidden},
		{"segment boundary", "api.example.com", "/v10", "Forbidden", http.StatusForbidden},
		{"root mapping", "rooted.example.com", "/", "", http.StatusOK},
		{"domain without mappings", "bare.example.com", "/v1", "Forbidden", http.StatusForbidden},
		{"unknown host reaches s3", "unknown.example.com", "/some-bucket", "NoSuchBucket", http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req, rerr := http.NewRequest(http.MethodGet, srv.URL+tt.path, nil)
			require.NoError(t, rerr)

			req.Host = tt.host

			resp, rerr := srv.Client().Do(req)
			require.NoError(t, rerr)
			defer resp.Body.Close()

			body, rerr := io.ReadAll(resp.Body)
			require.NoError(t, rerr)

			assert.Equal(t, tt.wantStatus, resp.StatusCode, string(body))
			assert.Contains(t, string(body), tt.wantBody)
		})
	}
}
