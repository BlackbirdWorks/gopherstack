package apigatewayv2_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	apigatewayv2sdk "github.com/aws/aws-sdk-go-v2/service/apigatewayv2"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/s3"
)

func TestCustomDomainRouting(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	h.SetLambdaInvoker(&mockLambdaInvoker{
		fn: func(_ context.Context, name, _ string, payload []byte) ([]byte, int, error) {
			var ev struct {
				RequestContext struct {
					Stage string `json:"stage"`
				} `json:"requestContext"`
				RawPath string `json:"rawPath"`
			}
			_ = json.Unmarshal(payload, &ev)
			b, _ := json.Marshal(map[string]any{
				"statusCode": 200,
				"body":       name + "|" + ev.RequestContext.Stage + "|" + ev.RawPath,
			})

			return b, 200, nil
		},
	})

	const lambdaPrefix = "arn:aws:lambda:us-east-1:123456789012:function:"

	apiA := buildHTTPAPIWithLambda(t, h, "GET /hello", lambdaPrefix+"fn-a/invocations")
	apiB := buildHTTPAPIWithLambda(t, h, "GET /hello", lambdaPrefix+"fn-b/invocations")

	for _, id := range []string{apiA, apiB} {
		ensureDefaultStage(t, h, id)
		rr := doRequest(t, h, http.MethodPost, "/v2/apis/"+id+"/stages", map[string]any{
			"stageName": "dev", "autoDeploy": true,
		})
		require.Equal(t, http.StatusCreated, rr.Code)
	}

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	require.NoError(t, registry.Register(s3.NewHandler(s3.NewInMemoryBackend(&s3.GzipCompressor{}))))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(t.Context(), awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")))
	require.NoError(t, err)

	client := apigatewayv2sdk.NewFromConfig(cfg, func(o *apigatewayv2sdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})

	_, err = client.CreateDomainName(t.Context(), &apigatewayv2sdk.CreateDomainNameInput{
		DomainName: aws.String("api.example.com"),
	})
	require.NoError(t, err)

	for key, m := range map[string][2]string{
		"":        {apiA, "$default"},
		"v1":      {apiA, "dev"},
		"v1/deep": {apiB, "dev"},
	} {
		in := &apigatewayv2sdk.CreateApiMappingInput{
			DomainName: aws.String("api.example.com"),
			ApiId:      aws.String(m[0]),
			Stage:      aws.String(m[1]),
		}
		if key != "" {
			in.ApiMappingKey = aws.String(key)
		}

		_, err = client.CreateApiMapping(t.Context(), in)
		require.NoError(t, err)
	}

	tests := []struct {
		name       string
		host       string
		path       string
		wantBody   string
		wantStatus int
	}{
		{"root mapping", "api.example.com:4566", "/hello", "fn-a|$default|/hello", http.StatusOK},
		{"mapping key and stage", "api.example.com", "/v1/hello", "fn-a|dev|/dev/hello", http.StatusOK},
		{"longest key wins", "api.example.com", "/v1/deep/hello", "fn-b|dev|/dev/hello", http.StatusOK},
		{"host case-insensitive", "API.Example.COM", "/hello", "fn-a|$default|/hello", http.StatusOK},
		{"unrouted path in mapped api", "api.example.com", "/v1/nope", "", http.StatusNotFound},
		{"unknown host falls to s3", "other.example.com", "/hello", "NoSuchBucket", http.StatusNotFound},
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

func TestCustomDomainRoutingForbiddenWithoutMapping(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	require.NoError(t, registry.Register(s3.NewHandler(s3.NewInMemoryBackend(&s3.GzipCompressor{}))))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	rr := doRequest(t, h, http.MethodPost, "/v2/domainnames", map[string]any{"domainName": "bare.example.com"})
	require.Equal(t, http.StatusCreated, rr.Code)

	req, err := http.NewRequest(http.MethodGet, srv.URL+"/anything", nil)
	require.NoError(t, err)

	req.Host = "bare.example.com"

	resp, err := srv.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
	assert.JSONEq(t, `{"message":"Forbidden"}`, string(body))
}
