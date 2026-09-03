package apigateway_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	apigatewaysdk "github.com/aws/aws-sdk-go-v2/service/apigateway"
	"github.com/aws/aws-sdk-go-v2/service/apigateway/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

// TestErrorEnvelope_GetRestApiNotFoundDecodesToTypedError drives GetRestApi
// for a nonexistent API through the real aws-sdk-go-v2 apigateway client
// and asserts errors.As unwraps to the concrete *types.NotFoundException --
// not merely that an error occurred. apigateway is restjson1
// (aws-sdk-go-v2/service/apigateway@v1.42.4: awsRestjson1_ prefix, verified
// 124-of-124 deserializeOpError functions in deserializers.go identically
// read the X-Amzn-ErrorType response header first, falling back to a JSON
// body "code"/"__type" key via restjson.GetErrorInfo). This backend's
// handleError (handler.go) writes ErrorResponse{Type: "__type", Message:
// "message"} with no header -- exercising the same body-fallback path
// already fixed for this service's dispatch-miss/malformed-body sites
// under gopherstack-wlo1 (PARITY.md).
//
// Also asserts on the raw response bytes/headers for the same case, to pin
// the exact envelope rather than trust the SDK's own leniency
// (parity-principles.md).
func TestErrorEnvelope_GetRestApiNotFoundDecodesToTypedError(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	client := apigatewaysdk.NewFromConfig(cfg, func(o *apigatewaysdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})

	_, err = client.GetRestApi(t.Context(), &apigatewaysdk.GetRestApiInput{
		RestApiId: aws.String("does-not-exist"),
	})
	require.Error(t, err)

	var notFound *types.NotFoundException
	require.ErrorAs(t, err, &notFound,
		"expected *types.NotFoundException via errors.As, got %T: %v", err, err)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet,
		srv.URL+"/restapis/does-not-exist", nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusNotFound, resp.StatusCode)

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var envelope map[string]any
	require.NoError(t, json.Unmarshal(raw, &envelope))
	require.Equal(t, "NotFoundException", envelope["__type"],
		"raw body must carry __type key restjson.GetErrorInfo's fallback reads: %s", raw)

	_, hasMessage := envelope["message"]
	require.True(t, hasMessage, "raw body must carry a message key: %s", raw)
}
