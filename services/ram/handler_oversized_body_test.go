package ram_test

import (
	"bytes"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	ramsdk "github.com/aws/aws-sdk-go-v2/service/ram"
	smithy "github.com/aws/smithy-go"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/ram"
)

// newRoundTripClient stands up a real aws-sdk-go-v2 ram client against an
// httptest server running this package's Handler, wired through the same
// pkgs/service registry/router used in production.
func newRoundTripClient(t *testing.T, h *ram.Handler) *ramsdk.Client {
	t.Helper()

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

	return ramsdk.NewFromConfig(cfg, func(o *ramsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestHandler_OversizedBodySurfacesServerInternalException drives a real ram
// client's CreateResourceShare with a Name large enough to push the request
// body past httputils.MaxRequestBodyBytes (a real client can legitimately
// send this; aws-sdk-go-v2 imposes no client-side cap). Before this fix,
// Handler()'s ReadBody-failure branch wrote a bare "internal server error"
// text/plain body -- the restjson1 error decoder (aws-sdk-go-v2@v1.43.4
// aws/protocol/restjson.GetErrorInfo) cannot parse plain text, so the client
// saw smithy.GenericAPIError{Code:"UnknownError"} instead of a typed error
// (gopherstack-o7gx). ServerInternalException is ram's own modeled internal
// error (ram@v1.39.4 types/errors.go).
func TestHandler_OversizedBodySurfacesServerInternalException(t *testing.T) {
	t.Parallel()

	backend := ram.NewInMemoryBackend("000000000000", "us-east-1")
	client := newRoundTripClient(t, ram.NewHandler(backend))

	huge := string(bytes.Repeat([]byte("x"), int(httputils.MaxRequestBodyBytes+1)))

	_, err := client.CreateResourceShare(t.Context(), &ramsdk.CreateResourceShareInput{
		Name: aws.String(huge),
	}, func(o *ramsdk.Options) { o.RetryMaxAttempts = 1 })
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "ServerInternalException", apiErr.ErrorCode())
	assert.NotEqual(t, "UnknownError", apiErr.ErrorCode())
}
