package sts_test

import (
	"bytes"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	stssdk "github.com/aws/aws-sdk-go-v2/service/sts"
	smithy "github.com/aws/smithy-go"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/sts"
)

// newTestSTSRoutedClient stands up the real aws-sdk-go-v2 STS client against
// an httptest server running this package's Handler, wired through the same
// pkgs/service registry/router used in production (service.NewRegistry +
// service.NewServiceRouter) -- unlike this package's other test helpers
// (newTestHandler/postForm, buildSTSClient+newEchoServer), which mount
// Handler() directly and bypass RouteMatcher entirely.
func newTestSTSRoutedClient(t *testing.T) *stssdk.Client {
	t.Helper()

	h := sts.NewHandler(sts.NewInMemoryBackend())
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

	return stssdk.NewFromConfig(cfg, func(o *stssdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestHandler_OversizedBodySurfacesInternalFailure drives a real STS
// client's GetSessionToken with a SerialNumber large enough to push the
// request body (form-urlencoded, Query/XML protocol) past
// httputils.MaxRequestBodyBytes, through the same registry/router used in
// production.
//
// Before this fix, RouteMatcher's own httputils.ReadBody call swallowed this
// same read failure as a plain "false", so the router found no owner and
// answered a generic 404 -- masking Handler()'s already-typed InternalFailure.
// RouteMatcher now falls back to the api/sts User-Agent marker (set by
// every aws-sdk-go-v2 sts client) when the body can't be read, claims the
// request, and lets Handler() produce the typed error.
func TestHandler_OversizedBodySurfacesInternalFailure(t *testing.T) {
	t.Parallel()

	client := newTestSTSRoutedClient(t)

	huge := aws.String(string(bytes.Repeat([]byte("x"), int(httputils.MaxRequestBodyBytes+1))))

	_, err := client.GetSessionToken(t.Context(), &stssdk.GetSessionTokenInput{
		SerialNumber: huge,
	}, func(o *stssdk.Options) { o.RetryMaxAttempts = 1 })
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "InternalFailure", apiErr.ErrorCode())
	assert.NotEqual(t, "UnknownError", apiErr.ErrorCode())
}

// TestHandler_NormalSizedBodyStillRoutes is the regression guard: a normal
// request must still reach Handler() and succeed now that RouteMatcher's
// read-failure branch has changed.
func TestHandler_NormalSizedBodyStillRoutes(t *testing.T) {
	t.Parallel()

	client := newTestSTSRoutedClient(t)

	out, err := client.GetSessionToken(t.Context(), &stssdk.GetSessionTokenInput{})
	require.NoError(t, err)
	assert.NotNil(t, out)
}
