package redshiftdata_test

import (
	"bytes"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	redshiftdatasdk "github.com/aws/aws-sdk-go-v2/service/redshiftdata"
	smithy "github.com/aws/smithy-go"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/redshiftdata"
)

// newRoundTripClient stands up a real aws-sdk-go-v2 redshiftdata client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production.
func newRoundTripClient(t *testing.T, h *redshiftdata.Handler) *redshiftdatasdk.Client {
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

	return redshiftdatasdk.NewFromConfig(cfg, func(o *redshiftdatasdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestHandler_OversizedBodySurfacesInternalServerException drives a real
// redshiftdata client's ExecuteStatement with a Sql string large enough to
// push the request body past httputils.MaxRequestBodyBytes (a real client
// can legitimately send this; aws-sdk-go-v2 imposes no client-side cap).
// Before this fix, Handler()'s ReadBody-failure branch wrote a bare
// "internal server error" text/plain body -- the awsjson1.1 error decoder
// (aws-sdk-go-v2@v1.43.4 aws/protocol/restjson.GetErrorInfo, which
// awsAwsjson11_deserializeOpError calls) cannot parse plain text, so the
// client saw smithy.GenericAPIError{Code:"UnknownError"} instead of a typed
// error (gopherstack-o7gx). InternalServerException matches this handler's
// own default fallback (modeled at redshiftdata@v1.43.4 types/errors.go).
func TestHandler_OversizedBodySurfacesInternalServerException(t *testing.T) {
	t.Parallel()

	backend := redshiftdata.NewInMemoryBackend("000000000000", "us-east-1")
	client := newRoundTripClient(t, redshiftdata.NewHandler(backend))

	huge := string(bytes.Repeat([]byte("x"), int(httputils.MaxRequestBodyBytes+1)))

	_, err := client.ExecuteStatement(t.Context(), &redshiftdatasdk.ExecuteStatementInput{
		Sql:               aws.String(huge),
		ClusterIdentifier: aws.String("test-cluster"),
		Database:          aws.String("dev"),
		DbUser:            aws.String("admin"),
	}, func(o *redshiftdatasdk.Options) { o.RetryMaxAttempts = 1 })
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "InternalServerException", apiErr.ErrorCode())
	assert.NotEqual(t, "UnknownError", apiErr.ErrorCode())
}
