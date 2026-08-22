package shield_test

import (
	"bytes"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	shieldsdk "github.com/aws/aws-sdk-go-v2/service/shield"
	smithy "github.com/aws/smithy-go"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/shield"
)

// newRoundTripClient stands up a real aws-sdk-go-v2 shield client against an
// httptest server running this package's Handler, wired through the same
// pkgs/service registry/router used in production.
func newRoundTripClient(t *testing.T, h *shield.Handler) *shieldsdk.Client {
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

	return shieldsdk.NewFromConfig(cfg, func(o *shieldsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestHandler_OversizedBodySurfacesInternalErrorException drives a real
// shield client's CreateProtection with a Name large enough to push the
// request body past httputils.MaxRequestBodyBytes (a real client can
// legitimately send this; aws-sdk-go-v2 imposes no client-side cap). Before
// this fix, Handler()'s ReadBody-failure branch wrote a bare
// "internal server error" text/plain body -- the awsjson1.1 error decoder
// (aws-sdk-go-v2@v1.43.4 aws/protocol/restjson.GetErrorInfo, which
// awsAwsjson11_deserializeOpError calls) cannot parse plain text, so the
// client saw smithy.GenericAPIError{Code:"UnknownError"} instead of the
// typed InternalErrorException classifyShieldError's default branch already
// produces for every unmatched backend error (gopherstack-o7gx).
func TestHandler_OversizedBodySurfacesInternalErrorException(t *testing.T) {
	t.Parallel()

	backend := shield.NewInMemoryBackend("000000000000", "us-east-1")
	client := newRoundTripClient(t, shield.NewHandler(backend))

	huge := string(bytes.Repeat([]byte("x"), int(httputils.MaxRequestBodyBytes+1)))

	_, err := client.CreateProtection(t.Context(), &shieldsdk.CreateProtectionInput{
		Name:        aws.String(huge),
		ResourceArn: aws.String("arn:aws:ec2:us-east-1:000000000000:eip-allocation/eipalloc-12345678"),
	}, func(o *shieldsdk.Options) { o.RetryMaxAttempts = 1 })
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "InternalErrorException", apiErr.ErrorCode())
	assert.NotEqual(t, "UnknownError", apiErr.ErrorCode())
}
