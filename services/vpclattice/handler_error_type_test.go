package vpclattice_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	vpclatticesdk "github.com/aws/aws-sdk-go-v2/service/vpclattice"
	smithy "github.com/aws/smithy-go"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/vpclattice"
)

// newTestVPCLatticeClient stands up the real aws-sdk-go-v2 VPC Lattice
// client against an httptest server running this package's Handler, wired
// through the same pkgs/service registry/router used in production.
func newTestVPCLatticeClient(t *testing.T, h *vpclattice.Handler) *vpclatticesdk.Client {
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

	return vpclatticesdk.NewFromConfig(cfg, func(o *vpclatticesdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestGetService_UnknownServiceSurfacesResourceNotFoundException drives
// GetService for a service that doesn't exist through a real SDK client.
// Before this fix, handleError never set X-Amzn-Errortype nor a body
// code/__type field, so restjson.GetErrorInfo had nothing to read and every
// error -- including this one -- deserialized client-side as a generic
// UnknownError instead of the modeled ResourceNotFoundException
// (gopherstack-ifni; same bug class fixed for mediatailor in f41d5b42f).
func TestGetService_UnknownServiceSurfacesResourceNotFoundException(t *testing.T) {
	t.Parallel()

	backend := vpclattice.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestVPCLatticeClient(t, vpclattice.NewHandler(backend))

	_, err := client.GetService(t.Context(), &vpclatticesdk.GetServiceInput{
		ServiceIdentifier: aws.String("no-such-service"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "ResourceNotFoundException", apiErr.ErrorCode())
}
