package appmesh_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	appmeshsdk "github.com/aws/aws-sdk-go-v2/service/appmesh"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/appmesh"
)

const rtTestRegion = "us-east-1"

// newRoundTripClient stands up the real aws-sdk-go-v2 appmesh client against
// an httptest server running this package's Handler through the same
// pkgs/service registry/router used in production. Round-tripping through the
// genuine SDK serializer/deserializer is what actually proves a response body
// is wire-compatible: a raw-body unit test can only confirm an expected key
// is present, never that the real client can decode it (see the wrapper-key
// bug in handler_meshes.go etc. -- a flat body decodes to a nil resource
// through the real client's DescribeMeshOutput.Mesh field even though a
// hand-rolled JSON assertion on the same body looks fine).
func newRoundTripClient(t *testing.T, h *appmesh.Handler) *appmeshsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(rtTestRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return appmeshsdk.NewFromConfig(cfg, func(o *appmeshsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// newTestHandlerAndClient is a convenience wrapper combining a fresh
// in-memory backend/handler pair with a round-trip SDK client against it.
func newTestHandlerAndClient(t *testing.T) *appmeshsdk.Client {
	t.Helper()

	backend := appmesh.NewInMemoryBackend("000000000000", rtTestRegion)
	h := appmesh.NewHandler(backend)
	client := newRoundTripClient(t, h)

	return client
}
