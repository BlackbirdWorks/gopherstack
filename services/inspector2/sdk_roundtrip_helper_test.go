package inspector2_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	inspector2sdk "github.com/aws/aws-sdk-go-v2/service/inspector2"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/inspector2"
)

const rtTestRegion = "us-east-1"
const rtTestAccountID = "123456789012"

// newRoundTripClient stands up the real aws-sdk-go-v2 inspector2 client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production. Round-tripping
// through the genuine SDK serializer/deserializer is what actually proves a
// response key is wire-compatible: the SDK silently decodes nothing from an
// unrecognised key, so asserting on raw JSON (as every other _test.go file
// in this package does via auditDo) cannot catch a wrong response-key bug --
// only a real client can.
func newRoundTripClient(t *testing.T, h *inspector2.Handler) *inspector2sdk.Client {
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

	return inspector2sdk.NewFromConfig(cfg, func(o *inspector2sdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// newRoundTripTestClient is a convenience wrapper combining a fresh
// in-memory backend/handler pair with a round-trip SDK client against it.
func newRoundTripTestClient(t *testing.T) *inspector2sdk.Client {
	t.Helper()

	backend := inspector2.NewInMemoryBackend(rtTestAccountID, rtTestRegion)
	h := inspector2.NewHandler(backend)

	return newRoundTripClient(t, h)
}
