package grafana_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	grafanasdk "github.com/aws/aws-sdk-go-v2/service/grafana"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/grafana"
)

const rtTestRegion = "us-east-1"

// newRoundTripClient stands up the real aws-sdk-go-v2 grafana client against
// an httptest server running this package's Handler, wired through the same
// pkgs/service registry/router used in production -- including the
// RouteMatcher and MatchPriority a unit test calling h.Handler()(c) directly
// would bypass. Round-tripping through the genuine SDK serializer/
// deserializer is what actually proves a request path and error response are
// wire-compatible: the /tags/{resourceArn} route's embedded, percent-encoded
// ARN (which itself contains a literal "/" from the "/workspaces/{id}"
// resource segment) and epoch-seconds timestamp fields look fine to ad-hoc
// JSON assertions but fail against the real client if rawPathSegments or
// awstime.Epoch is wrong.
func newRoundTripClient(t *testing.T, h *grafana.Handler) *grafanasdk.Client {
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

	return grafanasdk.NewFromConfig(cfg, func(o *grafanasdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// newTestHandlerAndClient is a convenience wrapper combining a fresh
// in-memory backend/handler pair with a round-trip SDK client against it.
func newTestHandlerAndClient(t *testing.T) (*grafana.Handler, *grafanasdk.Client) {
	t.Helper()

	backend := grafana.NewInMemoryBackend(t.Context(), "000000000000", rtTestRegion)
	t.Cleanup(backend.Close)

	h := grafana.NewHandler(backend)
	client := newRoundTripClient(t, h)

	return h, client
}
