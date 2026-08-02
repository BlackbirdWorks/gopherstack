package outposts_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	outpostssdk "github.com/aws/aws-sdk-go-v2/service/outposts"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/outposts"
)

const rtTestRegion = "us-east-1"

const rtTestAccountID = "000000000000"

// newRoundTripClient stands up the real aws-sdk-go-v2 outposts client against
// an httptest server running this package's Handler, wired through the same
// pkgs/service registry/router used in production -- including the
// RouteMatcher and MatchPriority a unit test calling h.Handler()(c) directly
// would bypass. Round-tripping through the genuine SDK serializer/
// deserializer is what actually proves a request path and error response are
// wire-compatible: this service's PascalCase document members, its
// percent-encoded ARN-in-path /tags/{resourceArn} route, and its
// epoch-seconds timestamp fields all look fine to ad-hoc JSON assertions but
// fail against the real client if the wire shape is wrong -- matching
// services/grafana's identical rationale for this helper.
func newRoundTripClient(t *testing.T, h *outposts.Handler) *outpostssdk.Client {
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

	return outpostssdk.NewFromConfig(cfg, func(o *outpostssdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// newTestHandlerAndClient is a convenience wrapper combining a fresh
// in-memory backend/handler pair with a round-trip SDK client against it.
func newTestHandlerAndClient(t *testing.T) (*outposts.Handler, *outpostssdk.Client) {
	t.Helper()

	backend := outposts.NewInMemoryBackend(t.Context(), rtTestAccountID, rtTestRegion)
	t.Cleanup(backend.Close)

	h := outposts.NewHandler(backend)
	client := newRoundTripClient(t, h)

	return h, client
}
