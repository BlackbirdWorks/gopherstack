package networkmanager_test

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	networkmanagersdk "github.com/aws/aws-sdk-go-v2/service/networkmanager"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/networkmanager"
)

const rtTestRegion = "us-east-1"

const rtTestAccountID = "000000000000"

// defaultAsyncWait/defaultAsyncPoll bound require.Eventually calls polling
// this service's async (PENDING/CREATING -> AVAILABLE, DELETING -> gone)
// state machines -- generous relative to asyncTransitionDelay's 100ms so CI
// jitter never flakes the test, matching services/mgn/outposts/
// resiliencehub/grafana's identical rationale for their own constants.
const (
	defaultAsyncWait = 2 * time.Second
	defaultAsyncPoll = 20 * time.Millisecond
)

// newRoundTripClient stands up the real aws-sdk-go-v2 networkmanager client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production -- including the
// RouteMatcher and MatchPriority a unit test calling h.Handler()(c) directly
// would bypass. Round-tripping through the genuine SDK serializer/
// deserializer is what actually proves a request path and error response are
// wire-compatible: this service's PascalCase JSON members, its genuine REST
// paths with real path parameters (unlike mgn/resiliencehub's single-action-
// slug convention), its percent-encoded ARN-in-path routes, and its
// epoch-seconds timestamp fields all look fine to ad-hoc JSON assertions but
// fail against the real client if the wire shape is wrong.
func newRoundTripClient(t *testing.T, h *networkmanager.Handler) *networkmanagersdk.Client {
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

	return networkmanagersdk.NewFromConfig(cfg, func(o *networkmanagersdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// newTestHandlerAndClient is a convenience wrapper combining a fresh
// in-memory backend/handler pair with a round-trip SDK client against it.
func newTestHandlerAndClient(t *testing.T) (*networkmanager.Handler, *networkmanagersdk.Client) {
	t.Helper()

	backend := networkmanager.NewInMemoryBackend(t.Context(), rtTestAccountID, rtTestRegion)
	t.Cleanup(backend.Close)

	h := networkmanager.NewHandler(backend)
	client := newRoundTripClient(t, h)

	return h, client
}
