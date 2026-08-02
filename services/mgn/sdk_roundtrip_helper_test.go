package mgn_test

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	mgnsdk "github.com/aws/aws-sdk-go-v2/service/mgn"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/mgn"
)

const rtTestRegion = "us-east-1"

const rtTestAccountID = "000000000000"

// defaultAsyncWait/defaultAsyncPoll bound require.Eventually calls polling
// this service's async state machines. This service's longest chain
// (SeedSourceServer's 3-tick replication progression) is
// 3*asyncTransitionDelay = 300ms; 5s is generous relative to that for CI
// jitter, matching services/resiliencehub/outposts/grafana's identical
// rationale for their own defaultAsyncWait.
const (
	defaultAsyncWait = 5 * time.Second
	defaultAsyncPoll = 20 * time.Millisecond
)

// newRoundTripClient stands up the real aws-sdk-go-v2 mgn client against an
// httptest server running this package's Handler, wired through the same
// pkgs/service registry/router used in production -- including the
// RouteMatcher and MatchPriority a unit test calling h.Handler()(c) directly
// would bypass. Round-tripping through the genuine SDK serializer/
// deserializer is what actually proves a request path and error response
// are wire-compatible: this service's lowerCamel JSON members (with
// embedded-acronym casing like "sourceServerID"), its literal PascalCase
// action paths (and the /network-migration/ prefix on 25 of them), its
// percent-encoded ARN-in-path /tags/{resourceArn} route, and its
// epoch-seconds vs. RFC3339-string dual timestamp convention all look fine
// to ad-hoc JSON assertions but fail against the real client if the wire
// shape is wrong -- matching services/outposts/resiliencehub/grafana's
// identical rationale for this helper.
func newRoundTripClient(t *testing.T, h *mgn.Handler) *mgnsdk.Client {
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

	return mgnsdk.NewFromConfig(cfg, func(o *mgnsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// newTestHandlerAndClient is a convenience wrapper combining a fresh
// in-memory backend/handler pair with a round-trip SDK client against it.
// The backend is pre-initialized (InitializeService) since 69 of 95
// operations return UninitializedAccountException otherwise (PARITY.md) --
// tests exercising that gate explicitly call a fresh, non-initialized
// backend instead.
func newTestHandlerAndClient(t *testing.T) (*mgn.Handler, *mgnsdk.Client) {
	t.Helper()

	backend := mgn.NewInMemoryBackend(t.Context(), rtTestAccountID, rtTestRegion)
	t.Cleanup(backend.Close)

	backend.InitializeService()

	h := mgn.NewHandler(backend)
	client := newRoundTripClient(t, h)

	return h, client
}
