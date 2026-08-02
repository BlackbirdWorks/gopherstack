package directconnect_test

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	directconnectsdk "github.com/aws/aws-sdk-go-v2/service/directconnect"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/directconnect"
)

const rtTestRegion = "us-east-1"

const rtTestAccountID = "000000000000"

// defaultAsyncWait/defaultAsyncPoll bound require.Eventually calls polling
// this service's async (requested/pending -> available) state machines --
// generous relative to asyncTransitionDelay's 100ms so CI jitter never
// flakes the test.
const (
	defaultAsyncWait = 2 * time.Second
	defaultAsyncPoll = 20 * time.Millisecond
)

// newRoundTripClient stands up the real aws-sdk-go-v2 directconnect client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production -- including
// the RouteMatcher and MatchPriority a unit test calling h.Handler()(c)
// directly would bypass. Round-tripping through the genuine SDK serializer/
// deserializer is what actually proves a request path and error response
// are wire-compatible: this service's exact lowerCamelCase JSON keys,
// epoch-seconds timestamps, and X-Amz-Target header dispatch all look fine
// to ad-hoc JSON assertions but fail against the real client if the wire
// shape is wrong -- matching services/outposts and services/resiliencehub's
// identical rationale for this helper.
func newRoundTripClient(t *testing.T, h *directconnect.Handler) *directconnectsdk.Client {
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

	return directconnectsdk.NewFromConfig(cfg, func(o *directconnectsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// newTestHandlerAndClient is a convenience wrapper combining a fresh
// in-memory backend/handler pair with a round-trip SDK client against it.
func newTestHandlerAndClient(t *testing.T) (*directconnect.Handler, *directconnectsdk.Client) {
	t.Helper()

	backend := directconnect.NewInMemoryBackend(t.Context(), rtTestAccountID, rtTestRegion)
	t.Cleanup(backend.Close)

	h := directconnect.NewHandler(backend)
	client := newRoundTripClient(t, h)

	return h, client
}
