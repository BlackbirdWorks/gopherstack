package lightsail_test

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	lightsailsdk "github.com/aws/aws-sdk-go-v2/service/lightsail"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/lightsail"
)

const rtTestRegion = "us-east-1"

const rtTestAccountID = "000000000000"

// defaultAsyncWait/defaultAsyncPoll bound require.Eventually calls polling
// this service's async state machines -- matching services/outposts/
// resiliencehub/grafana/directconnect/mgn/networkmanager's identical
// rationale for their own defaultAsyncWait. This service's longest chain
// (a ContainerService's PENDING creation steps plus its DEPLOYING steps) is
// 5*asyncTransitionDelay = 500ms; 5s is generous relative to that for CI
// jitter.
const (
	defaultAsyncWait = 5 * time.Second
	defaultAsyncPoll = 20 * time.Millisecond
)

// newRoundTripClient stands up the real aws-sdk-go-v2 lightsail client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production -- including
// the RouteMatcher and MatchPriority a unit test calling h.Handler()(c)
// directly would bypass. Round-tripping through the genuine SDK serializer/
// deserializer is what actually proves a request path and error response
// are wire-compatible: this service's lowerCamel JSON members, its literal
// "Lightsail_20161128.<Op>" X-Amz-Target header, and its epoch-seconds
// timestamp convention all look fine to ad-hoc JSON assertions but fail
// against the real client if the wire shape is wrong -- matching
// services/mgn/networkmanager/directconnect's identical rationale for this
// helper.
func newRoundTripClient(t *testing.T, h *lightsail.Handler) *lightsailsdk.Client {
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

	return lightsailsdk.NewFromConfig(cfg, func(o *lightsailsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// newTestClient is a convenience wrapper combining a fresh in-memory
// backend/handler pair with a round-trip SDK client against it. Every test
// in this package drives the backend purely through the SDK client, never
// the handler/backend directly, so only the client is returned.
func newTestClient(t *testing.T) *lightsailsdk.Client {
	t.Helper()

	backend := lightsail.NewInMemoryBackend(t.Context(), rtTestAccountID, rtTestRegion)
	t.Cleanup(backend.Close)

	h := lightsail.NewHandler(backend)

	return newRoundTripClient(t, h)
}
