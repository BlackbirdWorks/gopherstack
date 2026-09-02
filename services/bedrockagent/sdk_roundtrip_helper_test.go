package bedrockagent_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	bedrockagentsdk "github.com/aws/aws-sdk-go-v2/service/bedrockagent"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/bedrockagent"
)

const rtTestRegion = "us-east-1"

const rtTestAccountID = "123456789012"

// newRoundTripClient stands up the real aws-sdk-go-v2 bedrockagent client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production. Round-tripping
// through the genuine SDK serializer/deserializer is what proves wire
// compatibility -- in particular, that a List operation's maxResults/
// nextToken/filters/sortBy are read from wherever the real SDK actually
// binds them (mostly the JSON body here, not the query string a unit test
// calling h.Handler()(c) directly could get away with faking).
func newRoundTripClient(t *testing.T, h *bedrockagent.Handler) *bedrockagentsdk.Client {
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

	return bedrockagentsdk.NewFromConfig(cfg, func(o *bedrockagentsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// newTestHandlerAndClient is a convenience wrapper combining a fresh
// in-memory backend/handler pair with a round-trip SDK client against it.
func newTestHandlerAndClient(t *testing.T) *bedrockagentsdk.Client {
	t.Helper()

	backend := bedrockagent.NewTestBackend(rtTestRegion, rtTestAccountID)
	h := bedrockagent.NewTestHandler(backend)
	h.AccountID = rtTestAccountID
	h.DefaultRegion = rtTestRegion

	return newRoundTripClient(t, h)
}
