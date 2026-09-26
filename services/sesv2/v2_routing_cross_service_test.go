package sesv2_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sesv2sdk "github.com/aws/aws-sdk-go-v2/service/sesv2"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/ecr"
	"github.com/blackbirdworks/gopherstack/services/sesv2"
)

// newTestSESv2ECRRegistryServer wires up SESv2's real Handler alongside ECR
// with its embedded Docker Registry v2 proxy enabled -- the riskiest
// configuration, since a disabled registry never claims any /v2 path at
// all. ECR registers earlier in cli.go's chain and its RouteMatcher claims
// isRegistryPath(path), a bare-"/v2" prefix by cmd/routecollisions'
// extraction. The real guard (services/ecr/handler.go's isRegistryPath)
// requires a "/manifests/", "/blobs/" or "/tags/list" marker after "/v2/",
// or an exact "/v2", "/v2/" or "/v2/_catalog" -- SESv2's real
// ListConfigurationSets path (GET /v2/email/configuration-sets,
// sesv2@v1.66.4 serializers.go) matches none of those, so ECR's matcher
// never actually claims it (gopherstack-op3e census sweep 2026-09-19; the
// same guard was already fixed for gopherstack-61i8).
func newTestSESv2ECRRegistryServer(t *testing.T) *httptest.Server {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()

	stubRegistryHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})
	require.NoError(t, registry.Register(
		ecr.NewHandler(ecr.NewInMemoryBackend("000000000000", "us-east-1", ""), stubRegistryHandler),
	))
	require.NoError(t, registry.Register(sesv2.NewHandler(sesv2.NewInMemoryBackend())))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	return srv
}

// TestV2Routing_SESv2NotShadowedByECR proves SESv2's own
// ListConfigurationSets (GET /v2/email/configuration-sets) still reaches
// SESv2's handler (a real success response), not ECR's Docker registry
// proxy (which would have answered with the stub's 418), when ECR's local
// registry is enabled and registered ahead of it in the same router.
func TestV2Routing_SESv2NotShadowedByECR(t *testing.T) {
	t.Parallel()

	srv := newTestSESv2ECRRegistryServer(t)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	client := sesv2sdk.NewFromConfig(cfg, func(o *sesv2sdk.Options) { o.BaseEndpoint = aws.String(srv.URL) })

	out, sesErr := client.ListConfigurationSets(t.Context(), &sesv2sdk.ListConfigurationSetsInput{})
	require.NoError(t, sesErr)
	require.NotNil(t, out.ConfigurationSets)
}
