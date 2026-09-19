package apigatewayv2_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	apigatewayv2sdk "github.com/aws/aws-sdk-go-v2/service/apigatewayv2"
	appsyncsdk "github.com/aws/aws-sdk-go-v2/service/appsync"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
	"github.com/blackbirdworks/gopherstack/services/appsync"
	"github.com/blackbirdworks/gopherstack/services/ecr"
)

// newTestAPIGatewayV2RegistryServer wires up ApiGatewayV2's real Handler
// alongside ECR (with its embedded Docker Registry v2 proxy enabled -- the
// riskiest configuration, since a disabled registry never claims any /v2
// path at all) and AppSync. All three claim overlapping "/v2" path space:
// ECR's Docker Registry v2 API always lives at /v2/... (real docker
// clients), AppSync's GraphQL-API-as-code endpoints live at /v2/apis/...
// (appsync@v1.60.0), and ApiGatewayV2's own real REST surface is
// /v2/apis, /v2/domainnames, /v2/vpclinks, /v2/tags, /v2/portals and
// /v2/portalproducts (apigatewayv2@v1.37.4 serializers.go). ECR and AppSync
// both register earlier in cli.go's chain. cmd/routecollisions flags all of
// these as UNGUARDED-WINNER because it doesn't understand ECR's
// isRegistryPath compound guard (requires a "/manifests/", "/blobs/" or
// "/tags/list" marker -- see services/ecr/handler.go's isRegistryPath doc
// comment, already fixed for gopherstack-61i8) or AppSync's
// MatchesUserAgentMarker(header, "api/appsync") guard on /v2/apis
// (gopherstack-op3e census sweep 2026-09-19).
func newTestAPIGatewayV2RegistryServer(t *testing.T) *httptest.Server {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()

	stubRegistryHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})
	require.NoError(t, registry.Register(
		ecr.NewHandler(ecr.NewInMemoryBackend("000000000000", "us-east-1", ""), stubRegistryHandler),
	))
	require.NoError(t, registry.Register(
		appsync.NewHandler(appsync.NewInMemoryBackend("000000000000", "us-east-1", "")),
	))
	require.NoError(t, registry.Register(
		apigatewayv2.NewHandler(apigatewayv2.NewInMemoryBackend()),
	))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	return srv
}

// TestV2Routing_ApiGatewayV2NotShadowedByECROrAppSync sends the real
// AWS SDK request for every ApiGatewayV2 v2 endpoint cmd/routecollisions
// flagged as shadowed and confirms each reaches ApiGatewayV2's own handler
// (a real success response), not ECR's Docker registry proxy (which would
// have answered with the stub's 418) or AppSync's.
func TestV2Routing_ApiGatewayV2NotShadowedByECROrAppSync(t *testing.T) {
	t.Parallel()

	srv := newTestAPIGatewayV2RegistryServer(t)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	client := apigatewayv2sdk.NewFromConfig(
		cfg, func(o *apigatewayv2sdk.Options) { o.BaseEndpoint = aws.String(srv.URL) },
	)

	t.Run("apis", func(t *testing.T) {
		t.Parallel()

		out, apiErr := client.GetApis(t.Context(), &apigatewayv2sdk.GetApisInput{})
		require.NoError(t, apiErr)
		require.NotNil(t, out.Items)
	})

	t.Run("domainnames", func(t *testing.T) {
		t.Parallel()

		out, apiErr := client.GetDomainNames(t.Context(), &apigatewayv2sdk.GetDomainNamesInput{})
		require.NoError(t, apiErr)
		require.NotNil(t, out.Items)
	})

	t.Run("vpclinks", func(t *testing.T) {
		t.Parallel()

		out, apiErr := client.GetVpcLinks(t.Context(), &apigatewayv2sdk.GetVpcLinksInput{})
		require.NoError(t, apiErr)
		require.NotNil(t, out.Items)
	})

	t.Run("portals", func(t *testing.T) {
		t.Parallel()

		out, apiErr := client.ListPortals(t.Context(), &apigatewayv2sdk.ListPortalsInput{})
		require.NoError(t, apiErr)
		require.NotNil(t, out.Items)
	})

	t.Run("portalproducts", func(t *testing.T) {
		t.Parallel()

		out, apiErr := client.ListPortalProducts(t.Context(), &apigatewayv2sdk.ListPortalProductsInput{})
		require.NoError(t, apiErr)
		require.NotNil(t, out.Items)
	})
}

// TestV2Routing_AppSyncOwnApisNotShadowedByECR proves AppSync's own v2 Event
// API ListApis (GET /v2/apis, appsync@v1.60.0) still reaches AppSync's
// handler -- not ECR's Docker registry proxy -- when ECR's local registry
// is enabled and registered ahead of it in the same router. This is the
// "ecr shadows appsync" pair from the census (distinct from the
// "apis"/"portals"/etc subtests above, which cover ApiGatewayV2's own
// requests on the same literal prefixes).
func TestV2Routing_AppSyncOwnApisNotShadowedByECR(t *testing.T) {
	t.Parallel()

	srv := newTestAPIGatewayV2RegistryServer(t)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	client := appsyncsdk.NewFromConfig(cfg, func(o *appsyncsdk.Options) { o.BaseEndpoint = aws.String(srv.URL) })

	out, listErr := client.ListApis(t.Context(), &appsyncsdk.ListApisInput{})
	require.NoError(t, listErr)
	require.NotNil(t, out.Apis)
}
