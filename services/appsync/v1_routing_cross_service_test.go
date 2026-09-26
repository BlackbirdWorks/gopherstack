package appsync_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	appsyncsdk "github.com/aws/aws-sdk-go-v2/service/appsync"
	smithy "github.com/aws/smithy-go"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/appsync"
	"github.com/blackbirdworks/gopherstack/services/codeartifact"
	"github.com/blackbirdworks/gopherstack/services/polly"
)

// newTestAppSyncV1RegistryServer wires up AppSync's real Handler alongside
// Polly and CodeArtifact -- two services registered earlier in cli.go's
// chain whose RouteMatcher also claims "/v1/..." path space. Polly's
// pollyPathPrefix is a bare "/v1/" (services/polly/handler.go), but its
// matcher additionally requires parseRoute(method, path).operation !=
// opUnknown -- parseRoute is a closed whitelist of exactly 5 routes
// (/v1/speech, /v1/synthesisStream, /v1/synthesisTasks, /v1/voices,
// /v1/lexicons, and lexicon/task sub-resources), so it always rejects
// AppSync's paths regardless of which one is tried. CodeArtifact's
// pathV1Domain check requires a "/" boundary after "/v1/domain"
// (isDomainRepoPath), so it never matches "/v1/domainnames" (no separator),
// and its "/v1/tags" claim is an EXACT match that can never intercept
// AppSync's real "/v1/tags/{arn}" (always has a resource segment).
// cmd/routecollisions flags all of these as UNGUARDED-WINNER because its
// literal extraction can't see parseRoute's per-path whitelist or the
// exact-vs-prefix / boundary distinctions (gopherstack-op3e census sweep
// 2026-09-19).
func newTestAppSyncV1RegistryServer(t *testing.T) *httptest.Server {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()

	require.NoError(t, registry.Register(polly.NewHandler(polly.NewInMemoryBackend())))
	require.NoError(t, registry.Register(
		codeartifact.NewHandler(codeartifact.NewInMemoryBackend("000000000000", "us-east-1")),
	))
	require.NoError(t, registry.Register(
		appsync.NewHandler(appsync.NewInMemoryBackend("000000000000", "us-east-1", "")),
	))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	return srv
}

// TestV1Routing_AppSyncNotShadowedByPollyOrCodeArtifact sends the real
// AWS SDK request for AppSync's ListDomainNames (GET /v1/domainnames) and
// ListTagsForResource (GET /v1/tags/{arn}) and confirms both reach
// AppSync's own handler, not Polly's or CodeArtifact's -- the two AppSync
// endpoints most directly aliased by Polly's blanket "/v1/" claim and
// CodeArtifact's "/v1/domain" and exact "/v1/tags" claims. The remaining
// AppSync paths cmd/routecollisions lists (apis, mergedApis, sourceApis,
// dataplane-evaluatecode/template, dataSource-introspections) fail Polly's
// parseRoute whitelist for the identical reason -- none of them are one of
// its 5 modeled routes -- so they are not separately exercised here.
func TestV1Routing_AppSyncNotShadowedByPollyOrCodeArtifact(t *testing.T) {
	t.Parallel()

	srv := newTestAppSyncV1RegistryServer(t)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	client := appsyncsdk.NewFromConfig(cfg, func(o *appsyncsdk.Options) { o.BaseEndpoint = aws.String(srv.URL) })

	t.Run("domainnames", func(t *testing.T) {
		t.Parallel()

		out, listErr := client.ListDomainNames(t.Context(), &appsyncsdk.ListDomainNamesInput{})
		require.NoError(t, listErr)
		require.NotNil(t, out.DomainNameConfigs)
	})

	t.Run("tags", func(t *testing.T) {
		t.Parallel()

		_, tagErr := client.ListTagsForResource(t.Context(), &appsyncsdk.ListTagsForResourceInput{
			ResourceArn: aws.String("arn:aws:appsync:us-east-1:000000000000:apis/unknown-api-id"),
		})
		require.Error(t, tagErr, "listing tags for an unknown AppSync API must fail")

		var apiErr smithy.APIError
		require.ErrorAs(t, tagErr, &apiErr)
		require.Equal(
			t, "NotFoundException", apiErr.ErrorCode(),
			"must be AppSync's own not-found error, not a v1-prefix competitor swallowing the path",
		)
	})
}
