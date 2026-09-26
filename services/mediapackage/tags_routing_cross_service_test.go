package mediapackage_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	mediapackagesdk "github.com/aws/aws-sdk-go-v2/service/mediapackage"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/accessanalyzer"
	"github.com/blackbirdworks/gopherstack/services/amplify"
	"github.com/blackbirdworks/gopherstack/services/dlm"
	"github.com/blackbirdworks/gopherstack/services/eks"
	"github.com/blackbirdworks/gopherstack/services/mediapackage"
)

// newTestMediaPackageTagsRegistryServer wires up MediaPackage's real
// Handler alongside accessanalyzer, dlm, amplify and eks -- four services
// whose RouteMatcher also claims a "/tags" path at the same MatchPriority
// and register earlier in cli.go's chain. All five real AWS SDK clients
// send GET /tags/{ResourceArn} for ListTagsForResource
// (mediapackage@v1.42.4, accessanalyzer@v1.51.4, dlm@v1.39.4,
// amplify@v1.47.0, eks@v1.98.0 all bind the same URI).
// cmd/routecollisions flags MediaPackage as shadowed because it only sees
// each winner's bare "/tags" HasPrefix/CutPrefix check, missing the
// ARN-service-segment guard each one runs afterward (gopherstack-op3e
// census sweep 2026-09-19).
func newTestMediaPackageTagsRegistryServer(t *testing.T) *httptest.Server {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()

	require.NoError(t, registry.Register(
		accessanalyzer.NewHandler(accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")),
	))
	require.NoError(t, registry.Register(
		dlm.NewHandler(dlm.NewInMemoryBackend("000000000000", "us-east-1")),
	))
	require.NoError(t, registry.Register(
		amplify.NewHandler(amplify.NewInMemoryBackend("000000000000", "us-east-1")),
	))
	require.NoError(t, registry.Register(
		eks.NewHandler(eks.NewInMemoryBackend(context.Background(), "000000000000", "us-east-1")),
	))
	require.NoError(t, registry.Register(
		mediapackage.NewHandler(mediapackage.NewInMemoryBackend("000000000000", "us-east-1")),
	))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	return srv
}

// TestTagsRouting_MediaPackageNotShadowedByTagPathCompetitors proves
// MediaPackage's own ListTagsForResource (GET
// /tags/arn:...:mediapackage:...) still reaches MediaPackage's handler --
// which never requires the resource to pre-exist, always returning 200 with
// whatever tags (possibly none) it holds for that ARN -- when
// accessanalyzer, dlm, amplify and eks are registered ahead of it in the
// same router.
func TestTagsRouting_MediaPackageNotShadowedByTagPathCompetitors(t *testing.T) {
	t.Parallel()

	srv := newTestMediaPackageTagsRegistryServer(t)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	client := mediapackagesdk.NewFromConfig(
		cfg, func(o *mediapackagesdk.Options) { o.BaseEndpoint = aws.String(srv.URL) },
	)

	out, tagErr := client.ListTagsForResource(t.Context(), &mediapackagesdk.ListTagsForResourceInput{
		ResourceArn: aws.String("arn:aws:mediapackage:us-east-1:000000000000:channels/my-channel"),
	})
	require.NoError(
		t, tagErr,
		"MediaPackage's ListTagsForResource never 404s on an unknown ARN -- an error here means "+
			"a tag-path competitor swallowed the request and rejected it on its own terms",
	)
	require.NotNil(t, out.Tags)
	require.Empty(t, out.Tags)
}
