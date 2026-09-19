package mediatailor_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	mediatailorsdk "github.com/aws/aws-sdk-go-v2/service/mediatailor"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/accessanalyzer"
	"github.com/blackbirdworks/gopherstack/services/amplify"
	"github.com/blackbirdworks/gopherstack/services/eks"
	"github.com/blackbirdworks/gopherstack/services/mediatailor"
)

// newTestMediaTailorTagsRegistryServer wires up MediaTailor's real Handler
// alongside eks, amplify and accessanalyzer -- three services whose
// RouteMatcher also claims a "/tags/" prefix at the same MatchPriority and
// register earlier in cli.go's chain. All four real AWS SDK clients send
// GET /tags/{ResourceArn} for ListTagsForResource (mediatailor@v1.63.4,
// eks@v1.98.0, amplify@v1.47.0, accessanalyzer@v1.51.4 all bind the same
// URI). cmd/routecollisions flags MediaTailor as shadowed because it only
// sees each winner's bare "/tags/" HasPrefix check, missing the
// ARN-service-segment guard each one runs afterward (gopherstack-op3e
// census sweep 2026-09-19).
func newTestMediaTailorTagsRegistryServer(t *testing.T) *httptest.Server {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()

	require.NoError(t, registry.Register(
		eks.NewHandler(eks.NewInMemoryBackend(context.Background(), "000000000000", "us-east-1")),
	))
	require.NoError(t, registry.Register(
		amplify.NewHandler(amplify.NewInMemoryBackend("000000000000", "us-east-1")),
	))
	require.NoError(t, registry.Register(
		accessanalyzer.NewHandler(accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")),
	))
	require.NoError(t, registry.Register(
		mediatailor.NewHandler(mediatailor.NewInMemoryBackend("000000000000", "us-east-1")),
	))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	return srv
}

// TestTagsRouting_MediaTailorNotShadowedByTagPrefixCompetitors proves
// MediaTailor's own ListTagsForResource (GET /tags/arn:...:mediatailor:...)
// still reaches MediaTailor's handler -- which never requires the resource
// to pre-exist -- when eks, amplify and accessanalyzer are registered
// ahead of it in the same router. The request is SigV4-signed for
// mediatailor, matching MediaTailor's own guard of
// svc == "" || svc == "mediatailor" for every non-/channels path.
func TestTagsRouting_MediaTailorNotShadowedByTagPrefixCompetitors(t *testing.T) {
	t.Parallel()

	srv := newTestMediaTailorTagsRegistryServer(t)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	client := mediatailorsdk.NewFromConfig(
		cfg, func(o *mediatailorsdk.Options) { o.BaseEndpoint = aws.String(srv.URL) },
	)

	out, tagErr := client.ListTagsForResource(t.Context(), &mediatailorsdk.ListTagsForResourceInput{
		ResourceArn: aws.String("arn:aws:mediatailor:us-east-1:000000000000:channel/my-channel"),
	})
	require.NoError(
		t, tagErr,
		"MediaTailor's ListTagsForResource never 404s on an unknown ARN -- an error here means "+
			"a tag-prefix competitor swallowed the request and rejected it on its own terms",
	)
	require.NotNil(t, out.Tags)
	require.Empty(t, out.Tags)
}
