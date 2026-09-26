package detective_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	detectivesdk "github.com/aws/aws-sdk-go-v2/service/detective"
	smithy "github.com/aws/smithy-go"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/accessanalyzer"
	"github.com/blackbirdworks/gopherstack/services/amplify"
	"github.com/blackbirdworks/gopherstack/services/detective"
	"github.com/blackbirdworks/gopherstack/services/eks"
)

// newTestDetectiveTagsRegistryServer wires up Detective's real Handler
// alongside amplify, eks and accessanalyzer -- three services whose
// RouteMatcher also claims a "/tags/" prefix at the same MatchPriority and
// register earlier in cli.go's chain. All four real AWS SDK clients send
// GET /tags/{resourceArn} for ListTagsForResource (detective@v1.41.4,
// amplify@v1.47.0, eks@v1.98.0, accessanalyzer@v1.51.4 all bind the same
// URI). cmd/routecollisions flags Detective as shadowed because it only
// sees each winner's bare "/tags/" HasPrefix check, missing the
// ARN-service-segment guard each one runs afterward (gopherstack-op3e
// census sweep 2026-09-19).
func newTestDetectiveTagsRegistryServer(t *testing.T) *httptest.Server {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()

	require.NoError(t, registry.Register(
		amplify.NewHandler(amplify.NewInMemoryBackend("000000000000", "us-east-1")),
	))
	require.NoError(t, registry.Register(
		eks.NewHandler(eks.NewInMemoryBackend(context.Background(), "000000000000", "us-east-1")),
	))
	require.NoError(t, registry.Register(
		accessanalyzer.NewHandler(accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")),
	))
	require.NoError(t, registry.Register(
		detective.NewHandler(detective.NewInMemoryBackend("000000000000", "us-east-1")),
	))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	return srv
}

// TestTagsRouting_DetectiveNotShadowedByTagPrefixCompetitors proves
// Detective's own ListTagsForResource (GET /tags/arn:...:detective:...)
// still reaches Detective's handler when amplify, eks and accessanalyzer
// are registered ahead of it in the same router.
func TestTagsRouting_DetectiveNotShadowedByTagPrefixCompetitors(t *testing.T) {
	t.Parallel()

	srv := newTestDetectiveTagsRegistryServer(t)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	client := detectivesdk.NewFromConfig(cfg, func(o *detectivesdk.Options) { o.BaseEndpoint = aws.String(srv.URL) })

	_, tagErr := client.ListTagsForResource(t.Context(), &detectivesdk.ListTagsForResourceInput{
		ResourceArn: aws.String("arn:aws:detective:us-east-1:000000000000:graph:0123456789abcdef0123456789abcdef"),
	})
	require.Error(t, tagErr, "listing tags for an unknown Detective graph must fail")

	var apiErr smithy.APIError
	require.ErrorAs(t, tagErr, &apiErr)
	require.Equal(
		t, "ResourceNotFoundException", apiErr.ErrorCode(),
		"must be Detective's own not-found error, not a tag-prefix competitor swallowing the path",
	)
}
