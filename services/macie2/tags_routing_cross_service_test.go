package macie2_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	macie2sdk "github.com/aws/aws-sdk-go-v2/service/macie2"
	smithy "github.com/aws/smithy-go"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/accessanalyzer"
	"github.com/blackbirdworks/gopherstack/services/macie2"
	"github.com/blackbirdworks/gopherstack/services/vpclattice"
)

// newTestMacie2TagsRegistryServer wires up Macie2's real Handler alongside
// vpclattice and accessanalyzer -- two services whose RouteMatcher also
// claims a "/tags" path at the same MatchPriority and register earlier in
// cli.go's chain. All three real AWS SDK clients send GET
// /tags/{resourceArn} for ListTagsForResource (macie2@v1.54.4,
// vpclattice@v1.25.5, accessanalyzer@v1.51.4 all bind the same URI).
// cmd/routecollisions flags Macie2 as shadowed because it only sees each
// winner's bare "/tags" HasPrefix/CutPrefix check, missing the
// ARN-service-segment guard each one runs afterward (gopherstack-op3e
// census sweep 2026-09-19).
func newTestMacie2TagsRegistryServer(t *testing.T) *httptest.Server {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()

	require.NoError(t, registry.Register(
		vpclattice.NewHandler(vpclattice.NewInMemoryBackend("000000000000", "us-east-1")),
	))
	require.NoError(t, registry.Register(
		accessanalyzer.NewHandler(accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")),
	))
	require.NoError(t, registry.Register(
		macie2.NewHandler(macie2.NewInMemoryBackend("000000000000", "us-east-1")),
	))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	return srv
}

// TestTagsRouting_Macie2NotShadowedByTagPathCompetitors proves Macie2's own
// ListTagsForResource (GET /tags/arn:...:macie2:...) still reaches Macie2's
// handler when vpclattice and accessanalyzer are registered ahead of it in
// the same router.
func TestTagsRouting_Macie2NotShadowedByTagPathCompetitors(t *testing.T) {
	t.Parallel()

	srv := newTestMacie2TagsRegistryServer(t)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	client := macie2sdk.NewFromConfig(cfg, func(o *macie2sdk.Options) { o.BaseEndpoint = aws.String(srv.URL) })

	_, tagErr := client.ListTagsForResource(t.Context(), &macie2sdk.ListTagsForResourceInput{
		ResourceArn: aws.String(
			"arn:aws:macie2:us-east-1:000000000000:classification-job/0123456789abcdef0123456789abcdef",
		),
	})
	require.Error(t, tagErr, "listing tags for an unknown Macie2 resource must fail")

	var apiErr smithy.APIError
	require.ErrorAs(t, tagErr, &apiErr)
	require.Equal(
		t, "ResourceNotFoundException", apiErr.ErrorCode(),
		"must be Macie2's own not-found error, not a tag-path competitor swallowing the path",
	)
}
