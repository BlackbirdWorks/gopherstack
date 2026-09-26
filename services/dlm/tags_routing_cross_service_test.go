package dlm_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	accessanalyzersdk "github.com/aws/aws-sdk-go-v2/service/accessanalyzer"
	amplifysdk "github.com/aws/aws-sdk-go-v2/service/amplify"
	dlmsdk "github.com/aws/aws-sdk-go-v2/service/dlm"
	ekssdk "github.com/aws/aws-sdk-go-v2/service/eks"
	smithy "github.com/aws/smithy-go"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/accessanalyzer"
	"github.com/blackbirdworks/gopherstack/services/amplify"
	"github.com/blackbirdworks/gopherstack/services/dlm"
	"github.com/blackbirdworks/gopherstack/services/eks"
)

// newTestDLMTagsRegistryServer wires up DLM's real Handler alongside EKS,
// AccessAnalyzer and Amplify -- three other services whose RouteMatcher also
// claims a "/tags/" prefix at the same MatchPriority and register earlier in
// cli.go's chain. All four real AWS SDK clients send GET /tags/{resourceArn}
// for ListTagsForResource (dlm@v1.39.4, eks@v1.98.0, accessanalyzer@v1.51.4
// serializers.go; amplify@v1.47.0 schemas.go all bind the same URI).
// cmd/routecollisions flags DLM as shadowed because it only sees each
// winner's bare "/tags/" HasPrefix check, missing the ARN-service-segment
// guard each one runs afterward (gopherstack-op3e census sweep 2026-09-19).
func newTestDLMTagsRegistryServer(t *testing.T) *httptest.Server {
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
		dlm.NewHandler(dlm.NewInMemoryBackend("000000000000", "us-east-1")),
	))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	return srv
}

func newTagsRoutingDLMClient(t *testing.T, baseURL string) *dlmsdk.Client {
	t.Helper()

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	return dlmsdk.NewFromConfig(cfg, func(o *dlmsdk.Options) {
		o.BaseEndpoint = aws.String(baseURL)
	})
}

// TestTagsRouting_DLMNotShadowedByTagPrefixCompetitors proves DLM's own
// ListTagsForResource (GET /tags/arn:...:dlm:...) still reaches DLM's
// handler -- not amplify's, eks's, or accessanalyzer's -- when all four are
// registered in the same router ahead of it by priority/registration order.
func TestTagsRouting_DLMNotShadowedByTagPrefixCompetitors(t *testing.T) {
	t.Parallel()

	srv := newTestDLMTagsRegistryServer(t)
	client := newTagsRoutingDLMClient(t, srv.URL)

	_, err := client.ListTagsForResource(t.Context(), &dlmsdk.ListTagsForResourceInput{
		ResourceArn: aws.String("arn:aws:dlm:us-east-1:000000000000:policy/policy-0123456789abcdef0"),
	})
	require.Error(t, err, "listing tags for an unknown DLM policy must fail")

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	require.Equal(
		t, "ResourceNotFoundException", apiErr.ErrorCode(),
		"must be DLM's own not-found error, not a tag-prefix competitor swallowing the path",
	)
}

// TestTagsRouting_TagPrefixCompetitorsStillOwnTheirOwnARNs is a regression
// guard: registering DLM must not break amplify/eks/accessanalyzer's own
// ListTagsForResource traffic on their own ARNs in the same router.
func TestTagsRouting_TagPrefixCompetitorsStillOwnTheirOwnARNs(t *testing.T) {
	t.Parallel()

	srv := newTestDLMTagsRegistryServer(t)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	// amplify and eks require the ARN's resource to already exist, so an
	// unknown ARN reaching the RIGHT handler still surfaces as that
	// service's own not-found error (not a routing 404 and not DLM's).
	t.Run("amplify", func(t *testing.T) {
		t.Parallel()

		client := amplifysdk.NewFromConfig(cfg, func(o *amplifysdk.Options) { o.BaseEndpoint = aws.String(srv.URL) })
		_, tagErr := client.ListTagsForResource(t.Context(), &amplifysdk.ListTagsForResourceInput{
			ResourceArn: aws.String("arn:aws:amplify:us-east-1:000000000000:apps/d1234567"),
		})
		require.Error(t, tagErr)

		var apiErr smithy.APIError
		require.ErrorAs(t, tagErr, &apiErr)
		require.Equal(t, "ResourceNotFoundException", apiErr.ErrorCode())
	})

	t.Run("accessanalyzer", func(t *testing.T) {
		t.Parallel()

		client := accessanalyzersdk.NewFromConfig(
			cfg, func(o *accessanalyzersdk.Options) { o.BaseEndpoint = aws.String(srv.URL) },
		)
		out, tagErr := client.ListTagsForResource(t.Context(), &accessanalyzersdk.ListTagsForResourceInput{
			ResourceArn: aws.String("arn:aws:access-analyzer:us-east-1:000000000000:analyzer/my-analyzer"),
		})
		require.NoError(t, tagErr)
		require.NotNil(t, out.Tags)
	})

	t.Run("eks", func(t *testing.T) {
		t.Parallel()

		client := ekssdk.NewFromConfig(cfg, func(o *ekssdk.Options) { o.BaseEndpoint = aws.String(srv.URL) })
		_, tagErr := client.ListTagsForResource(t.Context(), &ekssdk.ListTagsForResourceInput{
			ResourceArn: aws.String("arn:aws:eks:us-east-1:000000000000:cluster/my-cluster"),
		})
		require.Error(t, tagErr)

		var apiErr smithy.APIError
		require.ErrorAs(t, tagErr, &apiErr)
		require.Equal(t, "NotFoundException", apiErr.ErrorCode())
	})
}
