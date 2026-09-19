package eks_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	ekssdk "github.com/aws/aws-sdk-go-v2/service/eks"
	smithy "github.com/aws/smithy-go"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/amplify"
	"github.com/blackbirdworks/gopherstack/services/eks"
)

// newTestEKSTagsRegistryServer wires up EKS's real Handler alongside
// Amplify, whose RouteMatcher also claims a "/tags/" prefix at the same
// MatchPriority and registers earlier in cli.go's chain. Both real AWS SDK
// clients send GET /tags/{resourceArn} for ListTagsForResource
// (eks@v1.98.0, amplify@v1.47.0 bind the same URI). cmd/routecollisions
// flags EKS as shadowed because it only sees amplify's bare "/tags/"
// HasPrefix check, missing the strings.Contains(arn, ":amplify") guard it
// runs afterward (gopherstack-op3e census sweep 2026-09-19).
func newTestEKSTagsRegistryServer(t *testing.T) *httptest.Server {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()

	require.NoError(t, registry.Register(
		amplify.NewHandler(amplify.NewInMemoryBackend("000000000000", "us-east-1")),
	))
	require.NoError(t, registry.Register(
		eks.NewHandler(eks.NewInMemoryBackend(context.Background(), "000000000000", "us-east-1")),
	))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	return srv
}

// TestTagsRouting_EKSNotShadowedByAmplify proves EKS's own
// ListTagsForResource (GET /tags/arn:...:eks:...) still reaches EKS's
// handler when amplify is registered ahead of it in the same router.
func TestTagsRouting_EKSNotShadowedByAmplify(t *testing.T) {
	t.Parallel()

	srv := newTestEKSTagsRegistryServer(t)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	client := ekssdk.NewFromConfig(cfg, func(o *ekssdk.Options) { o.BaseEndpoint = aws.String(srv.URL) })

	_, tagErr := client.ListTagsForResource(t.Context(), &ekssdk.ListTagsForResourceInput{
		ResourceArn: aws.String("arn:aws:eks:us-east-1:000000000000:cluster/my-cluster"),
	})
	require.Error(t, tagErr, "listing tags for an unknown EKS cluster must fail")

	var apiErr smithy.APIError
	require.ErrorAs(t, tagErr, &apiErr)
	require.Equal(
		t, "NotFoundException", apiErr.ErrorCode(),
		"must be EKS's own not-found error, not amplify swallowing the path",
	)
}
