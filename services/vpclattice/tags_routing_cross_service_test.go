package vpclattice_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	vpclatticesdk "github.com/aws/aws-sdk-go-v2/service/vpclattice"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/accessanalyzer"
	"github.com/blackbirdworks/gopherstack/services/vpclattice"
)

// newTestVPCLatticeTagsRegistryServer wires up VPC Lattice's real Handler
// alongside accessanalyzer, whose RouteMatcher also claims a "/tags" path
// at the same MatchPriority and registers earlier in cli.go's chain. Both
// real AWS SDK clients send GET /tags/{resourceArn} for
// ListTagsForResource (vpclattice@v1.25.5, accessanalyzer@v1.51.4 bind the
// same URI). cmd/routecollisions flags VPC Lattice as shadowed because it
// only sees accessanalyzer's bare "/tags" CutPrefix check, missing the
// ARN-service-segment guard it runs afterward (gopherstack-op3e census
// sweep 2026-09-19).
func newTestVPCLatticeTagsRegistryServer(t *testing.T) *httptest.Server {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()

	require.NoError(t, registry.Register(
		accessanalyzer.NewHandler(accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")),
	))
	require.NoError(t, registry.Register(
		vpclattice.NewHandler(vpclattice.NewInMemoryBackend("000000000000", "us-east-1")),
	))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	return srv
}

// TestTagsRouting_VPCLatticeNotShadowedByAccessAnalyzer proves VPC
// Lattice's own ListTagsForResource (GET /tags/arn:...:vpc-lattice:...)
// still reaches VPC Lattice's handler -- which never requires the resource
// to pre-exist -- when accessanalyzer is registered ahead of it in the
// same router.
func TestTagsRouting_VPCLatticeNotShadowedByAccessAnalyzer(t *testing.T) {
	t.Parallel()

	srv := newTestVPCLatticeTagsRegistryServer(t)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	client := vpclatticesdk.NewFromConfig(
		cfg, func(o *vpclatticesdk.Options) { o.BaseEndpoint = aws.String(srv.URL) },
	)

	out, tagErr := client.ListTagsForResource(t.Context(), &vpclatticesdk.ListTagsForResourceInput{
		ResourceArn: aws.String("arn:aws:vpc-lattice:us-east-1:000000000000:service/svc-0123456789abcdef0"),
	})
	require.NoError(
		t, tagErr,
		"VPC Lattice's ListTagsForResource never 404s on an unknown ARN -- an error here means "+
			"accessanalyzer swallowed the request and rejected it on its own terms",
	)
	require.NotNil(t, out.Tags)
	require.Empty(t, out.Tags)
}
