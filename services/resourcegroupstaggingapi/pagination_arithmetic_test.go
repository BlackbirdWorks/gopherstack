package resourcegroupstaggingapi_test

import (
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	rgtasdk "github.com/aws/aws-sdk-go-v2/service/resourcegroupstaggingapi"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
)

// newTestRGTAClient stands up the real aws-sdk-go-v2
// resourcegroupstaggingapi client against an httptest server running this
// package's Handler, wired through the same pkgs/service registry/router
// used in production. Existing pagination tests in this package (see
// TestGetResources_PaginationWalk / TestGetResources_UnmatchedTokenExpired)
// call the backend directly; this closes the gap by confirming the same
// behavior survives the real SDK serializer/deserializer round trip.
func newTestRGTAClient(t *testing.T, b *resourcegroupstaggingapi.InMemoryBackend) *rgtasdk.Client {
	t.Helper()

	h := resourcegroupstaggingapi.NewHandler(b)

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(testRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return rgtasdk.NewFromConfig(cfg, func(o *rgtasdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestGetResources_RealClient_BoundaryWalk confirms, through the real
// aws-sdk-go-v2 client, that paginateResources/findTokenStart (the
// "found flag or error" safe-by-construction pattern: a stale token returns
// PaginationTokenExpiredException rather than silently restarting at 0)
// walks a full GetResources collection without dropping or duplicating
// entries, and that a stale token errors instead of looping.
func TestGetResources_RealClient_BoundaryWalk(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	const n = 7

	resources := make([]resourcegroupstaggingapi.TaggedResource, n)
	arns := make([]string, n)

	for i := range n {
		arn := fmt.Sprintf("arn:aws:sqs:us-east-1:000000000000:q-%03d", i)
		arns[i] = arn
		resources[i] = resourcegroupstaggingapi.TaggedResource{
			ResourceARN:  arn,
			ResourceType: "sqs:queue",
			Tags:         map[string]string{"k": "v"},
		}
	}

	seedResources(b, resources)

	client := newTestRGTAClient(t, b)

	var got []string

	var token *string
	for range n + 1 {
		out, err := client.GetResources(t.Context(), &rgtasdk.GetResourcesInput{
			ResourcesPerPage: aws.Int32(3),
			PaginationToken:  token,
		})
		require.NoError(t, err)

		for _, r := range out.ResourceTagMappingList {
			got = append(got, aws.ToString(r.ResourceARN))
		}

		token = out.PaginationToken
		if aws.ToString(token) == "" {
			break
		}
	}

	assert.Equal(t, arns, got, "boundary walk must reproduce the collection exactly, in order")

	// Stale cursor: a token naming an ARN not in the current result set must
	// error (PaginationTokenExpiredException), not silently restart at 0.
	_, err := client.GetResources(t.Context(), &rgtasdk.GetResourcesInput{
		PaginationToken: aws.String("arn:aws:sqs:us-east-1:000000000000:does-not-exist"),
	})
	require.Error(t, err)
}
