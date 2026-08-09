package route53_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	route53sdk "github.com/aws/aws-sdk-go-v2/service/route53"
	"github.com/aws/aws-sdk-go-v2/service/route53/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/route53"
)

// newTestRoute53Client stands up the real aws-sdk-go-v2 Route53 client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production.
func newTestRoute53Client(t *testing.T, h *route53.Handler) *route53sdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return route53sdk.NewFromConfig(cfg, func(o *route53sdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestChangeTagsForResource_RoundTrip verifies ChangeTagsForResource ->
// ListTagsForResource / ListTagsForResources through the real SDK client for
// both taggable resource kinds (gopherstack-2mwl). Unlike most services swept
// in this issue, neither real CreateHostedZoneInput nor CreateHealthCheckInput
// has a Tags field (route53@v1.65.6: api_op_CreateHostedZone.go,
// api_op_CreateHealthCheck.go both verified with no Tags member) -- Route53
// tags exist only via the separate ChangeTagsForResource call, so there is no
// creation-time decode-drop to find here. Asserted through a real SDK client
// because Route53 is REST-XML: ListTagsForResource's response wraps
// ResourceTagSet inside ListTagsForResourceResponse (verified against
// awsRestxml_deserializeOpDocumentListTagsForResourceOutput, deserializers.go:8912,
// which reads ResourceTagSet as a child of the document root -- gopherstack's
// existing shape already matches).
func TestChangeTagsForResource_RoundTrip(t *testing.T) {
	t.Parallel()

	t.Run("hostedzone", func(t *testing.T) {
		t.Parallel()

		h := route53.NewHandler(route53.NewInMemoryBackend())
		client := newTestRoute53Client(t, h)

		zone, err := client.CreateHostedZone(t.Context(), &route53sdk.CreateHostedZoneInput{
			Name:            aws.String("example.com."),
			CallerReference: aws.String("caller-ref-1"),
		})
		require.NoError(t, err)

		// Real HostedZone.Id comes back as "/hostedzone/{id}"; ResourceId for
		// the tag-family ops takes the bare id (real AWS convention).
		zoneID := strings.TrimPrefix(aws.ToString(zone.HostedZone.Id), "/hostedzone/")

		_, err = client.ChangeTagsForResource(t.Context(), &route53sdk.ChangeTagsForResourceInput{
			ResourceType: types.TagResourceTypeHostedzone,
			ResourceId:   aws.String(zoneID),
			AddTags:      []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
		})
		require.NoError(t, err)

		out, err := client.ListTagsForResource(t.Context(), &route53sdk.ListTagsForResourceInput{
			ResourceType: types.TagResourceTypeHostedzone,
			ResourceId:   aws.String(zoneID),
		})
		require.NoError(t, err)
		require.Len(t, out.ResourceTagSet.Tags, 1)
		assert.Equal(t, "env", aws.ToString(out.ResourceTagSet.Tags[0].Key))
		assert.Equal(t, "prod", aws.ToString(out.ResourceTagSet.Tags[0].Value))

		batchOut, err := client.ListTagsForResources(t.Context(), &route53sdk.ListTagsForResourcesInput{
			ResourceType: types.TagResourceTypeHostedzone,
			ResourceIds:  []string{zoneID},
		})
		require.NoError(t, err)
		require.Len(t, batchOut.ResourceTagSets, 1)
		require.Len(t, batchOut.ResourceTagSets[0].Tags, 1)
		assert.Equal(t, "env", aws.ToString(batchOut.ResourceTagSets[0].Tags[0].Key))
	})

	t.Run("healthcheck", func(t *testing.T) {
		t.Parallel()

		h := route53.NewHandler(route53.NewInMemoryBackend())
		client := newTestRoute53Client(t, h)

		hc, err := client.CreateHealthCheck(t.Context(), &route53sdk.CreateHealthCheckInput{
			CallerReference: aws.String("caller-ref-hc-1"),
			HealthCheckConfig: &types.HealthCheckConfig{
				Type:                     types.HealthCheckTypeHttp,
				FullyQualifiedDomainName: aws.String("example.com"),
			},
		})
		require.NoError(t, err)

		hcID := aws.ToString(hc.HealthCheck.Id)

		_, err = client.ChangeTagsForResource(t.Context(), &route53sdk.ChangeTagsForResourceInput{
			ResourceType: types.TagResourceTypeHealthcheck,
			ResourceId:   aws.String(hcID),
			AddTags:      []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
		})
		require.NoError(t, err)

		out, err := client.ListTagsForResource(t.Context(), &route53sdk.ListTagsForResourceInput{
			ResourceType: types.TagResourceTypeHealthcheck,
			ResourceId:   aws.String(hcID),
		})
		require.NoError(t, err)
		require.Len(t, out.ResourceTagSet.Tags, 1)
		assert.Equal(t, "env", aws.ToString(out.ResourceTagSet.Tags[0].Key))
		assert.Equal(t, "prod", aws.ToString(out.ResourceTagSet.Tags[0].Value))
	})
}
