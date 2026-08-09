package cloudtrail_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	cloudtrailsdk "github.com/aws/aws-sdk-go-v2/service/cloudtrail"
	cttypes "github.com/aws/aws-sdk-go-v2/service/cloudtrail/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/cloudtrail"
)

const ctTagsRTRegion = "us-east-1"

// newTestCloudTrailClient stands up the real aws-sdk-go-v2 cloudtrail client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production.
func newTestCloudTrailClient(t *testing.T, h *cloudtrail.Handler) *cloudtrailsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(ctTagsRTRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return cloudtrailsdk.NewFromConfig(cfg, func(o *cloudtrailsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateChannel_TagsRoundTrip drives CreateChannel, whose real Input
// struct accepts Tags (cloudtrail@v1.58.4 api_op_CreateChannel.go:56),
// through the real SDK client and asserts ListTags sees what was supplied
// at creation (gopherstack-2mwl).
func TestCreateChannel_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	backend := cloudtrail.NewInMemoryBackend("000000000000", ctTagsRTRegion)
	client := newTestCloudTrailClient(t, cloudtrail.NewHandler(backend))

	out, err := client.CreateChannel(t.Context(), &cloudtrailsdk.CreateChannelInput{
		Name:   aws.String("tagged-channel"),
		Source: aws.String("Custom"),
		Destinations: []cttypes.Destination{
			{
				Type: cttypes.DestinationTypeEventDataStore,
				Location: aws.String(
					"arn:aws:cloudtrail:us-east-1:000000000000:eventdatastore/eds-1",
				),
			},
		},
		Tags: []cttypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
	})
	require.NoError(t, err)

	got, err := client.ListTags(t.Context(), &cloudtrailsdk.ListTagsInput{
		ResourceIdList: []string{aws.ToString(out.ChannelArn)},
	})
	require.NoError(t, err)
	require.Len(t, got.ResourceTagList, 1)
	require.Len(t, got.ResourceTagList[0].TagsList, 1)
	assert.Equal(t, "env", aws.ToString(got.ResourceTagList[0].TagsList[0].Key))
	assert.Equal(t, "test", aws.ToString(got.ResourceTagList[0].TagsList[0].Value))
}
