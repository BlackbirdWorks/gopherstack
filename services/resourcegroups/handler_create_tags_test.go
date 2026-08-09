package resourcegroups_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	resourcegroupssdk "github.com/aws/aws-sdk-go-v2/service/resourcegroups"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/resourcegroups"
)

const tagsRTRegion = "us-east-1"

// newTestResourceGroupsClient stands up the real aws-sdk-go-v2
// resourcegroups client against an httptest server running this package's
// Handler, wired through the same pkgs/service registry/router used in
// production.
func newTestResourceGroupsClient(t *testing.T, h *resourcegroups.Handler) *resourcegroupssdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(tagsRTRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return resourcegroupssdk.NewFromConfig(cfg, func(o *resourcegroupssdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateGroup_TagsRoundTrip drives CreateGroup, the only real
// resourcegroups op whose Input struct accepts Tags (resourcegroups@v1.36.4:
// api_op_CreateGroup.go), through the real SDK client and asserts GetTags
// (resourcegroups' ListTagsForResource equivalent) sees what was supplied at
// creation (gopherstack-2mwl).
func TestCreateGroup_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	backend := resourcegroups.NewInMemoryBackend("000000000000", tagsRTRegion)
	client := newTestResourceGroupsClient(t, resourcegroups.NewHandler(backend))

	out, err := client.CreateGroup(t.Context(), &resourcegroupssdk.CreateGroupInput{
		Name: aws.String("tagged-group"),
		Tags: map[string]string{"env": "test"},
	})
	require.NoError(t, err)

	resourceArn := aws.ToString(out.Group.GroupArn)
	require.NotEmpty(t, resourceArn)

	got, err := client.GetTags(t.Context(), &resourcegroupssdk.GetTagsInput{
		Arn: aws.String(resourceArn),
	})
	require.NoError(t, err)
	require.Len(t, got.Tags, 1)
	assert.Equal(t, "test", got.Tags["env"])
}
