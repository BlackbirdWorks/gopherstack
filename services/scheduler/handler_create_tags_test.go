package scheduler_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	schedulersdk "github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/scheduler"
)

const (
	rtTestRegion    = "us-east-1"
	rtTestAccountID = "000000000000"
)

// newTestHandlerAndClient stands up a fresh in-memory scheduler backend and
// a real aws-sdk-go-v2 client against an httptest server running its
// Handler, wired through the same pkgs/service registry/router used in
// production.
func newTestHandlerAndClient(t *testing.T) *schedulersdk.Client {
	t.Helper()

	backend := scheduler.NewInMemoryBackend(rtTestAccountID, rtTestRegion)
	h := scheduler.NewHandler(backend)

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(rtTestRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return schedulersdk.NewFromConfig(cfg, func(o *schedulersdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateScheduleGroup_TagsRoundTrip drives CreateScheduleGroup, the only
// scheduler Create* op whose real Input struct accepts Tags
// (scheduler@v1.20.4: api_op_CreateScheduleGroup.go, `Tags []types.Tag`;
// CreateSchedule takes no Tags), through the real SDK client and asserts
// ListTagsForResource sees what was supplied at creation (gopherstack-2mwl).
func TestCreateScheduleGroup_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	_, err := client.CreateScheduleGroup(t.Context(), &schedulersdk.CreateScheduleGroupInput{
		Name: aws.String("tagged-group"),
		Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
	})
	require.NoError(t, err)

	groupARN := "arn:aws:scheduler:" + rtTestRegion + ":" + rtTestAccountID + ":schedule-group/tagged-group"

	got, err := client.ListTagsForResource(t.Context(), &schedulersdk.ListTagsForResourceInput{
		ResourceArn: aws.String(groupARN),
	})
	require.NoError(t, err)
	assert.Equal(t, "env", aws.ToString(got.Tags[0].Key))
	assert.Equal(t, "prod", aws.ToString(got.Tags[0].Value))
}
