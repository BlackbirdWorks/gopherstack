package cloudwatchlogs_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	cwlsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwltypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

const cwlTagsRTRegion = "us-east-1"

// newTestCloudWatchLogsClient stands up the real aws-sdk-go-v2
// cloudwatchlogs client against an httptest server running this package's
// Handler, wired through the same pkgs/service registry/router used in
// production.
func newTestCloudWatchLogsClient(t *testing.T, h *cloudwatchlogs.Handler) *cwlsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(cwlTagsRTRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return cwlsdk.NewFromConfig(cfg, func(o *cwlsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateLogGroup_TagsRoundTrip drives CreateLogGroup, whose real Input
// struct accepts Tags (cloudwatchlogs@v1.81.1 api_op_CreateLogGroup.go:113),
// through the real SDK client and asserts ListTagsForResource -- the
// modern, non-deprecated read path (ListTagsLogGroup is legacy) -- sees
// what was supplied at creation (gopherstack-2mwl).
func TestCreateLogGroup_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	backend := cloudwatchlogs.NewInMemoryBackend()
	client := newTestCloudWatchLogsClient(t, cloudwatchlogs.NewHandler(backend))

	_, err := client.CreateLogGroup(t.Context(), &cwlsdk.CreateLogGroupInput{
		LogGroupName: aws.String("tagged-log-group"),
		Tags:         map[string]string{"env": "test"},
	})
	require.NoError(t, err)

	groupARN := "arn:aws:logs:" + cwlTagsRTRegion + ":000000000000:log-group:tagged-log-group"

	got, err := client.ListTagsForResource(t.Context(), &cwlsdk.ListTagsForResourceInput{
		ResourceArn: aws.String(groupARN),
	})
	require.NoError(t, err)
	require.Len(t, got.Tags, 1)
	assert.Equal(t, "test", got.Tags["env"])
}

// TestCreateLogAnomalyDetector_TagsRoundTrip drives CreateLogAnomalyDetector,
// whose real Input struct accepts Tags (cloudwatchlogs@v1.81.1
// api_op_CreateLogAnomalyDetector.go:105), through the real SDK client and
// asserts ListTagsForResource sees what was supplied at creation
// (gopherstack-2mwl).
func TestCreateLogAnomalyDetector_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	backend := cloudwatchlogs.NewInMemoryBackend()
	client := newTestCloudWatchLogsClient(t, cloudwatchlogs.NewHandler(backend))

	_, err := client.CreateLogGroup(t.Context(), &cwlsdk.CreateLogGroupInput{
		LogGroupName: aws.String("detector-source-group"),
	})
	require.NoError(t, err)

	groupARN := "arn:aws:logs:" + cwlTagsRTRegion + ":000000000000:log-group:detector-source-group"

	out, err := client.CreateLogAnomalyDetector(t.Context(), &cwlsdk.CreateLogAnomalyDetectorInput{
		LogGroupArnList: []string{groupARN},
		Tags:            map[string]string{"env": "test"},
	})
	require.NoError(t, err)

	got, err := client.ListTagsForResource(t.Context(), &cwlsdk.ListTagsForResourceInput{
		ResourceArn: out.AnomalyDetectorArn,
	})
	require.NoError(t, err)
	require.Len(t, got.Tags, 1)
	assert.Equal(t, "test", got.Tags["env"])
}

// TestCreateLookupTable_TagsRoundTrip drives CreateLookupTable, whose real
// Input struct accepts Tags, through the real SDK client and asserts
// ListTagsForResource sees what was supplied at creation (gopherstack-2mwl).
func TestCreateLookupTable_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	backend := cloudwatchlogs.NewInMemoryBackend()
	client := newTestCloudWatchLogsClient(t, cloudwatchlogs.NewHandler(backend))

	out, err := client.CreateLookupTable(t.Context(), &cwlsdk.CreateLookupTableInput{
		LookupTableName: aws.String("tagged_lookup"),
		TableBody:       aws.String("key,value\nfoo,bar\n"),
		Tags:            map[string]string{"env": "test"},
	})
	require.NoError(t, err)

	got, err := client.ListTagsForResource(t.Context(), &cwlsdk.ListTagsForResourceInput{
		ResourceArn: out.LookupTableArn,
	})
	require.NoError(t, err)
	require.Len(t, got.Tags, 1)
	assert.Equal(t, "test", got.Tags["env"])
}

// TestCreateScheduledQuery_TagsRoundTrip drives CreateScheduledQuery, whose
// real Input struct accepts Tags (cloudwatchlogs@v1.81.1
// api_op_CreateScheduledQuery.go:102), through the real SDK client and
// asserts ListTagsForResource sees what was supplied at creation
// (gopherstack-2mwl).
func TestCreateScheduledQuery_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	backend := cloudwatchlogs.NewInMemoryBackend()
	client := newTestCloudWatchLogsClient(t, cloudwatchlogs.NewHandler(backend))

	out, err := client.CreateScheduledQuery(t.Context(), &cwlsdk.CreateScheduledQueryInput{
		Name:               aws.String("tagged-query"),
		QueryString:        aws.String("fields @timestamp"),
		QueryLanguage:      cwltypes.QueryLanguageCwli,
		ScheduleExpression: aws.String("rate(1 hour)"),
		ExecutionRoleArn:   aws.String("arn:aws:iam::000000000000:role/access"),
		Tags:               map[string]string{"env": "test"},
	})
	require.NoError(t, err)

	got, err := client.ListTagsForResource(t.Context(), &cwlsdk.ListTagsForResourceInput{
		ResourceArn: out.ScheduledQueryArn,
	})
	require.NoError(t, err)
	require.Len(t, got.Tags, 1)
	assert.Equal(t, "test", got.Tags["env"])
}
