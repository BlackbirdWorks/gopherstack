package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	iotanalyticssdk "github.com/aws/aws-sdk-go-v2/service/iotanalytics"
	iotanalyticstype "github.com/aws/aws-sdk-go-v2/service/iotanalytics/types"
)

// createIoTAnalyticsClient returns an IoT Analytics client pointed at the shared test stack.
func createIoTAnalyticsClient(t *testing.T) *iotanalyticssdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return iotanalyticssdk.NewFromConfig(cfg, func(o *iotanalyticssdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_IoTAnalytics_ChannelLifecycle tests the full channel CRUD lifecycle via the SDK.
func TestIntegration_IoTAnalytics_ChannelLifecycle(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	client := createIoTAnalyticsClient(t)
	channelName := "integration-channel-" + t.Name()

	createOut, err := client.CreateChannel(
		ctx, &iotanalyticssdk.CreateChannelInput{ChannelName: aws.String(channelName)},
	)
	require.NoError(t, err, "CreateChannel should succeed")
	assert.Equal(t, channelName, aws.ToString(createOut.ChannelName))
	assert.NotEmpty(t, createOut.ChannelArn)

	listOut, err := client.ListChannels(
		ctx, &iotanalyticssdk.ListChannelsInput{},
	)
	require.NoError(t, err, "ListChannels should succeed")

	found := false

	for _, ch := range listOut.ChannelSummaries {
		if aws.ToString(ch.ChannelName) == channelName { //nolint:staticcheck // deprecated field
			found = true

			break
		}
	}

	assert.True(t, found, "created channel should appear in list")

	descOut, err := client.DescribeChannel(
		ctx, &iotanalyticssdk.DescribeChannelInput{ChannelName: aws.String(channelName)},
	)
	require.NoError(t, err, "DescribeChannel should succeed")
	ch := descOut.Channel
	assert.Equal(t, channelName, aws.ToString(ch.Name)) //nolint:staticcheck // deprecated field

	_, err = client.UpdateChannel(
		ctx, &iotanalyticssdk.UpdateChannelInput{ChannelName: aws.String(channelName)},
	)
	require.NoError(t, err, "UpdateChannel should succeed")

	_, err = client.DeleteChannel(
		ctx, &iotanalyticssdk.DeleteChannelInput{ChannelName: aws.String(channelName)},
	)
	require.NoError(t, err, "DeleteChannel should succeed")

	_, err = client.DescribeChannel(
		ctx, &iotanalyticssdk.DescribeChannelInput{ChannelName: aws.String(channelName)},
	)
	require.Error(t, err, "DescribeChannel after delete should return error")
}

// TestIntegration_IoTAnalytics_DatastoreLifecycle tests the full datastore CRUD lifecycle via the SDK.
func TestIntegration_IoTAnalytics_DatastoreLifecycle(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	client := createIoTAnalyticsClient(t)
	datastoreName := "integration-datastore-" + t.Name()

	createOut, err := client.CreateDatastore(
		ctx, &iotanalyticssdk.CreateDatastoreInput{DatastoreName: aws.String(datastoreName)},
	)
	require.NoError(t, err, "CreateDatastore should succeed")
	assert.Equal(t, datastoreName, aws.ToString(createOut.DatastoreName))

	listOut, err := client.ListDatastores(
		ctx, &iotanalyticssdk.ListDatastoresInput{},
	)
	require.NoError(t, err, "ListDatastores should succeed")

	found := false

	for _, ds := range listOut.DatastoreSummaries {
		if aws.ToString(ds.DatastoreName) == datastoreName { //nolint:staticcheck // deprecated field
			found = true

			break
		}
	}

	assert.True(t, found, "created datastore should appear in list")

	_, err = client.DeleteDatastore(
		ctx, &iotanalyticssdk.DeleteDatastoreInput{DatastoreName: aws.String(datastoreName)},
	)
	require.NoError(t, err, "DeleteDatastore should succeed")
}

// TestIntegration_IoTAnalytics_PipelineLifecycle tests the full pipeline CRUD lifecycle via the SDK.
func TestIntegration_IoTAnalytics_PipelineLifecycle(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	client := createIoTAnalyticsClient(t)
	pipelineName := "integration-pipeline-" + t.Name()

	createOut, err := client.CreatePipeline(
		ctx, &iotanalyticssdk.CreatePipelineInput{
			PipelineName: aws.String(pipelineName),
			PipelineActivities: []iotanalyticstype.PipelineActivity{
				{
					Channel: &iotanalyticstype.ChannelActivity{
						Name:        aws.String("channel-activity"),
						ChannelName: aws.String("test-channel"),
					},
				},
			},
		},
	)
	require.NoError(t, err, "CreatePipeline should succeed")
	assert.Equal(t, pipelineName, aws.ToString(createOut.PipelineName))

	listOut, err := client.ListPipelines(
		ctx, &iotanalyticssdk.ListPipelinesInput{},
	)
	require.NoError(t, err, "ListPipelines should succeed")

	found := false

	for _, p := range listOut.PipelineSummaries {
		if aws.ToString(p.PipelineName) == pipelineName { //nolint:staticcheck // deprecated field
			found = true

			break
		}
	}

	assert.True(t, found, "created pipeline should appear in list")

	descOut, err := client.DescribePipeline(
		ctx, &iotanalyticssdk.DescribePipelineInput{PipelineName: aws.String(pipelineName)},
	)
	require.NoError(t, err, "DescribePipeline should succeed")
	pipeline := descOut.Pipeline
	assert.Equal(t, pipelineName, aws.ToString(pipeline.Name)) //nolint:staticcheck // deprecated field

	_, err = client.DeletePipeline(
		ctx, &iotanalyticssdk.DeletePipelineInput{PipelineName: aws.String(pipelineName)},
	)
	require.NoError(t, err, "DeletePipeline should succeed")
}
