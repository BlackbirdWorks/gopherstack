package forecast_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	forecastsdk "github.com/aws/aws-sdk-go-v2/service/forecast"
	"github.com/aws/aws-sdk-go-v2/service/forecast/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/forecast"
)

// TestSlice17_Forecast_RealClient covers forecast's last four typed-client-
// uncovered ops (gopherstack-n3zi slice 17): TagResource, UntagResource,
// ResumeResource, DeleteResourceTree.
func TestSlice17_Forecast_RealClient(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	backend := forecast.NewInMemoryBackend("000000000000", tagsRTRegion)
	h := forecast.NewHandler(backend)
	client := newTestForecastClient(t, h)

	created, err := client.CreateDatasetGroup(ctx, &forecastsdk.CreateDatasetGroupInput{
		DatasetGroupName: aws.String("slice17_dataset_group"),
		Domain:           types.DomainRetail,
	})
	require.NoError(t, err)
	resourceARN := aws.ToString(created.DatasetGroupArn)
	require.NotEmpty(t, resourceARN)

	_, err = client.TagResource(ctx, &forecastsdk.TagResourceInput{
		ResourceArn: aws.String(resourceARN),
		Tags:        []types.Tag{{Key: aws.String("owner"), Value: aws.String("slice17")}},
	})
	require.NoError(t, err)

	tagged, err := client.ListTagsForResource(ctx, &forecastsdk.ListTagsForResourceInput{
		ResourceArn: aws.String(resourceARN),
	})
	require.NoError(t, err)
	require.Len(t, tagged.Tags, 1)
	assert.Equal(t, "owner", aws.ToString(tagged.Tags[0].Key))
	assert.Equal(t, "slice17", aws.ToString(tagged.Tags[0].Value))

	_, err = client.UntagResource(ctx, &forecastsdk.UntagResourceInput{
		ResourceArn: aws.String(resourceARN),
		TagKeys:     []string{"owner"},
	})
	require.NoError(t, err)

	untagged, err := client.ListTagsForResource(ctx, &forecastsdk.ListTagsForResourceInput{
		ResourceArn: aws.String(resourceARN),
	})
	require.NoError(t, err)
	assert.Empty(t, untagged.Tags)

	_, err = client.StopResource(ctx, &forecastsdk.StopResourceInput{
		ResourceArn: aws.String(resourceARN),
	})
	require.NoError(t, err)

	stopped, err := client.DescribeDatasetGroup(ctx, &forecastsdk.DescribeDatasetGroupInput{
		DatasetGroupArn: aws.String(resourceARN),
	})
	require.NoError(t, err)
	assert.Equal(t, "STOPPED", aws.ToString(stopped.Status))

	_, err = client.ResumeResource(ctx, &forecastsdk.ResumeResourceInput{
		ResourceArn: aws.String(resourceARN),
	})
	require.NoError(t, err)

	resumed, err := client.DescribeDatasetGroup(ctx, &forecastsdk.DescribeDatasetGroupInput{
		DatasetGroupArn: aws.String(resourceARN),
	})
	require.NoError(t, err)
	assert.Equal(t, "ACTIVE", aws.ToString(resumed.Status))

	_, err = client.DeleteResourceTree(ctx, &forecastsdk.DeleteResourceTreeInput{
		ResourceArn: aws.String(resourceARN),
	})
	require.NoError(t, err)

	_, err = client.DescribeDatasetGroup(ctx, &forecastsdk.DescribeDatasetGroupInput{
		DatasetGroupArn: aws.String(resourceARN),
	})
	require.Error(t, err)
}
