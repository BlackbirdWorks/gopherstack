package cloudwatchlogs_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwlsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwltypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

// TestDescribeMetricFilters_FilterNamePrefixIgnoredWithoutLogGroupName drives
// DescribeMetricFilters through the real aws-sdk-go-v2 client without
// LogGroupName. DescribeMetricFiltersInput.FilterNamePrefix's own doc comment
// (cloudwatchlogs@v1.81.1 api_op_DescribeMetricFilters.go:33) says CloudWatch
// Logs "uses the value that you set here only if you also include the
// logGroupName parameter in your request" -- so a prefix supplied without a
// log group name must be a no-op, matching every metric filter regardless of
// name, not narrowing the result set.
func TestDescribeMetricFilters_FilterNamePrefixIgnoredWithoutLogGroupName(t *testing.T) {
	t.Parallel()

	backend := cloudwatchlogs.NewInMemoryBackend()
	client := newTestCloudWatchLogsClient(t, cloudwatchlogs.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateLogGroup(ctx, &cwlsdk.CreateLogGroupInput{LogGroupName: aws.String("g1")})
	require.NoError(t, err)
	_, err = client.CreateLogGroup(ctx, &cwlsdk.CreateLogGroupInput{LogGroupName: aws.String("g2")})
	require.NoError(t, err)

	_, err = client.PutMetricFilter(ctx, &cwlsdk.PutMetricFilterInput{
		LogGroupName:  aws.String("g1"),
		FilterName:    aws.String("abc-filter"),
		FilterPattern: aws.String(""),
		MetricTransformations: []cwltypes.MetricTransformation{{
			MetricName:      aws.String("M1"),
			MetricNamespace: aws.String("NS"),
			MetricValue:     aws.String("1"),
		}},
	})
	require.NoError(t, err)

	_, err = client.PutMetricFilter(ctx, &cwlsdk.PutMetricFilterInput{
		LogGroupName:  aws.String("g2"),
		FilterName:    aws.String("xyz-filter"),
		FilterPattern: aws.String(""),
		MetricTransformations: []cwltypes.MetricTransformation{{
			MetricName:      aws.String("M2"),
			MetricNamespace: aws.String("NS"),
			MetricValue:     aws.String("1"),
		}},
	})
	require.NoError(t, err)

	out, err := client.DescribeMetricFilters(ctx, &cwlsdk.DescribeMetricFiltersInput{
		FilterNamePrefix: aws.String("abc"),
	})
	require.NoError(t, err)

	names := make([]string, 0, len(out.MetricFilters))
	for _, f := range out.MetricFilters {
		names = append(names, aws.ToString(f.FilterName))
	}

	assert.Contains(t, names, "abc-filter")
	assert.Contains(t, names, "xyz-filter",
		"FilterNamePrefix without LogGroupName must be ignored (AWS doc: "+
			"only applied when logGroupName is also set)")
	assert.Len(t, names, 2)
}
