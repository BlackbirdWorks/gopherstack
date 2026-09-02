package cloudwatch_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTagResourceFamily_SDKRoundTrip drives TagResource, UntagResource, and
// ListTagsForResource through the real aws-sdk-go-v2 client (cloudwatch@v1.66.3,
// rpc-v2 CBOR) instead of only exercising Tags supplied at Put*-time, to
// prove the Tags-as-array-of-{Key,Value} wire shape the SDK actually sends
// for these three ops decodes correctly end to end.
func TestTagResourceFamily_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	putOut, err := client.PutMetricAlarm(t.Context(), &cwsdk.PutMetricAlarmInput{
		AlarmName:          aws.String("tagfamily-alarm"),
		Namespace:          aws.String("NS"),
		MetricName:         aws.String("M"),
		ComparisonOperator: types.ComparisonOperatorGreaterThanThreshold,
		EvaluationPeriods:  aws.Int32(1),
		Period:             aws.Int32(60),
		Statistic:          types.StatisticAverage,
		Threshold:          aws.Float64(1),
	})
	require.NoError(t, err)
	_ = putOut

	descOut, err := client.DescribeAlarms(t.Context(), &cwsdk.DescribeAlarmsInput{
		AlarmNames: []string{"tagfamily-alarm"},
	})
	require.NoError(t, err)
	require.Len(t, descOut.MetricAlarms, 1)
	alarmArn := descOut.MetricAlarms[0].AlarmArn

	_, err = client.TagResource(t.Context(), &cwsdk.TagResourceInput{
		ResourceARN: alarmArn,
		Tags: []types.Tag{
			{Key: aws.String("env"), Value: aws.String("prod")},
			{Key: aws.String("team"), Value: aws.String("platform")},
		},
	})
	require.NoError(t, err)

	listOut, err := client.ListTagsForResource(t.Context(), &cwsdk.ListTagsForResourceInput{ResourceARN: alarmArn})
	require.NoError(t, err)
	require.Len(t, listOut.Tags, 2)

	got := map[string]string{}
	for _, tag := range listOut.Tags {
		got[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}
	assert.Equal(t, map[string]string{"env": "prod", "team": "platform"}, got)

	_, err = client.UntagResource(t.Context(), &cwsdk.UntagResourceInput{
		ResourceARN: alarmArn,
		TagKeys:     []string{"team"},
	})
	require.NoError(t, err)

	listOut2, err := client.ListTagsForResource(t.Context(), &cwsdk.ListTagsForResourceInput{ResourceARN: alarmArn})
	require.NoError(t, err)
	require.Len(t, listOut2.Tags, 1)
	assert.Equal(t, "env", aws.ToString(listOut2.Tags[0].Key))
	assert.Equal(t, "prod", aws.ToString(listOut2.Tags[0].Value))
}
