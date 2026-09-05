package cloudwatch_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDescribeAlarms_RealClient_BoundaryWalk confirms, through the real
// aws-sdk-go-v2 client, that paginateAlarmResults' combined-page-window
// offset (clamped via min(page.DecodeToken(nextToken), combinedTotal))
// walks a full DescribeAlarms collection without dropping or duplicating
// entries.
func TestDescribeAlarms_RealClient_BoundaryWalk(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	const n = 7

	names := make([]string, n)
	for i := range n {
		name := fmt.Sprintf("alarm-%03d", i)
		names[i] = name

		_, err := client.PutMetricAlarm(t.Context(), &cwsdk.PutMetricAlarmInput{
			AlarmName:          aws.String(name),
			Namespace:          aws.String("NS"),
			MetricName:         aws.String("M"),
			ComparisonOperator: types.ComparisonOperatorGreaterThanThreshold,
			EvaluationPeriods:  aws.Int32(1),
			Period:             aws.Int32(60),
			Statistic:          types.StatisticAverage,
			Threshold:          aws.Float64(1),
		})
		require.NoError(t, err)
	}

	var got []string

	var token *string
	for range n + 1 {
		out, err := client.DescribeAlarms(t.Context(), &cwsdk.DescribeAlarmsInput{
			MaxRecords: aws.Int32(3),
			NextToken:  token,
		})
		require.NoError(t, err)

		for _, a := range out.MetricAlarms {
			got = append(got, aws.ToString(a.AlarmName))
		}

		token = out.NextToken
		if aws.ToString(token) == "" {
			break
		}
	}

	assert.ElementsMatch(t, names, got, "boundary walk must reproduce the collection exactly, no drops or dupes")
}
