package cloudwatch_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDescribeAlarmHistory_AlarmTypesDefault_RealClient covers a wire-key bug: a
// real aws-sdk-go-v2 client serializes DescribeAlarmHistoryInput.AlarmTypes (a
// list) onto the CBOR wire under the key "AlarmTypes"
// (cloudwatch@v1.66.3/api_op_DescribeAlarmHistory.go:53,92), but
// cborDescribeAlarmHistory read a nonexistent singular "AlarmType" key -- so a
// real client's AlarmTypes filter was silently dropped on every call. The
// operation's own doc comment ("If you omit this parameter, only metric
// alarms are returned") was therefore also violated: composite-alarm history
// leaked into an unfiltered DescribeAlarmHistory call, and an explicit
// AlarmTypes=[CompositeAlarm] request returned nothing because the key it
// looked for was never present.
func TestDescribeAlarmHistory_AlarmTypesDefault_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	_, err := client.PutMetricAlarm(ctx, &cwsdk.PutMetricAlarmInput{
		AlarmName:          aws.String("hist-rt-metric"),
		Namespace:          aws.String("NS"),
		MetricName:         aws.String("M"),
		ComparisonOperator: cwtypes.ComparisonOperatorGreaterThanThreshold,
		Threshold:          aws.Float64(1),
		EvaluationPeriods:  aws.Int32(1),
		Period:             aws.Int32(60),
	})
	require.NoError(t, err)

	_, err = client.PutCompositeAlarm(ctx, &cwsdk.PutCompositeAlarmInput{
		AlarmName: aws.String("hist-rt-composite"),
		AlarmRule: aws.String(`ALARM("nonexistent")`),
	})
	require.NoError(t, err)

	t.Run("omitted defaults to metric alarms only", func(t *testing.T) {
		t.Parallel()

		out, histErr := client.DescribeAlarmHistory(ctx, &cwsdk.DescribeAlarmHistoryInput{})
		require.NoError(t, histErr)

		names := historyAlarmNames(out.AlarmHistoryItems)
		assert.Contains(t, names, "hist-rt-metric", "metric alarm history must be present by default")
		assert.NotContains(t, names, "hist-rt-composite",
			"composite alarm history must be excluded when AlarmTypes is omitted")
	})

	t.Run("explicit CompositeAlarm filter is honoured", func(t *testing.T) {
		t.Parallel()

		out, histErr := client.DescribeAlarmHistory(ctx, &cwsdk.DescribeAlarmHistoryInput{
			AlarmTypes: []cwtypes.AlarmType{cwtypes.AlarmTypeCompositeAlarm},
		})
		require.NoError(t, histErr)

		names := historyAlarmNames(out.AlarmHistoryItems)
		assert.Contains(t, names, "hist-rt-composite", "explicit CompositeAlarm filter must be honoured")
		assert.NotContains(t, names, "hist-rt-metric", "explicit CompositeAlarm filter must exclude metric alarms")
	})
}

func historyAlarmNames(items []cwtypes.AlarmHistoryItem) map[string]bool {
	names := make(map[string]bool, len(items))
	for _, item := range items {
		if item.AlarmName != nil {
			names[*item.AlarmName] = true
		}
	}

	return names
}
