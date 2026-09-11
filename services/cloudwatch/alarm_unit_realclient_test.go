package cloudwatch_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPutMetricAlarm_Unit_RoundTrips_RealClient covers gopherstack-l4ywn:
// MetricAlarm.Unit (cloudwatch@v1.66.3 schemas.go:3855, MetricAlarm_Unit; input
// member at schemas.go:4473, PutMetricAlarmInput_Unit) was previously dropped by
// PutMetricAlarm and never emitted by DescribeAlarms on the rpc-v2-cbor wire that
// a real aws-sdk-go-v2 client speaks.
func TestPutMetricAlarm_Unit_RoundTrips_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		unit cwtypes.StandardUnit
	}{
		{name: "Count", unit: cwtypes.StandardUnitCount},
		{name: "Bytes/Second", unit: cwtypes.StandardUnitBytesSecond},
		{name: "Percent", unit: cwtypes.StandardUnitPercent},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			ctx := t.Context()

			alarmName := "unit-rt-" + tc.name

			_, err := client.PutMetricAlarm(ctx, &cwsdk.PutMetricAlarmInput{
				AlarmName:          aws.String(alarmName),
				Namespace:          aws.String("NS"),
				MetricName:         aws.String("M"),
				ComparisonOperator: cwtypes.ComparisonOperatorGreaterThanThreshold,
				Threshold:          aws.Float64(1),
				EvaluationPeriods:  aws.Int32(1),
				Period:             aws.Int32(60),
				Unit:               tc.unit,
			})
			require.NoError(t, err)

			out, err := client.DescribeAlarms(ctx, &cwsdk.DescribeAlarmsInput{
				AlarmNames: []string{alarmName},
			})
			require.NoError(t, err)
			require.Len(t, out.MetricAlarms, 1)
			assert.Equal(t, tc.unit, out.MetricAlarms[0].Unit)
		})
	}
}

// TestPutMetricAlarm_UnitOmitted_RealClient asserts an alarm created without a
// Unit round-trips with the zero-value (empty) StandardUnit, matching real
// CloudWatch's "Unit is optional" behaviour on PutMetricAlarm.
func TestPutMetricAlarm_UnitOmitted_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	_, err := client.PutMetricAlarm(ctx, &cwsdk.PutMetricAlarmInput{
		AlarmName:          aws.String("unit-rt-omitted"),
		Namespace:          aws.String("NS"),
		MetricName:         aws.String("M"),
		ComparisonOperator: cwtypes.ComparisonOperatorGreaterThanThreshold,
		Threshold:          aws.Float64(1),
		EvaluationPeriods:  aws.Int32(1),
		Period:             aws.Int32(60),
	})
	require.NoError(t, err)

	out, err := client.DescribeAlarms(ctx, &cwsdk.DescribeAlarmsInput{
		AlarmNames: []string{"unit-rt-omitted"},
	})
	require.NoError(t, err)
	require.Len(t, out.MetricAlarms, 1)
	assert.Equal(t, cwtypes.StandardUnit(""), out.MetricAlarms[0].Unit)
}

// TestPutMetricAlarm_InvalidUnit_RealClient asserts an out-of-enum Unit value is
// rejected as InvalidParameterValueException on the CBOR wire.
func TestPutMetricAlarm_InvalidUnit_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	_, err := client.PutMetricAlarm(ctx, &cwsdk.PutMetricAlarmInput{
		AlarmName:          aws.String("unit-rt-invalid"),
		Namespace:          aws.String("NS"),
		MetricName:         aws.String("M"),
		ComparisonOperator: cwtypes.ComparisonOperatorGreaterThanThreshold,
		Threshold:          aws.Float64(1),
		EvaluationPeriods:  aws.Int32(1),
		Period:             aws.Int32(60),
		Unit:               cwtypes.StandardUnit("NotAUnit"),
	})
	require.Error(t, err)
}
