package autoscaling_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	assdk "github.com/aws/aws-sdk-go-v2/service/autoscaling"
	"github.com/aws/aws-sdk-go-v2/service/autoscaling/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSDKRoundTrip_RequiredOutputMembers_r80d proves gopherstack-r80d batch 29's
// two findings against autoscaling@v1.70.4.
//
// 1. types.Activity.Cause (types.go:298) is required, but gopherstack's
// ScalingActivity model had no Cause field at all -- every DescribeScalingActivities
// call decoded a nil Cause on every activity, in every reachable state (there is no
// construction site that could have populated it, since the field never existed).
//
// 2. types.LoadForecast.MetricSpecification (types.go:2670) is required, but
// GetPredictiveScalingForecast's response never populated it -- a real client
// decoded a nil MetricSpecification on every entry, even though the referenced
// policy's own PredictiveScalingConfiguration.MetricSpecifications (stored via
// PutScalingPolicy, bd gopherstack-2uti) already carries the real data.
func TestSDKRoundTrip_RequiredOutputMembers_r80d(t *testing.T) {
	t.Parallel()

	t.Run("scaling activity cause", func(t *testing.T) {
		t.Parallel()
		testScalingActivityCause(t)
	})

	t.Run("load forecast metric specification", func(t *testing.T) {
		t.Parallel()
		testLoadForecastMetricSpecification(t)
	})
}

func testScalingActivityCause(t *testing.T) {
	t.Helper()
	ctx := t.Context()
	client := newTestHandlerAndClient(t)

	const groupName = "rt-cause-group"

	_, err := client.CreateAutoScalingGroup(ctx, &assdk.CreateAutoScalingGroupInput{
		AutoScalingGroupName: aws.String(groupName),
		MinSize:              aws.Int32(1),
		MaxSize:              aws.Int32(3),
		DesiredCapacity:      aws.Int32(1),
		AvailabilityZones:    []string{"us-east-1a"},
	})
	require.NoError(t, err)

	out, err := client.DescribeScalingActivities(ctx, &assdk.DescribeScalingActivitiesInput{
		AutoScalingGroupName: aws.String(groupName),
	})
	require.NoError(t, err)
	require.NotEmpty(t, out.Activities)

	activity := out.Activities[0]
	assert.NotNil(t, activity.Cause)
	assert.NotEmpty(t, aws.ToString(activity.Cause))
	assert.NotNil(t, activity.ActivityId)
	assert.NotNil(t, activity.AutoScalingGroupName)
}

func testLoadForecastMetricSpecification(t *testing.T) {
	t.Helper()
	ctx := t.Context()
	client := newTestHandlerAndClient(t)

	const (
		groupName  = "rt-forecast-group"
		policyName = "rt-predictive-policy"
	)

	_, err := client.CreateAutoScalingGroup(ctx, &assdk.CreateAutoScalingGroupInput{
		AutoScalingGroupName: aws.String(groupName),
		MinSize:              aws.Int32(1),
		MaxSize:              aws.Int32(3),
		DesiredCapacity:      aws.Int32(1),
		AvailabilityZones:    []string{"us-east-1a"},
	})
	require.NoError(t, err)

	_, err = client.PutScalingPolicy(ctx, &assdk.PutScalingPolicyInput{
		AutoScalingGroupName: aws.String(groupName),
		PolicyName:           aws.String(policyName),
		PolicyType:           aws.String("PredictiveScaling"),
		PredictiveScalingConfiguration: &types.PredictiveScalingConfiguration{
			MetricSpecifications: []types.PredictiveScalingMetricSpecification{
				{
					TargetValue: aws.Float64(42),
					PredefinedLoadMetricSpecification: &types.PredictiveScalingPredefinedLoadMetric{
						PredefinedMetricType: types.PredefinedLoadMetricTypeASGTotalCPUUtilization,
					},
				},
			},
		},
	})
	require.NoError(t, err)

	now := time.Now().UTC()

	out, err := client.GetPredictiveScalingForecast(ctx, &assdk.GetPredictiveScalingForecastInput{
		AutoScalingGroupName: aws.String(groupName),
		PolicyName:           aws.String(policyName),
		StartTime:            aws.Time(now),
		EndTime:              aws.Time(now.Add(48 * time.Hour)),
	})
	require.NoError(t, err)
	require.NotEmpty(t, out.LoadForecast)

	forecast := out.LoadForecast[0]
	require.NotNil(t, forecast.MetricSpecification)
	assert.InDelta(t, 42.0, aws.ToFloat64(forecast.MetricSpecification.TargetValue), 0.0001)
	require.NotNil(t, forecast.MetricSpecification.PredefinedLoadMetricSpecification)
	assert.Equal(
		t,
		types.PredefinedLoadMetricTypeASGTotalCPUUtilization,
		forecast.MetricSpecification.PredefinedLoadMetricSpecification.PredefinedMetricType,
	)
	assert.NotNil(t, out.CapacityForecast)
	assert.NotEmpty(t, forecast.Timestamps)
	assert.NotEmpty(t, forecast.Values)
}
