package autoscaling_test

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAutoscalingHandler_ScalingPolicyStepAdjustmentsRoundTrip verifies StepAdjustments are stored and returned
// by DescribePolicies, and MinAdjustmentMagnitude is preserved.
func TestAutoscalingHandler_ScalingPolicyStepAdjustmentsRoundTrip(t *testing.T) {
	t.Parallel()

	h := newAutoscalingHandler()
	asgName := "step-asg"

	code, body := doAS(t, h, "CreateAutoScalingGroup", url.Values{
		"AutoScalingGroupName":       {asgName},
		"MinSize":                    {"1"},
		"MaxSize":                    {"10"},
		"AvailabilityZones.member.1": {"us-east-1a"},
	})
	require.Equal(t, 200, code, body)

	code, body = doAS(t, h, "PutScalingPolicy", url.Values{
		"AutoScalingGroupName":   {asgName},
		"PolicyName":             {"step-policy"},
		"PolicyType":             {"StepScaling"},
		"AdjustmentType":         {"ChangeInCapacity"},
		"MinAdjustmentMagnitude": {"2"},
		"StepAdjustments.member.1.ScalingAdjustment":        {"2"},
		"StepAdjustments.member.1.MetricIntervalLowerBound": {"0"},
		"StepAdjustments.member.1.MetricIntervalUpperBound": {"10"},
		"StepAdjustments.member.2.ScalingAdjustment":        {"4"},
		"StepAdjustments.member.2.MetricIntervalLowerBound": {"10"},
	})
	require.Equal(t, 200, code, body)

	code, body = doAS(t, h, "DescribePolicies", url.Values{
		"AutoScalingGroupName": {asgName},
		"PolicyNames.member.1": {"step-policy"},
	})
	require.Equal(t, 200, code)

	assert.Contains(t, body, "<MinAdjustmentMagnitude>2</MinAdjustmentMagnitude>",
		"MinAdjustmentMagnitude must be returned; got: %s", body)
	assert.Contains(t, body, "<ScalingAdjustment>2</ScalingAdjustment>",
		"first step ScalingAdjustment must be present")
	assert.Contains(t, body, "<ScalingAdjustment>4</ScalingAdjustment>",
		"second step ScalingAdjustment must be present")
	assert.Contains(t, body, "<MetricIntervalLowerBound>0</MetricIntervalLowerBound>",
		"first step MetricIntervalLowerBound must be present")
}

// TestAutoscalingHandler_ExecutePolicyStepScaling verifies that ExecutePolicy selects the matching
// StepAdjustment via (MetricValue-BreachThreshold) for a StepScaling policy, and
// rejects execution when those required fields are missing.
func TestAutoscalingHandler_ExecutePolicyStepScaling(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name            string
		metricValue     string
		breachThreshold string
		wantCode        int
		wantDesired     int32
	}{
		{name: "low step matches", metricValue: "15", breachThreshold: "10", wantCode: 200, wantDesired: 5},
		{
			name:            "high step matches unbounded upper",
			metricValue:     "25",
			breachThreshold: "10",
			wantCode:        200,
			wantDesired:     8,
		},
		{name: "missing MetricValue is rejected", metricValue: "", breachThreshold: "10", wantCode: 400},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()
			asgName := "step-exec-asg-" + tc.name

			code, body := doAS(t, h, "CreateAutoScalingGroup", url.Values{
				"AutoScalingGroupName":       {asgName},
				"MinSize":                    {"0"},
				"MaxSize":                    {"20"},
				"DesiredCapacity":            {"2"},
				"AvailabilityZones.member.1": {"us-east-1a"},
			})
			require.Equal(t, 200, code, body)

			code, body = doAS(t, h, "PutScalingPolicy", url.Values{
				"AutoScalingGroupName": {asgName},
				"PolicyName":           {"step1"},
				"PolicyType":           {"StepScaling"},
				"AdjustmentType":       {"ChangeInCapacity"},
				"StepAdjustments.member.1.ScalingAdjustment":        {"3"},
				"StepAdjustments.member.1.MetricIntervalLowerBound": {"0"},
				"StepAdjustments.member.1.MetricIntervalUpperBound": {"10"},
				"StepAdjustments.member.2.ScalingAdjustment":        {"6"},
				"StepAdjustments.member.2.MetricIntervalLowerBound": {"10"},
			})
			require.Equal(t, 200, code, body)

			executeVals := url.Values{
				"AutoScalingGroupName": {asgName},
				"PolicyName":           {"step1"},
				"BreachThreshold":      {tc.breachThreshold},
			}
			if tc.metricValue != "" {
				executeVals.Set("MetricValue", tc.metricValue)
			}

			code, body = doAS(t, h, "ExecutePolicy", executeVals)
			require.Equal(t, tc.wantCode, code, body)

			if tc.wantCode != 200 {
				return
			}

			code, body = doAS(t, h, "DescribeAutoScalingGroups", url.Values{
				"AutoScalingGroupNames.member.1": {asgName},
			})
			require.Equal(t, 200, code, body)

			parsed := describeASGInstances(t, body)
			assert.Equal(t, tc.wantDesired, parsed.Result.AutoScalingGroups.Members[0].DesiredCapacity)
		})
	}
}

// TestAutoscalingHandler_PutScalingPolicyMetricAggregationType verifies MetricAggregationType is
// parsed on PutScalingPolicy and returned by DescribePolicies (previously silently
// dropped on both the request and response side).
func TestAutoscalingHandler_PutScalingPolicyMetricAggregationType(t *testing.T) {
	t.Parallel()

	h := newAutoscalingHandler()
	asgName := "mat-asg"

	code, body := doAS(t, h, "CreateAutoScalingGroup", url.Values{
		"AutoScalingGroupName":       {asgName},
		"MinSize":                    {"0"},
		"MaxSize":                    {"5"},
		"AvailabilityZones.member.1": {"us-east-1a"},
	})
	require.Equal(t, 200, code, body)

	code, body = doAS(t, h, "PutScalingPolicy", url.Values{
		"AutoScalingGroupName":  {asgName},
		"PolicyName":            {"agg-policy"},
		"PolicyType":            {"StepScaling"},
		"AdjustmentType":        {"ChangeInCapacity"},
		"MetricAggregationType": {"Average"},
		"StepAdjustments.member.1.ScalingAdjustment": {"1"},
	})
	require.Equal(t, 200, code, body)

	code, body = doAS(t, h, "DescribePolicies", url.Values{
		"AutoScalingGroupName": {asgName},
	})
	require.Equal(t, 200, code, body)
	assert.Contains(t, body, "<MetricAggregationType>Average</MetricAggregationType>",
		"MetricAggregationType must round-trip; got: %s", body)
}

// TestAutoscalingHandler_PutScalingPolicyPredictiveScalingRoundTrip verifies that
// PredictiveScalingConfiguration (previously accepted by PutScalingPolicy and
// silently dropped) is parsed from the flattened query form and returned by
// DescribePolicies.
func TestAutoscalingHandler_PutScalingPolicyPredictiveScalingRoundTrip(t *testing.T) {
	t.Parallel()

	t.Run("predefined metric pair", func(t *testing.T) {
		t.Parallel()

		h := newAutoscalingHandler()
		asgName := "predictive-asg-pair"

		code, body := doAS(t, h, "CreateAutoScalingGroup", url.Values{
			"AutoScalingGroupName":       {asgName},
			"MinSize":                    {"1"},
			"MaxSize":                    {"10"},
			"AvailabilityZones.member.1": {"us-east-1a"},
		})
		require.Equal(t, 200, code, body)

		code, body = doAS(t, h, "PutScalingPolicy", url.Values{
			"AutoScalingGroupName": {asgName},
			"PolicyName":           {"predictive-policy"},
			"PolicyType":           {"PredictiveScaling"},
			"PredictiveScalingConfiguration.MaxCapacityBreachBehavior":                 {"IncreaseMaxCapacity"},
			"PredictiveScalingConfiguration.MaxCapacityBuffer":                         {"10"},
			"PredictiveScalingConfiguration.Mode":                                      {"ForecastAndScale"},
			"PredictiveScalingConfiguration.SchedulingBufferTime":                      {"300"},
			"PredictiveScalingConfiguration.MetricSpecifications.member.1.TargetValue": {"40"},
			"PredictiveScalingConfiguration.MetricSpecifications.member.1." +
				"PredefinedMetricPairSpecification.PredefinedMetricType": {"ASGCPUUtilization"},
			"PredictiveScalingConfiguration.MetricSpecifications.member.1." +
				"PredefinedMetricPairSpecification.ResourceLabel": {"app/my-alb/abc/targetgroup/my-tg/def"},
		})
		require.Equal(t, 200, code, body)

		code, body = doAS(t, h, "DescribePolicies", url.Values{
			"AutoScalingGroupName": {asgName},
			"PolicyNames.member.1": {"predictive-policy"},
		})
		require.Equal(t, 200, code, body)

		assert.Contains(t, body,
			"<MaxCapacityBreachBehavior>IncreaseMaxCapacity</MaxCapacityBreachBehavior>",
			"MaxCapacityBreachBehavior must round-trip; got: %s", body)
		assert.Contains(t, body, "<MaxCapacityBuffer>10</MaxCapacityBuffer>",
			"MaxCapacityBuffer must round-trip; got: %s", body)
		assert.Contains(t, body, "<Mode>ForecastAndScale</Mode>",
			"Mode must round-trip; got: %s", body)
		assert.Contains(t, body, "<SchedulingBufferTime>300</SchedulingBufferTime>",
			"SchedulingBufferTime must round-trip; got: %s", body)
		assert.Contains(t, body, "<TargetValue>40</TargetValue>",
			"metric specification TargetValue must round-trip; got: %s", body)
		assert.Contains(t, body, "<PredefinedMetricType>ASGCPUUtilization</PredefinedMetricType>",
			"PredefinedMetricPairSpecification.PredefinedMetricType must round-trip; got: %s", body)
		assert.Contains(t, body, "<ResourceLabel>app/my-alb/abc/targetgroup/my-tg/def</ResourceLabel>",
			"PredefinedMetricPairSpecification.ResourceLabel must round-trip; got: %s", body)
	})

	t.Run("predefined load and scaling metrics", func(t *testing.T) {
		t.Parallel()

		h := newAutoscalingHandler()
		asgName := "predictive-asg-load-scaling"

		code, body := doAS(t, h, "CreateAutoScalingGroup", url.Values{
			"AutoScalingGroupName":       {asgName},
			"MinSize":                    {"1"},
			"MaxSize":                    {"10"},
			"AvailabilityZones.member.1": {"us-east-1a"},
		})
		require.Equal(t, 200, code, body)

		code, body = doAS(t, h, "PutScalingPolicy", url.Values{
			"AutoScalingGroupName": {asgName},
			"PolicyName":           {"predictive-policy-2"},
			"PolicyType":           {"PredictiveScaling"},
			"PredictiveScalingConfiguration.MetricSpecifications.member.1.TargetValue": {"55"},
			"PredictiveScalingConfiguration.MetricSpecifications.member.1." +
				"PredefinedLoadMetricSpecification.PredefinedMetricType": {"ASGTotalCPUUtilization"},
			"PredictiveScalingConfiguration.MetricSpecifications.member.1." +
				"PredefinedScalingMetricSpecification.PredefinedMetricType": {"ASGAverageCPUUtilization"},
		})
		require.Equal(t, 200, code, body)

		code, body = doAS(t, h, "DescribePolicies", url.Values{
			"AutoScalingGroupName": {asgName},
			"PolicyNames.member.1": {"predictive-policy-2"},
		})
		require.Equal(t, 200, code, body)

		assert.Contains(t, body, "<PredefinedMetricType>ASGTotalCPUUtilization</PredefinedMetricType>",
			"PredefinedLoadMetricSpecification.PredefinedMetricType must round-trip; got: %s", body)
		assert.Contains(t, body, "<PredefinedMetricType>ASGAverageCPUUtilization</PredefinedMetricType>",
			"PredefinedScalingMetricSpecification.PredefinedMetricType must round-trip; got: %s", body)
		assert.Contains(t, body, "<TargetValue>55</TargetValue>",
			"metric specification TargetValue must round-trip; got: %s", body)
	})

	t.Run("absent when not a predictive policy", func(t *testing.T) {
		t.Parallel()

		h := newAutoscalingHandler()
		asgName := "predictive-asg-absent"

		code, body := doAS(t, h, "CreateAutoScalingGroup", url.Values{
			"AutoScalingGroupName":       {asgName},
			"MinSize":                    {"1"},
			"MaxSize":                    {"10"},
			"AvailabilityZones.member.1": {"us-east-1a"},
		})
		require.Equal(t, 200, code, body)

		code, body = doAS(t, h, "PutScalingPolicy", url.Values{
			"AutoScalingGroupName": {asgName},
			"PolicyName":           {"simple-policy"},
			"PolicyType":           {"SimpleScaling"},
			"AdjustmentType":       {"ChangeInCapacity"},
			"ScalingAdjustment":    {"1"},
		})
		require.Equal(t, 200, code, body)

		code, body = doAS(t, h, "DescribePolicies", url.Values{
			"AutoScalingGroupName": {asgName},
			"PolicyNames.member.1": {"simple-policy"},
		})
		require.Equal(t, 200, code, body)

		assert.NotContains(t, body, "PredictiveScalingConfiguration",
			"PredictiveScalingConfiguration must be absent for a non-predictive policy; got: %s", body)
	})
}
