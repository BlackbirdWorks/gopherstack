package autoscaling_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	assdk "github.com/aws/aws-sdk-go-v2/service/autoscaling"
	"github.com/aws/aws-sdk-go-v2/service/autoscaling/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPutScalingPolicy_TargetTrackingCustomizedMetricSpecificationRoundTrip
// drives a TargetTrackingScaling policy carrying a customized (not
// predefined) metric through the real SDK client. The customized metric's
// Metrics field is a CloudWatch metric-data-query list nesting a MetricStat
// with dimensions -- exactly the class of nested query-protocol structure
// that arrives flattened as TargetTrackingConfiguration.
// CustomizedMetricSpecification.Metrics.member.1.MetricStat.Metric.
// Dimensions.member.1.Name, etc. A raw-form test would not catch a parser
// that silently drops the nested body while accepting the top-level key.
func TestPutScalingPolicy_TargetTrackingCustomizedMetricSpecificationRoundTrip(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	_, err := client.CreateAutoScalingGroup(t.Context(), &assdk.CreateAutoScalingGroupInput{
		AutoScalingGroupName: aws.String("customized-metric-asg"),
		MinSize:              aws.Int32(0),
		MaxSize:              aws.Int32(1),
		AvailabilityZones:    []string{"us-east-1a"},
	})
	require.NoError(t, err)

	_, err = client.PutScalingPolicy(t.Context(), &assdk.PutScalingPolicyInput{
		AutoScalingGroupName: aws.String("customized-metric-asg"),
		PolicyName:           aws.String("custom-tt-policy"),
		PolicyType:           aws.String("TargetTrackingScaling"),
		TargetTrackingConfiguration: &types.TargetTrackingConfiguration{
			TargetValue: aws.Float64(75.5),
			CustomizedMetricSpecification: &types.CustomizedMetricSpecification{
				Metrics: []types.TargetTrackingMetricDataQuery{
					{
						Id:         aws.String("m1"),
						ReturnData: aws.Bool(true),
						MetricStat: &types.TargetTrackingMetricStat{
							Metric: &types.Metric{
								MetricName: aws.String("CPUUtilization"),
								Namespace:  aws.String("AWS/EC2"),
								Dimensions: []types.MetricDimension{
									{
										Name:  aws.String("AutoScalingGroupName"),
										Value: aws.String("customized-metric-asg"),
									},
								},
							},
							Stat:   aws.String("Average"),
							Period: aws.Int32(60),
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	got, err := client.DescribePolicies(t.Context(), &assdk.DescribePoliciesInput{
		AutoScalingGroupName: aws.String("customized-metric-asg"),
	})
	require.NoError(t, err)
	require.Len(t, got.ScalingPolicies, 1)

	ttc := got.ScalingPolicies[0].TargetTrackingConfiguration
	require.NotNil(t, ttc)
	require.NotNil(t, ttc.CustomizedMetricSpecification)
	require.Len(t, ttc.CustomizedMetricSpecification.Metrics, 1)

	q := ttc.CustomizedMetricSpecification.Metrics[0]
	assert.Equal(t, "m1", aws.ToString(q.Id))
	require.NotNil(t, q.MetricStat)
	assert.Equal(t, "Average", aws.ToString(q.MetricStat.Stat))
	assert.Equal(t, int32(60), aws.ToInt32(q.MetricStat.Period))
	require.NotNil(t, q.MetricStat.Metric)
	assert.Equal(t, "CPUUtilization", aws.ToString(q.MetricStat.Metric.MetricName))
	assert.Equal(t, "AWS/EC2", aws.ToString(q.MetricStat.Metric.Namespace))
	require.Len(t, q.MetricStat.Metric.Dimensions, 1)
	assert.Equal(t, "AutoScalingGroupName", aws.ToString(q.MetricStat.Metric.Dimensions[0].Name))
	assert.Equal(t, "customized-metric-asg", aws.ToString(q.MetricStat.Metric.Dimensions[0].Value))
}

// TestPutScalingPolicy_TargetTrackingLegacyCustomizedMetricRoundTrip drives a
// TargetTrackingScaling policy carrying the legacy (pre-metric-math)
// CustomizedMetricSpecification shape -- MetricName/Namespace/Dimensions/
// Statistic/Unit/Period with no Metrics list -- through the real SDK client.
func TestPutScalingPolicy_TargetTrackingLegacyCustomizedMetricRoundTrip(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	_, err := client.CreateAutoScalingGroup(t.Context(), &assdk.CreateAutoScalingGroupInput{
		AutoScalingGroupName: aws.String("legacy-customized-metric-asg"),
		MinSize:              aws.Int32(0),
		MaxSize:              aws.Int32(1),
		AvailabilityZones:    []string{"us-east-1a"},
	})
	require.NoError(t, err)

	_, err = client.PutScalingPolicy(t.Context(), &assdk.PutScalingPolicyInput{
		AutoScalingGroupName: aws.String("legacy-customized-metric-asg"),
		PolicyName:           aws.String("legacy-custom-tt-policy"),
		PolicyType:           aws.String("TargetTrackingScaling"),
		TargetTrackingConfiguration: &types.TargetTrackingConfiguration{
			TargetValue: aws.Float64(40),
			CustomizedMetricSpecification: &types.CustomizedMetricSpecification{
				MetricName: aws.String("CPUUtilization"),
				Namespace:  aws.String("AWS/EC2"),
				Statistic:  types.MetricStatisticAverage,
				Unit:       aws.String("Percent"),
				Period:     aws.Int32(60),
				Dimensions: []types.MetricDimension{
					{Name: aws.String("AutoScalingGroupName"), Value: aws.String("legacy-customized-metric-asg")},
				},
			},
		},
	})
	require.NoError(t, err)

	got, err := client.DescribePolicies(t.Context(), &assdk.DescribePoliciesInput{
		AutoScalingGroupName: aws.String("legacy-customized-metric-asg"),
	})
	require.NoError(t, err)
	require.Len(t, got.ScalingPolicies, 1)

	spec := got.ScalingPolicies[0].TargetTrackingConfiguration.CustomizedMetricSpecification
	require.NotNil(t, spec)
	assert.Equal(t, "CPUUtilization", aws.ToString(spec.MetricName))
	assert.Equal(t, "AWS/EC2", aws.ToString(spec.Namespace))
	assert.Equal(t, types.MetricStatisticAverage, spec.Statistic)
	assert.Equal(t, "Percent", aws.ToString(spec.Unit))
	assert.Equal(t, int32(60), aws.ToInt32(spec.Period))
	require.Len(t, spec.Dimensions, 1)
	assert.Equal(t, "AutoScalingGroupName", aws.ToString(spec.Dimensions[0].Name))
	assert.Empty(t, spec.Metrics)
}

// TestPutScalingPolicy_PredictiveScalingCustomizedMetricsRoundTrip drives a
// PredictiveScaling policy carrying all three Customized*MetricSpecification
// variants (Load/Scaling/Capacity), each a MetricDataQueries list, through
// the real SDK client.
func TestPutScalingPolicy_PredictiveScalingCustomizedMetricsRoundTrip(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	_, err := client.CreateAutoScalingGroup(t.Context(), &assdk.CreateAutoScalingGroupInput{
		AutoScalingGroupName: aws.String("predictive-customized-asg"),
		MinSize:              aws.Int32(0),
		MaxSize:              aws.Int32(1),
		AvailabilityZones:    []string{"us-east-1a"},
	})
	require.NoError(t, err)

	loadQuery := types.MetricDataQuery{
		Id:         aws.String("load1"),
		ReturnData: aws.Bool(true),
		MetricStat: &types.MetricStat{
			Metric: &types.Metric{
				MetricName: aws.String("RequestCount"),
				Namespace:  aws.String("AWS/ApplicationELB"),
			},
			Stat: aws.String("Sum"),
		},
	}
	scalingQuery := types.MetricDataQuery{
		Id:         aws.String("scaling1"),
		Expression: aws.String("scaling1_raw"),
		ReturnData: aws.Bool(true),
	}
	capacityQuery := types.MetricDataQuery{
		Id: aws.String("capacity1"),
		MetricStat: &types.MetricStat{
			Metric: &types.Metric{
				MetricName: aws.String("GroupInServiceInstances"),
				Namespace:  aws.String("AWS/AutoScaling"),
			},
			Stat: aws.String("Average"),
			Unit: aws.String("Count"),
		},
		ReturnData: aws.Bool(true),
	}

	_, err = client.PutScalingPolicy(t.Context(), &assdk.PutScalingPolicyInput{
		AutoScalingGroupName: aws.String("predictive-customized-asg"),
		PolicyName:           aws.String("custom-predictive-policy"),
		PolicyType:           aws.String("PredictiveScaling"),
		PredictiveScalingConfiguration: &types.PredictiveScalingConfiguration{
			MetricSpecifications: []types.PredictiveScalingMetricSpecification{
				{
					TargetValue: aws.Float64(1000),
					CustomizedLoadMetricSpecification: &types.PredictiveScalingCustomizedLoadMetric{
						MetricDataQueries: []types.MetricDataQuery{loadQuery},
					},
					CustomizedScalingMetricSpecification: &types.PredictiveScalingCustomizedScalingMetric{
						MetricDataQueries: []types.MetricDataQuery{scalingQuery},
					},
					CustomizedCapacityMetricSpecification: &types.PredictiveScalingCustomizedCapacityMetric{
						MetricDataQueries: []types.MetricDataQuery{capacityQuery},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	got, err := client.DescribePolicies(t.Context(), &assdk.DescribePoliciesInput{
		AutoScalingGroupName: aws.String("predictive-customized-asg"),
	})
	require.NoError(t, err)
	require.Len(t, got.ScalingPolicies, 1)

	psc := got.ScalingPolicies[0].PredictiveScalingConfiguration
	require.NotNil(t, psc)
	require.Len(t, psc.MetricSpecifications, 1)
	spec := psc.MetricSpecifications[0]

	require.NotNil(t, spec.CustomizedLoadMetricSpecification)
	require.Len(t, spec.CustomizedLoadMetricSpecification.MetricDataQueries, 1)
	gotLoad := spec.CustomizedLoadMetricSpecification.MetricDataQueries[0]
	assert.Equal(t, "load1", aws.ToString(gotLoad.Id))
	require.NotNil(t, gotLoad.MetricStat)
	require.NotNil(t, gotLoad.MetricStat.Metric)
	assert.Equal(t, "RequestCount", aws.ToString(gotLoad.MetricStat.Metric.MetricName))
	assert.Equal(t, "AWS/ApplicationELB", aws.ToString(gotLoad.MetricStat.Metric.Namespace))
	assert.Equal(t, "Sum", aws.ToString(gotLoad.MetricStat.Stat))

	require.NotNil(t, spec.CustomizedScalingMetricSpecification)
	require.Len(t, spec.CustomizedScalingMetricSpecification.MetricDataQueries, 1)
	assert.Equal(
		t,
		"scaling1_raw",
		aws.ToString(spec.CustomizedScalingMetricSpecification.MetricDataQueries[0].Expression),
	)

	require.NotNil(t, spec.CustomizedCapacityMetricSpecification)
	require.Len(t, spec.CustomizedCapacityMetricSpecification.MetricDataQueries, 1)
	gotCapacity := spec.CustomizedCapacityMetricSpecification.MetricDataQueries[0]
	assert.Equal(t, "capacity1", aws.ToString(gotCapacity.Id))
	require.NotNil(t, gotCapacity.MetricStat)
	assert.Equal(t, "Count", aws.ToString(gotCapacity.MetricStat.Unit))
}

// TestCreateAutoScalingGroup_BaselinePerformanceFactorsRoundTrip drives a
// MixedInstancesPolicy override's InstanceRequirements.
// BaselinePerformanceFactors through the real SDK client. Its wire shape is
// an outlier among this handler's query-protocol lists: the field
// serializes under the singular key "Reference" (not "References"), wrapped
// in "item" (not "member") -- verified against serializers.go:4971/5918, not
// inferred from the field name.
func TestCreateAutoScalingGroup_BaselinePerformanceFactorsRoundTrip(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	_, err := client.CreateAutoScalingGroup(t.Context(), &assdk.CreateAutoScalingGroupInput{
		AutoScalingGroupName: aws.String("baseline-perf-asg"),
		MinSize:              aws.Int32(0),
		MaxSize:              aws.Int32(1),
		AvailabilityZones:    []string{"us-east-1a"},
		MixedInstancesPolicy: &types.MixedInstancesPolicy{
			LaunchTemplate: &types.LaunchTemplate{
				LaunchTemplateSpecification: &types.LaunchTemplateSpecification{
					LaunchTemplateName: aws.String("baseline-perf-lt"),
					Version:            aws.String("$Latest"),
				},
				Overrides: []types.LaunchTemplateOverrides{
					{
						InstanceRequirements: &types.InstanceRequirements{
							VCpuCount: &types.VCpuCountRequest{Min: aws.Int32(2)},
							MemoryMiB: &types.MemoryMiBRequest{Min: aws.Int32(2048)},
							BaselinePerformanceFactors: &types.BaselinePerformanceFactorsRequest{
								Cpu: &types.CpuPerformanceFactorRequest{
									References: []types.PerformanceFactorReferenceRequest{
										{InstanceFamily: aws.String("m5")},
										{InstanceFamily: aws.String("c6g")},
									},
								},
							},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	got, err := client.DescribeAutoScalingGroups(t.Context(), &assdk.DescribeAutoScalingGroupsInput{
		AutoScalingGroupNames: []string{"baseline-perf-asg"},
	})
	require.NoError(t, err)
	require.Len(t, got.AutoScalingGroups, 1)

	mip := got.AutoScalingGroups[0].MixedInstancesPolicy
	require.NotNil(t, mip)
	require.NotNil(t, mip.LaunchTemplate)
	require.Len(t, mip.LaunchTemplate.Overrides, 1)

	ir := mip.LaunchTemplate.Overrides[0].InstanceRequirements
	require.NotNil(t, ir)
	require.NotNil(t, ir.BaselinePerformanceFactors)
	require.NotNil(t, ir.BaselinePerformanceFactors.Cpu)
	require.Len(t, ir.BaselinePerformanceFactors.Cpu.References, 2)
	assert.Equal(t, "m5", aws.ToString(ir.BaselinePerformanceFactors.Cpu.References[0].InstanceFamily))
	assert.Equal(t, "c6g", aws.ToString(ir.BaselinePerformanceFactors.Cpu.References[1].InstanceFamily))
}
