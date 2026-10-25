package applicationautoscaling_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	aassdk "github.com/aws/aws-sdk-go-v2/service/applicationautoscaling"
	aastypes "github.com/aws/aws-sdk-go-v2/service/applicationautoscaling/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/applicationautoscaling"
	cwbackend "github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

type fakeCloudWatchSibling struct{ h service.Registerable }

func (f *fakeCloudWatchSibling) GetCloudWatchHandler() service.Registerable { return f.h }

func TestScalingPolicy_AlarmsFromCloudWatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		policyType aastypes.PolicyType
		wantAlarms []string
		wired      bool
	}{
		{
			name: "step_scaling_references", policyType: aastypes.PolicyTypeStepScaling,
			wired: true, wantAlarms: []string{"alarm-a"},
		},
		{name: "target_tracking_untouched", policyType: aastypes.PolicyTypeTargetTrackingScaling, wired: true},
		{name: "cloudwatch_not_wired", policyType: aastypes.PolicyTypeStepScaling},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cwBk := cwbackend.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			bk := applicationautoscaling.NewInMemoryBackend("123456789012", "us-east-1")

			if tt.wired {
				bk.SetAppConfig(&fakeCloudWatchSibling{h: cwbackend.NewHandler(cwBk)})
			}

			client := newTestAASSDKClient(t, applicationautoscaling.NewHandler(bk))

			_, err := client.RegisterScalableTarget(t.Context(), &aassdk.RegisterScalableTargetInput{
				ServiceNamespace:  aastypes.ServiceNamespaceEcs,
				ResourceId:        aws.String("service/c/s"),
				ScalableDimension: aastypes.ScalableDimensionECSServiceDesiredCount,
				MinCapacity:       aws.Int32(1),
				MaxCapacity:       aws.Int32(5),
			})
			require.NoError(t, err)

			put := func(name string) string {
				in := &aassdk.PutScalingPolicyInput{
					ServiceNamespace:  aastypes.ServiceNamespaceEcs,
					ResourceId:        aws.String("service/c/s"),
					ScalableDimension: aastypes.ScalableDimensionECSServiceDesiredCount,
					PolicyName:        aws.String(name),
					PolicyType:        tt.policyType,
				}
				if tt.policyType == aastypes.PolicyTypeStepScaling {
					in.StepScalingPolicyConfiguration = &aastypes.StepScalingPolicyConfiguration{
						AdjustmentType: aastypes.AdjustmentTypeChangeInCapacity,
						StepAdjustments: []aastypes.StepAdjustment{
							{ScalingAdjustment: aws.Int32(1), MetricIntervalLowerBound: aws.Float64(0)},
						},
					}
				} else {
					in.TargetTrackingScalingPolicyConfiguration = &aastypes.TargetTrackingScalingPolicyConfiguration{
						TargetValue: aws.Float64(50),
						PredefinedMetricSpecification: &aastypes.PredefinedMetricSpecification{
							PredefinedMetricType: aastypes.MetricTypeECSServiceAverageCPUUtilization,
						},
					}
				}

				out, perr := client.PutScalingPolicy(t.Context(), in)
				require.NoError(t, perr)

				return aws.ToString(out.PolicyARN)
			}

			p1 := put("p1")
			p10 := put("p10")

			if tt.wired {
				for name, actions := range map[string][]string{"alarm-a": {p1}, "alarm-b": {p10}, "alarm-c": nil} {
					require.NoError(t, cwBk.PutMetricAlarm(&cwbackend.MetricAlarm{
						AlarmName: name, Namespace: "AWS/ECS", MetricName: "CPUUtilization",
						Statistic: "Average", ComparisonOperator: "GreaterThanThreshold",
						Threshold: 1, EvaluationPeriods: 1, Period: 60, AlarmActions: actions,
					}))
				}
			}

			out, err := client.DescribeScalingPolicies(t.Context(), &aassdk.DescribeScalingPoliciesInput{
				ServiceNamespace: aastypes.ServiceNamespaceEcs,
				PolicyNames:      []string{"p1"},
			})
			require.NoError(t, err)
			require.Len(t, out.ScalingPolicies, 1)

			var got []string
			for _, a := range out.ScalingPolicies[0].Alarms {
				got = append(got, aws.ToString(a.AlarmName))
				assert.Contains(t, aws.ToString(a.AlarmARN), ":alarm:"+aws.ToString(a.AlarmName))
			}

			assert.Equal(t, tt.wantAlarms, got)
		})
	}
}
