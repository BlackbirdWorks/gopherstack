package applicationautoscaling_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	aassdk "github.com/aws/aws-sdk-go-v2/service/applicationautoscaling"
	aastypes "github.com/aws/aws-sdk-go-v2/service/applicationautoscaling/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/applicationautoscaling"
)

// Documented MaxResults default/max: targets 50 (api_op_DescribeScalableTargets.go:40),
// scheduled actions 50 (:46), policies 10 (api_op_DescribeScalingPolicies.go:47).
func TestDescribePageSizeDefaults(t *testing.T) {
	t.Parallel()

	const resource = "service/default/page-svc"

	tests := []struct {
		maxRes   *int32
		name     string
		kind     string
		wantLen  int
		total    int
		wantNext bool
	}{
		{name: "targets default", kind: "targets", total: 60, wantLen: 50, wantNext: true},
		{
			name:     "targets over max clamps",
			kind:     "targets",
			total:    60,
			maxRes:   aws.Int32(100),
			wantLen:  50,
			wantNext: true,
		},
		{name: "targets explicit", kind: "targets", total: 60, maxRes: aws.Int32(7), wantLen: 7, wantNext: true},
		{name: "targets under default", kind: "targets", total: 5, wantLen: 5},
		{name: "scheduled default", kind: "scheduled", total: 60, wantLen: 50, wantNext: true},
		{
			name:     "scheduled over max clamps",
			kind:     "scheduled",
			total:    60,
			maxRes:   aws.Int32(100),
			wantLen:  50,
			wantNext: true,
		},
		{name: "policies default", kind: "policies", total: 12, wantLen: 10, wantNext: true},
		{
			name:     "policies over max clamps",
			kind:     "policies",
			total:    12,
			maxRes:   aws.Int32(50),
			wantLen:  10,
			wantNext: true,
		},
		{name: "policies under default", kind: "policies", total: 3, wantLen: 3},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestAASSDKClient(t, applicationautoscaling.NewHandler(
				applicationautoscaling.NewInMemoryBackend("123456789012", "us-east-1")))
			ctx := t.Context()

			register := func(id string) {
				_, err := client.RegisterScalableTarget(ctx, &aassdk.RegisterScalableTargetInput{
					ServiceNamespace:  aastypes.ServiceNamespaceEcs,
					ResourceId:        aws.String(id),
					ScalableDimension: aastypes.ScalableDimensionECSServiceDesiredCount,
					MinCapacity:       aws.Int32(1),
					MaxCapacity:       aws.Int32(10),
				})
				require.NoError(t, err)
			}

			var gotLen int
			var gotNext *string

			switch tc.kind {
			case "targets":
				for i := range tc.total {
					register(fmt.Sprintf("service/default/svc-%03d", i))
				}
				out, err := client.DescribeScalableTargets(ctx, &aassdk.DescribeScalableTargetsInput{
					ServiceNamespace: aastypes.ServiceNamespaceEcs, MaxResults: tc.maxRes,
				})
				require.NoError(t, err)
				gotLen, gotNext = len(out.ScalableTargets), out.NextToken
			case "scheduled":
				register(resource)
				for i := range tc.total {
					_, err := client.PutScheduledAction(ctx, &aassdk.PutScheduledActionInput{
						ServiceNamespace:    aastypes.ServiceNamespaceEcs,
						ResourceId:          aws.String(resource),
						ScalableDimension:   aastypes.ScalableDimensionECSServiceDesiredCount,
						ScheduledActionName: aws.String(fmt.Sprintf("act-%03d", i)),
						Schedule:            aws.String("rate(1 hour)"),
						StartTime:           aws.Time(time.Now().Add(time.Hour)),
						ScalableTargetAction: &aastypes.ScalableTargetAction{
							MinCapacity: aws.Int32(2),
							MaxCapacity: aws.Int32(5),
						},
					})
					require.NoError(t, err)
				}
				out, err := client.DescribeScheduledActions(ctx, &aassdk.DescribeScheduledActionsInput{
					ServiceNamespace: aastypes.ServiceNamespaceEcs, MaxResults: tc.maxRes,
				})
				require.NoError(t, err)
				gotLen, gotNext = len(out.ScheduledActions), out.NextToken
			case "policies":
				register(resource)
				for i := range tc.total {
					_, err := client.PutScalingPolicy(ctx, &aassdk.PutScalingPolicyInput{
						ServiceNamespace:  aastypes.ServiceNamespaceEcs,
						ResourceId:        aws.String(resource),
						ScalableDimension: aastypes.ScalableDimensionECSServiceDesiredCount,
						PolicyName:        aws.String(fmt.Sprintf("pol-%03d", i)),
						PolicyType:        aastypes.PolicyTypeTargetTrackingScaling,
						TargetTrackingScalingPolicyConfiguration: &aastypes.TargetTrackingScalingPolicyConfiguration{
							TargetValue: aws.Float64(70),
							PredefinedMetricSpecification: &aastypes.PredefinedMetricSpecification{
								PredefinedMetricType: aastypes.MetricTypeECSServiceAverageCPUUtilization,
							},
						},
					})
					require.NoError(t, err)
				}
				out, err := client.DescribeScalingPolicies(ctx, &aassdk.DescribeScalingPoliciesInput{
					ServiceNamespace: aastypes.ServiceNamespaceEcs, MaxResults: tc.maxRes,
				})
				require.NoError(t, err)
				gotLen, gotNext = len(out.ScalingPolicies), out.NextToken
			}

			assert.Equal(t, tc.wantLen, gotLen)

			if tc.wantNext {
				assert.NotEmpty(t, aws.ToString(gotNext))
			} else {
				assert.Empty(t, aws.ToString(gotNext))
			}
		})
	}
}
