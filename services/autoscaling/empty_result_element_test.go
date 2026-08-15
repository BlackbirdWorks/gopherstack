package autoscaling_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	assdk "github.com/aws/aws-sdk-go-v2/service/autoscaling"
	"github.com/aws/aws-sdk-go-v2/service/autoscaling/types"
	"github.com/stretchr/testify/require"
)

// TestEmptyResultElement_RealClient covers every autoscaling op whose real output
// shape has zero members but whose deserializer still calls
// decoder.GetElement("<Op>Result") (autoscaling@v1.70.4 deserializers.go, confirmed
// per-op). gopherstack omitted the element on all thirteen, so every real SDK client
// failed deserialization with "deserialization failed: failed to decode response
// body ... node not found" even though the backend mutation succeeded. The assertion
// is exactly that the call deserializes without error -- there is nothing else to
// check on an empty output.
func TestEmptyResultElement_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call func(t *testing.T, client *assdk.Client, groupName string) error
		name string
	}{
		{
			name: "attachloadbalancertargetgroups",
			call: func(t *testing.T, client *assdk.Client, groupName string) error {
				t.Helper()

				_, err := client.AttachLoadBalancerTargetGroups(
					t.Context(),
					&assdk.AttachLoadBalancerTargetGroupsInput{
						AutoScalingGroupName: aws.String(groupName),
						TargetGroupARNs: []string{
							"arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/tg/abc",
						},
					},
				)

				return err
			},
		},
		{
			name: "attachloadbalancers",
			call: func(t *testing.T, client *assdk.Client, groupName string) error {
				t.Helper()

				_, err := client.AttachLoadBalancers(t.Context(), &assdk.AttachLoadBalancersInput{
					AutoScalingGroupName: aws.String(groupName),
					LoadBalancerNames:    []string{"classic-lb"},
				})

				return err
			},
		},
		{
			name: "attachtrafficsources",
			call: func(t *testing.T, client *assdk.Client, groupName string) error {
				t.Helper()

				_, err := client.AttachTrafficSources(t.Context(), &assdk.AttachTrafficSourcesInput{
					AutoScalingGroupName: aws.String(groupName),
					TrafficSources: []types.TrafficSourceIdentifier{
						{
							Identifier: aws.String(
								"arn:aws:vpc-lattice:us-east-1:123456789012:targetgroup/tg-abc",
							),
						},
					},
				})

				return err
			},
		},
		{
			name: "completelifecycleaction",
			call: func(t *testing.T, client *assdk.Client, groupName string) error {
				t.Helper()

				_, err := client.CompleteLifecycleAction(
					t.Context(),
					&assdk.CompleteLifecycleActionInput{
						AutoScalingGroupName:  aws.String(groupName),
						LifecycleHookName:     aws.String("empty-result-hook"),
						LifecycleActionResult: aws.String("CONTINUE"),
					},
				)

				return err
			},
		},
		{
			name: "deletelifecyclehook",
			call: func(t *testing.T, client *assdk.Client, groupName string) error {
				t.Helper()

				_, err := client.PutLifecycleHook(t.Context(), &assdk.PutLifecycleHookInput{
					AutoScalingGroupName: aws.String(groupName),
					LifecycleHookName:    aws.String("empty-result-delete-hook"),
				})
				require.NoError(t, err)

				_, err = client.DeleteLifecycleHook(t.Context(), &assdk.DeleteLifecycleHookInput{
					AutoScalingGroupName: aws.String(groupName),
					LifecycleHookName:    aws.String("empty-result-delete-hook"),
				})

				return err
			},
		},
		{
			name: "deletewarmpool",
			call: func(t *testing.T, client *assdk.Client, groupName string) error {
				t.Helper()

				_, err := client.PutWarmPool(t.Context(), &assdk.PutWarmPoolInput{
					AutoScalingGroupName: aws.String(groupName),
				})
				require.NoError(t, err)

				_, err = client.DeleteWarmPool(t.Context(), &assdk.DeleteWarmPoolInput{
					AutoScalingGroupName: aws.String(groupName),
				})

				return err
			},
		},
		{
			name: "detachloadbalancertargetgroups",
			call: func(t *testing.T, client *assdk.Client, groupName string) error {
				t.Helper()

				_, err := client.DetachLoadBalancerTargetGroups(
					t.Context(),
					&assdk.DetachLoadBalancerTargetGroupsInput{
						AutoScalingGroupName: aws.String(groupName),
						TargetGroupARNs: []string{
							"arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/tg/abc",
						},
					},
				)

				return err
			},
		},
		{
			name: "detachloadbalancers",
			call: func(t *testing.T, client *assdk.Client, groupName string) error {
				t.Helper()

				_, err := client.DetachLoadBalancers(t.Context(), &assdk.DetachLoadBalancersInput{
					AutoScalingGroupName: aws.String(groupName),
					LoadBalancerNames:    []string{"classic-lb"},
				})

				return err
			},
		},
		{
			name: "detachtrafficsources",
			call: func(t *testing.T, client *assdk.Client, groupName string) error {
				t.Helper()

				_, err := client.DetachTrafficSources(t.Context(), &assdk.DetachTrafficSourcesInput{
					AutoScalingGroupName: aws.String(groupName),
					TrafficSources: []types.TrafficSourceIdentifier{
						{
							Identifier: aws.String(
								"arn:aws:vpc-lattice:us-east-1:123456789012:targetgroup/tg-abc",
							),
						},
					},
				})

				return err
			},
		},
		{
			name: "putlifecyclehook",
			call: func(t *testing.T, client *assdk.Client, groupName string) error {
				t.Helper()

				_, err := client.PutLifecycleHook(t.Context(), &assdk.PutLifecycleHookInput{
					AutoScalingGroupName: aws.String(groupName),
					LifecycleHookName:    aws.String("empty-result-put-hook"),
				})

				return err
			},
		},
		{
			name: "putwarmpool",
			call: func(t *testing.T, client *assdk.Client, groupName string) error {
				t.Helper()

				_, err := client.PutWarmPool(t.Context(), &assdk.PutWarmPoolInput{
					AutoScalingGroupName: aws.String(groupName),
				})

				return err
			},
		},
		{
			name: "recordlifecycleactionheartbeat",
			call: func(t *testing.T, client *assdk.Client, groupName string) error {
				t.Helper()

				_, err := client.PutLifecycleHook(t.Context(), &assdk.PutLifecycleHookInput{
					AutoScalingGroupName: aws.String(groupName),
					LifecycleHookName:    aws.String("empty-result-heartbeat-hook"),
				})
				require.NoError(t, err)

				_, err = client.RecordLifecycleActionHeartbeat(
					t.Context(),
					&assdk.RecordLifecycleActionHeartbeatInput{
						AutoScalingGroupName: aws.String(groupName),
						LifecycleHookName:    aws.String("empty-result-heartbeat-hook"),
					},
				)

				return err
			},
		},
		{
			name: "setinstanceprotection",
			call: func(t *testing.T, client *assdk.Client, groupName string) error {
				t.Helper()

				_, err := client.SetInstanceProtection(
					t.Context(),
					&assdk.SetInstanceProtectionInput{
						AutoScalingGroupName: aws.String(groupName),
						InstanceIds:          []string{"i-0123456789abcdef0"},
						ProtectedFromScaleIn: aws.Bool(true),
					},
				)

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)

			groupName := "empty-result-" + tt.name + "-asg"
			_, err := client.CreateAutoScalingGroup(t.Context(), &assdk.CreateAutoScalingGroupInput{
				AutoScalingGroupName: aws.String(groupName),
				MinSize:              aws.Int32(0),
				MaxSize:              aws.Int32(1),
				AvailabilityZones:    []string{"us-east-1a"},
			})
			require.NoError(t, err)

			require.NoError(t, tt.call(t, client, groupName))
		})
	}
}
