package autoscaling_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/autoscaling"
)

func TestInMemoryBackend_ARNFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, b *autoscaling.InMemoryBackend)
		name string
	}{
		{
			name: "asg_arn_uses_config_region_account",
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				g, err := b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "arn-asg",
					MinSize:              1,
					MaxSize:              3,
				})
				require.NoError(t, err)
				assert.Contains(t, g.AutoScalingGroupARN, "arn:aws:autoscaling:us-east-1:000000000000")
			},
		},
		{
			name: "lc_arn_uses_config_region_account",
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				lcs, err := b.CreateLaunchConfiguration(autoscaling.CreateLaunchConfigurationInput{
					LaunchConfigurationName: "arn-lc",
					ImageID:                 "ami-1234",
					InstanceType:            "t2.micro",
				})
				require.NoError(t, err)
				assert.Contains(t, lcs.LaunchConfigurationARN, "arn:aws:autoscaling:us-east-1:000000000000")
			},
		},
		{
			name: "policy_arn_uses_config_region_account",
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "arn-policy-asg",
					MinSize:              1,
					MaxSize:              3,
				})

				p, err := b.PutScalingPolicy(autoscaling.ScalingPolicyInput{
					PolicyName:           "my-policy",
					AutoScalingGroupName: "arn-policy-asg",
					PolicyType:           "TargetTrackingScaling",
				})
				require.NoError(t, err)
				assert.Contains(t, p.PolicyARN, "arn:aws:autoscaling:us-east-1:000000000000")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			tt.run(t, b)
		})
	}
}

func TestInMemoryBackend_DescribeMetaOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, b *autoscaling.InMemoryBackend)
		name string
	}{
		{
			name: "describe_adjustment_types",
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				types, err := b.DescribeAdjustmentTypes()
				require.NoError(t, err)
				assert.Contains(t, types, "ChangeInCapacity")
				assert.Contains(t, types, "ExactCapacity")
				assert.Contains(t, types, "PercentChangeInCapacity")
			},
		},
		{
			name: "describe_notification_types",
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				types, err := b.DescribeAutoScalingNotificationTypes()
				require.NoError(t, err)
				assert.NotEmpty(t, types)
				assert.Contains(t, types, "autoscaling:EC2_INSTANCE_LAUNCH")
			},
		},
		{
			name: "describe_lifecycle_hook_types",
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				types, err := b.DescribeLifecycleHookTypes()
				require.NoError(t, err)
				assert.Len(t, types, 2)
			},
		},
		{
			name: "describe_metric_collection_types",
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				types, err := b.DescribeMetricCollectionTypes()
				require.NoError(t, err)
				assert.NotEmpty(t, types)
			},
		},
		{
			name: "describe_scaling_process_types",
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				types, err := b.DescribeScalingProcessTypes()
				require.NoError(t, err)
				assert.Contains(t, types, "Launch")
				assert.Contains(t, types, "Terminate")
			},
		},
		{
			name: "describe_termination_policy_types",
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				types, err := b.DescribeTerminationPolicyTypes()
				require.NoError(t, err)
				assert.Contains(t, types, "Default")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			tt.run(t, b)
		})
	}
}

// TestInMemoryBackend_Purge exercises the Purge method via the Handler HTTP endpoint.
func TestInMemoryBackend_Purge(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, b *autoscaling.InMemoryBackend)
		name string
	}{
		{
			name: "purge_clears_all_state",
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "purge-asg",
					MinSize:              1,
					MaxSize:              5,
				})
				require.NoError(t, err)

				b.Purge(context.Background(), time.Now().Add(time.Hour))

				groups, err := b.DescribeAutoScalingGroups(nil)
				require.NoError(t, err)
				assert.Empty(t, groups)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			tt.run(t, b)
		})
	}
}
