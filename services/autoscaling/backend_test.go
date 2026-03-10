package autoscaling_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/autoscaling"
)

func TestInMemoryBackend_AutoScalingGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend)
		run     func(t *testing.T, b *autoscaling.InMemoryBackend)
		name    string
		wantErr bool
	}{
		{
			name: "create_group",
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				g, err := b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "my-asg",
					MinSize:              1,
					MaxSize:              5,
					DesiredCapacity:      2,
					AvailabilityZones:    []string{"us-east-1a"},
				})
				require.NoError(t, err)
				assert.Equal(t, "my-asg", g.AutoScalingGroupName)
				assert.Equal(t, int32(1), g.MinSize)
				assert.Equal(t, int32(5), g.MaxSize)
				assert.Equal(t, int32(2), g.DesiredCapacity)
				assert.Equal(t, "EC2", g.HealthCheckType)
				assert.NotEmpty(t, g.AutoScalingGroupARN)
			},
		},
		{
			name: "create_group_duplicate",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "dup-asg",
					MinSize:              1,
					MaxSize:              3,
				})
			},
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "dup-asg",
					MinSize:              1,
					MaxSize:              3,
				})
				require.Error(t, err)
			},
		},
		{
			name: "describe_all_groups",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "asg-1",
					MinSize:              1,
					MaxSize:              3,
				})
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "asg-2",
					MinSize:              2,
					MaxSize:              6,
				})
			},
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				groups, err := b.DescribeAutoScalingGroups(nil)
				require.NoError(t, err)
				require.Len(t, groups, 2)
				// sorted alphabetically
				assert.Equal(t, "asg-1", groups[0].AutoScalingGroupName)
				assert.Equal(t, "asg-2", groups[1].AutoScalingGroupName)
			},
		},
		{
			name: "describe_specific_group",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "specific-asg",
					MinSize:              1,
					MaxSize:              3,
				})
			},
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				groups, err := b.DescribeAutoScalingGroups([]string{"specific-asg"})
				require.NoError(t, err)
				require.Len(t, groups, 1)
				assert.Equal(t, "specific-asg", groups[0].AutoScalingGroupName)
			},
		},
		{
			name: "describe_nonexistent_group",
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				_, err := b.DescribeAutoScalingGroups([]string{"no-such-asg"})
				require.Error(t, err)
			},
		},
		{
			name: "update_group",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "update-asg",
					MinSize:              1,
					MaxSize:              5,
					DesiredCapacity:      2,
				})
			},
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				newMax := int32(10)
				g, err := b.UpdateAutoScalingGroup(autoscaling.UpdateAutoScalingGroupInput{
					AutoScalingGroupName: "update-asg",
					MaxSize:              &newMax,
				})
				require.NoError(t, err)
				assert.Equal(t, int32(10), g.MaxSize)
				assert.Equal(t, int32(1), g.MinSize) // unchanged
			},
		},
		{
			name: "delete_group",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "del-asg",
					MinSize:              1,
					MaxSize:              3,
				})
			},
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				err := b.DeleteAutoScalingGroup("del-asg")
				require.NoError(t, err)

				groups, err := b.DescribeAutoScalingGroups(nil)
				require.NoError(t, err)
				assert.Empty(t, groups)
			},
		},
		{
			name: "delete_nonexistent_group",
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				err := b.DeleteAutoScalingGroup("no-such-asg")
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			tt.run(t, b)
		})
	}
}

func TestInMemoryBackend_LaunchConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(b *autoscaling.InMemoryBackend)
		run   func(t *testing.T, b *autoscaling.InMemoryBackend)
		name  string
	}{
		{
			name: "create_launch_configuration",
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				lc, err := b.CreateLaunchConfiguration(autoscaling.CreateLaunchConfigurationInput{
					LaunchConfigurationName: "my-lc",
					ImageID:                 "ami-12345678",
					InstanceType:            "t2.micro",
				})
				require.NoError(t, err)
				assert.Equal(t, "my-lc", lc.LaunchConfigurationName)
				assert.Equal(t, "ami-12345678", lc.ImageID)
				assert.Equal(t, "t2.micro", lc.InstanceType)
				assert.NotEmpty(t, lc.LaunchConfigurationARN)
			},
		},
		{
			name: "create_launch_configuration_duplicate",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateLaunchConfiguration(autoscaling.CreateLaunchConfigurationInput{
					LaunchConfigurationName: "dup-lc",
					ImageID:                 "ami-12345678",
					InstanceType:            "t2.micro",
				})
			},
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateLaunchConfiguration(autoscaling.CreateLaunchConfigurationInput{
					LaunchConfigurationName: "dup-lc",
					ImageID:                 "ami-12345678",
					InstanceType:            "t2.micro",
				})
				require.Error(t, err)
			},
		},
		{
			name: "describe_launch_configurations",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateLaunchConfiguration(autoscaling.CreateLaunchConfigurationInput{
					LaunchConfigurationName: "lc-1",
					ImageID:                 "ami-aaa",
					InstanceType:            "t2.micro",
				})
				_, _ = b.CreateLaunchConfiguration(autoscaling.CreateLaunchConfigurationInput{
					LaunchConfigurationName: "lc-2",
					ImageID:                 "ami-bbb",
					InstanceType:            "t2.small",
				})
			},
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				lcs, err := b.DescribeLaunchConfigurations(nil)
				require.NoError(t, err)
				require.Len(t, lcs, 2)
				assert.Equal(t, "lc-1", lcs[0].LaunchConfigurationName)
				assert.Equal(t, "lc-2", lcs[1].LaunchConfigurationName)
			},
		},
		{
			name: "delete_launch_configuration",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateLaunchConfiguration(autoscaling.CreateLaunchConfigurationInput{
					LaunchConfigurationName: "del-lc",
					ImageID:                 "ami-12345678",
					InstanceType:            "t2.micro",
				})
			},
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				err := b.DeleteLaunchConfiguration("del-lc")
				require.NoError(t, err)

				lcs, err := b.DescribeLaunchConfigurations(nil)
				require.NoError(t, err)
				assert.Empty(t, lcs)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			tt.run(t, b)
		})
	}
}

func TestInMemoryBackend_ScalingActivities(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(b *autoscaling.InMemoryBackend)
		run   func(t *testing.T, b *autoscaling.InMemoryBackend)
		name  string
	}{
		{
			name: "describe_activities_after_create",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "act-asg",
					MinSize:              1,
					MaxSize:              3,
				})
			},
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				acts, err := b.DescribeScalingActivities("act-asg")
				require.NoError(t, err)
				require.NotEmpty(t, acts)
				assert.Equal(t, "act-asg", acts[0].AutoScalingGroupName)
				assert.Equal(t, "Successful", acts[0].StatusCode)
			},
		},
		{
			name: "describe_activities_nonexistent_group",
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				_, err := b.DescribeScalingActivities("no-such")
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			tt.run(t, b)
		})
	}
}

func TestInMemoryBackend_Persistence(t *testing.T) {
	t.Parallel()

	t.Run("snapshot_and_restore", func(t *testing.T) {
		t.Parallel()

		b := autoscaling.NewInMemoryBackend()
		_, err := b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
			AutoScalingGroupName: "persist-asg",
			MinSize:              1,
			MaxSize:              5,
		})
		require.NoError(t, err)

		_, err = b.CreateLaunchConfiguration(autoscaling.CreateLaunchConfigurationInput{
			LaunchConfigurationName: "persist-lc",
			ImageID:                 "ami-12345678",
			InstanceType:            "t2.micro",
		})
		require.NoError(t, err)

		data := b.Snapshot()
		require.NotNil(t, data)

		b2 := autoscaling.NewInMemoryBackend()
		err = b2.Restore(data)
		require.NoError(t, err)

		groups, err := b2.DescribeAutoScalingGroups(nil)
		require.NoError(t, err)
		require.Len(t, groups, 1)
		assert.Equal(t, "persist-asg", groups[0].AutoScalingGroupName)

		lcs, err := b2.DescribeLaunchConfigurations(nil)
		require.NoError(t, err)
		require.Len(t, lcs, 1)
		assert.Equal(t, "persist-lc", lcs[0].LaunchConfigurationName)
	})
}
