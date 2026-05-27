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

				// ForceDelete=true required because the group has instances.
				err := b.DeleteAutoScalingGroup("del-asg", true)
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

				err := b.DeleteAutoScalingGroup("no-such-asg", false)
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

func TestInMemoryBackend_AttachInstances(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend)
		name    string
		group   string
		ids     []string
		wantErr bool
		wantLen int
	}{
		{
			name: "attach_new_instances",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "g1",
					MinSize:              0,
					MaxSize:              5,
				})
			},
			group:   "g1",
			ids:     []string{"i-aaa", "i-bbb"},
			wantLen: 2,
		},
		{
			name:    "group_not_found",
			group:   "no-such",
			ids:     []string{"i-aaa"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			err := b.AttachInstances(tt.group, tt.ids)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			groups, err := b.DescribeAutoScalingGroups([]string{tt.group})
			require.NoError(t, err)
			assert.Len(t, groups[0].Instances, tt.wantLen)
		})
	}
}

func TestInMemoryBackend_AttachLoadBalancerTargetGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(b *autoscaling.InMemoryBackend)
		name        string
		group       string
		arns        []string
		wantErr     bool
		wantARNsLen int
	}{
		{
			name: "attach_new_tgs",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "tg-g",
					MinSize:              0,
					MaxSize:              5,
				})
			},
			group:       "tg-g",
			arns:        []string{"arn:aws:tg/one", "arn:aws:tg/two"},
			wantARNsLen: 2,
		},
		{
			name:    "group_not_found",
			group:   "no-such",
			arns:    []string{"arn:aws:tg/one"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			err := b.AttachLoadBalancerTargetGroups(tt.group, tt.arns)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			groups, err := b.DescribeAutoScalingGroups([]string{tt.group})
			require.NoError(t, err)
			assert.Len(t, groups[0].TargetGroupARNs, tt.wantARNsLen)
		})
	}
}

func TestInMemoryBackend_AttachLoadBalancers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend)
		name    string
		group   string
		lbNames []string
		wantErr bool
		wantLen int
	}{
		{
			name: "attach_new_lbs",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "lb-g",
					MinSize:              0,
					MaxSize:              5,
				})
			},
			group:   "lb-g",
			lbNames: []string{"elb-1", "elb-2"},
			wantLen: 2,
		},
		{
			name:    "group_not_found",
			group:   "no-such",
			lbNames: []string{"elb-1"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			err := b.AttachLoadBalancers(tt.group, tt.lbNames)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			groups, err := b.DescribeAutoScalingGroups([]string{tt.group})
			require.NoError(t, err)
			assert.Len(t, groups[0].LoadBalancerNames, tt.wantLen)
		})
	}
}

func TestInMemoryBackend_AttachTrafficSources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend)
		name    string
		group   string
		tss     []autoscaling.TrafficSource
		wantErr bool
		wantLen int
	}{
		{
			name: "attach_traffic_sources",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "ts-g",
					MinSize:              0,
					MaxSize:              5,
				})
			},
			group: "ts-g",
			tss: []autoscaling.TrafficSource{
				{Identifier: "arn:aws:vpc-lattice:us-east-1:123:tg/abc", Type: "vpc-lattice"},
			},
			wantLen: 1,
		},
		{
			name:    "group_not_found",
			group:   "no-such",
			tss:     []autoscaling.TrafficSource{{Identifier: "x", Type: "y"}},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			err := b.AttachTrafficSources(tt.group, tt.tss)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			groups, err := b.DescribeAutoScalingGroups([]string{tt.group})
			require.NoError(t, err)
			assert.Len(t, groups[0].TrafficSources, tt.wantLen)
		})
	}
}

func TestInMemoryBackend_BatchScheduledActions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend)
		run     func(t *testing.T, b *autoscaling.InMemoryBackend)
		name    string
		wantErr bool
	}{
		{
			name: "put_then_delete",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "sa-g",
					MinSize:              0,
					MaxSize:              5,
				})
			},
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				desired := int32(3)
				failed, err := b.BatchPutScheduledUpdateGroupAction("sa-g", []autoscaling.ScheduledUpdateGroupAction{
					{ScheduledActionName: "scale-up", DesiredCapacity: &desired},
				})
				require.NoError(t, err)
				assert.Empty(t, failed)

				failedDel, err := b.BatchDeleteScheduledAction("sa-g", []string{"scale-up"})
				require.NoError(t, err)
				assert.Empty(t, failedDel)
			},
		},
		{
			name: "delete_nonexistent_action_reports_failure",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "sa-empty",
					MinSize:              0,
					MaxSize:              5,
				})
			},
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				failed, err := b.BatchDeleteScheduledAction("sa-empty", []string{"ghost"})
				require.NoError(t, err)
				require.Len(t, failed, 1)
				assert.Equal(t, "ghost", failed[0].ScheduledActionName)
			},
		},
		{
			name: "put_missing_name_reports_failure",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "sa-badname",
					MinSize:              0,
					MaxSize:              5,
				})
			},
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				failed, err := b.BatchPutScheduledUpdateGroupAction("sa-badname",
					[]autoscaling.ScheduledUpdateGroupAction{{ScheduledActionName: ""}})
				require.NoError(t, err)
				require.Len(t, failed, 1)
			},
		},
		{
			name: "put_group_not_found",
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				_, err := b.BatchPutScheduledUpdateGroupAction("no-such", nil)
				require.Error(t, err)
			},
		},
		{
			name: "delete_group_not_found",
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				_, err := b.BatchDeleteScheduledAction("no-such", []string{"a"})
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

			if tt.run != nil {
				tt.run(t, b)
			}
		})
	}
}

func TestInMemoryBackend_CancelInstanceRefresh(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend)
		name    string
		group   string
		wantID  string
		wantErr bool
	}{
		{
			name: "cancel_active_refresh",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "refresh-g",
					MinSize:              0,
					MaxSize:              5,
				})
				_ = b.AddInstanceRefresh(autoscaling.InstanceRefresh{
					InstanceRefreshID:    "irs-abc",
					AutoScalingGroupName: "refresh-g",
					Status:               "InProgress",
				})
			},
			group:  "refresh-g",
			wantID: "irs-abc",
		},
		{
			name: "no_active_refresh",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "no-refresh-g",
					MinSize:              0,
					MaxSize:              5,
				})
			},
			group:   "no-refresh-g",
			wantErr: true,
		},
		{
			name:    "group_not_found",
			group:   "no-such",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			id, err := b.CancelInstanceRefresh(tt.group)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantID, id)
		})
	}
}

func TestInMemoryBackend_CompleteLifecycleAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend)
		name    string
		input   autoscaling.CompleteLifecycleActionInput
		wantErr bool
	}{
		{
			name: "complete_success",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "lca-g",
					MinSize:              0,
					MaxSize:              5,
				})
			},
			input: autoscaling.CompleteLifecycleActionInput{
				AutoScalingGroupName:  "lca-g",
				LifecycleHookName:     "my-hook",
				LifecycleActionToken:  "token-abc",
				LifecycleActionResult: "CONTINUE",
			},
		},
		{
			name: "group_not_found",
			input: autoscaling.CompleteLifecycleActionInput{
				AutoScalingGroupName:  "no-such",
				LifecycleHookName:     "my-hook",
				LifecycleActionResult: "CONTINUE",
			},
			wantErr: true,
		},
		{
			name: "missing_hook_name",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "lca-nohook",
					MinSize:              0,
					MaxSize:              5,
				})
			},
			input: autoscaling.CompleteLifecycleActionInput{
				AutoScalingGroupName:  "lca-nohook",
				LifecycleActionResult: "CONTINUE",
			},
			wantErr: true,
		},
		{
			name: "missing_result",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "lca-noresult",
					MinSize:              0,
					MaxSize:              5,
				})
			},
			input: autoscaling.CompleteLifecycleActionInput{
				AutoScalingGroupName: "lca-noresult",
				LifecycleHookName:    "my-hook",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			err := b.CompleteLifecycleAction(tt.input)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestInMemoryBackend_CreateOrUpdateTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend)
		wantTag autoscaling.Tag
		name    string
		tags    []autoscaling.ResourceTag
		wantErr bool
	}{
		{
			name: "create_tag",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "tag-g",
					MinSize:              0,
					MaxSize:              5,
				})
			},
			tags: []autoscaling.ResourceTag{
				{ResourceID: "tag-g", ResourceType: "auto-scaling-group", Key: "env", Value: "prod"},
			},
			wantTag: autoscaling.Tag{Key: "env", Value: "prod"},
		},
		{
			name: "update_existing_tag",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "upd-tag-g",
					MinSize:              0,
					MaxSize:              5,
					Tags:                 []autoscaling.Tag{{Key: "env", Value: "dev"}},
				})
			},
			tags: []autoscaling.ResourceTag{
				{ResourceID: "upd-tag-g", ResourceType: "auto-scaling-group", Key: "env", Value: "prod"},
			},
			wantTag: autoscaling.Tag{Key: "env", Value: "prod"},
		},
		{
			name: "group_not_found",
			tags: []autoscaling.ResourceTag{
				{ResourceID: "no-such", ResourceType: "auto-scaling-group", Key: "k", Value: "v"},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			err := b.CreateOrUpdateTags(tt.tags)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			if tt.wantTag.Key != "" {
				groups, gErr := b.DescribeAutoScalingGroups([]string{tt.tags[0].ResourceID})
				require.NoError(t, gErr)
				found := false
				for _, tag := range groups[0].Tags {
					if tag.Key == tt.wantTag.Key {
						assert.Equal(t, tt.wantTag.Value, tag.Value)
						found = true

						break
					}
				}
				assert.True(t, found, "expected tag %q not found", tt.wantTag.Key)
			}
		})
	}
}

func TestInMemoryBackend_DeleteLifecycleHook(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(b *autoscaling.InMemoryBackend)
		name     string
		group    string
		hookName string
		wantErr  bool
	}{
		{
			name: "delete_existing_hook",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "hook-g",
					MinSize:              0,
					MaxSize:              5,
				})
				_ = b.AddLifecycleHook(autoscaling.LifecycleHook{
					LifecycleHookName:    "launch-hook",
					AutoScalingGroupName: "hook-g",
				})
			},
			group:    "hook-g",
			hookName: "launch-hook",
		},
		{
			name: "delete_nonexistent_hook",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "no-hook-g",
					MinSize:              0,
					MaxSize:              5,
				})
			},
			group:    "no-hook-g",
			hookName: "ghost",
			wantErr:  true,
		},
		{
			name:     "group_not_found",
			group:    "no-such",
			hookName: "my-hook",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			err := b.DeleteLifecycleHook(tt.group, tt.hookName)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestInMemoryBackend_SetDesiredCapacity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend)
		name    string
		group   string
		desired int32
		wantErr bool
	}{
		{
			name: "increase_capacity",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "sdc-asg",
					MinSize:              1,
					MaxSize:              10,
					DesiredCapacity:      2,
				})
			},
			group:   "sdc-asg",
			desired: 5,
		},
		{
			name: "below_min_returns_error",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "sdc-min-asg",
					MinSize:              3,
					MaxSize:              10,
					DesiredCapacity:      3,
				})
			},
			group:   "sdc-min-asg",
			desired: 1,
			wantErr: true,
		},
		{
			name:    "group_not_found",
			group:   "no-such",
			desired: 2,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			err := b.SetDesiredCapacity(tt.group, tt.desired)
			if tt.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)

			groups, err := b.DescribeAutoScalingGroups([]string{tt.group})
			require.NoError(t, err)
			assert.Equal(t, tt.desired, groups[0].DesiredCapacity)
			assert.Len(t, groups[0].Instances, int(tt.desired))
		})
	}
}

func TestInMemoryBackend_PutAndDescribeLifecycleHooks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(b *autoscaling.InMemoryBackend)
		name      string
		hook      autoscaling.LifecycleHook
		hookNames []string
		wantCount int
		wantErr   bool
	}{
		{
			name: "put_and_describe_hook",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "hook-asg",
					MinSize:              0,
					MaxSize:              5,
				})
			},
			hook: autoscaling.LifecycleHook{
				LifecycleHookName:    "my-hook",
				AutoScalingGroupName: "hook-asg",
				LifecycleTransition:  "autoscaling:EC2_INSTANCE_LAUNCHING",
			},
			wantCount: 1,
		},
		{
			name: "put_hook_group_not_found",
			hook: autoscaling.LifecycleHook{
				LifecycleHookName:    "h",
				AutoScalingGroupName: "no-such",
			},
			wantErr: true,
		},
		{
			name: "put_hook_name_required",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "hook-req-asg",
					MinSize:              0,
					MaxSize:              5,
				})
			},
			hook: autoscaling.LifecycleHook{
				AutoScalingGroupName: "hook-req-asg",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			err := b.PutLifecycleHook(tt.hook)
			if tt.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)

			hooks, err := b.DescribeLifecycleHooks(tt.hook.AutoScalingGroupName, nil)
			require.NoError(t, err)
			assert.Len(t, hooks, tt.wantCount)
		})
	}
}

func TestInMemoryBackend_DescribeScheduledActions(t *testing.T) {
	t.Parallel()

	b := autoscaling.NewInMemoryBackend()
	_, err := b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
		AutoScalingGroupName: "sa-asg",
		MinSize:              0,
		MaxSize:              10,
	})
	require.NoError(t, err)

	_, err = b.BatchPutScheduledUpdateGroupAction("sa-asg", []autoscaling.ScheduledUpdateGroupAction{
		{ScheduledActionName: "action-a"},
		{ScheduledActionName: "action-b"},
	})
	require.NoError(t, err)

	// All actions for the group
	actions, err := b.DescribeScheduledActions("sa-asg", nil)
	require.NoError(t, err)
	assert.Len(t, actions, 2)

	// Filter by name
	filtered, err := b.DescribeScheduledActions("sa-asg", []string{"action-a"})
	require.NoError(t, err)
	assert.Len(t, filtered, 1)
	assert.Equal(t, "action-a", filtered[0].ScheduledActionName)

	// Group not found
	_, err = b.DescribeScheduledActions("no-such", nil)
	require.Error(t, err)
}

func TestInMemoryBackend_DeleteAndDescribeTags(t *testing.T) {
	t.Parallel()

	b := autoscaling.NewInMemoryBackend()
	_, err := b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
		AutoScalingGroupName: "tag-asg",
		MinSize:              0,
		MaxSize:              5,
		Tags:                 []autoscaling.Tag{{Key: "env", Value: "prod"}, {Key: "team", Value: "platform"}},
	})
	require.NoError(t, err)

	// DescribeTags returns all tags
	tags, err := b.DescribeTags(nil)
	require.NoError(t, err)
	assert.Len(t, tags, 2)

	// DeleteTags removes the env tag
	err = b.DeleteTags([]autoscaling.ResourceTag{
		{ResourceID: "tag-asg", ResourceType: "auto-scaling-group", Key: "env"},
	})
	require.NoError(t, err)

	remaining, err := b.DescribeTags(nil)
	require.NoError(t, err)
	assert.Len(t, remaining, 1)
	assert.Equal(t, "team", remaining[0].Key)
}

func TestInMemoryBackend_DescribeAutoScalingInstances(t *testing.T) {
	t.Parallel()

	b := autoscaling.NewInMemoryBackend()
	_, err := b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
		AutoScalingGroupName: "inst-asg",
		MinSize:              1,
		MaxSize:              5,
		DesiredCapacity:      2,
	})
	require.NoError(t, err)

	// Returns all instances when no filter
	instances, err := b.DescribeAutoScalingInstances(nil)
	require.NoError(t, err)
	assert.Len(t, instances, 2)
	assert.Equal(t, "inst-asg", instances[0].AutoScalingGroupName)

	// Filter by instance ID
	id := instances[0].InstanceID
	filtered, err := b.DescribeAutoScalingInstances([]string{id})
	require.NoError(t, err)
	assert.Len(t, filtered, 1)
	assert.Equal(t, id, filtered[0].InstanceID)
}

func TestInMemoryBackend_TerminateInstanceInAutoScalingGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup           func(b *autoscaling.InMemoryBackend) string
		name            string
		shouldDecrement bool
		wantErr         bool
		wantInstances   int
	}{
		{
			name: "terminate_with_decrement",
			setup: func(b *autoscaling.InMemoryBackend) string {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "term-asg",
					MinSize:              2,
					MaxSize:              5,
					DesiredCapacity:      2,
				})

				instances, _ := b.DescribeAutoScalingInstances(nil)

				return instances[0].InstanceID
			},
			shouldDecrement: true,
			wantInstances:   1, // DesiredCapacity decremented to 1
		},
		{
			name: "terminate_without_decrement_replaces",
			setup: func(b *autoscaling.InMemoryBackend) string {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "term-asg2",
					MinSize:              2,
					MaxSize:              5,
					DesiredCapacity:      2,
				})

				instances, _ := b.DescribeAutoScalingInstances(nil)

				return instances[0].InstanceID
			},
			shouldDecrement: false,
			wantInstances:   2, // Replacement launched
		},
		{
			name: "instance_not_found",
			setup: func(_ *autoscaling.InMemoryBackend) string {
				return "i-notfound"
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			instanceID := tt.setup(b)

			activity, err := b.TerminateInstanceInAutoScalingGroup(instanceID, tt.shouldDecrement)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, activity)
			assert.Equal(t, "Successful", activity.StatusCode)

			instances, err := b.DescribeAutoScalingInstances(nil)
			require.NoError(t, err)
			assert.Len(t, instances, tt.wantInstances)
		})
	}
}

func TestInMemoryBackend_CapacityValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend)
		run     func(t *testing.T, b *autoscaling.InMemoryBackend)
		name    string
		wantErr bool
	}{
		{
			name: "create_desired_below_min",
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "v-asg",
					MinSize:              5,
					MaxSize:              10,
					DesiredCapacity:      2,
				})
				require.Error(t, err)
			},
		},
		{
			name: "create_desired_above_max",
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "v-asg2",
					MinSize:              1,
					MaxSize:              5,
					DesiredCapacity:      10,
				})
				require.Error(t, err)
			},
		},
		{
			name: "create_min_above_max",
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "v-asg3",
					MinSize:              10,
					MaxSize:              5,
				})
				require.Error(t, err)
			},
		},
		{
			name: "update_desired_below_min",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "u-asg",
					MinSize:              3,
					MaxSize:              10,
					DesiredCapacity:      3,
				})
			},
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()
				newDesired := int32(1)
				_, err := b.UpdateAutoScalingGroup(autoscaling.UpdateAutoScalingGroupInput{
					AutoScalingGroupName: "u-asg",
					DesiredCapacity:      &newDesired,
				})
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

func TestInMemoryBackend_LaunchTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(b *autoscaling.InMemoryBackend)
		run   func(t *testing.T, b *autoscaling.InMemoryBackend)
		name  string
	}{
		{
			name: "create_asg_with_launch_template",
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				lt := &autoscaling.LaunchTemplateSpecification{
					LaunchTemplateID:   "lt-0123456789abcdef0",
					LaunchTemplateName: "my-template",
					Version:            "$Latest",
				}

				g, err := b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "lt-asg",
					LaunchTemplate:       lt,
					MinSize:              1,
					MaxSize:              3,
				})
				require.NoError(t, err)
				require.NotNil(t, g.LaunchTemplate)
				assert.Equal(t, "lt-0123456789abcdef0", g.LaunchTemplate.LaunchTemplateID)
				assert.Equal(t, "my-template", g.LaunchTemplate.LaunchTemplateName)
				assert.Equal(t, "$Latest", g.LaunchTemplate.Version)
				assert.Empty(t, g.LaunchConfigurationName)
			},
		},
		{
			name: "update_asg_clears_lc_when_lt_set",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName:    "lt-update-asg",
					LaunchConfigurationName: "my-lc",
					MinSize:                 1,
					MaxSize:                 3,
				})
			},
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				lt := &autoscaling.LaunchTemplateSpecification{
					LaunchTemplateID: "lt-abc",
					Version:          "1",
				}

				g, err := b.UpdateAutoScalingGroup(autoscaling.UpdateAutoScalingGroupInput{
					AutoScalingGroupName: "lt-update-asg",
					LaunchTemplate:       lt,
				})
				require.NoError(t, err)
				require.NotNil(t, g.LaunchTemplate)
				assert.Equal(t, "lt-abc", g.LaunchTemplate.LaunchTemplateID)
				assert.Empty(t, g.LaunchConfigurationName)
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

func TestInMemoryBackend_VPCZoneIdentifier(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(b *autoscaling.InMemoryBackend)
		run   func(t *testing.T, b *autoscaling.InMemoryBackend)
		name  string
	}{
		{
			name: "create_with_vpc_zone_identifier",
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				g, err := b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "vpc-asg",
					VPCZoneIdentifier:    "subnet-aaa,subnet-bbb",
					MinSize:              1,
					MaxSize:              3,
				})
				require.NoError(t, err)
				assert.Equal(t, "subnet-aaa,subnet-bbb", g.VPCZoneIdentifier)
			},
		},
		{
			name: "update_vpc_zone_identifier",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "vpc-update-asg",
					MinSize:              1,
					MaxSize:              3,
				})
			},
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				g, err := b.UpdateAutoScalingGroup(autoscaling.UpdateAutoScalingGroupInput{
					AutoScalingGroupName: "vpc-update-asg",
					VPCZoneIdentifier:    "subnet-ccc",
				})
				require.NoError(t, err)
				assert.Equal(t, "subnet-ccc", g.VPCZoneIdentifier)
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

func TestInMemoryBackend_TerminationPolicies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend)
		run     func(t *testing.T, b *autoscaling.InMemoryBackend)
		name    string
		wantErr bool
	}{
		{
			name: "valid_termination_policies",
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				g, err := b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "tp-asg",
					TerminationPolicies:  []string{"OldestInstance", "Default"},
					MinSize:              1,
					MaxSize:              3,
				})
				require.NoError(t, err)
				assert.Equal(t, []string{"OldestInstance", "Default"}, g.TerminationPolicies)
			},
		},
		{
			name: "closest_to_next_instance_hour_valid",
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "closest-asg",
					TerminationPolicies:  []string{"ClosestToNextInstanceHour"},
					MinSize:              1,
					MaxSize:              3,
				})
				require.NoError(t, err)
			},
		},
		{
			name:    "invalid_termination_policy",
			wantErr: true,
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "bad-tp-asg",
					TerminationPolicies:  []string{"ClosestToNextInstanceHourPrice"},
					MinSize:              1,
					MaxSize:              3,
				})
				require.Error(t, err)
			},
		},
		{
			name:    "invalid_termination_policy_update",
			wantErr: true,
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "tp-update-asg",
					MinSize:              1,
					MaxSize:              3,
				})
			},
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				_, err := b.UpdateAutoScalingGroup(autoscaling.UpdateAutoScalingGroupInput{
					AutoScalingGroupName: "tp-update-asg",
					TerminationPolicies:  []string{"NotReal"},
				})
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

func TestInMemoryBackend_HealthCheckTypeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		healthCheckType string
		wantErr         bool
	}{
		{name: "ec2_valid", healthCheckType: "EC2", wantErr: false},
		{name: "elb_valid", healthCheckType: "ELB", wantErr: false},
		{name: "vpc_lattice_valid", healthCheckType: "VPC_LATTICE", wantErr: false},
		{name: "empty_defaults_ec2", healthCheckType: "", wantErr: false},
		{name: "invalid_type", healthCheckType: "UNKNOWN", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			_, err := b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
				AutoScalingGroupName: "hct-asg",
				HealthCheckType:      tt.healthCheckType,
				MinSize:              1,
				MaxSize:              3,
			})

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestInMemoryBackend_NewInstancesProtectedFromScaleIn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(b *autoscaling.InMemoryBackend)
		run   func(t *testing.T, b *autoscaling.InMemoryBackend)
		name  string
	}{
		{
			name: "new_instances_protected_propagates_to_initial_instances",
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				g, err := b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName:             "prot-asg",
					MinSize:                          2,
					MaxSize:                          5,
					DesiredCapacity:                  2,
					NewInstancesProtectedFromScaleIn: true,
				})
				require.NoError(t, err)
				assert.True(t, g.NewInstancesProtectedFromScaleIn)
				for _, inst := range g.Instances {
					assert.True(t, inst.ProtectedFromScaleIn, "instance %s should be protected", inst.InstanceID)
				}
			},
		},
		{
			name: "update_new_instances_protected",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "prot-update-asg",
					MinSize:              1,
					MaxSize:              3,
				})
			},
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				prot := true
				g, err := b.UpdateAutoScalingGroup(autoscaling.UpdateAutoScalingGroupInput{
					AutoScalingGroupName:             "prot-update-asg",
					NewInstancesProtectedFromScaleIn: &prot,
				})
				require.NoError(t, err)
				assert.True(t, g.NewInstancesProtectedFromScaleIn)
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

func TestInMemoryBackend_SuspendProcessesValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend)
		run     func(t *testing.T, b *autoscaling.InMemoryBackend)
		name    string
		wantErr bool
	}{
		{
			name: "valid_processes_succeed",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "sp-asg",
					MinSize:              1,
					MaxSize:              3,
				})
			},
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				err := b.SuspendProcesses("sp-asg", []string{"Launch", "Terminate", "HealthCheck"})
				require.NoError(t, err)

				groups, _ := b.DescribeAutoScalingGroups([]string{"sp-asg"})
				require.Len(t, groups, 1)
				assert.Contains(t, groups[0].SuspendedProcesses, "Launch")
				assert.Contains(t, groups[0].SuspendedProcesses, "Terminate")
			},
		},
		{
			name:    "unknown_process_returns_error",
			wantErr: true,
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "sp-bad-asg",
					MinSize:              1,
					MaxSize:              3,
				})
			},
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				err := b.SuspendProcesses("sp-bad-asg", []string{"Launch", "NotAProcess"})
				require.Error(t, err)
			},
		},
		{
			name: "all_valid_processes",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "sp-all-asg",
					MinSize:              1,
					MaxSize:              3,
				})
			},
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				allProcesses := []string{
					"Launch", "Terminate", "HealthCheck", "ReplaceUnhealthy",
					"AZRebalance", "AlarmNotification", "ScheduledActions",
					"AddToLoadBalancer", "InstanceRefresh",
				}
				err := b.SuspendProcesses("sp-all-asg", allProcesses)
				require.NoError(t, err)
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

func TestInMemoryBackend_SetInstanceHealthGracePeriod(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(b *autoscaling.InMemoryBackend)
		run   func(t *testing.T, b *autoscaling.InMemoryBackend)
		name  string
	}{
		{
			name: "unhealthy_mark_honored_after_grace_period",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName:   "sih-asg",
					MinSize:                1,
					MaxSize:                3,
					DesiredCapacity:        1,
					HealthCheckGracePeriod: 0, // no grace period
				})
			},
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				groups, _ := b.DescribeAutoScalingGroups([]string{"sih-asg"})
				require.Len(t, groups[0].Instances, 1)
				instID := groups[0].Instances[0].InstanceID

				err := b.SetInstanceHealth(instID, "Unhealthy", true)
				require.NoError(t, err)

				groups, _ = b.DescribeAutoScalingGroups([]string{"sih-asg"})
				assert.Equal(t, "Unhealthy", groups[0].Instances[0].HealthStatus)
			},
		},
		{
			name: "unhealthy_ignored_within_grace_period",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName:   "sih-grace-asg",
					MinSize:                1,
					MaxSize:                3,
					DesiredCapacity:        1,
					HealthCheckGracePeriod: 3600, // 1 hour grace
				})
			},
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				groups, _ := b.DescribeAutoScalingGroups([]string{"sih-grace-asg"})
				require.Len(t, groups[0].Instances, 1)
				instID := groups[0].Instances[0].InstanceID

				// LaunchTime is just now, so we're within grace period
				err := b.SetInstanceHealth(instID, "Unhealthy", true)
				require.NoError(t, err)

				groups, _ = b.DescribeAutoScalingGroups([]string{"sih-grace-asg"})
				// Should still be Healthy — grace period honored
				assert.Equal(t, "Healthy", groups[0].Instances[0].HealthStatus)
			},
		},
		{
			name: "respect_grace_period_false_always_marks",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName:   "sih-norespect-asg",
					MinSize:                1,
					MaxSize:                3,
					DesiredCapacity:        1,
					HealthCheckGracePeriod: 3600,
				})
			},
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				groups, _ := b.DescribeAutoScalingGroups([]string{"sih-norespect-asg"})
				instID := groups[0].Instances[0].InstanceID

				// false = don't respect grace period
				err := b.SetInstanceHealth(instID, "Unhealthy", false)
				require.NoError(t, err)

				groups, _ = b.DescribeAutoScalingGroups([]string{"sih-norespect-asg"})
				assert.Equal(t, "Unhealthy", groups[0].Instances[0].HealthStatus)
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

func TestInMemoryBackend_PutLifecycleHookValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend)
		name    string
		hook    autoscaling.LifecycleHook
		wantErr bool
	}{
		{
			name: "valid_hook",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "lh-asg",
					MinSize:              1,
					MaxSize:              3,
				})
			},
			hook: autoscaling.LifecycleHook{
				LifecycleHookName:    "my-hook",
				AutoScalingGroupName: "lh-asg",
				LifecycleTransition:  "autoscaling:EC2_INSTANCE_LAUNCHING",
				DefaultResult:        "CONTINUE",
				HeartbeatTimeout:     300,
			},
			wantErr: false,
		},
		{
			name: "default_heartbeat_timeout_applied",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "lh-default-asg",
					MinSize:              1,
					MaxSize:              3,
				})
			},
			hook: autoscaling.LifecycleHook{
				LifecycleHookName:    "my-hook",
				AutoScalingGroupName: "lh-default-asg",
				DefaultResult:        "ABANDON",
			},
			wantErr: false,
		},
		{
			name: "heartbeat_too_low",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "lh-low-asg",
					MinSize:              1,
					MaxSize:              3,
				})
			},
			hook: autoscaling.LifecycleHook{
				LifecycleHookName:    "my-hook",
				AutoScalingGroupName: "lh-low-asg",
				HeartbeatTimeout:     10, // below min of 30
			},
			wantErr: true,
		},
		{
			name: "heartbeat_too_high",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "lh-high-asg",
					MinSize:              1,
					MaxSize:              3,
				})
			},
			hook: autoscaling.LifecycleHook{
				LifecycleHookName:    "my-hook",
				AutoScalingGroupName: "lh-high-asg",
				HeartbeatTimeout:     200000, // above max of 172800
			},
			wantErr: true,
		},
		{
			name: "invalid_default_result",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "lh-dr-asg",
					MinSize:              1,
					MaxSize:              3,
				})
			},
			hook: autoscaling.LifecycleHook{
				LifecycleHookName:    "my-hook",
				AutoScalingGroupName: "lh-dr-asg",
				DefaultResult:        "FAIL",
				HeartbeatTimeout:     300,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			err := b.PutLifecycleHook(tt.hook)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

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

func TestInMemoryBackend_Persistence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(b *autoscaling.InMemoryBackend)
		check func(t *testing.T, b *autoscaling.InMemoryBackend)
		name  string
	}{
		{
			name: "snapshot_restore_preserves_scaling_policies",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "persist-asg",
					MinSize:              1,
					MaxSize:              3,
				})
				_, _ = b.PutScalingPolicy(autoscaling.ScalingPolicyInput{
					PolicyName:           "my-policy",
					AutoScalingGroupName: "persist-asg",
					PolicyType:           "SimpleScaling",
					AdjustmentType:       "ChangeInCapacity",
					ScalingAdjustment:    2,
				})
			},
			check: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				policies, err := b.DescribePolicies("persist-asg", nil)
				require.NoError(t, err)
				require.Len(t, policies, 1)
				assert.Equal(t, "my-policy", policies[0].PolicyName)
				assert.Equal(t, int32(2), policies[0].ScalingAdjustment)
			},
		},
		{
			name: "snapshot_restore_preserves_notification_configs",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "notif-asg",
					MinSize:              1,
					MaxSize:              3,
				})
				_ = b.PutNotificationConfiguration(
					"notif-asg",
					"arn:aws:sns:us-east-1:000000000000:my-topic",
					[]string{"autoscaling:EC2_INSTANCE_LAUNCH"},
				)
			},
			check: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				notifs, err := b.DescribeNotificationConfigurations([]string{"notif-asg"})
				require.NoError(t, err)
				require.Len(t, notifs, 1)
				assert.Equal(t, "autoscaling:EC2_INSTANCE_LAUNCH", notifs[0].NotificationType)
			},
		},
		{
			name: "snapshot_restore_preserves_warm_pools",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "warm-persist-asg",
					MinSize:              1,
					MaxSize:              3,
				})
				_ = b.PutWarmPool(autoscaling.WarmPoolInput{
					AutoScalingGroupName:     "warm-persist-asg",
					MinSize:                  2,
					MaxGroupPreparedCapacity: 5,
				})
			},
			check: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				wp, err := b.DescribeWarmPool("warm-persist-asg")
				require.NoError(t, err)
				assert.Equal(t, int32(2), wp.MinSize)
				assert.Equal(t, int32(5), wp.MaxGroupPreparedCapacity)
			},
		},
		{
			name: "snapshot_restore_rebuilds_instance_index",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "idx-asg",
					MinSize:              1,
					MaxSize:              3,
					DesiredCapacity:      1,
				})
			},
			check: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				groups, _ := b.DescribeAutoScalingGroups([]string{"idx-asg"})
				require.Len(t, groups[0].Instances, 1)

				instID := groups[0].Instances[0].InstanceID
				// TerminateInstanceInAutoScalingGroup uses instanceIndex — it should work post-restore.
				_, err := b.TerminateInstanceInAutoScalingGroup(instID, true)
				require.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b1 := autoscaling.NewInMemoryBackend()
			tt.setup(b1)

			snap := b1.Snapshot()
			require.NotNil(t, snap)

			b2 := autoscaling.NewInMemoryBackend()
			err := b2.Restore(snap)
			require.NoError(t, err)

			tt.check(t, b2)
		})
	}
}

func TestInMemoryBackend_InstanceIndex(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(b *autoscaling.InMemoryBackend)
		run   func(t *testing.T, b *autoscaling.InMemoryBackend)
		name  string
	}{
		{
			name: "terminate_uses_index_not_linear_scan",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "idx-term-asg",
					MinSize:              2,
					MaxSize:              5,
					DesiredCapacity:      2,
				})
			},
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				groups, _ := b.DescribeAutoScalingGroups([]string{"idx-term-asg"})
				instID := groups[0].Instances[0].InstanceID

				activity, err := b.TerminateInstanceInAutoScalingGroup(instID, true)
				require.NoError(t, err)
				assert.Contains(t, activity.Description, instID)
			},
		},
		{
			name: "terminate_unknown_instance_returns_error",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "idx-notfound-asg",
					MinSize:              1,
					MaxSize:              3,
				})
			},
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				_, err := b.TerminateInstanceInAutoScalingGroup("i-notexist", false)
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

func TestInMemoryBackend_WarmPoolInstanceReusePolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(b *autoscaling.InMemoryBackend)
		run   func(t *testing.T, b *autoscaling.InMemoryBackend)
		name  string
	}{
		{
			name: "reuse_on_scale_in_stored",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "wp-reuse-asg",
					MinSize:              1,
					MaxSize:              3,
				})
			},
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				err := b.PutWarmPool(autoscaling.WarmPoolInput{
					AutoScalingGroupName:     "wp-reuse-asg",
					MinSize:                  1,
					MaxGroupPreparedCapacity: 3,
					InstanceReusePolicy:      autoscaling.InstanceReusePolicy{ReuseOnScaleIn: true},
				})
				require.NoError(t, err)

				wp, err := b.DescribeWarmPool("wp-reuse-asg")
				require.NoError(t, err)
				assert.True(t, wp.InstanceReusePolicy.ReuseOnScaleIn)
				assert.Equal(t, int32(3), wp.MaxGroupPreparedCapacity)
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

func TestInMemoryBackend_ScalingPolicyStepAdjustments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(b *autoscaling.InMemoryBackend)
		run   func(t *testing.T, b *autoscaling.InMemoryBackend)
		name  string
	}{
		{
			name: "step_adjustments_stored_and_retrieved",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "step-asg",
					MinSize:              1,
					MaxSize:              10,
				})
			},
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				lower := float64(0)
				upper := float64(10)

				policy, err := b.PutScalingPolicy(autoscaling.ScalingPolicyInput{
					PolicyName:            "step-policy",
					AutoScalingGroupName:  "step-asg",
					PolicyType:            "StepScaling",
					AdjustmentType:        "ChangeInCapacity",
					MetricAggregationType: "Average",
					StepAdjustments: []autoscaling.StepAdjustment{
						{MetricIntervalLowerBound: &lower, MetricIntervalUpperBound: &upper, ScalingAdjustment: 2},
						{MetricIntervalLowerBound: &upper, ScalingAdjustment: 4},
					},
				})
				require.NoError(t, err)
				require.Len(t, policy.StepAdjustments, 2)
				assert.Equal(t, int32(2), policy.StepAdjustments[0].ScalingAdjustment)
				assert.Equal(t, int32(4), policy.StepAdjustments[1].ScalingAdjustment)
				assert.Equal(t, "Average", policy.MetricAggregationType)
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

func TestInMemoryBackend_InstanceRefreshFullPreferences(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(b *autoscaling.InMemoryBackend)
		run   func(t *testing.T, b *autoscaling.InMemoryBackend)
		name  string
	}{
		{
			name: "full_preferences_stored",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "ir-full-asg",
					MinSize:              2,
					MaxSize:              5,
					DesiredCapacity:      2,
				})
			},
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				refresh, err := b.StartInstanceRefreshWithInput(autoscaling.StartInstanceRefreshInput{
					AutoScalingGroupName: "ir-full-asg",
					Strategy:             "Rolling",
					Preferences: autoscaling.InstanceRefreshPreferences{
						MinHealthyPercentage: 80,
						MaxHealthyPercentage: 110,
						InstanceWarmup:       300,
						SkipMatching:         true,
						AutoRollback:         true,
					},
				})
				require.NoError(t, err)
				assert.Equal(t, int32(80), refresh.Preferences.MinHealthyPercentage)
				assert.Equal(t, int32(110), refresh.Preferences.MaxHealthyPercentage)
				assert.Equal(t, int32(300), refresh.Preferences.InstanceWarmup)
				assert.True(t, refresh.Preferences.SkipMatching)
				assert.True(t, refresh.Preferences.AutoRollback)
			},
		},
		{
			name: "default_min_healthy_percentage",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "ir-default-asg",
					MinSize:              1,
					MaxSize:              3,
				})
			},
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				refresh, err := b.StartInstanceRefresh("ir-default-asg")
				require.NoError(t, err)
				assert.Equal(t, int32(90), refresh.Preferences.MinHealthyPercentage)
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

func TestInMemoryBackend_CreateLaunchConfigurationExtended(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, b *autoscaling.InMemoryBackend)
		name string
	}{
		{
			name: "spot_price_stored",
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateLaunchConfiguration(autoscaling.CreateLaunchConfigurationInput{
					LaunchConfigurationName: "spot-lc",
					ImageID:                 "ami-abc",
					InstanceType:            "t2.micro",
					SpotPrice:               "0.05",
				})
				require.NoError(t, err)

				lcs, err := b.DescribeLaunchConfigurations([]string{"spot-lc"})
				require.NoError(t, err)
				assert.Equal(t, "0.05", lcs[0].SpotPrice)
			},
		},
		{
			name: "block_device_mapping_with_ebs_stored",
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				bdm := autoscaling.BlockDeviceMapping{
					DeviceName: "/dev/sda1",
					Ebs: &autoscaling.EbsBlockDevice{
						VolumeType:          "gp3",
						VolumeSize:          50,
						Iops:                3000,
						DeleteOnTermination: true,
					},
				}

				_, err := b.CreateLaunchConfiguration(autoscaling.CreateLaunchConfigurationInput{
					LaunchConfigurationName: "ebs-lc",
					ImageID:                 "ami-abc",
					InstanceType:            "t2.micro",
					BlockDeviceMappings:     []autoscaling.BlockDeviceMapping{bdm},
				})
				require.NoError(t, err)

				lcs, err := b.DescribeLaunchConfigurations([]string{"ebs-lc"})
				require.NoError(t, err)
				require.Len(t, lcs[0].BlockDeviceMappings, 1)
				require.NotNil(t, lcs[0].BlockDeviceMappings[0].Ebs)
				assert.Equal(t, "gp3", lcs[0].BlockDeviceMappings[0].Ebs.VolumeType)
				assert.Equal(t, int32(50), lcs[0].BlockDeviceMappings[0].Ebs.VolumeSize)
			},
		},
		{
			name: "associate_public_ip_address_stored",
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateLaunchConfiguration(autoscaling.CreateLaunchConfigurationInput{
					LaunchConfigurationName:  "pubip-lc",
					ImageID:                  "ami-abc",
					InstanceType:             "t2.micro",
					AssociatePublicIPAddress: true,
					EbsOptimized:             true,
					InstanceMonitoring:       true,
				})
				require.NoError(t, err)

				lcs, err := b.DescribeLaunchConfigurations([]string{"pubip-lc"})
				require.NoError(t, err)
				assert.True(t, lcs[0].AssociatePublicIPAddress)
				assert.True(t, lcs[0].EbsOptimized)
				assert.True(t, lcs[0].InstanceMonitoring)
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
