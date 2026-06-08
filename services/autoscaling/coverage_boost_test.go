package autoscaling_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/autoscaling"
)

// Helper to create a backend with a single ASG for reuse in many tests.
func newBackendWithASG(t *testing.T, name string) *autoscaling.InMemoryBackend { //nolint:unused // existing issue.
	t.Helper()

	b := autoscaling.NewInMemoryBackend()
	_, err := b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
		AutoScalingGroupName: name,
		MinSize:              1,
		MaxSize:              5,
		DesiredCapacity:      2,
	})
	require.NoError(t, err)

	return b
}

func TestInMemoryBackend_ResumeProcesses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(b *autoscaling.InMemoryBackend)
		run       func(t *testing.T, b *autoscaling.InMemoryBackend)
		name      string
		wantErr   bool
		wantEmpty bool
	}{
		{
			name: "resume_specific_processes",
			setup: func(b *autoscaling.InMemoryBackend) {
				_ = b.SuspendProcesses("rp-asg", []string{"Launch", "Terminate", "HealthCheck"})
			},
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				err := b.ResumeProcesses("rp-asg", []string{"Launch"})
				require.NoError(t, err)

				groups, _ := b.DescribeAutoScalingGroups([]string{"rp-asg"})
				assert.NotContains(t, groups[0].SuspendedProcesses, "Launch")
				assert.Contains(t, groups[0].SuspendedProcesses, "Terminate")
				assert.Contains(t, groups[0].SuspendedProcesses, "HealthCheck")
			},
		},
		{
			name: "resume_all_processes_with_empty_slice",
			setup: func(b *autoscaling.InMemoryBackend) {
				_ = b.SuspendProcesses("rp-all-asg", []string{"Launch", "Terminate"})
			},
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				err := b.ResumeProcesses("rp-all-asg", []string{})
				require.NoError(t, err)

				groups, _ := b.DescribeAutoScalingGroups([]string{"rp-all-asg"})
				assert.Empty(t, groups[0].SuspendedProcesses)
			},
		},
		{
			name:    "resume_group_not_found",
			wantErr: true,
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				err := b.ResumeProcesses("no-such", []string{"Launch"})
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			_, err := b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
				AutoScalingGroupName: "rp-asg",
				MinSize:              1,
				MaxSize:              3,
			})
			require.NoError(t, err)

			_, err = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
				AutoScalingGroupName: "rp-all-asg",
				MinSize:              1,
				MaxSize:              3,
			})
			require.NoError(t, err)

			if tt.setup != nil {
				tt.setup(b)
			}

			tt.run(t, b)
		})
	}
}

func TestInMemoryBackend_EnterStandby(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend) []string
		name    string
		group   string
		wantErr bool
		decr    bool
	}{
		{
			name:  "enter_standby_with_decrement",
			group: "standby-asg",
			decr:  true,
			setup: func(b *autoscaling.InMemoryBackend) []string {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "standby-asg",
					MinSize:              1,
					MaxSize:              5,
					DesiredCapacity:      2,
				})

				instances, _ := b.DescribeAutoScalingInstances(nil)
				ids := make([]string, 0, 1)
				for _, inst := range instances {
					if inst.AutoScalingGroupName == "standby-asg" {
						ids = append(ids, inst.InstanceID)

						break
					}
				}

				return ids
			},
		},
		{
			name:  "enter_standby_without_decrement",
			group: "standby2-asg",
			decr:  false,
			setup: func(b *autoscaling.InMemoryBackend) []string {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "standby2-asg",
					MinSize:              1,
					MaxSize:              5,
					DesiredCapacity:      2,
				})

				instances, _ := b.DescribeAutoScalingInstances(nil)
				ids := make([]string, 0, 1)
				for _, inst := range instances {
					if inst.AutoScalingGroupName == "standby2-asg" {
						ids = append(ids, inst.InstanceID)

						break
					}
				}

				return ids
			},
		},
		{
			name:    "enter_standby_group_not_found",
			group:   "no-such",
			wantErr: true,
			setup:   func(_ *autoscaling.InMemoryBackend) []string { return []string{"i-fake"} },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			instanceIDs := tt.setup(b)

			activities, err := b.EnterStandby(tt.group, instanceIDs, tt.decr)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, activities)
		})
	}
}

func TestInMemoryBackend_ExitStandby(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend) []string
		name    string
		group   string
		wantErr bool
	}{
		{
			name:  "exit_standby_success",
			group: "exit-standby-asg",
			setup: func(b *autoscaling.InMemoryBackend) []string {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "exit-standby-asg",
					MinSize:              1,
					MaxSize:              5,
					DesiredCapacity:      2,
				})

				instances, _ := b.DescribeAutoScalingInstances(nil)
				ids := make([]string, 0, 1)
				for _, inst := range instances {
					ids = append(ids, inst.InstanceID)

					break
				}

				_, _ = b.EnterStandby("exit-standby-asg", ids, false)

				return ids
			},
		},
		{
			name:    "exit_standby_group_not_found",
			group:   "no-such",
			wantErr: true,
			setup:   func(_ *autoscaling.InMemoryBackend) []string { return []string{"i-fake"} },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			instanceIDs := tt.setup(b)

			activities, err := b.ExitStandby(tt.group, instanceIDs)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, activities)
		})
	}
}

func TestInMemoryBackend_SetInstanceProtection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup         func(b *autoscaling.InMemoryBackend) (string, []string)
		name          string
		wantErr       bool
		wantProtected bool
	}{
		{
			name: "protect_instances",
			setup: func(b *autoscaling.InMemoryBackend) (string, []string) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "prot-inst-asg",
					MinSize:              1,
					MaxSize:              5,
					DesiredCapacity:      1,
				})

				instances, _ := b.DescribeAutoScalingInstances(nil)

				return "prot-inst-asg", []string{instances[0].InstanceID}
			},
			wantProtected: true,
		},
		{
			name: "unprotect_instances",
			setup: func(b *autoscaling.InMemoryBackend) (string, []string) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName:             "unprot-inst-asg",
					MinSize:                          1,
					MaxSize:                          5,
					DesiredCapacity:                  1,
					NewInstancesProtectedFromScaleIn: true,
				})

				instances, _ := b.DescribeAutoScalingInstances(nil)

				return "unprot-inst-asg", []string{instances[0].InstanceID}
			},
			wantProtected: false,
		},
		{
			name: "group_not_found",
			setup: func(_ *autoscaling.InMemoryBackend) (string, []string) {
				return "no-such", []string{"i-fake"}
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			groupName, instanceIDs := tt.setup(b)

			err := b.SetInstanceProtection(groupName, instanceIDs, tt.wantProtected)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			groups, _ := b.DescribeAutoScalingGroups([]string{groupName})
			for _, inst := range groups[0].Instances {
				for _, id := range instanceIDs {
					if inst.InstanceID == id {
						assert.Equal(t, tt.wantProtected, inst.ProtectedFromScaleIn)
					}
				}
			}
		})
	}
}

func TestInMemoryBackend_RecordLifecycleActionHeartbeat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend)
		input   autoscaling.RecordLifecycleActionHeartbeatInput
		name    string
		wantErr bool
	}{
		{
			name: "heartbeat_hook_exists",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "hb-asg",
					MinSize:              0,
					MaxSize:              5,
				})
				_ = b.PutLifecycleHook(autoscaling.LifecycleHook{
					LifecycleHookName:    "my-hook",
					AutoScalingGroupName: "hb-asg",
					DefaultResult:        "CONTINUE",
				})
			},
			input: autoscaling.RecordLifecycleActionHeartbeatInput{
				AutoScalingGroupName: "hb-asg",
				LifecycleHookName:    "my-hook",
			},
		},
		{
			name:    "group_not_found",
			wantErr: true,
			input: autoscaling.RecordLifecycleActionHeartbeatInput{
				AutoScalingGroupName: "no-such",
				LifecycleHookName:    "my-hook",
			},
		},
		{
			name: "hook_not_found",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "hb-nohook-asg",
					MinSize:              0,
					MaxSize:              5,
				})
			},
			wantErr: true,
			input: autoscaling.RecordLifecycleActionHeartbeatInput{
				AutoScalingGroupName: "hb-nohook-asg",
				LifecycleHookName:    "ghost-hook",
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

			err := b.RecordLifecycleActionHeartbeat(tt.input)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestInMemoryBackend_ExecutePolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend)
		name    string
		input   autoscaling.ExecutePolicyInput
		wantErr bool
	}{
		{
			name: "execute_change_in_capacity",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "ep-asg",
					MinSize:              1,
					MaxSize:              10,
					DesiredCapacity:      2,
				})
				_, _ = b.PutScalingPolicy(autoscaling.ScalingPolicyInput{
					PolicyName:           "scale-up",
					AutoScalingGroupName: "ep-asg",
					PolicyType:           "SimpleScaling",
					AdjustmentType:       "ChangeInCapacity",
					ScalingAdjustment:    2,
				})
			},
			input: autoscaling.ExecutePolicyInput{
				AutoScalingGroupName: "ep-asg",
				PolicyName:           "scale-up",
			},
		},
		{
			name: "execute_exact_capacity",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "ep-exact-asg",
					MinSize:              1,
					MaxSize:              10,
					DesiredCapacity:      2,
				})
				_, _ = b.PutScalingPolicy(autoscaling.ScalingPolicyInput{
					PolicyName:           "set-to-5",
					AutoScalingGroupName: "ep-exact-asg",
					AdjustmentType:       "ExactCapacity",
					ScalingAdjustment:    5,
				})
			},
			input: autoscaling.ExecutePolicyInput{
				AutoScalingGroupName: "ep-exact-asg",
				PolicyName:           "set-to-5",
			},
		},
		{
			name: "execute_percent_change",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "ep-pct-asg",
					MinSize:              1,
					MaxSize:              10,
					DesiredCapacity:      4,
				})
				_, _ = b.PutScalingPolicy(autoscaling.ScalingPolicyInput{
					PolicyName:           "scale-by-pct",
					AutoScalingGroupName: "ep-pct-asg",
					AdjustmentType:       "PercentChangeInCapacity",
					ScalingAdjustment:    50,
				})
			},
			input: autoscaling.ExecutePolicyInput{
				AutoScalingGroupName: "ep-pct-asg",
				PolicyName:           "scale-by-pct",
			},
		},
		{
			name:    "group_not_found",
			wantErr: true,
			input: autoscaling.ExecutePolicyInput{
				AutoScalingGroupName: "no-such",
				PolicyName:           "my-policy",
			},
		},
		{
			name: "policy_not_found",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "ep-nopol-asg",
					MinSize:              1,
					MaxSize:              10,
					DesiredCapacity:      2,
				})
			},
			wantErr: true,
			input: autoscaling.ExecutePolicyInput{
				AutoScalingGroupName: "ep-nopol-asg",
				PolicyName:           "ghost-policy",
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

			err := b.ExecutePolicy(tt.input)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestInMemoryBackend_LaunchInstances(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(b *autoscaling.InMemoryBackend)
		name        string
		group       string
		count       int32
		wantErr     bool
		wantAtLeast int
	}{
		{
			name:  "launch_instances_success",
			group: "launch-asg",
			count: 3,
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "launch-asg",
					MinSize:              0,
					MaxSize:              10,
					DesiredCapacity:      0,
				})
			},
			wantAtLeast: 3,
		},
		{
			name:    "launch_instances_group_not_found",
			group:   "no-such",
			count:   1,
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

			instances, err := b.LaunchInstances(tt.group, tt.count)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, instances, int(tt.count))
		})
	}
}

func TestInMemoryBackend_GetPredictiveScalingForecast(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend)
		name    string
		group   string
		wantErr bool
	}{
		{
			name:  "group_exists",
			group: "psf-asg",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "psf-asg",
					MinSize:              1,
					MaxSize:              5,
				})
			},
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

			err := b.GetPredictiveScalingForecast(tt.group)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestInMemoryBackend_DeleteNotificationConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(b *autoscaling.InMemoryBackend)
		name     string
		group    string
		topicARN string
		wantErr  bool
	}{
		{
			name:     "delete_existing_notification",
			group:    "notif-del-asg",
			topicARN: "arn:aws:sns:us-east-1:000000000000:my-topic",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "notif-del-asg",
					MinSize:              0,
					MaxSize:              5,
				})
				_ = b.PutNotificationConfiguration(
					"notif-del-asg",
					"arn:aws:sns:us-east-1:000000000000:my-topic",
					[]string{"autoscaling:EC2_INSTANCE_LAUNCH"},
				)
			},
		},
		{
			name:     "delete_notification_group_not_found",
			group:    "no-such",
			topicARN: "arn:aws:sns:us-east-1:000000000000:t",
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

			err := b.DeleteNotificationConfiguration(tt.group, tt.topicARN)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			configs, _ := b.DescribeNotificationConfigurations([]string{tt.group})
			for _, c := range configs {
				assert.NotEqual(t, tt.topicARN, c.TopicARN)
			}
		})
	}
}

func TestInMemoryBackend_DeletePolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend)
		name    string
		group   string
		policy  string
		wantErr bool
	}{
		{
			name:   "delete_by_name",
			group:  "dp-asg",
			policy: "my-policy",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "dp-asg",
					MinSize:              0,
					MaxSize:              5,
				})
				_, _ = b.PutScalingPolicy(autoscaling.ScalingPolicyInput{
					PolicyName:           "my-policy",
					AutoScalingGroupName: "dp-asg",
					AdjustmentType:       "ChangeInCapacity",
					ScalingAdjustment:    1,
				})
			},
		},
		{
			name:   "delete_by_arn",
			group:  "dp-arn-asg",
			policy: "", // will be set from created policy ARN
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "dp-arn-asg",
					MinSize:              0,
					MaxSize:              5,
				})
			},
		},
		{
			name:    "delete_policy_not_found",
			group:   "no-such",
			policy:  "ghost-policy",
			wantErr: true,
		},
		{
			name:   "delete_by_name_empty_group_searches_all",
			group:  "",
			policy: "cross-group-policy",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "cg-asg",
					MinSize:              0,
					MaxSize:              5,
				})
				_, _ = b.PutScalingPolicy(autoscaling.ScalingPolicyInput{
					PolicyName:           "cross-group-policy",
					AutoScalingGroupName: "cg-asg",
					AdjustmentType:       "ChangeInCapacity",
					ScalingAdjustment:    1,
				})
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

			policyToDelete := tt.policy

			// For ARN-based test, create the policy and grab its ARN
			if tt.name == "delete_by_arn" {
				p, err := b.PutScalingPolicy(autoscaling.ScalingPolicyInput{
					PolicyName:           "arn-test-policy",
					AutoScalingGroupName: tt.group,
					AdjustmentType:       "ChangeInCapacity",
					ScalingAdjustment:    1,
				})
				require.NoError(t, err)
				policyToDelete = p.PolicyARN
			}

			err := b.DeletePolicy(tt.group, policyToDelete)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestInMemoryBackend_PutScheduledUpdateGroupAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		action  autoscaling.ScheduledUpdateGroupAction
		setup   func(b *autoscaling.InMemoryBackend)
		name    string
		group   string
		wantErr bool
	}{
		{
			name:  "put_scheduled_action_success",
			group: "suga-asg",
			action: autoscaling.ScheduledUpdateGroupAction{
				ScheduledActionName: "scale-out",
				Recurrence:          "0 9 * * MON-FRI",
			},
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "suga-asg",
					MinSize:              0,
					MaxSize:              10,
				})
			},
		},
		{
			name:    "put_scheduled_action_group_not_found",
			group:   "no-such",
			wantErr: true,
			action:  autoscaling.ScheduledUpdateGroupAction{ScheduledActionName: "a"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			err := b.PutScheduledUpdateGroupAction(tt.group, tt.action)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestInMemoryBackend_DeleteScheduledAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend)
		name    string
		group   string
		action  string
		wantErr bool
	}{
		{
			name:   "delete_existing_scheduled_action",
			group:  "dsa-asg",
			action: "scale-out",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "dsa-asg",
					MinSize:              0,
					MaxSize:              10,
				})
				_ = b.PutScheduledUpdateGroupAction("dsa-asg", autoscaling.ScheduledUpdateGroupAction{
					ScheduledActionName: "scale-out",
				})
			},
		},
		{
			name:    "delete_nonexistent_action",
			group:   "dsa-asg2",
			action:  "ghost-action",
			wantErr: true,
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "dsa-asg2",
					MinSize:              0,
					MaxSize:              10,
				})
			},
		},
		{
			name:    "delete_group_not_found",
			group:   "no-such",
			action:  "a",
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

			err := b.DeleteScheduledAction(tt.group, tt.action)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestInMemoryBackend_DeleteWarmPool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend)
		name    string
		group   string
		wantErr bool
	}{
		{
			name:  "delete_warm_pool_success",
			group: "dwp-asg",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "dwp-asg",
					MinSize:              0,
					MaxSize:              10,
				})
				_ = b.PutWarmPool(autoscaling.WarmPoolInput{
					AutoScalingGroupName: "dwp-asg",
				})
			},
		},
		{
			name:    "delete_warm_pool_group_not_found",
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

			err := b.DeleteWarmPool(tt.group)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestInMemoryBackend_DetachInstances(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend) (string, []string)
		name    string
		decr    bool
		wantErr bool
	}{
		{
			name: "detach_with_decrement",
			decr: true,
			setup: func(b *autoscaling.InMemoryBackend) (string, []string) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "detach-asg",
					MinSize:              0,
					MaxSize:              5,
					DesiredCapacity:      2,
				})

				instances, _ := b.DescribeAutoScalingInstances(nil)
				ids := []string{instances[0].InstanceID}

				return "detach-asg", ids
			},
		},
		{
			name: "detach_without_decrement",
			decr: false,
			setup: func(b *autoscaling.InMemoryBackend) (string, []string) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "detach2-asg",
					MinSize:              0,
					MaxSize:              5,
					DesiredCapacity:      2,
				})

				instances, _ := b.DescribeAutoScalingInstances(nil)
				ids := []string{instances[0].InstanceID}

				return "detach2-asg", ids
			},
		},
		{
			name:    "detach_group_not_found",
			wantErr: true,
			setup: func(_ *autoscaling.InMemoryBackend) (string, []string) {
				return "no-such", []string{"i-fake"}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			group, instanceIDs := tt.setup(b)

			activities, err := b.DetachInstances(group, instanceIDs, tt.decr)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, activities)
		})
	}
}

func TestInMemoryBackend_DetachLoadBalancers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend)
		name    string
		group   string
		lbs     []string
		wantErr bool
	}{
		{
			name:  "detach_existing_lb",
			group: "dlb-asg",
			lbs:   []string{"my-elb"},
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "dlb-asg",
					MinSize:              0,
					MaxSize:              5,
				})
				_ = b.AttachLoadBalancers("dlb-asg", []string{"my-elb"})
			},
		},
		{
			name:    "detach_lb_group_not_found",
			group:   "no-such",
			lbs:     []string{"elb-x"},
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

			err := b.DetachLoadBalancers(tt.group, tt.lbs)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			groups, _ := b.DescribeAutoScalingGroups([]string{tt.group})
			for _, lb := range tt.lbs {
				assert.NotContains(t, groups[0].LoadBalancerNames, lb)
			}
		})
	}
}

func TestInMemoryBackend_DetachLoadBalancerTargetGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend)
		name    string
		group   string
		arns    []string
		wantErr bool
	}{
		{
			name:  "detach_existing_tg",
			group: "dtg-asg",
			arns:  []string{"arn:aws:tg/one"},
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "dtg-asg",
					MinSize:              0,
					MaxSize:              5,
				})
				_ = b.AttachLoadBalancerTargetGroups("dtg-asg", []string{"arn:aws:tg/one"})
			},
		},
		{
			name:    "detach_tg_group_not_found",
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

			err := b.DetachLoadBalancerTargetGroups(tt.group, tt.arns)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			groups, _ := b.DescribeAutoScalingGroups([]string{tt.group})
			for _, arn := range tt.arns {
				assert.NotContains(t, groups[0].TargetGroupARNs, arn)
			}
		})
	}
}

func TestInMemoryBackend_DetachTrafficSources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend)
		name    string
		group   string
		sources []autoscaling.TrafficSource
		wantErr bool
	}{
		{
			name:  "detach_existing_traffic_source",
			group: "dts-asg",
			sources: []autoscaling.TrafficSource{
				{Identifier: "arn:ts-1", Type: "vpc-lattice"},
			},
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "dts-asg",
					MinSize:              0,
					MaxSize:              5,
				})
				_ = b.AttachTrafficSources("dts-asg", []autoscaling.TrafficSource{
					{Identifier: "arn:ts-1", Type: "vpc-lattice"},
				})
			},
		},
		{
			name:    "detach_traffic_sources_group_not_found",
			group:   "no-such",
			sources: []autoscaling.TrafficSource{{Identifier: "x", Type: "y"}},
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

			err := b.DetachTrafficSources(tt.group, tt.sources)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestInMemoryBackend_EnableDisableMetrics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend)
		name    string
		group   string
		metrics []string
		wantErr bool
		disable bool
	}{
		{
			name:    "enable_valid_metrics",
			group:   "met-asg",
			metrics: []string{"GroupMinSize", "GroupMaxSize"},
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "met-asg",
					MinSize:              0,
					MaxSize:              5,
				})
			},
		},
		{
			name:    "enable_unknown_metric_error",
			group:   "met2-asg",
			metrics: []string{"FakeMetric"},
			wantErr: true,
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "met2-asg",
					MinSize:              0,
					MaxSize:              5,
				})
			},
		},
		{
			name:    "enable_group_not_found",
			group:   "no-such",
			metrics: []string{"GroupMinSize"},
			wantErr: true,
		},
		{
			name:    "disable_metrics",
			group:   "met3-asg",
			metrics: []string{"GroupMinSize"},
			disable: true,
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "met3-asg",
					MinSize:              0,
					MaxSize:              5,
				})
				_ = b.EnableMetricsCollection("met3-asg", []string{"GroupMinSize", "GroupMaxSize"}, "1Minute")
			},
		},
		{
			name:    "disable_metrics_group_not_found",
			group:   "no-such",
			metrics: []string{"GroupMinSize"},
			disable: true,
			wantErr: true,
		},
		{
			name:    "enable_all_metrics_when_empty",
			group:   "met-all-asg",
			metrics: []string{},
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "met-all-asg",
					MinSize:              0,
					MaxSize:              5,
				})
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

			var err error
			if tt.disable {
				err = b.DisableMetricsCollection(tt.group, tt.metrics)
			} else {
				err = b.EnableMetricsCollection(tt.group, tt.metrics, "1Minute")
			}

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestInMemoryBackend_DescribeLoadBalancers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend)
		name    string
		group   string
		wantErr bool
		wantLen int
	}{
		{
			name:  "describe_load_balancers_with_data",
			group: "dlbs-asg",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "dlbs-asg",
					MinSize:              0,
					MaxSize:              5,
				})
				_ = b.AttachLoadBalancers("dlbs-asg", []string{"elb-a", "elb-b"})
			},
			wantLen: 2,
		},
		{
			name:    "describe_load_balancers_group_not_found",
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

			lbs, err := b.DescribeLoadBalancers(tt.group)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, lbs, tt.wantLen)
		})
	}
}

func TestInMemoryBackend_DescribeTrafficSources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend)
		name    string
		group   string
		wantErr bool
		wantLen int
	}{
		{
			name:  "describe_traffic_sources_with_data",
			group: "dts2-asg",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "dts2-asg",
					MinSize:              0,
					MaxSize:              5,
				})
				_ = b.AttachTrafficSources("dts2-asg", []autoscaling.TrafficSource{
					{Identifier: "arn:ts-x", Type: "vpc-lattice"},
				})
			},
			wantLen: 1,
		},
		{
			name:    "describe_traffic_sources_group_not_found",
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

			tss, err := b.DescribeTrafficSources(tt.group)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, tss, tt.wantLen)
		})
	}
}

func TestInMemoryBackend_DescribeInstanceRefreshes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(b *autoscaling.InMemoryBackend)
		name      string
		group     string
		wantErr   bool
		wantCount int
	}{
		{
			name:  "describe_with_active_refresh",
			group: "dir-asg",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "dir-asg",
					MinSize:              0,
					MaxSize:              5,
				})
				_, _ = b.StartInstanceRefresh("dir-asg")
			},
			wantCount: 1,
		},
		{
			name:    "describe_group_not_found",
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

			refreshes, err := b.DescribeInstanceRefreshes(tt.group, nil)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, refreshes, tt.wantCount)
		})
	}
}

func TestInMemoryBackend_RollbackInstanceRefresh(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend)
		name    string
		group   string
		wantErr bool
	}{
		{
			name:  "rollback_active_refresh",
			group: "rib-asg",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "rib-asg",
					MinSize:              0,
					MaxSize:              5,
				})
				_ = b.AddInstanceRefresh(autoscaling.InstanceRefresh{
					InstanceRefreshID:    "ir-rollback-123",
					AutoScalingGroupName: "rib-asg",
					Status:               "InProgress",
				})
			},
		},
		{
			name:    "rollback_group_not_found",
			group:   "no-such",
			wantErr: true,
		},
		{
			name:  "rollback_no_active_refresh",
			group: "rib-noreresh-asg",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "rib-noreresh-asg",
					MinSize:              0,
					MaxSize:              5,
				})
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

			_, err := b.RollbackInstanceRefresh(tt.group)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestInMemoryBackend_DescribePolicies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(b *autoscaling.InMemoryBackend)
		name        string
		group       string
		policyNames []string
		wantCount   int
	}{
		{
			name:  "describe_all_policies",
			group: "dpol-asg",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "dpol-asg",
					MinSize:              0,
					MaxSize:              5,
				})
				_, _ = b.PutScalingPolicy(autoscaling.ScalingPolicyInput{
					PolicyName:           "pol-a",
					AutoScalingGroupName: "dpol-asg",
					AdjustmentType:       "ChangeInCapacity",
					ScalingAdjustment:    1,
				})
				_, _ = b.PutScalingPolicy(autoscaling.ScalingPolicyInput{
					PolicyName:           "pol-b",
					AutoScalingGroupName: "dpol-asg",
					AdjustmentType:       "ChangeInCapacity",
					ScalingAdjustment:    -1,
				})
			},
			wantCount: 2,
		},
		{
			name:  "describe_policies_across_all_groups",
			group: "", // empty = describe all
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "dpol-cross-asg",
					MinSize:              0,
					MaxSize:              5,
				})
				_, _ = b.PutScalingPolicy(autoscaling.ScalingPolicyInput{
					PolicyName:           "cross-pol",
					AutoScalingGroupName: "dpol-cross-asg",
					AdjustmentType:       "ChangeInCapacity",
					ScalingAdjustment:    1,
				})
			},
			wantCount: 1,
		},
		{
			name:        "describe_policies_filter_by_name",
			group:       "dpol-filter-asg",
			policyNames: []string{"pol-x"},
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "dpol-filter-asg",
					MinSize:              0,
					MaxSize:              5,
				})
				_, _ = b.PutScalingPolicy(autoscaling.ScalingPolicyInput{
					PolicyName:           "pol-x",
					AutoScalingGroupName: "dpol-filter-asg",
					AdjustmentType:       "ChangeInCapacity",
					ScalingAdjustment:    1,
				})
				_, _ = b.PutScalingPolicy(autoscaling.ScalingPolicyInput{
					PolicyName:           "pol-y",
					AutoScalingGroupName: "dpol-filter-asg",
					AdjustmentType:       "ChangeInCapacity",
					ScalingAdjustment:    -1,
				})
			},
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			policies, err := b.DescribePolicies(tt.group, tt.policyNames)
			require.NoError(t, err)
			assert.Len(t, policies, tt.wantCount)
		})
	}
}

func TestInMemoryBackend_WarmPool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend)
		name    string
		input   autoscaling.WarmPoolInput
		wantErr bool
	}{
		{
			name: "put_warm_pool_default_state",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "wp-def-asg",
					MinSize:              0,
					MaxSize:              10,
				})
			},
			input: autoscaling.WarmPoolInput{
				AutoScalingGroupName: "wp-def-asg",
			},
		},
		{
			name: "put_warm_pool_running_state",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "wp-run-asg",
					MinSize:              0,
					MaxSize:              10,
				})
			},
			input: autoscaling.WarmPoolInput{
				AutoScalingGroupName: "wp-run-asg",
				PoolState:            "Running",
			},
		},
		{
			name: "put_warm_pool_invalid_state",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "wp-bad-asg",
					MinSize:              0,
					MaxSize:              10,
				})
			},
			input: autoscaling.WarmPoolInput{
				AutoScalingGroupName: "wp-bad-asg",
				PoolState:            "InvalidState",
			},
			wantErr: true,
		},
		{
			name:    "put_warm_pool_group_not_found",
			wantErr: true,
			input: autoscaling.WarmPoolInput{
				AutoScalingGroupName: "no-such",
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

			err := b.PutWarmPool(tt.input)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			wp, err := b.DescribeWarmPool(tt.input.AutoScalingGroupName)
			require.NoError(t, err)
			assert.NotNil(t, wp)
		})
	}
}

func TestInMemoryBackend_DescribeWarmPool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend)
		name    string
		group   string
		wantErr bool
	}{
		{
			name:    "describe_warm_pool_group_not_found",
			group:   "no-such",
			wantErr: true,
		},
		{
			name:  "describe_warm_pool_not_configured",
			group: "no-wp-asg",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "no-wp-asg",
					MinSize:              0,
					MaxSize:              5,
				})
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

			_, err := b.DescribeWarmPool(tt.group)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestInMemoryBackend_DescribeNotificationConfigurations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(b *autoscaling.InMemoryBackend)
		name       string
		groupNames []string
		wantCount  int
	}{
		{
			name: "describe_all_notification_configs",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "dnc-asg",
					MinSize:              0,
					MaxSize:              5,
				})
				_ = b.PutNotificationConfiguration(
					"dnc-asg",
					"arn:aws:sns:us-east-1:000000000000:topic-a",
					[]string{"autoscaling:EC2_INSTANCE_LAUNCH", "autoscaling:EC2_INSTANCE_TERMINATE"},
				)
			},
			groupNames: nil, // fetch all
			wantCount:  2,
		},
		{
			name: "describe_notification_configs_filtered",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "dnc-filter-asg",
					MinSize:              0,
					MaxSize:              5,
				})
				_ = b.PutNotificationConfiguration(
					"dnc-filter-asg",
					"arn:aws:sns:us-east-1:000000000000:topic-b",
					[]string{"autoscaling:EC2_INSTANCE_LAUNCH"},
				)
			},
			groupNames: []string{"dnc-filter-asg"},
			wantCount:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			configs, err := b.DescribeNotificationConfigurations(tt.groupNames)
			require.NoError(t, err)
			assert.Len(t, configs, tt.wantCount)
		})
	}
}

func TestInMemoryBackend_DescribeAccountLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(b *autoscaling.InMemoryBackend)
		name     string
		wantASGs int32
	}{
		{
			name:     "empty_backend",
			wantASGs: 0,
		},
		{
			name: "with_some_groups",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "lim-asg-1",
					MinSize:              0,
					MaxSize:              5,
				})
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "lim-asg-2",
					MinSize:              0,
					MaxSize:              5,
				})
			},
			wantASGs: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			limits, err := b.DescribeAccountLimits()
			require.NoError(t, err)
			require.NotNil(t, limits)
			assert.Equal(t, tt.wantASGs, limits.NumberOfAutoScalingGroups)
			assert.Positive(t, limits.MaxNumberOfAutoScalingGroups)
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

func TestInMemoryBackend_DescribeLoadBalancerTargetGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend)
		name    string
		group   string
		wantErr bool
		wantLen int
	}{
		{
			name:  "describe_target_groups_with_data",
			group: "dltg-asg",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "dltg-asg",
					MinSize:              0,
					MaxSize:              5,
				})
				_ = b.AttachLoadBalancerTargetGroups("dltg-asg", []string{
					"arn:aws:elasticloadbalancing:us-east-1:123:targetgroup/tg/abc",
				})
			},
			wantLen: 1,
		},
		{
			name:    "describe_target_groups_group_not_found",
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

			tgs, err := b.DescribeLoadBalancerTargetGroups(tt.group)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, tgs, tt.wantLen)
		})
	}
}

func TestInMemoryBackend_DescribeTags_WithFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(b *autoscaling.InMemoryBackend)
		name      string
		filters   []autoscaling.TagFilter
		wantCount int
	}{
		{
			name: "no_filters_returns_all",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "tfilter-asg",
					MinSize:              0,
					MaxSize:              5,
					Tags: []autoscaling.Tag{
						{Key: "env", Value: "prod"},
						{Key: "team", Value: "platform"},
					},
				})
			},
			filters:   nil,
			wantCount: 2,
		},
		{
			name: "filter_by_key",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "tfilter2-asg",
					MinSize:              0,
					MaxSize:              5,
					Tags: []autoscaling.Tag{
						{Key: "env", Value: "prod"},
						{Key: "team", Value: "platform"},
					},
				})
			},
			filters:   []autoscaling.TagFilter{{Name: "key", Values: []string{"env"}}},
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			tags, err := b.DescribeTags(tt.filters)
			require.NoError(t, err)
			assert.Len(t, tags, tt.wantCount)
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

// TestInMemoryBackend_ApplyDesiredCapacityChange exercises the scale-in path
// (reducing desired capacity removes unprotected instances).
func TestInMemoryBackend_ApplyDesiredCapacityChange(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup         func(b *autoscaling.InMemoryBackend) string
		name          string
		newDesired    int32
		wantInstances int
	}{
		{
			name:       "scale_in_removes_instances",
			newDesired: 1,
			setup: func(b *autoscaling.InMemoryBackend) string {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "scalein-asg",
					MinSize:              0,
					MaxSize:              5,
					DesiredCapacity:      3,
				})

				return "scalein-asg"
			},
			wantInstances: 1,
		},
		{
			name:       "scale_out_adds_instances",
			newDesired: 4,
			setup: func(b *autoscaling.InMemoryBackend) string {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "scaleout-asg",
					MinSize:              1,
					MaxSize:              10,
					DesiredCapacity:      1,
				})

				return "scaleout-asg"
			},
			wantInstances: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			groupName := tt.setup(b)

			err := b.SetDesiredCapacity(groupName, tt.newDesired)
			require.NoError(t, err)

			groups, err := b.DescribeAutoScalingGroups([]string{groupName})
			require.NoError(t, err)
			assert.Len(t, groups[0].Instances, tt.wantInstances)
		})
	}
}

// TestAutoscalingHandler_HTTPActions tests the HTTP handler dispatch for
// 0%-covered operations via the HTTP layer.
func TestAutoscalingHandler_HTTPActions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *autoscaling.Handler)
		name       string
		body       string
		wantStatus int
	}{
		// ResumeProcesses
		{
			name: "resume_processes_success",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=rp-http-asg&MinSize=1&MaxSize=5",
				)
				postAutoscalingForm(
					t,
					h,
					"Action=SuspendProcesses&Version=2011-01-01&AutoScalingGroupName=rp-http-asg&ScalingProcesses.member.1=Launch",
				)
			},
			body:       "Action=ResumeProcesses&Version=2011-01-01&AutoScalingGroupName=rp-http-asg&ScalingProcesses.member.1=Launch", //nolint:lll // existing issue.
			wantStatus: http.StatusOK,
		},
		{
			name:       "resume_processes_group_not_found",
			body:       "Action=ResumeProcesses&Version=2011-01-01&AutoScalingGroupName=no-such",
			wantStatus: http.StatusBadRequest,
		},
		// EnterStandby
		{
			name: "enter_standby_success",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=es-http-asg&MinSize=1&MaxSize=5&DesiredCapacity=2", //nolint:lll // existing issue.
				)
			},
			body:       "Action=EnterStandby&Version=2011-01-01&AutoScalingGroupName=es-http-asg&ShouldDecrementDesiredCapacity=true&InstanceIds.member.1=dummy", //nolint:lll // existing issue.
			wantStatus: http.StatusOK,
		},
		{
			name:       "enter_standby_group_not_found",
			body:       "Action=EnterStandby&Version=2011-01-01&AutoScalingGroupName=no-such&ShouldDecrementDesiredCapacity=false", //nolint:lll // existing issue.
			wantStatus: http.StatusBadRequest,
		},
		// ExitStandby
		{
			name: "exit_standby_success",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=xs-http-asg&MinSize=1&MaxSize=5&DesiredCapacity=2", //nolint:lll // existing issue.
				)
			},
			body:       "Action=ExitStandby&Version=2011-01-01&AutoScalingGroupName=xs-http-asg&InstanceIds.member.1=dummy",
			wantStatus: http.StatusOK,
		},
		{
			name:       "exit_standby_group_not_found",
			body:       "Action=ExitStandby&Version=2011-01-01&AutoScalingGroupName=no-such",
			wantStatus: http.StatusBadRequest,
		},
		// SetInstanceProtection
		{
			name: "set_instance_protection_success",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=sip-http-asg&MinSize=0&MaxSize=5",
				)
			},
			body:       "Action=SetInstanceProtection&Version=2011-01-01&AutoScalingGroupName=sip-http-asg&ProtectedFromScaleIn=true", //nolint:lll // existing issue.
			wantStatus: http.StatusOK,
		},
		{
			name:       "set_instance_protection_group_not_found",
			body:       "Action=SetInstanceProtection&Version=2011-01-01&AutoScalingGroupName=no-such&ProtectedFromScaleIn=true",
			wantStatus: http.StatusBadRequest,
		},
		// RecordLifecycleActionHeartbeat
		{
			name: "record_lifecycle_heartbeat_success",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=lhb-http-asg&MinSize=0&MaxSize=5",
				)
				postAutoscalingForm(
					t,
					h,
					"Action=PutLifecycleHook&Version=2011-01-01&AutoScalingGroupName=lhb-http-asg&LifecycleHookName=my-hook",
				)
			},
			body:       "Action=RecordLifecycleActionHeartbeat&Version=2011-01-01&AutoScalingGroupName=lhb-http-asg&LifecycleHookName=my-hook", //nolint:lll // existing issue.
			wantStatus: http.StatusOK,
		},
		{
			name:       "record_lifecycle_heartbeat_group_not_found",
			body:       "Action=RecordLifecycleActionHeartbeat&Version=2011-01-01&AutoScalingGroupName=no-such&LifecycleHookName=h", //nolint:lll // existing issue.
			wantStatus: http.StatusBadRequest,
		},
		// ExecutePolicy
		{
			name: "execute_policy_success",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=ep-http-asg&MinSize=1&MaxSize=10&DesiredCapacity=2", //nolint:lll // existing issue.
				)
				postAutoscalingForm(
					t,
					h,
					"Action=PutScalingPolicy&Version=2011-01-01&AutoScalingGroupName=ep-http-asg&PolicyName=scale-up&AdjustmentType=ChangeInCapacity&ScalingAdjustment=1", //nolint:lll // existing issue.
				)
			},
			body:       "Action=ExecutePolicy&Version=2011-01-01&AutoScalingGroupName=ep-http-asg&PolicyName=scale-up",
			wantStatus: http.StatusOK,
		},
		{
			name:       "execute_policy_group_not_found",
			body:       "Action=ExecutePolicy&Version=2011-01-01&AutoScalingGroupName=no-such&PolicyName=p",
			wantStatus: http.StatusBadRequest,
		},
		// GetPredictiveScalingForecast
		{
			name: "get_predictive_scaling_forecast_success",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=psf-http-asg&MinSize=0&MaxSize=5",
				)
			},
			body:       "Action=GetPredictiveScalingForecast&Version=2011-01-01&AutoScalingGroupName=psf-http-asg",
			wantStatus: http.StatusOK,
		},
		{
			name:       "get_predictive_scaling_forecast_not_found",
			body:       "Action=GetPredictiveScalingForecast&Version=2011-01-01&AutoScalingGroupName=no-such",
			wantStatus: http.StatusBadRequest,
		},
		// DeleteNotificationConfiguration
		{
			name: "delete_notification_configuration_success",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=dnc-http-asg&MinSize=0&MaxSize=5",
				)
				postAutoscalingForm(t, h,
					"Action=PutNotificationConfiguration&Version=2011-01-01&AutoScalingGroupName=dnc-http-asg"+
						"&TopicARN=arn:aws:sns:us-east-1:000000000000:t&NotificationTypes.member.1=autoscaling:EC2_INSTANCE_LAUNCH")
			},
			body:       "Action=DeleteNotificationConfiguration&Version=2011-01-01&AutoScalingGroupName=dnc-http-asg&TopicARN=arn:aws:sns:us-east-1:000000000000:t", //nolint:lll // existing issue.
			wantStatus: http.StatusOK,
		},
		{
			name:       "delete_notification_configuration_group_not_found",
			body:       "Action=DeleteNotificationConfiguration&Version=2011-01-01&AutoScalingGroupName=no-such&TopicARN=arn:aws:sns:us-east-1:x", //nolint:lll // existing issue.
			wantStatus: http.StatusBadRequest,
		},
		// DeletePolicy
		{
			name: "delete_policy_success",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=dp-http-asg&MinSize=0&MaxSize=5",
				)
				postAutoscalingForm(
					t,
					h,
					"Action=PutScalingPolicy&Version=2011-01-01&AutoScalingGroupName=dp-http-asg&PolicyName=my-pol&AdjustmentType=ChangeInCapacity&ScalingAdjustment=1", //nolint:lll // existing issue.
				)
			},
			body:       "Action=DeletePolicy&Version=2011-01-01&AutoScalingGroupName=dp-http-asg&PolicyName=my-pol",
			wantStatus: http.StatusOK,
		},
		{
			name:       "delete_policy_not_found",
			body:       "Action=DeletePolicy&Version=2011-01-01&AutoScalingGroupName=no-such&PolicyName=p",
			wantStatus: http.StatusBadRequest,
		},
		// PutScheduledUpdateGroupAction
		{
			name: "put_scheduled_update_group_action_success",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=psuga-http-asg&MinSize=0&MaxSize=10",
				)
			},
			body:       "Action=PutScheduledUpdateGroupAction&Version=2011-01-01&AutoScalingGroupName=psuga-http-asg&ScheduledActionName=sa1", //nolint:lll // existing issue.
			wantStatus: http.StatusOK,
		},
		{
			name:       "put_scheduled_update_group_action_group_not_found",
			body:       "Action=PutScheduledUpdateGroupAction&Version=2011-01-01&AutoScalingGroupName=no-such&ScheduledActionName=a", //nolint:lll // existing issue.
			wantStatus: http.StatusBadRequest,
		},
		// DeleteScheduledAction
		{
			name: "delete_scheduled_action_success",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=dsa-http-asg&MinSize=0&MaxSize=10",
				)
				postAutoscalingForm(
					t,
					h,
					"Action=PutScheduledUpdateGroupAction&Version=2011-01-01&AutoScalingGroupName=dsa-http-asg&ScheduledActionName=sa-del", //nolint:lll // existing issue.
				)
			},
			body:       "Action=DeleteScheduledAction&Version=2011-01-01&AutoScalingGroupName=dsa-http-asg&ScheduledActionName=sa-del", //nolint:lll // existing issue.
			wantStatus: http.StatusOK,
		},
		{
			name:       "delete_scheduled_action_not_found",
			body:       "Action=DeleteScheduledAction&Version=2011-01-01&AutoScalingGroupName=no-such&ScheduledActionName=a",
			wantStatus: http.StatusBadRequest,
		},
		// DeleteWarmPool
		{
			name: "delete_warm_pool_success",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=dwp-http-asg&MinSize=0&MaxSize=10",
				)
				postAutoscalingForm(t, h,
					"Action=PutWarmPool&Version=2011-01-01&AutoScalingGroupName=dwp-http-asg")
			},
			body:       "Action=DeleteWarmPool&Version=2011-01-01&AutoScalingGroupName=dwp-http-asg",
			wantStatus: http.StatusOK,
		},
		{
			name:       "delete_warm_pool_group_not_found",
			body:       "Action=DeleteWarmPool&Version=2011-01-01&AutoScalingGroupName=no-such",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postAutoscalingForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
