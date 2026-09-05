package autoscaling_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/autoscaling"
)

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

			groups, err := b.DescribeAutoScalingGroups([]string{tt.group}, nil)
			require.NoError(t, err)
			assert.Len(t, groups[0].Instances, tt.wantLen)
		})
	}
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

				groups, _ := b.DescribeAutoScalingGroups([]string{"sih-asg"}, nil)
				require.Len(t, groups[0].Instances, 1)
				instID := groups[0].Instances[0].InstanceID

				err := b.SetInstanceHealth(instID, "Unhealthy", true)
				require.NoError(t, err)

				groups, _ = b.DescribeAutoScalingGroups([]string{"sih-asg"}, nil)
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

				groups, _ := b.DescribeAutoScalingGroups([]string{"sih-grace-asg"}, nil)
				require.Len(t, groups[0].Instances, 1)
				instID := groups[0].Instances[0].InstanceID

				// LaunchTime is just now, so we're within grace period
				err := b.SetInstanceHealth(instID, "Unhealthy", true)
				require.NoError(t, err)

				groups, _ = b.DescribeAutoScalingGroups([]string{"sih-grace-asg"}, nil)
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

				groups, _ := b.DescribeAutoScalingGroups([]string{"sih-norespect-asg"}, nil)
				instID := groups[0].Instances[0].InstanceID

				// false = don't respect grace period
				err := b.SetInstanceHealth(instID, "Unhealthy", false)
				require.NoError(t, err)

				groups, _ = b.DescribeAutoScalingGroups([]string{"sih-norespect-asg"}, nil)
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

				groups, _ := b.DescribeAutoScalingGroups([]string{"idx-term-asg"}, nil)
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

			groups, _ := b.DescribeAutoScalingGroups([]string{groupName}, nil)
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
