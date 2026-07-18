package autoscaling_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/autoscaling"
)

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
