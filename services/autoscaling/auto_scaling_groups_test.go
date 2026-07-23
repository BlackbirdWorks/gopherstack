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

func TestInMemoryBackend_SetDesiredCapacity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(b *autoscaling.InMemoryBackend)
		name         string
		group        string
		wantInstance int
		desired      int32
		wantErr      bool
		useDefault   bool
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
			group:      "sdc-asg",
			desired:    5,
			useDefault: true,
		},
		{
			name: "decrease_capacity_no_hook_removes_instances_immediately",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "sdc-scalein-asg",
					MinSize:              0,
					MaxSize:              10,
					DesiredCapacity:      3,
				})
			},
			group:      "sdc-scalein-asg",
			desired:    1,
			useDefault: true,
		},
		{
			name: "decrease_capacity_respects_scale_in_protection",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName:             "sdc-protected-asg",
					MinSize:                          0,
					MaxSize:                          10,
					DesiredCapacity:                  3,
					NewInstancesProtectedFromScaleIn: true,
				})
			},
			group:        "sdc-protected-asg",
			desired:      1,
			wantInstance: 3, // all instances protected: none can be removed, count stays at 3
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

			wantInstance := tt.wantInstance
			if tt.useDefault {
				wantInstance = int(tt.desired)
			}

			assert.Len(t, groups[0].Instances, wantInstance)
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

// TestInMemoryBackend_DeletionProtection locks the gating behavior added to
// DeleteAutoScalingGroup: prevent-all-deletion blocks every delete regardless
// of ForceDelete, prevent-force-deletion only blocks a ForceDelete=true
// attempt, and none/"" behaves exactly as before this feature existed.
func TestInMemoryBackend_DeletionProtection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		deletionProtection string
		forceDelete        bool
		wantErr            bool
	}{
		{
			name:               "none_allows_delete",
			deletionProtection: "none",
			wantErr:            false,
		},
		{
			name:               "unset_allows_delete",
			deletionProtection: "",
			wantErr:            false,
		},
		{
			name:               "prevent_all_deletion_blocks_plain_delete",
			deletionProtection: "prevent-all-deletion",
			forceDelete:        false,
			wantErr:            true,
		},
		{
			name:               "prevent_all_deletion_blocks_force_delete",
			deletionProtection: "prevent-all-deletion",
			forceDelete:        true,
			wantErr:            true,
		},
		{
			name:               "prevent_force_deletion_blocks_force_delete",
			deletionProtection: "prevent-force-deletion",
			forceDelete:        true,
			wantErr:            true,
		},
		{
			name:               "prevent_force_deletion_allows_plain_delete",
			deletionProtection: "prevent-force-deletion",
			forceDelete:        false,
			wantErr:            false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			_, err := b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
				AutoScalingGroupName: "dp-asg",
				MinSize:              0,
				MaxSize:              1,
				DeletionProtection:   tt.deletionProtection,
			})
			require.NoError(t, err)

			delErr := b.DeleteAutoScalingGroup("dp-asg", tt.forceDelete)
			if tt.wantErr {
				require.Error(t, delErr)
				require.ErrorIs(t, delErr, autoscaling.ErrDeletionProtected)

				groups, describeErr := b.DescribeAutoScalingGroups([]string{"dp-asg"})
				require.NoError(t, describeErr)
				assert.Len(t, groups, 1, "group must still exist after a blocked delete")

				return
			}

			require.NoError(t, delErr)

			_, describeErr := b.DescribeAutoScalingGroups([]string{"dp-asg"})
			require.Error(t, describeErr, "group must be gone after an allowed delete")
		})
	}
}

// TestInMemoryBackend_DeletionProtectionInvalidValue locks that
// CreateAutoScalingGroup/UpdateAutoScalingGroup reject a DeletionProtection
// value outside the real AWS enum instead of silently accepting it (which
// would leave DeleteAutoScalingGroup's gating switch unable to ever match it).
func TestInMemoryBackend_DeletionProtectionInvalidValue(t *testing.T) {
	t.Parallel()

	b := autoscaling.NewInMemoryBackend()

	_, err := b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
		AutoScalingGroupName: "bad-dp-asg",
		MinSize:              0,
		MaxSize:              1,
		DeletionProtection:   "not-a-real-value",
	})
	require.Error(t, err)
	require.ErrorIs(t, err, autoscaling.ErrInvalidParameter)

	_, err = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
		AutoScalingGroupName: "ok-dp-asg",
		MinSize:              0,
		MaxSize:              1,
	})
	require.NoError(t, err)

	_, err = b.UpdateAutoScalingGroup(autoscaling.UpdateAutoScalingGroupInput{
		AutoScalingGroupName: "ok-dp-asg",
		DeletionProtection:   "not-a-real-value",
	})
	require.Error(t, err)
	require.ErrorIs(t, err, autoscaling.ErrInvalidParameter)
}

// TestInMemoryBackend_NewGroupPolicyFields locks that the seven previously
// unwired CreateAutoScalingGroupInput/UpdateAutoScalingGroupInput fields
// (AvailabilityZoneDistribution, AvailabilityZoneImpairmentPolicy,
// CapacityReservationSpecification, DeletionProtection,
// InstanceLifecyclePolicy, InstanceMaintenancePolicy,
// SkipZonalShiftValidation) are actually stored and echoed back, on both
// Create and Update.
func TestInMemoryBackend_NewGroupPolicyFields(t *testing.T) {
	t.Parallel()

	minHealthy := int32(50)
	maxHealthy := int32(150)

	b := autoscaling.NewInMemoryBackend()
	g, err := b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
		AutoScalingGroupName: "policy-fields-asg",
		MinSize:              1,
		MaxSize:              3,
		AvailabilityZoneDistribution: &autoscaling.AvailabilityZoneDistribution{
			CapacityDistributionStrategy: "balanced-best-effort",
		},
		AvailabilityZoneImpairmentPolicy: &autoscaling.AvailabilityZoneImpairmentPolicy{
			ImpairedZoneHealthCheckBehavior: "ReplaceUnhealthy",
			ZonalShiftEnabled:               true,
		},
		CapacityReservationSpecification: &autoscaling.CapacityReservationSpecification{
			CapacityReservationPreference: "capacity-reservations-first",
		},
		DeletionProtection: "prevent-force-deletion",
		InstanceLifecyclePolicy: &autoscaling.InstanceLifecyclePolicy{
			RetentionTriggers: &autoscaling.RetentionTriggers{TerminateHookAbandon: "retain"},
		},
		InstanceMaintenancePolicy: &autoscaling.InstanceMaintenancePolicy{
			MinHealthyPercentage: &minHealthy,
			MaxHealthyPercentage: &maxHealthy,
		},
		SkipZonalShiftValidation: true,
	})
	require.NoError(t, err)
	require.NotNil(t, g.AvailabilityZoneDistribution)
	assert.Equal(t, "balanced-best-effort", g.AvailabilityZoneDistribution.CapacityDistributionStrategy)
	require.NotNil(t, g.AvailabilityZoneImpairmentPolicy)
	assert.Equal(t, "ReplaceUnhealthy", g.AvailabilityZoneImpairmentPolicy.ImpairedZoneHealthCheckBehavior)
	assert.True(t, g.AvailabilityZoneImpairmentPolicy.ZonalShiftEnabled)
	require.NotNil(t, g.CapacityReservationSpecification)
	assert.Equal(t, "capacity-reservations-first", g.CapacityReservationSpecification.CapacityReservationPreference)
	assert.Equal(t, "prevent-force-deletion", g.DeletionProtection)
	require.NotNil(t, g.InstanceLifecyclePolicy)
	require.NotNil(t, g.InstanceLifecyclePolicy.RetentionTriggers)
	assert.Equal(t, "retain", g.InstanceLifecyclePolicy.RetentionTriggers.TerminateHookAbandon)
	require.NotNil(t, g.InstanceMaintenancePolicy)
	require.NotNil(t, g.InstanceMaintenancePolicy.MinHealthyPercentage)
	assert.Equal(t, minHealthy, *g.InstanceMaintenancePolicy.MinHealthyPercentage)
	require.NotNil(t, g.InstanceMaintenancePolicy.MaxHealthyPercentage)
	assert.Equal(t, maxHealthy, *g.InstanceMaintenancePolicy.MaxHealthyPercentage)

	// Update: each pointer-struct field is all-or-nothing, matching AWS's
	// opaque-nested-object replace semantics.
	updated, err := b.UpdateAutoScalingGroup(autoscaling.UpdateAutoScalingGroupInput{
		AutoScalingGroupName: "policy-fields-asg",
		AvailabilityZoneDistribution: &autoscaling.AvailabilityZoneDistribution{
			CapacityDistributionStrategy: "balanced-only",
		},
		DeletionProtection: "none",
	})
	require.NoError(t, err)
	assert.Equal(t, "balanced-only", updated.AvailabilityZoneDistribution.CapacityDistributionStrategy)
	assert.Equal(t, "none", updated.DeletionProtection)
	// Fields not present in the update request are untouched.
	require.NotNil(t, updated.InstanceLifecyclePolicy)
	assert.Equal(t, "retain", updated.InstanceLifecyclePolicy.RetentionTriggers.TerminateHookAbandon)
}
