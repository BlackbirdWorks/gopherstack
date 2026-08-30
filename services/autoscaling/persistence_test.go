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

// Test_SnapshotRestore_FullState is the Phase 3.3 (pkgs/store conversion)
// round-trip test: it seeds one instance of every resource kind the backend
// tracks -- both the store.Table-backed resources (groups, launch
// configurations, warm pools, scheduled actions, lifecycle hooks, scaling
// policies) and the resources deliberately left as raw maps by the
// conversion (activities, instance refreshes, notification configs) --
// snapshots the backend, restores into a fresh one, and asserts every
// resource survived the round-trip with its identifying fields intact.
func Test_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	src := autoscaling.NewInMemoryBackend()

	group, err := src.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
		AutoScalingGroupName:    "full-state-asg",
		LaunchConfigurationName: "full-state-lc",
		MinSize:                 1,
		MaxSize:                 5,
		DesiredCapacity:         1,
		AvailabilityZones:       []string{"us-east-1a"},
		LifecycleHookSpecificationList: []autoscaling.LifecycleHook{
			{
				LifecycleHookName:   "launch-hook",
				LifecycleTransition: "autoscaling:EC2_INSTANCE_LAUNCHING",
				HeartbeatTimeout:    60,
				DefaultResult:       "CONTINUE",
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, group)

	_, err = src.CreateLaunchConfiguration(autoscaling.CreateLaunchConfigurationInput{
		LaunchConfigurationName: "full-state-lc",
		ImageID:                 "ami-full-state",
		InstanceType:            "t3.micro",
	})
	require.NoError(t, err)

	// CreateAutoScalingGroup already records one "Launching a new EC2 instance"
	// scaling activity, exercising the raw (non-Table) activities map.
	acts, err := src.DescribeScalingActivities("full-state-asg", nil)
	require.NoError(t, err)
	require.NotEmpty(t, acts)

	require.NoError(t, src.AddInstanceRefresh(autoscaling.InstanceRefresh{
		InstanceRefreshID:    "refresh-full-state",
		AutoScalingGroupName: "full-state-asg",
		Status:               "InProgress",
	}))

	// A second lifecycle hook, added directly via PutLifecycleHook (rather
	// than at CreateAutoScalingGroup time), exercises the composite-key
	// store.Table + store.Index path for lifecycleHooks.
	require.NoError(t, src.PutLifecycleHook(autoscaling.LifecycleHook{
		AutoScalingGroupName: "full-state-asg",
		LifecycleHookName:    "terminate-hook",
		LifecycleTransition:  "autoscaling:EC2_INSTANCE_TERMINATING",
		HeartbeatTimeout:     120,
		DefaultResult:        "ABANDON",
	}))

	_, err = src.PutScalingPolicy(autoscaling.ScalingPolicyInput{
		AutoScalingGroupName: "full-state-asg",
		PolicyName:           "full-state-policy",
		PolicyType:           "SimpleScaling",
		AdjustmentType:       "ChangeInCapacity",
		ScalingAdjustment:    1,
	})
	require.NoError(t, err)

	require.NoError(t, src.PutScheduledUpdateGroupAction("full-state-asg", autoscaling.ScheduledUpdateGroupAction{
		ScheduledActionName: "full-state-schedule",
		Recurrence:          "0 0 * * *",
	}))

	require.NoError(t, src.PutNotificationConfiguration(
		"full-state-asg",
		"arn:aws:sns:us-east-1:123456789012:full-state-topic",
		[]string{"autoscaling:TEST_NOTIFICATION"},
	))

	require.NoError(t, src.PutWarmPool(autoscaling.WarmPoolInput{
		AutoScalingGroupName: "full-state-asg",
		MinSize:              1,
	}))

	data := src.Snapshot(ctx)
	require.NotEmpty(t, data)

	dst := autoscaling.NewInMemoryBackend()
	require.NoError(t, dst.Restore(ctx, data))

	groups, err := dst.DescribeAutoScalingGroups(nil, nil)
	require.NoError(t, err)
	require.Len(t, groups, 1)
	assert.Equal(t, "full-state-asg", groups[0].AutoScalingGroupName)
	assert.Equal(t, int32(1), groups[0].MinSize)
	assert.Equal(t, int32(5), groups[0].MaxSize)

	lcs, err := dst.DescribeLaunchConfigurations(nil)
	require.NoError(t, err)
	require.Len(t, lcs, 1)
	assert.Equal(t, "full-state-lc", lcs[0].LaunchConfigurationName)

	restoredActs, err := dst.DescribeScalingActivities("full-state-asg", nil)
	require.NoError(t, err)
	assert.NotEmpty(t, restoredActs)

	refreshes, err := dst.DescribeInstanceRefreshes("full-state-asg", nil)
	require.NoError(t, err)
	require.Len(t, refreshes, 1)
	assert.Equal(t, "refresh-full-state", refreshes[0].InstanceRefreshID)

	hooks, err := dst.DescribeLifecycleHooks("full-state-asg", nil)
	require.NoError(t, err)
	require.Len(t, hooks, 2)

	policies, err := dst.DescribePolicies("full-state-asg", nil, nil)
	require.NoError(t, err)
	require.Len(t, policies, 1)
	assert.Equal(t, "full-state-policy", policies[0].PolicyName)

	schedules, err := dst.DescribeScheduledActions("full-state-asg", nil, time.Time{}, time.Time{})
	require.NoError(t, err)
	require.Len(t, schedules, 1)
	assert.Equal(t, "full-state-schedule", schedules[0].ScheduledActionName)

	notifs, err := dst.DescribeNotificationConfigurations([]string{"full-state-asg"})
	require.NoError(t, err)
	require.Len(t, notifs, 1)
	assert.Equal(t, "arn:aws:sns:us-east-1:123456789012:full-state-topic", notifs[0].TopicARN)

	wp, err := dst.DescribeWarmPool("full-state-asg")
	require.NoError(t, err)
	require.NotNil(t, wp)
	assert.Equal(t, int32(1), wp.MinSize)

	// A second restore into the same backend must fully replace state, not
	// accumulate it (registry.RestoreAll resets every table first).
	require.NoError(t, dst.Restore(ctx, data))

	groupsAfterSecondRestore, err := dst.DescribeAutoScalingGroups(nil, nil)
	require.NoError(t, err)
	assert.Len(t, groupsAfterSecondRestore, 1)
}

// Test_Restore_IncompatibleVersion verifies that Restore discards a snapshot
// whose version does not match the current shape (e.g. a pre-Phase-3.3
// snapshot with no version field, or a malformed/foreign payload) instead of
// attempting to partially decode it -- it should reset to an empty backend
// and return nil, not an error.
func Test_Restore_IncompatibleVersion(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := autoscaling.NewInMemoryBackend()

	_, err := b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
		AutoScalingGroupName: "pre-existing-asg",
		MinSize:              1,
		MaxSize:              1,
	})
	require.NoError(t, err)

	// Simulates a pre-Phase-3.3 (unversioned) or otherwise incompatible
	// snapshot payload.
	err = b.Restore(ctx, []byte(`{"version":0,"tables":{}}`))
	require.NoError(t, err)

	groups, err := b.DescribeAutoScalingGroups(nil, nil)
	require.NoError(t, err)
	assert.Empty(t, groups)
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

				policies, err := b.DescribePolicies("persist-asg", nil, nil)
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
			name: "snapshot_restore_preserves_customized_metric_specification",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "customized-metric-persist-asg",
					MinSize:              1,
					MaxSize:              3,
				})
				_, _ = b.PutScalingPolicy(autoscaling.ScalingPolicyInput{
					PolicyName:           "customized-metric-policy",
					AutoScalingGroupName: "customized-metric-persist-asg",
					PolicyType:           "TargetTrackingScaling",
					TargetValue:          50,
					CustomizedMetricSpecification: &autoscaling.CustomizedMetricSpecification{
						Metrics: []autoscaling.MetricDataQuery{
							{
								ID: "m1",
								MetricStat: &autoscaling.MetricDataStat{
									Metric: &autoscaling.MetricRef{
										MetricName: "CPUUtilization",
										Namespace:  "AWS/EC2",
										Dimensions: []autoscaling.MetricDimension{
											{Name: "AutoScalingGroupName", Value: "customized-metric-persist-asg"},
										},
									},
									Stat: "Average",
								},
							},
						},
					},
				})
			},
			check: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				policies, err := b.DescribePolicies("customized-metric-persist-asg", nil, nil)
				require.NoError(t, err)
				require.Len(t, policies, 1)

				spec := policies[0].CustomizedMetricSpecification
				require.NotNil(t, spec)
				require.Len(t, spec.Metrics, 1)
				assert.Equal(t, "m1", spec.Metrics[0].ID)
				require.NotNil(t, spec.Metrics[0].MetricStat)
				require.NotNil(t, spec.Metrics[0].MetricStat.Metric)
				assert.Equal(t, "CPUUtilization", spec.Metrics[0].MetricStat.Metric.MetricName)
				require.Len(t, spec.Metrics[0].MetricStat.Metric.Dimensions, 1)
				assert.Equal(t, "AutoScalingGroupName", spec.Metrics[0].MetricStat.Metric.Dimensions[0].Name)
			},
		},
		{
			name: "snapshot_restore_preserves_baseline_performance_factors",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "baseline-perf-persist-asg",
					MinSize:              1,
					MaxSize:              3,
					MixedInstancesPolicy: &autoscaling.MixedInstancesPolicy{
						LaunchTemplate: autoscaling.MixedInstancesLaunchTemplate{
							Overrides: []autoscaling.LaunchTemplateOverride{
								{
									InstanceRequirements: &autoscaling.InstanceRequirements{
										BaselinePerformanceFactors: &autoscaling.BaselinePerformanceFactors{
											CPU: &autoscaling.CPUPerformanceFactor{
												References: []autoscaling.PerformanceFactorReference{
													{InstanceFamily: "m5"},
												},
											},
										},
									},
								},
							},
						},
					},
				})
			},
			check: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				groups, err := b.DescribeAutoScalingGroups([]string{"baseline-perf-persist-asg"}, nil)
				require.NoError(t, err)
				require.Len(t, groups, 1)
				require.NotNil(t, groups[0].MixedInstancesPolicy)
				require.Len(t, groups[0].MixedInstancesPolicy.LaunchTemplate.Overrides, 1)

				ir := groups[0].MixedInstancesPolicy.LaunchTemplate.Overrides[0].InstanceRequirements
				require.NotNil(t, ir)
				require.NotNil(t, ir.BaselinePerformanceFactors)
				require.NotNil(t, ir.BaselinePerformanceFactors.CPU)
				require.Len(t, ir.BaselinePerformanceFactors.CPU.References, 1)
				assert.Equal(t, "m5", ir.BaselinePerformanceFactors.CPU.References[0].InstanceFamily)
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

				groups, _ := b.DescribeAutoScalingGroups([]string{"idx-asg"}, nil)
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

			snap := b1.Snapshot(t.Context())
			require.NotNil(t, snap)

			b2 := autoscaling.NewInMemoryBackend()
			err := b2.Restore(t.Context(), snap)
			require.NoError(t, err)

			tt.check(t, b2)
		})
	}
}

func TestAutoscalingHandler_Persistence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *autoscaling.Handler)
		name string
	}{
		{
			name: "snapshot_and_restore",
			run: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()

				postAutoscalingForm(t, h, "Action=CreateAutoScalingGroup&Version=2011-01-01"+
					"&AutoScalingGroupName=snap-asg&MinSize=1&MaxSize=3")

				data := h.Snapshot(t.Context())
				require.NotNil(t, data)

				h2 := newAutoscalingHandler()
				err := h2.Restore(t.Context(), data)
				require.NoError(t, err)

				rec := postAutoscalingForm(t, h2, "Action=DescribeAutoScalingGroups&Version=2011-01-01")
				assert.Equal(t, http.StatusOK, rec.Code)
				assert.Contains(t, rec.Body.String(), "snap-asg")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()
			tt.run(t, h)
		})
	}
}
