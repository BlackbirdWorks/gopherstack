package autoscaling_test

import (
	"context"
	"testing"

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
	acts, err := src.DescribeScalingActivities("full-state-asg")
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

	groups, err := dst.DescribeAutoScalingGroups(nil)
	require.NoError(t, err)
	require.Len(t, groups, 1)
	assert.Equal(t, "full-state-asg", groups[0].AutoScalingGroupName)
	assert.Equal(t, int32(1), groups[0].MinSize)
	assert.Equal(t, int32(5), groups[0].MaxSize)

	lcs, err := dst.DescribeLaunchConfigurations(nil)
	require.NoError(t, err)
	require.Len(t, lcs, 1)
	assert.Equal(t, "full-state-lc", lcs[0].LaunchConfigurationName)

	restoredActs, err := dst.DescribeScalingActivities("full-state-asg")
	require.NoError(t, err)
	assert.NotEmpty(t, restoredActs)

	refreshes, err := dst.DescribeInstanceRefreshes("full-state-asg", nil)
	require.NoError(t, err)
	require.Len(t, refreshes, 1)
	assert.Equal(t, "refresh-full-state", refreshes[0].InstanceRefreshID)

	hooks, err := dst.DescribeLifecycleHooks("full-state-asg", nil)
	require.NoError(t, err)
	require.Len(t, hooks, 2)

	policies, err := dst.DescribePolicies("full-state-asg", nil)
	require.NoError(t, err)
	require.Len(t, policies, 1)
	assert.Equal(t, "full-state-policy", policies[0].PolicyName)

	schedules, err := dst.DescribeScheduledActions("full-state-asg", nil)
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

	groupsAfterSecondRestore, err := dst.DescribeAutoScalingGroups(nil)
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

	groups, err := b.DescribeAutoScalingGroups(nil)
	require.NoError(t, err)
	assert.Empty(t, groups)
}
