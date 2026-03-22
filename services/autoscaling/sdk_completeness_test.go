package autoscaling_test

import (
	"testing"

	autoscalingsdk "github.com/aws/aws-sdk-go-v2/service/autoscaling"
	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/autoscaling"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// autoscaling client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := autoscaling.NewInMemoryBackend()
	h := autoscaling.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &autoscalingsdk.Client{}, h.GetSupportedOperations(), []string{
		"AttachInstances",
		"AttachLoadBalancerTargetGroups",
		"AttachLoadBalancers",
		"AttachTrafficSources",
		"BatchDeleteScheduledAction",
		"BatchPutScheduledUpdateGroupAction",
		"CancelInstanceRefresh",
		"CompleteLifecycleAction",
		"CreateOrUpdateTags",
		"DeleteLifecycleHook",
		"DeleteNotificationConfiguration",
		"DeletePolicy",
		"DeleteScheduledAction",
		"DeleteTags",
		"DeleteWarmPool",
		"DescribeAccountLimits",
		"DescribeAdjustmentTypes",
		"DescribeAutoScalingGroups",
		"DescribeAutoScalingInstances",
		"DescribeAutoScalingNotificationTypes",
		"DescribeInstanceRefreshes",
		"DescribeLifecycleHookTypes",
		"DescribeLifecycleHooks",
		"DescribeLoadBalancerTargetGroups",
		"DescribeLoadBalancers",
		"DescribeMetricCollectionTypes",
		"DescribeNotificationConfigurations",
		"DescribePolicies",
		"DescribeScalingProcessTypes",
		"DescribeScheduledActions",
		"DescribeTags",
		"DescribeTerminationPolicyTypes",
		"DescribeTrafficSources",
		"DescribeWarmPool",
		"DetachInstances",
		"DetachLoadBalancerTargetGroups",
		"DetachLoadBalancers",
		"DetachTrafficSources",
		"DisableMetricsCollection",
		"EnableMetricsCollection",
		"EnterStandby",
		"ExecutePolicy",
		"ExitStandby",
		"GetPredictiveScalingForecast",
		"LaunchInstances",
		"PutLifecycleHook",
		"PutNotificationConfiguration",
		"PutScalingPolicy",
		"PutScheduledUpdateGroupAction",
		"PutWarmPool",
		"RecordLifecycleActionHeartbeat",
		"ResumeProcesses",
		"RollbackInstanceRefresh",
		"SetDesiredCapacity",
		"SetInstanceHealth",
		"SetInstanceProtection",
		"StartInstanceRefresh",
		"SuspendProcesses",
		"TerminateInstanceInAutoScalingGroup",
	})
}
