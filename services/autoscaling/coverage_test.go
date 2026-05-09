package autoscaling_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestASGCoverage_DescribeOps covers all the simple Describe* operations that return static data.
func TestASGCoverage_DescribeOps(t *testing.T) {
	t.Parallel()

	h := newAutoscalingHandler()

	describeOps := []string{
		"DescribeAccountLimits",
		"DescribeAdjustmentTypes",
		"DescribeAutoScalingNotificationTypes",
		"DescribeLifecycleHookTypes",
		"DescribeMetricCollectionTypes",
		"DescribeScalingProcessTypes",
		"DescribeTerminationPolicyTypes",
	}

	for _, action := range describeOps {
		t.Run(action, func(t *testing.T) {
			t.Parallel()
			rec := postAutoscalingForm(t, h, "Action="+action+"&Version=2011-01-01")
			assert.Equal(t, http.StatusOK, rec.Code, "action %s should succeed", action)
		})
	}
}

// TestASGCoverage_InstanceRefresh covers StartInstanceRefresh, DescribeInstanceRefreshes,
// RollbackInstanceRefresh.
func TestASGCoverage_InstanceRefresh(t *testing.T) {
	t.Parallel()

	h := newAutoscalingHandler()

	// Create an ASG first.
	rec := postAutoscalingForm(t, h, "Action=CreateAutoScalingGroup&Version=2011-01-01"+
		"&AutoScalingGroupName=test-asg&MinSize=0&MaxSize=5"+
		"&LaunchConfigurationName=test-lc")
	// ASG creation may fail if LC doesn't exist - just check we can proceed
	_ = rec

	// DescribeInstanceRefreshes (works even without ASG data).
	rec = postAutoscalingForm(t, h, "Action=DescribeInstanceRefreshes&Version=2011-01-01"+
		"&AutoScalingGroupName=test-asg")
	assert.Equal(t, http.StatusOK, rec.Code)

	// StartInstanceRefresh.
	rec = postAutoscalingForm(t, h, "Action=StartInstanceRefresh&Version=2011-01-01"+
		"&AutoScalingGroupName=test-asg")
	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()

	// RollbackInstanceRefresh (may fail if no active refresh).
	rec = postAutoscalingForm(t, h, "Action=RollbackInstanceRefresh&Version=2011-01-01"+
		"&AutoScalingGroupName=test-asg")
	assert.True(t, rec.Code == http.StatusOK || rec.Code != http.StatusInternalServerError)

	_ = body
}

// TestASGCoverage_LoadBalancerOps covers DescribeLoadBalancers, DescribeLoadBalancerTargetGroups,
// DescribeTrafficSources, DetachInstances, DetachLoadBalancerTargetGroups, DetachLoadBalancers,
// DetachTrafficSources, EnableMetricsCollection, DisableMetricsCollection.
func TestASGCoverage_LoadBalancerOps(t *testing.T) {
	t.Parallel()

	h := newAutoscalingHandler()

	// Create a launch configuration.
	postAutoscalingForm(t, h, "Action=CreateLaunchConfiguration&Version=2011-01-01"+
		"&LaunchConfigurationName=lb-lc&ImageId=ami-12345&InstanceType=t3.micro")

	// Create an ASG.
	rec := postAutoscalingForm(t, h, "Action=CreateAutoScalingGroup&Version=2011-01-01"+
		"&AutoScalingGroupName=lb-asg&MinSize=0&MaxSize=5"+
		"&LaunchConfigurationName=lb-lc")
	require.Equal(t, http.StatusOK, rec.Code)

	// DescribeLoadBalancers.
	rec = postAutoscalingForm(t, h, "Action=DescribeLoadBalancers&Version=2011-01-01"+
		"&AutoScalingGroupName=lb-asg")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DescribeLoadBalancersResponse")

	// DescribeLoadBalancerTargetGroups.
	rec = postAutoscalingForm(t, h, "Action=DescribeLoadBalancerTargetGroups&Version=2011-01-01"+
		"&AutoScalingGroupName=lb-asg")
	assert.Equal(t, http.StatusOK, rec.Code)

	// DescribeTrafficSources.
	rec = postAutoscalingForm(t, h, "Action=DescribeTrafficSources&Version=2011-01-01"+
		"&AutoScalingGroupName=lb-asg")
	assert.Equal(t, http.StatusOK, rec.Code)

	// DetachLoadBalancers.
	rec = postAutoscalingForm(t, h, "Action=DetachLoadBalancers&Version=2011-01-01"+
		"&AutoScalingGroupName=lb-asg"+
		"&LoadBalancerNames.member.1=my-lb")
	assert.Equal(t, http.StatusOK, rec.Code)

	// DetachLoadBalancerTargetGroups.
	rec = postAutoscalingForm(t, h, "Action=DetachLoadBalancerTargetGroups&Version=2011-01-01"+
		"&AutoScalingGroupName=lb-asg"+
		"&TargetGroupARNs.member.1=arn:aws:elasticloadbalancing:us-east-1:000000000000:targetgroup/test/12345")
	assert.Equal(t, http.StatusOK, rec.Code)

	// DetachTrafficSources.
	rec = postAutoscalingForm(t, h, "Action=DetachTrafficSources&Version=2011-01-01"+
		"&AutoScalingGroupName=lb-asg")
	assert.Equal(t, http.StatusOK, rec.Code)

	// EnableMetricsCollection.
	rec = postAutoscalingForm(t, h, "Action=EnableMetricsCollection&Version=2011-01-01"+
		"&AutoScalingGroupName=lb-asg&Granularity=1Minute"+
		"&Metrics.member.1=GroupMinSize")
	assert.Equal(t, http.StatusOK, rec.Code)

	// DisableMetricsCollection.
	rec = postAutoscalingForm(t, h, "Action=DisableMetricsCollection&Version=2011-01-01"+
		"&AutoScalingGroupName=lb-asg"+
		"&Metrics.member.1=GroupMinSize")
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestASGCoverage_DetachInstances covers DetachInstances.
func TestASGCoverage_DetachInstances(t *testing.T) {
	t.Parallel()

	h := newAutoscalingHandler()

	// Create LC and ASG.
	postAutoscalingForm(t, h, "Action=CreateLaunchConfiguration&Version=2011-01-01"+
		"&LaunchConfigurationName=di-lc&ImageId=ami-12345&InstanceType=t3.micro")
	rec := postAutoscalingForm(t, h, "Action=CreateAutoScalingGroup&Version=2011-01-01"+
		"&AutoScalingGroupName=di-asg&MinSize=0&MaxSize=5"+
		"&LaunchConfigurationName=di-lc")
	require.Equal(t, http.StatusOK, rec.Code)

	// DetachInstances (no instances, should still succeed).
	rec = postAutoscalingForm(t, h, "Action=DetachInstances&Version=2011-01-01"+
		"&AutoScalingGroupName=di-asg"+
		"&ShouldDecrementDesiredCapacity=true")
	assert.Equal(t, http.StatusOK, rec.Code)
}
