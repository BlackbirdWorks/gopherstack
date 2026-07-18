package cloudformation_test

import (
	"testing"

	appautoscalingbackend "github.com/blackbirdworks/gopherstack/services/applicationautoscaling"
	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestResourceCreator_AppAutoScaling_ScalableTarget_CreateDelete verifies
// scalable target is registered in the backend and deregistered on delete.
func TestResourceCreator_AppAutoScaling_ScalableTarget_CreateDelete(t *testing.T) {
	t.Parallel()

	backends := newExtraServiceBackends(t)
	rc := cloudformation.NewResourceCreator(backends)

	props := map[string]any{
		"ServiceNamespace":  "ecs",
		"ResourceId":        "service/my-cluster/my-service",
		"ScalableDimension": "ecs:service:DesiredCount",
		"MinCapacity":       float64(2),
		"MaxCapacity":       float64(20),
	}

	physID, err := rc.Create(t.Context(), "MyTarget", "AWS::ApplicationAutoScaling::ScalableTarget", props, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, physID)
	assert.Contains(t, physID, "scalable-target")

	// Verify it is registered in the backend.
	targets, _ := backends.AppAutoScaling.Backend.DescribeScalableTargets(
		appautoscalingbackend.DescribeScalableTargetsFilter{},
	)
	assert.Len(t, targets, 1)
	assert.Equal(t, "service/my-cluster/my-service", targets[0].ResourceID)

	err = rc.Delete(t.Context(), "AWS::ApplicationAutoScaling::ScalableTarget", physID, nil)
	require.NoError(t, err)

	// Verify it is deregistered.
	targets, _ = backends.AppAutoScaling.Backend.DescribeScalableTargets(
		appautoscalingbackend.DescribeScalableTargetsFilter{},
	)
	assert.Empty(t, targets)
}

// TestResourceCreator_AppAutoScaling_ScalingPolicy_CreateDelete verifies
// scaling policy is created and deleted correctly.
func TestResourceCreator_AppAutoScaling_ScalingPolicy_CreateDelete(t *testing.T) {
	t.Parallel()

	backends := newExtraServiceBackends(t)
	rc := cloudformation.NewResourceCreator(backends)

	props := map[string]any{
		"PolicyName":        "cfn-test-policy",
		"ServiceNamespace":  "ecs",
		"ResourceId":        "service/default/svc",
		"ScalableDimension": "ecs:service:DesiredCount",
		"PolicyType":        "TargetTrackingScaling",
	}

	physID, err := rc.Create(t.Context(), "MyPolicy", "AWS::ApplicationAutoScaling::ScalingPolicy", props, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, physID)

	policies, _ := backends.AppAutoScaling.Backend.DescribeScalingPolicies(
		appautoscalingbackend.DescribeScalingPoliciesFilter{},
	)
	assert.Len(t, policies, 1)
	assert.Equal(t, "cfn-test-policy", policies[0].PolicyName)

	err = rc.Delete(t.Context(), "AWS::ApplicationAutoScaling::ScalingPolicy", physID, nil)
	require.NoError(t, err)

	policies, _ = backends.AppAutoScaling.Backend.DescribeScalingPolicies(
		appautoscalingbackend.DescribeScalingPoliciesFilter{},
	)
	assert.Empty(t, policies)
}
