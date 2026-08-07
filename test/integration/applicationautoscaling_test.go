package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	applicationautoscalingsdk "github.com/aws/aws-sdk-go-v2/service/applicationautoscaling"
	aastypes "github.com/aws/aws-sdk-go-v2/service/applicationautoscaling/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_ApplicationAutoScaling_ScalableTargetAndPolicyLifecycle
// exercises the core Application Auto Scaling control-plane workflow:
// register a scalable target, describe it, attach a target-tracking scaling
// policy, describe/delete the policy, then deregister the target. Primary
// integration coverage for the AnyScaleFrontendService JSON-RPC handler.
func TestIntegration_ApplicationAutoScaling_ScalableTargetAndPolicyLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createApplicationAutoScalingClient(t)
	ctx := t.Context()

	const (
		resourceID = "service/it-cluster/it-service"
		policyName = "it-aas-policy"
	)

	namespace := aastypes.ServiceNamespaceEcs
	dimension := aastypes.ScalableDimensionECSServiceDesiredCount

	// RegisterScalableTarget.
	_, err := client.RegisterScalableTarget(ctx, &applicationautoscalingsdk.RegisterScalableTargetInput{
		ServiceNamespace:  namespace,
		ResourceId:        aws.String(resourceID),
		ScalableDimension: dimension,
		MinCapacity:       aws.Int32(1),
		MaxCapacity:       aws.Int32(10),
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeregisterScalableTarget(cleanupCtx, &applicationautoscalingsdk.DeregisterScalableTargetInput{
			ServiceNamespace:  namespace,
			ResourceId:        aws.String(resourceID),
			ScalableDimension: dimension,
		})
	})

	// DescribeScalableTargets.
	descOut, err := client.DescribeScalableTargets(ctx, &applicationautoscalingsdk.DescribeScalableTargetsInput{
		ServiceNamespace: namespace,
		ResourceIds:      []string{resourceID},
	})
	require.NoError(t, err)
	require.Len(t, descOut.ScalableTargets, 1)
	assert.Equal(t, resourceID, aws.ToString(descOut.ScalableTargets[0].ResourceId))
	assert.Equal(t, int32(1), aws.ToInt32(descOut.ScalableTargets[0].MinCapacity))
	assert.Equal(t, int32(10), aws.ToInt32(descOut.ScalableTargets[0].MaxCapacity))

	// PutScalingPolicy (target tracking).
	putOut, err := client.PutScalingPolicy(ctx, &applicationautoscalingsdk.PutScalingPolicyInput{
		PolicyName:        aws.String(policyName),
		ServiceNamespace:  namespace,
		ResourceId:        aws.String(resourceID),
		ScalableDimension: dimension,
		PolicyType:        aastypes.PolicyTypeTargetTrackingScaling,
		TargetTrackingScalingPolicyConfiguration: &aastypes.TargetTrackingScalingPolicyConfiguration{
			TargetValue: aws.Float64(70.0),
			PredefinedMetricSpecification: &aastypes.PredefinedMetricSpecification{
				PredefinedMetricType: aastypes.MetricTypeECSServiceAverageCPUUtilization,
			},
		},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(putOut.PolicyARN))

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteScalingPolicy(cleanupCtx, &applicationautoscalingsdk.DeleteScalingPolicyInput{
			PolicyName:        aws.String(policyName),
			ServiceNamespace:  namespace,
			ResourceId:        aws.String(resourceID),
			ScalableDimension: dimension,
		})
	})

	// DescribeScalingPolicies should include the new policy.
	descPolOut, err := client.DescribeScalingPolicies(ctx, &applicationautoscalingsdk.DescribeScalingPoliciesInput{
		ServiceNamespace: namespace,
		ResourceId:       aws.String(resourceID),
	})
	require.NoError(t, err)

	foundPolicy := false

	for _, p := range descPolOut.ScalingPolicies {
		if aws.ToString(p.PolicyName) == policyName {
			foundPolicy = true

			break
		}
	}

	assert.True(t, foundPolicy, "newly created scaling policy should be listed")
}
