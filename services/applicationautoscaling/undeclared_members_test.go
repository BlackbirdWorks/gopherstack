package applicationautoscaling_test

import (
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	aassdk "github.com/aws/aws-sdk-go-v2/service/applicationautoscaling"
	"github.com/aws/aws-sdk-go-v2/service/applicationautoscaling/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_DescribeScalingPolicies_OmitsLastModifiedTime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		absent  string
		present string
	}{
		{name: "scaling policy", absent: "LastModifiedTime", present: "CreationTime"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			seedTarget(t, h, "service/default/my-svc", 1, 10)

			put := doRequest(t, h, "PutScalingPolicy", map[string]any{
				"ServiceNamespace": "ecs", "ResourceId": "service/default/my-svc",
				"ScalableDimension": "ecs:service:DesiredCount", "PolicyName": "p",
				"PolicyType": "StepScaling",
				"StepScalingPolicyConfiguration": map[string]any{
					"AdjustmentType":  "ChangeInCapacity",
					"StepAdjustments": []map[string]any{{"MetricIntervalLowerBound": 0, "ScalingAdjustment": 1}},
				},
			})
			require.Equal(t, http.StatusOK, put.Code, put.Body.String())

			desc := doRequest(t, h, "DescribeScalingPolicies", map[string]any{"ServiceNamespace": "ecs"})
			require.Equal(t, http.StatusOK, desc.Code)
			assert.Contains(t, desc.Body.String(), tc.present)
			assert.NotContains(t, desc.Body.String(), tc.absent)
		})
	}
}

func TestSDK_DescribeScalingPoliciesRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		policy string
	}{
		{name: "first", policy: "p1"},
		{name: "second", policy: "p2"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			client := newTestAASSDKClient(t, h)
			seedTarget(t, h, "service/default/my-svc", 1, 10)

			_, err := client.PutScalingPolicy(t.Context(), &aassdk.PutScalingPolicyInput{
				ServiceNamespace:  types.ServiceNamespaceEcs,
				ResourceId:        aws.String("service/default/my-svc"),
				ScalableDimension: types.ScalableDimensionECSServiceDesiredCount,
				PolicyName:        aws.String(tc.policy),
				PolicyType:        types.PolicyTypeStepScaling,
				StepScalingPolicyConfiguration: &types.StepScalingPolicyConfiguration{
					AdjustmentType: types.AdjustmentTypeChangeInCapacity,
					StepAdjustments: []types.StepAdjustment{
						{MetricIntervalLowerBound: aws.Float64(0), ScalingAdjustment: aws.Int32(1)},
					},
				},
			})
			require.NoError(t, err)

			out, err := client.DescribeScalingPolicies(t.Context(), &aassdk.DescribeScalingPoliciesInput{
				ServiceNamespace: types.ServiceNamespaceEcs,
			})
			require.NoError(t, err)
			require.Len(t, out.ScalingPolicies, 1)
			assert.Equal(t, tc.policy, aws.ToString(out.ScalingPolicies[0].PolicyName))
			assert.NotEmpty(t, aws.ToString(out.ScalingPolicies[0].PolicyARN))
			assert.NotNil(t, out.ScalingPolicies[0].CreationTime)
		})
	}
}
