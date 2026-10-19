package eventbridge_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	gsebe "github.com/blackbirdworks/gopherstack/services/eventbridge"
)

func TestListTargets_EcsParametersWireKeyCase_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		rule string
	}{
		{name: "rule_a", rule: "ecs-case-a"},
		{name: "rule_b", rule: "ecs-case-b"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestEventBridgeClient(t, gsebe.NewHandler(gsebe.NewInMemoryBackend()))
			_, err := client.PutRule(t.Context(), &eventbridge.PutRuleInput{
				Name:         aws.String(tt.rule),
				EventPattern: aws.String(`{"source":["x"]}`),
			})
			require.NoError(t, err)

			put, err := client.PutTargets(t.Context(), &eventbridge.PutTargetsInput{
				Rule: aws.String(tt.rule),
				Targets: []ebtypes.Target{{
					Id:      aws.String("t1"),
					Arn:     aws.String("arn:aws:ecs:us-east-1:123456789012:cluster/c"),
					RoleArn: aws.String("arn:aws:iam::123456789012:role/r"),
					EcsParameters: &ebtypes.EcsParameters{
						TaskDefinitionArn: aws.String("arn:aws:ecs:us-east-1:123456789012:task-definition/td:1"),
						NetworkConfiguration: &ebtypes.NetworkConfiguration{
							AwsvpcConfiguration: &ebtypes.AwsVpcConfiguration{Subnets: []string{"subnet-aaa"}},
						},
						CapacityProviderStrategy: []ebtypes.CapacityProviderStrategyItem{
							{CapacityProvider: aws.String("FARGATE"), Weight: 2, Base: 1},
						},
						PlacementConstraints: []ebtypes.PlacementConstraint{
							{Type: ebtypes.PlacementConstraintTypeMemberOf, Expression: aws.String("attribute:x")},
						},
						PlacementStrategy: []ebtypes.PlacementStrategy{
							{Type: ebtypes.PlacementStrategyTypeSpread, Field: aws.String("host")},
						},
					},
				}},
			})
			require.NoError(t, err)
			require.Zero(t, put.FailedEntryCount)

			got, err := client.ListTargetsByRule(t.Context(),
				&eventbridge.ListTargetsByRuleInput{Rule: aws.String(tt.rule)})
			require.NoError(t, err)
			require.Len(t, got.Targets, 1)

			ecs := got.Targets[0].EcsParameters
			require.NotNil(t, ecs)
			require.NotNil(t, ecs.NetworkConfiguration)
			require.NotNil(t, ecs.NetworkConfiguration.AwsvpcConfiguration)
			assert.Equal(t, []string{"subnet-aaa"}, ecs.NetworkConfiguration.AwsvpcConfiguration.Subnets)
			require.Len(t, ecs.CapacityProviderStrategy, 1)
			assert.Equal(t, "FARGATE", aws.ToString(ecs.CapacityProviderStrategy[0].CapacityProvider))
			assert.Equal(t, int32(2), ecs.CapacityProviderStrategy[0].Weight)
			assert.Equal(t, int32(1), ecs.CapacityProviderStrategy[0].Base)
			require.Len(t, ecs.PlacementConstraints, 1)
			assert.Equal(t, ebtypes.PlacementConstraintTypeMemberOf, ecs.PlacementConstraints[0].Type)
			assert.Equal(t, "attribute:x", aws.ToString(ecs.PlacementConstraints[0].Expression))
			require.Len(t, ecs.PlacementStrategy, 1)
			assert.Equal(t, ebtypes.PlacementStrategyTypeSpread, ecs.PlacementStrategy[0].Type)
			assert.Equal(t, "host", aws.ToString(ecs.PlacementStrategy[0].Field))
		})
	}
}
