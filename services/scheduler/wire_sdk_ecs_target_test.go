package scheduler_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	schedulersdk "github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateSchedule_EcsParametersSDKRoundTrip drives CreateSchedule/GetSchedule
// through the real aws-sdk-go-v2 scheduler client (not gopherstack's own JSON
// tags) and asserts every EcsParameters nested field the real client's own
// case-sensitive deserializer parses back out. This is the shape that broke
// under gopherstack-2mwl-class PascalCase-vs-lower-camel key mismatches: the
// real SDK's generated deserializers switch on an exact-case key with a
// default no-op, so a wrong-case wire key doesn't error -- it silently comes
// back nil/empty, which this test catches by asserting non-empty values.
func TestCreateSchedule_EcsParametersSDKRoundTrip(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	_, err := client.CreateSchedule(t.Context(), &schedulersdk.CreateScheduleInput{
		Name:               aws.String("ecs-sdk-roundtrip"),
		ScheduleExpression: aws.String("rate(1 hour)"),
		FlexibleTimeWindow: &types.FlexibleTimeWindow{Mode: types.FlexibleTimeWindowModeOff},
		Target: &types.Target{
			Arn:     aws.String("arn:aws:ecs:us-east-1:123456789012:cluster/my-cluster"),
			RoleArn: aws.String("arn:aws:iam::123456789012:role/r"),
			EcsParameters: &types.EcsParameters{
				TaskDefinitionArn: aws.String("arn:aws:ecs:us-east-1:123456789012:task-definition/td:1"),
				NetworkConfiguration: &types.NetworkConfiguration{
					AwsvpcConfiguration: &types.AwsVpcConfiguration{
						Subnets:        []string{"subnet-aaa", "subnet-bbb"},
						SecurityGroups: []string{"sg-ccc"},
						AssignPublicIp: types.AssignPublicIpEnabled,
					},
				},
				CapacityProviderStrategy: []types.CapacityProviderStrategyItem{
					{CapacityProvider: aws.String("FARGATE"), Weight: 1},
					{CapacityProvider: aws.String("FARGATE_SPOT"), Weight: 2},
				},
				PlacementConstraints: []types.PlacementConstraint{
					{
						Type:       types.PlacementConstraintTypeMemberOf,
						Expression: aws.String("attribute:ecs.instance-type =~ g2.*"),
					},
				},
				PlacementStrategy: []types.PlacementStrategy{
					{Type: types.PlacementStrategyTypeSpread, Field: aws.String("attribute:ecs.availability-zone")},
				},
				Tags: []map[string]string{
					{"env": "prod"},
					{"team": "data"},
				},
			},
		},
	})
	require.NoError(t, err)

	got, err := client.GetSchedule(t.Context(), &schedulersdk.GetScheduleInput{
		Name: aws.String("ecs-sdk-roundtrip"),
	})
	require.NoError(t, err)

	require.NotNil(t, got.Target)
	require.NotNil(t, got.Target.EcsParameters)
	ecs := got.Target.EcsParameters

	require.NotNil(t, ecs.NetworkConfiguration, "NetworkConfiguration must survive the round trip")
	require.NotNil(t, ecs.NetworkConfiguration.AwsvpcConfiguration,
		"awsvpcConfiguration must survive the round trip (real SDK wraps it under a lower-camel key)")
	vpc := ecs.NetworkConfiguration.AwsvpcConfiguration
	assert.Equal(t, []string{"subnet-aaa", "subnet-bbb"}, vpc.Subnets)
	assert.Equal(t, []string{"sg-ccc"}, vpc.SecurityGroups)
	assert.Equal(t, types.AssignPublicIpEnabled, vpc.AssignPublicIp)

	require.Len(t, ecs.CapacityProviderStrategy, 2,
		"CapacityProviderStrategy items must survive the round trip (real SDK uses lower-camel keys)")
	assert.Equal(t, "FARGATE", aws.ToString(ecs.CapacityProviderStrategy[0].CapacityProvider))
	assert.Equal(t, int32(1), ecs.CapacityProviderStrategy[0].Weight)
	assert.Equal(t, "FARGATE_SPOT", aws.ToString(ecs.CapacityProviderStrategy[1].CapacityProvider))
	assert.Equal(t, int32(2), ecs.CapacityProviderStrategy[1].Weight)

	require.Len(t, ecs.PlacementConstraints, 1,
		"PlacementConstraints must survive the round trip (real SDK uses lower-camel keys)")
	assert.Equal(t, types.PlacementConstraintTypeMemberOf, ecs.PlacementConstraints[0].Type)
	assert.Equal(t, "attribute:ecs.instance-type =~ g2.*", aws.ToString(ecs.PlacementConstraints[0].Expression))

	require.Len(t, ecs.PlacementStrategy, 1,
		"PlacementStrategy must survive the round trip (real SDK uses lower-camel keys)")
	assert.Equal(t, types.PlacementStrategyTypeSpread, ecs.PlacementStrategy[0].Type)
	assert.Equal(t, "attribute:ecs.availability-zone", aws.ToString(ecs.PlacementStrategy[0].Field))

	require.Len(t, ecs.Tags, 2,
		"Tags must survive the round trip as free-form maps, not {Key,Value} objects")
	assert.Equal(t, map[string]string{"env": "prod"}, ecs.Tags[0])
	assert.Equal(t, map[string]string{"team": "data"}, ecs.Tags[1])
}
