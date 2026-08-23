package scheduler_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	schedulersdk "github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetSchedule_EcsNetworkConfigurationWireKeyCase proves gopherstack-r80d's
// mgn/redshiftdata/scheduler batch finding: scheduler@v1.20.4's
// NetworkConfiguration.AwsvpcConfiguration and every member of
// CapacityProviderStrategyItem are lowercase-first on the real wire
// (deserializers.go's awsRestjson1_deserializeDocumentNetworkConfiguration
// switches on "awsvpcConfiguration"; awsRestjson1_deserializeDocumentCapacityProviderStrategyItem
// switches on "capacityProvider"/"base"/"weight"), not the capitalized keys
// gopherstack's scheduleTargetEcsNetworkConfiguration/
// scheduleTargetEcsCapacityProviderStrategyItem previously carried. A real SDK
// client's response deserializer does an exact-case switch, so the wrong-cased
// keys fell into the `default` no-op branch on every decode -- the entire
// AwsvpcConfiguration object, and CapacityProviderStrategyItem's required
// CapacityProvider member (*string, provable) along with it, were invisible to
// any real client on GetSchedule/UpdateSchedule/ListSchedules, independent of
// value.
func TestGetSchedule_EcsNetworkConfigurationWireKeyCase(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	_, err := client.CreateSchedule(t.Context(), &schedulersdk.CreateScheduleInput{
		Name:               aws.String("ecs-wire-key-case"),
		ScheduleExpression: aws.String("rate(1 hour)"),
		FlexibleTimeWindow: &types.FlexibleTimeWindow{Mode: types.FlexibleTimeWindowModeOff},
		Target: &types.Target{
			Arn:     aws.String("arn:aws:ecs:us-east-1:123:cluster/my-cluster"),
			RoleArn: aws.String("arn:aws:iam::123:role/r"),
			EcsParameters: &types.EcsParameters{
				TaskDefinitionArn: aws.String("arn:aws:ecs:us-east-1:123:task-definition/my-td:1"),
				NetworkConfiguration: &types.NetworkConfiguration{
					AwsvpcConfiguration: &types.AwsVpcConfiguration{
						Subnets: []string{"subnet-aaa"},
					},
				},
				CapacityProviderStrategy: []types.CapacityProviderStrategyItem{
					{CapacityProvider: aws.String("FARGATE"), Base: 1, Weight: 2},
				},
			},
		},
	})
	require.NoError(t, err)

	got, err := client.GetSchedule(t.Context(), &schedulersdk.GetScheduleInput{
		Name: aws.String("ecs-wire-key-case"),
	})
	require.NoError(t, err)

	require.NotNil(t, got.Target)
	require.NotNil(t, got.Target.EcsParameters)

	require.NotNil(t, got.Target.EcsParameters.NetworkConfiguration,
		"NetworkConfiguration itself round-trips even before the fix")
	assert.NotNil(t, got.Target.EcsParameters.NetworkConfiguration.AwsvpcConfiguration,
		"AwsvpcConfiguration was silently dropped by the real SDK client's exact-case "+
			"response deserializer before the wire key was fixed to \"awsvpcConfiguration\"")

	require.Len(t, got.Target.EcsParameters.CapacityProviderStrategy, 1)
	item := got.Target.EcsParameters.CapacityProviderStrategy[0]
	assert.Equal(t, "FARGATE", aws.ToString(item.CapacityProvider),
		"CapacityProviderStrategyItem.CapacityProvider is required (types.go) and was "+
			"silently dropped by the real SDK client before the wire key was fixed to "+
			"\"capacityProvider\"")
	assert.Equal(t, int32(1), item.Base)
	assert.Equal(t, int32(2), item.Weight)
}

// TestGetSchedule_AwsVpcConfigurationSubnetsRequiredButReachablyEmpty proves the
// second, independent bug in the same area: scheduler@v1.20.4's
// types.AwsVpcConfiguration.Subnets is "This member is required." (types.go), but
// the real client-side validator (validators.go's validateAwsVpcConfiguration)
// only null-checks it -- `if v.Subnets == nil`, never len-checks -- so a real SDK
// client may legally construct Subnets: []string{} (non-nil, empty) and the
// request serializer (serializers.go's awsRestjson1_serializeDocumentAwsVpcConfiguration,
// `if v.Subnets != nil`) will send it on the wire. gopherstack's own
// scheduleTargetEcsAwsvpcConfiguration.Subnets was tagged `omitempty`, dropping the
// key on any such request, though gopherstack validates EcsParameters.TaskDefinitionArn
// and every other reachable-empty-string candidate on this service's Target -- it
// never validated Subnets's length. This is independent of, and would still have
// been a live bug after, the wire-key-case fix above.
func TestGetSchedule_AwsVpcConfigurationSubnetsRequiredButReachablyEmpty(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	_, err := client.CreateSchedule(t.Context(), &schedulersdk.CreateScheduleInput{
		Name:               aws.String("ecs-empty-subnets"),
		ScheduleExpression: aws.String("rate(1 hour)"),
		FlexibleTimeWindow: &types.FlexibleTimeWindow{Mode: types.FlexibleTimeWindowModeOff},
		Target: &types.Target{
			Arn:     aws.String("arn:aws:ecs:us-east-1:123:cluster/my-cluster"),
			RoleArn: aws.String("arn:aws:iam::123:role/r"),
			EcsParameters: &types.EcsParameters{
				TaskDefinitionArn: aws.String("arn:aws:ecs:us-east-1:123:task-definition/my-td:1"),
				NetworkConfiguration: &types.NetworkConfiguration{
					AwsvpcConfiguration: &types.AwsVpcConfiguration{
						Subnets: []string{},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	got, err := client.GetSchedule(t.Context(), &schedulersdk.GetScheduleInput{
		Name: aws.String("ecs-empty-subnets"),
	})
	require.NoError(t, err)

	require.NotNil(t, got.Target)
	require.NotNil(t, got.Target.EcsParameters)
	require.NotNil(t, got.Target.EcsParameters.NetworkConfiguration)
	require.NotNil(t, got.Target.EcsParameters.NetworkConfiguration.AwsvpcConfiguration)
	assert.NotNil(t, got.Target.EcsParameters.NetworkConfiguration.AwsvpcConfiguration.Subnets,
		"AwsVpcConfiguration.Subnets is required (types.go) and reachably empty "+
			"(validateAwsVpcConfiguration only null-checks); omitempty must not drop it")
}
