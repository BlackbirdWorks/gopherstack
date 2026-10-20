package pipes_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	pipessdk "github.com/aws/aws-sdk-go-v2/service/pipes"
	pipestypes "github.com/aws/aws-sdk-go-v2/service/pipes/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/pipes"
)

func TestRealClient_EcsTargetWireKeyCase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "ecs-wire-case-a"},
		{name: "ecs-wire-case-b"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestPipesClient(t, pipes.NewHandler(pipes.NewInMemoryBackend("123456789012", "us-east-1")))

			_, err := client.CreatePipe(t.Context(), &pipessdk.CreatePipeInput{
				Name:    aws.String(tt.name),
				RoleArn: aws.String("arn:aws:iam::123456789012:role/r"),
				Source:  aws.String("arn:aws:sqs:us-east-1:123456789012:q"),
				Target:  aws.String("arn:aws:ecs:us-east-1:123456789012:cluster/cluster"),
				TargetParameters: &pipestypes.PipeTargetParameters{
					EcsTaskParameters: &pipestypes.PipeTargetEcsTaskParameters{
						TaskDefinitionArn: aws.String("arn:aws:ecs:us-east-1:123456789012:task-definition/td:1"),
						NetworkConfiguration: &pipestypes.NetworkConfiguration{
							AwsvpcConfiguration: &pipestypes.AwsVpcConfiguration{Subnets: []string{"subnet-aaa"}},
						},
						CapacityProviderStrategy: []pipestypes.CapacityProviderStrategyItem{
							{CapacityProvider: aws.String("FARGATE"), Weight: 2, Base: 1},
						},
						PlacementConstraints: []pipestypes.PlacementConstraint{
							{Type: pipestypes.PlacementConstraintTypeMemberOf, Expression: aws.String("attribute:x")},
						},
						PlacementStrategy: []pipestypes.PlacementStrategy{
							{Type: pipestypes.PlacementStrategyTypeSpread, Field: aws.String("host")},
						},
					},
				},
			})
			require.NoError(t, err)

			got, err := client.DescribePipe(t.Context(), &pipessdk.DescribePipeInput{Name: aws.String(tt.name)})
			require.NoError(t, err)

			ecs := got.TargetParameters.EcsTaskParameters
			require.NotNil(t, ecs)
			require.NotNil(t, ecs.NetworkConfiguration)
			require.NotNil(t, ecs.NetworkConfiguration.AwsvpcConfiguration)
			assert.Equal(t, []string{"subnet-aaa"}, ecs.NetworkConfiguration.AwsvpcConfiguration.Subnets)
			require.Len(t, ecs.CapacityProviderStrategy, 1)
			assert.Equal(t, "FARGATE", aws.ToString(ecs.CapacityProviderStrategy[0].CapacityProvider))
			assert.Equal(t, int32(2), ecs.CapacityProviderStrategy[0].Weight)
			assert.Equal(t, int32(1), ecs.CapacityProviderStrategy[0].Base)
			require.Len(t, ecs.PlacementConstraints, 1)
			assert.Equal(t, pipestypes.PlacementConstraintTypeMemberOf, ecs.PlacementConstraints[0].Type)
			assert.Equal(t, "attribute:x", aws.ToString(ecs.PlacementConstraints[0].Expression))
			require.Len(t, ecs.PlacementStrategy, 1)
			assert.Equal(t, pipestypes.PlacementStrategyTypeSpread, ecs.PlacementStrategy[0].Type)
			assert.Equal(t, "host", aws.ToString(ecs.PlacementStrategy[0].Field))
		})
	}
}

func TestRealClient_KafkaConsumerGroupWireKeyCase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		params *pipestypes.PipeSourceParameters
		name   string
		source string
	}{
		{
			name:   "self-managed",
			source: "arn:aws:kafka:us-east-1:123456789012:cluster/c/t",
			params: &pipestypes.PipeSourceParameters{
				SelfManagedKafkaParameters: &pipestypes.PipeSourceSelfManagedKafkaParameters{
					TopicName:       aws.String("t"),
					ConsumerGroupID: aws.String("grp"),
				},
			},
		},
		{
			name:   "msk",
			source: "arn:aws:kafka:us-east-1:123456789012:cluster/c/t",
			params: &pipestypes.PipeSourceParameters{
				ManagedStreamingKafkaParameters: &pipestypes.PipeSourceManagedStreamingKafkaParameters{
					TopicName:       aws.String("t"),
					ConsumerGroupID: aws.String("grp"),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestPipesClient(t, pipes.NewHandler(pipes.NewInMemoryBackend("123456789012", "us-east-1")))

			_, err := client.CreatePipe(t.Context(), &pipessdk.CreatePipeInput{
				Name:             aws.String("kafka-" + tt.name),
				RoleArn:          aws.String("arn:aws:iam::123456789012:role/r"),
				Source:           aws.String(tt.source),
				Target:           aws.String("arn:aws:lambda:us-east-1:123456789012:function:fn"),
				SourceParameters: tt.params,
			})
			require.NoError(t, err)

			got, err := client.DescribePipe(t.Context(),
				&pipessdk.DescribePipeInput{Name: aws.String("kafka-" + tt.name)})
			require.NoError(t, err)

			sp := got.SourceParameters
			require.NotNil(t, sp)

			if tt.params.SelfManagedKafkaParameters != nil {
				require.NotNil(t, sp.SelfManagedKafkaParameters)
				assert.Equal(t, "grp", aws.ToString(sp.SelfManagedKafkaParameters.ConsumerGroupID))

				return
			}

			require.NotNil(t, sp.ManagedStreamingKafkaParameters)
			assert.Equal(t, "grp", aws.ToString(sp.ManagedStreamingKafkaParameters.ConsumerGroupID))
		})
	}
}
