package bedrockagent_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	bedrockagentsdk "github.com/aws/aws-sdk-go-v2/service/bedrockagent"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagent/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAgent_OrchestrationType drives Create/GetAgent and UpdateAgent through
// the real aws-sdk-go-v2 client. api_op_CreateAgent.go's OrchestrationType
// doc: "This is set to DEFAULT orchestration type, by default" -- previously
// dropped end to end (accepted on the wire, never stored, so GetAgent always
// echoed the Go zero value instead of the real SDK's typed
// types.OrchestrationTypeDefault).
func TestAgent_OrchestrationType(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	t.Run("defaults to DEFAULT when omitted", func(t *testing.T) {
		t.Parallel()

		created, err := client.CreateAgent(ctx, &bedrockagentsdk.CreateAgentInput{
			AgentName:            aws.String("orchestration-default-agent"),
			FoundationModel:      aws.String("anthropic.claude-v2"),
			AgentResourceRoleArn: aws.String("arn:aws:iam::123456789012:role/BedrockRole"),
		})
		require.NoError(t, err)
		assert.Equal(t, types.OrchestrationTypeDefault, created.Agent.OrchestrationType)

		got, err := client.GetAgent(ctx, &bedrockagentsdk.GetAgentInput{AgentId: created.Agent.AgentId})
		require.NoError(t, err)
		assert.Equal(t, types.OrchestrationTypeDefault, got.Agent.OrchestrationType)
	})

	t.Run("create honors an explicit value", func(t *testing.T) {
		t.Parallel()

		created, err := client.CreateAgent(ctx, &bedrockagentsdk.CreateAgentInput{
			AgentName:            aws.String("orchestration-custom-agent"),
			FoundationModel:      aws.String("anthropic.claude-v2"),
			AgentResourceRoleArn: aws.String("arn:aws:iam::123456789012:role/BedrockRole"),
			OrchestrationType:    types.OrchestrationTypeCustomOrchestration,
		})
		require.NoError(t, err)
		assert.Equal(t, types.OrchestrationTypeCustomOrchestration, created.Agent.OrchestrationType)

		got, err := client.GetAgent(ctx, &bedrockagentsdk.GetAgentInput{AgentId: created.Agent.AgentId})
		require.NoError(t, err)
		assert.Equal(t, types.OrchestrationTypeCustomOrchestration, got.Agent.OrchestrationType)
	})

	t.Run("update changes it", func(t *testing.T) {
		t.Parallel()

		created, err := client.CreateAgent(ctx, &bedrockagentsdk.CreateAgentInput{
			AgentName:            aws.String("orchestration-update-agent"),
			FoundationModel:      aws.String("anthropic.claude-v2"),
			AgentResourceRoleArn: aws.String("arn:aws:iam::123456789012:role/BedrockRole"),
		})
		require.NoError(t, err)

		updated, err := client.UpdateAgent(ctx, &bedrockagentsdk.UpdateAgentInput{
			AgentId:              created.Agent.AgentId,
			AgentName:            created.Agent.AgentName,
			FoundationModel:      created.Agent.FoundationModel,
			AgentResourceRoleArn: created.Agent.AgentResourceRoleArn,
			OrchestrationType:    types.OrchestrationTypeCustomOrchestration,
		})
		require.NoError(t, err)
		assert.Equal(t, types.OrchestrationTypeCustomOrchestration, updated.Agent.OrchestrationType)

		got, err := client.GetAgent(ctx, &bedrockagentsdk.GetAgentInput{AgentId: created.Agent.AgentId})
		require.NoError(t, err)
		assert.Equal(t, types.OrchestrationTypeCustomOrchestration, got.Agent.OrchestrationType)
	})
}
