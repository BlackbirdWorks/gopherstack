package bedrockagent_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	bedrockagentsdk "github.com/aws/aws-sdk-go-v2/service/bedrockagent"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrockagent"
)

// TestTypedClientSmoke_ListsAndTags drives ListAgents, ListFlows,
// ListPrompts, ListKnowledgeBases, and TagResource through the real
// aws-sdk-go-v2 bedrockagent client (gopherstack-m2eiu), proving the basic
// list-and-tag surface round-trips against a real serializer/deserializer,
// not just this package's own JSON shape.
func TestTypedClientSmoke_ListsAndTags(t *testing.T) {
	t.Parallel()

	backend := bedrockagent.NewTestBackend(rtTestRegion, rtTestAccountID)
	h := bedrockagent.NewTestHandler(backend)
	h.AccountID = rtTestAccountID
	h.DefaultRegion = rtTestRegion
	client := newRoundTripClient(t, h)
	ctx := t.Context()

	agent, err := backend.CreateAgent(ctx, bedrockagent.AgentConfig{
		AgentName: "smoke-agent",
		RoleARN:   "arn:aws:iam::123456789012:role/agent-role",
	})
	require.NoError(t, err)

	flow, err := backend.CreateFlow(ctx, bedrockagent.FlowConfig{
		Name:    "smoke-flow",
		RoleARN: "arn:aws:iam::123456789012:role/flow-role",
	})
	require.NoError(t, err)

	prompt, err := backend.CreatePrompt(ctx, bedrockagent.PromptConfig{Name: "smoke-prompt"})
	require.NoError(t, err)

	kb, err := backend.CreateKnowledgeBase(ctx, bedrockagent.KnowledgeBaseConfig{
		Name:    "smoke-kb",
		RoleARN: "arn:aws:iam::123456789012:role/kb-role",
	})
	require.NoError(t, err)

	tests := []struct {
		check func(t *testing.T)
		name  string
	}{
		{name: "list agents", check: func(t *testing.T) {
			t.Helper()

			out, listErr := client.ListAgents(ctx, &bedrockagentsdk.ListAgentsInput{})
			require.NoError(t, listErr)
			require.Len(t, out.AgentSummaries, 1)
			assert.Equal(t, agent.AgentID, aws.ToString(out.AgentSummaries[0].AgentId))
			assert.Equal(t, agent.AgentName, aws.ToString(out.AgentSummaries[0].AgentName))
		}},
		{name: "list flows", check: func(t *testing.T) {
			t.Helper()

			out, listErr := client.ListFlows(ctx, &bedrockagentsdk.ListFlowsInput{})
			require.NoError(t, listErr)
			require.Len(t, out.FlowSummaries, 1)
			assert.Equal(t, flow.FlowID, aws.ToString(out.FlowSummaries[0].Id))
			assert.Equal(t, flow.FlowARN, aws.ToString(out.FlowSummaries[0].Arn))
		}},
		{name: "list prompts", check: func(t *testing.T) {
			t.Helper()

			out, listErr := client.ListPrompts(ctx, &bedrockagentsdk.ListPromptsInput{})
			require.NoError(t, listErr)
			require.Len(t, out.PromptSummaries, 1)
			assert.Equal(t, prompt.PromptID, aws.ToString(out.PromptSummaries[0].Id))
			assert.Equal(t, prompt.PromptARN, aws.ToString(out.PromptSummaries[0].Arn))
		}},
		{name: "list knowledge bases", check: func(t *testing.T) {
			t.Helper()

			out, listErr := client.ListKnowledgeBases(ctx, &bedrockagentsdk.ListKnowledgeBasesInput{})
			require.NoError(t, listErr)
			require.Len(t, out.KnowledgeBaseSummaries, 1)
			assert.Equal(t, kb.KnowledgeBaseID, aws.ToString(out.KnowledgeBaseSummaries[0].KnowledgeBaseId))
			assert.Equal(t, kb.Name, aws.ToString(out.KnowledgeBaseSummaries[0].Name))
		}},
		{name: "tag resource", check: func(t *testing.T) {
			t.Helper()

			_, tagErr := client.TagResource(ctx, &bedrockagentsdk.TagResourceInput{
				ResourceArn: aws.String(agent.AgentARN),
				Tags:        map[string]string{"team": "platform"},
			})
			require.NoError(t, tagErr)

			got, listErr := client.ListTagsForResource(ctx, &bedrockagentsdk.ListTagsForResourceInput{
				ResourceArn: aws.String(agent.AgentARN),
			})
			require.NoError(t, listErr)
			assert.Equal(t, "platform", got.Tags["team"])
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.check(t)
		})
	}
}
