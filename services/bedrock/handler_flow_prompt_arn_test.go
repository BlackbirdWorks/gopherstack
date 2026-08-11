package bedrock_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	bedrockagentsdk "github.com/aws/aws-sdk-go-v2/service/bedrockagent"
	bedrockagenttypes "github.com/aws/aws-sdk-go-v2/service/bedrockagent/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFlowPromptArn_UseBedrockServiceSegment proves FlowArn, FlowAliasArn and
// PromptArn are built with the "bedrock" service segment, matching
// bedrock-agent@2023-06-05's FlowArn/FlowAliasArn/PromptArn patterns
// (service-2.json: "arn:aws:bedrock:[a-z0-9-]{1,20}:[0-9]{12}:flow/...",
// not "bedrock-agent" -- the control-plane API is named bedrock-agent but
// its resource ARNs live under the bedrock service).
//
// This drives the assertion through a real aws-sdk-go-v2 bedrockagent
// client rather than hand-parsing the response body: CreateFlowResponse,
// CreateFlowAliasResponse and CreatePromptResponse have no httpPayload
// member (they were wrapped in a "flow"/"flowAlias"/"prompt" envelope this
// handler never sent for real), so a raw-JSON test that builds its own
// {"flow":{"flowId":...}} decode struct agrees with the wrong shape and
// can't see that bug -- see gopherstack-3ez4. A typed client fails closed:
// wrong shape means every field decodes to its zero value.
func TestFlowPromptArn_UseBedrockServiceSegment(t *testing.T) {
	t.Parallel()

	t.Run("create flow", func(t *testing.T) {
		t.Parallel()

		srv := newTestBedrockRegistryServer(t)
		client := newTestBedrockAgentSDKClient(t, srv.URL)

		out, err := client.CreateFlow(t.Context(), &bedrockagentsdk.CreateFlowInput{
			Name:             aws.String("arn-flow"),
			ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/role"),
		})
		require.NoError(t, err)
		require.NotEmpty(t, aws.ToString(out.Id))

		wantArn := "arn:aws:bedrock:us-east-1:000000000000:flow/" + aws.ToString(out.Id)
		assert.Equal(t, wantArn, aws.ToString(out.Arn))
	})

	t.Run("create flow alias", func(t *testing.T) {
		t.Parallel()

		srv := newTestBedrockRegistryServer(t)
		client := newTestBedrockAgentSDKClient(t, srv.URL)

		flowOut, err := client.CreateFlow(t.Context(), &bedrockagentsdk.CreateFlowInput{
			Name:             aws.String("arn-flow-for-alias"),
			ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/role"),
		})
		require.NoError(t, err)
		require.NotEmpty(t, aws.ToString(flowOut.Id))

		aliasOut, err := client.CreateFlowAlias(t.Context(), &bedrockagentsdk.CreateFlowAliasInput{
			FlowIdentifier: flowOut.Id,
			Name:           aws.String("arn-alias"),
			RoutingConfiguration: []bedrockagenttypes.FlowAliasRoutingConfigurationListItem{
				{FlowVersion: aws.String("DRAFT")},
			},
		})
		require.NoError(t, err)
		require.NotEmpty(t, aws.ToString(aliasOut.Id))

		wantArn := "arn:aws:bedrock:us-east-1:000000000000:flow/" + aws.ToString(flowOut.Id) +
			"/alias/" + aws.ToString(aliasOut.Id)
		assert.Equal(t, wantArn, aws.ToString(aliasOut.Arn))
	})

	t.Run("create prompt", func(t *testing.T) {
		t.Parallel()

		srv := newTestBedrockRegistryServer(t)
		client := newTestBedrockAgentSDKClient(t, srv.URL)

		out, err := client.CreatePrompt(t.Context(), &bedrockagentsdk.CreatePromptInput{
			Name: aws.String("arn-prompt"),
		})
		require.NoError(t, err)
		require.NotEmpty(t, aws.ToString(out.Id))

		wantArn := "arn:aws:bedrock:us-east-1:000000000000:prompt/" + aws.ToString(out.Id)
		assert.Equal(t, wantArn, aws.ToString(out.Arn))
	})
}
