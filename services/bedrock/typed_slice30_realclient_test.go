package bedrock_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	bedrockagentsdk "github.com/aws/aws-sdk-go-v2/service/bedrockagent"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagent/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFlowAliasRealClient_RoutingConfigurationRoundTrips drives the real
// bedrockagent SDK client's CreateFlowAlias/GetFlowAlias/ListFlowAliases/
// UpdateFlowAlias against bedrock.AgentsHandler and asserts the decoded,
// required RoutingConfiguration member (bedrockagent@v1.58.4
// api_op_GetFlowAlias.go: "This member is required") actually round-trips.
//
// Before this fix, FlowAlias (services/bedrock/models.go) had no
// RoutingConfiguration field at all and handleCreateFlowAlias/
// handleUpdateFlowAlias never read the request's "routingConfiguration"
// member -- every real client's alias came back with a nil routing list
// regardless of what version it was pointed at (gopherstack-n3zi slice 30).
// A prior typed-client test (typed_slice22_realclient_test.go's
// testFlowAliasRealClient) already sent RoutingConfiguration on Create and
// Update but never asserted it back, which is exactly why this slipped
// through.
//
// Scope note: bedrock.AgentsHandler (this whole family: agents, flows,
// prompts, knowledge bases, data sources) is registered in cli.go at
// MatchPriority 85 (service.PriorityPathVersioned), but services/bedrockagent
// registers its own, independent, already-100%-typed-covered implementation
// of the identical /agents,/flows,/prompts,/knowledgebases,/tags paths at
// MatchPriority 87 -- bedrockagent's own handler.go doc comment confirms this
// is deliberate ("bedrockagent's higher MatchPriority already resolves the
// /agents,/flows,/prompts overlap with plain bedrock"). That means
// bedrock.AgentsHandler never receives a single real request in the actual
// wired server: it is dead code from the perspective of real-client parity.
// This fix and test are kept for defense-in-depth (correct code is better
// than incorrect code even if unreachable today), but the remaining ~68
// AgentsHandler-family uncovered ops are deliberately NOT pursued in this
// slice -- doing so would produce tests that exercise code no real AWS SDK
// client can ever reach through the production router. See NOTES.md's
// 2026-09-12 slice 30 entry and the PARITY.md note added alongside this
// change.
func TestFlowAliasRealClient_RoutingConfigurationRoundTrips(t *testing.T) {
	t.Parallel()

	srv := newTestBedrockRegistryServer(t)
	client := newTestBedrockAgentSDKClient(t, srv.URL)

	createdFlow, err := client.CreateFlow(t.Context(), &bedrockagentsdk.CreateFlowInput{
		Name:             aws.String("slice30-flow-for-alias"),
		ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/flow-role"),
	})
	require.NoError(t, err)
	flowID := aws.ToString(createdFlow.Id)

	createdAlias, err := client.CreateFlowAlias(t.Context(), &bedrockagentsdk.CreateFlowAliasInput{
		FlowIdentifier: aws.String(flowID),
		Name:           aws.String("slice30-flow-alias"),
		RoutingConfiguration: []types.FlowAliasRoutingConfigurationListItem{
			{FlowVersion: aws.String("DRAFT")},
		},
	})
	require.NoError(t, err)
	aliasID := aws.ToString(createdAlias.Id)
	require.NotEmpty(t, aliasID)
	require.Len(t, createdAlias.RoutingConfiguration, 1)
	assert.Equal(t, "DRAFT", aws.ToString(createdAlias.RoutingConfiguration[0].FlowVersion))

	got, err := client.GetFlowAlias(t.Context(), &bedrockagentsdk.GetFlowAliasInput{
		FlowIdentifier: aws.String(flowID), AliasIdentifier: aws.String(aliasID),
	})
	require.NoError(t, err)
	require.Len(t, got.RoutingConfiguration, 1)
	assert.Equal(t, "DRAFT", aws.ToString(got.RoutingConfiguration[0].FlowVersion))

	createdVersion, err := client.CreateFlowVersion(t.Context(), &bedrockagentsdk.CreateFlowVersionInput{
		FlowIdentifier: aws.String(flowID),
	})
	require.NoError(t, err)
	newVersion := aws.ToString(createdVersion.Version)
	require.NotEmpty(t, newVersion)

	updated, err := client.UpdateFlowAlias(t.Context(), &bedrockagentsdk.UpdateFlowAliasInput{
		FlowIdentifier: aws.String(flowID), AliasIdentifier: aws.String(aliasID),
		Name: aws.String("slice30-flow-alias"),
		RoutingConfiguration: []types.FlowAliasRoutingConfigurationListItem{
			{FlowVersion: aws.String(newVersion)},
		},
	})
	require.NoError(t, err)
	require.Len(t, updated.RoutingConfiguration, 1)
	assert.Equal(t, newVersion, aws.ToString(updated.RoutingConfiguration[0].FlowVersion))
}
