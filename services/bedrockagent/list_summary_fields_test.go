package bedrockagent_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	bedrockagentsdk "github.com/aws/aws-sdk-go-v2/service/bedrockagent"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagent/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrockagent"
)

// TestListSummaryFields proves per-item response fields the wrapper-key
// sweep never checked (gopherstack-21my): AgentSummary/AgentVersionSummary
// dropped GuardrailConfiguration and AgentSummary dropped LatestAgentVersion;
// AgentAliasSummary/FlowAliasSummary dropped the required
// RoutingConfiguration; DataSource/DataSourceSummary used the wrong wire key
// "dataSourceStatus" for their Status member (the real deserializer only
// reads "status"); KnowledgeBaseDocumentDetail never carried UpdatedAt.
func TestListSummaryFields(t *testing.T) {
	t.Parallel()

	backend := bedrockagent.NewTestBackend(rtTestRegion, rtTestAccountID)
	h := bedrockagent.NewTestHandler(backend)
	h.AccountID = rtTestAccountID
	h.DefaultRegion = rtTestRegion
	client := newRoundTripClient(t, h)
	ctx := t.Context()

	agent, err := backend.CreateAgent(ctx, bedrockagent.AgentConfig{
		AgentName:       "list-fields-agent",
		FoundationModel: "anthropic.claude-v2",
		RoleARN:         "arn:aws:iam::123456789012:role/BedrockRole",
		Guardrail: map[string]any{
			"guardrailIdentifier": "gr-abc123",
			"guardrailVersion":    "DRAFT",
		},
	})
	require.NoError(t, err)

	// Empty RoutingConfiguration triggers the auto-create-a-version path
	// (agent_versions.go's newAgentVersionLocked), giving agent version "1".
	_, err = backend.CreateAgentAlias(ctx, agent.AgentID, bedrockagent.AliasConfig{
		AliasName: "auto-version-alias",
	})
	require.NoError(t, err)

	routedAlias, err := backend.CreateAgentAlias(ctx, agent.AgentID, bedrockagent.AliasConfig{
		AliasName:            "routed-alias",
		RoutingConfiguration: []bedrockagent.AliasRouting{{AgentVersion: "1"}},
	})
	require.NoError(t, err)

	flow, err := backend.CreateFlow(ctx, bedrockagent.FlowConfig{
		Name:    "list-fields-flow",
		RoleARN: "arn:aws:iam::123456789012:role/FlowRole",
	})
	require.NoError(t, err)

	flowVersion, err := backend.CreateFlowVersion(ctx, flow.FlowID, "v1")
	require.NoError(t, err)

	flowAlias, err := backend.CreateFlowAlias(ctx, flow.FlowID, bedrockagent.FlowAliasConfig{
		Name:                 "flow-routed-alias",
		RoutingConfiguration: []bedrockagent.FlowAliasRouting{{FlowVersion: flowVersion.Version}},
	})
	require.NoError(t, err)

	kb, err := backend.CreateKnowledgeBase(ctx, bedrockagent.KnowledgeBaseConfig{
		Name:    "list-fields-kb",
		RoleARN: "arn:aws:iam::123456789012:role/KBRole",
	})
	require.NoError(t, err)

	ds, err := backend.CreateDataSource(ctx, kb.KnowledgeBaseID, bedrockagent.DataSourceConfig{
		Name: "list-fields-ds",
	})
	require.NoError(t, err)

	_, err = backend.IngestKnowledgeBaseDocuments(ctx, kb.KnowledgeBaseID, ds.DataSourceID, []bedrockagent.KBDocument{
		{Identifier: bedrockagent.KBDocumentIdentifier{
			DataSourceType: "CUSTOM",
			Custom:         &bedrockagent.KBCustomDocumentIdentifier{ID: "doc-1"},
		}},
	})
	require.NoError(t, err)

	tests := []struct {
		check func(t *testing.T)
		name  string
	}{
		{name: "agent summary carries guardrail and latest version", check: func(t *testing.T) {
			t.Helper()

			out, listErr := client.ListAgents(ctx, &bedrockagentsdk.ListAgentsInput{})
			require.NoError(t, listErr)
			require.Len(t, out.AgentSummaries, 1)

			s := out.AgentSummaries[0]
			require.NotNil(t, s.GuardrailConfiguration)
			assert.Equal(t, "gr-abc123", aws.ToString(s.GuardrailConfiguration.GuardrailIdentifier))
			assert.Equal(t, "1", aws.ToString(s.LatestAgentVersion))
		}},
		{name: "agent version summary carries guardrail", check: func(t *testing.T) {
			t.Helper()

			out, listErr := client.ListAgentVersions(ctx, &bedrockagentsdk.ListAgentVersionsInput{
				AgentId: aws.String(agent.AgentID),
			})
			require.NoError(t, listErr)
			require.Len(t, out.AgentVersionSummaries, 1)

			s := out.AgentVersionSummaries[0]
			require.NotNil(t, s.GuardrailConfiguration)
			assert.Equal(t, "gr-abc123", aws.ToString(s.GuardrailConfiguration.GuardrailIdentifier))
		}},
		{name: "agent alias summary carries routing configuration", check: func(t *testing.T) {
			t.Helper()

			out, listErr := client.ListAgentAliases(ctx, &bedrockagentsdk.ListAgentAliasesInput{
				AgentId: aws.String(agent.AgentID),
			})
			require.NoError(t, listErr)

			idx := indexByID(len(out.AgentAliasSummaries), func(i int) string {
				return aws.ToString(out.AgentAliasSummaries[i].AgentAliasId)
			}, routedAlias.AgentAliasID)
			require.GreaterOrEqual(t, idx, 0, "routed alias not found in ListAgentAliases")

			routing := out.AgentAliasSummaries[idx].RoutingConfiguration
			require.Len(t, routing, 1)
			assert.Equal(t, "1", aws.ToString(routing[0].AgentVersion))
		}},
		{name: "flow alias summary carries routing configuration", check: func(t *testing.T) {
			t.Helper()

			out, listErr := client.ListFlowAliases(ctx, &bedrockagentsdk.ListFlowAliasesInput{
				FlowIdentifier: aws.String(flow.FlowID),
			})
			require.NoError(t, listErr)

			idx := indexByID(len(out.FlowAliasSummaries), func(i int) string {
				return aws.ToString(out.FlowAliasSummaries[i].Id)
			}, flowAlias.AliasID)
			require.GreaterOrEqual(t, idx, 0, "flow alias not found in ListFlowAliases")

			routing := out.FlowAliasSummaries[idx].RoutingConfiguration
			require.Len(t, routing, 1)
			assert.Equal(t, flowVersion.Version, aws.ToString(routing[0].FlowVersion))
		}},
		{name: "data source status wire key", check: func(t *testing.T) {
			t.Helper()

			getOut, getErr := client.GetDataSource(ctx, &bedrockagentsdk.GetDataSourceInput{
				KnowledgeBaseId: aws.String(kb.KnowledgeBaseID),
				DataSourceId:    aws.String(ds.DataSourceID),
			})
			require.NoError(t, getErr)
			assert.Equal(t, types.DataSourceStatusAvailable, getOut.DataSource.Status)

			listOut, listErr := client.ListDataSources(ctx, &bedrockagentsdk.ListDataSourcesInput{
				KnowledgeBaseId: aws.String(kb.KnowledgeBaseID),
			})
			require.NoError(t, listErr)
			require.Len(t, listOut.DataSourceSummaries, 1)
			assert.Equal(t, types.DataSourceStatusAvailable, listOut.DataSourceSummaries[0].Status)
		}},
		{name: "knowledge base document detail carries updated at", check: func(t *testing.T) {
			t.Helper()

			out, listErr := client.ListKnowledgeBaseDocuments(ctx, &bedrockagentsdk.ListKnowledgeBaseDocumentsInput{
				KnowledgeBaseId: aws.String(kb.KnowledgeBaseID),
				DataSourceId:    aws.String(ds.DataSourceID),
			})
			require.NoError(t, listErr)
			require.Len(t, out.DocumentDetails, 1)
			assert.NotNil(t, out.DocumentDetails[0].UpdatedAt)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.check(t)
		})
	}
}

// indexByID returns the index i in [0,n) for which id(i) equals want, or -1.
func indexByID(n int, id func(i int) string, want string) int {
	for i := range n {
		if id(i) == want {
			return i
		}
	}

	return -1
}
