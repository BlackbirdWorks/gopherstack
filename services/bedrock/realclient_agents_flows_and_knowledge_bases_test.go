package bedrock_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	bedrocksdk "github.com/aws/aws-sdk-go-v2/service/bedrock"
	bedrocktypes "github.com/aws/aws-sdk-go-v2/service/bedrock/types"
	bedrockagentsdk "github.com/aws/aws-sdk-go-v2/service/bedrockagent"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagent/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/bedrock"
)

// TestRealClient_AgentsFlowsAndKnowledgeBases drives every gopherstack-n3zi
// uncovered AgentsHandler op (the bedrock-agent-shaped Agent/Flow/
// Prompt/KnowledgeBase/DataSource family, plus core Handler's
// EnforcedGuardrailConfiguration trio) through the real aws-sdk-go-v2
// bedrockagent (and bedrock) clients, asserting on decoded typed values.
// Four real wire bugs were found and fixed while building this test (see
// each fix's own package-level doc comment): KnowledgeBaseDocument
// identifiers were a flat "documentIds" string list instead of the real
// dataSourceType+s3/custom DocumentIdentifier union (knowledge_base_documents.go,
// handler_knowledge_base_documents.go); AgentCollaborator's collaborator
// reference used an invented flat "collaboratorArn" instead of the real
// nested agentDescriptor.aliasArn, and dropped required
// collaborationInstruction/collaboratorName/lastUpdatedAt entirely
// (agent_collaborators.go, handler_agent_collaborators.go); Flow's
// executionRoleArn and version (both required CreateFlowOutput/GetFlowOutput
// members) were never stored or emitted (flows.go, handler_flows.go);
// AgentKnowledgeBaseAssociation dropped the required createdAt/updatedAt
// members entirely (agent_knowledge_base_associations.go).
func TestRealClient_AgentsFlowsAndKnowledgeBases(t *testing.T) {
	t.Parallel()

	cases := []struct {
		fn   func(t *testing.T)
		name string
	}{
		{testAgentLifecycleRealClient, "agent_lifecycle"},
		{testAgentActionGroupRealClient, "agent_action_group"},
		{testAgentAliasRealClient, "agent_alias"},
		{testAgentCollaboratorRealClient, "agent_collaborator"},
		{testAgentKnowledgeBaseRealClient, "agent_knowledge_base"},
		{testAgentVersionRealClient, "agent_version"},
		{testKnowledgeBaseRealClient, "knowledge_base"},
		{testDataSourceRealClient, "data_source"},
		{testIngestionJobRealClient, "ingestion_job"},
		{testKBDocumentsTypedRealClient, "kb_documents_typed"},
		{testFlowRealClient, "flow"},
		{testFlowAliasRealClient, "flow_alias"},
		{testFlowVersionRealClient, "flow_version"},
		{testPromptRealClient, "prompt"},
		{testPromptVersionRealClient, "prompt_version"},
		{testEnforcedGuardrailConfigRealClient, "enforced_guardrail_config"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t)
		})
	}
}

func newAgentRealClient(t *testing.T) *bedrockagentsdk.Client {
	t.Helper()

	srv := newTestBedrockRegistryServer(t)

	return newTestBedrockAgentSDKClient(t, srv.URL)
}

// newTestBedrockAgentClientFor registers an already-built AgentsHandler (so
// its backing InMemoryBackend can be seeded directly by the caller before
// any SDK call) and returns a real bedrockagent client pointed at it.
func newTestBedrockAgentClientFor(t *testing.T, h *bedrock.AgentsHandler) *bedrockagentsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	return newTestBedrockAgentSDKClient(t, srv.URL)
}

func testAgentLifecycleRealClient(t *testing.T) {
	t.Helper()

	client := newAgentRealClient(t)

	created, err := client.CreateAgent(t.Context(), &bedrockagentsdk.CreateAgentInput{
		AgentName:            aws.String("slice22-agent"),
		FoundationModel:      aws.String("amazon.titan-text-express-v1"),
		Instruction:          aws.String("be helpful"),
		AgentResourceRoleArn: aws.String("arn:aws:iam::000000000000:role/agent-role"),
		Description:          aws.String("slice22 agent"),
	})
	require.NoError(t, err)
	require.NotNil(t, created.Agent)
	agentID := aws.ToString(created.Agent.AgentId)
	assert.NotEmpty(t, agentID)
	assert.Equal(t, "slice22-agent", aws.ToString(created.Agent.AgentName))
	assert.Equal(t, types.AgentStatusNotPrepared, created.Agent.AgentStatus)

	got, err := client.GetAgent(
		t.Context(),
		&bedrockagentsdk.GetAgentInput{AgentId: aws.String(agentID)},
	)
	require.NoError(t, err)
	assert.Equal(t, "be helpful", aws.ToString(got.Agent.Instruction))

	listed, err := client.ListAgents(t.Context(), &bedrockagentsdk.ListAgentsInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, listed.AgentSummaries)

	updated, err := client.UpdateAgent(t.Context(), &bedrockagentsdk.UpdateAgentInput{
		AgentId:              aws.String(agentID),
		AgentName:            aws.String("slice22-agent"),
		FoundationModel:      aws.String("amazon.titan-text-express-v1"),
		AgentResourceRoleArn: aws.String("arn:aws:iam::000000000000:role/agent-role"),
		Description:          aws.String("updated description"),
	})
	require.NoError(t, err)
	assert.Equal(t, "updated description", aws.ToString(updated.Agent.Description))

	prepared, err := client.PrepareAgent(t.Context(), &bedrockagentsdk.PrepareAgentInput{
		AgentId: aws.String(agentID),
	})
	require.NoError(t, err)
	assert.Equal(t, types.AgentStatusPreparing, prepared.AgentStatus)

	_, err = client.DeleteAgent(t.Context(), &bedrockagentsdk.DeleteAgentInput{
		AgentId:                aws.String(agentID),
		SkipResourceInUseCheck: true,
	})
	require.NoError(t, err)

	_, err = client.GetAgent(
		t.Context(),
		&bedrockagentsdk.GetAgentInput{AgentId: aws.String(agentID)},
	)
	require.Error(t, err, "deleted agent must not be gettable")
}

func createTestAgent(t *testing.T, client *bedrockagentsdk.Client, name string) string {
	t.Helper()

	created, err := client.CreateAgent(t.Context(), &bedrockagentsdk.CreateAgentInput{
		AgentName:            aws.String(name),
		FoundationModel:      aws.String("amazon.titan-text-express-v1"),
		AgentResourceRoleArn: aws.String("arn:aws:iam::000000000000:role/agent-role"),
	})
	require.NoError(t, err)

	return aws.ToString(created.Agent.AgentId)
}

func testAgentActionGroupRealClient(t *testing.T) {
	t.Helper()

	client := newAgentRealClient(t)
	agentID := createTestAgent(t, client, "slice22-ag-actiongroup")

	created, err := client.CreateAgentActionGroup(
		t.Context(),
		&bedrockagentsdk.CreateAgentActionGroupInput{
			AgentId:         aws.String(agentID),
			AgentVersion:    aws.String("DRAFT"),
			ActionGroupName: aws.String("slice22-action-group"),
			Description:     aws.String("does things"),
			ActionGroupExecutor: &types.ActionGroupExecutorMemberLambda{
				Value: "arn:aws:lambda:us-east-1:000000000000:function:fn",
			},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, created.AgentActionGroup)
	agID := aws.ToString(created.AgentActionGroup.ActionGroupId)
	assert.NotEmpty(t, agID)
	assert.Equal(t, "slice22-action-group", aws.ToString(created.AgentActionGroup.ActionGroupName))

	lambdaExec, ok := created.AgentActionGroup.ActionGroupExecutor.(*types.ActionGroupExecutorMemberLambda)
	require.True(t, ok, "ActionGroupExecutor must decode as the real lambda union member")
	assert.Equal(t, "arn:aws:lambda:us-east-1:000000000000:function:fn", lambdaExec.Value)

	got, err := client.GetAgentActionGroup(t.Context(), &bedrockagentsdk.GetAgentActionGroupInput{
		AgentId: aws.String(
			agentID,
		), AgentVersion: aws.String("DRAFT"), ActionGroupId: aws.String(agID),
	})
	require.NoError(t, err)
	assert.Equal(t, "does things", aws.ToString(got.AgentActionGroup.Description))

	listed, err := client.ListAgentActionGroups(
		t.Context(),
		&bedrockagentsdk.ListAgentActionGroupsInput{
			AgentId: aws.String(agentID), AgentVersion: aws.String("DRAFT"),
		},
	)
	require.NoError(t, err)
	assert.Len(t, listed.ActionGroupSummaries, 1)

	updated, err := client.UpdateAgentActionGroup(
		t.Context(),
		&bedrockagentsdk.UpdateAgentActionGroupInput{
			AgentId: aws.String(
				agentID,
			), AgentVersion: aws.String("DRAFT"), ActionGroupId: aws.String(agID),
			ActionGroupName: aws.String("slice22-action-group"),
			Description:     aws.String("updated"),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "updated", aws.ToString(updated.AgentActionGroup.Description))

	_, err = client.DeleteAgentActionGroup(
		t.Context(),
		&bedrockagentsdk.DeleteAgentActionGroupInput{
			AgentId: aws.String(
				agentID,
			), AgentVersion: aws.String("DRAFT"), ActionGroupId: aws.String(agID),
		},
	)
	require.NoError(t, err)
}

func testAgentAliasRealClient(t *testing.T) {
	t.Helper()

	client := newAgentRealClient(t)
	agentID := createTestAgent(t, client, "slice22-ag-alias")

	created, err := client.CreateAgentAlias(t.Context(), &bedrockagentsdk.CreateAgentAliasInput{
		AgentId:        aws.String(agentID),
		AgentAliasName: aws.String("slice22-alias"),
		Description:    aws.String("alias desc"),
	})
	require.NoError(t, err)
	aliasID := aws.ToString(created.AgentAlias.AgentAliasId)
	assert.NotEmpty(t, aliasID)
	assert.Equal(t, types.AgentAliasStatusPrepared, created.AgentAlias.AgentAliasStatus)

	got, err := client.GetAgentAlias(t.Context(), &bedrockagentsdk.GetAgentAliasInput{
		AgentId: aws.String(agentID), AgentAliasId: aws.String(aliasID),
	})
	require.NoError(t, err)
	assert.Equal(t, "slice22-alias", aws.ToString(got.AgentAlias.AgentAliasName))

	listed, err := client.ListAgentAliases(t.Context(), &bedrockagentsdk.ListAgentAliasesInput{
		AgentId: aws.String(agentID),
	})
	require.NoError(t, err)
	assert.Len(t, listed.AgentAliasSummaries, 1)

	updated, err := client.UpdateAgentAlias(t.Context(), &bedrockagentsdk.UpdateAgentAliasInput{
		AgentId: aws.String(agentID), AgentAliasId: aws.String(aliasID),
		AgentAliasName: aws.String("slice22-alias-renamed"),
	})
	require.NoError(t, err)
	assert.Equal(t, "slice22-alias-renamed", aws.ToString(updated.AgentAlias.AgentAliasName))

	_, err = client.DeleteAgentAlias(t.Context(), &bedrockagentsdk.DeleteAgentAliasInput{
		AgentId: aws.String(agentID), AgentAliasId: aws.String(aliasID),
	})
	require.NoError(t, err)
}

func testAgentCollaboratorRealClient(t *testing.T) {
	t.Helper()

	client := newAgentRealClient(t)
	agentID := createTestAgent(t, client, "slice22-ag-collab")

	associated, err := client.AssociateAgentCollaborator(
		t.Context(),
		&bedrockagentsdk.AssociateAgentCollaboratorInput{
			AgentId:      aws.String(agentID),
			AgentVersion: aws.String("DRAFT"),
			AgentDescriptor: &types.AgentDescriptor{
				AliasArn: aws.String(
					"arn:aws:bedrock:us-east-1:000000000000:agent-alias/other/ALIAS1",
				),
			},
			CollaboratorName:         aws.String("slice22-collab"),
			CollaborationInstruction: aws.String("collaborate well"),
			RelayConversationHistory: types.RelayConversationHistoryToCollaborator,
		},
	)
	require.NoError(t, err)
	require.NotNil(t, associated.AgentCollaborator)
	collabID := aws.ToString(associated.AgentCollaborator.CollaboratorId)
	assert.NotEmpty(t, collabID)
	require.NotNil(t, associated.AgentCollaborator.AgentDescriptor)
	assert.Equal(t,
		"arn:aws:bedrock:us-east-1:000000000000:agent-alias/other/ALIAS1",
		aws.ToString(associated.AgentCollaborator.AgentDescriptor.AliasArn),
	)
	assert.Equal(
		t,
		"collaborate well",
		aws.ToString(associated.AgentCollaborator.CollaborationInstruction),
	)
	assert.Equal(t, "slice22-collab", aws.ToString(associated.AgentCollaborator.CollaboratorName))
	require.NotNil(t, associated.AgentCollaborator.CreatedAt)
	require.NotNil(t, associated.AgentCollaborator.LastUpdatedAt)

	got, err := client.GetAgentCollaborator(t.Context(), &bedrockagentsdk.GetAgentCollaboratorInput{
		AgentId: aws.String(
			agentID,
		), AgentVersion: aws.String("DRAFT"), CollaboratorId: aws.String(collabID),
	})
	require.NoError(t, err)
	assert.Equal(t, "slice22-collab", aws.ToString(got.AgentCollaborator.CollaboratorName))

	listed, err := client.ListAgentCollaborators(
		t.Context(),
		&bedrockagentsdk.ListAgentCollaboratorsInput{
			AgentId: aws.String(agentID), AgentVersion: aws.String("DRAFT"),
		},
	)
	require.NoError(t, err)
	assert.Len(t, listed.AgentCollaboratorSummaries, 1)

	updated, err := client.UpdateAgentCollaborator(
		t.Context(),
		&bedrockagentsdk.UpdateAgentCollaboratorInput{
			AgentId: aws.String(
				agentID,
			), AgentVersion: aws.String("DRAFT"), CollaboratorId: aws.String(collabID),
			AgentDescriptor: &types.AgentDescriptor{
				AliasArn: aws.String(
					"arn:aws:bedrock:us-east-1:000000000000:agent-alias/other/ALIAS1",
				),
			},
			CollaboratorName:         aws.String("slice22-collab-renamed"),
			CollaborationInstruction: aws.String("collaborate better"),
		},
	)
	require.NoError(t, err)
	assert.Equal(
		t,
		"slice22-collab-renamed",
		aws.ToString(updated.AgentCollaborator.CollaboratorName),
	)

	_, err = client.DisassociateAgentCollaborator(
		t.Context(),
		&bedrockagentsdk.DisassociateAgentCollaboratorInput{
			AgentId: aws.String(
				agentID,
			), AgentVersion: aws.String("DRAFT"), CollaboratorId: aws.String(collabID),
		},
	)
	require.NoError(t, err)
}

func createRealClientKnowledgeBase(t *testing.T, client *bedrockagentsdk.Client, name string) string {
	t.Helper()

	created, err := client.CreateKnowledgeBase(
		t.Context(),
		&bedrockagentsdk.CreateKnowledgeBaseInput{
			Name:    aws.String(name),
			RoleArn: aws.String("arn:aws:iam::000000000000:role/kb-role"),
			KnowledgeBaseConfiguration: &types.KnowledgeBaseConfiguration{
				Type: types.KnowledgeBaseTypeVector,
			},
		},
	)
	require.NoError(t, err)

	return aws.ToString(created.KnowledgeBase.KnowledgeBaseId)
}

func testAgentKnowledgeBaseRealClient(t *testing.T) {
	t.Helper()

	client := newAgentRealClient(t)
	agentID := createTestAgent(t, client, "slice22-ag-kb")
	kbID := createRealClientKnowledgeBase(t, client, "slice22-kb-for-agent")

	associated, err := client.AssociateAgentKnowledgeBase(
		t.Context(),
		&bedrockagentsdk.AssociateAgentKnowledgeBaseInput{
			AgentId:         aws.String(agentID),
			AgentVersion:    aws.String("DRAFT"),
			KnowledgeBaseId: aws.String(kbID),
			Description:     aws.String("kb for agent"),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, associated.AgentKnowledgeBase)
	assert.Equal(t, "kb for agent", aws.ToString(associated.AgentKnowledgeBase.Description))
	require.NotNil(t, associated.AgentKnowledgeBase.CreatedAt)
	require.NotNil(t, associated.AgentKnowledgeBase.UpdatedAt)

	got, err := client.GetAgentKnowledgeBase(
		t.Context(),
		&bedrockagentsdk.GetAgentKnowledgeBaseInput{
			AgentId: aws.String(
				agentID,
			), AgentVersion: aws.String("DRAFT"), KnowledgeBaseId: aws.String(kbID),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, types.KnowledgeBaseStateEnabled, got.AgentKnowledgeBase.KnowledgeBaseState)

	listed, err := client.ListAgentKnowledgeBases(
		t.Context(),
		&bedrockagentsdk.ListAgentKnowledgeBasesInput{
			AgentId: aws.String(agentID), AgentVersion: aws.String("DRAFT"),
		},
	)
	require.NoError(t, err)
	assert.Len(t, listed.AgentKnowledgeBaseSummaries, 1)

	updated, err := client.UpdateAgentKnowledgeBase(
		t.Context(),
		&bedrockagentsdk.UpdateAgentKnowledgeBaseInput{
			AgentId: aws.String(
				agentID,
			), AgentVersion: aws.String("DRAFT"), KnowledgeBaseId: aws.String(kbID),
			KnowledgeBaseState: types.KnowledgeBaseStateDisabled,
		},
	)
	require.NoError(t, err)
	assert.Equal(t, types.KnowledgeBaseStateDisabled, updated.AgentKnowledgeBase.KnowledgeBaseState)

	_, err = client.DisassociateAgentKnowledgeBase(
		t.Context(),
		&bedrockagentsdk.DisassociateAgentKnowledgeBaseInput{
			AgentId: aws.String(
				agentID,
			), AgentVersion: aws.String("DRAFT"), KnowledgeBaseId: aws.String(kbID),
		},
	)
	require.NoError(t, err)
}

// testAgentVersionRealClient drives GetAgentVersion/ListAgentVersions/
// DeleteAgentVersion through the real client. CreateAgentVersion is not a
// real bedrock-agent wire operation (confirmed by
// handler_agent_sdk_route_table_test.go's route table, absent from the real
// SDK's serializers.go) -- numbered agent versions are an implicit side
// effect of the real control plane, so this backend's own
// CreateAgentVersion helper (an internal test convenience, per
// AgentsHandler.GetSupportedOperations' comment) seeds the fixture directly.
func testAgentVersionRealClient(t *testing.T) {
	t.Helper()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	agent, err := b.CreateAgent(
		"slice22-ag-version", "amazon.titan-text-express-v1", "",
		"arn:aws:iam::000000000000:role/agent-role", nil,
	)
	require.NoError(t, err)

	av, err := b.CreateAgentVersion(agent.AgentID)
	require.NoError(t, err)

	agentsHandler := bedrock.NewAgentsHandler(b)
	client := newTestBedrockAgentClientFor(t, agentsHandler)

	got, err := client.GetAgentVersion(t.Context(), &bedrockagentsdk.GetAgentVersionInput{
		AgentId: aws.String(agent.AgentID), AgentVersion: aws.String(av.AgentVersion),
	})
	require.NoError(t, err)
	require.NotNil(t, got.AgentVersion)
	assert.Equal(t, av.AgentVersion, aws.ToString(got.AgentVersion.Version))
	assert.Equal(t, agent.AgentArn, aws.ToString(got.AgentVersion.AgentArn))
	assert.Equal(t, agent.AgentName, aws.ToString(got.AgentVersion.AgentName))

	listed, err := client.ListAgentVersions(t.Context(), &bedrockagentsdk.ListAgentVersionsInput{
		AgentId: aws.String(agent.AgentID),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, listed.AgentVersionSummaries)

	_, err = client.DeleteAgentVersion(t.Context(), &bedrockagentsdk.DeleteAgentVersionInput{
		AgentId: aws.String(agent.AgentID), AgentVersion: aws.String(av.AgentVersion),
	})
	require.NoError(t, err)

	_, err = client.GetAgentVersion(t.Context(), &bedrockagentsdk.GetAgentVersionInput{
		AgentId: aws.String(agent.AgentID), AgentVersion: aws.String(av.AgentVersion),
	})
	require.Error(t, err, "deleted agent version must not be gettable")
}

// GetAgentMemory/DeleteAgentMemory are NOT reachable from this test: they
// are real AWS ops, but they belong to the bedrock-agent-runtime API
// (Amazon's separate runtime-plane service), not bedrock-agent (control
// plane) -- confirmed by their total absence from every vendored SDK module
// (bedrockagent@v1.58.4, bedrock@v1.66.4, bedrockruntime@v1.57.1 all lack
// api_op_GetAgentMemory.go/api_op_DeleteAgentMemory.go, and go.mod carries
// no bedrockagentruntime module at all). AgentsHandler implements and
// advertises both under the bedrock-agent control-plane wire family
// (handler_agents.go, handler_agents_dispatch.go's GetSupportedOperations)
// regardless -- a misplacement this pass leaves as-is (adding a new go.mod
// dependency is out of scope) but records here: these two ops stay
// permanently unreachable by any typed client this repo can build without a
// new dependency, not merely untested.

func testKnowledgeBaseRealClient(t *testing.T) {
	t.Helper()

	client := newAgentRealClient(t)

	created, err := client.CreateKnowledgeBase(
		t.Context(),
		&bedrockagentsdk.CreateKnowledgeBaseInput{
			Name:        aws.String("slice22-kb"),
			Description: aws.String("kb desc"),
			RoleArn:     aws.String("arn:aws:iam::000000000000:role/kb-role"),
			KnowledgeBaseConfiguration: &types.KnowledgeBaseConfiguration{
				Type: types.KnowledgeBaseTypeVector,
			},
		},
	)
	require.NoError(t, err)
	kbID := aws.ToString(created.KnowledgeBase.KnowledgeBaseId)
	assert.NotEmpty(t, kbID)
	assert.Equal(t, "kb desc", aws.ToString(created.KnowledgeBase.Description))

	got, err := client.GetKnowledgeBase(t.Context(), &bedrockagentsdk.GetKnowledgeBaseInput{
		KnowledgeBaseId: aws.String(kbID),
	})
	require.NoError(t, err)
	assert.Equal(t, "slice22-kb", aws.ToString(got.KnowledgeBase.Name))

	listed, err := client.ListKnowledgeBases(
		t.Context(),
		&bedrockagentsdk.ListKnowledgeBasesInput{},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, listed.KnowledgeBaseSummaries)

	updated, err := client.UpdateKnowledgeBase(
		t.Context(),
		&bedrockagentsdk.UpdateKnowledgeBaseInput{
			KnowledgeBaseId: aws.String(kbID),
			Name:            aws.String("slice22-kb"),
			RoleArn:         aws.String("arn:aws:iam::000000000000:role/kb-role"),
			Description:     aws.String("updated kb desc"),
			KnowledgeBaseConfiguration: &types.KnowledgeBaseConfiguration{
				Type: types.KnowledgeBaseTypeVector,
			},
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "updated kb desc", aws.ToString(updated.KnowledgeBase.Description))

	_, err = client.DeleteKnowledgeBase(t.Context(), &bedrockagentsdk.DeleteKnowledgeBaseInput{
		KnowledgeBaseId: aws.String(kbID),
	})
	require.NoError(t, err)
}

func createTestDataSource(
	t *testing.T,
	client *bedrockagentsdk.Client,
	kbID, name string,
) string {
	t.Helper()

	created, err := client.CreateDataSource(t.Context(), &bedrockagentsdk.CreateDataSourceInput{
		KnowledgeBaseId: aws.String(kbID),
		Name:            aws.String(name),
		DataSourceConfiguration: &types.DataSourceConfiguration{
			Type: types.DataSourceTypeS3,
			S3Configuration: &types.S3DataSourceConfiguration{
				BucketArn: aws.String("arn:aws:s3:::slice22-bucket"),
			},
		},
	})
	require.NoError(t, err)

	return aws.ToString(created.DataSource.DataSourceId)
}

func testDataSourceRealClient(t *testing.T) {
	t.Helper()

	client := newAgentRealClient(t)
	kbID := createRealClientKnowledgeBase(t, client, "slice22-kb-for-ds")

	dsID := createTestDataSource(t, client, kbID, "slice22-ds")
	assert.NotEmpty(t, dsID)

	got, err := client.GetDataSource(t.Context(), &bedrockagentsdk.GetDataSourceInput{
		KnowledgeBaseId: aws.String(kbID), DataSourceId: aws.String(dsID),
	})
	require.NoError(t, err)
	assert.Equal(t, "slice22-ds", aws.ToString(got.DataSource.Name))

	listed, err := client.ListDataSources(t.Context(), &bedrockagentsdk.ListDataSourcesInput{
		KnowledgeBaseId: aws.String(kbID),
	})
	require.NoError(t, err)
	assert.Len(t, listed.DataSourceSummaries, 1)

	updated, err := client.UpdateDataSource(t.Context(), &bedrockagentsdk.UpdateDataSourceInput{
		KnowledgeBaseId: aws.String(kbID), DataSourceId: aws.String(dsID),
		Name: aws.String("slice22-ds-renamed"),
		DataSourceConfiguration: &types.DataSourceConfiguration{
			Type: types.DataSourceTypeS3,
			S3Configuration: &types.S3DataSourceConfiguration{
				BucketArn: aws.String("arn:aws:s3:::slice22-bucket"),
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "slice22-ds-renamed", aws.ToString(updated.DataSource.Name))

	_, err = client.DeleteDataSource(t.Context(), &bedrockagentsdk.DeleteDataSourceInput{
		KnowledgeBaseId: aws.String(kbID), DataSourceId: aws.String(dsID),
	})
	require.NoError(t, err)
}

func testIngestionJobRealClient(t *testing.T) {
	t.Helper()

	client := newAgentRealClient(t)
	kbID := createRealClientKnowledgeBase(t, client, "slice22-kb-for-job")
	dsID := createTestDataSource(t, client, kbID, "slice22-ds-for-job")

	started, err := client.StartIngestionJob(t.Context(), &bedrockagentsdk.StartIngestionJobInput{
		KnowledgeBaseId: aws.String(kbID), DataSourceId: aws.String(dsID),
		Description: aws.String("slice22 ingestion"),
	})
	require.NoError(t, err)
	jobID := aws.ToString(started.IngestionJob.IngestionJobId)
	assert.NotEmpty(t, jobID)
	assert.Equal(t, "slice22 ingestion", aws.ToString(started.IngestionJob.Description))

	got, err := client.GetIngestionJob(t.Context(), &bedrockagentsdk.GetIngestionJobInput{
		KnowledgeBaseId: aws.String(
			kbID,
		), DataSourceId: aws.String(dsID), IngestionJobId: aws.String(jobID),
	})
	require.NoError(t, err)
	assert.Equal(t, jobID, aws.ToString(got.IngestionJob.IngestionJobId))

	listed, err := client.ListIngestionJobs(t.Context(), &bedrockagentsdk.ListIngestionJobsInput{
		KnowledgeBaseId: aws.String(kbID), DataSourceId: aws.String(dsID),
	})
	require.NoError(t, err)
	assert.Len(t, listed.IngestionJobSummaries, 1)

	_, err = client.StopIngestionJob(t.Context(), &bedrockagentsdk.StopIngestionJobInput{
		KnowledgeBaseId: aws.String(
			kbID,
		), DataSourceId: aws.String(dsID), IngestionJobId: aws.String(jobID),
	})
	require.NoError(t, err)
}

// testKBDocumentsTypedRealClient drives Ingest/Get/List/DeleteKnowledgeBaseDocuments
// through the real typed client -- the fix that made this decode at all (see
// this file's top-level doc comment).
func testKBDocumentsTypedRealClient(t *testing.T) {
	t.Helper()

	client := newAgentRealClient(t)
	kbID := createRealClientKnowledgeBase(t, client, "slice22-kb-for-docs")
	dsID := createTestDataSource(t, client, kbID, "slice22-ds-for-docs")

	ingested, err := client.IngestKnowledgeBaseDocuments(
		t.Context(),
		&bedrockagentsdk.IngestKnowledgeBaseDocumentsInput{
			KnowledgeBaseId: aws.String(kbID), DataSourceId: aws.String(dsID),
			Documents: []types.KnowledgeBaseDocument{
				{Content: &types.DocumentContent{
					DataSourceType: types.ContentDataSourceTypeS3,
					S3: &types.S3Content{
						S3Location: &types.S3Location{Uri: aws.String("s3://slice22-bucket/doc-1")},
					},
				}},
			},
		},
	)
	require.NoError(t, err)
	require.Len(t, ingested.DocumentDetails, 1)
	detail := ingested.DocumentDetails[0]
	assert.Equal(t, types.DocumentStatusIndexed, detail.Status)
	require.NotNil(t, detail.Identifier)
	require.NotNil(t, detail.Identifier.S3)
	assert.Equal(t, "s3://slice22-bucket/doc-1", aws.ToString(detail.Identifier.S3.Uri))
	require.NotNil(t, detail.UpdatedAt)

	listed, err := client.ListKnowledgeBaseDocuments(
		t.Context(),
		&bedrockagentsdk.ListKnowledgeBaseDocumentsInput{
			KnowledgeBaseId: aws.String(kbID), DataSourceId: aws.String(dsID),
		},
	)
	require.NoError(t, err)
	assert.Len(t, listed.DocumentDetails, 1)

	got, err := client.GetKnowledgeBaseDocuments(
		t.Context(),
		&bedrockagentsdk.GetKnowledgeBaseDocumentsInput{
			KnowledgeBaseId: aws.String(kbID), DataSourceId: aws.String(dsID),
			DocumentIdentifiers: []types.DocumentIdentifier{
				{
					DataSourceType: types.ContentDataSourceTypeS3,
					S3:             &types.S3Location{Uri: aws.String("s3://slice22-bucket/doc-1")},
				},
			},
		},
	)
	require.NoError(t, err)
	require.Len(t, got.DocumentDetails, 1)
	assert.Equal(
		t,
		"s3://slice22-bucket/doc-1",
		aws.ToString(got.DocumentDetails[0].Identifier.S3.Uri),
	)

	_, err = client.DeleteKnowledgeBaseDocuments(
		t.Context(),
		&bedrockagentsdk.DeleteKnowledgeBaseDocumentsInput{
			KnowledgeBaseId: aws.String(kbID), DataSourceId: aws.String(dsID),
			DocumentIdentifiers: []types.DocumentIdentifier{
				{
					DataSourceType: types.ContentDataSourceTypeS3,
					S3:             &types.S3Location{Uri: aws.String("s3://slice22-bucket/doc-1")},
				},
			},
		},
	)
	require.NoError(t, err)

	listed, err = client.ListKnowledgeBaseDocuments(
		t.Context(),
		&bedrockagentsdk.ListKnowledgeBaseDocumentsInput{
			KnowledgeBaseId: aws.String(kbID), DataSourceId: aws.String(dsID),
		},
	)
	require.NoError(t, err)
	assert.Empty(t, listed.DocumentDetails, "deleted document must not still be listed")
}

func testFlowRealClient(t *testing.T) {
	t.Helper()

	client := newAgentRealClient(t)

	created, err := client.CreateFlow(t.Context(), &bedrockagentsdk.CreateFlowInput{
		Name:             aws.String("slice22-flow"),
		Description:      aws.String("flow desc"),
		ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/flow-role"),
	})
	require.NoError(t, err)
	flowID := aws.ToString(created.Id)
	assert.NotEmpty(t, flowID)
	assert.Equal(
		t,
		"arn:aws:iam::000000000000:role/flow-role",
		aws.ToString(created.ExecutionRoleArn),
	)
	assert.Equal(t, "DRAFT", aws.ToString(created.Version))
	assert.Equal(t, types.FlowStatusNotPrepared, created.Status)

	got, err := client.GetFlow(
		t.Context(),
		&bedrockagentsdk.GetFlowInput{FlowIdentifier: aws.String(flowID)},
	)
	require.NoError(t, err)
	assert.Equal(t, "flow desc", aws.ToString(got.Description))
	assert.Equal(t, "arn:aws:iam::000000000000:role/flow-role", aws.ToString(got.ExecutionRoleArn))
	assert.Equal(t, "DRAFT", aws.ToString(got.Version))

	listed, err := client.ListFlows(t.Context(), &bedrockagentsdk.ListFlowsInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, listed.FlowSummaries)

	updated, err := client.UpdateFlow(t.Context(), &bedrockagentsdk.UpdateFlowInput{
		FlowIdentifier:   aws.String(flowID),
		Name:             aws.String("slice22-flow"),
		ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/flow-role-2"),
	})
	require.NoError(t, err)
	assert.Equal(
		t,
		"arn:aws:iam::000000000000:role/flow-role-2",
		aws.ToString(updated.ExecutionRoleArn),
	)

	prepared, err := client.PrepareFlow(t.Context(), &bedrockagentsdk.PrepareFlowInput{
		FlowIdentifier: aws.String(flowID),
	})
	require.NoError(t, err)
	assert.Equal(t, types.FlowStatusPrepared, prepared.Status)

	validated, err := client.ValidateFlowDefinition(
		t.Context(),
		&bedrockagentsdk.ValidateFlowDefinitionInput{
			Definition: &types.FlowDefinition{},
		},
	)
	require.NoError(t, err)
	assert.NotNil(t, validated.Validations)

	_, err = client.DeleteFlow(
		t.Context(),
		&bedrockagentsdk.DeleteFlowInput{FlowIdentifier: aws.String(flowID)},
	)
	require.NoError(t, err)
}

func createTestFlow(t *testing.T, client *bedrockagentsdk.Client, name string) string {
	t.Helper()

	created, err := client.CreateFlow(t.Context(), &bedrockagentsdk.CreateFlowInput{
		Name:             aws.String(name),
		ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/flow-role"),
	})
	require.NoError(t, err)

	return aws.ToString(created.Id)
}

func testFlowAliasRealClient(t *testing.T) {
	t.Helper()

	client := newAgentRealClient(t)
	flowID := createTestFlow(t, client, "slice22-flow-for-alias")

	created, err := client.CreateFlowAlias(t.Context(), &bedrockagentsdk.CreateFlowAliasInput{
		FlowIdentifier: aws.String(flowID),
		Name:           aws.String("slice22-flow-alias"),
		Description:    aws.String("flow alias desc"),
		RoutingConfiguration: []types.FlowAliasRoutingConfigurationListItem{
			{FlowVersion: aws.String("DRAFT")},
		},
	})
	require.NoError(t, err)
	aliasID := aws.ToString(created.Id)
	assert.NotEmpty(t, aliasID)
	assert.Equal(t, flowID, aws.ToString(created.FlowId))

	got, err := client.GetFlowAlias(t.Context(), &bedrockagentsdk.GetFlowAliasInput{
		FlowIdentifier: aws.String(flowID), AliasIdentifier: aws.String(aliasID),
	})
	require.NoError(t, err)
	assert.Equal(t, "flow alias desc", aws.ToString(got.Description))

	listed, err := client.ListFlowAliases(t.Context(), &bedrockagentsdk.ListFlowAliasesInput{
		FlowIdentifier: aws.String(flowID),
	})
	require.NoError(t, err)
	assert.Len(t, listed.FlowAliasSummaries, 1)

	updated, err := client.UpdateFlowAlias(t.Context(), &bedrockagentsdk.UpdateFlowAliasInput{
		FlowIdentifier: aws.String(flowID), AliasIdentifier: aws.String(aliasID),
		Name: aws.String("slice22-flow-alias-renamed"),
		RoutingConfiguration: []types.FlowAliasRoutingConfigurationListItem{
			{FlowVersion: aws.String("DRAFT")},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "slice22-flow-alias-renamed", aws.ToString(updated.Name))

	_, err = client.DeleteFlowAlias(t.Context(), &bedrockagentsdk.DeleteFlowAliasInput{
		FlowIdentifier: aws.String(flowID), AliasIdentifier: aws.String(aliasID),
	})
	require.NoError(t, err)
}

func testFlowVersionRealClient(t *testing.T) {
	t.Helper()

	client := newAgentRealClient(t)
	flowID := createTestFlow(t, client, "slice22-flow-for-version")

	created, err := client.CreateFlowVersion(t.Context(), &bedrockagentsdk.CreateFlowVersionInput{
		FlowIdentifier: aws.String(flowID),
	})
	require.NoError(t, err)
	version := aws.ToString(created.Version)
	assert.NotEmpty(t, version)

	got, err := client.GetFlowVersion(t.Context(), &bedrockagentsdk.GetFlowVersionInput{
		FlowIdentifier: aws.String(flowID), FlowVersion: aws.String(version),
	})
	require.NoError(t, err)
	assert.Equal(t, version, aws.ToString(got.Version))

	listed, err := client.ListFlowVersions(t.Context(), &bedrockagentsdk.ListFlowVersionsInput{
		FlowIdentifier: aws.String(flowID),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, listed.FlowVersionSummaries)

	_, err = client.DeleteFlowVersion(t.Context(), &bedrockagentsdk.DeleteFlowVersionInput{
		FlowIdentifier: aws.String(flowID), FlowVersion: aws.String(version),
	})
	require.NoError(t, err)
}

func testPromptRealClient(t *testing.T) {
	t.Helper()

	client := newAgentRealClient(t)

	created, err := client.CreatePrompt(t.Context(), &bedrockagentsdk.CreatePromptInput{
		Name:        aws.String("slice22-prompt"),
		Description: aws.String("prompt desc"),
	})
	require.NoError(t, err)
	promptID := aws.ToString(created.Id)
	assert.NotEmpty(t, promptID)

	got, err := client.GetPrompt(
		t.Context(),
		&bedrockagentsdk.GetPromptInput{PromptIdentifier: aws.String(promptID)},
	)
	require.NoError(t, err)
	assert.Equal(t, "prompt desc", aws.ToString(got.Description))

	listed, err := client.ListPrompts(t.Context(), &bedrockagentsdk.ListPromptsInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, listed.PromptSummaries)

	updated, err := client.UpdatePrompt(t.Context(), &bedrockagentsdk.UpdatePromptInput{
		PromptIdentifier: aws.String(promptID),
		Name:             aws.String("slice22-prompt-renamed"),
	})
	require.NoError(t, err)
	assert.Equal(t, "slice22-prompt-renamed", aws.ToString(updated.Name))

	_, err = client.DeletePrompt(t.Context(), &bedrockagentsdk.DeletePromptInput{
		PromptIdentifier: aws.String(promptID),
	})
	require.NoError(t, err)
}

func testPromptVersionRealClient(t *testing.T) {
	t.Helper()

	client := newAgentRealClient(t)

	created, err := client.CreatePrompt(t.Context(), &bedrockagentsdk.CreatePromptInput{
		Name: aws.String("slice22-prompt-for-version"),
	})
	require.NoError(t, err)
	promptID := aws.ToString(created.Id)

	version, err := client.CreatePromptVersion(
		t.Context(),
		&bedrockagentsdk.CreatePromptVersionInput{
			PromptIdentifier: aws.String(promptID),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, promptID, aws.ToString(version.Id))
	assert.NotEqual(t, "DRAFT", aws.ToString(version.Version))
}

func testEnforcedGuardrailConfigRealClient(t *testing.T) {
	t.Helper()

	h := bedrock.NewHandler(bedrock.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestBedrockClient(t, h)

	guardrail, err := client.CreateGuardrail(t.Context(), &bedrocksdk.CreateGuardrailInput{
		Name:                    aws.String("slice22-guardrail"),
		BlockedInputMessaging:   aws.String("blocked input"),
		BlockedOutputsMessaging: aws.String("blocked output"),
	})
	require.NoError(t, err)

	version, err := client.CreateGuardrailVersion(
		t.Context(),
		&bedrocksdk.CreateGuardrailVersionInput{
			GuardrailIdentifier: guardrail.GuardrailId,
		},
	)
	require.NoError(t, err)

	put, err := client.PutEnforcedGuardrailConfiguration(
		t.Context(),
		&bedrocksdk.PutEnforcedGuardrailConfigurationInput{
			GuardrailInferenceConfig: &bedrocktypes.AccountEnforcedGuardrailInferenceInputConfiguration{
				GuardrailIdentifier: guardrail.GuardrailId,
				GuardrailVersion:    version.Version,
			},
		},
	)
	require.NoError(t, err)
	configID := aws.ToString(put.ConfigId)
	assert.NotEmpty(t, configID)
	assert.NotNil(t, put.UpdatedAt)

	listed, err := client.ListEnforcedGuardrailsConfiguration(
		t.Context(),
		&bedrocksdk.ListEnforcedGuardrailsConfigurationInput{},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, listed.GuardrailsConfig)

	_, err = client.DeleteEnforcedGuardrailConfiguration(
		t.Context(),
		&bedrocksdk.DeleteEnforcedGuardrailConfigurationInput{
			ConfigId: aws.String(configID),
		},
	)
	require.NoError(t, err)
}
