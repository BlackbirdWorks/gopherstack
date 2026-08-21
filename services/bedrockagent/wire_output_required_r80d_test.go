package bedrockagent_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	bedrockagentsdk "github.com/aws/aws-sdk-go-v2/service/bedrockagent"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagent/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/bedrockagent"
)

// newTestBedrockAgentClient stands up the real aws-sdk-go-v2 bedrockagent
// client against an httptest server running this package's Handler, wired
// through the same pkgs/service registry/router used in production.
// Round-tripping through the genuine SDK deserializer -- not a raw-JSON
// assertion -- is what proves a required field actually decodes; the real
// deserializer silently ignores any key it doesn't recognize but leaves a
// required *string/*time.Time nil when the key never arrives.
func newTestBedrockAgentClient(t *testing.T, h *bedrockagent.Handler) *bedrockagentsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return bedrockagentsdk.NewFromConfig(cfg, func(o *bedrockagentsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

func newTestBedrockAgentHandler() *bedrockagent.Handler {
	return bedrockagent.NewHandler(bedrockagent.NewInMemoryBackend("us-east-1", "123456789012"))
}

// Test_SDKRoundTrip_CreateFlowVersion_ExecutionRoleArn proves
// CreateFlowVersionOutput/GetFlowVersionOutput's required "executionRoleArn"
// (api_op_CreateFlowVersion.go) decodes through the real SDK client.
// FlowVersion previously had no field for it at all -- the "member with no
// struct field" class -- despite the value (the parent Flow's RoleARN)
// already being stored and available to snapshot honestly.
func Test_SDKRoundTrip_CreateFlowVersion_ExecutionRoleArn(t *testing.T) {
	t.Parallel()

	client := newTestBedrockAgentClient(t, newTestBedrockAgentHandler())

	flow, err := client.CreateFlow(t.Context(), &bedrockagentsdk.CreateFlowInput{
		Name:             aws.String("r80d-flow"),
		ExecutionRoleArn: aws.String("arn:aws:iam::123456789012:role/FlowRole"),
	})
	require.NoError(t, err)

	version, err := client.CreateFlowVersion(t.Context(), &bedrockagentsdk.CreateFlowVersionInput{
		FlowIdentifier: flow.Id,
	})
	require.NoError(t, err)
	require.NotNil(
		t, version.ExecutionRoleArn,
		"ExecutionRoleArn must decode from the real \"executionRoleArn\" wire key",
	)
	assert.Equal(t, "arn:aws:iam::123456789012:role/FlowRole", *version.ExecutionRoleArn)

	got, err := client.GetFlowVersion(t.Context(), &bedrockagentsdk.GetFlowVersionInput{
		FlowIdentifier: flow.Id,
		FlowVersion:    version.Version,
	})
	require.NoError(t, err)
	require.NotNil(t, got.ExecutionRoleArn)
	assert.Equal(t, "arn:aws:iam::123456789012:role/FlowRole", *got.ExecutionRoleArn)
}

// Test_SDKRoundTrip_ListFlows_Summary proves ListFlowsOutput's element type
// (types.FlowSummary)'s required "arn"/"createdAt" decode through the real
// SDK client. FlowSummary previously had no fields for either at all.
func Test_SDKRoundTrip_ListFlows_Summary(t *testing.T) {
	t.Parallel()

	client := newTestBedrockAgentClient(t, newTestBedrockAgentHandler())

	flow, err := client.CreateFlow(t.Context(), &bedrockagentsdk.CreateFlowInput{
		Name:             aws.String("r80d-list-flow"),
		ExecutionRoleArn: aws.String("arn:aws:iam::123456789012:role/FlowRole"),
	})
	require.NoError(t, err)

	listed, err := client.ListFlows(t.Context(), &bedrockagentsdk.ListFlowsInput{})
	require.NoError(t, err)
	require.Len(t, listed.FlowSummaries, 1)
	require.NotNil(t, listed.FlowSummaries[0].Arn, "Arn must decode from the real \"arn\" wire key")
	assert.Equal(t, *flow.Arn, *listed.FlowSummaries[0].Arn)
	require.NotNil(t, listed.FlowSummaries[0].CreatedAt, "CreatedAt must decode from the real \"createdAt\" wire key")
	assert.False(t, listed.FlowSummaries[0].CreatedAt.IsZero())
}

// Test_SDKRoundTrip_CreatePromptVersion_UpdatedAt proves
// CreatePromptVersionOutput/GetPromptVersionOutput's required "updatedAt"
// (api_op_CreatePromptVersion.go) decodes through the real SDK client.
// PromptVersion previously had no field for it at all.
func Test_SDKRoundTrip_CreatePromptVersion_UpdatedAt(t *testing.T) {
	t.Parallel()

	client := newTestBedrockAgentClient(t, newTestBedrockAgentHandler())

	prompt, err := client.CreatePrompt(t.Context(), &bedrockagentsdk.CreatePromptInput{
		Name: aws.String("r80d-prompt"),
	})
	require.NoError(t, err)

	version, err := client.CreatePromptVersion(t.Context(), &bedrockagentsdk.CreatePromptVersionInput{
		PromptIdentifier: prompt.Id,
	})
	require.NoError(t, err)
	require.NotNil(t, version.UpdatedAt, "UpdatedAt must decode from the real \"updatedAt\" wire key")
	assert.False(t, version.UpdatedAt.IsZero())
}

// Test_SDKRoundTrip_AgentCollaborator_LastUpdatedAt proves
// types.AgentCollaborator/AgentCollaboratorSummary's required
// "lastUpdatedAt" decodes through the real SDK client. This struct
// previously tagged the field "updatedAt", a key no real deserializer for
// this shape reads, so it decoded nil on every op in the family.
func Test_SDKRoundTrip_AgentCollaborator_LastUpdatedAt(t *testing.T) {
	t.Parallel()

	client := newTestBedrockAgentClient(t, newTestBedrockAgentHandler())

	agent, err := client.CreateAgent(t.Context(), &bedrockagentsdk.CreateAgentInput{
		AgentName: aws.String("r80d-collab-agent"),
	})
	require.NoError(t, err)

	collab, err := client.AssociateAgentCollaborator(t.Context(), &bedrockagentsdk.AssociateAgentCollaboratorInput{
		AgentId:                  agent.Agent.AgentId,
		AgentVersion:             aws.String("DRAFT"),
		CollaboratorName:         aws.String("r80d-collab"),
		CollaborationInstruction: aws.String("collaborate"),
		AgentDescriptor: &types.AgentDescriptor{
			AliasArn: aws.String("arn:aws:bedrock:us-east-1:123456789012:agent-alias/AGENT1234X/ALIAS12345"),
		},
	})
	require.NoError(t, err)
	require.NotNil(
		t, collab.AgentCollaborator.LastUpdatedAt,
		"LastUpdatedAt must decode from the real \"lastUpdatedAt\" wire key",
	)
	assert.False(t, collab.AgentCollaborator.LastUpdatedAt.IsZero())

	listed, err := client.ListAgentCollaborators(t.Context(), &bedrockagentsdk.ListAgentCollaboratorsInput{
		AgentId:      agent.Agent.AgentId,
		AgentVersion: aws.String("DRAFT"),
	})
	require.NoError(t, err)
	require.Len(t, listed.AgentCollaboratorSummaries, 1)
	require.NotNil(
		t, listed.AgentCollaboratorSummaries[0].LastUpdatedAt,
		"LastUpdatedAt must decode from the real \"lastUpdatedAt\" wire key on the List element too",
	)
}

// Test_SDKRoundTrip_AgentVersion_IdleSessionTTL proves
// GetAgentVersionOutput's required "idleSessionTTLInSeconds" decodes
// through the real SDK client. AgentVersion previously had no field for it
// at all, despite the live Agent's own value already being stored and
// available to snapshot honestly.
func Test_SDKRoundTrip_AgentVersion_IdleSessionTTL(t *testing.T) {
	t.Parallel()

	client := newTestBedrockAgentClient(t, newTestBedrockAgentHandler())

	agent, err := client.CreateAgent(t.Context(), &bedrockagentsdk.CreateAgentInput{
		AgentName:               aws.String("r80d-ttl-agent"),
		IdleSessionTTLInSeconds: aws.Int32(1234),
	})
	require.NoError(t, err)

	alias, err := client.CreateAgentAlias(t.Context(), &bedrockagentsdk.CreateAgentAliasInput{
		AgentId:        agent.Agent.AgentId,
		AgentAliasName: aws.String("r80d-ttl-alias"),
	})
	require.NoError(t, err)
	require.Len(t, alias.AgentAlias.RoutingConfiguration, 1)

	got, err := client.GetAgentVersion(t.Context(), &bedrockagentsdk.GetAgentVersionInput{
		AgentId:      agent.Agent.AgentId,
		AgentVersion: alias.AgentAlias.RoutingConfiguration[0].AgentVersion,
	})
	require.NoError(t, err)
	require.NotNil(
		t, got.AgentVersion.IdleSessionTTLInSeconds,
		"IdleSessionTTLInSeconds must decode from the real \"idleSessionTTLInSeconds\" wire key",
	)
	assert.EqualValues(t, 1234, *got.AgentVersion.IdleSessionTTLInSeconds)

	listed, err := client.ListAgentVersions(t.Context(), &bedrockagentsdk.ListAgentVersionsInput{
		AgentId: agent.Agent.AgentId,
	})
	require.NoError(t, err)
	require.Len(t, listed.AgentVersionSummaries, 1)
	require.NotNil(
		t, listed.AgentVersionSummaries[0].CreatedAt,
		"CreatedAt must decode from the real \"createdAt\" wire key on ListAgentVersions' element",
	)
}

// Test_SDKRoundTrip_ListAgentActionGroups_UpdatedAt proves
// types.ActionGroupSummary's required "updatedAt" decodes through the real
// SDK client. ActionGroupSummary previously had no field for it at all.
func Test_SDKRoundTrip_ListAgentActionGroups_UpdatedAt(t *testing.T) {
	t.Parallel()

	client := newTestBedrockAgentClient(t, newTestBedrockAgentHandler())

	agent, err := client.CreateAgent(t.Context(), &bedrockagentsdk.CreateAgentInput{
		AgentName: aws.String("r80d-ag-agent"),
	})
	require.NoError(t, err)

	_, err = client.CreateAgentActionGroup(t.Context(), &bedrockagentsdk.CreateAgentActionGroupInput{
		AgentId:         agent.Agent.AgentId,
		AgentVersion:    aws.String("DRAFT"),
		ActionGroupName: aws.String("r80d-action-group"),
	})
	require.NoError(t, err)

	listed, err := client.ListAgentActionGroups(t.Context(), &bedrockagentsdk.ListAgentActionGroupsInput{
		AgentId:      agent.Agent.AgentId,
		AgentVersion: aws.String("DRAFT"),
	})
	require.NoError(t, err)
	require.Len(t, listed.ActionGroupSummaries, 1)
	require.NotNil(
		t, listed.ActionGroupSummaries[0].UpdatedAt,
		"UpdatedAt must decode from the real \"updatedAt\" wire key",
	)
}

// Test_SDKRoundTrip_ListAgentAliases_CreatedAtUpdatedAt proves
// types.AgentAliasSummary's required "createdAt"/"updatedAt" decode through
// the real SDK client. AgentAliasSummary previously had no fields for
// either at all.
func Test_SDKRoundTrip_ListAgentAliases_CreatedAtUpdatedAt(t *testing.T) {
	t.Parallel()

	client := newTestBedrockAgentClient(t, newTestBedrockAgentHandler())

	agent, err := client.CreateAgent(t.Context(), &bedrockagentsdk.CreateAgentInput{
		AgentName: aws.String("r80d-alias-agent"),
	})
	require.NoError(t, err)

	_, err = client.CreateAgentAlias(t.Context(), &bedrockagentsdk.CreateAgentAliasInput{
		AgentId:        agent.Agent.AgentId,
		AgentAliasName: aws.String("r80d-alias"),
	})
	require.NoError(t, err)

	listed, err := client.ListAgentAliases(t.Context(), &bedrockagentsdk.ListAgentAliasesInput{
		AgentId: agent.Agent.AgentId,
	})
	require.NoError(t, err)
	require.Len(t, listed.AgentAliasSummaries, 1)
	require.NotNil(
		t, listed.AgentAliasSummaries[0].CreatedAt,
		"CreatedAt must decode from the real \"createdAt\" wire key",
	)
	require.NotNil(
		t, listed.AgentAliasSummaries[0].UpdatedAt,
		"UpdatedAt must decode from the real \"updatedAt\" wire key",
	)
}
