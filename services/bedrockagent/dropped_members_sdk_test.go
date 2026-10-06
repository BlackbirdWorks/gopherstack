package bedrockagent_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	bedrockagentsdk "github.com/aws/aws-sdk-go-v2/service/bedrockagent"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagent/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	droppedRoleARN = "arn:aws:iam::123456789012:role/BedrockRole"
	droppedKMSARN  = "arn:aws:kms:us-east-1:123456789012:key/11111111-2222-3333-4444-555555555555"
)

func newDroppedAgent(t *testing.T, c *bedrockagentsdk.Client, name string) *types.Agent {
	t.Helper()

	out, err := c.CreateAgent(t.Context(), &bedrockagentsdk.CreateAgentInput{
		AgentName:            aws.String(name),
		FoundationModel:      aws.String("anthropic.claude-v2"),
		AgentResourceRoleArn: aws.String(droppedRoleARN),
	})
	require.NoError(t, err)

	return out.Agent
}

func newDroppedFlow(t *testing.T, c *bedrockagentsdk.Client, name string) *bedrockagentsdk.CreateFlowOutput {
	t.Helper()

	out, err := c.CreateFlow(t.Context(), &bedrockagentsdk.CreateFlowInput{
		Name:             aws.String(name),
		ExecutionRoleArn: aws.String(droppedRoleARN),
	})
	require.NoError(t, err)

	return out
}

func TestDroppedMembers_AgentConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, c *bedrockagentsdk.Client)
		name string
	}{
		{
			name: "create stores kms key, custom orchestration and prompt overrides",
			run: func(t *testing.T, c *bedrockagentsdk.Client) {
				t.Helper()

				created, err := c.CreateAgent(t.Context(), &bedrockagentsdk.CreateAgentInput{
					AgentName:                aws.String("cfg-agent"),
					FoundationModel:          aws.String("anthropic.claude-v2"),
					AgentResourceRoleArn:     aws.String(droppedRoleARN),
					CustomerEncryptionKeyArn: aws.String(droppedKMSARN),
					OrchestrationType:        types.OrchestrationTypeCustomOrchestration,
					CustomOrchestration: &types.CustomOrchestration{
						Executor: &types.OrchestrationExecutorMemberLambda{
							Value: "arn:aws:lambda:us-east-1:123456789012:function:orch",
						},
					},
					PromptOverrideConfiguration: &types.PromptOverrideConfiguration{
						OverrideLambda: aws.String("arn:aws:lambda:us-east-1:123456789012:function:parser"),
						PromptConfigurations: []types.PromptConfiguration{{
							PromptType:             types.PromptTypePreProcessing,
							PromptState:            types.PromptStateDisabled,
							BasePromptTemplate:     aws.String("template"),
							ParserMode:             types.CreationModeOverridden,
							PromptCreationMode:     types.CreationModeOverridden,
							InferenceConfiguration: &types.InferenceConfiguration{MaximumLength: aws.Int32(256)},
						}},
					},
				})
				require.NoError(t, err)

				got, err := c.GetAgent(t.Context(), &bedrockagentsdk.GetAgentInput{AgentId: created.Agent.AgentId})
				require.NoError(t, err)
				assert.Equal(t, droppedKMSARN, aws.ToString(got.Agent.CustomerEncryptionKeyArn))

				lambda, ok := got.Agent.CustomOrchestration.Executor.(*types.OrchestrationExecutorMemberLambda)
				require.True(t, ok)
				assert.Equal(t, "arn:aws:lambda:us-east-1:123456789012:function:orch", lambda.Value)

				po := got.Agent.PromptOverrideConfiguration
				require.NotNil(t, po)
				assert.Equal(
					t,
					"arn:aws:lambda:us-east-1:123456789012:function:parser",
					aws.ToString(po.OverrideLambda),
				)
				require.Len(t, po.PromptConfigurations, 1)
				assert.Equal(t, types.PromptStateDisabled, po.PromptConfigurations[0].PromptState)
				assert.Equal(t, "template", aws.ToString(po.PromptConfigurations[0].BasePromptTemplate))
			},
		},
		{
			name: "update replaces the kms key and numbered versions snapshot it",
			run: func(t *testing.T, c *bedrockagentsdk.Client) {
				t.Helper()

				agent := newDroppedAgent(t, c, "kms-update-agent")
				_, err := c.UpdateAgent(t.Context(), &bedrockagentsdk.UpdateAgentInput{
					AgentId:                  agent.AgentId,
					AgentName:                agent.AgentName,
					FoundationModel:          aws.String("anthropic.claude-v2"),
					AgentResourceRoleArn:     aws.String(droppedRoleARN),
					CustomerEncryptionKeyArn: aws.String(droppedKMSARN),
				})
				require.NoError(t, err)

				alias, err := c.CreateAgentAlias(t.Context(), &bedrockagentsdk.CreateAgentAliasInput{
					AgentId: agent.AgentId, AgentAliasName: aws.String("live"),
				})
				require.NoError(t, err)

				version := alias.AgentAlias.RoutingConfiguration[0].AgentVersion
				got, err := c.GetAgentVersion(t.Context(), &bedrockagentsdk.GetAgentVersionInput{
					AgentId: agent.AgentId, AgentVersion: version,
				})
				require.NoError(t, err)
				assert.Equal(t, droppedKMSARN, aws.ToString(got.AgentVersion.CustomerEncryptionKeyArn))
			},
		},
		{
			name: "same client token returns the same agent",
			run: func(t *testing.T, c *bedrockagentsdk.Client) {
				t.Helper()

				in := &bedrockagentsdk.CreateAgentInput{
					AgentName:   aws.String("token-agent"),
					ClientToken: aws.String("token-agent-1"),
				}
				first, err := c.CreateAgent(t.Context(), in)
				require.NoError(t, err)

				second, err := c.CreateAgent(t.Context(), in)
				require.NoError(t, err)
				assert.Equal(t, aws.ToString(first.Agent.AgentId), aws.ToString(second.Agent.AgentId))
				assert.Equal(t, "token-agent-1", aws.ToString(second.Agent.ClientToken))

				listed, err := c.ListAgents(t.Context(), &bedrockagentsdk.ListAgentsInput{})
				require.NoError(t, err)
				assert.Len(t, listed.AgentSummaries, 1)
			},
		},
		{
			name: "different client token on an existing name conflicts",
			run: func(t *testing.T, c *bedrockagentsdk.Client) {
				t.Helper()

				_, err := c.CreateAgent(t.Context(), &bedrockagentsdk.CreateAgentInput{
					AgentName: aws.String("conflict-agent"), ClientToken: aws.String("a"),
				})
				require.NoError(t, err)

				_, err = c.CreateAgent(t.Context(), &bedrockagentsdk.CreateAgentInput{
					AgentName: aws.String("conflict-agent"), ClientToken: aws.String("b"),
				})
				var conflict *types.ConflictException
				require.ErrorAs(t, err, &conflict)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.run(t, newTestHandlerAndClient(t))
		})
	}
}

func TestDroppedMembers_AgentSubResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, c *bedrockagentsdk.Client)
		name string
	}{
		{
			name: "parent action group signature is stored and echoed as parentActionSignature",
			run: func(t *testing.T, c *bedrockagentsdk.Client) {
				t.Helper()

				agent := newDroppedAgent(t, c, "sig-agent")
				created, err := c.CreateAgentActionGroup(t.Context(), &bedrockagentsdk.CreateAgentActionGroupInput{
					AgentId:                    agent.AgentId,
					AgentVersion:               aws.String("DRAFT"),
					ActionGroupName:            aws.String("user-input"),
					ParentActionGroupSignature: types.ActionGroupSignatureAmazonUserinput,
				})
				require.NoError(t, err)
				assert.Equal(
					t,
					types.ActionGroupSignatureAmazonUserinput,
					created.AgentActionGroup.ParentActionSignature,
				)

				got, err := c.GetAgentActionGroup(t.Context(), &bedrockagentsdk.GetAgentActionGroupInput{
					AgentId:       agent.AgentId,
					AgentVersion:  aws.String("DRAFT"),
					ActionGroupId: created.AgentActionGroup.ActionGroupId,
				})
				require.NoError(t, err)
				assert.Equal(t, types.ActionGroupSignatureAmazonUserinput, got.AgentActionGroup.ParentActionSignature)
			},
		},
		{
			name: "parent signature with a description is rejected",
			run: func(t *testing.T, c *bedrockagentsdk.Client) {
				t.Helper()

				agent := newDroppedAgent(t, c, "sig-bad-agent")
				_, err := c.CreateAgentActionGroup(t.Context(), &bedrockagentsdk.CreateAgentActionGroupInput{
					AgentId:                    agent.AgentId,
					AgentVersion:               aws.String("DRAFT"),
					ActionGroupName:            aws.String("bad"),
					Description:                aws.String("must be blank"),
					ParentActionGroupSignature: types.ActionGroupSignatureAmazonCodeinterpreter,
				})
				var verr *types.ValidationException
				require.ErrorAs(t, err, &verr)
			},
		},
		{
			name: "unknown parent signature is rejected",
			run: func(t *testing.T, c *bedrockagentsdk.Client) {
				t.Helper()

				agent := newDroppedAgent(t, c, "sig-enum-agent")
				_, err := c.CreateAgentActionGroup(t.Context(), &bedrockagentsdk.CreateAgentActionGroupInput{
					AgentId:                    agent.AgentId,
					AgentVersion:               aws.String("DRAFT"),
					ActionGroupName:            aws.String("bad"),
					ParentActionGroupSignature: types.ActionGroupSignature("AMAZON.Nope"),
				})
				var verr *types.ValidationException
				require.ErrorAs(t, err, &verr)
			},
		},
		{
			name: "action group client token replays",
			run: func(t *testing.T, c *bedrockagentsdk.Client) {
				t.Helper()

				agent := newDroppedAgent(t, c, "ag-token-agent")
				in := &bedrockagentsdk.CreateAgentActionGroupInput{
					AgentId:         agent.AgentId,
					AgentVersion:    aws.String("DRAFT"),
					ActionGroupName: aws.String("ag"),
					ClientToken:     aws.String("ag-token"),
				}
				first, err := c.CreateAgentActionGroup(t.Context(), in)
				require.NoError(t, err)

				second, err := c.CreateAgentActionGroup(t.Context(), in)
				require.NoError(t, err)
				assert.Equal(
					t,
					aws.ToString(first.AgentActionGroup.ActionGroupId),
					aws.ToString(second.AgentActionGroup.ActionGroupId),
				)
				assert.Equal(t, "ag-token", aws.ToString(second.AgentActionGroup.ClientToken))
			},
		},
		{
			name: "alias invocation state defaults to accept and update applies it",
			run: func(t *testing.T, c *bedrockagentsdk.Client) {
				t.Helper()

				agent := newDroppedAgent(t, c, "alias-state-agent")
				created, err := c.CreateAgentAlias(t.Context(), &bedrockagentsdk.CreateAgentAliasInput{
					AgentId: agent.AgentId, AgentAliasName: aws.String("live"), ClientToken: aws.String("alias-token"),
				})
				require.NoError(t, err)
				assert.Equal(t, types.AliasInvocationStateAcceptInvocations, created.AgentAlias.AliasInvocationState)
				assert.Equal(t, "alias-token", aws.ToString(created.AgentAlias.ClientToken))

				replay, err := c.CreateAgentAlias(t.Context(), &bedrockagentsdk.CreateAgentAliasInput{
					AgentId: agent.AgentId, AgentAliasName: aws.String("live"), ClientToken: aws.String("alias-token"),
				})
				require.NoError(t, err)
				assert.Equal(
					t,
					aws.ToString(created.AgentAlias.AgentAliasId),
					aws.ToString(replay.AgentAlias.AgentAliasId),
				)

				updated, err := c.UpdateAgentAlias(t.Context(), &bedrockagentsdk.UpdateAgentAliasInput{
					AgentId:              agent.AgentId,
					AgentAliasId:         created.AgentAlias.AgentAliasId,
					AgentAliasName:       aws.String("live"),
					AliasInvocationState: types.AliasInvocationStateRejectInvocations,
				})
				require.NoError(t, err)
				assert.Equal(t, types.AliasInvocationStateRejectInvocations, updated.AgentAlias.AliasInvocationState)

				listed, err := c.ListAgentAliases(
					t.Context(),
					&bedrockagentsdk.ListAgentAliasesInput{AgentId: agent.AgentId},
				)
				require.NoError(t, err)
				require.Len(t, listed.AgentAliasSummaries, 1)
				assert.Equal(
					t,
					types.AliasInvocationStateRejectInvocations,
					listed.AgentAliasSummaries[0].AliasInvocationState,
				)
			},
		},
		{
			name: "collaborator client token replays",
			run: func(t *testing.T, c *bedrockagentsdk.Client) {
				t.Helper()

				agent := newDroppedAgent(t, c, "collab-token-agent")
				in := &bedrockagentsdk.AssociateAgentCollaboratorInput{
					AgentId:                  agent.AgentId,
					AgentVersion:             aws.String("DRAFT"),
					CollaboratorName:         aws.String("helper"),
					CollaborationInstruction: aws.String("help"),
					ClientToken:              aws.String("collab-token"),
					AgentDescriptor: &types.AgentDescriptor{
						AliasArn: aws.String(
							"arn:aws:bedrock:us-east-1:123456789012:agent-alias/AGENT1234X/ALIAS12345",
						),
					},
				}
				first, err := c.AssociateAgentCollaborator(t.Context(), in)
				require.NoError(t, err)

				second, err := c.AssociateAgentCollaborator(t.Context(), in)
				require.NoError(t, err)
				assert.Equal(
					t,
					aws.ToString(first.AgentCollaborator.CollaboratorId),
					aws.ToString(second.AgentCollaborator.CollaboratorId),
				)
				assert.Equal(t, "collab-token", aws.ToString(second.AgentCollaborator.ClientToken))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.run(t, newTestHandlerAndClient(t))
		})
	}
}

func TestDroppedMembers_FlowsAndPrompts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, c *bedrockagentsdk.Client)
		name string
	}{
		{
			name: "flow kms key flows into versions and token replays",
			run: func(t *testing.T, c *bedrockagentsdk.Client) {
				t.Helper()

				in := &bedrockagentsdk.CreateFlowInput{
					Name:                     aws.String("kms-flow"),
					ExecutionRoleArn:         aws.String(droppedRoleARN),
					CustomerEncryptionKeyArn: aws.String(droppedKMSARN),
					ClientToken:              aws.String("flow-token"),
				}
				flow, err := c.CreateFlow(t.Context(), in)
				require.NoError(t, err)
				assert.Equal(t, droppedKMSARN, aws.ToString(flow.CustomerEncryptionKeyArn))

				replay, err := c.CreateFlow(t.Context(), in)
				require.NoError(t, err)
				assert.Equal(t, aws.ToString(flow.Id), aws.ToString(replay.Id))

				version, err := c.CreateFlowVersion(t.Context(), &bedrockagentsdk.CreateFlowVersionInput{
					FlowIdentifier: flow.Id, ClientToken: aws.String("fv-token"),
				})
				require.NoError(t, err)
				assert.Equal(t, droppedKMSARN, aws.ToString(version.CustomerEncryptionKeyArn))

				again, err := c.CreateFlowVersion(t.Context(), &bedrockagentsdk.CreateFlowVersionInput{
					FlowIdentifier: flow.Id, ClientToken: aws.String("fv-token"),
				})
				require.NoError(t, err)
				assert.Equal(t, aws.ToString(version.Version), aws.ToString(again.Version))
			},
		},
		{
			name: "get flow with METADATA_ONLY omits the definition",
			run: func(t *testing.T, c *bedrockagentsdk.Client) {
				t.Helper()

				flow, err := c.CreateFlow(t.Context(), &bedrockagentsdk.CreateFlowInput{
					Name:             aws.String("meta-flow"),
					ExecutionRoleArn: aws.String(droppedRoleARN),
					Definition: &types.FlowDefinition{Nodes: []types.FlowNode{{
						Name: aws.String("in"), Type: types.FlowNodeTypeInput,
						Configuration: &types.FlowNodeConfigurationMemberInput{
							Value: types.InputFlowNodeConfiguration{},
						},
					}}},
				})
				require.NoError(t, err)

				full, err := c.GetFlow(t.Context(), &bedrockagentsdk.GetFlowInput{FlowIdentifier: flow.Id})
				require.NoError(t, err)
				require.NotNil(t, full.Definition)

				meta, err := c.GetFlow(t.Context(), &bedrockagentsdk.GetFlowInput{
					FlowIdentifier: flow.Id, IncludedData: types.IncludedDataMetadataOnly,
				})
				require.NoError(t, err)
				assert.Nil(t, meta.Definition)
				assert.Equal(t, aws.ToString(flow.Name), aws.ToString(meta.Name))

				version, err := c.CreateFlowVersion(
					t.Context(),
					&bedrockagentsdk.CreateFlowVersionInput{FlowIdentifier: flow.Id},
				)
				require.NoError(t, err)

				metaVersion, err := c.GetFlowVersion(t.Context(), &bedrockagentsdk.GetFlowVersionInput{
					FlowIdentifier: flow.Id, FlowVersion: version.Version, IncludedData: types.IncludedDataMetadataOnly,
				})
				require.NoError(t, err)
				assert.Nil(t, metaVersion.Definition)
			},
		},
		{
			name: "flow alias stores concurrency configuration and token replays",
			run: func(t *testing.T, c *bedrockagentsdk.Client) {
				t.Helper()

				flow := newDroppedFlow(t, c, "alias-flow")
				version, err := c.CreateFlowVersion(
					t.Context(),
					&bedrockagentsdk.CreateFlowVersionInput{FlowIdentifier: flow.Id},
				)
				require.NoError(t, err)

				in := &bedrockagentsdk.CreateFlowAliasInput{
					FlowIdentifier:       flow.Id,
					Name:                 aws.String("live"),
					ClientToken:          aws.String("fa-token"),
					RoutingConfiguration: []types.FlowAliasRoutingConfigurationListItem{{FlowVersion: version.Version}},
					ConcurrencyConfiguration: &types.FlowAliasConcurrencyConfiguration{
						Type: types.ConcurrencyTypeManual, MaxConcurrency: aws.Int32(4),
					},
				}
				alias, err := c.CreateFlowAlias(t.Context(), in)
				require.NoError(t, err)
				require.NotNil(t, alias.ConcurrencyConfiguration)
				assert.Equal(t, types.ConcurrencyTypeManual, alias.ConcurrencyConfiguration.Type)
				assert.EqualValues(t, 4, aws.ToInt32(alias.ConcurrencyConfiguration.MaxConcurrency))

				replay, err := c.CreateFlowAlias(t.Context(), in)
				require.NoError(t, err)
				assert.Equal(t, aws.ToString(alias.Id), aws.ToString(replay.Id))

				updated, err := c.UpdateFlowAlias(t.Context(), &bedrockagentsdk.UpdateFlowAliasInput{
					FlowIdentifier:       flow.Id,
					AliasIdentifier:      alias.Id,
					Name:                 aws.String("live"),
					RoutingConfiguration: in.RoutingConfiguration,
					ConcurrencyConfiguration: &types.FlowAliasConcurrencyConfiguration{
						Type: types.ConcurrencyTypeAutomatic,
					},
				})
				require.NoError(t, err)
				assert.Equal(t, types.ConcurrencyTypeAutomatic, updated.ConcurrencyConfiguration.Type)

				got, err := c.GetFlowAlias(t.Context(), &bedrockagentsdk.GetFlowAliasInput{
					FlowIdentifier: flow.Id, AliasIdentifier: alias.Id,
				})
				require.NoError(t, err)
				assert.Equal(t, types.ConcurrencyTypeAutomatic, got.ConcurrencyConfiguration.Type)
			},
		},
		{
			name: "prompt kms key, versions, promptVersion lookup and delete",
			run: func(t *testing.T, c *bedrockagentsdk.Client) {
				t.Helper()

				prompt, err := c.CreatePrompt(t.Context(), &bedrockagentsdk.CreatePromptInput{
					Name:                     aws.String("kms-prompt"),
					CustomerEncryptionKeyArn: aws.String(droppedKMSARN),
					ClientToken:              aws.String("p-token"),
					DefaultVariant:           aws.String("v1"),
					Variants: []types.PromptVariant{{
						Name:         aws.String("v1"),
						TemplateType: types.PromptTemplateTypeText,
						ModelId:      aws.String("anthropic.claude-v2"),
						TemplateConfiguration: &types.PromptTemplateConfigurationMemberText{
							Value: types.TextPromptTemplateConfiguration{Text: aws.String("hello")},
						},
					}},
				})
				require.NoError(t, err)
				assert.Equal(t, droppedKMSARN, aws.ToString(prompt.CustomerEncryptionKeyArn))

				replay, err := c.CreatePrompt(t.Context(), &bedrockagentsdk.CreatePromptInput{
					Name: aws.String("kms-prompt"), ClientToken: aws.String("p-token"),
				})
				require.NoError(t, err)
				assert.Equal(t, aws.ToString(prompt.Id), aws.ToString(replay.Id))

				v1, err := c.CreatePromptVersion(t.Context(), &bedrockagentsdk.CreatePromptVersionInput{
					PromptIdentifier: prompt.Id,
					ClientToken:      aws.String("pv-token"),
					Tags:             map[string]string{"team": "a"},
				})
				require.NoError(t, err)
				assert.Equal(t, droppedKMSARN, aws.ToString(v1.CustomerEncryptionKeyArn))
				assert.Equal(t, "v1", aws.ToString(v1.DefaultVariant))
				assert.Equal(t, aws.ToString(prompt.Arn)+":1", aws.ToString(v1.Arn))

				again, err := c.CreatePromptVersion(t.Context(), &bedrockagentsdk.CreatePromptVersionInput{
					PromptIdentifier: prompt.Id, ClientToken: aws.String("pv-token"),
				})
				require.NoError(t, err)
				assert.Equal(t, "1", aws.ToString(again.Version))

				_, err = c.UpdatePrompt(t.Context(), &bedrockagentsdk.UpdatePromptInput{
					PromptIdentifier: prompt.Id,
					Name:             aws.String("kms-prompt"),
					DefaultVariant:   aws.String("v1"),
					Variants: []types.PromptVariant{{
						Name:         aws.String("v1"),
						TemplateType: types.PromptTemplateTypeText,
						TemplateConfiguration: &types.PromptTemplateConfigurationMemberText{
							Value: types.TextPromptTemplateConfiguration{Text: aws.String("changed")},
						},
					}},
				})
				require.NoError(t, err)

				tags, err := c.ListTagsForResource(
					t.Context(),
					&bedrockagentsdk.ListTagsForResourceInput{ResourceArn: v1.Arn},
				)
				require.NoError(t, err)
				assert.Equal(t, map[string]string{"team": "a"}, tags.Tags)

				got, err := c.GetPrompt(t.Context(), &bedrockagentsdk.GetPromptInput{
					PromptIdentifier: prompt.Id, PromptVersion: aws.String("1"),
				})
				require.NoError(t, err)
				assert.Equal(t, "1", aws.ToString(got.Version))
				require.Len(t, got.Variants, 1)

				meta, err := c.GetPrompt(t.Context(), &bedrockagentsdk.GetPromptInput{
					PromptIdentifier: prompt.Id, IncludedData: types.IncludedDataMetadataOnly,
				})
				require.NoError(t, err)
				assert.Empty(t, meta.Variants)

				listed, err := c.ListPrompts(
					t.Context(),
					&bedrockagentsdk.ListPromptsInput{PromptIdentifier: prompt.Id},
				)
				require.NoError(t, err)
				require.Len(t, listed.PromptSummaries, 2)
				assert.Equal(t, "DRAFT", aws.ToString(listed.PromptSummaries[0].Version))
				assert.Equal(t, "1", aws.ToString(listed.PromptSummaries[1].Version))

				all, err := c.ListPrompts(t.Context(), &bedrockagentsdk.ListPromptsInput{})
				require.NoError(t, err)
				assert.Len(t, all.PromptSummaries, 1)

				del, err := c.DeletePrompt(t.Context(), &bedrockagentsdk.DeletePromptInput{
					PromptIdentifier: prompt.Id, PromptVersion: aws.String("1"),
				})
				require.NoError(t, err)
				assert.Equal(t, "1", aws.ToString(del.Version))

				_, err = c.GetPrompt(t.Context(), &bedrockagentsdk.GetPromptInput{
					PromptIdentifier: prompt.Id, PromptVersion: aws.String("1"),
				})
				var nf *types.ResourceNotFoundException
				require.ErrorAs(t, err, &nf)

				still, err := c.GetPrompt(t.Context(), &bedrockagentsdk.GetPromptInput{PromptIdentifier: prompt.Id})
				require.NoError(t, err)
				assert.Equal(t, aws.ToString(prompt.Id), aws.ToString(still.Id))
			},
		},
		{
			name: "list prompts for an unknown identifier is not found",
			run: func(t *testing.T, c *bedrockagentsdk.Client) {
				t.Helper()

				_, err := c.ListPrompts(
					t.Context(),
					&bedrockagentsdk.ListPromptsInput{PromptIdentifier: aws.String("NOSUCHPROMPT")},
				)
				var nf *types.ResourceNotFoundException
				require.ErrorAs(t, err, &nf)
			},
		},
		{
			name: "invalid includedData is rejected",
			run: func(t *testing.T, c *bedrockagentsdk.Client) {
				t.Helper()

				flow := newDroppedFlow(t, c, "bad-included")
				_, err := c.GetFlow(t.Context(), &bedrockagentsdk.GetFlowInput{
					FlowIdentifier: flow.Id, IncludedData: types.IncludedData("SOME"),
				})
				var verr *types.ValidationException
				require.ErrorAs(t, err, &verr)
			},
		},
		{
			name: "invalid concurrency type is rejected",
			run: func(t *testing.T, c *bedrockagentsdk.Client) {
				t.Helper()

				flow := newDroppedFlow(t, c, "bad-concurrency")
				_, err := c.CreateFlowAlias(t.Context(), &bedrockagentsdk.CreateFlowAliasInput{
					FlowIdentifier: flow.Id,
					Name:           aws.String("a"),
					RoutingConfiguration: []types.FlowAliasRoutingConfigurationListItem{
						{FlowVersion: aws.String("1")},
					},
					ConcurrencyConfiguration: &types.FlowAliasConcurrencyConfiguration{
						Type: types.ConcurrencyType("Weird"),
					},
				})
				var verr *types.ValidationException
				require.ErrorAs(t, err, &verr)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.run(t, newTestHandlerAndClient(t))
		})
	}
}

func TestDroppedMembers_KnowledgeBases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, c *bedrockagentsdk.Client)
		name string
	}{
		{
			name: "knowledge base, data source and ingestion job tokens replay and sse config round-trips",
			run: func(t *testing.T, c *bedrockagentsdk.Client) {
				t.Helper()

				kbIn := &bedrockagentsdk.CreateKnowledgeBaseInput{
					Name:        aws.String("kb"),
					RoleArn:     aws.String(droppedRoleARN),
					ClientToken: aws.String("kb-token"),
					KnowledgeBaseConfiguration: &types.KnowledgeBaseConfiguration{
						Type: types.KnowledgeBaseTypeVector,
					},
					StorageConfiguration: &types.StorageConfiguration{
						Type: types.KnowledgeBaseStorageTypeOpensearchServerless,
					},
				}
				kb, err := c.CreateKnowledgeBase(t.Context(), kbIn)
				require.NoError(t, err)

				kbAgain, err := c.CreateKnowledgeBase(t.Context(), kbIn)
				require.NoError(t, err)
				assert.Equal(
					t,
					aws.ToString(kb.KnowledgeBase.KnowledgeBaseId),
					aws.ToString(kbAgain.KnowledgeBase.KnowledgeBaseId),
				)

				dsIn := &bedrockagentsdk.CreateDataSourceInput{
					KnowledgeBaseId: kb.KnowledgeBase.KnowledgeBaseId,
					Name:            aws.String("ds"),
					ClientToken:     aws.String("ds-token"),
					DataSourceConfiguration: &types.DataSourceConfiguration{
						Type: types.DataSourceTypeS3,
						S3Configuration: &types.S3DataSourceConfiguration{
							BucketArn: aws.String("arn:aws:s3:::bucket"),
						},
					},
					ServerSideEncryptionConfiguration: &types.ServerSideEncryptionConfiguration{
						KmsKeyArn: aws.String(droppedKMSARN),
					},
				}
				ds, err := c.CreateDataSource(t.Context(), dsIn)
				require.NoError(t, err)
				assert.Equal(t, droppedKMSARN, aws.ToString(ds.DataSource.ServerSideEncryptionConfiguration.KmsKeyArn))

				dsAgain, err := c.CreateDataSource(t.Context(), dsIn)
				require.NoError(t, err)
				assert.Equal(t, aws.ToString(ds.DataSource.DataSourceId), aws.ToString(dsAgain.DataSource.DataSourceId))

				updated, err := c.UpdateDataSource(t.Context(), &bedrockagentsdk.UpdateDataSourceInput{
					KnowledgeBaseId:         kb.KnowledgeBase.KnowledgeBaseId,
					DataSourceId:            ds.DataSource.DataSourceId,
					Name:                    aws.String("ds"),
					DataSourceConfiguration: dsIn.DataSourceConfiguration,
					ServerSideEncryptionConfiguration: &types.ServerSideEncryptionConfiguration{
						KmsKeyArn: aws.String(droppedKMSARN + "-2"),
					},
				})
				require.NoError(t, err)
				assert.Equal(
					t,
					droppedKMSARN+"-2",
					aws.ToString(updated.DataSource.ServerSideEncryptionConfiguration.KmsKeyArn),
				)

				jobIn := &bedrockagentsdk.StartIngestionJobInput{
					KnowledgeBaseId: kb.KnowledgeBase.KnowledgeBaseId,
					DataSourceId:    ds.DataSource.DataSourceId,
					ClientToken:     aws.String("job-token"),
				}
				job, err := c.StartIngestionJob(t.Context(), jobIn)
				require.NoError(t, err)

				jobAgain, err := c.StartIngestionJob(t.Context(), jobIn)
				require.NoError(t, err)
				assert.Equal(
					t,
					aws.ToString(job.IngestionJob.IngestionJobId),
					aws.ToString(jobAgain.IngestionJob.IngestionJobId),
				)

				other, err := c.StartIngestionJob(t.Context(), &bedrockagentsdk.StartIngestionJobInput{
					KnowledgeBaseId: kb.KnowledgeBase.KnowledgeBaseId,
					DataSourceId:    ds.DataSource.DataSourceId,
					ClientToken:     aws.String("job-token-2"),
				})
				require.NoError(t, err)
				assert.NotEqual(
					t,
					aws.ToString(job.IngestionJob.IngestionJobId),
					aws.ToString(other.IngestionJob.IngestionJobId),
				)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.run(t, newTestHandlerAndClient(t))
		})
	}
}
