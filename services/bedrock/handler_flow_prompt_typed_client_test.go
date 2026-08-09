package bedrock_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	bedrockagentsdk "github.com/aws/aws-sdk-go-v2/service/bedrockagent"
	bedrockagenttypes "github.com/aws/aws-sdk-go-v2/service/bedrockagent/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testExecutionRoleArn = "arn:aws:iam::000000000000:role/role"

// TestFlowCRUD_TypedClient drives Create/Get/List/Update/Prepare/Delete for
// a Flow through a real aws-sdk-go-v2 bedrockagent client. gopherstack-3ez4:
// CreateFlow/GetFlow/UpdateFlow wrapped their JSON body in a "flow" member
// and used "flowId"/"flowArn" keys, but CreateFlowResponse/GetFlowResponse/
// UpdateFlowResponse (botocore bedrock-agent 2023-06-05) have no httpPayload
// member and use flat "id"/"arn" keys -- a typed client decoded every field
// to its zero value. A raw-JSON test that hand-builds the same wrong
// envelope can't see this; only a typed client fails closed.
func TestFlowCRUD_TypedClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "create populates id arn name and status",
			run: func(t *testing.T) {
				t.Helper()

				srv := newTestBedrockRegistryServer(t)
				client := newTestBedrockAgentSDKClient(t, srv.URL)

				out, err := client.CreateFlow(t.Context(), &bedrockagentsdk.CreateFlowInput{
					Name:             aws.String("typed-flow"),
					ExecutionRoleArn: aws.String(testExecutionRoleArn),
				})
				require.NoError(t, err)

				assert.NotEmpty(t, aws.ToString(out.Id))
				assert.NotEmpty(t, aws.ToString(out.Arn))
				assert.Equal(t, "typed-flow", aws.ToString(out.Name))
				assert.Equal(t, bedrockagenttypes.FlowStatusNotPrepared, out.Status)
			},
		},
		{
			name: "get returns the created flow",
			run: func(t *testing.T) {
				t.Helper()

				srv := newTestBedrockRegistryServer(t)
				client := newTestBedrockAgentSDKClient(t, srv.URL)

				created, err := client.CreateFlow(t.Context(), &bedrockagentsdk.CreateFlowInput{
					Name:             aws.String("typed-flow-get"),
					ExecutionRoleArn: aws.String(testExecutionRoleArn),
				})
				require.NoError(t, err)

				got, err := client.GetFlow(t.Context(), &bedrockagentsdk.GetFlowInput{
					FlowIdentifier: created.Id,
				})
				require.NoError(t, err)

				assert.Equal(t, aws.ToString(created.Id), aws.ToString(got.Id))
				assert.Equal(t, aws.ToString(created.Arn), aws.ToString(got.Arn))
				assert.Equal(t, "typed-flow-get", aws.ToString(got.Name))
			},
		},
		{
			name: "list includes the created flow",
			run: func(t *testing.T) {
				t.Helper()

				srv := newTestBedrockRegistryServer(t)
				client := newTestBedrockAgentSDKClient(t, srv.URL)

				created, err := client.CreateFlow(t.Context(), &bedrockagentsdk.CreateFlowInput{
					Name:             aws.String("typed-flow-list"),
					ExecutionRoleArn: aws.String(testExecutionRoleArn),
				})
				require.NoError(t, err)

				out, err := client.ListFlows(t.Context(), &bedrockagentsdk.ListFlowsInput{})
				require.NoError(t, err)
				require.Len(t, out.FlowSummaries, 1)

				summary := out.FlowSummaries[0]
				assert.Equal(t, aws.ToString(created.Id), aws.ToString(summary.Id))
				assert.Equal(t, aws.ToString(created.Arn), aws.ToString(summary.Arn))
			},
		},
		{
			name: "update changes description and keeps id and arn",
			run: func(t *testing.T) {
				t.Helper()

				srv := newTestBedrockRegistryServer(t)
				client := newTestBedrockAgentSDKClient(t, srv.URL)

				created, err := client.CreateFlow(t.Context(), &bedrockagentsdk.CreateFlowInput{
					Name:             aws.String("typed-flow-update"),
					ExecutionRoleArn: aws.String(testExecutionRoleArn),
				})
				require.NoError(t, err)

				updated, err := client.UpdateFlow(t.Context(), &bedrockagentsdk.UpdateFlowInput{
					FlowIdentifier:   created.Id,
					Name:             aws.String("typed-flow-update"),
					Description:      aws.String("updated description"),
					ExecutionRoleArn: aws.String(testExecutionRoleArn),
				})
				require.NoError(t, err)

				assert.Equal(t, aws.ToString(created.Id), aws.ToString(updated.Id))
				assert.Equal(t, aws.ToString(created.Arn), aws.ToString(updated.Arn))
				assert.Equal(t, "updated description", aws.ToString(updated.Description))
			},
		},
		{
			name: "prepare returns the flow id and prepared status",
			run: func(t *testing.T) {
				t.Helper()

				srv := newTestBedrockRegistryServer(t)
				client := newTestBedrockAgentSDKClient(t, srv.URL)

				created, err := client.CreateFlow(t.Context(), &bedrockagentsdk.CreateFlowInput{
					Name:             aws.String("typed-flow-prepare"),
					ExecutionRoleArn: aws.String(testExecutionRoleArn),
				})
				require.NoError(t, err)

				out, err := client.PrepareFlow(t.Context(), &bedrockagentsdk.PrepareFlowInput{
					FlowIdentifier: created.Id,
				})
				require.NoError(t, err)

				assert.Equal(t, aws.ToString(created.Id), aws.ToString(out.Id))
				assert.Equal(t, bedrockagenttypes.FlowStatusPrepared, out.Status)
			},
		},
		{
			name: "delete returns the flow id",
			run: func(t *testing.T) {
				t.Helper()

				srv := newTestBedrockRegistryServer(t)
				client := newTestBedrockAgentSDKClient(t, srv.URL)

				created, err := client.CreateFlow(t.Context(), &bedrockagentsdk.CreateFlowInput{
					Name:             aws.String("typed-flow-delete"),
					ExecutionRoleArn: aws.String(testExecutionRoleArn),
				})
				require.NoError(t, err)

				out, err := client.DeleteFlow(t.Context(), &bedrockagentsdk.DeleteFlowInput{
					FlowIdentifier: created.Id,
				})
				require.NoError(t, err)
				assert.Equal(t, aws.ToString(created.Id), aws.ToString(out.Id))

				_, err = client.GetFlow(t.Context(), &bedrockagentsdk.GetFlowInput{
					FlowIdentifier: created.Id,
				})
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

// TestFlowAliasCRUD_TypedClient covers Create/Get/Update/Delete for a Flow
// alias through a real typed client (gopherstack-3ez4: same envelope/key
// class of bug as Flow -- see TestFlowCRUD_TypedClient).
func TestFlowAliasCRUD_TypedClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "create populates id arn and parent flow id",
			run: func(t *testing.T) {
				t.Helper()

				srv := newTestBedrockRegistryServer(t)
				client := newTestBedrockAgentSDKClient(t, srv.URL)

				flow := createTypedFlow(t, client, "typed-alias-flow-create")

				out, err := client.CreateFlowAlias(t.Context(), &bedrockagentsdk.CreateFlowAliasInput{
					FlowIdentifier: flow.Id,
					Name:           aws.String("typed-alias"),
					RoutingConfiguration: []bedrockagenttypes.FlowAliasRoutingConfigurationListItem{
						{FlowVersion: aws.String("DRAFT")},
					},
				})
				require.NoError(t, err)

				assert.NotEmpty(t, aws.ToString(out.Id))
				assert.NotEmpty(t, aws.ToString(out.Arn))
				assert.Equal(t, aws.ToString(flow.Id), aws.ToString(out.FlowId))
			},
		},
		{
			name: "get returns the created alias",
			run: func(t *testing.T) {
				t.Helper()

				srv := newTestBedrockRegistryServer(t)
				client := newTestBedrockAgentSDKClient(t, srv.URL)

				flow := createTypedFlow(t, client, "typed-alias-flow-get")
				alias := createTypedFlowAlias(t, client, flow, "typed-alias-get")

				got, err := client.GetFlowAlias(t.Context(), &bedrockagentsdk.GetFlowAliasInput{
					FlowIdentifier:  flow.Id,
					AliasIdentifier: alias.Id,
				})
				require.NoError(t, err)

				assert.Equal(t, aws.ToString(alias.Id), aws.ToString(got.Id))
				assert.Equal(t, aws.ToString(alias.Arn), aws.ToString(got.Arn))
			},
		},
		{
			name: "update changes the alias name",
			run: func(t *testing.T) {
				t.Helper()

				srv := newTestBedrockRegistryServer(t)
				client := newTestBedrockAgentSDKClient(t, srv.URL)

				flow := createTypedFlow(t, client, "typed-alias-flow-update")
				alias := createTypedFlowAlias(t, client, flow, "typed-alias-update")

				updated, err := client.UpdateFlowAlias(t.Context(), &bedrockagentsdk.UpdateFlowAliasInput{
					FlowIdentifier:  flow.Id,
					AliasIdentifier: alias.Id,
					Name:            aws.String("typed-alias-renamed"),
					RoutingConfiguration: []bedrockagenttypes.FlowAliasRoutingConfigurationListItem{
						{FlowVersion: aws.String("DRAFT")},
					},
				})
				require.NoError(t, err)

				assert.Equal(t, aws.ToString(alias.Id), aws.ToString(updated.Id))
				assert.Equal(t, "typed-alias-renamed", aws.ToString(updated.Name))
			},
		},
		{
			name: "delete returns the flow id and alias id",
			run: func(t *testing.T) {
				t.Helper()

				srv := newTestBedrockRegistryServer(t)
				client := newTestBedrockAgentSDKClient(t, srv.URL)

				flow := createTypedFlow(t, client, "typed-alias-flow-delete")
				alias := createTypedFlowAlias(t, client, flow, "typed-alias-delete")

				out, err := client.DeleteFlowAlias(t.Context(), &bedrockagentsdk.DeleteFlowAliasInput{
					FlowIdentifier:  flow.Id,
					AliasIdentifier: alias.Id,
				})
				require.NoError(t, err)

				assert.Equal(t, aws.ToString(flow.Id), aws.ToString(out.FlowId))
				assert.Equal(t, aws.ToString(alias.Id), aws.ToString(out.Id))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

// TestPromptCRUD_TypedClient covers Create/Get/Update/Delete for a Prompt
// through a real typed client (gopherstack-3ez4: same envelope/key class of
// bug as Flow -- see TestFlowCRUD_TypedClient).
func TestPromptCRUD_TypedClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "create populates id arn and name",
			run: func(t *testing.T) {
				t.Helper()

				srv := newTestBedrockRegistryServer(t)
				client := newTestBedrockAgentSDKClient(t, srv.URL)

				out, err := client.CreatePrompt(t.Context(), &bedrockagentsdk.CreatePromptInput{
					Name: aws.String("typed-prompt"),
				})
				require.NoError(t, err)

				assert.NotEmpty(t, aws.ToString(out.Id))
				assert.NotEmpty(t, aws.ToString(out.Arn))
				assert.Equal(t, "typed-prompt", aws.ToString(out.Name))
			},
		},
		{
			name: "get returns the created prompt",
			run: func(t *testing.T) {
				t.Helper()

				srv := newTestBedrockRegistryServer(t)
				client := newTestBedrockAgentSDKClient(t, srv.URL)

				created, err := client.CreatePrompt(t.Context(), &bedrockagentsdk.CreatePromptInput{
					Name: aws.String("typed-prompt-get"),
				})
				require.NoError(t, err)

				got, err := client.GetPrompt(t.Context(), &bedrockagentsdk.GetPromptInput{
					PromptIdentifier: created.Id,
				})
				require.NoError(t, err)

				assert.Equal(t, aws.ToString(created.Id), aws.ToString(got.Id))
				assert.Equal(t, aws.ToString(created.Arn), aws.ToString(got.Arn))
			},
		},
		{
			name: "update changes the prompt name",
			run: func(t *testing.T) {
				t.Helper()

				srv := newTestBedrockRegistryServer(t)
				client := newTestBedrockAgentSDKClient(t, srv.URL)

				created, err := client.CreatePrompt(t.Context(), &bedrockagentsdk.CreatePromptInput{
					Name: aws.String("typed-prompt-update"),
				})
				require.NoError(t, err)

				updated, err := client.UpdatePrompt(t.Context(), &bedrockagentsdk.UpdatePromptInput{
					PromptIdentifier: created.Id,
					Name:             aws.String("typed-prompt-renamed"),
				})
				require.NoError(t, err)

				assert.Equal(t, aws.ToString(created.Id), aws.ToString(updated.Id))
				assert.Equal(t, "typed-prompt-renamed", aws.ToString(updated.Name))
			},
		},
		{
			name: "delete returns the prompt id",
			run: func(t *testing.T) {
				t.Helper()

				srv := newTestBedrockRegistryServer(t)
				client := newTestBedrockAgentSDKClient(t, srv.URL)

				created, err := client.CreatePrompt(t.Context(), &bedrockagentsdk.CreatePromptInput{
					Name: aws.String("typed-prompt-delete"),
				})
				require.NoError(t, err)

				out, err := client.DeletePrompt(t.Context(), &bedrockagentsdk.DeletePromptInput{
					PromptIdentifier: created.Id,
				})
				require.NoError(t, err)
				assert.Equal(t, aws.ToString(created.Id), aws.ToString(out.Id))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

// TestFlowVersionCRUD_TypedClient covers Create/Get/Delete for a Flow
// version through a real typed client. CreateFlowVersion/GetFlowVersion had
// the same "flowVersion"-envelope bug as Create/GetFlow (gopherstack-3ez4).
func TestFlowVersionCRUD_TypedClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "create populates id arn and version",
			run: func(t *testing.T) {
				t.Helper()

				srv := newTestBedrockRegistryServer(t)
				client := newTestBedrockAgentSDKClient(t, srv.URL)

				flow := createTypedFlow(t, client, "typed-version-flow-create")

				out, err := client.CreateFlowVersion(t.Context(), &bedrockagentsdk.CreateFlowVersionInput{
					FlowIdentifier: flow.Id,
				})
				require.NoError(t, err)

				assert.Equal(t, aws.ToString(flow.Id), aws.ToString(out.Id))
				assert.NotEmpty(t, aws.ToString(out.Arn))
				assert.Equal(t, "1", aws.ToString(out.Version))
			},
		},
		{
			name: "get returns the created version",
			run: func(t *testing.T) {
				t.Helper()

				srv := newTestBedrockRegistryServer(t)
				client := newTestBedrockAgentSDKClient(t, srv.URL)

				flow := createTypedFlow(t, client, "typed-version-flow-get")

				created, err := client.CreateFlowVersion(t.Context(), &bedrockagentsdk.CreateFlowVersionInput{
					FlowIdentifier: flow.Id,
				})
				require.NoError(t, err)

				got, err := client.GetFlowVersion(t.Context(), &bedrockagentsdk.GetFlowVersionInput{
					FlowIdentifier: flow.Id,
					FlowVersion:    created.Version,
				})
				require.NoError(t, err)

				assert.Equal(t, aws.ToString(flow.Id), aws.ToString(got.Id))
				assert.Equal(t, aws.ToString(created.Version), aws.ToString(got.Version))
			},
		},
		{
			name: "delete returns the flow id and version",
			run: func(t *testing.T) {
				t.Helper()

				srv := newTestBedrockRegistryServer(t)
				client := newTestBedrockAgentSDKClient(t, srv.URL)

				flow := createTypedFlow(t, client, "typed-version-flow-delete")

				created, err := client.CreateFlowVersion(t.Context(), &bedrockagentsdk.CreateFlowVersionInput{
					FlowIdentifier: flow.Id,
				})
				require.NoError(t, err)

				out, err := client.DeleteFlowVersion(t.Context(), &bedrockagentsdk.DeleteFlowVersionInput{
					FlowIdentifier: flow.Id,
					FlowVersion:    created.Version,
				})
				require.NoError(t, err)

				assert.Equal(t, aws.ToString(flow.Id), aws.ToString(out.Id))
				assert.Equal(t, aws.ToString(created.Version), aws.ToString(out.Version))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

func createTypedFlow(
	t *testing.T, client *bedrockagentsdk.Client, name string,
) *bedrockagentsdk.CreateFlowOutput {
	t.Helper()

	out, err := client.CreateFlow(t.Context(), &bedrockagentsdk.CreateFlowInput{
		Name:             aws.String(name),
		ExecutionRoleArn: aws.String(testExecutionRoleArn),
	})
	require.NoError(t, err)

	return out
}

func createTypedFlowAlias(
	t *testing.T, client *bedrockagentsdk.Client, flow *bedrockagentsdk.CreateFlowOutput, name string,
) *bedrockagentsdk.CreateFlowAliasOutput {
	t.Helper()

	out, err := client.CreateFlowAlias(t.Context(), &bedrockagentsdk.CreateFlowAliasInput{
		FlowIdentifier: flow.Id,
		Name:           aws.String(name),
		RoutingConfiguration: []bedrockagenttypes.FlowAliasRoutingConfigurationListItem{
			{FlowVersion: aws.String("DRAFT")},
		},
	})
	require.NoError(t, err)

	return out
}
