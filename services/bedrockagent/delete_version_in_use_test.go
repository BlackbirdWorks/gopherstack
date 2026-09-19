package bedrockagent_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	bedrockagentsdk "github.com/aws/aws-sdk-go-v2/service/bedrockagent"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagent/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrockagent"
)

// TestDeleteAgentVersion_BlockedWhileAliasReferencesIt guards
// api_op_DeleteAgentVersion.go's documented precondition: "By default, this
// value is false and deletion is stopped if the resource is in use. If you
// set it to true, the resource will be deleted even if the resource is in
// use." An agent version is in use when an alias's routingConfiguration
// still points at it.
func TestDeleteAgentVersion_BlockedWhileAliasReferencesIt(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	cases := []struct {
		name    string
		skip    bool
		wantErr bool
	}{
		{name: "blocked by default", skip: false, wantErr: true},
		{name: "skip flag deletes anyway", skip: true, wantErr: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := bedrockagent.NewTestBackend("us-east-1", "123456789012")

			agent, err := b.CreateAgent(ctx, bedrockagent.AgentConfig{
				AgentName:       "in-use-agent",
				FoundationModel: "anthropic.claude-v2",
				RoleARN:         "arn:aws:iam::123456789012:role/BedrockRole",
			})
			require.NoError(t, err)

			alias, err := b.CreateAgentAlias(ctx, agent.AgentID, bedrockagent.AliasConfig{
				AliasName: "in-use-alias",
			})
			require.NoError(t, err)
			version := alias.RoutingConfiguration[0].AgentVersion

			err = b.DeleteAgentVersion(ctx, agent.AgentID, version, tc.skip)

			if !tc.wantErr {
				require.NoError(t, err)
				_, getErr := b.GetAgentVersion(ctx, agent.AgentID, version)
				assert.ErrorIs(t, getErr, bedrockagent.ErrNotFound)

				return
			}

			require.Error(t, err)
			require.ErrorIs(t, err, bedrockagent.ErrResourceInUse)

			_, getErr := b.GetAgentVersion(ctx, agent.AgentID, version)
			assert.NoError(t, getErr, "version must survive a blocked delete")
		})
	}
}

// TestDeleteFlowVersion_BlockedWhileAliasReferencesIt mirrors the agent
// version case for api_op_DeleteFlowVersion.go's identical
// skipResourceInUseCheck precondition, checked against a flow alias's
// routingConfiguration.
func TestDeleteFlowVersion_BlockedWhileAliasReferencesIt(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	cases := []struct {
		name    string
		skip    bool
		wantErr bool
	}{
		{name: "blocked by default", skip: false, wantErr: true},
		{name: "skip flag deletes anyway", skip: true, wantErr: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := bedrockagent.NewTestBackend("us-east-1", "123456789012")

			flow, err := b.CreateFlow(ctx, bedrockagent.FlowConfig{
				Name:    "in-use-flow",
				RoleARN: "arn:aws:iam::123456789012:role/FlowRole",
			})
			require.NoError(t, err)

			fv, err := b.CreateFlowVersion(ctx, flow.FlowID, "")
			require.NoError(t, err)

			_, err = b.CreateFlowAlias(ctx, flow.FlowID, bedrockagent.FlowAliasConfig{
				Name:                 "in-use-alias",
				RoutingConfiguration: []bedrockagent.FlowAliasRouting{{FlowVersion: fv.Version}},
			})
			require.NoError(t, err)

			err = b.DeleteFlowVersion(ctx, flow.FlowID, fv.Version, tc.skip)

			if !tc.wantErr {
				require.NoError(t, err)
				_, getErr := b.GetFlowVersion(ctx, flow.FlowID, fv.Version)
				assert.ErrorIs(t, getErr, bedrockagent.ErrNotFound)

				return
			}

			require.Error(t, err)
			require.ErrorIs(t, err, bedrockagent.ErrResourceInUse)

			_, getErr := b.GetFlowVersion(ctx, flow.FlowID, fv.Version)
			assert.NoError(t, getErr, "version must survive a blocked delete")
		})
	}
}

// TestHandleDeleteAgentVersion_ConflictOverHTTP confirms the HTTP layer
// parses the skipResourceInUseCheck query parameter (api_op_DeleteAgentVersion
// serializes it as a query bool, serializers.go:~1657) and surfaces a real
// 409 ConflictException rather than a 500 -- handleErr must map
// awserr.ErrConflict, which was previously unhandled and would have fallen
// through to InternalServerException.
func TestHandleDeleteAgentVersion_ConflictOverHTTP(t *testing.T) {
	t.Parallel()

	h, e := setupHandler(t)

	createRec := doRequest(t, h, e, http.MethodPut, "/agents", map[string]any{
		"agentName":            "http-in-use-agent",
		"foundationModel":      "anthropic.claude-v2",
		"agentResourceRoleArn": "arn:aws:iam::123456789012:role/BedrockRole",
	})
	require.Equal(t, http.StatusOK, createRec.Code, createRec.Body.String())

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
	agentWrap, _ := created["agent"].(map[string]any)
	agentID, _ := agentWrap["agentId"].(string)
	require.NotEmpty(t, agentID)

	aliasRec := doRequest(t, h, e, http.MethodPut, "/agents/"+agentID+"/agentaliases", map[string]any{
		"agentAliasName": "http-in-use-alias",
	})
	require.Equal(t, http.StatusOK, aliasRec.Code, aliasRec.Body.String())

	var aliasBody map[string]any
	require.NoError(t, json.Unmarshal(aliasRec.Body.Bytes(), &aliasBody))
	aliasWrap, _ := aliasBody["agentAlias"].(map[string]any)
	routing, _ := aliasWrap["routingConfiguration"].([]any)
	require.NotEmpty(t, routing)
	firstRoute, _ := routing[0].(map[string]any)
	version, _ := firstRoute["agentVersion"].(string)
	require.NotEmpty(t, version)

	blockedRec := doRequest(
		t, h, e, http.MethodDelete, "/agents/"+agentID+"/agentversions/"+version, nil,
	)
	require.Equal(t, http.StatusConflict, blockedRec.Code, blockedRec.Body.String())
	assert.Equal(t, "ConflictException", blockedRec.Header().Get("X-Amzn-Errortype"))

	skipRec := doRequest(
		t, h, e, http.MethodDelete,
		"/agents/"+agentID+"/agentversions/"+version+"?skipResourceInUseCheck=true", nil,
	)
	require.Equal(t, http.StatusOK, skipRec.Code, skipRec.Body.String())
}

// TestDeleteAgent_BlockedWhileAliasExists guards api_op_DeleteAgent.go's
// documented precondition (same SkipResourceInUseCheck contract as
// DeleteAgentVersion above): an agent with any alias is "in use" because the
// alias always routes to a numbered snapshot of this agent
// (CreateAgentAlias), so deleting the agent out from under it would strand
// that reference.
func TestDeleteAgent_BlockedWhileAliasExists(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	cases := []struct {
		name    string
		skip    bool
		wantErr bool
	}{
		{name: "blocked by default", skip: false, wantErr: true},
		{name: "skip flag deletes anyway", skip: true, wantErr: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := bedrockagent.NewTestBackend("us-east-1", "123456789012")

			agent, err := b.CreateAgent(ctx, bedrockagent.AgentConfig{
				AgentName:       "in-use-agent",
				FoundationModel: "anthropic.claude-v2",
				RoleARN:         "arn:aws:iam::123456789012:role/BedrockRole",
			})
			require.NoError(t, err)

			_, err = b.CreateAgentAlias(ctx, agent.AgentID, bedrockagent.AliasConfig{
				AliasName: "in-use-alias",
			})
			require.NoError(t, err)

			err = b.DeleteAgent(ctx, agent.AgentID, tc.skip)

			if !tc.wantErr {
				require.NoError(t, err)
				_, getErr := b.GetAgent(ctx, agent.AgentID)
				assert.ErrorIs(t, getErr, bedrockagent.ErrNotFound)

				return
			}

			require.Error(t, err)
			require.ErrorIs(t, err, bedrockagent.ErrResourceInUse)

			_, getErr := b.GetAgent(ctx, agent.AgentID)
			assert.NoError(t, getErr, "agent must survive a blocked delete")
		})
	}
}

// TestDeleteFlow_BlockedWhileAliasExists mirrors
// TestDeleteAgent_BlockedWhileAliasExists for api_op_DeleteFlow.go's
// identical skipResourceInUseCheck precondition.
func TestDeleteFlow_BlockedWhileAliasExists(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	cases := []struct {
		name    string
		skip    bool
		wantErr bool
	}{
		{name: "blocked by default", skip: false, wantErr: true},
		{name: "skip flag deletes anyway", skip: true, wantErr: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := bedrockagent.NewTestBackend("us-east-1", "123456789012")

			flow, err := b.CreateFlow(ctx, bedrockagent.FlowConfig{
				Name:    "in-use-flow",
				RoleARN: "arn:aws:iam::123456789012:role/FlowRole",
			})
			require.NoError(t, err)

			fv, err := b.CreateFlowVersion(ctx, flow.FlowID, "")
			require.NoError(t, err)

			_, err = b.CreateFlowAlias(ctx, flow.FlowID, bedrockagent.FlowAliasConfig{
				Name:                 "in-use-alias",
				RoutingConfiguration: []bedrockagent.FlowAliasRouting{{FlowVersion: fv.Version}},
			})
			require.NoError(t, err)

			err = b.DeleteFlow(ctx, flow.FlowID, tc.skip)

			if !tc.wantErr {
				require.NoError(t, err)
				_, getErr := b.GetFlow(ctx, flow.FlowID)
				assert.ErrorIs(t, getErr, bedrockagent.ErrNotFound)

				return
			}

			require.Error(t, err)
			require.ErrorIs(t, err, bedrockagent.ErrResourceInUse)

			_, getErr := b.GetFlow(ctx, flow.FlowID)
			assert.NoError(t, getErr, "flow must survive a blocked delete")
		})
	}
}

// TestDeleteAgent_ConflictIsTypedOverSDKClient drives DeleteAgent through the
// real aws-sdk-go-v2 client and proves the "in use" rejection decodes as the
// SDK's own typed types.ConflictException, not a generic smithy error --
// the observable effect a real caller's error-handling code actually
// branches on.
func TestDeleteAgent_ConflictIsTypedOverSDKClient(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	created, err := client.CreateAgent(ctx, &bedrockagentsdk.CreateAgentInput{
		AgentName:            aws.String("typed-conflict-agent"),
		FoundationModel:      aws.String("anthropic.claude-v2"),
		AgentResourceRoleArn: aws.String("arn:aws:iam::123456789012:role/BedrockRole"),
	})
	require.NoError(t, err)
	agentID := aws.ToString(created.Agent.AgentId)

	_, err = client.CreateAgentAlias(ctx, &bedrockagentsdk.CreateAgentAliasInput{
		AgentId:        aws.String(agentID),
		AgentAliasName: aws.String("typed-conflict-alias"),
	})
	require.NoError(t, err)

	_, err = client.DeleteAgent(ctx, &bedrockagentsdk.DeleteAgentInput{AgentId: aws.String(agentID)})
	require.Error(t, err)

	var conflict *types.ConflictException
	require.ErrorAs(t, err, &conflict, "want a typed ConflictException, got %T: %v", err, err)

	_, err = client.DeleteAgent(ctx, &bedrockagentsdk.DeleteAgentInput{
		AgentId:                aws.String(agentID),
		SkipResourceInUseCheck: true,
	})
	require.NoError(t, err)
}

// TestDeleteFlow_ConflictIsTypedOverSDKClient mirrors
// TestDeleteAgent_ConflictIsTypedOverSDKClient for DeleteFlow.
func TestDeleteFlow_ConflictIsTypedOverSDKClient(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	created, err := client.CreateFlow(ctx, &bedrockagentsdk.CreateFlowInput{
		Name:             aws.String("typed-conflict-flow"),
		ExecutionRoleArn: aws.String("arn:aws:iam::123456789012:role/FlowRole"),
	})
	require.NoError(t, err)
	flowID := aws.ToString(created.Id)

	fv, err := client.CreateFlowVersion(
		ctx, &bedrockagentsdk.CreateFlowVersionInput{FlowIdentifier: aws.String(flowID)},
	)
	require.NoError(t, err)

	_, err = client.CreateFlowAlias(ctx, &bedrockagentsdk.CreateFlowAliasInput{
		FlowIdentifier: aws.String(flowID),
		Name:           aws.String("typed-conflict-alias"),
		RoutingConfiguration: []types.FlowAliasRoutingConfigurationListItem{
			{FlowVersion: fv.Version},
		},
	})
	require.NoError(t, err)

	_, err = client.DeleteFlow(ctx, &bedrockagentsdk.DeleteFlowInput{FlowIdentifier: aws.String(flowID)})
	require.Error(t, err)

	var conflict *types.ConflictException
	require.ErrorAs(t, err, &conflict, "want a typed ConflictException, got %T: %v", err, err)

	_, err = client.DeleteFlow(ctx, &bedrockagentsdk.DeleteFlowInput{
		FlowIdentifier:         aws.String(flowID),
		SkipResourceInUseCheck: true,
	})
	require.NoError(t, err)
}
