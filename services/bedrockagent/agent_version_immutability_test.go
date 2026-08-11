package bedrockagent_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrockagent"
)

// TestMutateSubResourceRejectsNonDraftVersion locks in real AWS's DRAFT-only
// {agentVersion} URI path constraint on every action-group/collaborator/
// KB-association mutation op, not just their Create/Associate counterparts
// (already covered by TestCreateAgentActionGroupVersion et al.). Confirmed
// via the AWS API reference: UpdateAgentActionGroup, DeleteAgentActionGroup,
// UpdateAgentCollaborator, DisassociateAgentCollaborator,
// UpdateAgentKnowledgeBase, and DisassociateAgentKnowledgeBase all document
// agentVersion as Pattern: `DRAFT`, Length Constraints: Fixed length of 5 --
// same as their Create/Associate counterparts. Numbered versions now carry
// real snapshot rows (gopherstack-rvyd), so without this check a client
// could mutate or delete an "immutable" version snapshot directly.
func TestMutateSubResourceRejectsNonDraftVersion(t *testing.T) {
	t.Parallel()

	h, e := setupHandler(t)
	ctx := context.Background()
	backend := h.Backend

	agent, err := backend.CreateAgent(ctx, bedrockagent.AgentConfig{
		AgentName:       "immutable-mutate-agent",
		FoundationModel: "anthropic.claude-v2",
		RoleARN:         "arn:aws:iam::123456789012:role/BedrockRole",
	})
	require.NoError(t, err)

	ag, err := backend.CreateAgentActionGroup(ctx, agent.AgentID, "DRAFT", bedrockagent.ActionGroupConfig{
		ActionGroupName: "immutable-ag",
	})
	require.NoError(t, err)

	collab, err := backend.AssociateAgentCollaborator(ctx, agent.AgentID, "DRAFT", bedrockagent.CollaboratorConfig{
		CollaboratorName:         "immutable-collab",
		CollaborationInstruction: "help",
		AgentDescriptor: map[string]any{
			"aliasArn": "arn:aws:bedrock:us-east-1:123456789012:agent-alias/OTHER12345/ALIASOTHER",
		},
	})
	require.NoError(t, err)

	kb, err := backend.CreateKnowledgeBase(ctx, bedrockagent.KnowledgeBaseConfig{
		Name:    "immutable-kb",
		RoleARN: "arn:aws:iam::123456789012:role/KBRole",
	})
	require.NoError(t, err)

	_, err = backend.AssociateAgentKnowledgeBase(ctx, agent.AgentID, "DRAFT", kb.KnowledgeBaseID, "desc", "ENABLED")
	require.NoError(t, err)

	tests := []struct {
		body   any
		name   string
		method string
		path   string
	}{
		{
			name:   "update action group",
			method: http.MethodPut,
			path:   "/agents/" + agent.AgentID + "/agentversions/1/actiongroups/" + ag.ActionGroupID,
			body:   map[string]any{"actionGroupName": "renamed"},
		},
		{
			name:   "delete action group",
			method: http.MethodDelete,
			path:   "/agents/" + agent.AgentID + "/agentversions/1/actiongroups/" + ag.ActionGroupID,
		},
		{
			name:   "update collaborator",
			method: http.MethodPut,
			path:   "/agents/" + agent.AgentID + "/agentversions/1/agentcollaborators/" + collab.CollaboratorID,
			body:   map[string]any{"collaboratorName": "renamed"},
		},
		{
			name:   "disassociate collaborator",
			method: http.MethodDelete,
			path:   "/agents/" + agent.AgentID + "/agentversions/1/agentcollaborators/" + collab.CollaboratorID,
		},
		{
			name:   "update knowledge base",
			method: http.MethodPut,
			path:   "/agents/" + agent.AgentID + "/agentversions/1/knowledgebases/" + kb.KnowledgeBaseID,
			body:   map[string]any{"description": "renamed"},
		},
		{
			name:   "disassociate knowledge base",
			method: http.MethodDelete,
			path:   "/agents/" + agent.AgentID + "/agentversions/1/knowledgebases/" + kb.KnowledgeBaseID,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, e, tt.method, tt.path, tt.body)

			assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
			assert.Equal(t, "ValidationException", rec.Header().Get("X-Amzn-Errortype"))
		})
	}
}
