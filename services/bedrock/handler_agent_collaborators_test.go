package bedrock_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/bedrock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAgentCollaboratorCRUD(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)

	// Create agent
	rec := doAgentRequest(t, h, http.MethodPost, "/agents", map[string]any{
		"agentName":            "collab-agent",
		"foundationModel":      "amazon.titan-text-express-v1",
		"agentResourceRoleArn": "arn:aws:iam::000000000000:role/role",
	})
	require.Equal(t, http.StatusAccepted, rec.Code)

	var ab map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &ab))
	agentID := ab["agent"].(map[string]any)["agentId"].(string)

	collabPath := fmt.Sprintf(
		"/agents/%s/agentversions/DRAFT/agentcollaborators", agentID,
	)

	// Associate collaborator
	rec = doAgentRequest(t, h, http.MethodPost, collabPath, map[string]any{
		"agentVersion":             "DRAFT",
		"collaboratorArn":          "arn:aws:bedrock:us-east-1:000000000000:agent/other",
		"relayConversationHistory": "TO_COLLABORATOR",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var cb map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cb))
	collabID := cb["agentCollaborator"].(map[string]any)["collaboratorId"].(string)

	// Get collaborator
	rec = doAgentRequest(t, h, http.MethodGet,
		fmt.Sprintf("%s/%s", collabPath, collabID), nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// List collaborators
	rec = doAgentRequest(t, h, http.MethodGet, collabPath, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var lb map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &lb))
	assert.Len(t, lb["agentCollaboratorSummaries"], 1)

	// Update collaborator
	rec = doAgentRequest(
		t, h, http.MethodPut,
		fmt.Sprintf("%s/%s", collabPath, collabID),
		map[string]any{"relayConversationHistory": "DISABLED"},
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Disassociate collaborator
	rec = doAgentRequest(t, h, http.MethodDelete,
		fmt.Sprintf("%s/%s", collabPath, collabID), nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

func TestAccuracy_AgentCollaborator_RelayConversationHistoryPreserved(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		relayHistory string
	}{
		{name: "relay enabled", relayHistory: "TO_COLLABORATOR"},
		{name: "relay disabled", relayHistory: "DISABLED"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestAgentsHandler(t)
			supervisor, err := b.CreateAgent("supervisor-"+tt.name, "model", "", "", nil)
			require.NoError(t, err)

			rec := doAgentRequest(
				t, h, http.MethodPost,
				fmt.Sprintf("/agents/%s/agentversions/DRAFT/agentcollaborators", supervisor.AgentID),
				map[string]any{
					"collaboratorArn":          "arn:aws:bedrock:us-east-1:000000000000:agent/collab-agent",
					"agentVersion":             "DRAFT",
					"relayConversationHistory": tt.relayHistory,
				},
			)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			var body map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			collab := body["agentCollaborator"].(map[string]any)
			collabID := collab["collaboratorId"].(string)
			assert.NotEmpty(t, collabID)
			assert.Equal(t, tt.relayHistory, collab["relayConversationHistory"])

			// Verify GET preserves the relay setting
			getRec := doAgentRequest(t, h, http.MethodGet,
				fmt.Sprintf("/agents/%s/agentversions/DRAFT/agentcollaborators/%s", supervisor.AgentID, collabID), nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			var getBody map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getBody))
			gotCollab := getBody["agentCollaborator"].(map[string]any)
			assert.Equal(t, tt.relayHistory, gotCollab["relayConversationHistory"])
		})
	}
}

func TestAccuracy_AgentCollaborator_UpdateRelayHistory(t *testing.T) {
	t.Parallel()

	h, b := newTestAgentsHandler(t)
	supervisor, err := b.CreateAgent("relay-update-agent", "model", "", "", nil)
	require.NoError(t, err)

	// Associate with relay disabled
	assocRec := doAgentRequest(
		t, h, http.MethodPost,
		fmt.Sprintf("/agents/%s/agentversions/DRAFT/agentcollaborators", supervisor.AgentID),
		map[string]any{
			"collaboratorArn":          "arn:aws:bedrock:us-east-1:000000000000:agent/subagent",
			"agentVersion":             "DRAFT",
			"relayConversationHistory": "DISABLED",
		},
	)
	require.Equal(t, http.StatusOK, assocRec.Code)

	var assocBody map[string]any
	require.NoError(t, json.Unmarshal(assocRec.Body.Bytes(), &assocBody))
	collabID := assocBody["agentCollaborator"].(map[string]any)["collaboratorId"].(string)

	// Update relay to enabled
	updateRec := doAgentRequest(
		t, h, http.MethodPut,
		fmt.Sprintf("/agents/%s/agentversions/DRAFT/agentcollaborators/%s", supervisor.AgentID, collabID),
		map[string]any{"relayConversationHistory": "TO_COLLABORATOR"},
	)
	require.Equal(t, http.StatusOK, updateRec.Code)

	var updateBody map[string]any
	require.NoError(t, json.Unmarshal(updateRec.Body.Bytes(), &updateBody))
	updatedCollab := updateBody["agentCollaborator"].(map[string]any)
	assert.Equal(t, "TO_COLLABORATOR", updatedCollab["relayConversationHistory"])
}

func TestAccuracy_AgentCollaborator_SupervisorPattern(t *testing.T) {
	t.Parallel()

	h, b := newTestAgentsHandler(t)

	// Create supervisor agent with SUPERVISOR collaboration
	supervisor, err := b.CreateAgentWithConfiguration(bedrock.AgentConfiguration{
		AgentName:          "supervisor",
		FoundationModel:    "model",
		AgentCollaboration: "SUPERVISOR",
	})
	require.NoError(t, err)
	assert.Equal(t, "SUPERVISOR", supervisor.AgentCollaboration)

	// Associate two subagents
	for i := range 2 {
		rec := doAgentRequest(
			t, h, http.MethodPost,
			fmt.Sprintf("/agents/%s/agentversions/DRAFT/agentcollaborators", supervisor.AgentID),
			map[string]any{
				"collaboratorArn": fmt.Sprintf("arn:aws:bedrock:us-east-1:000000000000:agent/subagent-%d", i),
				"agentVersion":    "DRAFT",
			},
		)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// List should show 2 collaborators
	listRec := doAgentRequest(t, h, http.MethodGet,
		fmt.Sprintf("/agents/%s/agentversions/DRAFT/agentcollaborators", supervisor.AgentID), nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listBody map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listBody))
	assert.Len(t, listBody["agentCollaboratorSummaries"], 2)
}

func TestAccuracy_AgentCollaborator_DisassociateRemovesFromList(t *testing.T) {
	t.Parallel()

	h, b := newTestAgentsHandler(t)
	agent, err := b.CreateAgent("disassoc-agent", "model", "", "", nil)
	require.NoError(t, err)

	// Associate
	rec := doAgentRequest(
		t, h, http.MethodPost,
		fmt.Sprintf("/agents/%s/agentversions/DRAFT/agentcollaborators", agent.AgentID),
		map[string]any{
			"collaboratorArn": "arn:aws:bedrock:us-east-1:000000000000:agent/temp-collab",
			"agentVersion":    "DRAFT",
		},
	)
	require.Equal(t, http.StatusOK, rec.Code)

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	collabID := body["agentCollaborator"].(map[string]any)["collaboratorId"].(string)

	// Disassociate
	deleteRec := doAgentRequest(t, h, http.MethodDelete,
		fmt.Sprintf("/agents/%s/agentversions/DRAFT/agentcollaborators/%s", agent.AgentID, collabID), nil)
	require.Equal(t, http.StatusNoContent, deleteRec.Code)

	// List should be empty
	listRec := doAgentRequest(t, h, http.MethodGet,
		fmt.Sprintf("/agents/%s/agentversions/DRAFT/agentcollaborators", agent.AgentID), nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listBody map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listBody))
	assert.Empty(t, listBody["agentCollaboratorSummaries"])
}
