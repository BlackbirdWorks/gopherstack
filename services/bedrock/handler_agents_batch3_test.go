package bedrock_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrock"
)

// ---------------------------------------------------------------------------
// Helper: create a KB + data source in one call
// ---------------------------------------------------------------------------

func createKBAndDS(t *testing.T, h *bedrock.AgentsHandler) (string, string) {
	t.Helper()

	kbResp := doAgentRequest(t, h, http.MethodPost, "/knowledgebases", map[string]any{
		"name":    "test-kb",
		"roleArn": "arn:aws:iam::000000000000:role/kb-role",
	})
	require.Equal(t, http.StatusAccepted, kbResp.Code)

	var kbBody map[string]any
	require.NoError(t, json.Unmarshal(kbResp.Body.Bytes(), &kbBody))
	kbID := kbBody["knowledgeBase"].(map[string]any)["knowledgeBaseId"].(string)

	dsResp := doAgentRequest(t, h, http.MethodPost,
		fmt.Sprintf("/knowledgebases/%s/datasources", kbID),
		map[string]any{"name": "test-ds"},
	)
	require.Equal(t, http.StatusOK, dsResp.Code)

	var dsBody map[string]any
	require.NoError(t, json.Unmarshal(dsResp.Body.Bytes(), &dsBody))
	dsID := dsBody["dataSource"].(map[string]any)["dataSourceId"].(string)

	return kbID, dsID
}

// ---------------------------------------------------------------------------
// Flow CRUD
// ---------------------------------------------------------------------------

func TestFlowCRUD(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)

	// Create
	rec := doAgentRequest(t, h, http.MethodPost, "/flows", map[string]any{
		"name":        "my-flow",
		"description": "test flow",
	})
	assert.Equal(t, http.StatusCreated, rec.Code)

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	flow := body["flow"].(map[string]any)
	flowID := flow["flowId"].(string)
	assert.NotEmpty(t, flowID)
	assert.Equal(t, "my-flow", flow["name"])
	assert.Equal(t, "NOT_PREPARED", flow["status"])

	// Get
	rec = doAgentRequest(t, h, http.MethodGet, "/flows/"+flowID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// List
	rec = doAgentRequest(t, h, http.MethodGet, "/flows", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var listBody map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listBody))
	assert.Len(t, listBody["flowSummaries"], 1)

	// Update
	rec = doAgentRequest(t, h, http.MethodPut, "/flows/"+flowID, map[string]any{
		"description": "updated",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Prepare
	rec = doAgentRequest(t, h, http.MethodPost, "/flows/"+flowID+"/prepare", nil)
	assert.Equal(t, http.StatusAccepted, rec.Code)

	// Delete
	rec = doAgentRequest(t, h, http.MethodDelete, "/flows/"+flowID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Get after delete → 404
	rec = doAgentRequest(t, h, http.MethodGet, "/flows/"+flowID, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestCreateFlow_ConflictOnDuplicate(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)

	doAgentRequest(t, h, http.MethodPost, "/flows", map[string]any{"name": "dup"})
	rec := doAgentRequest(t, h, http.MethodPost, "/flows", map[string]any{"name": "dup"})
	assert.Equal(t, http.StatusConflict, rec.Code)
}

// ---------------------------------------------------------------------------
// Flow Aliases
// ---------------------------------------------------------------------------

func TestFlowAliasCRUD(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)

	// Create flow
	rec := doAgentRequest(t, h, http.MethodPost, "/flows", map[string]any{"name": "fa-flow"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var fb map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &fb))
	flowID := fb["flow"].(map[string]any)["flowId"].(string)

	// Create alias
	rec = doAgentRequest(t, h, http.MethodPost,
		fmt.Sprintf("/flows/%s/aliases", flowID),
		map[string]any{"name": "my-alias"},
	)
	assert.Equal(t, http.StatusCreated, rec.Code)

	var ab map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &ab))
	aliasID := ab["flowAlias"].(map[string]any)["flowAliasId"].(string)

	// Get alias
	rec = doAgentRequest(t, h, http.MethodGet,
		fmt.Sprintf("/flows/%s/aliases/%s", flowID, aliasID), nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// List aliases
	rec = doAgentRequest(t, h, http.MethodGet,
		fmt.Sprintf("/flows/%s/aliases", flowID), nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var lb map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &lb))
	assert.Len(t, lb["flowAliasSummaries"], 1)

	// Update alias
	rec = doAgentRequest(t, h, http.MethodPut,
		fmt.Sprintf("/flows/%s/aliases/%s", flowID, aliasID),
		map[string]any{"name": "updated-alias"},
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Delete alias
	rec = doAgentRequest(t, h, http.MethodDelete,
		fmt.Sprintf("/flows/%s/aliases/%s", flowID, aliasID), nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---------------------------------------------------------------------------
// Flow Versions
// ---------------------------------------------------------------------------

func TestFlowVersionCRUD(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)

	// Create flow
	rec := doAgentRequest(t, h, http.MethodPost, "/flows", map[string]any{"name": "fv-flow"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var fb map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &fb))
	flowID := fb["flow"].(map[string]any)["flowId"].(string)

	// Create version
	rec = doAgentRequest(t, h, http.MethodPost,
		fmt.Sprintf("/flows/%s/versions", flowID), nil)
	assert.Equal(t, http.StatusCreated, rec.Code)

	var vb map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &vb))
	version := vb["flowVersion"].(map[string]any)["version"].(string)
	assert.Equal(t, "1", version)

	// Get version
	rec = doAgentRequest(t, h, http.MethodGet,
		fmt.Sprintf("/flows/%s/versions/%s", flowID, version), nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// List versions
	rec = doAgentRequest(t, h, http.MethodGet,
		fmt.Sprintf("/flows/%s/versions", flowID), nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var lb map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &lb))
	assert.Len(t, lb["flowVersionSummaries"], 1)

	// Delete version
	rec = doAgentRequest(t, h, http.MethodDelete,
		fmt.Sprintf("/flows/%s/versions/%s", flowID, version), nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---------------------------------------------------------------------------
// ValidateFlowDefinition
// ---------------------------------------------------------------------------

func TestValidateFlowDefinition(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)

	rec := doAgentRequest(t, h, http.MethodPost, "/flows/validateFlowDefinition",
		map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.NotNil(t, body["validations"])
}

// ---------------------------------------------------------------------------
// Prompt CRUD
// ---------------------------------------------------------------------------

func TestPromptCRUD(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)

	// Create
	rec := doAgentRequest(t, h, http.MethodPost, "/prompts", map[string]any{
		"name": "my-prompt",
	})
	assert.Equal(t, http.StatusCreated, rec.Code)

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	promptID := body["prompt"].(map[string]any)["promptId"].(string)
	assert.NotEmpty(t, promptID)

	// Get
	rec = doAgentRequest(t, h, http.MethodGet, "/prompts/"+promptID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// List
	rec = doAgentRequest(t, h, http.MethodGet, "/prompts", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var lb map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &lb))
	assert.Len(t, lb["promptSummaries"], 1)

	// Update
	rec = doAgentRequest(t, h, http.MethodPut, "/prompts/"+promptID, map[string]any{
		"description": "updated",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Delete
	rec = doAgentRequest(t, h, http.MethodDelete, "/prompts/"+promptID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Get after delete → 404
	rec = doAgentRequest(t, h, http.MethodGet, "/prompts/"+promptID, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestCreatePrompt_ConflictOnDuplicate(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)

	doAgentRequest(t, h, http.MethodPost, "/prompts", map[string]any{"name": "dup"})
	rec := doAgentRequest(t, h, http.MethodPost, "/prompts", map[string]any{"name": "dup"})
	assert.Equal(t, http.StatusConflict, rec.Code)
}

// ---------------------------------------------------------------------------
// Prompt Versions
// ---------------------------------------------------------------------------

func TestPromptVersionCRUD(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)

	// Create prompt
	rec := doAgentRequest(t, h, http.MethodPost, "/prompts", map[string]any{"name": "pv-prompt"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var pb map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &pb))
	promptID := pb["prompt"].(map[string]any)["promptId"].(string)

	// Create version
	rec = doAgentRequest(t, h, http.MethodPost,
		fmt.Sprintf("/prompts/%s/versions", promptID), nil)
	assert.Equal(t, http.StatusCreated, rec.Code)

	var vb map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &vb))
	version := vb["promptVersion"].(map[string]any)["version"].(string)
	assert.Equal(t, "1", version)

	// Get version
	rec = doAgentRequest(t, h, http.MethodGet,
		fmt.Sprintf("/prompts/%s/versions/%s", promptID, version), nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// List versions
	rec = doAgentRequest(t, h, http.MethodGet,
		fmt.Sprintf("/prompts/%s/versions", promptID), nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var lb map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &lb))
	assert.Len(t, lb["promptVersionSummaries"], 1)

	// Delete version
	rec = doAgentRequest(t, h, http.MethodDelete,
		fmt.Sprintf("/prompts/%s/versions/%s", promptID, version), nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---------------------------------------------------------------------------
// Agent Versions
// ---------------------------------------------------------------------------

func TestAgentVersionCRUD(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)

	// Create agent
	rec := doAgentRequest(t, h, http.MethodPost, "/agents", map[string]any{
		"agentName":            "av-agent",
		"foundationModel":      "amazon.titan-text-express-v1",
		"agentResourceRoleArn": "arn:aws:iam::000000000000:role/role",
	})
	require.Equal(t, http.StatusAccepted, rec.Code)

	var ab map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &ab))
	agentID := ab["agent"].(map[string]any)["agentId"].(string)

	// Create version
	rec = doAgentRequest(t, h, http.MethodPost,
		fmt.Sprintf("/agents/%s/versions", agentID), nil)
	assert.Equal(t, http.StatusAccepted, rec.Code)

	var vb map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &vb))
	version := vb["agentVersion"].(map[string]any)["agentVersion"].(string)
	assert.Equal(t, "1", version)

	// Get version
	rec = doAgentRequest(t, h, http.MethodGet,
		fmt.Sprintf("/agents/%s/versions/%s", agentID, version), nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// List versions
	rec = doAgentRequest(t, h, http.MethodGet,
		fmt.Sprintf("/agents/%s/versions", agentID), nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var lb map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &lb))
	assert.Len(t, lb["agentVersionSummaries"], 1)

	// Delete version
	rec = doAgentRequest(t, h, http.MethodDelete,
		fmt.Sprintf("/agents/%s/versions/%s", agentID, version), nil)
	assert.Equal(t, http.StatusAccepted, rec.Code)
}

// ---------------------------------------------------------------------------
// Agent Collaborators
// ---------------------------------------------------------------------------

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
		"/agents/%s/agentversions/DRAFT/agentcollaborators", agentID)

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
	rec = doAgentRequest(t, h, http.MethodPut,
		fmt.Sprintf("%s/%s", collabPath, collabID),
		map[string]any{"relayConversationHistory": "DISABLED"},
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Disassociate collaborator
	rec = doAgentRequest(t, h, http.MethodDelete,
		fmt.Sprintf("%s/%s", collabPath, collabID), nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

// ---------------------------------------------------------------------------
// KB Documents
// ---------------------------------------------------------------------------

func TestKBDocumentsCRUD(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)

	kbID, dsID := createKBAndDS(t, h)
	docPath := fmt.Sprintf("/knowledgebases/%s/datasources/%s/documents", kbID, dsID)

	// Ingest documents
	rec := doAgentRequest(t, h, http.MethodPost, docPath, map[string]any{
		"documentIds": []string{"doc-1", "doc-2"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var ib map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &ib))
	assert.Len(t, ib["documents"], 2)

	// List documents
	rec = doAgentRequest(t, h, http.MethodGet, docPath, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var lb map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &lb))
	assert.Len(t, lb["documentDetails"], 2)

	// Update documents
	rec = doAgentRequest(t, h, http.MethodPut, docPath, map[string]any{
		"documentIds": []string{"doc-1", "doc-3"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Delete documents
	rec = doAgentRequest(t, h, http.MethodDelete, docPath, map[string]any{
		"documentIds": []string{"doc-1"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---------------------------------------------------------------------------
// Stop Ingestion Job
// ---------------------------------------------------------------------------

func TestStopIngestionJob(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)

	kbID, dsID := createKBAndDS(t, h)

	// Start ingestion job
	startRec := doAgentRequest(t, h, http.MethodPost,
		fmt.Sprintf("/knowledgebases/%s/datasources/%s/ingestionjobs", kbID, dsID),
		map[string]any{},
	)
	require.Equal(t, http.StatusAccepted, startRec.Code)

	var jb map[string]any
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &jb))
	jobID := jb["ingestionJob"].(map[string]any)["ingestionJobId"].(string)

	// Stop it
	rec := doAgentRequest(t, h, http.MethodPost,
		fmt.Sprintf("/knowledgebases/%s/datasources/%s/ingestionjobs/%s/stop",
			kbID, dsID, jobID),
		nil,
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	var sb map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &sb))
	assert.Equal(t, "STOPPED", sb["ingestionJob"].(map[string]any)["status"])
}

// ---------------------------------------------------------------------------
// Resource Tags (agent-domain)
// ---------------------------------------------------------------------------

func TestAgentResourceTags(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)

	resourceArn := "arn:aws:bedrock-agent:us-east-1:000000000000:flow/flow-00000001"

	// Tag
	rec := doAgentRequest(t, h, http.MethodPost, "/tags/"+resourceArn, map[string]any{
		"tags": map[string]string{"env": "prod", "team": "core"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// List tags
	rec = doAgentRequest(t, h, http.MethodGet, "/tags/"+resourceArn, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var lb map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &lb))
	tags := lb["tags"].(map[string]any)
	assert.Equal(t, "prod", tags["env"])
	assert.Equal(t, "core", tags["team"])

	// Untag
	rec = doAgentRequest(t, h, http.MethodDelete, "/tags/"+resourceArn, map[string]any{
		"tagKeys": []string{"team"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// List tags again — team should be gone
	rec = doAgentRequest(t, h, http.MethodGet, "/tags/"+resourceArn, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var lb2 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &lb2))
	tags2 := lb2["tags"].(map[string]any)
	assert.Equal(t, "prod", tags2["env"])
	_, hasTeam := tags2["team"]
	assert.False(t, hasTeam)
}

// ---------------------------------------------------------------------------
// Agent Memory
// ---------------------------------------------------------------------------

func TestAgentMemory(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)

	// Create agent
	rec := doAgentRequest(t, h, http.MethodPost, "/agents", map[string]any{
		"agentName":            "mem-agent",
		"foundationModel":      "amazon.titan-text-express-v1",
		"agentResourceRoleArn": "arn:aws:iam::000000000000:role/role",
	})
	require.Equal(t, http.StatusAccepted, rec.Code)

	var ab map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &ab))
	agentID := ab["agent"].(map[string]any)["agentId"].(string)

	memPath := fmt.Sprintf("/agents/%s/agentversions/DRAFT/memories", agentID)

	// Get memory (empty)
	rec = doAgentRequest(t, h, http.MethodGet, memPath+"?sessionId=s1", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Delete memory
	rec = doAgentRequest(t, h, http.MethodDelete, memPath+"?sessionId=s1", nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

// ---------------------------------------------------------------------------
// GetSupportedOperations count
// ---------------------------------------------------------------------------

func TestAgentsHandler_SupportedOperationsCount(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)
	ops := h.GetSupportedOperations()
	// Should be at least 75 operations (33 original + 43 new)
	assert.GreaterOrEqual(t, len(ops), 75)
}
