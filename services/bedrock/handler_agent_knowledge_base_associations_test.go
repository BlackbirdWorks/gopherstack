package bedrock_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAgentsHandler_AgentKBAssociationLifecycle(t *testing.T) {
	t.Parallel()

	h, b := newTestAgentsHandler(t)
	ag, err := b.CreateAgent("kb-assoc-agent", "", "", "", nil)
	require.NoError(t, err)
	kb, err := b.CreateKnowledgeBase("kb-assoc-kb", "", "", nil, nil, nil)
	require.NoError(t, err)

	agentID := ag.AgentID
	kbID := kb.KnowledgeBaseID

	// Associate KB with agent.
	rec := doAgentRequest(
		t, h, http.MethodPut,
		fmt.Sprintf("/agents/%s/agentversions/DRAFT/knowledgebases", agentID),
		map[string]any{
			"knowledgeBaseId": kbID,
			"description":     "test association",
		},
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	var assocOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &assocOut))
	assert.NotNil(t, assocOut["agentKnowledgeBase"])

	// List KB associations.
	rec2 := doAgentRequest(
		t, h, http.MethodGet,
		fmt.Sprintf("/agents/%s/agentversions/DRAFT/knowledgebases", agentID), nil,
	)
	assert.Equal(t, http.StatusOK, rec2.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &listOut))
	assert.Len(t, listOut["agentKnowledgeBaseSummaries"].([]any), 1)

	// Get specific KB association.
	rec3 := doAgentRequest(
		t, h, http.MethodGet,
		fmt.Sprintf("/agents/%s/agentversions/DRAFT/knowledgebases/%s", agentID, kbID), nil,
	)
	assert.Equal(t, http.StatusOK, rec3.Code)

	// Disassociate KB (204 No Content — echo may return body write error for 204, ignore it).
	e := echo.New()
	reqDel := httptest.NewRequest(
		http.MethodDelete,
		fmt.Sprintf("/agents/%s/agentversions/DRAFT/knowledgebases/%s", agentID, kbID),
		http.NoBody,
	)
	recDel := httptest.NewRecorder()
	cDel := e.NewContext(reqDel, recDel)
	_ = h.Handler()(cDel) // ignore body-write error on 204
	assert.Equal(t, http.StatusNoContent, recDel.Code)

	// Now list should be empty.
	rec5 := doAgentRequest(
		t, h, http.MethodGet,
		fmt.Sprintf("/agents/%s/agentversions/DRAFT/knowledgebases", agentID), nil,
	)
	assert.Equal(t, http.StatusOK, rec5.Code)

	var listOut2 map[string]any
	require.NoError(t, json.Unmarshal(rec5.Body.Bytes(), &listOut2))
	assert.Empty(t, listOut2["agentKnowledgeBaseSummaries"].([]any))
}

func TestAgentsHandler_AgentKBAssociation_AgentNotFound(t *testing.T) {
	t.Parallel()

	h, b := newTestAgentsHandler(t)
	kb, err := b.CreateKnowledgeBase("notfound-assoc-kb", "", "", nil, nil, nil)
	require.NoError(t, err)

	rec := doAgentRequest(
		t, h, http.MethodPut,
		"/agents/nonexistent/agentversions/DRAFT/knowledgebases",
		map[string]any{"knowledgeBaseId": kb.KnowledgeBaseID},
	)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAgentsHandler_AgentKBAssociation_KBNotFound(t *testing.T) {
	t.Parallel()

	h, b := newTestAgentsHandler(t)
	ag, err := b.CreateAgent("kbnotfound-agent", "", "", "", nil)
	require.NoError(t, err)

	rec := doAgentRequest(
		t, h, http.MethodPut,
		fmt.Sprintf("/agents/%s/agentversions/DRAFT/knowledgebases", ag.AgentID),
		map[string]any{"knowledgeBaseId": "nonexistent-kb"},
	)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAgentsHandler_KnowledgeBaseAssociationUpdateAndDocumentGet(t *testing.T) {
	t.Parallel()

	h, b := newTestAgentsHandler(t)
	agent, err := b.CreateAgent("kb-update-agent", "model", "", "", nil)
	require.NoError(t, err)
	kbID, dsID := createKBAndDS(t, h)
	assocPath := fmt.Sprintf("/agents/%s/agentversions/DRAFT/knowledgebases", agent.AgentID)

	rec := doAgentRequest(t, h, http.MethodPut, assocPath, map[string]any{"knowledgeBaseId": kbID})
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doAgentRequest(t, h, http.MethodPut, assocPath+"/"+kbID, map[string]any{
		"description":        "updated association",
		"knowledgeBaseState": "DISABLED",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var assoc map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &assoc))
	assert.Equal(t, "DISABLED", assoc["agentKnowledgeBase"].(map[string]any)["knowledgeBaseState"])

	docPath := fmt.Sprintf("/knowledgebases/%s/datasources/%s/documents", kbID, dsID)
	// Real IngestKnowledgeBaseDocuments is PUT.
	rec = doAgentRequest(t, h, http.MethodPut, docPath, map[string]any{"documentIds": []string{"one", "two"}})
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doAgentRequest(t, h, http.MethodPost, docPath+"/getDocuments", map[string]any{"documentIds": []string{"two"}})
	require.Equal(t, http.StatusOK, rec.Code)
	var docs map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &docs))
	assert.Len(t, docs["documentDetails"], 1)
}
