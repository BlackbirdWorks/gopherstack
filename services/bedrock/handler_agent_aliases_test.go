package bedrock_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAgentsHandler_AgentAliasLifecycle(t *testing.T) {
	t.Parallel()

	h, b := newTestAgentsHandler(t)
	ag, err := b.CreateAgent("alias-agent", "", "", "", nil)
	require.NoError(t, err)
	agentID := ag.AgentID

	// Create alias.
	rec := doAgentRequest(t, h, http.MethodPut, "/agents/"+agentID+"/aliases", map[string]any{
		"agentAliasName": "my-alias",
	})
	assert.Equal(t, http.StatusAccepted, rec.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))
	alias := createOut["agentAlias"].(map[string]any)
	aliasID := alias["agentAliasId"].(string)
	assert.NotEmpty(t, aliasID)

	// List aliases.
	rec2 := doAgentRequest(t, h, http.MethodGet, "/agents/"+agentID+"/aliases", nil)
	assert.Equal(t, http.StatusOK, rec2.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &listOut))
	assert.Len(t, listOut["agentAliasSummaries"].([]any), 1)

	// Get alias.
	rec3 := doAgentRequest(t, h, http.MethodGet, "/agents/"+agentID+"/aliases/"+aliasID, nil)
	assert.Equal(t, http.StatusOK, rec3.Code)

	// Update alias.
	rec4 := doAgentRequest(t, h, http.MethodPut, "/agents/"+agentID+"/aliases/"+aliasID, map[string]any{
		"agentAliasName": "updated-alias",
	})
	assert.Equal(t, http.StatusOK, rec4.Code)

	// Delete alias.
	rec5 := doAgentRequest(t, h, http.MethodDelete, "/agents/"+agentID+"/aliases/"+aliasID, nil)
	assert.Equal(t, http.StatusAccepted, rec5.Code)

	// Alias should be gone.
	rec6 := doAgentRequest(t, h, http.MethodGet, "/agents/"+agentID+"/aliases/"+aliasID, nil)
	assert.Equal(t, http.StatusNotFound, rec6.Code)
}

func TestAgentsHandler_Alias_AgentNotFound(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)

	rec := doAgentRequest(t, h, http.MethodPut, "/agents/nonexistent/aliases", map[string]any{
		"agentAliasName": "alias",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAgentsHandler_UpdateAlias_InvalidJSON(t *testing.T) {
	t.Parallel()

	h, b := newTestAgentsHandler(t)
	ag, err := b.CreateAgent("alias-json-agent", "", "", "", nil)
	require.NoError(t, err)
	alias, err := b.CreateAgentAlias(ag.AgentID, "test-alias", "DRAFT")
	require.NoError(t, err)

	e := echo.New()
	req := httptest.NewRequest(
		http.MethodPut,
		fmt.Sprintf("/agents/%s/aliases/%s", ag.AgentID, alias.AgentAliasID),
		bytes.NewReader([]byte("bad json")),
	)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAgentsHandler_AliasHistoryEvents(t *testing.T) {
	t.Parallel()

	h, b := newTestAgentsHandler(t)
	agent, err := b.CreateAgent("history-agent", "model", "", "", nil)
	require.NoError(t, err)
	path := "/agents/" + agent.AgentID + "/aliases"

	create := doAgentRequest(t, h, http.MethodPut, path, map[string]any{
		"agentAliasName":       "live",
		"routingConfiguration": []map[string]string{{"agentVersion": "1"}},
	})
	require.Equal(t, http.StatusAccepted, create.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(create.Body.Bytes(), &created))
	aliasID := created["agentAlias"].(map[string]any)["agentAliasId"].(string)
	update := doAgentRequest(t, h, http.MethodPut, path+"/"+aliasID, map[string]any{
		"routingConfiguration": []map[string]string{{"agentVersion": "2"}},
	})
	require.Equal(t, http.StatusOK, update.Code)

	var updated map[string]any
	require.NoError(t, json.Unmarshal(update.Body.Bytes(), &updated))
	alias := updated["agentAlias"].(map[string]any)
	events := alias["agentAliasHistoryEvents"].([]any)
	require.Len(t, events, 2)
	assert.NotEmpty(t, events[0].(map[string]any)["endDate"])
	assert.Equal(t, "2", alias["routingConfiguration"].([]any)[0].(map[string]any)["agentVersion"])
}
