package bedrock_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrock"
)

// newTestAgentsHandler creates a Handler and AgentsHandler sharing the same backend.
func newTestAgentsHandler(t *testing.T) (*bedrock.AgentsHandler, *bedrock.InMemoryBackend) {
	t.Helper()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")

	return bedrock.NewAgentsHandler(b), b
}

func doAgentRequest(
	t *testing.T,
	h *bedrock.AgentsHandler,
	method, path string,
	body any,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()
	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// ---------------------------------------------------------------------------
// AgentsHandler metadata tests
// ---------------------------------------------------------------------------

func TestAgentsHandler_Name(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)
	assert.Equal(t, "BedrockAgents", h.Name())
}

func TestAgentsHandler_ChaosServiceName(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)
	assert.Equal(t, "bedrock-agent", h.ChaosServiceName())
}

func TestAgentsHandler_ChaosOperations(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)
	assert.Equal(t, h.GetSupportedOperations(), h.ChaosOperations())
}

func TestAgentsHandler_ChaosRegions(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)
	assert.Equal(t, []string{"us-east-1"}, h.ChaosRegions())
}

func TestAgentsHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)
	assert.Positive(t, h.MatchPriority())
}

func TestAgentsHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/agents/agent-001", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	// ExtractResource always returns empty for AgentsHandler.
	assert.Empty(t, h.ExtractResource(c))
}

func TestAgentsHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)
	ops := h.GetSupportedOperations()

	for _, op := range []string{
		"CreateAgent", "GetAgent", "ListAgents", "UpdateAgent", "DeleteAgent", "PrepareAgent",
		"CreateAgentActionGroup", "GetAgentActionGroup", "ListAgentActionGroups",
		"UpdateAgentActionGroup", "DeleteAgentActionGroup",
		"CreateAgentAlias", "GetAgentAlias", "ListAgentAliases", "UpdateAgentAlias", "DeleteAgentAlias",
		"AssociateAgentKnowledgeBase", "DisassociateAgentKnowledgeBase",
		"GetAgentKnowledgeBase", "ListAgentKnowledgeBases",
		"CreateKnowledgeBase", "GetKnowledgeBase", "ListKnowledgeBases",
		"UpdateKnowledgeBase", "DeleteKnowledgeBase",
		"CreateDataSource", "GetDataSource", "ListDataSources", "UpdateDataSource", "DeleteDataSource",
		"StartIngestionJob", "GetIngestionJob", "ListIngestionJobs",
	} {
		assert.Contains(t, ops, op)
	}
}

func TestAgentsHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)
	e := echo.New()

	tests := []struct {
		name string
		path string
		want bool
	}{
		{"agents root", "/agents", true},
		{"agent with id", "/agents/agent-001", true},
		{"knowledgebases root", "/knowledgebases", true},
		{"knowledgebase with id", "/knowledgebases/kb-001", true},
		{"unmatched", "/other-path", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			assert.Equal(t, tt.want, h.RouteMatcher()(c))
		})
	}
}

func TestAgentsHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)
	e := echo.New()

	tests := []struct {
		name   string
		method string
		path   string
		want   string
	}{
		{"CreateAgent", http.MethodPost, "/agents", "CreateAgent"},
		{"ListAgents", http.MethodGet, "/agents", "ListAgents"},
		{"PrepareAgent", http.MethodPost, "/agents/agent-001/prepare", "PrepareAgent"},
		{"CreateAgentActionGroup", http.MethodPost, "/agents/agent-001/action-groups", "CreateAgentActionGroup"},
		{"ListAgentActionGroups", http.MethodGet, "/agents/agent-001/action-groups", "ListAgentActionGroups"},
		{"CreateAgentAlias", http.MethodPost, "/agents/agent-001/aliases", "CreateAgentAlias"},
		{"ListAgentAliases", http.MethodGet, "/agents/agent-001/aliases", "ListAgentAliases"},
		{"CreateKnowledgeBase", http.MethodPost, "/knowledgebases", "CreateKnowledgeBase"},
		{"ListKnowledgeBases", http.MethodGet, "/knowledgebases", "ListKnowledgeBases"},
		{
			"StartIngestionJob",
			http.MethodPost,
			"/knowledgebases/kb-001/datasources/ds-001/ingestionjobs",
			"StartIngestionJob",
		},
		{
			"ListIngestionJobs",
			http.MethodGet,
			"/knowledgebases/kb-001/datasources/ds-001/ingestionjobs",
			"ListIngestionJobs",
		},
		{"Unknown", http.MethodGet, "/unknown-path", "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			assert.Equal(t, tt.want, h.ExtractOperation(c))
		})
	}
}

// ---------------------------------------------------------------------------
// Agent CRUD
// ---------------------------------------------------------------------------

func TestAgentsHandler_CreateAgent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input      map[string]any
		name       string
		wantStatus int
		wantAgent  bool
	}{
		{
			name: "valid agent",
			input: map[string]any{
				"agentName":       "my-agent",
				"foundationModel": "anthropic.claude-v2",
				"instruction":     "You are a helpful assistant",
			},
			wantStatus: http.StatusAccepted,
			wantAgent:  true,
		},
		{
			name: "duplicate name",
			input: map[string]any{
				"agentName": "dup-agent",
			},
			wantStatus: http.StatusConflict,
		},
		{
			name:       "invalid json",
			input:      nil,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestAgentsHandler(t)

			if tt.name == "duplicate name" {
				_, err := b.CreateAgent("dup-agent", "", "", "", nil)
				require.NoError(t, err)
			}

			if tt.input == nil {
				e := echo.New()
				req := httptest.NewRequest(http.MethodPost, "/agents", bytes.NewReader([]byte("bad json")))
				req.Header.Set("Content-Type", "application/json")
				rec := httptest.NewRecorder()
				c := e.NewContext(req, rec)
				err := h.Handler()(c)
				require.NoError(t, err)
				assert.Equal(t, tt.wantStatus, rec.Code)

				return
			}

			rec := doAgentRequest(t, h, http.MethodPost, "/agents", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantAgent {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				agent := out["agent"].(map[string]any)
				assert.NotEmpty(t, agent["agentId"])
				assert.NotEmpty(t, agent["agentArn"])
			}
		})
	}
}

func TestAgentsHandler_GetAgent(t *testing.T) {
	t.Parallel()

	h, b := newTestAgentsHandler(t)
	ag, err := b.CreateAgent("get-agent", "anthropic.claude-v2", "", "", nil)
	require.NoError(t, err)

	t.Run("existing agent", func(t *testing.T) {
		t.Parallel()

		rec := doAgentRequest(t, h, http.MethodGet, "/agents/"+ag.AgentID, nil)
		assert.Equal(t, http.StatusOK, rec.Code)

		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		agent := out["agent"].(map[string]any)
		assert.Equal(t, ag.AgentID, agent["agentId"])
	})

	t.Run("nonexistent agent", func(t *testing.T) {
		t.Parallel()

		rec := doAgentRequest(t, h, http.MethodGet, "/agents/nonexistent", nil)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})
}

func TestAgentsHandler_ListAgents(t *testing.T) {
	t.Parallel()

	h, b := newTestAgentsHandler(t)

	rec := doAgentRequest(t, h, http.MethodGet, "/agents", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Empty(t, out["agentSummaries"])

	// Add agents.
	_, err := b.CreateAgent("agent-a", "", "", "", nil)
	require.NoError(t, err)
	_, err = b.CreateAgent("agent-b", "", "", "", nil)
	require.NoError(t, err)

	rec2 := doAgentRequest(t, h, http.MethodGet, "/agents", nil)
	assert.Equal(t, http.StatusOK, rec2.Code)

	var out2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out2))
	assert.Len(t, out2["agentSummaries"].([]any), 2)
}

func TestAgentsHandler_UpdateAgent(t *testing.T) {
	t.Parallel()

	h, b := newTestAgentsHandler(t)
	ag, err := b.CreateAgent("update-agent", "", "", "", nil)
	require.NoError(t, err)

	t.Run("update existing", func(t *testing.T) {
		t.Parallel()

		rec := doAgentRequest(t, h, http.MethodPut, "/agents/"+ag.AgentID, map[string]any{
			"foundationModel": "anthropic.claude-v2",
		})
		assert.Equal(t, http.StatusOK, rec.Code)

		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		agent := out["agent"].(map[string]any)
		assert.Equal(t, "anthropic.claude-v2", agent["foundationModel"])
	})

	t.Run("update nonexistent", func(t *testing.T) {
		t.Parallel()

		rec := doAgentRequest(t, h, http.MethodPut, "/agents/nonexistent", map[string]any{
			"foundationModel": "anthropic.claude-v2",
		})
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("invalid json", func(t *testing.T) {
		t.Parallel()

		e := echo.New()
		req := httptest.NewRequest(http.MethodPut, "/agents/"+ag.AgentID, bytes.NewReader([]byte("bad json")))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		handlerErr := h.Handler()(c)
		require.NoError(t, handlerErr)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

func TestAgentsHandler_DeleteAgent(t *testing.T) {
	t.Parallel()

	h, b := newTestAgentsHandler(t)
	ag, err := b.CreateAgent("delete-agent", "", "", "", nil)
	require.NoError(t, err)

	rec := doAgentRequest(t, h, http.MethodDelete, "/agents/"+ag.AgentID, nil)
	assert.Equal(t, http.StatusAccepted, rec.Code)

	// Now it should be gone.
	rec2 := doAgentRequest(t, h, http.MethodGet, "/agents/"+ag.AgentID, nil)
	assert.Equal(t, http.StatusNotFound, rec2.Code)

	// Delete nonexistent.
	rec3 := doAgentRequest(t, h, http.MethodDelete, "/agents/nonexistent", nil)
	assert.Equal(t, http.StatusNotFound, rec3.Code)
}

func TestAgentsHandler_PrepareAgent(t *testing.T) {
	t.Parallel()

	h, b := newTestAgentsHandler(t)
	ag, err := b.CreateAgent("prepare-agent", "amazon.titan-text-express-v1", "", "", nil)
	require.NoError(t, err)

	rec := doAgentRequest(t, h, http.MethodPost, "/agents/"+ag.AgentID+"/prepare", nil)
	assert.Equal(t, http.StatusAccepted, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "PREPARING", out["agentStatus"])

	// Prepare nonexistent.
	rec2 := doAgentRequest(t, h, http.MethodPost, "/agents/nonexistent/prepare", nil)
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}

// ---------------------------------------------------------------------------
// Agent Action Groups
// ---------------------------------------------------------------------------

func TestAgentsHandler_AgentActionGroupLifecycle(t *testing.T) {
	t.Parallel()

	h, b := newTestAgentsHandler(t)
	ag, err := b.CreateAgent("ag-group-agent", "", "", "", nil)
	require.NoError(t, err)
	agentID := ag.AgentID

	// Create action group.
	rec := doAgentRequest(t, h, http.MethodPost, "/agents/"+agentID+"/action-groups", map[string]any{
		"actionGroupName": "my-action-group",
		"description":     "test action group",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))
	ag2 := createOut["agentActionGroup"].(map[string]any)
	agGroupID := ag2["actionGroupId"].(string)
	assert.NotEmpty(t, agGroupID)

	// List action groups.
	rec2 := doAgentRequest(t, h, http.MethodGet, "/agents/"+agentID+"/action-groups", nil)
	assert.Equal(t, http.StatusOK, rec2.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &listOut))
	assert.Len(t, listOut["actionGroupSummaries"].([]any), 1)

	// Get action group (via /action-groups/{version}/{id} path).
	rec3 := doAgentRequest(
		t, h, http.MethodGet,
		fmt.Sprintf("/agents/%s/action-groups/DRAFT/%s", agentID, agGroupID), nil,
	)
	assert.Equal(t, http.StatusOK, rec3.Code)

	// Update action group.
	rec4 := doAgentRequest(
		t, h, http.MethodPut,
		fmt.Sprintf("/agents/%s/action-groups/DRAFT/%s", agentID, agGroupID),
		map[string]any{"description": "updated description"},
	)
	assert.Equal(t, http.StatusOK, rec4.Code)

	// Delete action group.
	rec5 := doAgentRequest(
		t, h, http.MethodDelete,
		fmt.Sprintf("/agents/%s/action-groups/DRAFT/%s", agentID, agGroupID), nil,
	)
	assert.Equal(t, http.StatusOK, rec5.Code)

	// Now list should be empty.
	rec6 := doAgentRequest(t, h, http.MethodGet, "/agents/"+agentID+"/action-groups", nil)
	assert.Equal(t, http.StatusOK, rec6.Code)

	var listOut2 map[string]any
	require.NoError(t, json.Unmarshal(rec6.Body.Bytes(), &listOut2))
	assert.Empty(t, listOut2["actionGroupSummaries"].([]any))
}

func TestAgentsHandler_CreateActionGroup_AgentNotFound(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)

	rec := doAgentRequest(t, h, http.MethodPost, "/agents/nonexistent/action-groups", map[string]any{
		"actionGroupName": "test",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAgentsHandler_CreateActionGroup_InvalidJSON(t *testing.T) {
	t.Parallel()

	h, b := newTestAgentsHandler(t)
	ag, err := b.CreateAgent("json-agent", "", "", "", nil)
	require.NoError(t, err)

	e := echo.New()
	req := httptest.NewRequest(
		http.MethodPost,
		"/agents/"+ag.AgentID+"/action-groups",
		bytes.NewReader([]byte("bad json")),
	)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAgentsHandler_UpdateActionGroup_NotFound(t *testing.T) {
	t.Parallel()

	h, b := newTestAgentsHandler(t)
	ag, err := b.CreateAgent("upd-ag-agent", "", "", "", nil)
	require.NoError(t, err)

	rec := doAgentRequest(
		t, h, http.MethodPut,
		fmt.Sprintf("/agents/%s/action-groups/DRAFT/nonexistent", ag.AgentID),
		map[string]any{"description": "x"},
	)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAgentsHandler_DeleteActionGroup_NotFound(t *testing.T) {
	t.Parallel()

	h, b := newTestAgentsHandler(t)
	ag, err := b.CreateAgent("del-ag-agent", "", "", "", nil)
	require.NoError(t, err)

	rec := doAgentRequest(
		t, h, http.MethodDelete,
		fmt.Sprintf("/agents/%s/action-groups/DRAFT/nonexistent", ag.AgentID), nil,
	)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ---------------------------------------------------------------------------
// Agent Aliases
// ---------------------------------------------------------------------------

func TestAgentsHandler_AgentAliasLifecycle(t *testing.T) {
	t.Parallel()

	h, b := newTestAgentsHandler(t)
	ag, err := b.CreateAgent("alias-agent", "", "", "", nil)
	require.NoError(t, err)
	agentID := ag.AgentID

	// Create alias.
	rec := doAgentRequest(t, h, http.MethodPost, "/agents/"+agentID+"/aliases", map[string]any{
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

	rec := doAgentRequest(t, h, http.MethodPost, "/agents/nonexistent/aliases", map[string]any{
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

// ---------------------------------------------------------------------------
// Knowledge Bases
// ---------------------------------------------------------------------------

func TestAgentsHandler_KnowledgeBaseLifecycle(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)

	// Create KB.
	rec := doAgentRequest(t, h, http.MethodPost, "/knowledgebases", map[string]any{
		"name":        "my-kb",
		"description": "test kb",
		"roleArn":     "arn:aws:iam::000000000000:role/test",
	})
	assert.Equal(t, http.StatusAccepted, rec.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))
	kb := createOut["knowledgeBase"].(map[string]any)
	kbID := kb["knowledgeBaseId"].(string)
	assert.NotEmpty(t, kbID)
	assert.Equal(t, "ACTIVE", kb["status"])

	// List KBs.
	rec2 := doAgentRequest(t, h, http.MethodGet, "/knowledgebases", nil)
	assert.Equal(t, http.StatusOK, rec2.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &listOut))
	assert.Len(t, listOut["knowledgeBaseSummaries"].([]any), 1)

	// Get KB.
	rec3 := doAgentRequest(t, h, http.MethodGet, "/knowledgebases/"+kbID, nil)
	assert.Equal(t, http.StatusOK, rec3.Code)

	var getOut map[string]any
	require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &getOut))
	assert.Equal(t, "my-kb", getOut["knowledgeBase"].(map[string]any)["name"])

	// Update KB.
	rec4 := doAgentRequest(t, h, http.MethodPut, "/knowledgebases/"+kbID, map[string]any{
		"description": "updated description",
	})
	assert.Equal(t, http.StatusOK, rec4.Code)

	// Delete KB.
	rec5 := doAgentRequest(t, h, http.MethodDelete, "/knowledgebases/"+kbID, nil)
	assert.Equal(t, http.StatusAccepted, rec5.Code)

	// KB should be gone.
	rec6 := doAgentRequest(t, h, http.MethodGet, "/knowledgebases/"+kbID, nil)
	assert.Equal(t, http.StatusNotFound, rec6.Code)
}

func TestAgentsHandler_CreateKnowledgeBase_Duplicate(t *testing.T) {
	t.Parallel()

	h, b := newTestAgentsHandler(t)
	_, err := b.CreateKnowledgeBase("dup-kb", "", "", nil, nil, nil)
	require.NoError(t, err)

	rec := doAgentRequest(t, h, http.MethodPost, "/knowledgebases", map[string]any{
		"name": "dup-kb",
	})
	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestAgentsHandler_KnowledgeBase_NotFound(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)

	t.Run("get nonexistent", func(t *testing.T) {
		t.Parallel()

		rec := doAgentRequest(t, h, http.MethodGet, "/knowledgebases/nonexistent", nil)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("update nonexistent", func(t *testing.T) {
		t.Parallel()

		rec := doAgentRequest(t, h, http.MethodPut, "/knowledgebases/nonexistent", map[string]any{
			"name": "test",
		})
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("delete nonexistent", func(t *testing.T) {
		t.Parallel()

		rec := doAgentRequest(t, h, http.MethodDelete, "/knowledgebases/nonexistent", nil)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})
}

func TestAgentsHandler_KnowledgeBase_InvalidJSON(t *testing.T) {
	t.Parallel()

	h, b := newTestAgentsHandler(t)
	kb, err := b.CreateKnowledgeBase("json-kb", "", "", nil, nil, nil)
	require.NoError(t, err)

	for _, tc := range []struct {
		path   string
		method string
	}{
		{"/knowledgebases", http.MethodPost},
		{"/knowledgebases/" + kb.KnowledgeBaseID, http.MethodPut},
	} {
		e := echo.New()
		req := httptest.NewRequest(tc.method, tc.path, bytes.NewReader([]byte("bad json")))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		require.NoError(t, h.Handler()(c))
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	}
}

// ---------------------------------------------------------------------------
// Data Sources
// ---------------------------------------------------------------------------

func TestAgentsHandler_DataSourceLifecycle(t *testing.T) {
	t.Parallel()

	h, b := newTestAgentsHandler(t)
	kb, err := b.CreateKnowledgeBase("ds-kb", "", "", nil, nil, nil)
	require.NoError(t, err)
	kbID := kb.KnowledgeBaseID

	// Create data source.
	rec := doAgentRequest(t, h, http.MethodPost, "/knowledgebases/"+kbID+"/datasources", map[string]any{
		"name":        "my-ds",
		"description": "test data source",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))
	ds := createOut["dataSource"].(map[string]any)
	dsID := ds["dataSourceId"].(string)
	assert.NotEmpty(t, dsID)

	// List data sources.
	rec2 := doAgentRequest(t, h, http.MethodGet, "/knowledgebases/"+kbID+"/datasources", nil)
	assert.Equal(t, http.StatusOK, rec2.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &listOut))
	assert.Len(t, listOut["dataSourceSummaries"].([]any), 1)

	// Get data source.
	rec3 := doAgentRequest(t, h, http.MethodGet, "/knowledgebases/"+kbID+"/datasources/"+dsID, nil)
	assert.Equal(t, http.StatusOK, rec3.Code)

	// Update data source.
	rec4 := doAgentRequest(t, h, http.MethodPut, "/knowledgebases/"+kbID+"/datasources/"+dsID, map[string]any{
		"description": "updated",
	})
	assert.Equal(t, http.StatusOK, rec4.Code)

	// Delete data source.
	rec5 := doAgentRequest(t, h, http.MethodDelete, "/knowledgebases/"+kbID+"/datasources/"+dsID, nil)
	assert.Equal(t, http.StatusOK, rec5.Code)
}

func TestAgentsHandler_DataSource_KBNotFound(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)

	rec := doAgentRequest(t, h, http.MethodPost, "/knowledgebases/nonexistent/datasources", map[string]any{
		"name": "ds",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAgentsHandler_DataSource_NotFound(t *testing.T) {
	t.Parallel()

	h, b := newTestAgentsHandler(t)
	kb, err := b.CreateKnowledgeBase("notfound-kb", "", "", nil, nil, nil)
	require.NoError(t, err)

	for _, tc := range []struct {
		path   string
		method string
	}{
		{"/knowledgebases/" + kb.KnowledgeBaseID + "/datasources/nonexistent", http.MethodGet},
		{"/knowledgebases/" + kb.KnowledgeBaseID + "/datasources/nonexistent", http.MethodPut},
		{"/knowledgebases/" + kb.KnowledgeBaseID + "/datasources/nonexistent", http.MethodDelete},
	} {
		rec := doAgentRequest(t, h, tc.method, tc.path, map[string]any{"name": "test"})
		assert.Equal(t, http.StatusNotFound, rec.Code)
	}
}

// ---------------------------------------------------------------------------
// Ingestion Jobs
// ---------------------------------------------------------------------------

func TestAgentsHandler_IngestionJobLifecycle(t *testing.T) {
	t.Parallel()

	h, b := newTestAgentsHandler(t)
	kb, err := b.CreateKnowledgeBase("ij-kb", "", "", nil, nil, nil)
	require.NoError(t, err)
	ds, err := b.CreateDataSource(kb.KnowledgeBaseID, "ij-ds", "", nil)
	require.NoError(t, err)

	kbID := kb.KnowledgeBaseID
	dsID := ds.DataSourceID

	// Start ingestion job.
	rec := doAgentRequest(
		t, h, http.MethodPost,
		fmt.Sprintf("/knowledgebases/%s/datasources/%s/ingestionjobs", kbID, dsID),
		map[string]any{"description": "test ingestion"},
	)
	assert.Equal(t, http.StatusAccepted, rec.Code)

	var startOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startOut))
	job := startOut["ingestionJob"].(map[string]any)
	jobID := job["ingestionJobId"].(string)
	assert.NotEmpty(t, jobID)
	assert.Equal(t, "STARTING", job["status"])

	// Get ingestion job.
	rec2 := doAgentRequest(
		t, h, http.MethodGet,
		fmt.Sprintf("/knowledgebases/%s/datasources/%s/ingestionjobs/%s", kbID, dsID, jobID), nil,
	)
	assert.Equal(t, http.StatusOK, rec2.Code)

	// List ingestion jobs.
	rec3 := doAgentRequest(
		t, h, http.MethodGet,
		fmt.Sprintf("/knowledgebases/%s/datasources/%s/ingestionjobs", kbID, dsID), nil,
	)
	assert.Equal(t, http.StatusOK, rec3.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &listOut))
	assert.Len(t, listOut["ingestionJobSummaries"].([]any), 1)
}

func TestAgentsHandler_IngestionJob_EventuallyComplete(t *testing.T) {
	t.Parallel()

	_, b := newTestAgentsHandler(t)
	kb, err := b.CreateKnowledgeBase("ij-complete-kb", "", "", nil, nil, nil)
	require.NoError(t, err)
	ds, err := b.CreateDataSource(kb.KnowledgeBaseID, "ij-complete-ds", "", nil)
	require.NoError(t, err)

	job, err := b.StartIngestionJob(kb.KnowledgeBaseID, ds.DataSourceID, "test")
	require.NoError(t, err)
	assert.Equal(t, "STARTING", job.Status)

	// Wait for async completion.
	time.Sleep(500 * time.Millisecond)

	got, err := b.GetIngestionJob(kb.KnowledgeBaseID, ds.DataSourceID, job.IngestionJobID)
	require.NoError(t, err)
	assert.Equal(t, "COMPLETE", got.Status)
}

func TestAgentsHandler_StartIngestionJob_InvalidJSON(t *testing.T) {
	t.Parallel()

	h, b := newTestAgentsHandler(t)
	kb, err := b.CreateKnowledgeBase("ij-json-kb", "", "", nil, nil, nil)
	require.NoError(t, err)
	ds, err := b.CreateDataSource(kb.KnowledgeBaseID, "ij-json-ds", "", nil)
	require.NoError(t, err)

	e := echo.New()
	req := httptest.NewRequest(
		http.MethodPost,
		fmt.Sprintf("/knowledgebases/%s/datasources/%s/ingestionjobs", kb.KnowledgeBaseID, ds.DataSourceID),
		bytes.NewReader([]byte("bad json")),
	)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// Agent KB Associations
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// AgentsHandler Reset
// ---------------------------------------------------------------------------

func TestAgentsHandler_Reset(t *testing.T) {
	t.Parallel()

	h, b := newTestAgentsHandler(t)

	// Create a variety of resources.
	ag, err := b.CreateAgent("reset-agent", "", "", "", nil)
	require.NoError(t, err)

	kb, err := b.CreateKnowledgeBase("reset-kb", "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateDataSource(kb.KnowledgeBaseID, "reset-ds", "", nil)
	require.NoError(t, err)

	_, err = b.CreateAgentAlias(ag.AgentID, "reset-alias", "DRAFT")
	require.NoError(t, err)

	// Verify resources exist.
	rec := doAgentRequest(t, h, http.MethodGet, "/agents", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listOut))
	assert.Len(t, listOut["agentSummaries"].([]any), 1)

	// Reset.
	h.Reset()

	// All agents should be gone.
	rec2 := doAgentRequest(t, h, http.MethodGet, "/agents", nil)
	assert.Equal(t, http.StatusOK, rec2.Code)

	var listOut2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &listOut2))
	assert.Empty(t, listOut2["agentSummaries"].([]any))

	// All KBs should be gone.
	rec3 := doAgentRequest(t, h, http.MethodGet, "/knowledgebases", nil)
	assert.Equal(t, http.StatusOK, rec3.Code)

	var listKBOut map[string]any
	require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &listKBOut))
	assert.Empty(t, listKBOut["knowledgeBaseSummaries"].([]any))

	// IDs reset - first agent after reset should get sequential id again.
	ag2, err := b.CreateAgent("post-reset-agent", "", "", "", nil)
	require.NoError(t, err)
	assert.Equal(t, "agent-00000001", ag2.AgentID)
}

// ---------------------------------------------------------------------------
// Unknown operation
// ---------------------------------------------------------------------------

func TestAgentsHandler_UnknownOperation(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)
	rec := doAgentRequest(t, h, http.MethodGet, "/unknown-path", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAgentsHandler_AgentPreparationTerminalStates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		foundationModel string
		wantTerminal    string
	}{
		{name: "prepared", foundationModel: "amazon.titan-text-express-v1", wantTerminal: "PREPARED"},
		{name: "failed without model", wantTerminal: "FAILED"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, b := newTestAgentsHandler(t)
			ag, err := b.CreateAgent(tt.name, tt.foundationModel, "", "", nil)
			require.NoError(t, err)
			assert.Equal(t, "NOT_PREPARED", ag.AgentStatus)

			preparing, err := b.PrepareAgent(ag.AgentID)
			require.NoError(t, err)
			assert.Equal(t, "PREPARING", preparing.AgentStatus)

			time.Sleep(150 * time.Millisecond)
			got, err := b.GetAgent(ag.AgentID)
			require.NoError(t, err)
			assert.Equal(t, tt.wantTerminal, got.AgentStatus)
			if tt.wantTerminal == "FAILED" {
				assert.NotEmpty(t, got.FailureReasons)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Custom Model CRUD (via HTTP handler)
// ---------------------------------------------------------------------------

func TestHandler_CustomModelLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create via handler.
	rec := doRequest(t, h, http.MethodPost, "/custom-models/create-custom-model", map[string]any{
		"modelName": "lifecycle-model",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))
	modelARN := createOut["modelArn"].(string)

	// List custom models.
	rec2 := doRequest(t, h, http.MethodGet, "/custom-models", nil)
	assert.Equal(t, http.StatusOK, rec2.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &listOut))
	assert.NotEmpty(t, listOut["modelSummaries"])

	// Get custom model by ARN.
	rec3 := doRequest(t, h, http.MethodGet, "/custom-models/"+url.PathEscape(modelARN), nil)
	assert.Equal(t, http.StatusOK, rec3.Code)

	// Delete custom model.
	rec4 := doRequest(t, h, http.MethodDelete, "/custom-models/"+url.PathEscape(modelARN), nil)
	assert.Equal(t, http.StatusOK, rec4.Code)
}

func TestHandler_InferenceProfileLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create inference profile.
	rec := doRequest(t, h, http.MethodPost, "/inference-profiles", map[string]any{
		"inferenceProfileName": "my-profile",
		"modelSource":          map[string]any{"copyFrom": "anthropic.claude-v2"},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))
	profileARN := createOut["inferenceProfileArn"].(string)

	// List profiles.
	rec2 := doRequest(t, h, http.MethodGet, "/inference-profiles", nil)
	assert.Equal(t, http.StatusOK, rec2.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &listOut))
	assert.NotEmpty(t, listOut["inferenceProfileSummaries"])

	// Get profile.
	rec3 := doRequest(t, h, http.MethodGet, "/inference-profiles/"+url.PathEscape(profileARN), nil)
	assert.Equal(t, http.StatusOK, rec3.Code)

	// Delete profile.
	rec4 := doRequest(t, h, http.MethodDelete, "/inference-profiles/"+url.PathEscape(profileARN), nil)
	assert.Equal(t, http.StatusOK, rec4.Code)
}

func TestHandler_ModelCustomizationJobLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create customization job.
	rec := doRequest(t, h, http.MethodPost, "/model-customization-jobs", map[string]any{
		"jobName":         "my-customization-job",
		"baseModelId":     "amazon.titan-text-express-v1",
		"customModelName": "my-fine-tuned-model",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))
	jobARN := createOut["jobArn"].(string)
	assert.NotEmpty(t, jobARN)

	// List jobs.
	rec2 := doRequest(t, h, http.MethodGet, "/model-customization-jobs", nil)
	assert.Equal(t, http.StatusOK, rec2.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &listOut))
	assert.NotEmpty(t, listOut["modelCustomizationJobSummaries"])

	// Get job.
	rec3 := doRequest(t, h, http.MethodGet, "/model-customization-jobs/"+url.PathEscape(jobARN), nil)
	assert.Equal(t, http.StatusOK, rec3.Code)

	// Stop job.
	rec4 := doRequest(t, h, http.MethodPost, "/model-customization-jobs/"+url.PathEscape(jobARN)+"/stop", nil)
	assert.Equal(t, http.StatusOK, rec4.Code)
}

func TestHandler_MarketplaceModelEndpointLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create marketplace endpoint.
	rec := doRequest(t, h, http.MethodPost, "/marketplace-model/endpoints", map[string]any{
		"endpointName":          "my-market-endpoint",
		"modelSourceIdentifier": "arn:aws:sagemaker:us-east-1:000000000000:hub-content/my-hub/Model/my-model",
		"endpointConfig":        map[string]any{},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))
	endpointARN := createOut["marketplaceModelEndpoint"].(map[string]any)["endpointArn"].(string)

	// List endpoints.
	rec2 := doRequest(t, h, http.MethodGet, "/marketplace-model/endpoints", nil)
	assert.Equal(t, http.StatusOK, rec2.Code)

	// Get endpoint.
	rec3 := doRequest(t, h, http.MethodGet, "/marketplace-model/endpoints/"+url.PathEscape(endpointARN), nil)
	assert.Equal(t, http.StatusOK, rec3.Code)

	// Update endpoint.
	rec4 := doRequest(t, h, http.MethodPut, "/marketplace-model/endpoints/"+url.PathEscape(endpointARN), map[string]any{
		"endpointConfig": map[string]any{},
	})
	assert.Equal(t, http.StatusOK, rec4.Code)

	// Register endpoint.
	regPath := "/marketplace-model/endpoints/" + url.PathEscape(endpointARN) + "/registration"
	rec5 := doRequest(t, h, http.MethodPost, regPath, nil)
	assert.Equal(t, http.StatusOK, rec5.Code)

	// Deregister endpoint.
	deregPath := "/marketplace-model/endpoints/" + url.PathEscape(endpointARN) + "/deregistration"
	rec6 := doRequest(t, h, http.MethodPost, deregPath, nil)
	assert.Equal(t, http.StatusOK, rec6.Code)

	// Delete endpoint.
	rec7 := doRequest(t, h, http.MethodDelete, "/marketplace-model/endpoints/"+url.PathEscape(endpointARN), nil)
	assert.Equal(t, http.StatusOK, rec7.Code)
}

func TestHandler_ModelInvocationLoggingConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Get (returns empty by default).
	rec := doRequest(t, h, http.MethodGet, "/logging/modelinvocations", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Put.
	rec2 := doRequest(t, h, http.MethodPut, "/logging/modelinvocations", map[string]any{
		"loggingConfig": map[string]any{
			"cloudWatchConfig": map[string]any{
				"logGroupName": "/aws/bedrock/model-invocations",
			},
		},
	})
	assert.Equal(t, http.StatusOK, rec2.Code)

	// Get again — should have config now.
	rec3 := doRequest(t, h, http.MethodGet, "/logging/modelinvocations", nil)
	assert.Equal(t, http.StatusOK, rec3.Code)

	var getOut map[string]any
	require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &getOut))
	assert.NotNil(t, getOut["loggingConfig"])

	// Delete.
	rec4 := doRequest(t, h, http.MethodDelete, "/logging/modelinvocations", nil)
	assert.Equal(t, http.StatusOK, rec4.Code)
}
