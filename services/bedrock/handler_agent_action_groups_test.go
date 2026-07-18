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

func TestAgentsHandler_ActionGroupSchemas(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		field     string
		schema    map[string]any
		wantValue string
	}{
		{
			name:      "openapi schema",
			field:     "apiSchema",
			schema:    map[string]any{"payload": "openapi: 3.0.0"},
			wantValue: "openapi: 3.0.0",
		},
		{
			name:      "function schema",
			field:     "functionSchema",
			schema:    map[string]any{"functions": "search"},
			wantValue: "search",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestAgentsHandler(t)
			agent, err := b.CreateAgent("schema-"+tt.name, "model", "", "", nil)
			require.NoError(t, err)
			body := map[string]any{"actionGroupName": "tools", tt.field: tt.schema}
			rec := doAgentRequest(t, h, http.MethodPost, "/agents/"+agent.AgentID+"/action-groups", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			schema := out["agentActionGroup"].(map[string]any)[tt.field].(map[string]any)
			assert.Contains(t, fmt.Sprint(schema), tt.wantValue)
		})
	}
}

func TestAccuracy_ActionGroup_ReturnControlExecutorPreserved(t *testing.T) {
	t.Parallel()

	h, b := newTestAgentsHandler(t)
	agent, err := b.CreateAgent("rc-agent", "amazon.titan-text-express-v1", "", "", nil)
	require.NoError(t, err)

	rec := doAgentRequest(
		t, h, http.MethodPost,
		fmt.Sprintf("/agents/%s/action-groups", agent.AgentID),
		map[string]any{
			"actionGroupName": "return-control-group",
			"actionGroupExecutor": map[string]any{
				"returnControl": map[string]any{},
			},
			"functionSchema": map[string]any{
				"functions": []map[string]any{
					{
						"name":        "get_order_status",
						"description": "Get the status of an order",
						"parameters": map[string]any{
							"order_id": map[string]any{
								"type":     "string",
								"required": true,
							},
						},
					},
				},
			},
		},
	)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	ag := body["agentActionGroup"].(map[string]any)
	agID := ag["actionGroupId"].(string)
	assert.NotEmpty(t, agID)

	executor := ag["actionGroupExecutor"].(map[string]any)
	assert.Contains(t, executor, "returnControl", "executor should contain returnControl key")
}

func TestAccuracy_ActionGroup_LambdaExecutorPreserved(t *testing.T) {
	t.Parallel()

	h, b := newTestAgentsHandler(t)
	agent, err := b.CreateAgent("lambda-agent", "amazon.titan-text-express-v1", "", "", nil)
	require.NoError(t, err)

	const lambdaARN = "arn:aws:lambda:us-east-1:000000000000:function:my-action-handler"

	rec := doAgentRequest(
		t, h, http.MethodPost,
		fmt.Sprintf("/agents/%s/action-groups", agent.AgentID),
		map[string]any{
			"actionGroupName": "lambda-group",
			"actionGroupExecutor": map[string]any{
				"lambda": lambdaARN,
			},
			"apiSchema": map[string]any{
				"payload": "openapi: 3.0.0\ninfo:\n  title: My API\n",
			},
		},
	)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	ag := body["agentActionGroup"].(map[string]any)
	executor := ag["actionGroupExecutor"].(map[string]any)

	assert.Equal(t, lambdaARN, executor["lambda"])
}

func TestAccuracy_ActionGroup_ReturnControlVsLambda(t *testing.T) {
	t.Parallel()

	tests := []struct {
		executorKey   string
		executorValue any
		name          string
	}{
		{
			name:          "return-control executor",
			executorKey:   "returnControl",
			executorValue: map[string]any{},
		},
		{
			name:          "lambda executor",
			executorKey:   "lambda",
			executorValue: "arn:aws:lambda:us-east-1:000000000000:function:handler",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestAgentsHandler(t)
			agent, err := b.CreateAgent("exec-test-"+tt.name, "model", "", "", nil)
			require.NoError(t, err)

			rec := doAgentRequest(
				t, h, http.MethodPost,
				fmt.Sprintf("/agents/%s/action-groups", agent.AgentID),
				map[string]any{
					"actionGroupName": "exec-group",
					"actionGroupExecutor": map[string]any{
						tt.executorKey: tt.executorValue,
					},
				},
			)
			require.Equal(t, http.StatusOK, rec.Code)

			var body map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			ag := body["agentActionGroup"].(map[string]any)
			executor := ag["actionGroupExecutor"].(map[string]any)
			assert.Contains(t, executor, tt.executorKey)
		})
	}
}
