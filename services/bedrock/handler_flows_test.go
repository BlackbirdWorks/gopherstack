package bedrock_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFlowCRUD(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)

	// Create
	rec := doAgentRequest(t, h, http.MethodPost, "/flows", map[string]any{
		"name":        "my-flow",
		"description": "test flow",
	})
	assert.Equal(t, http.StatusCreated, rec.Code)

	var flow map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &flow))
	flowID, _ := flow["id"].(string)
	assert.NotEmpty(t, flowID)
	assert.Equal(t, "my-flow", flow["name"])
	assert.Equal(t, "NotPrepared", flow["status"])

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

	// Prepare: real PrepareFlow POSTs to the same "/flows/{id}" path as
	// Get/Update/Delete (botocore bedrock-agent 2023-06-05 has no
	// "/prepare" suffix).
	rec = doAgentRequest(t, h, http.MethodPost, "/flows/"+flowID, nil)
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
