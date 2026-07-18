package medialive_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEBRuleTemplateGroup_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(
		t,
		h,
		http.MethodPost,
		"/prod/eventbridge-rule-template-groups",
		map[string]any{
			"name": "eb-group-1",
		},
	)
	require.Equal(t, http.StatusCreated, rec.Code)
	var created map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	id := created["id"].(string)
	assert.NotEmpty(t, id)
	assert.NotEmpty(t, created["createdAt"])
	assert.NotEmpty(t, created["modifiedAt"])

	// Get
	rec = doRequest(t, h, http.MethodGet, "/prod/eventbridge-rule-template-groups/"+id, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Update
	rec = doRequest(
		t,
		h,
		http.MethodPatch,
		"/prod/eventbridge-rule-template-groups/"+id,
		map[string]any{
			"description": "updated desc",
		},
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	// List
	rec = doRequest(t, h, http.MethodGet, "/prod/eventbridge-rule-template-groups", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Len(t, listResp["eventBridgeRuleTemplateGroups"].([]any), 1)

	// Delete
	rec = doRequest(t, h, http.MethodDelete, "/prod/eventbridge-rule-template-groups/"+id, nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

func TestEBRuleTemplate_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/prod/eventbridge-rule-templates", map[string]any{
		"name":      "eb-template-1",
		"eventType": "MEDIALIVE_CHANNEL_STATE_CHANGE",
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	var created map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	id := created["id"].(string)
	assert.NotEmpty(t, created["createdAt"])
	assert.NotEmpty(t, created["modifiedAt"])
	_, hasGroupIdentifier := created["groupIdentifier"]
	assert.False(t, hasGroupIdentifier, "real GetEventBridgeRuleTemplateOutput has no groupIdentifier field")

	// Get by ID
	rec = doRequest(t, h, http.MethodGet, "/prod/eventbridge-rule-templates/"+id, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Update (PATCH)
	rec = doRequest(t, h, http.MethodPatch, "/prod/eventbridge-rule-templates/"+id, map[string]any{
		"eventType": "MEDIALIVE_MULTIPLEX_ALERT",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var updated map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updated))
	assert.Equal(t, "MEDIALIVE_MULTIPLEX_ALERT", updated["eventType"])

	// List
	rec = doRequest(t, h, http.MethodGet, "/prod/eventbridge-rule-templates", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Len(t, listResp["eventBridgeRuleTemplates"].([]any), 1)

	// Delete
	rec = doRequest(t, h, http.MethodDelete, "/prod/eventbridge-rule-templates/"+id, nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)
}
