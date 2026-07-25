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

// TestEBRuleTemplateGroup_ListTemplateCount mirrors
// TestCWAlarmTemplateGroup_ListTemplateCount for EventBridge rule template
// groups: Get/Create/Update must omit "templateCount" (not a real field on
// those shapes), List must include the live count.
func TestEBRuleTemplateGroup_ListTemplateCount(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/prod/eventbridge-rule-template-groups",
		map[string]any{"name": "eb-group-count"})
	require.Equal(t, http.StatusCreated, rec.Code)
	created := decodeBody(t, rec.Body.Bytes())
	groupID := created["id"].(string)
	_, hasCount := created["templateCount"]
	assert.False(t, hasCount, "real Create/Get/UpdateEventBridgeRuleTemplateGroupOutput has no templateCount field")

	rec = doRequest(t, h, http.MethodPost, "/prod/eventbridge-rule-templates", map[string]any{
		"name": "eb-tmpl-count-1", "groupIdentifier": groupID, "eventType": "MEDIALIVE_CHANNEL_ALERT",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	rec = doRequest(t, h, http.MethodGet, "/prod/eventbridge-rule-template-groups", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	items := decodeBody(t, rec.Body.Bytes())["eventBridgeRuleTemplateGroups"].([]any)
	require.Len(t, items, 1)

	group := items[0].(map[string]any)
	assert.InDelta(t, float64(1), group["templateCount"], 0)
}

// TestEBRuleTemplate_ListEventTargetCount locks in a fix for a gap where
// ListEventBridgeRuleTemplates reused the Get/Create/Update shape's full
// "eventTargets" array. The real ListEventBridgeRuleTemplatesOutput's
// per-item Summary shape has "eventTargetCount" (an integer) instead
// (verified against aws-sdk-go-v2/service/medialive's
// EventBridgeRuleTemplateSummary type) -- Get/Create/Update must still
// return the full array.
func TestEBRuleTemplate_ListEventTargetCount(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/prod/eventbridge-rule-templates", map[string]any{
		"name": "eb-tmpl-targets", "eventType": "MEDIALIVE_CHANNEL_ALERT",
		"eventTargets": []map[string]any{
			{"arn": "arn:aws:sns:us-east-1:000000000000:topic-a"},
			{"arn": "arn:aws:sns:us-east-1:000000000000:topic-b"},
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	created := decodeBody(t, rec.Body.Bytes())
	assert.Len(t, created["eventTargets"].([]any), 2, "Create/Get/Update must return the full eventTargets array")
	_, hasCount := created["eventTargetCount"]
	assert.False(t, hasCount, "real Create/Get/UpdateEventBridgeRuleTemplateOutput has no eventTargetCount field")

	rec = doRequest(t, h, http.MethodGet, "/prod/eventbridge-rule-templates", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	items := decodeBody(t, rec.Body.Bytes())["eventBridgeRuleTemplates"].([]any)
	require.Len(t, items, 1)

	item := items[0].(map[string]any)
	assert.InDelta(t, float64(2), item["eventTargetCount"], 0)
	_, hasTargets := item["eventTargets"]
	assert.False(t, hasTargets, "List's Summary shape has eventTargetCount, not the full eventTargets array")
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
