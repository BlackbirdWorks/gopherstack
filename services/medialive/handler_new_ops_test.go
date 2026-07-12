package medialive_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Signal Map tests ---

func TestSignalMap_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check    func(t *testing.T, body []byte)
		name     string
		wantCode int
	}{
		{
			name:     "create returns 201 with id and SUCCEEDED status",
			wantCode: http.StatusCreated,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.NotEmpty(t, resp["id"])
				assert.Equal(t, "SUCCEEDED", resp["status"])
				assert.Equal(t, "NOT_DEPLOYED", resp["monitorDeploymentStatus"])
				assert.NotEmpty(t, resp["createdAt"])
				assert.NotEmpty(t, resp["modifiedAt"])
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/prod/signal-maps", map[string]any{
				"name":                   "test-signal-map",
				"discoveryEntryPointArn": "arn:aws:medialive:us-east-1:000000000000:channel:abc123",
			})
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestSignalMap_GetListDelete(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	// Create
	rec := doRequest(t, h, http.MethodPost, "/prod/signal-maps", map[string]any{
		"name": "sig-map-1",
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	var created map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	id := created["id"].(string)

	// Get by ID
	rec = doRequest(t, h, http.MethodGet, "/prod/signal-maps/"+id, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Get by Name
	rec = doRequest(t, h, http.MethodGet, "/prod/signal-maps/sig-map-1", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// List
	rec = doRequest(t, h, http.MethodGet, "/prod/signal-maps", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	items := listResp["signalMaps"].([]any)
	assert.Len(t, items, 1)

	// StartUpdateSignalMap (PATCH)
	rec = doRequest(t, h, http.MethodPatch, "/prod/signal-maps/"+id, map[string]any{
		"name": "sig-map-updated",
	})
	assert.Equal(t, http.StatusAccepted, rec.Code)

	// StartMonitorDeployment
	rec = doRequest(t, h, http.MethodPost, "/prod/signal-maps/"+id+"/monitor-deployment", nil)
	require.Equal(t, http.StatusAccepted, rec.Code)
	var deployResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &deployResp))
	assert.Equal(t, "DEPLOYED", deployResp["monitorDeploymentStatus"])

	// Delete
	rec = doRequest(t, h, http.MethodDelete, "/prod/signal-maps/"+id, nil)
	assert.Equal(t, http.StatusAccepted, rec.Code)

	// Get after delete returns 404
	rec = doRequest(t, h, http.MethodGet, "/prod/signal-maps/"+id, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- CloudWatch Alarm Template Group tests ---

func TestCWAlarmTemplateGroup_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		check    func(t *testing.T, body []byte)
		name     string
		wantCode int
	}{
		{
			name:     "create returns 201 with id and name",
			wantCode: http.StatusCreated,
			body:     map[string]any{"name": "test-cw-group"},
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.NotEmpty(t, resp["id"])
				assert.Equal(t, "test-cw-group", resp["name"])
				assert.NotEmpty(t, resp["createdAt"])
				assert.NotEmpty(t, resp["modifiedAt"])
			},
		},
		{
			name:     "create without name returns 400",
			wantCode: http.StatusBadRequest,
			body:     map[string]any{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(
				t,
				h,
				http.MethodPost,
				"/prod/cloudwatch-alarm-template-groups",
				tc.body,
			)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestCWAlarmTemplateGroup_GetUpdateListDelete(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(
		t,
		h,
		http.MethodPost,
		"/prod/cloudwatch-alarm-template-groups",
		map[string]any{
			"name": "cw-group-1",
		},
	)
	require.Equal(t, http.StatusCreated, rec.Code)
	var created map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	id := created["id"].(string)

	// Get
	rec = doRequest(t, h, http.MethodGet, "/prod/cloudwatch-alarm-template-groups/"+id, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Update (PATCH)
	rec = doRequest(
		t,
		h,
		http.MethodPatch,
		"/prod/cloudwatch-alarm-template-groups/"+id,
		map[string]any{
			"name": "cw-group-updated",
		},
	)
	require.Equal(t, http.StatusOK, rec.Code)
	var updated map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updated))
	assert.Equal(t, "cw-group-updated", updated["name"])

	// List
	rec = doRequest(t, h, http.MethodGet, "/prod/cloudwatch-alarm-template-groups", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	items := listResp["cloudWatchAlarmTemplateGroups"].([]any)
	assert.Len(t, items, 1)

	// Delete (204)
	rec = doRequest(t, h, http.MethodDelete, "/prod/cloudwatch-alarm-template-groups/"+id, nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Get after delete returns 404
	rec = doRequest(t, h, http.MethodGet, "/prod/cloudwatch-alarm-template-groups/"+id, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- CloudWatch Alarm Template tests ---

func TestCWAlarmTemplate_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		check    func(t *testing.T, body []byte)
		name     string
		wantCode int
	}{
		{
			name:     "create returns 201 with metric fields",
			wantCode: http.StatusCreated,
			body: map[string]any{
				"name": "cw-template-1", "metricName": "InputLossSeconds",
				"namespace": "MediaLive", "statistic": "Sum",
				"comparisonOperator": "GreaterThanThreshold", "threshold": 0.0,
				"evaluationPeriods": 1.0, "period": 300.0,
			},
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.NotEmpty(t, resp["id"])
				assert.Equal(t, "InputLossSeconds", resp["metricName"])
				assert.NotEmpty(t, resp["createdAt"])
				assert.NotEmpty(t, resp["modifiedAt"])
				_, hasNamespace := resp["namespace"]
				assert.False(t, hasNamespace, "real GetCloudWatchAlarmTemplateOutput has no namespace field")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/prod/cloudwatch-alarm-templates", tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestCWAlarmTemplate_GetUpdateListDelete(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/prod/cloudwatch-alarm-templates", map[string]any{
		"name": "cw-template-1", "metricName": "OutputLossSeconds",
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	var created map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	id := created["id"].(string)

	// Get by ID
	rec = doRequest(t, h, http.MethodGet, "/prod/cloudwatch-alarm-templates/"+id, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Get by Name
	rec = doRequest(t, h, http.MethodGet, "/prod/cloudwatch-alarm-templates/cw-template-1", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Update (PATCH)
	rec = doRequest(t, h, http.MethodPatch, "/prod/cloudwatch-alarm-templates/"+id, map[string]any{
		"metricName": "ActiveAlerts",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var updated map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updated))
	assert.Equal(t, "ActiveAlerts", updated["metricName"])

	// List
	rec = doRequest(t, h, http.MethodGet, "/prod/cloudwatch-alarm-templates", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	items := listResp["cloudWatchAlarmTemplates"].([]any)
	assert.Len(t, items, 1)

	// Delete
	rec = doRequest(t, h, http.MethodDelete, "/prod/cloudwatch-alarm-templates/"+id, nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

// --- EventBridge Rule Template Group tests ---

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

// --- EventBridge Rule Template tests ---

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

// --- Offering tests ---

func TestOfferings_ListDescribe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check    func(t *testing.T, body []byte)
		name     string
		wantCode int
	}{
		{
			name:     "list returns seeded offerings",
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				offerings := resp["offerings"].([]any)
				assert.GreaterOrEqual(t, len(offerings), 3)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodGet, "/prod/offerings", nil)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestOfferings_Describe(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/prod/offerings/87654321", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "87654321", resp["offeringId"])
	assert.NotEmpty(t, resp["region"])
	_, hasName := resp["name"]
	assert.False(t, hasName, "real DescribeOfferingOutput has no name field")

	// Unknown offering
	rec = doRequest(t, h, http.MethodGet, "/prod/offerings/99999999", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- Reservation tests ---

func TestReservations_PurchaseListDescribeDeleteUpdate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Purchase
	rec := doRequest(t, h, http.MethodPost, "/prod/offerings/87654321/purchase", map[string]any{
		"name":  "test-reservation",
		"count": 2.0,
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	var purchaseResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &purchaseResp))
	resv := purchaseResp["reservation"].(map[string]any)
	reservationID := resv["reservationId"].(string)
	assert.NotEmpty(t, reservationID)
	assert.Equal(t, "ACTIVE", resv["state"])
	assert.InDelta(t, float64(2), resv["count"], 0.001)

	// Describe
	rec = doRequest(t, h, http.MethodGet, "/prod/reservations/"+reservationID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// List
	rec = doRequest(t, h, http.MethodGet, "/prod/reservations", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Len(t, listResp["reservations"].([]any), 1)

	// Update name
	rec = doRequest(t, h, http.MethodPut, "/prod/reservations/"+reservationID, map[string]any{
		"name": "renamed-reservation",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var updatedResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updatedResp))
	updatedResv := updatedResp["reservation"].(map[string]any)
	assert.Equal(t, "renamed-reservation", updatedResv["name"])

	// Delete (cancel) -- DeleteReservationOutput is NOT wrapped in "reservation".
	rec = doRequest(t, h, http.MethodDelete, "/prod/reservations/"+reservationID, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	var deletedResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &deletedResp))
	assert.Equal(t, "CANCELED", deletedResp["state"])

	// Describe after delete returns 404
	rec = doRequest(t, h, http.MethodGet, "/prod/reservations/"+reservationID, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- Batch ops tests ---

func TestBatch_StartStopDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
		path     string
		wantCode int
	}{
		{
			name:     "batch start channels",
			path:     "/prod/batch/start",
			wantCode: http.StatusOK,
			body:     map[string]any{"channelIds": []any{}},
		},
		{
			name:     "batch stop channels",
			path:     "/prod/batch/stop",
			wantCode: http.StatusOK,
			body:     map[string]any{"channelIds": []any{}},
		},
		{
			name:     "batch delete channels",
			path:     "/prod/batch/delete",
			wantCode: http.StatusOK,
			body:     map[string]any{"channelIds": []any{}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, tc.path, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

func TestBatch_StartStopKnownChannels(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	chID := createTestChannel(t, h)

	// Batch start
	rec := doRequest(t, h, http.MethodPost, "/prod/batch/start", map[string]any{
		"channelIds": []any{chID},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var startResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
	successful := startResp["successful"].([]any)
	assert.Len(t, successful, 1)
	assert.Equal(t, chID, successful[0].(map[string]any)["id"])

	// Batch stop
	rec = doRequest(t, h, http.MethodPost, "/prod/batch/stop", map[string]any{
		"channelIds": []any{chID},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Batch delete (channel must be idle)
	rec = doRequest(t, h, http.MethodPost, "/prod/batch/delete", map[string]any{
		"channelIds": []any{chID},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var delResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &delResp))
	delSuccessful := delResp["successful"].([]any)
	assert.Len(t, delSuccessful, 1)
}

func TestBatch_StartNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/prod/batch/start", map[string]any{
		"channelIds": []any{"nonexistent"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	failed := resp["failed"].([]any)
	assert.Len(t, failed, 1)
}

// TestBatch_DeleteInputSecurityGroups exercises the bug fix documented in
// PARITY.md: BatchDeleteInput has an inputSecurityGroupIds field that the
// handler previously never parsed.
func TestBatch_DeleteInputSecurityGroups(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/prod/inputSecurityGroups", map[string]any{})
	require.Equal(t, http.StatusCreated, rec.Code)
	var created map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	groupID := created["securityGroup"].(map[string]any)["id"].(string)

	rec = doRequest(t, h, http.MethodPost, "/prod/batch/delete", map[string]any{
		"inputSecurityGroupIds": []any{groupID},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	successful := resp["successful"].([]any)
	require.Len(t, successful, 1)
	assert.Equal(t, groupID, successful[0].(map[string]any)["id"])

	rec = doRequest(t, h, http.MethodGet, "/prod/inputSecurityGroups/"+groupID, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestBatchUpdateSchedule(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	chID := createTestChannel(t, h)

	// Add schedule actions
	rec := doRequest(t, h, http.MethodPut, "/prod/channels/"+chID+"/schedule", map[string]any{
		"creates": map[string]any{
			"scheduleActions": []any{
				map[string]any{"actionName": "start-at-midnight"},
			},
		},
		"deletes": map[string]any{"actionNames": []any{}},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	creates := resp["creates"].(map[string]any)["scheduleActions"].([]any)
	assert.Len(t, creates, 1)

	// Delete the action
	rec = doRequest(t, h, http.MethodPut, "/prod/channels/"+chID+"/schedule", map[string]any{
		"creates": map[string]any{"scheduleActions": []any{}},
		"deletes": map[string]any{"actionNames": []any{"start-at-midnight"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)
}
