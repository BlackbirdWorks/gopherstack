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
				assert.NotEmpty(t, resp["Id"])
				assert.Equal(t, "SUCCEEDED", resp["Status"])
				assert.Equal(t, "NOT_DEPLOYED", resp["MonitorDeploymentStatus"])
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/prod/signal-maps", map[string]any{
				"name":                   "test-signal-map",
				"DiscoveryEntryPointArn": "arn:aws:medialive:us-east-1:000000000000:channel:abc123",
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
	id := created["Id"].(string)

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
	items := listResp["SignalMaps"].([]any)
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
	assert.Equal(t, "DEPLOYED", deployResp["MonitorDeploymentStatus"])

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
				assert.NotEmpty(t, resp["Id"])
				assert.Equal(t, "test-cw-group", resp["Name"])
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
	id := created["Id"].(string)

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
	assert.Equal(t, "cw-group-updated", updated["Name"])

	// List
	rec = doRequest(t, h, http.MethodGet, "/prod/cloudwatch-alarm-template-groups", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	items := listResp["CloudWatchAlarmTemplateGroups"].([]any)
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
				"name": "cw-template-1", "MetricName": "InputLossSeconds",
				"Namespace": "MediaLive", "Statistic": "Sum",
				"ComparisonOperator": "GreaterThanThreshold", "Threshold": 0.0,
				"EvaluationPeriods": 1.0, "Period": 300.0,
			},
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.NotEmpty(t, resp["Id"])
				assert.Equal(t, "InputLossSeconds", resp["MetricName"])
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
		"name": "cw-template-1", "MetricName": "OutputLossSeconds",
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	var created map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	id := created["Id"].(string)

	// Get by ID
	rec = doRequest(t, h, http.MethodGet, "/prod/cloudwatch-alarm-templates/"+id, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Get by Name
	rec = doRequest(t, h, http.MethodGet, "/prod/cloudwatch-alarm-templates/cw-template-1", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Update (PATCH)
	rec = doRequest(t, h, http.MethodPatch, "/prod/cloudwatch-alarm-templates/"+id, map[string]any{
		"MetricName": "ActiveAlerts",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var updated map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updated))
	assert.Equal(t, "ActiveAlerts", updated["MetricName"])

	// List
	rec = doRequest(t, h, http.MethodGet, "/prod/cloudwatch-alarm-templates", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	items := listResp["CloudWatchAlarmTemplates"].([]any)
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
	id := created["Id"].(string)
	assert.NotEmpty(t, id)

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
			"Description": "updated desc",
		},
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	// List
	rec = doRequest(t, h, http.MethodGet, "/prod/eventbridge-rule-template-groups", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Len(t, listResp["EventBridgeRuleTemplateGroups"].([]any), 1)

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
		"EventType": "MEDIALIVE_CHANNEL_STATE_CHANGE",
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	var created map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	id := created["Id"].(string)

	// Get by ID
	rec = doRequest(t, h, http.MethodGet, "/prod/eventbridge-rule-templates/"+id, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Update (PATCH)
	rec = doRequest(t, h, http.MethodPatch, "/prod/eventbridge-rule-templates/"+id, map[string]any{
		"EventType": "MEDIALIVE_MULTIPLEX_ALERT",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var updated map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updated))
	assert.Equal(t, "MEDIALIVE_MULTIPLEX_ALERT", updated["EventType"])

	// List
	rec = doRequest(t, h, http.MethodGet, "/prod/eventbridge-rule-templates", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Len(t, listResp["EventBridgeRuleTemplates"].([]any), 1)

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
				offerings := resp["Offerings"].([]any)
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
	assert.Equal(t, "87654321", resp["OfferingId"])

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
		"Count": 2.0,
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	var purchaseResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &purchaseResp))
	resv := purchaseResp["Reservation"].(map[string]any)
	reservationID := resv["ReservationId"].(string)
	assert.NotEmpty(t, reservationID)
	assert.Equal(t, "ACTIVE", resv["State"])
	assert.InDelta(t, float64(2), resv["Count"], 0.001)

	// Describe
	rec = doRequest(t, h, http.MethodGet, "/prod/reservations/"+reservationID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// List
	rec = doRequest(t, h, http.MethodGet, "/prod/reservations", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Len(t, listResp["Reservations"].([]any), 1)

	// Update name
	rec = doRequest(t, h, http.MethodPut, "/prod/reservations/"+reservationID, map[string]any{
		"name": "renamed-reservation",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var updatedResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updatedResp))
	assert.Equal(t, "renamed-reservation", updatedResp["Name"])

	// Delete (cancel)
	rec = doRequest(t, h, http.MethodDelete, "/prod/reservations/"+reservationID, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	var deletedResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &deletedResp))
	assert.Equal(t, "CANCELED", deletedResp["State"])

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
			body:     map[string]any{"ChannelIds": []any{}},
		},
		{
			name:     "batch stop channels",
			path:     "/prod/batch/stop",
			wantCode: http.StatusOK,
			body:     map[string]any{"ChannelIds": []any{}},
		},
		{
			name:     "batch delete channels",
			path:     "/prod/batch/delete",
			wantCode: http.StatusOK,
			body:     map[string]any{"ChannelIds": []any{}},
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
		"ChannelIds": []any{chID},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var startResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
	successful := startResp["Successful"].([]any)
	assert.Len(t, successful, 1)
	assert.Equal(t, chID, successful[0].(map[string]any)["Id"])

	// Batch stop
	rec = doRequest(t, h, http.MethodPost, "/prod/batch/stop", map[string]any{
		"ChannelIds": []any{chID},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Batch delete (channel must be idle)
	rec = doRequest(t, h, http.MethodPost, "/prod/batch/delete", map[string]any{
		"ChannelIds": []any{chID},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var delResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &delResp))
	delSuccessful := delResp["Successful"].([]any)
	assert.Len(t, delSuccessful, 1)
}

func TestBatch_StartNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/prod/batch/start", map[string]any{
		"ChannelIds": []any{"nonexistent"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	failed := resp["Failed"].([]any)
	assert.Len(t, failed, 1)
}

func TestBatchUpdateSchedule(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	chID := createTestChannel(t, h)

	// Add schedule actions
	rec := doRequest(t, h, http.MethodPut, "/prod/channels/"+chID+"/schedule", map[string]any{
		"Creates": map[string]any{
			"ScheduleActions": []any{
				map[string]any{"ActionName": "start-at-midnight"},
			},
		},
		"Deletes": map[string]any{"ActionNames": []any{}},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	creates := resp["Creates"].(map[string]any)["ScheduleActions"].([]any)
	assert.Len(t, creates, 1)

	// Delete the action
	rec = doRequest(t, h, http.MethodPut, "/prod/channels/"+chID+"/schedule", map[string]any{
		"Creates": map[string]any{"ScheduleActions": []any{}},
		"Deletes": map[string]any{"ActionNames": []any{"start-at-midnight"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)
}
