package opensearch_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opensearch"
)

func TestScheduledActions_ListForDomain(t *testing.T) {
	t.Parallel()

	b, h := newTestHandlerAndBackend()

	opensearch.AddScheduledActionInternal(b, "sched-domain", &opensearch.ScheduledAction{
		ID:            "action-1",
		Type:          "JVM_HEAP_SIZE_TUNING",
		Severity:      "MEDIUM",
		ScheduledBy:   "SYSTEM",
		Status:        "PENDING_UPDATE",
		ScheduledTime: 1_800_000_000,
	})

	// List scoped to the domain that has the action.
	resp := doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/domain/sched-domain/scheduledActions", nil)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	actions, ok := out["ScheduledActions"].([]any)
	require.True(t, ok)
	require.Len(t, actions, 1)
	assert.Equal(t, "action-1", actions[0].(map[string]any)["Id"])

	// A different domain has no scheduled actions.
	otherResp := doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/domain/other-domain/scheduledActions", nil)
	defer otherResp.Body.Close()
	require.Equal(t, http.StatusOK, otherResp.StatusCode)

	var otherOut map[string]any
	require.NoError(t, json.NewDecoder(otherResp.Body).Decode(&otherOut))
	assert.Empty(t, otherOut["ScheduledActions"])
}

func TestScheduledActions_UpdateReschedulesExistingAction(t *testing.T) {
	t.Parallel()

	b, h := newTestHandlerAndBackend()

	opensearch.AddScheduledActionInternal(b, "upd-sched-domain", &opensearch.ScheduledAction{
		ID:            "action-upd",
		Type:          "SERVICE_SOFTWARE_UPDATE",
		Severity:      "HIGH",
		ScheduledBy:   "SYSTEM",
		Status:        "PENDING_UPDATE",
		ScheduledTime: 1_700_000_000,
	})

	resp := doRequest(t, h, http.MethodPut,
		"/2021-01-01/opensearch/domain/upd-sched-domain/scheduledAction/update",
		map[string]any{
			"ActionID":         "action-upd",
			"ActionType":       "SERVICE_SOFTWARE_UPDATE",
			"ScheduleAt":       "TIMESTAMP",
			"DesiredStartTime": 1_900_000_000,
		})
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	action, ok := out["ScheduledAction"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "action-upd", action["Id"])
	assert.InDelta(t, 1_900_000_000, action["ScheduledTime"], 0)
	assert.Equal(t, "CUSTOMER", action["ScheduledBy"])
}

func TestScheduledActions_UpdateUnknownActionReturnsNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp := doRequest(t, h, http.MethodPut,
		"/2021-01-01/opensearch/domain/no-such-action-domain/scheduledAction/update",
		map[string]any{
			"ActionID":   "nonexistent",
			"ActionType": "SERVICE_SOFTWARE_UPDATE",
			"ScheduleAt": "NOW",
		})
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestScheduledActions_UpdateMissingRequiredFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
	}{
		{name: "missing_action_id", body: map[string]any{"ActionType": "SERVICE_SOFTWARE_UPDATE", "ScheduleAt": "NOW"}},
		{name: "missing_action_type", body: map[string]any{"ActionID": "a1", "ScheduleAt": "NOW"}},
		{name: "missing_schedule_at", body: map[string]any{"ActionID": "a1", "ActionType": "SERVICE_SOFTWARE_UPDATE"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			resp := doRequest(t, h, http.MethodPut,
				"/2021-01-01/opensearch/domain/some-domain/scheduledAction/update", tt.body)
			defer resp.Body.Close()
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		})
	}
}
