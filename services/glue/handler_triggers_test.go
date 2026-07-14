package glue_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

func triggerState(t *testing.T, h *glue.Handler, name string) string {
	t.Helper()

	rec := doGlueRequest(t, h, "GetTrigger", map[string]any{"Name": name})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Trigger struct {
			State string `json:"State"`
		} `json:"Trigger"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	return out.Trigger.State
}

func Test_StopTrigger_OnDemand_StaysCreated(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doGlueRequest(t, h, "CreateTrigger", map[string]any{"Name": "od-stop", "Type": "ON_DEMAND"})

	rec := doGlueRequest(t, h, "StopTrigger", map[string]any{"Name": "od-stop"})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "CREATED", triggerState(t, h, "od-stop"))
}

func Test_StartTrigger_Scheduled_StillActivates(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doGlueRequest(t, h, "CreateTrigger", map[string]any{"Name": "sched1", "Type": "SCHEDULED"})

	rec := doGlueRequest(t, h, "StartTrigger", map[string]any{"Name": "sched1"})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "ACTIVATED", triggerState(t, h, "sched1"))
}

func Test_CreateTrigger_StartOnCreation_ActivatesScheduledTrigger(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "CreateTrigger", map[string]any{
		"Name":            "auto-start",
		"Type":            "SCHEDULED",
		"Schedule":        "cron(0 12 * * ? *)",
		"StartOnCreation": true,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "ACTIVATED", triggerState(t, h, "auto-start"))
}

func Test_CreateTrigger_StartOnCreation_IgnoredForOnDemand(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "CreateTrigger", map[string]any{
		"Name":            "auto-start-od",
		"Type":            "ON_DEMAND",
		"StartOnCreation": true,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// AWS: "True is not supported for ON_DEMAND triggers" — they stay CREATED.
	assert.Equal(t, "CREATED", triggerState(t, h, "auto-start-od"))
}

func TestBatch3_Trigger_Create_OnDemand(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "on-demand trigger",
			body:     map[string]any{"Name": "trig-ondemand", "Type": "ON_DEMAND"},
			wantCode: http.StatusOK,
		},
		{
			name: "scheduled trigger",
			body: map[string]any{
				"Name":     "trig-sched",
				"Type":     "SCHEDULED",
				"Schedule": "cron(0 12 * * ? *)",
			},
			wantCode: http.StatusOK,
		},
		{
			name: "conditional trigger",
			body: map[string]any{
				"Name": "trig-cond",
				"Type": "CONDITIONAL",
				"Predicate": map[string]any{
					"Logical": "ANY",
					"Conditions": []map[string]any{
						{"JobName": "my-job", "LogicalOperator": "EQUALS", "State": "SUCCEEDED"},
					},
				},
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "duplicate trigger",
			body:     map[string]any{"Name": "trig-ondemand", "Type": "ON_DEMAND"},
			wantCode: http.StatusBadRequest,
		},
	}

	h := newTestHandler(t)
	// Create the first one to enable duplicate test.
	require.Equal(t, http.StatusOK,
		doGlueRequest(t, h, "CreateTrigger", map[string]any{"Name": "trig-ondemand", "Type": "ON_DEMAND"}).Code)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h2 := newTestHandler(t)
			if tt.name == "duplicate trigger" {
				// Pre-create to cause duplicate.
				doGlueRequest(t, h2, "CreateTrigger", map[string]any{"Name": "trig-ondemand", "Type": "ON_DEMAND"})
			}
			rec := doGlueRequest(t, h2, "CreateTrigger", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestBatch3_Trigger_GetTrigger(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		lookupKey string
		wantCode  int
		wantErr   bool
	}{
		{name: "found", lookupKey: "my-trigger", wantCode: http.StatusOK},
		{name: "not-found", lookupKey: "no-such-trigger", wantCode: http.StatusBadRequest, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doGlueRequest(t, h, "CreateTrigger", map[string]any{"Name": "my-trigger", "Type": "ON_DEMAND"})

			rec := doGlueRequest(t, h, "GetTrigger", map[string]any{"Name": tt.lookupKey})
			assert.Equal(t, tt.wantCode, rec.Code)
			if !tt.wantErr {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				trig, ok := out["Trigger"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "my-trigger", trig["Name"])
			}
		})
	}
}

func TestBatch3_Trigger_UpdateTrigger(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		trigName string
		wantCode int
	}{
		{name: "success", trigName: "upd-trigger", wantCode: http.StatusOK},
		{name: "not-found", trigName: "no-trigger", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doGlueRequest(t, h, "CreateTrigger", map[string]any{
				"Name":     "upd-trigger",
				"Type":     "SCHEDULED",
				"Schedule": "cron(0 0 * * ? *)",
			})

			rec := doGlueRequest(t, h, "UpdateTrigger", map[string]any{
				"Name": tt.trigName,
				"TriggerUpdate": map[string]any{
					"Schedule": "cron(0 6 * * ? *)",
				},
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestBatch3_Trigger_DeleteTrigger(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		trigName string
		wantCode int
	}{
		{name: "success", trigName: "del-trigger", wantCode: http.StatusOK},
		{name: "not-found", trigName: "no-trigger", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doGlueRequest(t, h, "CreateTrigger", map[string]any{"Name": "del-trigger", "Type": "ON_DEMAND"})

			rec := doGlueRequest(t, h, "DeleteTrigger", map[string]any{"Name": tt.trigName})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestBatch3_Trigger_StartStopActivate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		op        string
		trigName  string
		wantState string
		wantCode  int
	}{
		{name: "start-found", op: "StartTrigger", trigName: "t1", wantCode: http.StatusOK, wantState: "ACTIVATED"},
		{name: "stop-found", op: "StopTrigger", trigName: "t1", wantCode: http.StatusOK, wantState: "DEACTIVATED"},
		{name: "start-missing", op: "StartTrigger", trigName: "no-trigger", wantCode: http.StatusBadRequest},
		{name: "stop-missing", op: "StopTrigger", trigName: "no-trigger", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doGlueRequest(t, h, "CreateTrigger", map[string]any{"Name": "t1", "Type": "SCHEDULED"})

			rec := doGlueRequest(t, h, tt.op, map[string]any{"Name": tt.trigName})
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantCode == http.StatusOK && tt.wantState != "" {
				get := doGlueRequest(t, h, "GetTrigger", map[string]any{"Name": "t1"})
				var out map[string]any
				require.NoError(t, json.Unmarshal(get.Body.Bytes(), &out))
				trig := out["Trigger"].(map[string]any)
				assert.Equal(t, tt.wantState, trig["State"])
			}
		})
	}
}

func TestBatch3_Trigger_BatchGetAndList(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doGlueRequest(t, h, "CreateTrigger", map[string]any{"Name": "tr-a", "Type": "ON_DEMAND"})
	doGlueRequest(t, h, "CreateTrigger", map[string]any{"Name": "tr-b", "Type": "SCHEDULED"})

	t.Run("batch-get", func(t *testing.T) {
		t.Parallel()
		rec := doGlueRequest(t, h, "BatchGetTriggers", map[string]any{
			"TriggerNames": []string{"tr-a", "tr-b", "no-such"},
		})
		require.Equal(t, http.StatusOK, rec.Code)
		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		triggers := out["Triggers"].([]any)
		assert.Len(t, triggers, 2)
		missing := out["TriggersNotFound"].([]any)
		assert.Len(t, missing, 1)
	})

	t.Run("get-triggers", func(t *testing.T) {
		t.Parallel()
		rec := doGlueRequest(t, h, "GetTriggers", map[string]any{})
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "tr-a")
		assert.Contains(t, rec.Body.String(), "tr-b")
	})

	t.Run("list-triggers", func(t *testing.T) {
		t.Parallel()
		rec := doGlueRequest(t, h, "ListTriggers", map[string]any{})
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "tr-a")
	})
}

func TestBatch3_Trigger_WithActions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "CreateTrigger", map[string]any{
		"Name":     "trig-actions",
		"Type":     "SCHEDULED",
		"Schedule": "cron(0 0 * * ? *)",
		"Actions": []map[string]any{
			{
				"JobName":   "job-a",
				"Arguments": map[string]any{"--input": "s3://bucket/in"},
			},
			{
				"JobName": "job-b",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	getRec := doGlueRequest(t, h, "GetTrigger", map[string]any{"Name": "trig-actions"})
	require.Equal(t, http.StatusOK, getRec.Code)
	var out map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &out))
	trig := out["Trigger"].(map[string]any)
	actions := trig["Actions"].([]any)
	assert.Len(t, actions, 2)
}

func TestGlue_Triggers(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// CreateTrigger
	rec := doGlueRequest(t, h, "CreateTrigger", map[string]any{
		"Name":    "my-trigger",
		"Type":    "ON_DEMAND",
		"Actions": []map[string]any{{"JobName": "my-job"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// GetTrigger
	rec = doGlueRequest(t, h, "GetTrigger", map[string]any{
		"Name": "my-trigger",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// GetTriggers
	rec = doGlueRequest(t, h, "GetTriggers", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// UpdateTrigger
	rec = doGlueRequest(t, h, "UpdateTrigger", map[string]any{
		"Name":          "my-trigger",
		"TriggerUpdate": map[string]any{"Description": "updated"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// DeleteTrigger
	rec = doGlueRequest(t, h, "DeleteTrigger", map[string]any{
		"Name": "my-trigger",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}
