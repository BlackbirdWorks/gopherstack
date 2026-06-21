package scheduler_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_UpdateScheduleRequiredFieldValidation verifies that UpdateSchedule
// enforces the same required-field rules as CreateSchedule: ScheduleExpression,
// Target.Arn, Target.RoleArn, and FlexibleTimeWindow.Mode are all mandatory.
// The previous implementation wrapped each check in "if non-empty" guards, so
// omitting a required field silently zeroed it out in the stored schedule.
func TestParity_UpdateScheduleRequiredFieldValidation(t *testing.T) {
	t.Parallel()

	validBody := map[string]any{
		"Name":               "test-sched",
		"ScheduleExpression": "rate(5 minutes)",
		"Target":             map[string]string{"Arn": "arn:a", "RoleArn": "arn:r"},
		"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
	}

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "valid_update_accepted",
			body:     validBody,
			wantCode: http.StatusOK,
		},
		{
			name: "missing_schedule_expression_rejected",
			body: map[string]any{
				"Name":               "test-sched",
				"Target":             map[string]string{"Arn": "arn:a", "RoleArn": "arn:r"},
				"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing_target_arn_rejected",
			body: map[string]any{
				"Name":               "test-sched",
				"ScheduleExpression": "rate(5 minutes)",
				"Target":             map[string]string{"RoleArn": "arn:r"},
				"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing_target_role_arn_rejected",
			body: map[string]any{
				"Name":               "test-sched",
				"ScheduleExpression": "rate(5 minutes)",
				"Target":             map[string]string{"Arn": "arn:a"},
				"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing_flexible_time_window_mode_rejected",
			body: map[string]any{
				"Name":               "test-sched",
				"ScheduleExpression": "rate(5 minutes)",
				"Target":             map[string]string{"Arn": "arn:a", "RoleArn": "arn:r"},
				"FlexibleTimeWindow": map[string]any{},
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSchedulerHandler(t)

			// Seed the schedule so update has something to update.
			createRec := doSchedulerRequest(t, h, "CreateSchedule", validBody)
			require.Equal(t, http.StatusOK, createRec.Code, "failed to create seed schedule")

			rec := doSchedulerRequest(t, h, "UpdateSchedule", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code, "UpdateSchedule body=%v", tt.body)

			if tt.wantCode == http.StatusBadRequest {
				var errResp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
				assert.Equal(t, "ValidationException", errResp["__type"])
			}
		})
	}
}

// TestParity_UpdateScheduleDoesNotBlankFields verifies that a valid UpdateSchedule
// replaces the stored schedule correctly (not zeroing fields from prior state).
func TestParity_UpdateScheduleDoesNotBlankFields(t *testing.T) {
	t.Parallel()

	h := newTestSchedulerHandler(t)

	createRec := doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
		"Name":               "blank-test",
		"ScheduleExpression": "rate(1 minute)",
		"Target":             map[string]string{"Arn": "arn:orig", "RoleArn": "arn:role-orig"},
		"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	updateRec := doSchedulerRequest(t, h, "UpdateSchedule", map[string]any{
		"Name":               "blank-test",
		"ScheduleExpression": "rate(10 minutes)",
		"Target":             map[string]string{"Arn": "arn:new", "RoleArn": "arn:role-new"},
		"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	getRec := doSchedulerRequest(t, h, "GetSchedule", map[string]any{"Name": "blank-test"})
	require.Equal(t, http.StatusOK, getRec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(getRec.Body).Decode(&out))

	assert.Equal(t, "rate(10 minutes)", out["ScheduleExpression"])

	target, _ := out["Target"].(map[string]any)
	assert.Equal(t, "arn:new", target["Arn"])
	assert.Equal(t, "arn:role-new", target["RoleArn"])
}
