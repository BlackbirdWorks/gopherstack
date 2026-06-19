package scheduler_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateSchedule_ScheduleExpression_Validation asserts ValidationException for bad cron formats.
func TestCreateSchedule_ScheduleExpression_Validation(t *testing.T) {
	t.Parallel()

	validTarget := map[string]any{
		"Arn":     "arn:aws:lambda:us-east-1:123456789012:function:my-function",
		"RoleArn": "arn:aws:iam::123456789012:role/scheduler-role",
	}

	tests := []struct {
		name     string
		expr     string
		wantCode int
		wantType string
	}{
		{
			name:     "valid_rate_expression",
			expr:     "rate(5 minutes)",
			wantCode: http.StatusOK,
		},
		{
			name:     "valid_cron_6_fields",
			expr:     "cron(0 12 * * ? *)",
			wantCode: http.StatusOK,
		},
		{
			name:     "valid_at_expression",
			expr:     "at(2024-01-01T00:00:00)",
			wantCode: http.StatusOK,
		},
		{
			name:     "empty_expression_rejected",
			expr:     "",
			wantCode: http.StatusBadRequest,
			wantType: "ValidationException",
		},
		{
			name:     "unknown_prefix_rejected",
			expr:     "every(5 minutes)",
			wantCode: http.StatusBadRequest,
			wantType: "ValidationException",
		},
		{
			name:     "cron_missing_closing_paren_rejected",
			expr:     "cron(0 12 * * ?",
			wantCode: http.StatusBadRequest,
			wantType: "ValidationException",
		},
		{
			name:     "cron_too_few_fields_rejected",
			expr:     "cron(0 12 * *)",
			wantCode: http.StatusBadRequest,
			wantType: "ValidationException",
		},
		{
			name:     "cron_too_many_fields_rejected",
			expr:     "cron(0 12 * * ? * extra)",
			wantCode: http.StatusBadRequest,
			wantType: "ValidationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSchedulerHandler(t)

			rec := doSchedulerRequest(t, h, "CreateSchedule", map[string]any{
				"Name":               "sched-" + tt.name,
				"ScheduleExpression": tt.expr,
				"Target":             validTarget,
				"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantType != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.wantType, resp["__type"])
			}
		})
	}
}
