package cloudwatchlogs_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_ScheduledQuery_Create(t *testing.T) {
	t.Parallel()

	e := echo.New()
	backend := cloudwatchlogs.NewInMemoryBackend()
	h := cloudwatchlogs.NewHandler(backend)

	rec := doLogsRequest(
		t,
		h,
		e,
		"CreateScheduledQuery",
		`{"name":"my-sched","queryString":"fields @message | limit 100",`+
			`"scheduleExpression":"cron(0 * * * ? *)","executionRoleArn":"arn:aws:iam::123:role/r","state":"DISABLED"}`,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	queryARN, ok := out["scheduledQueryArn"].(string)
	require.True(t, ok)
	assert.Contains(t, queryARN, "scheduled-query")
	assert.Equal(t, "DISABLED", out["state"])
}

func TestHandler_CreateScheduledQuery_StateValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantCode int
	}{
		{
			name: "enabled_state_ok",
			body: `{"name":"q","queryString":"fields @message","scheduleExpression":"cron(0 * * * ? *)",` +
				`"state":"ENABLED"}`,
			wantCode: http.StatusOK,
		},
		{
			name: "disabled_state_ok",
			body: `{"name":"q2","queryString":"fields @message","scheduleExpression":"cron(0 * * * ? *)",` +
				`"state":"DISABLED"}`,
			wantCode: http.StatusOK,
		},
		{
			name: "invalid_state_fails",
			body: `{"name":"q3","queryString":"fields @message","scheduleExpression":"cron(0 * * * ? *)",` +
				`"state":"ACTIVE"}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name: "empty_state_defaults_to_enabled",
			body: `{"name":"q4","queryString":"fields @message","scheduleExpression":"cron(0 * * * ? *)",` +
				`"state":""}`,
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := makeLogsRequest(t, "CreateScheduledQuery", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_CreateScheduledQueryOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo)
		body     map[string]any
		name     string
		action   string
		wantKey  string
		wantVal  string
		wantCode int
	}{
		// CreateScheduledQuery
		{
			name:   "CreateScheduledQuery/OK",
			action: "CreateScheduledQuery",
			body: map[string]any{
				"name":               "my-query",
				"queryString":        "fields @timestamp | sort @timestamp desc",
				"scheduleExpression": "cron(0 * * * ? *)",
				"executionRoleArn":   "arn:aws:iam::123:role/role",
			},
			wantCode: http.StatusOK,
			wantKey:  "scheduledQueryArn",
		},
		{
			name:     "CreateScheduledQuery/DefaultStateEnabled",
			action:   "CreateScheduledQuery",
			body:     map[string]any{"name": "q2", "queryString": "fields @message"},
			wantCode: http.StatusOK,
			wantKey:  "state",
			wantVal:  "ENABLED",
		},
		{
			name:     "CreateScheduledQuery/MissingName",
			action:   "CreateScheduledQuery",
			body:     map[string]any{"queryString": "fields @message"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "CreateScheduledQuery/MissingQueryString",
			action:   "CreateScheduledQuery",
			body:     map[string]any{"name": "my-query"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			backend := cloudwatchlogs.NewInMemoryBackend()
			h := cloudwatchlogs.NewHandler(backend)

			if tt.setup != nil {
				tt.setup(t, h, e)
			}

			bodyBytes, err := json.Marshal(tt.body)
			require.NoError(t, err)

			rec := doLogsRequest(t, h, e, tt.action, string(bodyBytes))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK && tt.wantKey != "" {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				if tt.wantVal != "" {
					assert.Equal(t, tt.wantVal, out[tt.wantKey])
				} else {
					assert.NotEmpty(t, out[tt.wantKey], "expected non-empty %s", tt.wantKey)
				}
			}
		})
	}
}
