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
		`{"name":"my-sched","queryString":"fields @message | limit 100","queryLanguage":"CWLI",`+
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

// TestHandler_GetScheduledQuery_WireShape locks the AWS wire key for a
// scheduled query's ARN: aws-sdk-go-v2 GetScheduledQueryOutput.ScheduledQueryArn
// serializes to "scheduledQueryArn", not "arn". A previous version of the
// ScheduledQuery model used the wrong key, so a real SDK client's
// ScheduledQueryArn field would always deserialize empty from
// GetScheduledQuery/ListScheduledQueries responses. It also locks that
// GetScheduledQueryOutput's members sit flat at the response's top level --
// a previous revision wrapped them under a "scheduledQuery" key that has no
// real wire representation at all.
func TestHandler_GetScheduledQuery_WireShape(t *testing.T) {
	t.Parallel()

	e := echo.New()
	backend := cloudwatchlogs.NewInMemoryBackend()
	h := cloudwatchlogs.NewHandler(backend)

	createRec := doLogsRequest(t, h, e, "CreateScheduledQuery",
		`{"name":"my-sched","queryString":"fields @message | limit 100","queryLanguage":"CWLI",`+
			`"scheduleExpression":"cron(0 * * * ? *)","executionRoleArn":"arn:aws:iam::123:role/r"}`)
	require.Equal(t, http.StatusOK, createRec.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))
	queryARN, ok := createOut["scheduledQueryArn"].(string)
	require.True(t, ok)
	require.NotEmpty(t, queryARN)

	getRec := doLogsRequest(t, h, e, "GetScheduledQuery", `{"scheduledQueryArn":"`+queryARN+`"}`)
	require.Equal(t, http.StatusOK, getRec.Code)

	var sq map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &sq))

	assert.Equal(t, queryARN, sq["scheduledQueryArn"], "wire key must be scheduledQueryArn, not arn")
	_, hasOldKey := sq["arn"]
	assert.False(t, hasOldKey, "bare \"arn\" key must not appear on a scheduled query")
	_, hasWrapper := sq["scheduledQuery"]
	assert.False(t, hasWrapper, "GetScheduledQueryOutput members must be flat, not nested under scheduledQuery")
	assert.Equal(t, "arn:aws:iam::123:role/r", sq["executionRoleArn"])
	assert.Equal(t, "CWLI", sq["queryLanguage"])
	assert.Equal(t, "CUSTOMER_MANAGED", sq["scheduleType"])
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
			body: `{"name":"q","queryString":"fields @message","queryLanguage":"CWLI",` +
				`"scheduleExpression":"cron(0 * * * ? *)","executionRoleArn":"arn:aws:iam::123:role/r",` +
				`"state":"ENABLED"}`,
			wantCode: http.StatusOK,
		},
		{
			name: "disabled_state_ok",
			body: `{"name":"q2","queryString":"fields @message","queryLanguage":"CWLI",` +
				`"scheduleExpression":"cron(0 * * * ? *)","executionRoleArn":"arn:aws:iam::123:role/r",` +
				`"state":"DISABLED"}`,
			wantCode: http.StatusOK,
		},
		{
			name: "invalid_state_fails",
			body: `{"name":"q3","queryString":"fields @message","queryLanguage":"CWLI",` +
				`"scheduleExpression":"cron(0 * * * ? *)","executionRoleArn":"arn:aws:iam::123:role/r",` +
				`"state":"ACTIVE"}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name: "empty_state_defaults_to_enabled",
			body: `{"name":"q4","queryString":"fields @message","queryLanguage":"CWLI",` +
				`"scheduleExpression":"cron(0 * * * ? *)","executionRoleArn":"arn:aws:iam::123:role/r",` +
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
				"queryLanguage":      "CWLI",
				"scheduleExpression": "cron(0 * * * ? *)",
				"executionRoleArn":   "arn:aws:iam::123:role/role",
			},
			wantCode: http.StatusOK,
			wantKey:  "scheduledQueryArn",
		},
		{
			name:   "CreateScheduledQuery/DefaultStateEnabled",
			action: "CreateScheduledQuery",
			body: map[string]any{
				"name":               "q2",
				"queryString":        "fields @message",
				"queryLanguage":      "CWLI",
				"scheduleExpression": "cron(0 * * * ? *)",
				"executionRoleArn":   "arn:aws:iam::123:role/role",
			},
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
