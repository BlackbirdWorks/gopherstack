package cloudwatchlogs_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
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

	getRec := doLogsRequest(t, h, e, "GetScheduledQuery", `{"identifier":"`+queryARN+`"}`)
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

// TestHandler_ScheduledQuery_DestinationConfiguration locks the destination
// union shape (aws-sdk-go-v2 types.DestinationConfiguration,
// types.go:773): s3Configuration and lookupTableConfiguration are
// alternatives, neither required, and each must round-trip through
// Create/Get without the other's key appearing on the wire -- an unset
// member must be genuinely absent, not serialised as an empty object.
func TestHandler_ScheduledQuery_DestinationConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		destConfig string
		wantS3     bool
		wantLookup bool
	}{
		{
			name: "s3_destination_unchanged",
			destConfig: `"destinationConfiguration":{"s3Configuration":{` +
				`"destinationIdentifier":"arn:aws:s3:::bucket","roleArn":"arn:aws:iam::123:role/s3-role"}},`,
			wantS3: true,
		},
		{
			name: "lookup_table_destination",
			destConfig: `"destinationConfiguration":{"lookupTableConfiguration":{` +
				`"tableName":"my-table","roleArn":"arn:aws:iam::123:role/lookup-role",` +
				`"description":"a lookup table","kmsKeyId":"kms-key","tags":{"env":"prod"}}},`,
			wantLookup: true,
		},
		{
			name: "neither_destination_set",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			backend := cloudwatchlogs.NewInMemoryBackend()
			h := cloudwatchlogs.NewHandler(backend)

			body := `{` + tt.destConfig + `"name":"q","queryString":"fields @message","queryLanguage":"CWLI",` +
				`"scheduleExpression":"cron(0 * * * ? *)","executionRoleArn":"arn:aws:iam::123:role/r"}`

			createRec := doLogsRequest(t, h, e, "CreateScheduledQuery", body)
			require.Equal(t, http.StatusOK, createRec.Code)

			var createOut map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))
			queryARN, arnOK := createOut["scheduledQueryArn"].(string)
			require.True(t, arnOK)

			getRec := doLogsRequest(t, h, e, "GetScheduledQuery", `{"identifier":"`+queryARN+`"}`)
			require.Equal(t, http.StatusOK, getRec.Code)

			var sq map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &sq))

			destRaw, hasDest := sq["destinationConfiguration"]
			if !tt.wantS3 && !tt.wantLookup {
				assert.False(t, hasDest, "destinationConfiguration must be absent from the wire when unset")

				return
			}

			require.True(t, hasDest)
			dest, destOK := destRaw.(map[string]any)
			require.True(t, destOK)

			_, hasS3 := dest["s3Configuration"]
			_, hasLookup := dest["lookupTableConfiguration"]
			assert.Equal(t, tt.wantS3, hasS3)
			assert.Equal(t, tt.wantLookup, hasLookup)

			if tt.wantLookup {
				lookup, lookupOK := dest["lookupTableConfiguration"].(map[string]any)
				require.True(t, lookupOK)
				assert.Equal(t, "my-table", lookup["tableName"])
				assert.Equal(t, "arn:aws:iam::123:role/lookup-role", lookup["roleArn"])
				assert.Equal(t, "a lookup table", lookup["description"])
				assert.Equal(t, "kms-key", lookup["kmsKeyId"])
				assert.Equal(t, map[string]any{"env": "prod"}, lookup["tags"])
			}
		})
	}
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
