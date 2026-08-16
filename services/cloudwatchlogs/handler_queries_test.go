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

func TestHandler_InsightsWorkflow(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())

	// Set up log group, stream, and events.
	doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"/insights/test"}`)
	doLogsRequest(
		t,
		h,
		e,
		"CreateLogStream",
		`{"logGroupName":"/insights/test","logStreamName":"stream1"}`,
	)
	doLogsRequest(t, h, e, "PutLogEvents", `{
"logGroupName":"/insights/test","logStreamName":"stream1",
"logEvents":[
{"message":"ERROR: disk full","timestamp":1000},
{"message":"INFO: startup","timestamp":2000},
{"message":"ERROR: connection refused","timestamp":3000}
]
}`)

	// StartQuery with filter.
	rec := doLogsRequest(t, h, e, "StartQuery", `{
"logGroupName":"/insights/test",
"queryString":"fields @timestamp, @message | filter @message like /ERROR/",
"startTime":0,"endTime":0
}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var startOut map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startOut))
	queryID := startOut["queryId"]
	require.NotEmpty(t, queryID)

	// GetQueryResults.
	rec2 := doLogsRequest(t, h, e, "GetQueryResults", `{"queryId":"`+queryID+`"}`)
	require.Equal(t, http.StatusOK, rec2.Code)

	var resultsOut map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resultsOut))
	assert.Equal(t, "Complete", resultsOut["status"])
	rows, ok := resultsOut["results"].([]any)
	require.True(t, ok)
	assert.Len(t, rows, 2) // only 2 ERROR events

	// DescribeQueries lists the query.
	rec3 := doLogsRequest(t, h, e, "DescribeQueries", `{"logGroupName":"/insights/test"}`)
	require.Equal(t, http.StatusOK, rec3.Code)

	var descOut map[string]any
	require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &descOut))
	queries, ok := descOut["queries"].([]any)
	require.True(t, ok)
	assert.Len(t, queries, 1)

	// Put query into Running state so StopQuery can cancel it (AWS parity: StopQuery
	// returns InvalidOperationException if the query is already Complete).
	cloudwatchlogs.SetQueryStatusInternal(
		h.Backend.(*cloudwatchlogs.InMemoryBackend),
		queryID,
		cloudwatchlogs.QueryStatusRunning,
	)

	// StopQuery cancels the query.
	rec4 := doLogsRequest(t, h, e, "StopQuery", `{"queryId":"`+queryID+`"}`)
	require.Equal(t, http.StatusOK, rec4.Code)

	var stopOut map[string]any
	require.NoError(t, json.Unmarshal(rec4.Body.Bytes(), &stopOut))
	assert.Equal(t, true, stopOut["success"])

	// GetQueryResults now shows Cancelled.
	rec5 := doLogsRequest(t, h, e, "GetQueryResults", `{"queryId":"`+queryID+`"}`)
	require.Equal(t, http.StatusOK, rec5.Code)

	var resultsOut2 map[string]any
	require.NoError(t, json.Unmarshal(rec5.Body.Bytes(), &resultsOut2))
	assert.Equal(t, "Cancelled", resultsOut2["status"])
}

func TestHandler_InsightsQueryLanguage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		query       string
		wantResults int
	}{
		{
			name:        "fields_projection",
			query:       "fields @timestamp, @message",
			wantResults: 3,
		},
		{
			name:        "filter_regex",
			query:       "filter @message like /ERROR/",
			wantResults: 2,
		},
		{
			name:        "filter_limit",
			query:       "fields @message | limit 1",
			wantResults: 1,
		},
		{
			name:        "sort_desc",
			query:       "fields @timestamp | sort @timestamp desc",
			wantResults: 3,
		},
		{
			name:        "stats_count",
			query:       "stats count(*) by @message",
			wantResults: 3,
		},
		{
			name:        "empty_query",
			query:       "",
			wantResults: 3,
		},
		{
			name:        "unknown_command_ignored",
			query:       "parse @message \"* *\" as level, rest",
			wantResults: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())

			doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"/grp"}`)
			doLogsRequest(t, h, e, "CreateLogStream", `{"logGroupName":"/grp","logStreamName":"s"}`)
			doLogsRequest(t, h, e, "PutLogEvents", `{
"logGroupName":"/grp","logStreamName":"s",
"logEvents":[
{"message":"ERROR: one","timestamp":1000},
{"message":"INFO: two","timestamp":2000},
{"message":"ERROR: three","timestamp":3000}
]
}`)

			bodyJSON, merr := json.Marshal(map[string]any{
				"logGroupName": "/grp",
				"queryString":  tt.query,
				"startTime":    0,
				"endTime":      0,
			})
			require.NoError(t, merr)

			rec := doLogsRequest(t, h, e, "StartQuery", string(bodyJSON))
			require.Equal(t, http.StatusOK, rec.Code)

			var startOut map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startOut))
			queryID := startOut["queryId"]

			rec2 := doLogsRequest(t, h, e, "GetQueryResults", `{"queryId":"`+queryID+`"}`)
			require.Equal(t, http.StatusOK, rec2.Code)

			var resultsOut map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resultsOut))
			rows, ok := resultsOut["results"].([]any)
			require.True(t, ok, "results should be an array")
			assert.Len(t, rows, tt.wantResults)
		})
	}
}

func TestHandler_InsightsARNIdentifier(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())

	// Create log group named "/arn-test" and add events.
	doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"/arn-test"}`)
	doLogsRequest(t, h, e, "CreateLogStream", `{"logGroupName":"/arn-test","logStreamName":"s"}`)
	doLogsRequest(t, h, e, "PutLogEvents", `{
"logGroupName":"/arn-test","logStreamName":"s",
"logEvents":[{"message":"hello","timestamp":1000}]
}`)

	tests := []struct {
		body        map[string]any
		name        string
		wantResults int
	}{
		{
			name: "arn_identifier_resolves_to_name",
			body: map[string]any{
				"logGroupIdentifiers": []string{
					"arn:aws:logs:us-east-1:123456789012:log-group:/arn-test",
				},
				"queryString": "fields @message",
				"startTime":   0,
				"endTime":     0,
			},
			wantResults: 1,
		},
		{
			name: "plain_name_identifier",
			body: map[string]any{
				"logGroupIdentifiers": []string{"/arn-test"},
				"queryString":         "fields @message",
				"startTime":           0,
				"endTime":             0,
			},
			wantResults: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bodyBytes, err := json.Marshal(tt.body)
			require.NoError(t, err)

			rec := doLogsRequest(t, h, e, "StartQuery", string(bodyBytes))
			require.Equal(t, http.StatusOK, rec.Code)

			var startOut map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startOut))
			queryID := startOut["queryId"]
			require.NotEmpty(t, queryID)

			rec2 := doLogsRequest(t, h, e, "GetQueryResults", `{"queryId":"`+queryID+`"}`)
			require.Equal(t, http.StatusOK, rec2.Code)

			var resultsOut map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resultsOut))
			rows, ok := resultsOut["results"].([]any)
			require.True(t, ok)
			assert.Len(t, rows, tt.wantResults)
		})
	}
}

func TestHandler_DescribeQueries_Pagination(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())

	doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"/pg"}`)
	doLogsRequest(t, h, e, "CreateLogStream", `{"logGroupName":"/pg","logStreamName":"s"}`)

	// Start 3 queries.
	for i := range 3 {
		bodyBytes, err := json.Marshal(map[string]any{
			"logGroupName": "/pg",
			"queryString":  "fields @message",
			"startTime":    i,
			"endTime":      0,
		})
		require.NoError(t, err)

		rec := doLogsRequest(t, h, e, "StartQuery", string(bodyBytes))
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// First page: maxResults=2.
	rec := doLogsRequest(t, h, e, "DescribeQueries", `{"logGroupName":"/pg","maxResults":2}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var page1 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page1))
	page1Queries, ok := page1["queries"].([]any)
	require.True(t, ok, "page1 queries should be an array")
	assert.Len(t, page1Queries, 2)
	nextToken, _ := page1["nextToken"].(string)
	assert.NotEmpty(t, nextToken)

	// Second page.
	bodyBytes, err := json.Marshal(map[string]any{
		"logGroupName": "/pg",
		"maxResults":   2,
		"nextToken":    nextToken,
	})
	require.NoError(t, err)

	rec2 := doLogsRequest(t, h, e, "DescribeQueries", string(bodyBytes))
	require.Equal(t, http.StatusOK, rec2.Code)

	var page2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &page2))
	page2Queries, ok := page2["queries"].([]any)
	require.True(t, ok, "page2 queries should be an array")
	assert.Len(t, page2Queries, 1)
}

func TestHandler_InsightsInvalidJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		action string
	}{
		{name: "StartQuery", action: "StartQuery"},
		{name: "GetQueryResults", action: "GetQueryResults"},
		{name: "StopQuery", action: "StopQuery"},
		{name: "DescribeQueries", action: "DescribeQueries"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := makeLogsRequest(t, tt.action, "not-valid-json")
			assert.Equal(t, http.StatusInternalServerError, rec.Code)
		})
	}
}

func TestHandler_QueryOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup             func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo)
		body              map[string]any
		name              string
		action            string
		wantListField     string
		wantNotEmptyField string
		wantCode          int
		wantListLen       int
	}{
		{
			name: "StartQuery",
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"qgrp"}`)
			},
			action: "StartQuery",
			body: map[string]any{
				"logGroupName": "qgrp",
				"queryString":  "fields @timestamp, @message",
				"startTime":    0,
				"endTime":      0,
			},
			wantCode:          http.StatusOK,
			wantNotEmptyField: "queryId",
		},
		{
			name: "StartQuery/MultipleGroups",
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"grp1"}`)
				doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"grp2"}`)
			},
			action: "StartQuery",
			body: map[string]any{
				"logGroupNames": []string{"grp1", "grp2"},
				"queryString":   "fields @message",
				"startTime":     0,
				"endTime":       0,
			},
			wantCode:          http.StatusOK,
			wantNotEmptyField: "queryId",
		},
		{
			name:     "StartQuery/InvalidQuery",
			action:   "StartQuery",
			body:     map[string]any{"logGroupName": "grp", "queryString": "limit notanumber"},
			wantCode: http.StatusInternalServerError,
		},
		{
			name:     "GetQueryResults/NotFound",
			action:   "GetQueryResults",
			body:     map[string]any{"queryId": "no-such-id"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "StopQuery/NotFound",
			action:   "StopQuery",
			body:     map[string]any{"queryId": "no-such-id"},
			wantCode: http.StatusNotFound,
		},
		{
			name:          "DescribeQueries/Empty",
			action:        "DescribeQueries",
			body:          map[string]any{},
			wantCode:      http.StatusOK,
			wantListField: "queries",
			wantListLen:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())

			if tt.setup != nil {
				tt.setup(t, h, e)
			}

			bodyBytes, err := json.Marshal(tt.body)
			require.NoError(t, err)

			rec := doLogsRequest(t, h, e, tt.action, string(bodyBytes))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantListField != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Len(t, resp[tt.wantListField].([]any), tt.wantListLen)
			}

			if tt.wantNotEmptyField != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp[tt.wantNotEmptyField])
			}
		})
	}
}
