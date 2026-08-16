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

func TestHandler_DeleteLogStream(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())

	doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"g"}`)
	doLogsRequest(t, h, e, "CreateLogStream", `{"logGroupName":"g","logStreamName":"s"}`)

	// Verify stream exists.
	rec := doLogsRequest(t, h, e, "DescribeLogStreams", `{"logGroupName":"g"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	var desc1 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &desc1))
	assert.Len(t, desc1["logStreams"], 1)

	// Delete it.
	rec = doLogsRequest(t, h, e, "DeleteLogStream", `{"logGroupName":"g","logStreamName":"s"}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify stream gone.
	rec = doLogsRequest(t, h, e, "DescribeLogStreams", `{"logGroupName":"g"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	var desc2 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &desc2))
	assert.Empty(t, desc2["logStreams"])

	// Delete non-existent stream → 404.
	rec = doLogsRequest(
		t,
		h,
		e,
		"DeleteLogStream",
		`{"logGroupName":"g","logStreamName":"nonexistent"}`,
	)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_CreateLogStream_EmptyNames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantCode int
	}{
		{
			name:     "empty_group_name",
			body:     `{"logGroupName":"","logStreamName":"my-stream"}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "empty_stream_name",
			body:     `{"logGroupName":"/my/group","logStreamName":""}`,
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := makeLogsRequest(t, "CreateLogStream", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_LogStreamOperations(t *testing.T) {
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
			name: "CreateLogStream",
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"grp"}`)
			},
			action:   "CreateLogStream",
			body:     map[string]any{"logGroupName": "grp", "logStreamName": "stream"},
			wantCode: http.StatusOK,
		},
		{
			name:     "CreateLogStream/GroupNotFound",
			action:   "CreateLogStream",
			body:     map[string]any{"logGroupName": "nonexistent", "logStreamName": "stream"},
			wantCode: http.StatusNotFound,
		},
		{
			name: "CreateLogStream/AlreadyExists",
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"grp"}`)
				doLogsRequest(
					t,
					h,
					e,
					"CreateLogStream",
					`{"logGroupName":"grp","logStreamName":"dup"}`,
				)
			},
			action:   "CreateLogStream",
			body:     map[string]any{"logGroupName": "grp", "logStreamName": "dup"},
			wantCode: http.StatusConflict,
		},
		{
			name: "DescribeLogStreams",
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"grp"}`)
				doLogsRequest(
					t,
					h,
					e,
					"CreateLogStream",
					`{"logGroupName":"grp","logStreamName":"s1"}`,
				)
				doLogsRequest(
					t,
					h,
					e,
					"CreateLogStream",
					`{"logGroupName":"grp","logStreamName":"s2"}`,
				)
			},
			action:        "DescribeLogStreams",
			body:          map[string]any{"logGroupName": "grp"},
			wantCode:      http.StatusOK,
			wantListField: "logStreams",
			wantListLen:   2,
		},
		{
			name:     "DescribeLogStreams/GroupNotFound",
			action:   "DescribeLogStreams",
			body:     map[string]any{"logGroupName": "nonexistent"},
			wantCode: http.StatusNotFound,
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
