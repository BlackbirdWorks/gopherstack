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

func TestHandler_ExportTask_CancelRoundTrip(t *testing.T) {
	t.Parallel()

	e := echo.New()
	backend := cloudwatchlogs.NewInMemoryBackend()
	h := cloudwatchlogs.NewHandler(backend)

	// Create an export task.
	rec := doLogsRequest(t, h, e, "CreateExportTask",
		`{"logGroupName":"/my/group","destination":"my-bucket","from":1000,"to":2000}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))
	taskID, ok := createOut["taskId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, taskID)

	// Cancel the task.
	bodyBytes, err := json.Marshal(map[string]any{"taskId": taskID})
	require.NoError(t, err)
	rec = doLogsRequest(t, h, e, "CancelExportTask", string(bodyBytes))
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_ImportTask_CancelRoundTrip(t *testing.T) {
	t.Parallel()

	e := echo.New()
	backend := cloudwatchlogs.NewInMemoryBackend()
	h := cloudwatchlogs.NewHandler(backend)

	// Create an import task.
	createBody := `{"importRoleArn":"arn:aws:iam::123:role/my-role",` +
		`"importSourceArn":"arn:aws:cloudtrail:us-east-1:123:eventdatastore/abc"}`
	rec := doLogsRequest(t, h, e, "CreateImportTask", createBody)
	require.Equal(t, http.StatusOK, rec.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))
	importID, ok := createOut["importId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, importID)

	importDestARN, ok := createOut["importDestinationArn"].(string)
	require.True(t, ok)
	require.NotEmpty(t, importDestARN)

	// Cancel the task.
	bodyBytes, err := json.Marshal(map[string]any{"importId": importID})
	require.NoError(t, err)
	rec = doLogsRequest(t, h, e, "CancelImportTask", string(bodyBytes))
	require.Equal(t, http.StatusOK, rec.Code)

	var cancelOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cancelOut))
	assert.Equal(t, importID, cancelOut["importId"])
	assert.Equal(t, "CANCELLED", cancelOut["importStatus"])
}

func TestHandler_CancelExportTask_StateValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		seedStatus string
		wantCode   int
	}{
		{
			name:       "cancel_pending_succeeds",
			seedStatus: "PENDING",
			wantCode:   http.StatusOK,
		},
		{
			name:       "cancel_running_succeeds",
			seedStatus: "RUNNING",
			wantCode:   http.StatusOK,
		},
		{
			name:       "cancel_completed_fails",
			seedStatus: "COMPLETED",
			wantCode:   http.StatusBadRequest,
		},
		{
			name:       "cancel_failed_fails",
			seedStatus: "FAILED",
			wantCode:   http.StatusBadRequest,
		},
		{
			name:       "cancel_already_cancelled_fails",
			seedStatus: "CANCELLED",
			wantCode:   http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := cloudwatchlogs.NewInMemoryBackend()
			taskID := "task-" + tt.name
			cloudwatchlogs.AddExportTaskInternal(backend, cloudwatchlogs.ExportTask{
				TaskID:       taskID,
				LogGroupName: "/grp",
				Destination:  "my-bucket",
				Status:       tt.seedStatus,
				From:         1000,
				To:           2000,
			})

			e := echo.New()
			h := cloudwatchlogs.NewHandler(backend)

			bodyBytes, _ := json.Marshal(map[string]any{"taskId": taskID})
			rec := doLogsRequest(t, h, e, "CancelExportTask", string(bodyBytes))
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_CancelImportTask_StateValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		seedStatus string
		wantCode   int
	}{
		{
			name:       "cancel_active_succeeds",
			seedStatus: "ACTIVE",
			wantCode:   http.StatusOK,
		},
		{
			name:       "cancel_failed_task_fails",
			seedStatus: "FAILED",
			wantCode:   http.StatusBadRequest,
		},
		{
			name:       "cancel_succeeded_task_fails",
			seedStatus: "SUCCEEDED",
			wantCode:   http.StatusBadRequest,
		},
		{
			name:       "cancel_already_cancelled_fails",
			seedStatus: "CANCELLED",
			wantCode:   http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := cloudwatchlogs.NewInMemoryBackend()
			importID := "import-" + tt.name
			cloudwatchlogs.AddImportTaskInternal(backend, cloudwatchlogs.ImportTask{
				ImportID:             importID,
				ImportSourceArn:      "arn:aws:cloudtrail:us-east-1:123:eventdatastore/abc",
				ImportRoleArn:        "arn:aws:iam::123:role/role",
				ImportDestinationArn: "arn:aws:logs:us-east-1:123:log-group:/aws/import",
				Status:               tt.seedStatus,
			})

			e := echo.New()
			h := cloudwatchlogs.NewHandler(backend)

			bodyBytes, _ := json.Marshal(map[string]any{"importId": importID})
			rec := doLogsRequest(t, h, e, "CancelImportTask", string(bodyBytes))
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_CreateExportTask_FromToValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantCode int
	}{
		{
			name:     "from_equal_to_fails",
			body:     `{"logGroupName":"/grp","destination":"bucket","from":1000,"to":1000}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "from_greater_than_to_fails",
			body:     `{"logGroupName":"/grp","destination":"bucket","from":2000,"to":1000}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "from_less_than_to_succeeds",
			body:     `{"logGroupName":"/grp","destination":"bucket","from":1000,"to":2000}`,
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := makeLogsRequest(t, "CreateExportTask", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_ExportImportTaskOperations(t *testing.T) {
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
		// CancelExportTask
		{
			name:     "CancelExportTask/EmptyTaskId",
			action:   "CancelExportTask",
			body:     map[string]any{"taskId": ""},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "CancelExportTask/NotFound",
			action:   "CancelExportTask",
			body:     map[string]any{"taskId": "nonexistent-task"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "CancelExportTask/MissingTaskId",
			action:   "CancelExportTask",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		// CancelImportTask
		{
			name:     "CancelImportTask/NotFound",
			action:   "CancelImportTask",
			body:     map[string]any{"importId": "nonexistent"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "CancelImportTask/MissingImportId",
			action:   "CancelImportTask",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		// CreateExportTask
		{
			name:   "CreateExportTask/OK",
			action: "CreateExportTask",
			body: map[string]any{
				"logGroupName": "/my/group",
				"destination":  "my-bucket",
				"from":         1000,
				"to":           2000,
			},
			wantCode: http.StatusOK,
			wantKey:  "taskId",
		},
		{
			name:     "CreateExportTask/MissingLogGroup",
			action:   "CreateExportTask",
			body:     map[string]any{"destination": "my-bucket", "from": 1000, "to": 2000},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "CreateExportTask/MissingDestination",
			action:   "CreateExportTask",
			body:     map[string]any{"logGroupName": "/my/group", "from": 1000, "to": 2000},
			wantCode: http.StatusBadRequest,
		},
		// CreateImportTask
		{
			name:   "CreateImportTask/OK",
			action: "CreateImportTask",
			body: map[string]any{
				"importRoleArn":   "arn:aws:iam::123:role/my-role",
				"importSourceArn": "arn:aws:cloudtrail:us-east-1:123:eventdatastore/abc",
			},
			wantCode: http.StatusOK,
			wantKey:  "importId",
		},
		{
			name:   "CreateImportTask/MissingRoleArn",
			action: "CreateImportTask",
			body: map[string]any{
				"importSourceArn": "arn:aws:cloudtrail:us-east-1:123:eventdatastore/abc",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "CreateImportTask/MissingSourceArn",
			action:   "CreateImportTask",
			body:     map[string]any{"importRoleArn": "arn:aws:iam::123:role/my-role"},
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

func TestHandler_ImportTaskBatchesValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body          map[string]any
		name          string
		action        string
		wantListField string
		wantCode      int
	}{
		{
			// DescribeImportTaskBatches is validation-only: taskId is required.
			name:     "DescribeImportTaskBatches/RequiresTaskID",
			action:   "DescribeImportTaskBatches",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bodyBytes, err := json.Marshal(tt.body)
			require.NoError(t, err)

			rec := makeLogsRequest(t, tt.action, string(bodyBytes))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantListField != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				list, ok := resp[tt.wantListField].([]any)
				require.True(t, ok, "expected list field %q in response", tt.wantListField)
				assert.Empty(t, list)
			}
		})
	}
}
