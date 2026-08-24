package datasync_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/datasync"
)

func TestDataSync_Task(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		check    func(t *testing.T, body []byte)
		name     string
		action   string
		wantCode int
	}{
		{
			name:     "CreateTask missing SourceLocationArn returns 400",
			action:   "CreateTask",
			body:     map[string]any{"DestinationLocationArn": "arn:x"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "CreateTask missing DestinationLocationArn returns 400",
			action:   "CreateTask",
			body:     map[string]any{"SourceLocationArn": "arn:x"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "DescribeTask unknown ARN returns 400",
			action:   "DescribeTask",
			body:     map[string]any{"TaskArn": "arn:aws:datasync:us-east-1:000000000000:task/notexist"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "DeleteTask unknown ARN returns 400",
			action:   "DeleteTask",
			body:     map[string]any{"TaskArn": "arn:aws:datasync:us-east-1:000000000000:task/notexist"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "ListTasks empty returns empty list",
			action:   "ListTasks",
			body:     map[string]any{},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				tasks, ok := resp["Tasks"].([]any)
				require.True(t, ok)
				assert.Empty(t, tasks)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestDataSync_TaskCRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Setup two locations
	srcArn := createTestLocationS3(t, h)
	dstArn := createTestLocationS3(t, h)

	// Create task
	taskArn := createTestTask(t, h, srcArn, dstArn)
	assert.Equal(t, 1, datasync.TaskCount(h.Backend.(*datasync.InMemoryBackend)))

	// Describe
	rec := doRequest(t, h, "DescribeTask", map[string]any{"TaskArn": taskArn})
	assert.Equal(t, http.StatusOK, rec.Code)
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "AVAILABLE", descResp["Status"])
	assert.Equal(t, "test-task", descResp["Name"])

	// Update
	rec = doRequest(t, h, "UpdateTask", map[string]any{"TaskArn": taskArn, "Name": "renamed-task"})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify update
	rec = doRequest(t, h, "DescribeTask", map[string]any{"TaskArn": taskArn})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "renamed-task", descResp["Name"])

	// List
	rec = doRequest(t, h, "ListTasks", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Len(t, listResp["Tasks"], 1)

	// Delete
	rec = doRequest(t, h, "DeleteTask", map[string]any{"TaskArn": taskArn})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, datasync.TaskCount(h.Backend.(*datasync.InMemoryBackend)))
}

func TestDataSync_TaskExecution(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Setup
	srcArn := createTestLocationS3(t, h)
	dstArn := createTestLocationS3(t, h)
	taskArn := createTestTask(t, h, srcArn, dstArn)

	// StartTaskExecution
	rec := doRequest(t, h, "StartTaskExecution", map[string]any{"TaskArn": taskArn})
	assert.Equal(t, http.StatusOK, rec.Code)
	var startResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
	execArn, ok := startResp["TaskExecutionArn"].(string)
	require.True(t, ok)
	assert.Contains(t, execArn, "/execution/")

	// DescribeTaskExecution
	rec = doRequest(t, h, "DescribeTaskExecution", map[string]any{"TaskExecutionArn": execArn})
	assert.Equal(t, http.StatusOK, rec.Code)
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "SUCCESS", descResp["Status"])

	// ListTaskExecutions
	rec = doRequest(t, h, "ListTaskExecutions", map[string]any{"TaskArn": taskArn})
	assert.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Len(t, listResp["TaskExecutions"], 1)

	// CancelTaskExecution on an execution already settled into SUCCESS (by
	// the DescribeTaskExecution call above) must be rejected, not silently
	// overwrite the outcome to ERROR -- see TestDataSync_CancelTaskExecution_RejectsTerminal.
	rec = doRequest(t, h, "CancelTaskExecution", map[string]any{"TaskExecutionArn": execArn})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// List after the rejected cancel - execution is unchanged, still SUCCESS.
	rec = doRequest(t, h, "ListTaskExecutions", map[string]any{"TaskArn": taskArn})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	execs, ok := listResp["TaskExecutions"].([]any)
	require.True(t, ok)
	require.Len(t, execs, 1)
	execEntry, ok := execs[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "SUCCESS", execEntry["Status"])

	// StartTaskExecution unknown task returns 400
	rec = doRequest(
		t,
		h,
		"StartTaskExecution",
		map[string]any{"TaskArn": "arn:aws:datasync:us-east-1:000000000000:task/notexist"},
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestDataSync_UpdateTaskExecution covers UpdateTaskExecution.
func TestDataSync_UpdateTaskExecution(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	srcArn := createTestLocationS3(t, h)
	dstArn := createTestLocationS3(t, h)
	taskArn := createTestTask(t, h, srcArn, dstArn)

	rec := doRequest(t, h, "StartTaskExecution", map[string]any{"TaskArn": taskArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var startResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
	execArn := startResp["TaskExecutionArn"].(string)

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "update existing execution with options",
			body: map[string]any{
				"TaskExecutionArn": execArn,
				"Options":          map[string]any{"BytesPerSecond": 1048576},
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "missing Options returns 400",
			body:     map[string]any{"TaskExecutionArn": execArn},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing TaskExecutionArn returns 400",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "not found returns 400",
			body: map[string]any{
				"TaskExecutionArn": "arn:aws:datasync:us-east-1:000000000000:task/notexist/execution/notexist",
				"Options":          map[string]any{"BytesPerSecond": 1048576},
			},
			wantCode: http.StatusBadRequest,
		},
	}

	// The Options applied via UpdateTaskExecution must be observable on
	// DescribeTaskExecution (the round-trip the prior stub broke).
	// Use a fresh execution on a second, independent task so the Describe
	// call (which auto-advances state) does not race with the parallel table
	// subtests that expect execArn to remain updatable (LAUNCHING); AWS only
	// allows one in-progress execution per task at a time, so reusing taskArn
	// here would be rejected while execArn is still LAUNCHING.
	src2Arn := createTestLocationS3(t, h)
	dst2Arn := createTestLocationS3(t, h)
	taskArn2 := createTestTask(t, h, src2Arn, dst2Arn)

	rec2 := doRequest(t, h, "StartTaskExecution", map[string]any{"TaskArn": taskArn2})
	require.Equal(t, http.StatusOK, rec2.Code)

	var startResp2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &startResp2))
	execArn2 := startResp2["TaskExecutionArn"].(string)

	updRec := doRequest(t, h, "UpdateTaskExecution", map[string]any{
		"TaskExecutionArn": execArn2,
		"Options":          map[string]any{"BytesPerSecond": 2097152},
	})
	require.Equal(t, http.StatusOK, updRec.Code)

	descRec := doRequest(t, h, "DescribeTaskExecution", map[string]any{"TaskExecutionArn": execArn2})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp struct {
		Options map[string]any `json:"Options"`
	}

	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	assert.InDelta(t, float64(2097152), descResp.Options["BytesPerSecond"], 0)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := doRequest(t, h, "UpdateTaskExecution", tc.body) //nolint:govet // existing issue.
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

// TestDataSync_CancelTaskExecutionStatusChange verifies that CancelTaskExecution
// transitions a LAUNCHING execution to ERROR (AWS has no CANCELLED
// TaskExecutionStatus enum value) rather than deleting it.
func TestDataSync_CancelTaskExecutionStatusChange(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	srcArn := createTestLocationS3(t, h)
	dstArn := createTestLocationS3(t, h)
	taskArn := createTestTask(t, h, srcArn, dstArn)

	rec := doRequest(t, h, "StartTaskExecution", map[string]any{"TaskArn": taskArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var startResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
	execArn := startResp["TaskExecutionArn"].(string)

	// Cancel.
	rec = doRequest(t, h, "CancelTaskExecution", map[string]any{"TaskExecutionArn": execArn})
	require.Equal(t, http.StatusOK, rec.Code)

	// Execution should now appear as ERROR in the list, not absent.
	rec = doRequest(t, h, "ListTaskExecutions", map[string]any{"TaskArn": taskArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))

	execs, ok := listResp["TaskExecutions"].([]any)
	require.True(t, ok)
	require.Len(t, execs, 1)
	assert.Equal(t, "ERROR", execs[0].(map[string]any)["Status"])
}

// TestDataSync_CancelTaskExecution_RejectsTerminal covers a gopherstack-g8k9
// bug: CancelTaskExecution had no terminal-state guard at all, unlike its
// sibling UpdateTaskExecution (which already rejects SUCCESS/ERROR
// executions). A DataSync task execution's LAUNCHING state is lazily
// advanced to SUCCESS the first time anyone calls DescribeTaskExecution, so
// an execution a client had already observed as finished could still be
// silently "cancelled" into ERROR, overwriting a real, already-reported
// outcome. Real DataSync only documents CancelTaskExecution as stopping "a
// task execution that's in progress" (api_op_CancelTaskExecution.go,
// datasync@v1.61.4), which this backend's own PARITY.md already flagged as
// a suspected but unconfirmed gap.
func TestDataSync_CancelTaskExecution_RejectsTerminal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		settle  func(t *testing.T, h *datasync.Handler, execArn string)
		wantErr string
	}{
		{
			name: "already SUCCESS via DescribeTaskExecution",
			settle: func(t *testing.T, h *datasync.Handler, execArn string) {
				t.Helper()
				rec := doRequest(t, h, "DescribeTaskExecution", map[string]any{"TaskExecutionArn": execArn})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantErr: "SUCCESS",
		},
		{
			name: "already ERROR via a prior Cancel",
			settle: func(t *testing.T, h *datasync.Handler, execArn string) {
				t.Helper()
				rec := doRequest(t, h, "CancelTaskExecution", map[string]any{"TaskExecutionArn": execArn})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantErr: "ERROR",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			srcArn := createTestLocationS3(t, h)
			dstArn := createTestLocationS3(t, h)
			taskArn := createTestTask(t, h, srcArn, dstArn)

			rec := doRequest(t, h, "StartTaskExecution", map[string]any{"TaskArn": taskArn})
			require.Equal(t, http.StatusOK, rec.Code)

			var startResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
			execArn := startResp["TaskExecutionArn"].(string)

			tt.settle(t, h, execArn)

			// Second cancel, now against a terminal execution, must be rejected.
			rec = doRequest(t, h, "CancelTaskExecution", map[string]any{"TaskExecutionArn": execArn})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantErr)

			// The execution's status must be unchanged by the rejected cancel.
			rec = doRequest(t, h, "DescribeTaskExecution", map[string]any{"TaskExecutionArn": execArn})
			require.Equal(t, http.StatusOK, rec.Code)
			var descResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
			assert.Equal(t, tt.wantErr, descResp["Status"])
		})
	}
}

// TestDataSync_DescribeTaskExecutionLazyAdvance verifies that DescribeTaskExecution
// transitions a LAUNCHING execution to SUCCESS on first call (lazy state advance).
func TestDataSync_DescribeTaskExecutionLazyAdvance(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	srcArn := createTestLocationS3(t, h)
	dstArn := createTestLocationS3(t, h)
	taskArn := createTestTask(t, h, srcArn, dstArn)

	rec := doRequest(t, h, "StartTaskExecution", map[string]any{"TaskArn": taskArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var startResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
	execArn := startResp["TaskExecutionArn"].(string)

	// Before describe: execution exists as LAUNCHING in list.
	rec = doRequest(t, h, "ListTaskExecutions", map[string]any{"TaskArn": taskArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))

	execs := listResp["TaskExecutions"].([]any)
	require.Len(t, execs, 1)
	assert.Equal(t, "LAUNCHING", execs[0].(map[string]any)["Status"])

	// First describe: should return SUCCESS.
	rec = doRequest(t, h, "DescribeTaskExecution", map[string]any{"TaskExecutionArn": execArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "SUCCESS", descResp["Status"])
}

// TestDataSync_ListTaskExecutionsUnknownTask verifies that listing executions for
// a non-existent task returns 400.
func TestDataSync_ListTaskExecutionsUnknownTask(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListTaskExecutions", map[string]any{
		"TaskArn": "arn:aws:datasync:us-east-1:000000000000:task/notexist",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestDataSync_StartTaskExecutionRejectsConcurrent verifies that starting a
// second execution while one is still in progress is rejected, matching the
// documented AWS behavior ("For each task, you can only run one task
// execution at a time.").
func TestDataSync_StartTaskExecutionRejectsConcurrent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	srcArn := createTestLocationS3(t, h)
	dstArn := createTestLocationS3(t, h)
	taskArn := createTestTask(t, h, srcArn, dstArn)

	rec := doRequest(t, h, "StartTaskExecution", map[string]any{"TaskArn": taskArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var startResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
	execArn := startResp["TaskExecutionArn"].(string)

	// Second start while the first execution is still LAUNCHING must fail.
	rec = doRequest(t, h, "StartTaskExecution", map[string]any{"TaskArn": taskArn})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Once the first execution settles into a terminal state, starting a new
	// one is allowed again.
	rec = doRequest(t, h, "DescribeTaskExecution", map[string]any{"TaskExecutionArn": execArn})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "StartTaskExecution", map[string]any{"TaskArn": taskArn})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestDataSync_TaskStatusRunningWhileExecuting verifies that a task's Status
// reports RUNNING for the duration of an in-progress execution and reverts
// to AVAILABLE once that execution finishes, matching AWS (task Status is
// distinct from -- but driven by -- task execution status).
func TestDataSync_TaskStatusRunningWhileExecuting(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	srcArn := createTestLocationS3(t, h)
	dstArn := createTestLocationS3(t, h)
	taskArn := createTestTask(t, h, srcArn, dstArn)

	rec := doRequest(t, h, "DescribeTask", map[string]any{"TaskArn": taskArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var taskResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &taskResp))
	assert.Equal(t, "AVAILABLE", taskResp["Status"])

	rec = doRequest(t, h, "StartTaskExecution", map[string]any{"TaskArn": taskArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var startResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
	execArn := startResp["TaskExecutionArn"].(string)

	rec = doRequest(t, h, "DescribeTask", map[string]any{"TaskArn": taskArn})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &taskResp))
	assert.Equal(t, "RUNNING", taskResp["Status"])

	// Settling the execution (lazy advance on Describe) reverts the task.
	rec = doRequest(t, h, "DescribeTaskExecution", map[string]any{"TaskExecutionArn": execArn})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "DescribeTask", map[string]any{"TaskArn": taskArn})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &taskResp))
	assert.Equal(t, "AVAILABLE", taskResp["Status"])
}

// TestDataSync_TaskSettings covers CreateTask/UpdateTask/DescribeTask's
// Options, Schedule, Excludes, Includes, ManifestConfig, TaskReportConfig,
// and TaskMode members -- fields present on the real
// CreateTaskInput/UpdateTaskInput/DescribeTaskOutput that were previously
// entirely unmodeled (silently dropped on Create, absent from Describe).
func TestDataSync_TaskSettings(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	srcArn := createTestLocationS3(t, h)
	dstArn := createTestLocationS3(t, h)

	rec := doRequest(t, h, "CreateTask", map[string]any{
		"SourceLocationArn":      srcArn,
		"DestinationLocationArn": dstArn,
		"Name":                   "settings-task",
		"Options":                map[string]any{"LogLevel": "TRANSFER", "BytesPerSecond": 1048576},
		"Schedule": map[string]any{
			"ScheduleExpression": "rate(1 hours)",
			"Status":             "ENABLED",
		},
		"Excludes": []any{
			map[string]any{"FilterType": "SIMPLE_PATTERN", "Value": "/tmp"},
		},
		"Includes": []any{
			map[string]any{"FilterType": "SIMPLE_PATTERN", "Value": "/data"},
		},
		"ManifestConfig": map[string]any{
			"Action": "TRANSFER",
			"Source": map[string]any{"S3": map[string]any{"ManifestObjectPath": "manifest.csv"}},
		},
		"TaskReportConfig": map[string]any{
			"OutputType": "STANDARD",
		},
		"TaskMode": "ENHANCED",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	taskArn := createResp["TaskArn"].(string)

	rec = doRequest(t, h, "DescribeTask", map[string]any{"TaskArn": taskArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))

	opts, ok := descResp["Options"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "TRANSFER", opts["LogLevel"])

	schedule, ok := descResp["Schedule"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "rate(1 hours)", schedule["ScheduleExpression"])
	assert.Equal(t, "ENABLED", schedule["Status"])

	excludes, ok := descResp["Excludes"].([]any)
	require.True(t, ok)
	require.Len(t, excludes, 1)
	assert.Equal(t, "/tmp", excludes[0].(map[string]any)["Value"])

	includes, ok := descResp["Includes"].([]any)
	require.True(t, ok)
	require.Len(t, includes, 1)
	assert.Equal(t, "/data", includes[0].(map[string]any)["Value"])

	manifestCfg, ok := descResp["ManifestConfig"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "TRANSFER", manifestCfg["Action"])

	reportCfg, ok := descResp["TaskReportConfig"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "STANDARD", reportCfg["OutputType"])

	assert.Equal(t, "ENHANCED", descResp["TaskMode"])

	// UpdateTask: a field that is entirely omitted from the request must
	// leave the existing value untouched (AWS's "only supplied fields
	// change" semantics) -- here we only update Name, so Options/Schedule/
	// Excludes/etc. must survive unchanged.
	rec = doRequest(t, h, "UpdateTask", map[string]any{
		"TaskArn": taskArn,
		"Name":    "settings-task-renamed",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "DescribeTask", map[string]any{"TaskArn": taskArn})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "settings-task-renamed", descResp["Name"])
	opts, ok = descResp["Options"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "TRANSFER", opts["LogLevel"])

	// UpdateTask: explicitly supplying a field replaces it.
	rec = doRequest(t, h, "UpdateTask", map[string]any{
		"TaskArn": taskArn,
		"Excludes": []any{
			map[string]any{"FilterType": "SIMPLE_PATTERN", "Value": "/var"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "DescribeTask", map[string]any{"TaskArn": taskArn})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	excludes, ok = descResp["Excludes"].([]any)
	require.True(t, ok)
	require.Len(t, excludes, 1)
	assert.Equal(t, "/var", excludes[0].(map[string]any)["Value"])
}

// TestDataSync_TaskModeDefaultsToBasic verifies that CreateTask defaults
// TaskMode to BASIC when omitted, matching the real API's documented default
// ("BASIC (default)").
func TestDataSync_TaskModeDefaultsToBasic(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	srcArn := createTestLocationS3(t, h)
	dstArn := createTestLocationS3(t, h)
	taskArn := createTestTask(t, h, srcArn, dstArn)

	rec := doRequest(t, h, "DescribeTask", map[string]any{"TaskArn": taskArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "BASIC", descResp["TaskMode"])
}

// TestDataSync_ListTaskExecutionsAllTasks verifies that omitting TaskArn lists
// executions across every task, since TaskArn is an optional filter on the
// real ListTaskExecutions API rather than a required parameter.
func TestDataSync_ListTaskExecutionsAllTasks(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	src1, dst1 := createTestLocationS3(t, h), createTestLocationS3(t, h)
	task1 := createTestTask(t, h, src1, dst1)
	rec := doRequest(t, h, "StartTaskExecution", map[string]any{"TaskArn": task1})
	require.Equal(t, http.StatusOK, rec.Code)

	src2, dst2 := createTestLocationS3(t, h), createTestLocationS3(t, h)
	task2 := createTestTask(t, h, src2, dst2)
	rec = doRequest(t, h, "StartTaskExecution", map[string]any{"TaskArn": task2})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "ListTaskExecutions", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))

	execs, ok := listResp["TaskExecutions"].([]any)
	require.True(t, ok)
	assert.Len(t, execs, 2)
}
