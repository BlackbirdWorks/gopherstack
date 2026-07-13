package datasync_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/datasync"
)

// TestParity_NotFound_Returns404 verifies that all describe/delete/update operations
// return 404 (not 400) for unknown ARNs, matching real AWS DataSync behavior.
func TestParity_NotFound_Returns404(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   map[string]any
		name   string
		action string
	}{
		{
			name:   "DescribeAgent",
			action: "DescribeAgent",
			body:   map[string]any{"AgentArn": "arn:aws:datasync:us-east-1:000000000000:agent/notexist"},
		},
		{
			name:   "DeleteAgent",
			action: "DeleteAgent",
			body:   map[string]any{"AgentArn": "arn:aws:datasync:us-east-1:000000000000:agent/notexist"},
		},
		{
			name:   "DescribeTask",
			action: "DescribeTask",
			body:   map[string]any{"TaskArn": "arn:aws:datasync:us-east-1:000000000000:task/notexist"},
		},
		{
			name:   "DeleteTask",
			action: "DeleteTask",
			body:   map[string]any{"TaskArn": "arn:aws:datasync:us-east-1:000000000000:task/notexist"},
		},
		{
			name:   "DescribeLocation",
			action: "DescribeLocationS3",
			body:   map[string]any{"LocationArn": "arn:aws:datasync:us-east-1:000000000000:location/notexist"},
		},
		{
			name:   "DeleteLocation",
			action: "DeleteLocation",
			body:   map[string]any{"LocationArn": "arn:aws:datasync:us-east-1:000000000000:location/notexist"},
		},
		{
			name:   "DescribeTaskExecution",
			action: "DescribeTaskExecution",
			body: map[string]any{
				"TaskExecutionArn": "arn:aws:datasync:us-east-1:000000000000:task/notexist/execution/notexist",
			},
		},
		{
			name:   "CancelTaskExecution",
			action: "CancelTaskExecution",
			body: map[string]any{
				"TaskExecutionArn": "arn:aws:datasync:us-east-1:000000000000:task/notexist/execution/notexist",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.action, tt.body)
			assert.Equal(t, http.StatusNotFound, rec.Code, "expected 404 for %s with unknown ARN", tt.action)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, "ResourceNotFoundException", resp["__type"])
		})
	}
}

// TestParity_ARNFormat verifies that created resources return well-formed AWS ARNs.
func TestParity_ARNFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setup     func(t *testing.T, h *datasync.Handler) string
		arnPrefix string
	}{
		{
			name: "agent ARN",
			setup: func(t *testing.T, h *datasync.Handler) string {
				t.Helper()

				return createTestAgent(t, h)
			},
			arnPrefix: "arn:aws:datasync:us-east-1:000000000000:agent/",
		},
		{
			name: "location ARN",
			setup: func(t *testing.T, h *datasync.Handler) string {
				t.Helper()

				return createTestLocationS3(t, h)
			},
			arnPrefix: "arn:aws:datasync:us-east-1:000000000000:location/",
		},
		{
			name: "task ARN",
			setup: func(t *testing.T, h *datasync.Handler) string {
				t.Helper()
				srcArn := createTestLocationS3(t, h)
				dstArn := createTestLocationS3(t, h)

				return createTestTask(t, h, srcArn, dstArn)
			},
			arnPrefix: "arn:aws:datasync:us-east-1:000000000000:task/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			arn := tt.setup(t, h)
			assert.True(t, strings.HasPrefix(arn, tt.arnPrefix), "ARN %q should start with %q", arn, tt.arnPrefix)
		})
	}
}

// TestParity_TagsRoundTrip verifies that tags applied via TagResource are visible
// on ListTagsForResource, and that UntagResource removes them.
func TestParity_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	agentArn := createTestAgent(t, h)

	// Apply tags.
	rec := doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": agentArn,
		"Tags": []map[string]any{
			{"Key": "Env", "Value": "prod"},
			{"Key": "Team", "Value": "data"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify tags appear.
	rec = doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": agentArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))

	tags, ok := listResp["Tags"].([]any)
	require.True(t, ok)
	assert.Len(t, tags, 2)

	// Verify tags are sorted by key.
	if len(tags) == 2 {
		tag0 := tags[0].(map[string]any)
		tag1 := tags[1].(map[string]any)
		assert.Equal(t, "Env", tag0["Key"])
		assert.Equal(t, "Team", tag1["Key"])
	}

	// Remove one tag.
	rec = doRequest(t, h, "UntagResource", map[string]any{
		"ResourceArn": agentArn,
		"Keys":        []string{"Env"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": agentArn})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))

	tags, ok = listResp["Tags"].([]any)
	require.True(t, ok)
	assert.Len(t, tags, 1)
	assert.Equal(t, "Team", tags[0].(map[string]any)["Key"])
}

// TestParity_CancelTaskExecution_StatusChange verifies that CancelTaskExecution
// transitions a LAUNCHING execution to ERROR (AWS has no CANCELLED
// TaskExecutionStatus enum value) rather than deleting it.
func TestParity_CancelTaskExecution_StatusChange(t *testing.T) {
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

// TestParity_DescribeTaskExecution_LazyAdvance verifies that DescribeTaskExecution
// transitions a LAUNCHING execution to SUCCESS on first call (lazy state advance).
func TestParity_DescribeTaskExecution_LazyAdvance(t *testing.T) {
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

// TestParity_ListTaskExecutions_UnknownTask verifies that listing executions for
// a non-existent task returns 404.
func TestParity_ListTaskExecutions_UnknownTask(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListTaskExecutions", map[string]any{
		"TaskArn": "arn:aws:datasync:us-east-1:000000000000:task/notexist",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestParity_StartTaskExecution_RejectsConcurrent verifies that starting a
// second execution while one is still in progress is rejected, matching the
// documented AWS behavior ("For each task, you can only run one task
// execution at a time.").
func TestParity_StartTaskExecution_RejectsConcurrent(t *testing.T) {
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

// TestParity_TaskStatus_RunningWhileExecuting verifies that a task's Status
// reports RUNNING for the duration of an in-progress execution and reverts
// to AVAILABLE once that execution finishes, matching AWS (task Status is
// distinct from -- but driven by -- task execution status).
func TestParity_TaskStatus_RunningWhileExecuting(t *testing.T) {
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

// TestParity_ListTaskExecutions_AllTasks verifies that omitting TaskArn lists
// executions across every task, since TaskArn is an optional filter on the
// real ListTaskExecutions API rather than a required parameter.
func TestParity_ListTaskExecutions_AllTasks(t *testing.T) {
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
