package datasync_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/datasync"
)

func newTestHandler(t *testing.T) *datasync.Handler {
	t.Helper()
	backend := datasync.NewInMemoryBackend("000000000000", "us-east-1")

	return datasync.NewHandler(backend)
}

func doRequest(t *testing.T, h *datasync.Handler, action string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var marshalErr error

		bodyBytes, marshalErr = json.Marshal(body)
		require.NoError(t, marshalErr)
	}

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "FmrsService."+action)
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	handlerErr := h.Handler()(c)
	require.NoError(t, handlerErr)

	return rec
}

func createTestAgent(t *testing.T, h *datasync.Handler) string {
	t.Helper()
	rec := doRequest(t, h, "CreateAgent", map[string]any{
		"ActivationKey": "AAAAA-BBBBB-CCCCC-DDDDD-EEEEE",
		"AgentName":     "test-agent",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	arn, ok := resp["AgentArn"].(string)
	require.True(t, ok)
	require.NotEmpty(t, arn)

	return arn
}

func createTestLocationS3(t *testing.T, h *datasync.Handler) string {
	t.Helper()
	rec := doRequest(t, h, "CreateLocationS3", map[string]any{
		"S3BucketArn":  "arn:aws:s3:::my-test-bucket",
		"Subdirectory": "/data",
		"S3Config": map[string]any{
			"BucketAccessRoleArn": "arn:aws:iam::000000000000:role/DataSyncRole",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	arn, ok := resp["LocationArn"].(string)
	require.True(t, ok)
	require.NotEmpty(t, arn)

	return arn
}

func createTestTask(t *testing.T, h *datasync.Handler, srcArn, dstArn string) string {
	t.Helper()
	rec := doRequest(t, h, "CreateTask", map[string]any{
		"SourceLocationArn":      srcArn,
		"DestinationLocationArn": dstArn,
		"Name":                   "test-task",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	arn, ok := resp["TaskArn"].(string)
	require.True(t, ok)
	require.NotEmpty(t, arn)

	return arn
}

func TestDataSync_Agent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		setup    func(h *datasync.Handler)
		check    func(t *testing.T, body []byte)
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "CreateAgent returns AgentArn",
			action: "CreateAgent",
			body: map[string]any{
				"ActivationKey": "AAAAA-BBBBB-CCCCC-DDDDD-EEEEE",
				"AgentName":     "my-agent",
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Contains(t, resp["AgentArn"], "arn:aws:datasync:us-east-1:000000000000:agent/")
			},
		},
		{
			name:     "CreateAgent missing ActivationKey returns 400",
			action:   "CreateAgent",
			body:     map[string]any{"AgentName": "x"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "DescribeAgent returns agent details",
			action: "DescribeAgent",
			setup:  func(h *datasync.Handler) { createTestAgent(t, h) },
			body: func() any {
				return nil // populated in setup via describe after create
			}(),
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "ONLINE", resp["Status"])
			},
		},
		{
			name:     "DescribeAgent unknown ARN returns 404",
			action:   "DescribeAgent",
			body:     map[string]any{"AgentArn": "arn:aws:datasync:us-east-1:000000000000:agent/notexist"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "DescribeAgent missing ARN returns 400",
			action:   "DescribeAgent",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "UpdateAgent unknown ARN returns 404",
			action: "UpdateAgent",
			body: map[string]any{
				"AgentArn": "arn:aws:datasync:us-east-1:000000000000:agent/notexist",
				"Name":     "new",
			},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "DeleteAgent unknown ARN returns 404",
			action:   "DeleteAgent",
			body:     map[string]any{"AgentArn": "arn:aws:datasync:us-east-1:000000000000:agent/notexist"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "ListAgents empty returns empty list",
			action:   "ListAgents",
			body:     map[string]any{},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				agents, ok := resp["Agents"].([]any)
				require.True(t, ok)
				assert.Empty(t, agents)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tc.setup != nil {
				tc.setup(h)
			}

			body := tc.body
			// For DescribeAgent after setup, derive body from created agent.
			if tc.action == "DescribeAgent" && tc.setup != nil && tc.body == nil {
				agentArn := createTestAgent(t, h)
				body = map[string]any{"AgentArn": agentArn}
			}

			rec := doRequest(t, h, tc.action, body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestDataSync_AgentCRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	agentArn := createTestAgent(t, h)
	assert.Equal(t, 1, datasync.AgentCount(h.Backend.(*datasync.InMemoryBackend)))

	// Describe
	rec := doRequest(t, h, "DescribeAgent", map[string]any{"AgentArn": agentArn})
	assert.Equal(t, http.StatusOK, rec.Code)
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "test-agent", descResp["Name"])

	// Update
	rec = doRequest(t, h, "UpdateAgent", map[string]any{"AgentArn": agentArn, "Name": "updated-agent"})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify update
	rec = doRequest(t, h, "DescribeAgent", map[string]any{"AgentArn": agentArn})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "updated-agent", descResp["Name"])

	// List
	rec = doRequest(t, h, "ListAgents", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Len(t, listResp["Agents"], 1)

	// Delete
	rec = doRequest(t, h, "DeleteAgent", map[string]any{"AgentArn": agentArn})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, datasync.AgentCount(h.Backend.(*datasync.InMemoryBackend)))

	// Describe deleted returns 404
	rec = doRequest(t, h, "DescribeAgent", map[string]any{"AgentArn": agentArn})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestDataSync_LocationS3(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		setup    func(h *datasync.Handler)
		check    func(t *testing.T, body []byte)
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "CreateLocationS3 returns LocationArn",
			action: "CreateLocationS3",
			body: map[string]any{
				"S3BucketArn":  "arn:aws:s3:::my-bucket",
				"Subdirectory": "/prefix",
				"S3Config": map[string]any{
					"BucketAccessRoleArn": "arn:aws:iam::000000000000:role/Role",
				},
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Contains(t, resp["LocationArn"], "arn:aws:datasync:us-east-1:000000000000:location/")
			},
		},
		{
			name:   "CreateLocationS3 missing S3BucketArn returns 400",
			action: "CreateLocationS3",
			body: map[string]any{
				"S3Config": map[string]any{
					"BucketAccessRoleArn": "arn:aws:iam::000000000000:role/Role",
				},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "CreateLocationS3 missing S3Config returns 400",
			action:   "CreateLocationS3",
			body:     map[string]any{"S3BucketArn": "arn:aws:s3:::my-bucket"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "DescribeLocationS3 unknown ARN returns 404",
			action:   "DescribeLocationS3",
			body:     map[string]any{"LocationArn": "arn:aws:datasync:us-east-1:000000000000:location/notexist"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "DeleteLocation unknown ARN returns 404",
			action:   "DeleteLocation",
			body:     map[string]any{"LocationArn": "arn:aws:datasync:us-east-1:000000000000:location/notexist"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "ListLocations empty returns empty list",
			action:   "ListLocations",
			body:     map[string]any{},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				locs, ok := resp["Locations"].([]any)
				require.True(t, ok)
				assert.Empty(t, locs)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tc.setup != nil {
				tc.setup(h)
			}

			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestDataSync_LocationS3CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	locationArn := createTestLocationS3(t, h)
	assert.Equal(t, 1, datasync.LocationCount(h.Backend.(*datasync.InMemoryBackend)))

	// Describe
	rec := doRequest(t, h, "DescribeLocationS3", map[string]any{"LocationArn": locationArn})
	assert.Equal(t, http.StatusOK, rec.Code)
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Contains(t, descResp["LocationUri"].(string), "s3://my-test-bucket/")

	// List
	rec = doRequest(t, h, "ListLocations", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Len(t, listResp["Locations"], 1)

	// Delete
	rec = doRequest(t, h, "DeleteLocation", map[string]any{"LocationArn": locationArn})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, datasync.LocationCount(h.Backend.(*datasync.InMemoryBackend)))
}

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
			name:     "DescribeTask unknown ARN returns 404",
			action:   "DescribeTask",
			body:     map[string]any{"TaskArn": "arn:aws:datasync:us-east-1:000000000000:task/notexist"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "DeleteTask unknown ARN returns 404",
			action:   "DeleteTask",
			body:     map[string]any{"TaskArn": "arn:aws:datasync:us-east-1:000000000000:task/notexist"},
			wantCode: http.StatusNotFound,
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

	// CancelTaskExecution
	rec = doRequest(t, h, "CancelTaskExecution", map[string]any{"TaskExecutionArn": execArn})
	assert.Equal(t, http.StatusOK, rec.Code)

	// List after cancel - execution persists with CANCELLED status
	rec = doRequest(t, h, "ListTaskExecutions", map[string]any{"TaskArn": taskArn})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	execs, ok := listResp["TaskExecutions"].([]any)
	require.True(t, ok)
	require.Len(t, execs, 1)
	execEntry, ok := execs[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "CANCELLED", execEntry["Status"])

	// StartTaskExecution unknown task returns 404
	rec = doRequest(
		t,
		h,
		"StartTaskExecution",
		map[string]any{"TaskArn": "arn:aws:datasync:us-east-1:000000000000:task/notexist"},
	)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestDataSync_Tags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	agentArn := createTestAgent(t, h)

	// TagResource
	rec := doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": agentArn,
		"Tags": []any{
			map[string]any{"Key": "env", "Value": "prod"},
			map[string]any{"Key": "team", "Value": "infra"},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListTagsForResource
	rec = doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": agentArn})
	assert.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	tags, ok := listResp["Tags"].([]any)
	require.True(t, ok)
	assert.Len(t, tags, 2)

	// UntagResource
	rec = doRequest(t, h, "UntagResource", map[string]any{
		"ResourceArn": agentArn,
		"Keys":        []string{"env"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify untag
	rec = doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": agentArn})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	tags = listResp["Tags"].([]any)
	assert.Len(t, tags, 1)

	// TagResource unknown resource returns 404
	rec = doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": "arn:aws:datasync:us-east-1:000000000000:agent/notexist",
		"Tags":        []any{map[string]any{"Key": "k", "Value": "v"}},
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestDataSync_UnknownAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "BogusOperation", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
