package datasync_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
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

func TestDataSync_UnknownAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "BogusOperation", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestDataSync_NotFoundReturns404 verifies that all describe/delete/update operations
// return 404 (not 400) for unknown ARNs, matching real AWS DataSync behavior.
func TestDataSync_NotFoundReturns404(t *testing.T) {
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

// TestDataSync_ARNFormat verifies that created resources return well-formed AWS ARNs.
func TestDataSync_ARNFormat(t *testing.T) {
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
