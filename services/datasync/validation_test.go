package datasync_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDataSync_RequestValidation(t *testing.T) {
	t.Parallel()

	const role = "arn:aws:iam::000000000000:role/r"

	tests := []struct {
		body    func(src, dst string) map[string]any
		name    string
		action  string
		wantMsg string
	}{
		{
			name:   "bad bucket arn",
			action: "CreateLocationS3",
			body: func(_, _ string) map[string]any {
				return map[string]any{"S3BucketArn": "bad", "S3Config": map[string]any{"BucketAccessRoleArn": role}}
			},
			wantMsg: "S3BucketArn",
		},
		{
			name:   "bad task name",
			action: "CreateTask",
			body: func(src, dst string) map[string]any {
				return map[string]any{"SourceLocationArn": src, "DestinationLocationArn": dst, "Name": "bad name!"}
			},
			wantMsg: "Name",
		},
		{
			name:   "bad schedule",
			action: "CreateTask",
			body: func(src, dst string) map[string]any {
				return map[string]any{
					"SourceLocationArn": src, "DestinationLocationArn": dst,
					"Schedule": map[string]any{"ScheduleExpression": "bogus"},
				}
			},
			wantMsg: "ScheduleExpression",
		},
		{
			name:   "bad option enum",
			action: "CreateTask",
			body: func(src, dst string) map[string]any {
				return map[string]any{
					"SourceLocationArn": src, "DestinationLocationArn": dst,
					"Options": map[string]any{"VerifyMode": "BOGUS"},
				}
			},
			wantMsg: "Options.VerifyMode",
		},
		{
			name:   "bad next token",
			action: "ListTasks",
			body: func(_, _ string) map[string]any {
				return map[string]any{"NextToken": "garbage!"}
			},
			wantMsg: "NextToken",
		},
		{
			name:   "unknown task names arn",
			action: "DescribeTask",
			body: func(_, _ string) map[string]any {
				return map[string]any{"TaskArn": "arn:aws:datasync:us-east-1:000000000000:task/task-0123456789abcdef0"}
			},
			wantMsg: "task-0123456789abcdef0 not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			src, dst := createTestLocationS3(t, h), createTestLocationS3(t, h)
			rec := doRequest(t, h, tt.action, tt.body(src, dst))
			require.Equal(t, http.StatusBadRequest, rec.Code)

			var out map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Equal(t, "InvalidRequestException", out["__type"])
			assert.Contains(t, out["message"], tt.wantMsg)
			assert.NotContains(t, out["message"], "InvalidRequestException:")
		})
	}
}

func TestDataSync_ResourceIDFormats(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	src, dst := createTestLocationS3(t, h), createTestLocationS3(t, h)
	task := createTestTask(t, h, src, dst)
	agent := createTestAgent(t, h)

	rec := doRequest(t, h, "StartTaskExecution", map[string]any{"TaskArn": task})
	require.Equal(t, http.StatusOK, rec.Code)

	var start map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &start))

	tests := []struct {
		name string
		arn  string
		re   string
	}{
		{"location", src, `:location/loc-[0-9a-f]{17}$`},
		{"task", task, `:task/task-[0-9a-f]{17}$`},
		{"agent", agent, `:agent/agent-[0-9a-f]{17}$`},
		{"execution", start["TaskExecutionArn"], `:task/task-[0-9a-f]{17}/execution/exec-[0-9a-f]{17}$`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Regexp(t, tt.re, tt.arn)
		})
	}
}
