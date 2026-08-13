package eventbridge_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_Pipes(t *testing.T) {
	t.Parallel()

	e := echo.New()
	b := newBackend()
	h := eventbridge.NewHandler(b)

	rec := auditMakeRequest(t, h, e, "CreatePipe", map[string]any{
		"Name":      "my-pipe",
		"SourceArn": "arn:aws:sqs:us-east-1:123456789012:source-q",
		"TargetArn": "arn:aws:lambda:us-east-1:123456789012:function:fn",
		"RoleArn":   "arn:aws:iam::123456789012:role/pipe-role",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "my-pipe")

	rec = auditMakeRequest(t, h, e, "DescribePipe", map[string]any{"Name": "my-pipe"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "ListPipes", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "my-pipe")

	rec = auditMakeRequest(t, h, e, "UpdatePipe", map[string]any{
		"Name":        "my-pipe",
		"Description": "updated",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "DeletePipe", map[string]any{"Name": "my-pipe"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "DescribePipe", map[string]any{"Name": "my-pipe"})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestCreatePipe_CreationTimeIsEpochFloat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		pipeName  string
		sourceArn string
		targetArn string
	}{
		{
			name:      "CreationTime in CreatePipe response is a JSON number",
			pipeName:  "pipe-ts-test",
			sourceArn: "arn:aws:sqs:us-east-1:123456789012:src-q",
			targetArn: "arn:aws:lambda:us-east-1:123456789012:function:fn",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			b := eventbridge.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			h := eventbridge.NewHandler(b)

			rec := auditMakeRequest(t, h, e, "CreatePipe", map[string]any{
				"Name":      tt.pipeName,
				"SourceArn": tt.sourceArn,
				"TargetArn": tt.targetArn,
				"RoleArn":   "arn:aws:iam::123456789012:role/pipe-role",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var raw map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

			ctRaw, ok := raw["CreationTime"]
			require.True(t, ok, "CreationTime must be present in CreatePipe response")

			var f float64
			err := json.Unmarshal(ctRaw, &f)
			require.NoError(t, err, "CreationTime must be a JSON number (epoch seconds), got: %s", string(ctRaw))
			assert.Greater(t, f, float64(0))
		})
	}
}

// TestDescribePipeAndListPipes_TimestampsAreEpochFloat proves DescribePipe
// and ListPipes emit CreationTime/LastModifiedTime as epoch-seconds JSON
// numbers, not RFC3339 strings -- both ops used to return the backend's Pipe
// struct directly (json.Marshal on time.Time), unlike CreatePipe/UpdatePipe
// which already built epoch-converted anonymous response structs.
func TestDescribePipeAndListPipes_TimestampsAreEpochFloat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		action string
	}{
		{name: "describe pipe", action: "DescribePipe"},
		{name: "list pipes", action: "ListPipes"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			b := eventbridge.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			h := eventbridge.NewHandler(b)

			auditMakeRequest(t, h, e, "CreatePipe", map[string]any{
				"Name":      "epoch-pipe",
				"SourceArn": "arn:aws:sqs:us-east-1:123456789012:src-q",
				"TargetArn": "arn:aws:lambda:us-east-1:123456789012:function:fn",
				"RoleArn":   "arn:aws:iam::123456789012:role/pipe-role",
			})

			var rec *httptest.ResponseRecorder
			if tt.action == "DescribePipe" {
				rec = auditMakeRequest(t, h, e, tt.action, map[string]any{"Name": "epoch-pipe"})
			} else {
				rec = auditMakeRequest(t, h, e, tt.action, map[string]any{})
			}

			require.Equal(t, http.StatusOK, rec.Code)

			var creationRaw json.RawMessage
			if tt.action == "DescribePipe" {
				var raw map[string]json.RawMessage
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))
				creationRaw = raw["CreationTime"]
			} else {
				var raw struct {
					Pipes []map[string]json.RawMessage `json:"Pipes"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))
				require.Len(t, raw.Pipes, 1)
				creationRaw = raw.Pipes[0]["CreationTime"]
			}

			require.NotNil(t, creationRaw, "CreationTime must be present")

			var f float64
			err := json.Unmarshal(creationRaw, &f)
			require.NoError(t, err, "CreationTime must be a JSON number (epoch seconds), got: %s", string(creationRaw))
			assert.Greater(t, f, float64(0))
		})
	}
}

func TestUpdatePipe_LastModifiedTimeIsEpochFloat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		pipeName string
	}{
		{
			name:     "LastModifiedTime in UpdatePipe response is a JSON number",
			pipeName: "pipe-update-ts",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			b := eventbridge.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			h := eventbridge.NewHandler(b)

			auditMakeRequest(t, h, e, "CreatePipe", map[string]any{
				"Name":      tt.pipeName,
				"SourceArn": "arn:aws:sqs:us-east-1:123456789012:src-q",
				"TargetArn": "arn:aws:lambda:us-east-1:123456789012:function:fn",
				"RoleArn":   "arn:aws:iam::123456789012:role/pipe-role",
			})

			rec := auditMakeRequest(t, h, e, "UpdatePipe", map[string]any{
				"Name":        tt.pipeName,
				"Description": "updated",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var raw map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

			lmtRaw, ok := raw["LastModifiedTime"]
			require.True(t, ok, "LastModifiedTime must be present in UpdatePipe response")

			var f float64
			err := json.Unmarshal(lmtRaw, &f)
			require.NoError(t, err, "LastModifiedTime must be a JSON number (epoch seconds), got: %s", string(lmtRaw))
			assert.Greater(t, f, float64(0))
		})
	}
}
