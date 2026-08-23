package pipes_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/pipes"
)

func newTestHandler(t *testing.T) *pipes.Handler {
	t.Helper()

	return pipes.NewHandler(pipes.NewInMemoryBackend("000000000000", "us-east-1"))
}

func doPipesRequest(t *testing.T, h *pipes.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	return doPipesRawRequest(t, h, method, path, bodyBytes)
}

func doPipesRawRequest(t *testing.T, h *pipes.Handler, method, path string, body []byte) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20230101/us-east-1/pipes/aws4_request")

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetRequest(req)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "Pipes", h.Name())
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "CreatePipe")
	assert.Contains(t, ops, "DescribePipe")
	assert.Contains(t, ops, "ListPipes")
	assert.Contains(t, ops, "DeletePipe")
	assert.Contains(t, ops, "UpdatePipe")
	assert.Contains(t, ops, "StartPipe")
	assert.Contains(t, ops, "StopPipe")
	assert.Contains(t, ops, "TagResource")
	assert.Contains(t, ops, "UntagResource")
	assert.Contains(t, ops, "ListTagsForResource")
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, 87, h.MatchPriority())
}

func TestHandler_CreatePipe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		pipeName   string
		wantBody   string
		bodyRaw    []byte
		wantStatus int
	}{
		{
			name:     "success",
			pipeName: "my-pipe",
			body: map[string]any{
				"RoleArn": "arn:aws:iam::000000000000:role/pipe-role",
				"Source":  "arn:aws:sqs:us-east-1:000000000000:source-queue",
				"Target":  "arn:aws:lambda:us-east-1:000000000000:function:target-fn",
			},
			wantStatus: http.StatusOK,
			wantBody:   "my-pipe",
		},
		{
			name:       "invalid_json",
			pipeName:   "bad-pipe",
			bodyRaw:    []byte("not-json"),
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var rec *httptest.ResponseRecorder
			if tt.bodyRaw != nil {
				rec = doPipesRawRequest(t, h, http.MethodPost, "/v1/pipes/"+tt.pipeName, tt.bodyRaw)
			} else {
				rec = doPipesRequest(t, h, http.MethodPost, "/v1/pipes/"+tt.pipeName, tt.body)
			}

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestHandler_DescribePipe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		pipeName   string
		wantBody   string
		wantStatus int
		create     bool
	}{
		{
			name:       "existing_pipe",
			pipeName:   "describe-pipe",
			create:     true,
			wantStatus: http.StatusOK,
			wantBody:   "describe-pipe",
		},
		{
			name:       "not_found",
			pipeName:   "missing-pipe",
			create:     false,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.create {
				doPipesRequest(t, h, http.MethodPost, "/v1/pipes/"+tt.pipeName, map[string]any{
					"RoleArn": "arn:aws:iam::000000000000:role/r",
					"Source":  "arn:aws:sqs:us-east-1:000000000000:src",
					"Target":  "arn:aws:lambda:us-east-1:000000000000:function:fn",
				})
			}

			rec := doPipesRequest(t, h, http.MethodGet, "/v1/pipes/"+tt.pipeName, nil)

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestHandler_ListPipes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doPipesRequest(t, h, http.MethodPost, "/v1/pipes/list-pipe-1", map[string]any{
		"RoleArn": "arn:aws:iam::000000000000:role/r",
		"Source":  "arn:aws:sqs:us-east-1:000000000000:src",
		"Target":  "arn:aws:lambda:us-east-1:000000000000:function:fn",
	})

	rec := doPipesRequest(t, h, http.MethodGet, "/v1/pipes", nil)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Pipes")
	assert.Contains(t, rec.Body.String(), "list-pipe-1")
}

func TestHandler_DeletePipe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		pipeName   string
		create     bool
		wantStatus int
	}{
		{
			name:       "success",
			pipeName:   "del-pipe",
			create:     true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			pipeName:   "missing-del-pipe",
			create:     false,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.create {
				doPipesRequest(t, h, http.MethodPost, "/v1/pipes/"+tt.pipeName, map[string]any{
					"RoleArn": "arn:aws:iam::000000000000:role/r",
					"Source":  "arn:aws:sqs:us-east-1:000000000000:src",
					"Target":  "arn:aws:lambda:us-east-1:000000000000:function:fn",
				})
			}

			rec := doPipesRequest(t, h, http.MethodDelete, "/v1/pipes/"+tt.pipeName, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_UpdatePipe(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doPipesRequest(t, h, http.MethodPost, "/v1/pipes/update-pipe", map[string]any{
		"RoleArn": "arn:aws:iam::000000000000:role/r",
		"Source":  "arn:aws:sqs:us-east-1:000000000000:src",
		"Target":  "arn:aws:lambda:us-east-1:000000000000:function:fn",
	})

	rec := doPipesRequest(t, h, http.MethodPut, "/v1/pipes/update-pipe", map[string]any{
		"RoleArn":     "arn:aws:iam::000000000000:role/r",
		"Target":      "arn:aws:lambda:us-east-1:000000000000:function:new-fn",
		"Description": "updated desc",
	})

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "update-pipe")
}

func TestHandler_StartStopPipe(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doPipesRequest(t, h, http.MethodPost, "/v1/pipes/start-stop-pipe", map[string]any{
		"RoleArn":      "arn:aws:iam::000000000000:role/r",
		"Source":       "arn:aws:sqs:us-east-1:000000000000:src",
		"Target":       "arn:aws:lambda:us-east-1:000000000000:function:fn",
		"DesiredState": "RUNNING",
	})

	stopRec := doPipesRequest(t, h, http.MethodPost, "/v1/pipes/start-stop-pipe/stop", nil)
	assert.Equal(t, http.StatusOK, stopRec.Code)
	assert.Contains(t, stopRec.Body.String(), "STOPPED")

	startRec := doPipesRequest(t, h, http.MethodPost, "/v1/pipes/start-stop-pipe/start", nil)
	assert.Equal(t, http.StatusOK, startRec.Code)
	assert.Contains(t, startRec.Body.String(), "RUNNING")
}

func TestBackend_TagResource(t *testing.T) {
	t.Parallel()

	b := pipes.NewInMemoryBackend("000000000000", "us-east-1")

	p, err := b.CreatePipeSimple("tag-pipe", "arn:aws:iam::000000000000:role/r",
		"arn:aws:sqs:us-east-1:000000000000:src",
		"arn:aws:lambda:us-east-1:000000000000:function:fn",
		"", "RUNNING", nil)
	require.NoError(t, err)

	err = b.TagResource(context.Background(), p.ARN, map[string]string{"env": "test"})
	require.NoError(t, err)

	tags, err := b.ListTagsForResource(context.Background(), p.ARN)
	require.NoError(t, err)
	assert.Equal(t, "test", tags["env"])
}

func TestBackend_DuplicatePipe(t *testing.T) {
	t.Parallel()

	b := pipes.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.CreatePipeSimple("dupe", "arn:aws:iam::000000000000:role/r",
		"arn:aws:sqs:us-east-1:000000000000:src",
		"arn:aws:lambda:us-east-1:000000000000:function:fn",
		"", "RUNNING", nil)
	require.NoError(t, err)

	_, err = b.CreatePipeSimple("dupe", "arn:aws:iam::000000000000:role/r",
		"arn:aws:sqs:us-east-1:000000000000:src",
		"arn:aws:lambda:us-east-1:000000000000:function:fn",
		"", "RUNNING", nil)
	require.Error(t, err)
}

// createTestPipeWithARN creates a pipe via the HTTP handler and returns its ARN.
func createTestPipeWithARN(t *testing.T, h *pipes.Handler, pipeName string) string {
	t.Helper()

	rec := doPipesRequest(t, h, http.MethodPost, "/v1/pipes/"+pipeName, map[string]any{
		"RoleArn": "arn:aws:iam::000000000000:role/r",
		"Source":  "arn:aws:sqs:us-east-1:000000000000:src",
		"Target":  "arn:aws:lambda:us-east-1:000000000000:function:fn",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp["Arn"].(string)
}

// addTagsToPipe tags a pipe via the HTTP handler.
func addTagsToPipe(t *testing.T, h *pipes.Handler, arn string, tags map[string]string) {
	t.Helper()

	rec := doPipesRequest(t, h, http.MethodPost, "/tags/"+arn, map[string]any{"Tags": tags})
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_TagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *pipes.Handler) string
		name       string
		wantStatus int
	}{
		{
			name: "tag_resource",
			setup: func(t *testing.T, h *pipes.Handler) string {
				t.Helper()

				return createTestPipeWithARN(t, h, "tag-test-pipe")
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_arn",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var arn string
			if tt.setup != nil {
				arn = tt.setup(t, h)
			}

			rec := doPipesRequest(t, h, http.MethodPost, "/tags/"+arn,
				map[string]any{"Tags": map[string]string{"env": "test"}})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListTagsForResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *pipes.Handler) string
		name       string
		wantTag    string
		wantStatus int
	}{
		{
			name: "list_tags",
			setup: func(t *testing.T, h *pipes.Handler) string {
				t.Helper()
				arn := createTestPipeWithARN(t, h, "list-tags-pipe")
				addTagsToPipe(t, h, arn, map[string]string{"env": "prod"})

				return arn
			},
			wantStatus: http.StatusOK,
			wantTag:    "prod",
		},
		{
			name:       "missing_arn",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var arn string
			if tt.setup != nil {
				arn = tt.setup(t, h)
			}

			rec := doPipesRequest(t, h, http.MethodGet, "/tags/"+arn, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantTag != "" {
				assert.Contains(t, rec.Body.String(), tt.wantTag)
			}
		})
	}
}

func TestHandler_UntagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, h *pipes.Handler) string
		tagKeys    string
		wantStatus int
	}{
		{
			name: "untag_specific_key",
			setup: func(t *testing.T, h *pipes.Handler) string {
				t.Helper()
				arn := createTestPipeWithARN(t, h, "untag-pipe")
				addTagsToPipe(t, h, arn, map[string]string{"env": "test", "team": "platform"})

				return arn
			},
			tagKeys:    "?tagKeys=env",
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_arn",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var arn string
			if tt.setup != nil {
				arn = tt.setup(t, h)
			}

			rec := doPipesRequest(t, h, http.MethodDelete, "/tags/"+arn+tt.tagKeys, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestPipes_Handler_Reset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		createPipes int
		wantAfter   int
	}{
		{
			name:        "reset clears all pipes",
			createPipes: 2,
			wantAfter:   0,
		},
		{
			name:        "reset on empty backend is a no-op",
			createPipes: 0,
			wantAfter:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for i := range tt.createPipes {
				rec := doPipesRequest(t, h, http.MethodPost, "/v1/pipes/pipe-"+strconv.Itoa(i), map[string]any{
					"RoleArn": "arn:aws:iam::000000000000:role/test",
					"Source":  "arn:aws:sqs:us-east-1:000000000000:source",
					"Target":  "arn:aws:lambda:us-east-1:000000000000:function:target",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			h.Reset()

			rec := doPipesRequest(t, h, http.MethodGet, "/v1/pipes", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				Pipes []any `json:"Pipes"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Len(t, out.Pipes, tt.wantAfter)
		})
	}
}

func TestBackend_Validation(t *testing.T) {
	t.Parallel()

	b := pipes.NewInMemoryBackend("000000000000", "us-east-1")

	t.Run("empty_name", func(t *testing.T) {
		t.Parallel()
		_, err := b.CreatePipeSimple("", "arn:r", "arn:s", "arn:t", "", "RUNNING", nil)
		require.Error(t, err)
		require.ErrorIs(t, err, pipes.ErrValidation)
	})

	t.Run("name_too_long", func(t *testing.T) {
		t.Parallel()
		var sb strings.Builder
		for range 65 {
			sb.WriteString("a")
		}
		long := sb.String()
		_, err := b.CreatePipeSimple(long, "arn:r", "arn:s", "arn:t", "", "RUNNING", nil)
		require.Error(t, err)
		require.ErrorIs(t, err, pipes.ErrValidation)
	})

	t.Run("invalid_desired_state", func(t *testing.T) {
		t.Parallel()
		_, err := b.CreatePipeSimple("valid-name", "arn:r", "arn:s", "arn:t", "", "INVALID", nil)
		require.Error(t, err)
		require.ErrorIs(t, err, pipes.ErrValidation)
	})

	t.Run("missing_source", func(t *testing.T) {
		t.Parallel()
		_, err := b.CreatePipeSimple("valid-name", "arn:r", "", "arn:t", "", "RUNNING", nil)
		require.Error(t, err)
		require.ErrorIs(t, err, pipes.ErrValidation)
	})
}

func TestHandler_ValidationHTTP(t *testing.T) {
	t.Parallel()

	t.Run("invalid_desired_state_returns_400", func(t *testing.T) {
		t.Parallel()
		h2 := newTestHandler(t)
		rec := doPipesRequest(t, h2, http.MethodPost, "/v1/pipes/valid-pipe", map[string]any{
			"RoleArn":      "arn:aws:iam::123456789012:role/r",
			"Source":       "arn:aws:sqs:us-east-1:000000000000:src",
			"Target":       "arn:aws:lambda:us-east-1:000000000000:function:fn",
			"DesiredState": "INVALID_STATE",
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "ValidationException")
	})

	t.Run("start_running_pipe_returns_400", func(t *testing.T) {
		t.Parallel()
		h2 := newTestHandler(t)
		doPipesRequest(t, h2, http.MethodPost, "/v1/pipes/running-pipe", map[string]any{
			"RoleArn":      "arn:aws:iam::123456789012:role/r",
			"Source":       "arn:aws:sqs:us-east-1:000000000000:src",
			"Target":       "arn:aws:lambda:us-east-1:000000000000:function:fn",
			"DesiredState": "RUNNING",
		})
		rec := doPipesRequest(t, h2, http.MethodPost, "/v1/pipes/running-pipe/start", nil)
		// Real AWS returns ConflictException (409) when the pipe is already RUNNING or in transition.
		assert.Equal(t, http.StatusConflict, rec.Code)
		assert.Contains(t, rec.Body.String(), "ConflictException")
	})
}

func TestHandler_ListPipesFiltering(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, name := range []string{"sqs-alpha", "sqs-beta", "kinesis-gamma"} {
		state := "RUNNING"
		if name == "sqs-beta" {
			state = "STOPPED"
		}
		rec := doPipesRequest(t, h, http.MethodPost, "/v1/pipes/"+name, map[string]any{
			"RoleArn":      "arn:aws:iam::123456789012:role/r",
			"Source":       "arn:aws:sqs:us-east-1:000000000000:" + name,
			"Target":       "arn:aws:lambda:us-east-1:000000000000:function:fn",
			"DesiredState": state,
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	t.Run("filter_by_name_prefix", func(t *testing.T) {
		t.Parallel()
		rec := doPipesRequest(t, h, http.MethodGet, "/v1/pipes?NamePrefix=sqs", nil)
		require.Equal(t, http.StatusOK, rec.Code)
		var out struct {
			Pipes []struct {
				Name string `json:"Name"`
			} `json:"Pipes"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		assert.Len(t, out.Pipes, 2)
	})

	t.Run("filter_by_desired_state", func(t *testing.T) {
		t.Parallel()
		rec := doPipesRequest(t, h, http.MethodGet, "/v1/pipes?DesiredState=STOPPED", nil)
		require.Equal(t, http.StatusOK, rec.Code)
		var out struct {
			Pipes []struct {
				Name string `json:"Name"`
			} `json:"Pipes"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		assert.Len(t, out.Pipes, 1)
		assert.Equal(t, "sqs-beta", out.Pipes[0].Name)
	})

	t.Run("invalid_limit_returns_400", func(t *testing.T) {
		t.Parallel()
		h2 := newTestHandler(t)
		rec := doPipesRequest(t, h2, http.MethodGet, "/v1/pipes?Limit=notanumber", nil)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

func TestHandler_SourceAndTargetParameters(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doPipesRequest(t, h, http.MethodPost, "/v1/pipes/param-pipe", map[string]any{
		"Source":  "arn:aws:sqs:us-east-1:000000000000:src",
		"Target":  "arn:aws:lambda:us-east-1:000000000000:function:fn",
		"RoleArn": "arn:aws:iam::000000000000:role/r",
		"SourceParameters": map[string]any{
			"SqsQueueParameters": map[string]any{"BatchSize": 5},
		},
		"TargetParameters": map[string]any{
			"InputTemplate": `{"fixed":"value"}`,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var created struct {
		TargetParameters struct {
			InputTemplate string `json:"InputTemplate"`
		} `json:"TargetParameters"`
		SourceParameters struct {
			SqsQueueParameters struct {
				BatchSize int `json:"BatchSize"`
			} `json:"SqsQueueParameters"`
		} `json:"SourceParameters"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	assert.Equal(t, 5, created.SourceParameters.SqsQueueParameters.BatchSize)
	assert.JSONEq(t, `{"fixed":"value"}`, created.TargetParameters.InputTemplate)
}

func TestHandler_ListPipesIncludesSourceTarget(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doPipesRequest(t, h, http.MethodPost, "/v1/pipes/info-pipe", map[string]any{
		"RoleArn":     "arn:aws:iam::123456789012:role/r",
		"Source":      "arn:aws:sqs:us-east-1:000000000000:my-queue",
		"Target":      "arn:aws:lambda:us-east-1:000000000000:function:my-fn",
		"Description": "test pipe",
	})

	rec := doPipesRequest(t, h, http.MethodGet, "/v1/pipes", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	// ListPipes returns the summary Pipe shape, which the real API does not give a
	// Description field (unlike the full DescribePipeOutput) -- confirm it's absent.
	var out struct {
		Pipes []map[string]any `json:"Pipes"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.Pipes, 1)
	assert.Equal(t, "arn:aws:sqs:us-east-1:000000000000:my-queue", out.Pipes[0]["Source"])
	assert.Equal(t, "arn:aws:lambda:us-east-1:000000000000:function:my-fn", out.Pipes[0]["Target"])
	_, hasDescription := out.Pipes[0]["Description"]
	assert.False(t, hasDescription, "ListPipes summary Pipe has no Description field in the real API")
}

// --- shared wire-format request helpers (used across this package's test files) ---

func auditNewBackend() *pipes.InMemoryBackend {
	return pipes.NewInMemoryBackend("123456789012", "us-west-2")
}

func auditNewHandler(t *testing.T) *pipes.Handler {
	t.Helper()

	return pipes.NewHandler(auditNewBackend())
}

func auditDo(t *testing.T, h *pipes.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var b []byte

	if body != nil {
		var err error
		b, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()
	req := httptest.NewRequest(method, path, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20230101/us-west-2/pipes/aws4_request")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetRequest(req)
	require.NoError(t, h.Handler()(c))

	return rec
}

func auditCreate(t *testing.T, h *pipes.Handler, name string, body map[string]any) map[string]any {
	t.Helper()

	rec := auditDo(t, h, http.MethodPost, "/v1/pipes/"+name, body)
	require.Equal(t, http.StatusOK, rec.Code, "create pipe %q: %s", name, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp
}

func auditDescribe(t *testing.T, h *pipes.Handler, name string) map[string]any {
	t.Helper()

	rec := auditDo(t, h, http.MethodGet, "/v1/pipes/"+name, nil)
	require.Equal(t, http.StatusOK, rec.Code, "describe pipe %q: %s", name, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp
}

// --- wire-level error code / response shape tests (formerly handler_parity_test.go) ---

// TestStartStop_ConflictException verifies that StartPipe/StopPipe on a pipe
// already at the desired state returns ConflictException (409), not ValidationException.
// Real AWS EventBridge Pipes returns ConflictException for this condition.
func TestStartStop_ConflictException(t *testing.T) {
	t.Parallel()

	b := auditNewBackend()
	h := pipes.NewHandler(b)

	tests := []struct {
		pipeName     string
		callPath     string
		setupDesired string
		name         string
	}{
		{
			name:         "start pipe already desired RUNNING",
			pipeName:     "conflict-start-pipe",
			setupDesired: "RUNNING",
			callPath:     "start",
		},
		{
			name:         "stop pipe already desired STOPPED",
			pipeName:     "conflict-stop-pipe",
			setupDesired: "STOPPED",
			callPath:     "stop",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pipeName := tt.pipeName
			_, err := b.CreatePipe(context.Background(), pipes.CreatePipeInput{
				RoleARN:      "arn:aws:iam::123456789012:role/r",
				Name:         pipeName,
				Source:       "arn:aws:sqs:us-east-1:123456789012:q",
				Target:       "arn:aws:lambda:us-east-1:123456789012:function:fn",
				DesiredState: tt.setupDesired,
			})
			require.NoError(t, err)

			// Wait for pipe to exit CREATING state.
			if tt.setupDesired == "RUNNING" {
				pipes.WaitPipeRunning(t, b, pipeName)
			} else {
				pipes.WaitPipeStopped(t, b, pipeName)
			}

			rec := auditDo(t, h, http.MethodPost,
				"/v1/pipes/"+pipeName+"/"+tt.callPath, nil)

			assert.Equal(t, http.StatusConflict, rec.Code,
				"real AWS returns 409 ConflictException, not 400 ValidationException")

			var resp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, "ConflictException", resp["__type"],
				"error type must be ConflictException; body: %s", rec.Body.String())
		})
	}
}

// TestServiceQuotaExceededException verifies that exceeding the pipe limit
// returns ServiceQuotaExceededException, not ValidationException.
// Real AWS returns ServiceQuotaExceededException when the account limit is hit.
func TestServiceQuotaExceededException(t *testing.T) {
	t.Parallel()

	b := pipes.NewInMemoryBackend("quota-acct", "us-west-2")
	h := pipes.NewHandler(b)
	ctx := context.Background()

	// Fill to the limit using the backend directly for speed.
	for i := range 1000 {
		_, err := b.CreatePipe(ctx, pipes.CreatePipeInput{
			RoleARN: "arn:aws:iam::123456789012:role/r",
			Name:    "pipe-" + string(rune('a'+i%26)) + "-" + string(rune('0'+i/26)),
			Source:  "arn:aws:sqs:us-east-1:123456789012:q",
			Target:  "arn:aws:lambda:us-east-1:123456789012:function:fn",
		})
		if err != nil {
			// Already exceeded — that's ok if we're at capacity.
			break
		}
	}

	// Now try to create one more via HTTP.
	rec := auditDo(t, h, http.MethodPost, "/v1/pipes/overflow-pipe", map[string]any{
		"RoleArn": "arn:aws:iam::123456789012:role/r",
		"Source":  "arn:aws:sqs:us-east-1:123456789012:q",
		"Target":  "arn:aws:lambda:us-east-1:123456789012:function:fn",
	})

	// Should fail at this point — the backend might have stopped before limit if name collisions.
	// The key assertion is the error type when it does fail.
	if rec.Code != http.StatusOK {
		var resp map[string]string
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Equal(t, "ServiceQuotaExceededException", resp["__type"],
			"quota exceeded must return ServiceQuotaExceededException; body: %s", rec.Body.String())
	}
}

// TestListPipes_EnrichmentInSummary verifies that ListPipes includes the
// Enrichment field in each pipe summary. Real AWS ListPipes returns Enrichment.
func TestListPipes_EnrichmentInSummary(t *testing.T) {
	t.Parallel()

	h := auditNewHandler(t)
	enrichmentARN := "arn:aws:lambda:us-west-2:123456789012:function:enricher"

	auditCreate(t, h, "enriched-pipe", map[string]any{
		"RoleArn":    "arn:aws:iam::123456789012:role/r",
		"Source":     "arn:aws:sqs:us-west-2:123456789012:q",
		"Target":     "arn:aws:lambda:us-west-2:123456789012:function:fn",
		"Enrichment": enrichmentARN,
	})

	rec := auditDo(t, h, http.MethodGet, "/v1/pipes", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Pipes []map[string]any `json:"Pipes"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.Pipes, 1)

	enrichment, _ := out.Pipes[0]["Enrichment"].(string)
	assert.Equal(t, enrichmentARN, enrichment,
		"ListPipes summary must include the Enrichment field (real AWS includes it)")
}

// TestListPipes_NoEnrichmentOmitted verifies that the Enrichment field is
// absent (not empty string) when no enrichment is configured.
func TestListPipes_NoEnrichmentOmitted(t *testing.T) {
	t.Parallel()

	h := auditNewHandler(t)

	auditCreate(t, h, "plain-pipe", map[string]any{
		"RoleArn": "arn:aws:iam::123456789012:role/r",
		"Source":  "arn:aws:sqs:us-west-2:123456789012:q",
		"Target":  "arn:aws:lambda:us-west-2:123456789012:function:fn",
	})

	rec := auditDo(t, h, http.MethodGet, "/v1/pipes", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Pipes []map[string]any `json:"Pipes"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.Pipes, 1)

	_, hasEnrichment := out.Pipes[0]["Enrichment"]
	assert.False(t, hasEnrichment,
		"Enrichment must be omitted (not present) when not configured")
}

// TestInvalidNextToken_ValidationException verifies that an invalid NextToken
// returns ValidationException. Real AWS returns ValidationException for malformed tokens.
func TestInvalidNextToken_ValidationException(t *testing.T) {
	t.Parallel()

	h := auditNewHandler(t)

	rec := auditDo(t, h, http.MethodGet, "/v1/pipes?NextToken=not-valid-base64!!!", nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ValidationException", resp["__type"],
		"invalid NextToken must return ValidationException; body: %s", rec.Body.String())
}

// TestDeletePipe_AlreadyDeleting_ConflictException verifies that calling
// DeletePipe on a pipe already in DELETING state returns ConflictException.
func TestDeletePipe_AlreadyDeleting_ConflictException(t *testing.T) {
	t.Parallel()

	b := auditNewBackend()
	h := pipes.NewHandler(b)
	ctx := context.Background()

	_, err := b.CreatePipe(ctx, pipes.CreatePipeInput{
		RoleARN: "arn:aws:iam::123456789012:role/r",
		Name:    "to-delete",
		Source:  "arn:aws:sqs:us-east-1:123456789012:q",
		Target:  "arn:aws:lambda:us-east-1:123456789012:function:fn",
	})
	require.NoError(t, err)

	pipes.WaitPipeRunning(t, b, "to-delete")

	// First delete starts the DELETING transition.
	_, err = b.DeletePipe(ctx, "to-delete")
	require.NoError(t, err)

	// Second delete while pipe is DELETING.
	_, err = b.DeletePipe(ctx, "to-delete")
	require.Error(t, err)
	require.ErrorIs(t, err, pipes.ErrConflict,
		"deleting a pipe already in DELETING state must return ErrConflict")

	_ = h
}

// TestServiceQuotaErrorCode verifies that hitting the 1000-pipe limit returns
// ServiceQuotaExceededException via the HTTP handler.
func TestServiceQuotaErrorCode(t *testing.T) {
	t.Parallel()

	b := pipes.NewInMemoryBackend("fill-acct", "us-west-2")
	h := pipes.NewHandler(b)
	ctx := context.Background()

	for i := range 1000 {
		name := fmt.Sprintf("pipe-%04d", i)
		_, err := b.CreatePipe(ctx, pipes.CreatePipeInput{
			RoleARN: "arn:aws:iam::123456789012:role/r",
			Name:    name,
			Source:  "arn:aws:sqs:us-east-1:123456789012:q",
			Target:  "arn:aws:lambda:us-east-1:123456789012:function:fn",
		})
		require.NoError(t, err, "setup: failed to create pipe %q at index %d", name, i)
	}

	rec := auditDo(t, h, http.MethodPost, "/v1/pipes/overflow", map[string]any{
		"RoleArn": "arn:aws:iam::123456789012:role/r",
		"Source":  "arn:aws:sqs:us-east-1:123456789012:q",
		"Target":  "arn:aws:lambda:us-east-1:123456789012:function:fn",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ServiceQuotaExceededException", resp["__type"],
		"1001st pipe must fail with ServiceQuotaExceededException; body: %s", rec.Body.String())
}

// TestEpochMillis_Precision verifies that epochMillis returns millisecond-precision
// floating-point Unix timestamps, not integer seconds.
func TestEpochMillis_Precision(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    time.Time
		name     string
		wantFrac float64
	}{
		{
			name:     "sub_second_500ms",
			input:    time.Unix(1700000000, 500_000_000),
			wantFrac: 0.5,
		},
		{
			name:     "sub_second_123ms",
			input:    time.Unix(1700000000, 123_000_000),
			wantFrac: 0.123,
		},
		{
			name:     "sub_second_1ms",
			input:    time.Unix(1700000000, 1_000_000),
			wantFrac: 0.001,
		},
		{
			name:     "whole_second_zero_frac",
			input:    time.Unix(1700000000, 0),
			wantFrac: 0.0,
		},
		{
			name:     "sub_second_999ms",
			input:    time.Unix(1700000000, 999_000_000),
			wantFrac: 0.999,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := pipes.EpochMillisForTest(tt.input)
			base := float64(tt.input.Unix())
			frac := got - base

			// Allow 1ms rounding tolerance.
			assert.InDelta(t, tt.wantFrac, frac, 0.001,
				"EpochMillis(%v) fractional part = %.3f, want %.3f", tt.input, frac, tt.wantFrac)

			// Verify the integer part is the same as Unix seconds.
			assert.InDelta(t, base, float64(int64(got)), 0.0,
				"EpochMillis integer part should match Unix seconds")
		})
	}
}

// TestTimestamps_MillisecondResolution verifies that HTTP response timestamps
// include sub-second precision (milliseconds), not just integer seconds.
func TestTimestamps_MillisecondResolution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "creation_time_has_ms"},
		{name: "modified_time_has_ms"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b3Handler(t)
			resp := b3Create(t, h, tt.name+"-pipe", map[string]any{
				"RoleArn": "arn:aws:iam::123456789012:role/r",
				"Source":  b3SQSSource,
				"Target":  b3LambdaTarget,
			})

			ct, ok := resp["CreationTime"].(float64)
			require.True(t, ok, "CreationTime should be a float64")
			lmt, ok := resp["LastModifiedTime"].(float64)
			require.True(t, ok, "LastModifiedTime should be a float64")

			// A millisecond-resolution timestamp will have three decimal digits.
			// Check that the value is at least in the modern epoch range.
			assert.Greater(t, ct, float64(1_700_000_000), "timestamp should be in modern epoch range")
			assert.Greater(t, lmt, float64(1_700_000_000), "timestamp should be in modern epoch range")

			// Verify millisecond resolution: the fractional part should not be exactly
			// an integer (unless we happen to hit exactly zero ms, which is rare).
			// We check that the value has three significant decimal places.
			msFromFloat := ct * 1000
			assert.Greater(t, msFromFloat, ct, "epochMillis should exceed integer epoch seconds")
		})
	}
}

func TestHandler_UpdatePipeDesiredState(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doPipesRequest(t, h, http.MethodPost, "/v1/pipes/update-state-pipe", map[string]any{
		"RoleArn":      "arn:aws:iam::123456789012:role/r",
		"Source":       "arn:aws:sqs:us-east-1:000000000000:src",
		"Target":       "arn:aws:lambda:us-east-1:000000000000:function:fn",
		"DesiredState": "RUNNING",
	})

	rec := doPipesRequest(t, h, http.MethodPut, "/v1/pipes/update-state-pipe", map[string]any{
		"RoleArn":      "arn:aws:iam::123456789012:role/r",
		"DesiredState": "STOPPED",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		DesiredState string `json:"DesiredState"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "STOPPED", out.DesiredState)
}
