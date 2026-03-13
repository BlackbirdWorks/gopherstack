package pipes_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

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

	p, err := b.CreatePipe("tag-pipe", "arn:aws:iam::000000000000:role/r",
		"arn:aws:sqs:us-east-1:000000000000:src",
		"arn:aws:lambda:us-east-1:000000000000:function:fn",
		"", "RUNNING", nil)
	require.NoError(t, err)

	err = b.TagResource(p.ARN, map[string]string{"env": "test"})
	require.NoError(t, err)

	tags, err := b.ListTagsForResource(p.ARN)
	require.NoError(t, err)
	assert.Equal(t, "test", tags["env"])
}

func TestBackend_DuplicatePipe(t *testing.T) {
	t.Parallel()

	b := pipes.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.CreatePipe("dupe", "arn:aws:iam::000000000000:role/r",
		"arn:aws:sqs:us-east-1:000000000000:src",
		"arn:aws:lambda:us-east-1:000000000000:function:fn",
		"", "RUNNING", nil)
	require.NoError(t, err)

	_, err = b.CreatePipe("dupe", "arn:aws:iam::000000000000:role/r",
		"arn:aws:sqs:us-east-1:000000000000:src",
		"arn:aws:lambda:us-east-1:000000000000:function:fn",
		"", "RUNNING", nil)
	require.Error(t, err)
}

func TestHandler_TagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *pipes.Handler) string
		name       string
		pipeName   string
		wantStatus int
	}{
		{
			name:     "tag_resource",
			pipeName: "tag-test-pipe",
			setup: func(t *testing.T, h *pipes.Handler) string {
				t.Helper()
				rec := doPipesRequest(t, h, http.MethodPost, "/v1/pipes/tag-test-pipe", map[string]any{
					"RoleArn": "arn:aws:iam::000000000000:role/r",
					"Source":  "arn:aws:sqs:us-east-1:000000000000:src",
					"Target":  "arn:aws:lambda:us-east-1:000000000000:function:fn",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				return resp["Arn"].(string)
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
				rec := doPipesRequest(t, h, http.MethodPost, "/v1/pipes/list-tags-pipe", map[string]any{
					"RoleArn": "arn:aws:iam::000000000000:role/r",
					"Source":  "arn:aws:sqs:us-east-1:000000000000:src",
					"Target":  "arn:aws:lambda:us-east-1:000000000000:function:fn",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				arn := resp["Arn"].(string)
				tagRec := doPipesRequest(t, h, http.MethodPost, "/tags/"+arn,
					map[string]any{"Tags": map[string]string{"env": "prod"}})
				require.Equal(t, http.StatusOK, tagRec.Code)

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
				rec := doPipesRequest(t, h, http.MethodPost, "/v1/pipes/untag-pipe", map[string]any{
					"RoleArn": "arn:aws:iam::000000000000:role/r",
					"Source":  "arn:aws:sqs:us-east-1:000000000000:src",
					"Target":  "arn:aws:lambda:us-east-1:000000000000:function:fn",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				arn := resp["Arn"].(string)
				tagRec := doPipesRequest(t, h, http.MethodPost, "/tags/"+arn,
					map[string]any{"Tags": map[string]string{"env": "test", "team": "platform"}})
				require.Equal(t, http.StatusOK, tagRec.Code)

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
