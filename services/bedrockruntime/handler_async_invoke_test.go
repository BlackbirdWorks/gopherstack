package bedrockruntime_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrockruntime"
)

// --- StartAsyncInvoke tests ---

func TestHandler_StartAsyncInvoke(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body    map[string]any
		name    string
		errCode int
		wantErr bool
	}{
		{
			name: "starts async invoke successfully",
			body: map[string]any{
				"modelId":    "anthropic.claude-v2",
				"modelInput": map[string]any{"prompt": "Hello"},
				"outputDataConfig": map[string]any{
					"s3OutputDataConfig": map[string]any{
						"s3Uri": "s3://my-bucket/output/",
					},
				},
			},
		},
		{
			name: "starts async invoke with client request token",
			body: map[string]any{
				"modelId":            "amazon.titan-text-express-v1",
				"modelInput":         map[string]any{"inputText": "Hello"},
				"clientRequestToken": "my-idempotency-token",
				"outputDataConfig": map[string]any{
					"s3OutputDataConfig": map[string]any{
						"s3Uri": "s3://my-bucket/output/",
					},
				},
			},
		},
		{
			name:    "fails when modelId is missing",
			body:    map[string]any{"modelInput": map[string]any{"prompt": "Hello"}},
			wantErr: true,
			errCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/async-invoke", tt.body)

			if tt.wantErr {
				assert.Equal(t, tt.errCode, rec.Code)

				return
			}

			assert.Equal(t, http.StatusAccepted, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Contains(t, out, "invocationArn")
			assert.NotEmpty(t, out["invocationArn"])
		})
	}
}

func TestHandler_StartAsyncInvoke_MissingS3URI(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/async-invoke", map[string]any{
		"modelId":    "anthropic.claude-v2",
		"modelInput": map[string]any{"prompt": "Hello"},
		// outputDataConfig present but s3Uri empty
		"outputDataConfig": map[string]any{
			"s3OutputDataConfig": map[string]any{
				"s3Uri": "",
			},
		},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Contains(t, out, "__type")
}

func TestHandler_StartAsyncInvoke_WithTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/async-invoke", map[string]any{
		"modelId": "anthropic.claude-v2",
		"outputDataConfig": map[string]any{
			"s3OutputDataConfig": map[string]any{
				"s3Uri": "s3://bucket/output/",
			},
		},
		"tags": map[string]string{"env": "prod", "team": "ml"},
	})

	require.Equal(t, http.StatusAccepted, rec.Code)

	var startOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startOut))
	invocationArn := startOut["invocationArn"].(string)

	getPath := "/async-invoke/" + url.PathEscape(invocationArn)
	getRec := doRequest(t, h, http.MethodGet, getPath, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
	assert.Contains(t, getOut, "tags")
}

func TestHandler_StartAsyncInvoke_InvalidJSON(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	req := httptest.NewRequest(http.MethodPost, "/async-invoke", bytes.NewReader([]byte("not-json")))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestStartAsyncInvoke_InvalidS3URI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		s3URI string
	}{
		{name: "no_scheme", s3URI: "my-bucket/prefix/"},
		{name: "http_scheme", s3URI: "http://my-bucket/prefix/"},
		{name: "empty_bucket", s3URI: "s3:///prefix/"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/async-invoke",
				map[string]any{
					"modelId": "anthropic.claude-v2",
					"outputDataConfig": map[string]any{
						"s3OutputDataConfig": map[string]any{
							"s3Uri": tt.s3URI,
						},
					},
				})

			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestStartAsyncInvoke_ValidS3URI(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/async-invoke",
		map[string]any{
			"modelId": "anthropic.claude-v2",
			"outputDataConfig": map[string]any{
				"s3OutputDataConfig": map[string]any{
					"s3Uri": "s3://valid-bucket/prefix/",
				},
			},
		})

	assert.Equal(t, http.StatusAccepted, rec.Code)
}

func TestStartAsyncInvoke_InferenceProfileIdentifier(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/async-invoke",
		map[string]any{
			"inferenceProfileIdentifier": "arn:aws:bedrock:us-east-1::inference-profile/" +
				"us.anthropic.claude-3-sonnet-20240229-v1-0",
			"outputDataConfig": map[string]any{
				"s3OutputDataConfig": map[string]any{
					"s3Uri": "s3://valid-bucket/output/",
				},
			},
		})

	assert.Equal(t, http.StatusAccepted, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.NotEmpty(t, out["invocationArn"])
}

func TestAsyncInvoke_MissingS3URI_Returns400(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
	}{
		{
			name: "missing outputDataConfig",
			body: map[string]any{"modelId": "anthropic.claude-v2"},
		},
		{
			name: "missing s3Uri",
			body: map[string]any{
				"modelId": "anthropic.claude-v2",
				"outputDataConfig": map[string]any{
					"s3OutputDataConfig": map[string]any{"s3Uri": ""},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/async-invoke", tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// --- GetAsyncInvoke tests ---

func TestHandler_GetAsyncInvoke(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantErr bool
		errCode int
	}{
		{
			name: "gets existing async invoke",
		},
		{
			name:    "returns 404 for unknown ARN",
			wantErr: true,
			errCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if !tt.wantErr {
				// Start an invocation first.
				startBody := map[string]any{
					"modelId":    "anthropic.claude-v2",
					"modelInput": map[string]any{"prompt": "Hello"},
					"outputDataConfig": map[string]any{
						"s3OutputDataConfig": map[string]any{
							"s3Uri": "s3://my-bucket/output/",
						},
					},
				}
				startRec := doRequest(t, h, http.MethodPost, "/async-invoke", startBody)
				require.Equal(t, http.StatusAccepted, startRec.Code)

				var startOut map[string]any
				require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startOut))
				invocationArn := startOut["invocationArn"].(string)

				getPath := "/async-invoke/" + url.PathEscape(invocationArn)
				rec := doRequest(t, h, http.MethodGet, getPath, nil)

				assert.Equal(t, http.StatusOK, rec.Code)

				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, invocationArn, out["invocationArn"])
				assert.Contains(t, out, "modelArn")
				assert.Contains(t, out, "status")
				assert.Contains(t, out, "submitTime")
				assert.Contains(t, out, "outputDataConfig")
			} else {
				unknownArn := "arn:aws:bedrock:us-east-1:000000000000:async-invoke/nonexistent"
				getPath := "/async-invoke/" + url.PathEscape(unknownArn)
				rec := doRequest(t, h, http.MethodGet, getPath, nil)

				assert.Equal(t, tt.errCode, rec.Code)
			}
		})
	}
}

func TestHandler_GetAsyncInvoke_FullFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Start with token and tags.
	startRec := doRequest(t, h, http.MethodPost, "/async-invoke", map[string]any{
		"modelId":            "anthropic.claude-v2",
		"clientRequestToken": "my-idempotency-token",
		"outputDataConfig": map[string]any{
			"s3OutputDataConfig": map[string]any{"s3Uri": "s3://bucket/output/"},
		},
		"tags": map[string]string{"env": "test"},
	})
	require.Equal(t, http.StatusAccepted, startRec.Code)

	var startOut map[string]any
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startOut))
	invocationArn := startOut["invocationArn"].(string)

	getPath := "/async-invoke/" + url.PathEscape(invocationArn)
	rec := doRequest(t, h, http.MethodGet, getPath, nil)

	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Contains(t, out, "clientRequestToken")
	assert.Equal(t, "my-idempotency-token", out["clientRequestToken"])
	assert.Contains(t, out, "tags")
}

func TestAsyncInvoke_GetResponseShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		modelID  string
		s3URI    string
		tokenKey string
	}{
		{
			name:     "claude async invoke",
			modelID:  "anthropic.claude-v2",
			s3URI:    "s3://my-bucket/results/",
			tokenKey: "unique-token-1",
		},
		{
			name:     "titan async invoke",
			modelID:  "amazon.titan-text-express-v1",
			s3URI:    "s3://other-bucket/output/",
			tokenKey: "unique-token-2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			recCreate := doRequest(
				t, h, http.MethodPost, "/async-invoke",
				map[string]any{
					"modelId": tt.modelID,
					"outputDataConfig": map[string]any{
						"s3OutputDataConfig": map[string]any{"s3Uri": tt.s3URI},
					},
					"clientRequestToken": tt.tokenKey,
				},
			)
			require.Equal(t, http.StatusAccepted, recCreate.Code)

			var createOut map[string]any
			require.NoError(t, json.Unmarshal(recCreate.Body.Bytes(), &createOut))
			arn := createOut["invocationArn"].(string)

			recGet := doRequest(t, h, http.MethodGet, "/async-invoke/"+arn, nil)
			require.Equal(t, http.StatusOK, recGet.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(recGet.Body.Bytes(), &out))
			assert.Equal(t, arn, out["invocationArn"])
			assert.Contains(t, out["modelArn"].(string), tt.modelID)
			assert.Equal(t, "InProgress", out["status"])
			assert.NotEmpty(t, out["submitTime"])
			_, hasEndTime := out["endTime"]
			assert.False(t, hasEndTime, "endTime must be absent while InProgress")
		})
	}
}

func TestAsyncInvoke_GetNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/async-invoke/nonexistent-arn", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- ListAsyncInvokes tests ---

func TestHandler_ListAsyncInvokes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		invokeCount   int
		wantSummaries int
	}{
		{
			name:          "empty list",
			invokeCount:   0,
			wantSummaries: 0,
		},
		{
			name:          "lists single invocation",
			invokeCount:   1,
			wantSummaries: 1,
		},
		{
			name:          "lists multiple invocations",
			invokeCount:   3,
			wantSummaries: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for i := range tt.invokeCount {
				startBody := map[string]any{
					"modelId":    fmt.Sprintf("model-%d", i),
					"modelInput": map[string]any{"prompt": "Hello"},
					"outputDataConfig": map[string]any{
						"s3OutputDataConfig": map[string]any{
							"s3Uri": fmt.Sprintf("s3://my-bucket/output/%d/", i),
						},
					},
				}
				startRec := doRequest(t, h, http.MethodPost, "/async-invoke", startBody)
				require.Equal(t, http.StatusAccepted, startRec.Code)
			}

			rec := doRequest(t, h, http.MethodGet, "/async-invoke", nil)

			assert.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Contains(t, out, "asyncInvokeSummaries")

			summaries, ok := out["asyncInvokeSummaries"].([]any)
			require.True(t, ok)
			assert.Len(t, summaries, tt.wantSummaries)
		})
	}
}

func TestHandler_ListAsyncInvokes_StatusFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		statusFilter string
		wantCount    int
	}{
		{
			name:         "no filter returns all",
			statusFilter: "",
			wantCount:    2,
		},
		{
			name:         "filter InProgress returns matching",
			statusFilter: bedrockruntime.AsyncInvokeStatusInProgress,
			wantCount:    2,
		},
		{
			name:         "filter Completed returns none",
			statusFilter: bedrockruntime.AsyncInvokeStatusCompleted,
			wantCount:    0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			startBody := func(i int) map[string]any {
				return map[string]any{
					"modelId": fmt.Sprintf("model-%d", i),
					"outputDataConfig": map[string]any{
						"s3OutputDataConfig": map[string]any{
							"s3Uri": fmt.Sprintf("s3://bucket/%d/", i),
						},
					},
				}
			}

			rec1 := doRequest(t, h, http.MethodPost, "/async-invoke", startBody(1))
			require.Equal(t, http.StatusAccepted, rec1.Code)
			rec2 := doRequest(t, h, http.MethodPost, "/async-invoke", startBody(2))
			require.Equal(t, http.StatusAccepted, rec2.Code)

			path := "/async-invoke"
			if tt.statusFilter != "" {
				path += "?statusEquals=" + tt.statusFilter
			}

			rec := doRequest(t, h, http.MethodGet, path, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			summaries, ok := out["asyncInvokeSummaries"].([]any)
			require.True(t, ok)
			assert.Len(t, summaries, tt.wantCount)
		})
	}
}

func TestHandler_ListAsyncInvokes_WithClientToken(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/async-invoke", map[string]any{
		"modelId":            "anthropic.claude-v2",
		"clientRequestToken": "summary-token",
		"outputDataConfig": map[string]any{
			"s3OutputDataConfig": map[string]any{"s3Uri": "s3://bucket/"},
		},
	})
	require.Equal(t, http.StatusAccepted, rec.Code)

	listRec := doRequest(t, h, http.MethodGet, "/async-invoke", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))

	summaries, ok := out["asyncInvokeSummaries"].([]any)
	require.True(t, ok)
	require.Len(t, summaries, 1)

	summary := summaries[0].(map[string]any)
	assert.Equal(t, "summary-token", summary["clientRequestToken"])
}

func TestListAsyncInvokes_EmptyList(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/async-invoke", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	invokes, ok := out["asyncInvokeSummaries"].([]any)
	require.True(t, ok)
	assert.Empty(t, invokes)
}

func TestListAsyncInvokes_AfterCreate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		invokeURLs []string
		wantLen    int
	}{
		{
			name:       "single invocation",
			invokeURLs: []string{"s3://bucket/output/1/"},
			wantLen:    1,
		},
		{
			name:       "three invocations",
			invokeURLs: []string{"s3://bucket/out/1/", "s3://bucket/out/2/", "s3://bucket/out/3/"},
			wantLen:    3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for i, s3URL := range tt.invokeURLs {
				rec := doRequest(
					t, h, http.MethodPost, "/async-invoke",
					map[string]any{
						"modelId": "anthropic.claude-v2",
						"outputDataConfig": map[string]any{
							"s3OutputDataConfig": map[string]any{"s3Uri": s3URL},
						},
						"clientRequestToken": fmt.Sprintf("token-%d", i),
					},
				)
				require.Equal(t, http.StatusAccepted, rec.Code)
			}

			recList := doRequest(t, h, http.MethodGet, "/async-invoke", nil)
			require.Equal(t, http.StatusOK, recList.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &out))
			invokes := out["asyncInvokeSummaries"].([]any)
			assert.Len(t, invokes, tt.wantLen)

			for _, raw := range invokes {
				inv := raw.(map[string]any)
				assert.NotEmpty(t, inv["invocationArn"])
				assert.NotEmpty(t, inv["submitTime"])
				assert.Equal(t, "InProgress", inv["status"])
			}
		})
	}
}

// --- Method-not-allowed test ---

func TestHandler_AsyncInvoke_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{
			name:   "PUT on /async-invoke",
			method: http.MethodPut,
			path:   "/async-invoke",
		},
		{
			name:   "DELETE on /async-invoke",
			method: http.MethodDelete,
			path:   "/async-invoke",
		},
		{
			name:   "POST on /async-invoke/{arn}",
			method: http.MethodPost,
			path:   "/async-invoke/arn:aws:bedrock:us-east-1:000000000000:async-invoke%2F1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.method, tt.path, nil)

			assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
		})
	}
}
