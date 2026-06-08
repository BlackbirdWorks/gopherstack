package bedrockruntime_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/bedrockruntime"
)

func newTestHandler(t *testing.T) *bedrockruntime.Handler {
	t.Helper()

	return bedrockruntime.NewHandler(bedrockruntime.NewInMemoryBackend("000000000000", "us-east-1"))
}

func doRequest(
	t *testing.T,
	h *bedrockruntime.Handler,
	method, path string,
	body any,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()
	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// --- Handler metadata tests ---

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "BedrockRuntime", h.Name())
}

func TestHandler_ChaosServiceName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "bedrockruntime", h.ChaosServiceName())
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, service.PriorityPathVersioned, h.MatchPriority())
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	for _, op := range []string{
		"ApplyGuardrail",
		"Converse",
		"ConverseStream",
		"CountTokens",
		"GetAsyncInvoke",
		"InvokeModel",
		"InvokeModelWithBidirectionalStream",
		"InvokeModelWithResponseStream",
		"ListAsyncInvokes",
		"StartAsyncInvoke",
	} {
		assert.Contains(t, ops, op)
	}
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	tests := []struct {
		name  string
		path  string
		match bool
	}{
		{
			name:  "matches invoke path",
			path:  "/model/anthropic.claude-3-sonnet-20240229-v1:0/invoke",
			match: true,
		},
		{
			name:  "matches converse path",
			path:  "/model/anthropic.claude-v2/converse",
			match: true,
		},
		{
			name:  "does not match other path",
			path:  "/queues/myqueue",
			match: false,
		},
	}

	matcher := h.RouteMatcher()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.match, matcher(c))
		})
	}
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	tests := []struct {
		name   string
		path   string
		wantOp string
	}{
		{
			name:   "InvokeModel",
			path:   "/model/anthropic.claude-v2/invoke",
			wantOp: "InvokeModel",
		},
		{
			name:   "InvokeModelWithResponseStream",
			path:   "/model/anthropic.claude-v2/invoke-with-response-stream",
			wantOp: "InvokeModelWithResponseStream",
		},
		{
			name:   "Converse",
			path:   "/model/anthropic.claude-v2/converse",
			wantOp: "Converse",
		},
		{
			name:   "ConverseStream",
			path:   "/model/anthropic.claude-v2/converse-stream",
			wantOp: "ConverseStream",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	tests := []struct {
		name      string
		path      string
		wantModel string
	}{
		{
			name:      "claude model id",
			path:      "/model/anthropic.claude-v2/invoke",
			wantModel: "anthropic.claude-v2",
		},
		{
			name:      "titan model id",
			path:      "/model/amazon.titan-text-express-v1/converse",
			wantModel: "amazon.titan-text-express-v1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantModel, h.ExtractResource(c))
		})
	}
}

// --- InvokeModel tests ---

func TestHandler_InvokeModel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		modelID string
		body    map[string]any
		wantKey string
	}{
		{
			name:    "claude model returns completion",
			modelID: "anthropic.claude-v2",
			body:    map[string]any{"prompt": "Hello"},
			wantKey: "completion",
		},
		{
			name:    "titan model returns results",
			modelID: "amazon.titan-text-express-v1",
			body:    map[string]any{"inputText": "Hello"},
			wantKey: "results",
		},
		{
			name:    "llama model returns generation",
			modelID: "meta.llama2-13b-chat-v1",
			body:    map[string]any{"prompt": "Hello"},
			wantKey: "generation",
		},
		{
			name:    "unknown model returns completion",
			modelID: "unknown.model-v1",
			body:    map[string]any{"prompt": "Hello"},
			wantKey: "completion",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/model/"+tt.modelID+"/invoke", tt.body)

			assert.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Contains(t, out, tt.wantKey)

			invocations := h.Backend.ListInvocations()
			require.Len(t, invocations, 1)
			assert.Equal(t, "InvokeModel", invocations[0].Operation)
			assert.Equal(t, tt.modelID, invocations[0].ModelID)
		})
	}
}

// --- InvokeModelWithResponseStream tests ---

func TestHandler_InvokeModelWithResponseStream(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model/anthropic.claude-v2/invoke-with-response-stream",
		map[string]any{"prompt": "Hello"})

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/vnd.amazon.eventstream", rec.Header().Get("Content-Type"))
	assert.NotEmpty(t, rec.Body.Bytes())

	invocations := h.Backend.ListInvocations()
	require.Len(t, invocations, 1)
	assert.Equal(t, "InvokeModelWithResponseStream", invocations[0].Operation)
}

// --- Converse tests ---

func TestHandler_Converse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body    map[string]any
		name    string
		modelID string
	}{
		{
			name:    "basic converse",
			modelID: "anthropic.claude-3-sonnet-20240229-v1:0",
			body: map[string]any{
				"messages": []map[string]any{
					{"role": "user", "content": []map[string]any{{"text": "Hello"}}},
				},
			},
		},
		{
			name:    "converse with different model",
			modelID: "amazon.titan-text-express-v1",
			body: map[string]any{
				"messages": []map[string]any{
					{"role": "user", "content": []map[string]any{{"text": "What is 1+1?"}}},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/model/"+tt.modelID+"/converse", tt.body)

			assert.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Contains(t, out, "output")
			assert.Contains(t, out, "stopReason")

			outputVal, ok := out["output"].(map[string]any)
			require.True(t, ok)
			assert.Contains(t, outputVal, "message")

			invocations := h.Backend.ListInvocations()
			require.Len(t, invocations, 1)
			assert.Equal(t, "Converse", invocations[0].Operation)
			assert.Equal(t, tt.modelID, invocations[0].ModelID)
		})
	}
}

// --- ConverseStream tests ---

func TestHandler_ConverseStream(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model/anthropic.claude-v2/converse-stream",
		map[string]any{
			"messages": []map[string]any{
				{"role": "user", "content": []map[string]any{{"text": "Hello"}}},
			},
		})

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/vnd.amazon.eventstream", rec.Header().Get("Content-Type"))

	invocations := h.Backend.ListInvocations()
	require.Len(t, invocations, 1)
	assert.Equal(t, "ConverseStream", invocations[0].Operation)
}

// --- Error path tests ---

func TestHandler_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/model/anthropic.claude-v2/invoke", nil)

	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandler_MissingModelID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/model/", bytes.NewReader(nil))
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_UnknownOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model/anthropic.claude-v2/unknown-op", nil)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- Backend tests ---

func TestBackend_RecordAndList(t *testing.T) {
	t.Parallel()

	b := bedrockruntime.NewInMemoryBackend("123456789012", "us-east-1")
	assert.Equal(t, "us-east-1", b.Region())

	invocations := b.ListInvocations()
	assert.Empty(t, invocations)

	inv := b.RecordInvocation("InvokeModel", "anthropic.claude-v2", `{"prompt":"hi"}`, `{"completion":"hello"}`)
	assert.Equal(t, "InvokeModel", inv.Operation)
	assert.Equal(t, "anthropic.claude-v2", inv.ModelID)
	assert.NotZero(t, inv.CreatedAt)

	invocations = b.ListInvocations()
	require.Len(t, invocations, 1)
	assert.Equal(t, "InvokeModel", invocations[0].Operation)
}

func TestBackend_InvocationHistoryCap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		recordCount int
		wantLen     int
	}{
		{
			name:        "below_cap",
			recordCount: 10,
			wantLen:     10,
		},
		{
			name:        "at_cap",
			recordCount: bedrockruntime.MaxInvocationHistory,
			wantLen:     bedrockruntime.MaxInvocationHistory,
		},
		{
			name:        "above_cap_retains_most_recent",
			recordCount: bedrockruntime.MaxInvocationHistory + 50,
			wantLen:     bedrockruntime.MaxInvocationHistory,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrockruntime.NewInMemoryBackend("123456789012", "us-east-1")

			for i := range tt.recordCount {
				b.RecordInvocation("InvokeModel", "model", fmt.Sprintf(`{"seq":%d}`, i), `{}`)
			}

			invocations := b.ListInvocations()
			assert.Len(t, invocations, tt.wantLen)

			// Verify the most recent entries are retained (not the oldest).
			if tt.recordCount > bedrockruntime.MaxInvocationHistory {
				last := invocations[len(invocations)-1]
				assert.Contains(t, last.Input, fmt.Sprintf(`"seq":%d`, tt.recordCount-1))
			}
		})
	}
}

// --- CountTokens tests ---

func TestHandler_CountTokens(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body    map[string]any
		name    string
		modelID string
	}{
		{
			name:    "counts tokens for claude",
			modelID: "anthropic.claude-v2",
			body:    map[string]any{"prompt": "Hello, how are you?"},
		},
		{
			name:    "counts tokens for titan",
			modelID: "amazon.titan-text-express-v1",
			body:    map[string]any{"inputText": "Count these tokens"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/model/"+tt.modelID+"/count-tokens", tt.body)

			assert.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Contains(t, out, "inputTokens")

			invocations := h.Backend.ListInvocations()
			require.Len(t, invocations, 1)
			assert.Equal(t, "CountTokens", invocations[0].Operation)
			assert.Equal(t, tt.modelID, invocations[0].ModelID)
		})
	}
}

// --- InvokeModelWithBidirectionalStream tests ---

func TestHandler_InvokeModelWithBidirectionalStream(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model/anthropic.claude-v2/invoke-with-bidirectional-stream",
		map[string]any{"prompt": "Hello"})

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/vnd.amazon.eventstream", rec.Header().Get("Content-Type"))
	assert.NotEmpty(t, rec.Body.Bytes())

	invocations := h.Backend.ListInvocations()
	require.Len(t, invocations, 1)
	assert.Equal(t, "InvokeModelWithBidirectionalStream", invocations[0].Operation)
}

// --- ApplyGuardrail tests ---

func TestHandler_ApplyGuardrail(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body             map[string]any
		name             string
		guardrailID      string
		guardrailVersion string
	}{
		{
			name:             "applies guardrail to input content",
			guardrailID:      "my-guardrail-id",
			guardrailVersion: "DRAFT",
			body: map[string]any{
				"source":  "INPUT",
				"content": []map[string]any{{"text": map[string]any{"text": "Hello world"}}},
			},
		},
		{
			name:             "applies guardrail to output content",
			guardrailID:      "guardrail-abc123",
			guardrailVersion: "1",
			body: map[string]any{
				"source":  "OUTPUT",
				"content": []map[string]any{{"text": map[string]any{"text": "Some output text"}}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			path := "/guardrail/" + tt.guardrailID + "/version/" + tt.guardrailVersion + "/apply"
			rec := doRequest(t, h, http.MethodPost, path, tt.body)

			assert.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Contains(t, out, "action")
			assert.Contains(t, out, "assessments")
			assert.Contains(t, out, "outputs")
			assert.Contains(t, out, "usage")
			assert.Equal(t, "NONE", out["action"])
		})
	}
}

func TestHandler_ApplyGuardrail_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/guardrail/my-guardrail/version/1/apply", nil)

	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

// --- Async Invoke tests ---

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

// --- Backend async invoke tests ---

func TestBackend_AsyncInvoke_CRUD(t *testing.T) {
	t.Parallel()

	b := bedrockruntime.NewInMemoryBackend("123456789012", "us-east-1")

	// Initially empty.
	invocations := b.ListAsyncInvokes(bedrockruntime.ListAsyncInvokesFilter{})
	assert.Empty(t, invocations)

	// Start an invocation.
	inv, err := b.StartAsyncInvoke(
		"anthropic.claude-v2",
		"s3://bucket/prefix/",
		"token-1",
		map[string]string{"env": "test"},
	)
	require.NoError(t, err)
	require.NotNil(t, inv)
	assert.NotEmpty(t, inv.InvocationArn)
	assert.NotEmpty(t, inv.ModelArn)
	assert.Equal(t, "s3://bucket/prefix/", inv.OutputS3URI)
	assert.Equal(t, bedrockruntime.AsyncInvokeStatusInProgress, inv.Status)
	assert.NotZero(t, inv.SubmitTime)
	require.NotNil(t, inv.ClientRequestToken)
	assert.Equal(t, "token-1", *inv.ClientRequestToken)
	assert.Equal(t, map[string]string{"env": "test"}, inv.Tags)

	// GetAsyncInvoke returns the invocation.
	got, err := b.GetAsyncInvoke(inv.InvocationArn)
	require.NoError(t, err)
	assert.Equal(t, inv.InvocationArn, got.InvocationArn)
	assert.Equal(t, bedrockruntime.AsyncInvokeStatusInProgress, got.Status)

	// ListAsyncInvokes returns it.
	list := b.ListAsyncInvokes(bedrockruntime.ListAsyncInvokesFilter{})
	require.Len(t, list, 1)
	assert.Equal(t, inv.InvocationArn, list[0].InvocationArn)

	// GetAsyncInvoke returns error for unknown ARN.
	_, err = b.GetAsyncInvoke("arn:aws:bedrock:us-east-1:123456789012:async-invoke/nonexistent")
	require.Error(t, err)
}

func TestBackend_AsyncInvoke_MultipleInvocations(t *testing.T) {
	t.Parallel()

	b := bedrockruntime.NewInMemoryBackend("123456789012", "us-east-1")

	inv1, err := b.StartAsyncInvoke("model-1", "s3://bucket/1/", "", nil)
	require.NoError(t, err)
	inv2, err := b.StartAsyncInvoke("model-2", "s3://bucket/2/", "", nil)
	require.NoError(t, err)
	inv3, err := b.StartAsyncInvoke("model-3", "s3://bucket/3/", "", nil)
	require.NoError(t, err)

	assert.NotEqual(t, inv1.InvocationArn, inv2.InvocationArn)
	assert.NotEqual(t, inv2.InvocationArn, inv3.InvocationArn)

	list := b.ListAsyncInvokes(bedrockruntime.ListAsyncInvokesFilter{})
	assert.Len(t, list, 3)
}

func TestBackend_AsyncInvoke_NoClientToken(t *testing.T) {
	t.Parallel()

	b := bedrockruntime.NewInMemoryBackend("123456789012", "us-east-1")

	inv, err := b.StartAsyncInvoke("anthropic.claude-v2", "s3://bucket/prefix/", "", nil)
	require.NoError(t, err)
	require.NotNil(t, inv)
	assert.Nil(t, inv.ClientRequestToken)
}

// --- ExtractOperation for new operations ---

func TestHandler_ExtractOperation_NewOps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	tests := []struct {
		name   string
		method string
		path   string
		wantOp string
	}{
		{
			name:   "CountTokens",
			method: http.MethodPost,
			path:   "/model/anthropic.claude-v2/count-tokens",
			wantOp: "CountTokens",
		},
		{
			name:   "InvokeModelWithBidirectionalStream",
			method: http.MethodPost,
			path:   "/model/anthropic.claude-v2/invoke-with-bidirectional-stream",
			wantOp: "InvokeModelWithBidirectionalStream",
		},
		{
			name:   "ApplyGuardrail",
			method: http.MethodPost,
			path:   "/guardrail/my-id/version/1/apply",
			wantOp: "ApplyGuardrail",
		},
		{
			name:   "ListAsyncInvokes",
			method: http.MethodGet,
			path:   "/async-invoke",
			wantOp: "ListAsyncInvokes",
		},
		{
			name:   "StartAsyncInvoke",
			method: http.MethodPost,
			path:   "/async-invoke",
			wantOp: "StartAsyncInvoke",
		},
		{
			name:   "GetAsyncInvoke",
			method: http.MethodGet,
			path:   "/async-invoke/arn:aws:bedrock:us-east-1:000000000000:async-invoke/1",
			wantOp: "GetAsyncInvoke",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

// --- ExtractResource for new operations ---

func TestHandler_ExtractResource_NewOps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	tests := []struct {
		name         string
		method       string
		path         string
		wantResource string
	}{
		{
			name:         "guardrail resource",
			method:       http.MethodPost,
			path:         "/guardrail/my-guardrail-id/version/1/apply",
			wantResource: "my-guardrail-id",
		},
		{
			name:         "async invoke collection",
			method:       http.MethodGet,
			path:         "/async-invoke",
			wantResource: "",
		},
		{
			// Item path must return stable label "async-invoke", not the full ARN,
			// because ExtractResource is used as a low-cardinality telemetry label.
			name:         "async invoke by arn returns stable label",
			method:       http.MethodGet,
			path:         "/async-invoke/arn:aws:bedrock:us-east-1:000000000000:async-invoke/1",
			wantResource: "async-invoke",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantResource, h.ExtractResource(c))
		})
	}
}

// --- RouteMatcher for new operations ---

func TestHandler_RouteMatcher_NewOps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	tests := []struct {
		name  string
		path  string
		match bool
	}{
		{
			name:  "matches guardrail apply path",
			path:  "/guardrail/my-id/version/1/apply",
			match: true,
		},
		{
			name:  "matches async invoke list path",
			path:  "/async-invoke",
			match: true,
		},
		{
			name:  "matches async invoke get path",
			path:  "/async-invoke/arn:aws:bedrock:us-east-1:000000000000:async-invoke/1",
			match: true,
		},
	}

	matcher := h.RouteMatcher()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.match, matcher(c))
		})
	}
}

// --- Backend Reset ---

func TestBackend_Reset(t *testing.T) {
	t.Parallel()

	b := bedrockruntime.NewInMemoryBackend("123456789012", "us-east-1")

	// Add some state.
	b.RecordInvocation("InvokeModel", "model", `{"prompt":"hi"}`, `{}`)
	_, err := b.StartAsyncInvoke("model", "s3://bucket/", "token-abc", nil)
	require.NoError(t, err)

	require.Len(t, b.ListInvocations(), 1)
	require.Len(t, b.ListAsyncInvokes(bedrockruntime.ListAsyncInvokesFilter{}), 1)

	// Reset clears all state.
	b.Reset()

	assert.Empty(t, b.ListInvocations())
	assert.Empty(t, b.ListAsyncInvokes(bedrockruntime.ListAsyncInvokesFilter{}))

	// After reset the idempotency index is also cleared:
	// same token can be used again.
	inv, err := b.StartAsyncInvoke("model", "s3://bucket/", "token-abc", nil)
	require.NoError(t, err)
	assert.NotEmpty(t, inv.InvocationArn)
}

// --- Handler Reset ---

func TestHandler_Reset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	h.Backend.RecordInvocation("InvokeModel", "model", `{}`, `{}`)
	require.Len(t, h.Backend.ListInvocations(), 1)

	h.Reset()

	assert.Empty(t, h.Backend.ListInvocations())
}

// --- Chaos tests ---

func TestHandler_ChaosOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.ChaosOperations()

	require.NotEmpty(t, ops)

	for _, op := range h.GetSupportedOperations() {
		assert.Contains(t, ops, op)
	}
}

func TestHandler_ChaosRegions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	regions := h.ChaosRegions()

	require.Len(t, regions, 1)
	assert.Equal(t, "us-east-1", regions[0])
}

// --- Provider tests ---

func TestProvider_Name(t *testing.T) {
	t.Parallel()

	p := &bedrockruntime.Provider{}
	assert.Equal(t, "BedrockRuntime", p.Name())
}

func TestProvider_Init_NilContext(t *testing.T) {
	t.Parallel()

	p := &bedrockruntime.Provider{}
	reg, err := p.Init(nil)

	require.NoError(t, err)
	require.NotNil(t, reg)
}

func TestProvider_Init_WithConfig(t *testing.T) {
	t.Parallel()

	p := &bedrockruntime.Provider{}
	reg, err := p.Init(&service.AppContext{})

	require.NoError(t, err)
	require.NotNil(t, reg)
}

// --- Idempotency test ---

func TestBackend_StartAsyncInvoke_Idempotency(t *testing.T) {
	t.Parallel()

	b := bedrockruntime.NewInMemoryBackend("123456789012", "us-east-1")

	// First invocation with token.
	first, err := b.StartAsyncInvoke("anthropic.claude-v2", "s3://bucket/", "my-token", nil)
	require.NoError(t, err)

	// Second call with same token should return the same ARN.
	second, err := b.StartAsyncInvoke("anthropic.claude-v2", "s3://bucket/", "my-token", nil)
	require.NoError(t, err)
	assert.Equal(t, first.InvocationArn, second.InvocationArn)

	// Only one invocation stored.
	list := b.ListAsyncInvokes(bedrockruntime.ListAsyncInvokesFilter{})
	assert.Len(t, list, 1)

	// Different token creates a new invocation.
	third, err := b.StartAsyncInvoke("anthropic.claude-v2", "s3://bucket/", "other-token", nil)
	require.NoError(t, err)
	assert.NotEqual(t, first.InvocationArn, third.InvocationArn)

	// Empty token always creates a new invocation.
	a, err := b.StartAsyncInvoke("anthropic.claude-v2", "s3://bucket/", "", nil)
	require.NoError(t, err)
	aa, err := b.StartAsyncInvoke("anthropic.claude-v2", "s3://bucket/", "", nil)
	require.NoError(t, err)
	assert.NotEqual(t, a.InvocationArn, aa.InvocationArn)
}

// --- StartAsyncInvoke validation tests ---

func TestBackend_StartAsyncInvoke_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		modelID string
		s3URI   string
		wantErr bool
	}{
		{
			name:    "missing modelId returns error",
			modelID: "",
			s3URI:   "s3://bucket/",
			wantErr: true,
		},
		{
			name:    "missing s3Uri returns error",
			modelID: "anthropic.claude-v2",
			s3URI:   "",
			wantErr: true,
		},
		{
			name:    "valid params succeeds",
			modelID: "anthropic.claude-v2",
			s3URI:   "s3://bucket/",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrockruntime.NewInMemoryBackend("123456789012", "us-east-1")
			_, err := b.StartAsyncInvoke(tt.modelID, tt.s3URI, "", nil)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
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

// --- ListAsyncInvokes status filter test ---

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

// --- StartAsyncInvoke with tags ---

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

// --- Purge handler test ---

func TestHandler_Purge(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	h.Backend.RecordInvocation("InvokeModel", "model", `{}`, `{}`)

	require.Len(t, h.Backend.ListInvocations(), 1)

	// Purge with future cutoff removes all.
	h.Purge(t.Context(), time.Now().Add(time.Hour))

	assert.Empty(t, h.Backend.ListInvocations())
}

// --- ApplyGuardrail invalid path test ---

func TestHandler_ApplyGuardrail_InvalidPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		code int
	}{
		{
			name: "missing guardrailId returns 400",
			path: "/guardrail//version/1/apply",
			code: http.StatusBadRequest,
		},
		{
			name: "missing version returns 400",
			path: "/guardrail/my-id/version//apply",
			code: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, tt.path, map[string]any{"source": "INPUT"})
			assert.Equal(t, tt.code, rec.Code)
		})
	}
}

// --- InvokeModel model family tests ---

func TestHandler_InvokeModel_ModelFamilies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		modelID string
		wantKey string
	}{
		{
			name:    "mistral model",
			modelID: "mistral.mistral-7b-instruct-v0:2",
			wantKey: "outputs",
		},
		{
			name:    "command model",
			modelID: "cohere.command-r-v1:0",
			wantKey: "text",
		},
		{
			name:    "nova model",
			modelID: "amazon.nova-pro-v1:0",
			wantKey: "output",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/model/"+tt.modelID+"/invoke",
				map[string]any{"prompt": "Hello"})

			assert.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Contains(t, out, tt.wantKey)
		})
	}
}

// --- Guardrail unknown operation test ---

func TestHandler_GuardrailPath_UnknownOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/guardrail/my-id/version/1/unknown", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- StartAsyncInvoke invalid JSON test ---

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

// --- GetAsyncInvoke full fields test ---

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

// --- ListAsyncInvokes with client request token in summary ---

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

// --- Provider Init with full config ---

func TestProvider_Init_FullConfig(t *testing.T) {
	t.Parallel()

	p := &bedrockruntime.Provider{}
	ctx := &service.AppContext{Config: &mockProviderConfig{accountID: "111122223333", region: "eu-west-1"}}
	reg, err := p.Init(ctx)

	require.NoError(t, err)
	require.NotNil(t, reg)

	h, ok := reg.(*bedrockruntime.Handler)
	require.True(t, ok)
	assert.Equal(t, "eu-west-1", h.Backend.Region())
}

// mockProviderConfig implements config.Provider for testing.
type mockProviderConfig struct {
	accountID string
	region    string
}

func (m *mockProviderConfig) GetGlobalConfig() *config.GlobalConfig {
	const noLatency = 0
	const noJanitorTimeout = 0
	const noAutoPurgeTTL = 0

	return config.NewGlobalConfig(m.accountID, m.region, noLatency, noJanitorTimeout, false, noAutoPurgeTTL)
}
