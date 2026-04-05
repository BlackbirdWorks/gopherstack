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
		"InvokeModel",
		"InvokeModelWithResponseStream",
		"Converse",
		"ConverseStream",
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
	invocations := b.ListAsyncInvokes()
	assert.Empty(t, invocations)

	// Start an invocation.
	inv := b.StartAsyncInvoke("anthropic.claude-v2", "s3://bucket/prefix/", "token-1")
	require.NotNil(t, inv)
	assert.NotEmpty(t, inv.InvocationArn)
	assert.NotEmpty(t, inv.ModelArn)
	assert.Equal(t, "s3://bucket/prefix/", inv.OutputS3URI)
	assert.Equal(t, bedrockruntime.AsyncInvokeStatusInProgress, inv.Status)
	assert.NotZero(t, inv.SubmitTime)
	require.NotNil(t, inv.ClientRequestToken)
	assert.Equal(t, "token-1", *inv.ClientRequestToken)

	// GetAsyncInvoke returns the invocation.
	got, ok := b.GetAsyncInvoke(inv.InvocationArn)
	require.True(t, ok)
	assert.Equal(t, inv.InvocationArn, got.InvocationArn)
	assert.Equal(t, bedrockruntime.AsyncInvokeStatusInProgress, got.Status)

	// ListAsyncInvokes returns it.
	list := b.ListAsyncInvokes()
	require.Len(t, list, 1)
	assert.Equal(t, inv.InvocationArn, list[0].InvocationArn)

	// GetAsyncInvoke returns false for unknown ARN.
	_, ok = b.GetAsyncInvoke("arn:aws:bedrock:us-east-1:123456789012:async-invoke/nonexistent")
	assert.False(t, ok)
}

func TestBackend_AsyncInvoke_MultipleInvocations(t *testing.T) {
	t.Parallel()

	b := bedrockruntime.NewInMemoryBackend("123456789012", "us-east-1")

	inv1 := b.StartAsyncInvoke("model-1", "s3://bucket/1/", "")
	inv2 := b.StartAsyncInvoke("model-2", "s3://bucket/2/", "")
	inv3 := b.StartAsyncInvoke("model-3", "s3://bucket/3/", "")

	assert.NotEqual(t, inv1.InvocationArn, inv2.InvocationArn)
	assert.NotEqual(t, inv2.InvocationArn, inv3.InvocationArn)

	list := b.ListAsyncInvokes()
	assert.Len(t, list, 3)
}

func TestBackend_AsyncInvoke_NoClientToken(t *testing.T) {
	t.Parallel()

	b := bedrockruntime.NewInMemoryBackend("123456789012", "us-east-1")

	inv := b.StartAsyncInvoke("anthropic.claude-v2", "s3://bucket/prefix/", "")
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
			name:         "async invoke by arn",
			method:       http.MethodGet,
			path:         "/async-invoke/arn:aws:bedrock:us-east-1:000000000000:async-invoke/1",
			wantResource: "arn:aws:bedrock:us-east-1:000000000000:async-invoke/1",
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
