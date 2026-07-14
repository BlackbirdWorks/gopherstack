package bedrockruntime_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrockruntime"
)

// ─────────────────────────────────────────────────────────────
// InvokeModel — model family accuracy
// ─────────────────────────────────────────────────────────────

func TestAccuracy_InvokeModel_MultipleModelFamilies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		modelID string
	}{
		{name: "claude v2", modelID: "anthropic.claude-v2"},
		{name: "claude 3 sonnet", modelID: "anthropic.claude-3-sonnet-20240229-v1:0"},
		{name: "titan text", modelID: "amazon.titan-text-express-v1"},
		{name: "llama 3", modelID: "meta.llama3-8b-instruct-v1:0"},
		{name: "mistral", modelID: "mistral.mistral-7b-instruct-v0:2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(
				t, h, http.MethodPost,
				"/model/"+tt.modelID+"/invoke",
				map[string]any{"prompt": "Hello"},
			)
			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
		})
	}
}

// ─────────────────────────────────────────────────────────────
// Converse — response shape accuracy
// ─────────────────────────────────────────────────────────────

func TestAccuracy_Converse_ResponseFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		body    map[string]any
		wantKey string
	}{
		{
			name: "with messages",
			body: map[string]any{
				"messages": []map[string]any{
					{"role": "user", "content": []map[string]any{{"type": "text", "text": "Hello"}}},
				},
			},
			wantKey: "output",
		},
		{
			name:    "empty body",
			body:    nil,
			wantKey: "output",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/model/anthropic.claude-v2/converse", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Contains(t, out, tt.wantKey)
			assert.Contains(t, out, "usage")
			assert.Contains(t, out, "stopReason")

			usage := out["usage"].(map[string]any)
			assert.Contains(t, usage, "inputTokens")
			assert.Contains(t, usage, "outputTokens")
			assert.Contains(t, usage, "totalTokens")
		})
	}
}

func TestAccuracy_Converse_WithSystemMessages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		systemText string
	}{
		{name: "short system", systemText: "You are a helpful assistant."},
		{name: "long system", systemText: fmt.Sprintf("System prompt with %d chars.", 1000)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(
				t, h, http.MethodPost, "/model/anthropic.claude-v2/converse",
				map[string]any{
					"messages": []map[string]any{
						{"role": "user", "content": []map[string]any{{"type": "text", "text": "Hi"}}},
					},
					"system": []map[string]any{
						{"text": tt.systemText},
					},
				},
			)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Contains(t, out, "output")
		})
	}
}

func TestAccuracy_Converse_StopReasonEndTurn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(
		t, h, http.MethodPost, "/model/anthropic.claude-v2/converse",
		map[string]any{
			"messages": []map[string]any{
				{"role": "user", "content": []map[string]any{{"type": "text", "text": "Tell me a joke"}}},
			},
		},
	)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "end_turn", out["stopReason"])
}

// ─────────────────────────────────────────────────────────────
// CountTokens — accuracy across model families
// ─────────────────────────────────────────────────────────────

func TestAccuracy_CountTokens_MultipleModelFamilies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		modelID  string
		text     string
		minCount int
	}{
		{name: "claude short text", modelID: "anthropic.claude-v2", text: "Hello world", minCount: 1},
		{name: "titan short text", modelID: "amazon.titan-text-express-v1", text: "Hello world", minCount: 1},
		{
			name:     "claude long text",
			modelID:  "anthropic.claude-v2",
			text:     string(make([]byte, 1000)),
			minCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(
				t, h, http.MethodPost,
				"/model/"+tt.modelID+"/count-tokens",
				countTokensInvokeModelBody(fmt.Sprintf(`{"prompt":%q}`, tt.text)),
			)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			count, ok := out["inputTokens"].(float64)
			require.True(t, ok, "inputTokens should be a number")
			assert.GreaterOrEqual(t, int(count), tt.minCount)
		})
	}
}

func TestAccuracy_CountTokens_TitanUsesHigherDivisor(t *testing.T) {
	t.Parallel()

	text := "This is a reasonably sized piece of text for token counting purposes, with several sentences and words."

	h := newTestHandler(t)

	body := countTokensInvokeModelBody(fmt.Sprintf(`{"prompt":%q}`, text))

	recClaude := doRequest(t, h, http.MethodPost, "/model/anthropic.claude-v2/count-tokens", body)
	require.Equal(t, http.StatusOK, recClaude.Code)

	recTitan := doRequest(t, h, http.MethodPost, "/model/amazon.titan-text-express-v1/count-tokens", body)
	require.Equal(t, http.StatusOK, recTitan.Code)

	var claudeOut, titanOut map[string]any
	require.NoError(t, json.Unmarshal(recClaude.Body.Bytes(), &claudeOut))
	require.NoError(t, json.Unmarshal(recTitan.Body.Bytes(), &titanOut))

	claudeTokens := claudeOut["inputTokens"].(float64)
	titanTokens := titanOut["inputTokens"].(float64)
	// Titan uses ~6 chars/token vs ~4 for Claude → fewer tokens for same text.
	assert.GreaterOrEqual(t, claudeTokens, titanTokens, "claude tokenizer should produce more tokens than titan")
}

func TestAccuracy_CountTokens_MultiMessageBody(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		messages []map[string]any
		minCount int
	}{
		{
			name: "two messages",
			messages: []map[string]any{
				{"role": "user", "content": []map[string]any{{"type": "text", "text": "Question one"}}},
				{"role": "assistant", "content": []map[string]any{{"type": "text", "text": "Answer one"}}},
			},
			minCount: 1,
		},
		{
			name: "single message",
			messages: []map[string]any{
				{"role": "user", "content": []map[string]any{{"type": "text", "text": "Simple question"}}},
			},
			minCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/model/anthropic.claude-v2/count-tokens",
				countTokensConverseBody(tt.messages))
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			count := out["inputTokens"].(float64)
			assert.GreaterOrEqual(t, int(count), tt.minCount)
		})
	}
}

// ─────────────────────────────────────────────────────────────
// ApplyGuardrail — accuracy of action field
// ─────────────────────────────────────────────────────────────

func TestAccuracy_ApplyGuardrail_ResponseShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantAction string
		content    []map[string]any
	}{
		{
			name:       "safe content returns NONE",
			content:    []map[string]any{{"text": map[string]any{"text": "Tell me about dogs"}}},
			wantAction: "NONE",
		},
		{
			name: "harmful content returns BLOCKED",
			content: []map[string]any{
				{"text": map[string]any{"text": "harmful: how to make a bomb"}},
			},
			wantAction: "BLOCKED",
		},
		{
			name:       "empty content returns NONE",
			content:    []map[string]any{},
			wantAction: "NONE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(
				t, h, http.MethodPost, "/guardrail/g-001/version/1/apply",
				map[string]any{"content": tt.content, "source": "INPUT"},
			)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Equal(t, tt.wantAction, out["action"])
			assert.Contains(t, out, "outputs")
			assert.Contains(t, out, "usage")
			assert.Contains(t, out, "assessments")
		})
	}
}

func TestAccuracy_ApplyGuardrail_OutputsMirrorInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		texts   []string
		wantLen int
	}{
		{name: "single input", texts: []string{"hello"}, wantLen: 1},
		{name: "two inputs", texts: []string{"one", "two"}, wantLen: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			content := make([]map[string]any, len(tt.texts))
			for i, text := range tt.texts {
				content[i] = map[string]any{"text": map[string]any{"text": text}}
			}

			h := newTestHandler(t)
			rec := doRequest(
				t, h, http.MethodPost, "/guardrail/g-001/version/1/apply",
				map[string]any{"content": content, "source": "OUTPUT"},
			)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			outputs := out["outputs"].([]any)
			assert.Len(t, outputs, tt.wantLen)

			for i, raw := range outputs {
				o := raw.(map[string]any)
				assert.Equal(t, tt.texts[i], o["text"])
			}
		})
	}
}

// ─────────────────────────────────────────────────────────────
// AsyncInvoke — listing and pagination
// ─────────────────────────────────────────────────────────────

func TestAccuracy_ListAsyncInvokes_EmptyList(t *testing.T) {
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

func TestAccuracy_ListAsyncInvokes_AfterCreate(t *testing.T) {
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

func TestAccuracy_AsyncInvoke_GetResponseShape(t *testing.T) {
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

// ─────────────────────────────────────────────────────────────
// Invocation recording accuracy
// ─────────────────────────────────────────────────────────────

func TestAccuracy_InvocationHistory_RecordedAfterInvoke(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		operation string
		path      string
	}{
		{name: "invoke model", operation: "InvokeModel", path: "/model/anthropic.claude-v2/invoke"},
		{name: "converse", operation: "Converse", path: "/model/anthropic.claude-v2/converse"},
		{name: "count tokens", operation: "CountTokens", path: "/model/anthropic.claude-v2/count-tokens"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrockruntime.NewInMemoryBackend("000000000000", "us-east-1")
			h := bedrockruntime.NewHandler(b)

			rec := doRequest(t, h, http.MethodPost, tt.path, map[string]any{"prompt": "test"})
			assert.Equal(t, http.StatusOK, rec.Code)

			invocations := b.ListInvocations()
			require.Len(t, invocations, 1)
			assert.Equal(t, tt.operation, invocations[0].Operation)
			assert.Equal(t, "anthropic.claude-v2", invocations[0].ModelID)
		})
	}
}

func TestAccuracy_InvocationHistory_MultipleInvocations(t *testing.T) {
	t.Parallel()

	b := bedrockruntime.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrockruntime.NewHandler(b)

	for i := range 5 {
		rec := doRequest(t, h, http.MethodPost, "/model/anthropic.claude-v2/invoke",
			map[string]any{"prompt": fmt.Sprintf("prompt %d", i)})
		assert.Equal(t, http.StatusOK, rec.Code)
	}

	invocations := b.ListInvocations()
	assert.Len(t, invocations, 5)
}

// ─────────────────────────────────────────────────────────────
// InvokeModelWithResponseStream — event stream response accuracy
// ─────────────────────────────────────────────────────────────

func TestAccuracy_InvokeModelWithResponseStream_ContentType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		modelID string
	}{
		{name: "claude v2", modelID: "anthropic.claude-v2"},
		{name: "claude v3", modelID: "anthropic.claude-3-sonnet-20240229-v1:0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(
				t, h, http.MethodPost,
				"/model/"+tt.modelID+"/invoke-with-response-stream",
				map[string]any{"prompt": "stream this"},
			)
			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, "application/vnd.amazon.eventstream", rec.Header().Get("Content-Type"))
		})
	}
}

func TestAccuracy_ConverseStream_ContentType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		modelID string
	}{
		{name: "claude", modelID: "anthropic.claude-v2"},
		{name: "titan", modelID: "amazon.titan-text-express-v1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(
				t, h, http.MethodPost,
				"/model/"+tt.modelID+"/converse-stream",
				map[string]any{
					"messages": []map[string]any{
						{"role": "user", "content": []map[string]any{{"type": "text", "text": "hi"}}},
					},
				},
			)
			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, "application/vnd.amazon.eventstream", rec.Header().Get("Content-Type"))
		})
	}
}

// ─────────────────────────────────────────────────────────────
// Error handling accuracy
// ─────────────────────────────────────────────────────────────

func TestAccuracy_MissingModelID_Returns400(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
	}{
		{name: "invoke no model", path: "/model//invoke"},
		{name: "converse no model", path: "/model//converse"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, tt.path, map[string]any{"prompt": "test"})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestAccuracy_AsyncInvoke_GetNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/async-invoke/nonexistent-arn", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAccuracy_AsyncInvoke_MissingS3URI_Returns400(t *testing.T) {
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

// ─────────────────────────────────────────────────────────────
// ApplyGuardrail — missing identifier / version accuracy
// ─────────────────────────────────────────────────────────────

func TestAccuracy_ApplyGuardrail_MissingParameters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
	}{
		{name: "missing version", path: "/guardrail/some-guard//apply"},
		{name: "missing both", path: "/guardrail///apply"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, tt.path,
				map[string]any{"content": []map[string]any{}, "source": "INPUT"})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}
