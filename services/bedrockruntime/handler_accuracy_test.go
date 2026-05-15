package bedrockruntime_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrockruntime"
)

// ─────────────────────────────────────────────────────────────
// Gap 1 / 14: InvokeModel Content-Type / Accept negotiation
// ─────────────────────────────────────────────────────────────

func TestAccuracy_InvokeModel_DefaultContentType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model/anthropic.claude-v2/invoke",
		map[string]any{"prompt": "Hello"})

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
}

func TestAccuracy_InvokeModel_AcceptHeader_JSON(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body, _ := json.Marshal(map[string]any{"prompt": "Hello"})

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/model/anthropic.claude-v2/invoke", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
}

func TestAccuracy_InvokeModel_AcceptHeader_Wildcard(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body, _ := json.Marshal(map[string]any{"prompt": "Hello"})

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/model/anthropic.claude-v2/invoke", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "*/*")

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
}

// ─────────────────────────────────────────────────────────────
// Gap 2: Converse / ConverseStream payload parsed and reflected
// ─────────────────────────────────────────────────────────────

func TestAccuracy_Converse_ReflectsInputTokens(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Long message should produce higher token count than the fixed mock default.
	longText := "This is a much longer prompt than usual that should produce" +
		" more tokens than the default mock value of 10." +
		" Adding more words to ensure the character count is significant."
	rec := doRequest(t, h, http.MethodPost, "/model/anthropic.claude-v2/converse",
		map[string]any{
			"messages": []map[string]any{
				{"role": "user", "content": []map[string]any{{"text": longText}}},
			},
		})

	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	usage, ok := out["usage"].(map[string]any)
	require.True(t, ok)

	inputTokens, ok := usage["inputTokens"].(float64)
	require.True(t, ok)
	assert.Greater(t, int(inputTokens), 10, "long input should produce more tokens than the 10-token mock default")
}

func TestAccuracy_Converse_WithSystem(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model/anthropic.claude-3-sonnet-20240229-v1:0/converse",
		map[string]any{
			"messages": []map[string]any{
				{"role": "user", "content": []map[string]any{{"text": "Hello"}}},
			},
			"system": []map[string]any{
				{"text": "You are a helpful assistant."},
			},
			"inferenceConfig": map[string]any{
				"maxTokens":   100,
				"temperature": 0.5,
			},
		})

	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Contains(t, out, "output")
	assert.Contains(t, out, "stopReason")
	assert.Contains(t, out, "usage")
	assert.Contains(t, out, "metrics")
}

func TestAccuracy_ConverseStream_Flushable(t *testing.T) {
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
	assert.NotEmpty(t, rec.Body.Bytes())
}

// ─────────────────────────────────────────────────────────────
// Gap 3 / 15: CountTokens approximates from input
// ─────────────────────────────────────────────────────────────

func TestAccuracy_CountTokens_ReflectsInputLength(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	shortBody := map[string]any{"prompt": "Hi"}
	longBody := map[string]any{
		"messages": []map[string]any{
			{"role": "user", "content": []map[string]any{
				{
					"text": "This is a long prompt with enough words" +
						" to produce a meaningful token count approximation.",
				},
			}},
		},
	}

	recShort := doRequest(t, h, http.MethodPost, "/model/anthropic.claude-v2/count-tokens", shortBody)
	recLong := doRequest(t, h, http.MethodPost, "/model/anthropic.claude-v2/count-tokens", longBody)

	require.Equal(t, http.StatusOK, recShort.Code)
	require.Equal(t, http.StatusOK, recLong.Code)

	var shortOut, longOut map[string]any
	require.NoError(t, json.Unmarshal(recShort.Body.Bytes(), &shortOut))
	require.NoError(t, json.Unmarshal(recLong.Body.Bytes(), &longOut))

	shortTokens := shortOut["inputTokens"].(float64)
	longTokens := longOut["inputTokens"].(float64)

	assert.Greater(t, longTokens, shortTokens, "longer input should yield more tokens than shorter input")
}

func TestAccuracy_CountTokens_TitanUsesLargerDivisor(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Same body, different models — Titan uses ~6 chars/token vs ~4 for others.
	body := map[string]any{
		"inputText": "This is a moderately long prompt that should produce different token counts across model families.",
	}

	recClaude := doRequest(t, h, http.MethodPost, "/model/anthropic.claude-v2/count-tokens", body)
	recTitan := doRequest(t, h, http.MethodPost, "/model/amazon.titan-text-express-v1/count-tokens", body)

	require.Equal(t, http.StatusOK, recClaude.Code)
	require.Equal(t, http.StatusOK, recTitan.Code)

	var claudeOut, titanOut map[string]any
	require.NoError(t, json.Unmarshal(recClaude.Body.Bytes(), &claudeOut))
	require.NoError(t, json.Unmarshal(recTitan.Body.Bytes(), &titanOut))

	claudeTokens := claudeOut["inputTokens"].(float64)
	titanTokens := titanOut["inputTokens"].(float64)

	assert.Greater(
		t,
		claudeTokens,
		titanTokens,
		"Claude (4 chars/token) should produce more tokens than Titan (6 chars/token) for the same input",
	)
}

// ─────────────────────────────────────────────────────────────
// Gap 4: ApplyGuardrail parses body, deterministic decisions
// ─────────────────────────────────────────────────────────────

func TestAccuracy_ApplyGuardrail_NoneForSafeContent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/guardrail/my-guardrail/version/1/apply",
		map[string]any{
			"source": "INPUT",
			"content": []map[string]any{
				{"text": map[string]any{"text": "Hello world, this is safe content."}},
			},
		})

	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "NONE", out["action"])
}

func TestAccuracy_ApplyGuardrail_BlockedForHarmfulContent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/guardrail/my-guardrail/version/1/apply",
		map[string]any{
			"source": "INPUT",
			"content": []map[string]any{
				{"text": map[string]any{"text": "This is harmful content that should be blocked."}},
			},
		})

	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "BLOCKED", out["action"])
}

func TestAccuracy_ApplyGuardrail_BlockedForToxicContent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/guardrail/my-guardrail/version/DRAFT/apply",
		map[string]any{
			"source": "OUTPUT",
			"content": []map[string]any{
				{"text": map[string]any{"text": "Some toxic message here."}},
			},
		})

	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "BLOCKED", out["action"])
}

func TestAccuracy_ApplyGuardrail_OutputsReflectsInput(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	inputText := "Tell me about the weather."
	rec := doRequest(t, h, http.MethodPost, "/guardrail/my-guardrail/version/1/apply",
		map[string]any{
			"source": "INPUT",
			"content": []map[string]any{
				{"text": map[string]any{"text": inputText}},
			},
		})

	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	outputs, ok := out["outputs"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, outputs)

	first := outputs[0].(map[string]any)
	assert.Equal(t, inputText, first["text"])
}

// ─────────────────────────────────────────────────────────────
// Gap 5: StartAsyncInvoke validates s3:// URI format
// ─────────────────────────────────────────────────────────────

func TestAccuracy_StartAsyncInvoke_InvalidS3URI(t *testing.T) {
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

func TestAccuracy_StartAsyncInvoke_ValidS3URI(t *testing.T) {
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

func TestAccuracy_StartAsyncInvoke_InferenceProfileIdentifier(t *testing.T) {
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

// ─────────────────────────────────────────────────────────────
// Gap 8: invocationRing truncates long strings
// ─────────────────────────────────────────────────────────────

func TestAccuracy_InvocationTruncation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Build a body larger than maxInvocationStringBytes (10,000 chars).
	largePrompt := make([]byte, 15_000)
	for i := range largePrompt {
		largePrompt[i] = 'x'
	}

	body, _ := json.Marshal(map[string]any{"prompt": string(largePrompt)})

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/model/anthropic.claude-v2/invoke", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, rec.Code)

	invocations := h.Backend.ListInvocations()
	require.Len(t, invocations, 1)
	assert.LessOrEqual(t, len(invocations[0].Input), 10_000, "input should be truncated to maxInvocationStringBytes")
}

// ─────────────────────────────────────────────────────────────
// Gap 9 / 11: GetAsyncInvoke - terminal-state fields conditional
// ─────────────────────────────────────────────────────────────

func TestAccuracy_GetAsyncInvoke_EndTimeAbsentForInProgress(t *testing.T) {
	t.Parallel()

	b := bedrockruntime.NewInMemoryBackend("000000000000", "us-east-1")
	inv, err := b.StartAsyncInvoke("anthropic.claude-v2", "s3://bucket/prefix/", "", nil)
	require.NoError(t, err)

	// Status is InProgress immediately after creation.
	got, err := b.GetAsyncInvoke(inv.InvocationArn)
	require.NoError(t, err)
	assert.Equal(t, bedrockruntime.AsyncInvokeStatusInProgress, got.Status)
	assert.Nil(t, got.EndTime, "endTime must be nil while InProgress")
}

// ─────────────────────────────────────────────────────────────
// Gap 9: Janitor advances InProgress → Completed
// ─────────────────────────────────────────────────────────────

func TestAccuracy_Janitor_AdvancesAsyncInvoke(t *testing.T) {
	t.Parallel()

	b := bedrockruntime.NewInMemoryBackend("000000000000", "us-east-1")
	inv, err := b.StartAsyncInvoke("anthropic.claude-v2", "s3://bucket/prefix/", "", nil)
	require.NoError(t, err)
	assert.Equal(t, bedrockruntime.AsyncInvokeStatusInProgress, inv.Status)

	// Directly force-advance; janitor does this after completion delay.
	b.AdvanceAsyncInvokesForTest(time.Duration(0))

	got, err := b.GetAsyncInvoke(inv.InvocationArn)
	require.NoError(t, err)
	assert.Equal(t, bedrockruntime.AsyncInvokeStatusCompleted, got.Status)
	assert.NotNil(t, got.EndTime)
}

// ─────────────────────────────────────────────────────────────
// Gap 7: Header buffer overflow protection
// ─────────────────────────────────────────────────────────────

func TestAccuracy_EventStreamHeaders_DynamicBuffer(t *testing.T) {
	t.Parallel()

	// Trigger event-stream path to ensure headers encode correctly.
	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model/amazon.nova-pro-v1:0/invoke-with-response-stream",
		map[string]any{"prompt": "test"})

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/vnd.amazon.eventstream", rec.Header().Get("Content-Type"))

	// Verify the event stream frame has valid structure (non-empty, parseable length).
	frameBytes := rec.Body.Bytes()
	require.GreaterOrEqual(t, len(frameBytes), 12, "event stream frame must be at least 12 bytes (prelude)")
}

// ─────────────────────────────────────────────────────────────
// Gap: S3 URI validation in backend
// ─────────────────────────────────────────────────────────────

func TestAccuracy_Backend_StartAsyncInvoke_S3URIValidation(t *testing.T) {
	t.Parallel()

	b := bedrockruntime.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.StartAsyncInvoke("anthropic.claude-v2", "not-s3://bucket/prefix", "", nil)
	require.Error(t, err, "non-s3:// URI must be rejected")

	_, err = b.StartAsyncInvoke("anthropic.claude-v2", "s3:///no-bucket", "", nil)
	require.Error(t, err, "empty bucket name must be rejected")

	_, valid := b.StartAsyncInvoke("anthropic.claude-v2", "s3://valid-bucket/prefix", "", nil)
	require.NoError(t, valid, "valid s3:// URI must be accepted")
}
