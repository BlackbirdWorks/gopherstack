package bedrockruntime_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func TestHandler_GuardrailPath_UnknownOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/guardrail/my-id/version/1/unknown", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestApplyGuardrail_NoneForSafeContent(t *testing.T) {
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

func TestApplyGuardrail_BlockedForHarmfulContent(t *testing.T) {
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

func TestApplyGuardrail_BlockedForToxicContent(t *testing.T) {
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

func TestApplyGuardrail_OutputsReflectsInput(t *testing.T) {
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

func TestApplyGuardrail_ResponseShape(t *testing.T) {
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

func TestApplyGuardrail_OutputsMirrorInput(t *testing.T) {
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

func TestApplyGuardrail_AssessmentsReflectBlockedMatch(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/guardrail/my-guardrail/version/1/apply",
		map[string]any{
			"source": "INPUT",
			"content": []map[string]any{
				{"text": map[string]any{"text": "This is toxic content."}},
			},
		})

	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Equal(t, "BLOCKED", out["action"])

	assessments, ok := out["assessments"].([]any)
	require.True(t, ok)
	require.Len(t, assessments, 1, "BLOCKED action must report the policy match, not an empty assessments list")

	assessment := assessments[0].(map[string]any)
	wordPolicy, ok := assessment["wordPolicy"].(map[string]any)
	require.True(t, ok, "assessment must carry a wordPolicy reflecting the matched keyword")

	customWords, ok := wordPolicy["customWords"].([]any)
	require.True(t, ok)
	require.Len(t, customWords, 1)

	match := customWords[0].(map[string]any)
	assert.Equal(t, "toxic", match["match"])
	assert.Equal(t, "BLOCKED", match["action"])
	assert.Equal(t, true, match["detected"])
}

func TestApplyGuardrail_AssessmentsEmptyForNoneAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/guardrail/my-guardrail/version/1/apply",
		map[string]any{
			"source": "INPUT",
			"content": []map[string]any{
				{"text": map[string]any{"text": "Tell me about the weather."}},
			},
		})

	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Equal(t, "NONE", out["action"])

	assessments, ok := out["assessments"].([]any)
	require.True(t, ok)
	assert.Empty(t, assessments)
}

func TestApplyGuardrail_MissingParameters(t *testing.T) {
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
