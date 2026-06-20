package comprehend_test

// Parity audit (go-hgsm5): fix two genuine AWS behavioral gaps.
//
// 1. DetectSyntax BeginOffset/EndOffset correctness for repeated tokens.
//    Old code: strings.Index(text, token) always returns first-occurrence index,
//    so every duplicate word gets the wrong offset. Fixed by scanning forward.
//
// 2. ContainsPiiEntities label name fidelity.
//    Real AWS returns {"Name":"EMAIL"} / {"Name":"SSN"} — specific entity types.
//    Old code returned the generic {"Name":"PII"} for any PII hit.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/comprehend"
)

func parityHandler(t *testing.T) *comprehend.Handler {
	t.Helper()

	return comprehend.NewHandler(comprehend.NewInMemoryBackend("000000000000", "us-east-1"))
}

func parityDo(t *testing.T, h *comprehend.Handler, action, body string) map[string]any {
	t.Helper()

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("X-Amz-Target", "Comprehend_20171127."+action)
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var m map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))

	return m
}

// TestParity_DetectSyntax_OffsetCorrectness verifies that BeginOffset and
// EndOffset are correct for each token, including repeated words. Previously
// strings.Index(text, token) always found the first occurrence, so "the" in
// "the cat the mat" got offset 0 for both tokens.
func TestParity_DetectSyntax_OffsetCorrectness(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		text   string
		tokens []struct {
			text  string
			begin int
			end   int
		}
	}{
		{
			name: "no_repeated_words",
			text: "Alice went home",
			tokens: []struct {
				text  string
				begin int
				end   int
			}{
				{"Alice", 0, 5},
				{"went", 6, 10},
				{"home", 11, 15},
			},
		},
		{
			name: "repeated_word_gets_correct_offset",
			// "the" appears at 0 and 8; "mat" at 12.
			// Old code: second "the" would have BeginOffset=0 (wrong).
			text: "the cat the mat",
			tokens: []struct {
				text  string
				begin int
				end   int
			}{
				{"the", 0, 3},
				{"cat", 4, 7},
				{"the", 8, 11},
				{"mat", 12, 15},
			},
		},
		{
			name: "three_occurrences",
			// "go" at 0, 3, 6
			text: "go go go",
			tokens: []struct {
				text  string
				begin int
				end   int
			}{
				{"go", 0, 2},
				{"go", 3, 5},
				{"go", 6, 8},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := parityHandler(t)
			body := `{"Text":"` + tt.text + `","LanguageCode":"en"}`
			m := parityDo(t, h, "DetectSyntax", body)

			raw, ok := m["SyntaxTokens"].([]any)
			require.True(t, ok, "SyntaxTokens must be a list")
			require.Len(t, raw, len(tt.tokens), "token count must match")

			for i, want := range tt.tokens {
				tok := raw[i].(map[string]any)
				assert.Equal(t, want.text, tok["Text"], "token[%d] Text", i)
				assert.Equal(t, float64(want.begin), tok["BeginOffset"], "token[%d] BeginOffset for %q", i, want.text)
				assert.Equal(t, float64(want.end), tok["EndOffset"], "token[%d] EndOffset for %q", i, want.text)
			}
		})
	}
}

// TestParity_ContainsPiiEntities_LabelTypes verifies that ContainsPiiEntities
// returns Labels with specific PII entity type names (EMAIL, SSN) rather than
// the generic "PII" label that the old code produced.
func TestParity_ContainsPiiEntities_LabelTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		text       string
		wantTypes  []string
		wantEmpty  bool
	}{
		{
			name:      "email_only",
			text:      "Contact us at user@example.com for support.",
			wantTypes: []string{"EMAIL"},
		},
		{
			name:      "ssn_only",
			text:      "SSN on file: 123-45-6789.",
			wantTypes: []string{"SSN"},
		},
		{
			name:      "email_and_ssn",
			text:      "Email user@example.com and SSN 987-65-4321.",
			wantTypes: []string{"EMAIL", "SSN"},
		},
		{
			name:     "no_pii",
			text:     "The weather is sunny today.",
			wantEmpty: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := parityHandler(t)
			body := `{"Text":"` + tt.text + `","LanguageCode":"en"}`
			m := parityDo(t, h, "ContainsPiiEntities", body)

			labels, ok := m["Labels"].([]any)
			require.True(t, ok, "Labels must be a list")

			if tt.wantEmpty {
				assert.Empty(t, labels, "no PII in text — Labels must be empty")

				return
			}

			require.Len(t, labels, len(tt.wantTypes), "label count must match detected PII types")
			gotNames := make([]string, 0, len(labels))
			for _, raw := range labels {
				label := raw.(map[string]any)
				name, nameOK := label["Name"].(string)
				require.True(t, nameOK, "each label must have a Name field")
				assert.NotEqual(t, "PII", name, "label Name must be a specific type, not generic PII")
				gotNames = append(gotNames, name)
			}
			for _, want := range tt.wantTypes {
				assert.Contains(t, gotNames, want, "expected PII type %q in Labels", want)
			}
		})
	}
}
