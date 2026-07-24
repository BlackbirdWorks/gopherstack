package comprehend_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- LanguageCode required/unsupported validation ---

func TestDetectLanguageCodeRequired(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		action string
		body   string
	}{
		{name: "sentiment_missing", action: "DetectSentiment", body: `{"Text":"hello there"}`},
		{name: "key_phrases_missing", action: "DetectKeyPhrases", body: `{"Text":"hello there"}`},
		{name: "pii_missing", action: "DetectPiiEntities", body: `{"Text":"hello there"}`},
		{name: "syntax_missing", action: "DetectSyntax", body: `{"Text":"hello there"}`},
		{
			name: "targeted_sentiment_missing", action: "DetectTargetedSentiment",
			body: `{"Text":"hello there"}`,
		},
		{
			name: "toxic_content_missing", action: "DetectToxicContent",
			body: `{"TextSegments":[{"Text":"hello"}]}`,
		},
		{name: "contains_pii_missing", action: "ContainsPiiEntities", body: `{"Text":"hello there"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := rawRequest(t, newHandler(), tt.action, tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
			m := decodeBody(t, rec)
			assert.Equal(t, "InvalidRequestException", m["__type"])
		})
	}
}

func TestDetectUnsupportedLanguage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		action string
		body   string
	}{
		{
			name: "sentiment_bogus_code", action: "DetectSentiment",
			body: `{"Text":"hello there","LanguageCode":"xx"}`,
		},
		{
			// DetectSyntax only supports de/en/es/fr/it/pt -- "ar" is a valid
			// general LanguageCode but not a valid SyntaxLanguageCode.
			name: "syntax_valid_general_but_unsupported", action: "DetectSyntax",
			body: `{"Text":"hello there","LanguageCode":"ar"}`,
		},
		{
			// DetectToxicContent only supports English despite typing
			// LanguageCode as the general enum.
			name: "toxic_content_non_english", action: "DetectToxicContent",
			body: `{"TextSegments":[{"Text":"hello"}],"LanguageCode":"es"}`,
		},
		{
			name: "targeted_sentiment_non_english", action: "DetectTargetedSentiment",
			body: `{"Text":"hello there","LanguageCode":"fr"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := rawRequest(t, newHandler(), tt.action, tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
			m := decodeBody(t, rec)
			assert.Equal(t, "UnsupportedLanguageException", m["__type"])
		})
	}
}

// TestDetectEntitiesLanguageCodeOptional verifies DetectEntities does NOT
// require LanguageCode (unlike every other Detect* op here): the real API
// lets a caller supply EndpointArn for a custom model instead, so an absent
// LanguageCode must succeed, not fail InvalidRequestException.
func TestDetectEntitiesLanguageCodeOptional(t *testing.T) {
	t.Parallel()

	m := request(t, newHandler(), "DetectEntities", map[string]any{"Text": "Alice works here."})
	assert.Contains(t, m, "Entities")
}

// --- Text size limits ---

func TestDetectTextSizeLimitExceeded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input  map[string]any
		name   string
		action string
	}{
		{
			name: "sentiment_over_5kb", action: "DetectSentiment",
			input: map[string]any{"Text": strings.Repeat("a", 5001), "LanguageCode": "en"},
		},
		{
			name: "syntax_over_5kb", action: "DetectSyntax",
			input: map[string]any{"Text": strings.Repeat("a", 5001), "LanguageCode": "en"},
		},
		{
			name: "key_phrases_over_100kb", action: "DetectKeyPhrases",
			input: map[string]any{"Text": strings.Repeat("a", 100001), "LanguageCode": "en"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := rawRequest(t, newHandler(), tt.action, toJSON(t, tt.input))
			assert.Equal(t, http.StatusBadRequest, rec.Code)
			m := decodeBody(t, rec)
			assert.Equal(t, "TextSizeLimitExceededException", m["__type"])
		})
	}
}

func TestDetectToxicContentSegmentLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   map[string]any
		wantErr string
	}{
		{
			name: "single_segment_over_1kb",
			input: map[string]any{
				"LanguageCode": "en",
				"TextSegments": []any{map[string]any{"Text": strings.Repeat("a", 1025)}},
			},
			wantErr: "TextSizeLimitExceededException",
		},
		{
			name: "total_over_10kb",
			input: map[string]any{
				"LanguageCode": "en",
				"TextSegments": []any{
					map[string]any{"Text": strings.Repeat("a", 1024)},
					map[string]any{"Text": strings.Repeat("b", 1024)},
					map[string]any{"Text": strings.Repeat("c", 1024)},
					map[string]any{"Text": strings.Repeat("d", 1024)},
					map[string]any{"Text": strings.Repeat("e", 1024)},
					map[string]any{"Text": strings.Repeat("f", 1024)},
					map[string]any{"Text": strings.Repeat("g", 1024)},
					map[string]any{"Text": strings.Repeat("h", 1024)},
					map[string]any{"Text": strings.Repeat("i", 1024)},
					map[string]any{"Text": strings.Repeat("j", 1024)},
					map[string]any{"Text": strings.Repeat("k", 1024)},
				},
			},
			wantErr: "TextSizeLimitExceededException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := rawRequest(t, newHandler(), "DetectToxicContent", toJSON(t, tt.input))
			assert.Equal(t, http.StatusBadRequest, rec.Code)
			m := decodeBody(t, rec)
			assert.Equal(t, tt.wantErr, m["__type"])
		})
	}
}

// --- Batch limits: whole-request BatchSizeLimitExceededException,
// per-item ErrorList entries ---

func TestBatchSizeLimitExceeded(t *testing.T) {
	t.Parallel()

	texts := make([]any, 26)
	for i := range texts {
		texts[i] = "short text"
	}

	rec := rawRequest(t, newHandler(), "BatchDetectSentiment", toJSON(t, map[string]any{
		"TextList": texts, "LanguageCode": "en",
	}))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	m := decodeBody(t, rec)
	assert.Equal(t, "BatchSizeLimitExceededException", m["__type"])
}

func TestBatchDetectUnsupportedLanguage(t *testing.T) {
	t.Parallel()

	rec := rawRequest(t, newHandler(), "BatchDetectSentiment", toJSON(t, map[string]any{
		"TextList": []any{"hello"}, "LanguageCode": "xx",
	}))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	m := decodeBody(t, rec)
	assert.Equal(t, "UnsupportedLanguageException", m["__type"])
}

// TestBatchDetectPerItemErrorList verifies an oversized TextList entry
// becomes a per-item ErrorList entry (matching each item's Index) rather
// than aborting the whole batch -- every Batch*Output doc comment states
// "If there are no errors in the batch, the ErrorList is empty", implying
// per-item failures are an expected, ordinary batch outcome, and the
// well-formed items alongside it must still succeed.
func TestBatchDetectPerItemErrorList(t *testing.T) {
	t.Parallel()

	m := request(t, newHandler(), "BatchDetectSentiment", map[string]any{
		"TextList":     []any{"great product", strings.Repeat("a", 5001), "bad service"},
		"LanguageCode": "en",
	})

	results, ok := m["ResultList"].([]any)
	require.True(t, ok, "ResultList must be a list")
	require.Len(t, results, 2, "the two well-formed items must still succeed")

	errList, ok := m["ErrorList"].([]any)
	require.True(t, ok, "ErrorList must be a list")
	require.Len(t, errList, 1)

	errEntry := errList[0].(map[string]any)
	assert.InEpsilon(t, float64(1), errEntry["Index"], 0, "the oversized item was at Index 1")
	assert.Equal(t, "TEXT_SIZE_LIMIT_EXCEEDED", errEntry["ErrorCode"])
	assert.NotEmpty(t, errEntry["ErrorMessage"])

	// Successful entries retain their original Index too (0 and 2, not
	// renumbered after skipping the failed item at 1).
	gotIndexes := make([]int, 0, len(results))
	for _, raw := range results {
		gotIndexes = append(gotIndexes, int(raw.(map[string]any)["Index"].(float64)))
	}
	assert.ElementsMatch(t, []int{0, 2}, gotIndexes)
}
