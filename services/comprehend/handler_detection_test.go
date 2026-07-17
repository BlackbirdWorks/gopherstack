package comprehend_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- DetectSentiment field shapes ---

func TestDetectSentimentFieldShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		text          string
		wantSentiment string
	}{
		{name: "positive_text", text: "I love this great product!", wantSentiment: "POSITIVE"},
		{name: "negative_text", text: "I hate this terrible experience", wantSentiment: "NEGATIVE"},
		{name: "neutral_text", text: "The package arrived yesterday", wantSentiment: "NEUTRAL"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := request(t, newHandler(), "DetectSentiment", map[string]any{"Text": tt.text, "LanguageCode": "en"})
			assert.Equal(t, tt.wantSentiment, m["Sentiment"], "Sentiment field must match keyword")

			score, ok := m["SentimentScore"].(map[string]any)
			require.True(t, ok, "SentimentScore must be a map")
			assert.Contains(t, score, "Positive", "SentimentScore must have Positive key")
			assert.Contains(t, score, "Negative", "SentimentScore must have Negative key")
			assert.Contains(t, score, "Neutral", "SentimentScore must have Neutral key")
			assert.Contains(t, score, "Mixed", "SentimentScore must have Mixed key")
		})
	}
}

// --- DetectEntities field shapes ---

func TestDetectEntitiesFieldShapes(t *testing.T) {
	t.Parallel()

	m := request(t, newHandler(), "DetectEntities", map[string]any{
		"Text": "Alice went to Paris.", "LanguageCode": "en",
	})
	entities, ok := m["Entities"].([]any)
	require.True(t, ok, "Entities must be a list")
	require.NotEmpty(t, entities, "capitalized words should produce entities")

	entity := entities[0].(map[string]any)
	assert.Contains(t, entity, "Text", "entity must have Text field")
	assert.Contains(t, entity, "Score", "entity must have Score field")
	assert.Contains(t, entity, "BeginOffset", "entity must have BeginOffset field")
	assert.Contains(t, entity, "EndOffset", "entity must have EndOffset field")
	assert.Contains(t, entity, "Type", "entity must have Type field")
}

// --- DetectKeyPhrases field shapes ---

func TestDetectKeyPhrasesFieldShapes(t *testing.T) {
	t.Parallel()

	m := request(
		t, newHandler(), "DetectKeyPhrases", map[string]any{"Text": "customer support ticket", "LanguageCode": "en"},
	)
	phrases, ok := m["KeyPhrases"].([]any)
	require.True(t, ok, "KeyPhrases must be a list")
	require.NotEmpty(t, phrases)

	phrase := phrases[0].(map[string]any)
	assert.Contains(t, phrase, "Text", "key phrase must have Text field")
	assert.Contains(t, phrase, "Score", "key phrase must have Score field")
	assert.Contains(t, phrase, "BeginOffset", "key phrase must have BeginOffset field")
	assert.Contains(t, phrase, "EndOffset", "key phrase must have EndOffset field")
}

// --- DetectPiiEntities field shapes ---

func TestDetectPiiEntitiesFieldShapes(t *testing.T) {
	t.Parallel()

	m := request(t, newHandler(), "DetectPiiEntities", map[string]any{
		"Text": "Email user@example.com please.", "LanguageCode": "en",
	})
	entities, ok := m["Entities"].([]any)
	require.True(t, ok, "Entities must be a list")
	require.NotEmpty(t, entities, "email address should produce PII entity")

	entity := entities[0].(map[string]any)
	assert.Contains(t, entity, "Text", "PII entity must have Text field")
	assert.Contains(t, entity, "Score", "PII entity must have Score field")
	assert.Contains(t, entity, "BeginOffset", "PII entity must have BeginOffset field")
	assert.Contains(t, entity, "EndOffset", "PII entity must have EndOffset field")
	assert.Contains(t, entity, "Type", "PII entity must have Type field")
	assert.Equal(t, "EMAIL", entity["Type"])
}

// --- DetectSyntax field shapes ---

func TestDetectSyntaxFieldShapes(t *testing.T) {
	t.Parallel()

	m := request(t, newHandler(), "DetectSyntax", map[string]any{"Text": "The quick brown fox", "LanguageCode": "en"})
	tokens, ok := m["SyntaxTokens"].([]any)
	require.True(t, ok, "SyntaxTokens must be a list")
	require.NotEmpty(t, tokens)

	token := tokens[0].(map[string]any)
	assert.Contains(t, token, "TokenId", "syntax token must have TokenId")
	assert.Contains(t, token, "Text", "syntax token must have Text")
	assert.Contains(t, token, "BeginOffset", "syntax token must have BeginOffset")
	assert.Contains(t, token, "EndOffset", "syntax token must have EndOffset")

	pos, ok := token["PartOfSpeech"].(map[string]any)
	require.True(t, ok, "PartOfSpeech must be a map")
	assert.Contains(t, pos, "Tag", "PartOfSpeech must have Tag field")
	assert.Contains(t, pos, "Score", "PartOfSpeech must have Score field")
}

// TestDetectSyntaxOffsetCorrectness verifies that BeginOffset and EndOffset
// are correct for each token, including repeated words. Previously
// strings.Index(text, token) always found the first occurrence, so "the" in
// "the cat the mat" got offset 0 for both tokens.
func TestDetectSyntaxOffsetCorrectness(t *testing.T) {
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

			m := request(t, newHandler(), "DetectSyntax", map[string]any{"Text": tt.text, "LanguageCode": "en"})

			raw, ok := m["SyntaxTokens"].([]any)
			require.True(t, ok, "SyntaxTokens must be a list")
			require.Len(t, raw, len(tt.tokens), "token count must match")

			for i, want := range tt.tokens {
				tok := raw[i].(map[string]any)
				assert.Equal(t, want.text, tok["Text"], "token[%d] Text", i)
				gotBegin := int(tok["BeginOffset"].(float64))
				gotEnd := int(tok["EndOffset"].(float64))
				assert.Equal(t, want.begin, gotBegin, "token[%d] BeginOffset for %q", i, want.text)
				assert.Equal(t, want.end, gotEnd, "token[%d] EndOffset for %q", i, want.text)
			}
		})
	}
}

// --- DetectDominantLanguage field shapes ---

func TestDetectDominantLanguageFieldShapes(t *testing.T) {
	t.Parallel()

	m := request(t, newHandler(), "DetectDominantLanguage", map[string]any{"Text": "Hello world this is a test"})
	langs, ok := m["Languages"].([]any)
	require.True(t, ok, "Languages must be a list")
	require.NotEmpty(t, langs)

	lang := langs[0].(map[string]any)
	assert.Contains(t, lang, "LanguageCode", "language must have LanguageCode field")
	assert.Contains(t, lang, "Score", "language must have Score field")
}

// --- DetectToxicContent field shapes ---

func TestDetectToxicContentFieldShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		text     string
		wantHigh bool
	}{
		{name: "toxic_text", text: "I hate everyone", wantHigh: true},
		{name: "normal_text", text: "Have a great day", wantHigh: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := request(t, newHandler(), "DetectToxicContent", map[string]any{
				"TextSegments": []any{map[string]any{"Text": tt.text}},
				"LanguageCode": "en",
			})
			results, ok := m["ResultList"].([]any)
			require.True(t, ok, "ResultList must be a list")
			require.Len(t, results, 1)

			result := results[0].(map[string]any)
			assert.Contains(t, result, "Toxicity", "result must have Toxicity field")
			labels, ok := result["Labels"].([]any)
			require.True(t, ok, "Labels must be a list")
			require.NotEmpty(t, labels)

			label := labels[0].(map[string]any)
			assert.Contains(t, label, "Name", "label must have Name field")
			assert.Contains(t, label, "Score", "label must have Score field")

			toxicity := result["Toxicity"].(float64)
			if tt.wantHigh {
				assert.Greater(t, toxicity, 0.5, "hate text should score high toxicity")
			} else {
				assert.Less(t, toxicity, 0.5, "normal text should score low toxicity")
			}
		})
	}
}

// --- Batch operation field shapes ---

func TestBatchOperationsFieldShapes(t *testing.T) {
	t.Parallel()

	ops := []struct {
		action string
		texts  []any
	}{
		{action: "BatchDetectSentiment", texts: []any{"great product", "bad service"}},
		{action: "BatchDetectEntities", texts: []any{"Alice went to Paris", "Bob met Charlie"}},
		{action: "BatchDetectKeyPhrases", texts: []any{"customer service", "product launch"}},
		{action: "BatchDetectSyntax", texts: []any{"quick brown fox", "lazy dog"}},
		{action: "BatchDetectDominantLanguage", texts: []any{"hello world", "bonjour monde"}},
	}

	for _, op := range ops {
		t.Run(op.action, func(t *testing.T) {
			t.Parallel()

			input := map[string]any{"TextList": op.texts, "LanguageCode": "en"}
			m := request(t, newHandler(), op.action, input)
			results, ok := m["ResultList"].([]any)
			require.True(t, ok, "ResultList must be a list")
			require.Len(t, results, 2, "one result per input text")

			for i, raw := range results {
				item := raw.(map[string]any)
				idx, idxOK := item["Index"].(float64)
				require.True(t, idxOK, "each batch result must have Index field")
				assert.Equal(t, i, int(idx), "Index must be sequential")
			}

			assert.Contains(t, m, "ErrorList", "batch response must have ErrorList")
		})
	}
}
