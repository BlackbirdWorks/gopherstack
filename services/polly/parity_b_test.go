package polly_test

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListLexiconsOpaqueToken verifies that the NextToken returned from a
// paginated ListLexicons response is opaque (base64-encoded), not a raw integer.
func TestListLexiconsOpaqueToken(t *testing.T) {
	t.Parallel()

	handler := newHandler()

	for _, name := range []string{"alpha", "bravo"} {
		rec := request(t, handler, http.MethodPut, "/v1/lexicons/"+name, map[string]any{
			"Content": `<?xml version="1.0" encoding="UTF-8"?><lexicon version="1.0"></lexicon>`,
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := request(t, handler, http.MethodGet, "/v1/lexicons?MaxResults=1", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	token, ok := out["NextToken"].(string)
	require.True(t, ok, "NextToken must be present when more results exist")
	assert.NotEmpty(t, token)

	decoded, err := base64.StdEncoding.DecodeString(token)
	require.NoError(t, err, "NextToken should be base64-encoded")
	assert.NotEmpty(t, decoded, "decoded token should contain position bytes")
}

// TestListSpeechSynthesisTasksOpaqueToken verifies that the pagination token
// for ListSpeechSynthesisTasks is opaque (base64-encoded).
func TestListSpeechSynthesisTasksOpaqueToken(t *testing.T) {
	t.Parallel()

	handler := newHandler()

	for range 2 {
		startTask(t, handler, "pagination token test")
	}

	rec := request(t, handler, http.MethodGet, "/v1/synthesisTasks?MaxResults=1", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	token, ok := out["NextToken"].(string)
	require.True(t, ok, "NextToken must be present when more results exist")
	assert.NotEmpty(t, token)

	decoded, err := base64.StdEncoding.DecodeString(token)
	require.NoError(t, err, "NextToken should be base64-encoded")
	assert.NotEmpty(t, decoded)
}

// TestDescribeVoicesExpandedCatalogue verifies that the built-in voice catalogue
// is substantially populated (≥70 voices) to reflect the real AWS Polly list.
func TestDescribeVoicesExpandedCatalogue(t *testing.T) {
	t.Parallel()

	handler := newHandler()

	tests := []struct {
		name      string
		path      string
		minVoices int
	}{
		{
			name:      "all_voices",
			path:      "/v1/voices",
			minVoices: 70,
		},
		{
			name:      "us_english",
			path:      "/v1/voices?LanguageCode=en-US",
			minVoices: 8,
		},
		{
			name:      "british_english",
			path:      "/v1/voices?LanguageCode=en-GB",
			minVoices: 3,
		},
		{
			name:      "neural_engine",
			path:      "/v1/voices?Engine=neural",
			minVoices: 40,
		},
		{
			name:      "generative_engine",
			path:      "/v1/voices?Engine=generative",
			minVoices: 3,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			rec := request(t, handler, http.MethodGet, test.path, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			voices, ok := out["Voices"].([]any)
			require.True(t, ok)
			assert.GreaterOrEqual(t, len(voices), test.minVoices,
				"expected at least %d voices for %s", test.minVoices, test.path)
		})
	}
}

// TestSpeechMarksVisemeVariety verifies that viseme speech marks include
// multiple distinct visemes (not just the fallback "p") for a sentence
// containing vowels and different consonants.
func TestSpeechMarksVisemeVariety(t *testing.T) {
	t.Parallel()

	handler := newHandler()

	rec := request(t, handler, http.MethodPost, "/v1/speech", map[string]any{
		"OutputFormat":    "json",
		"Text":            "See the ship sail away",
		"VoiceId":         "Joanna",
		"SpeechMarkTypes": []string{"viseme"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var marks []map[string]any
	for _, line := range splitLines(rec.Body.Bytes()) {
		if len(line) == 0 {
			continue
		}

		var m map[string]any
		require.NoError(t, json.Unmarshal(line, &m))

		if m["type"] == "viseme" {
			marks = append(marks, m)
		}
	}

	require.NotEmpty(t, marks, "expected at least one viseme mark")

	seen := make(map[string]bool)
	for _, m := range marks {
		if v, ok := m["value"].(string); ok {
			seen[v] = true
		}
	}

	assert.Greater(t, len(seen), 1, "expected multiple distinct visemes, got: %v", seen)
}

func splitLines(data []byte) [][]byte {
	var lines [][]byte
	start := 0

	for i, b := range data {
		if b == '\n' {
			lines = append(lines, data[start:i])
			start = i + 1
		}
	}

	if start < len(data) {
		lines = append(lines, data[start:])
	}

	return lines
}
