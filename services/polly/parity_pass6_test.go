package polly_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_SpeechMarkCounts verifies that sentence and ssml speech marks are
// emitted once per semantic unit (sentence/SSML element), not once per word.
// AWS emits exactly one sentence mark per sentence and one ssml mark for the
// implicit <speak> wrapper — the previous implementation multiplied marks by
// the word count, which broke subtitle-timing and lip-sync consumers.
func TestParity_SpeechMarkCounts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantCounts    map[string]int // type → exact count
		wantNotCounts map[string]int // type → count that must NOT appear
		name          string
		text          string
		marks         []string
	}{
		{
			name:       "sentence_single_no_punct",
			text:       "hello world",
			marks:      []string{"sentence"},
			wantCounts: map[string]int{"sentence": 1},
		},
		{
			name:       "sentence_two_sentences",
			text:       "Hello world. Goodbye now.",
			marks:      []string{"sentence"},
			wantCounts: map[string]int{"sentence": 2},
		},
		{
			name:       "sentence_three_sentences_mixed_punct",
			text:       "Hello! How are you? Goodbye.",
			marks:      []string{"sentence"},
			wantCounts: map[string]int{"sentence": 3},
		},
		{
			name:       "ssml_once_regardless_of_word_count",
			text:       "one two three four five",
			marks:      []string{"ssml"},
			wantCounts: map[string]int{"ssml": 1},
		},
		{
			name:       "word_marks_per_word",
			text:       "alpha beta gamma",
			marks:      []string{"word"},
			wantCounts: map[string]int{"word": 3},
		},
		{
			name:       "viseme_marks_per_word",
			text:       "alpha beta gamma",
			marks:      []string{"viseme"},
			wantCounts: map[string]int{"viseme": 3},
		},
		{
			name:  "combined_sentence_word_viseme",
			text:  "Hello world. Goodbye.",
			marks: []string{"sentence", "word", "viseme"},
			// 2 sentences, 3 words (Hello, world., Goodbye.), 3 visemes
			wantCounts: map[string]int{"sentence": 2, "word": 3, "viseme": 3},
		},
		{
			name:       "ssml_not_per_word",
			text:       "one two three",
			marks:      []string{"ssml"},
			wantCounts: map[string]int{"ssml": 1},
			wantNotCounts: map[string]int{
				// Must NOT be 3 (one per word — the old bug)
				"ssml": 3,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := request(t, newHandler(), http.MethodPost, "/v1/speech", map[string]any{
				"OutputFormat":    "json",
				"SpeechMarkTypes": tc.marks,
				"Text":            tc.text,
				"VoiceId":         "Joanna",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			// Each line in the response is a JSON speech mark event.
			counts := make(map[string]int)
			for line := range strings.SplitSeq(strings.TrimSpace(rec.Body.String()), "\n") {
				if line == "" {
					continue
				}
				var ev map[string]any
				require.NoError(t, json.Unmarshal([]byte(line), &ev), "invalid JSON line: %s", line)
				typ, _ := ev["type"].(string)
				counts[typ]++
			}

			for typ, want := range tc.wantCounts {
				assert.Equal(t, want, counts[typ], "count of %q marks", typ)
			}
			for typ, notWant := range tc.wantNotCounts {
				assert.NotEqual(t, notWant, counts[typ], "count of %q marks must not equal %d", typ, notWant)
			}
		})
	}
}

// TestParity_SpeechMarkTimeOrder verifies that speech marks are ordered by
// ascending time, matching AWS output ordering. Sentence marks precede word
// marks at the same time position (stable sort on equal times).
func TestParity_SpeechMarkTimeOrder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		text  string
		marks []string
	}{
		{
			name:  "sentence_before_word_at_t0",
			text:  "hello world",
			marks: []string{"sentence", "word"},
		},
		{
			name:  "multi_sentence_interleaved_with_words",
			text:  "Hello. World.",
			marks: []string{"sentence", "word"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := request(t, newHandler(), http.MethodPost, "/v1/speech", map[string]any{
				"OutputFormat":    "json",
				"SpeechMarkTypes": tc.marks,
				"Text":            tc.text,
				"VoiceId":         "Joanna",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var prevTime float64 = -1
			for line := range strings.SplitSeq(strings.TrimSpace(rec.Body.String()), "\n") {
				if line == "" {
					continue
				}
				var ev map[string]any
				require.NoError(t, json.Unmarshal([]byte(line), &ev))
				timeVal, _ := ev["time"].(float64)
				assert.GreaterOrEqual(t, timeVal, prevTime, "marks must be non-decreasing by time")
				prevTime = timeVal
			}
		})
	}
}
