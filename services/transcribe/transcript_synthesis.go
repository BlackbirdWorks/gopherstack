package transcribe

import (
	"encoding/json"
	"fmt"
	"strings"
)

// syntheticWordBaseDuration is the base duration in seconds for synthetic word timing.
const syntheticWordBaseDuration = 0.3

// syntheticWordDurationPerChar is the additional duration per character for synthetic word timing.
const syntheticWordDurationPerChar = 0.05

// syntheticWordGap is the pause between words in synthetic timing.
const syntheticWordGap = 0.05

// marshalJSON marshals v to JSON bytes, returning nil on error (only used for synthetic output).
func marshalJSON(v any) ([]byte, error) {
	return json.Marshal(v)
}

// deriveTranscriptText produces a transcript string from the job name and media URI.
// The filename component of the media URI is extracted and incorporated so the transcript
// differs meaningfully between jobs — unlike a fixed "synthetic" prefix for every job.
func deriveTranscriptText(jobName, mediaURI string) string {
	filename := mediaURIFilename(mediaURI)
	if filename != "" {
		return "Transcription of audio file " + filename + " for job " + jobName + "."
	}

	return "Transcription result for job " + jobName + "."
}

// mediaURIFilename extracts the base filename (without extension) from an S3 URI.
// Returns empty string if the URI cannot be parsed.
func mediaURIFilename(uri string) string {
	if uri == "" {
		return ""
	}
	// Strip protocol prefix.
	s := uri
	if idx := strings.LastIndex(s, "/"); idx >= 0 {
		s = s[idx+1:]
	}
	// Strip query string.
	if idx := strings.IndexByte(s, '?'); idx >= 0 {
		s = s[:idx]
	}
	// Strip extension.
	if idx := strings.LastIndexByte(s, '.'); idx >= 0 {
		s = s[:idx]
	}

	return s
}

// synthesizeTranscriptJSON generates a realistic Transcribe JSON transcript document.
// The transcript contains word-level items with synthetic timing derived from the job name.
func synthesizeTranscriptJSON(jobName, transcriptText string) []byte {
	// Build word-level items with deterministic timing.
	words := splitWords(transcriptText)
	items := make([]transcriptItem, 0, len(words))
	startSec := 0.0

	for i, word := range words {
		dur := syntheticWordBaseDuration + float64(len(word))*syntheticWordDurationPerChar
		items = append(items, transcriptItem{
			StartTime:    fmt.Sprintf("%.3f", startSec),
			EndTime:      fmt.Sprintf("%.3f", startSec+dur),
			Alternatives: []transcriptAlternative{{Content: word, Confidence: "0.99"}},
			Type:         "pronunciation",
		})

		// Add punctuation after every 7th word.
		if (i+1)%7 == 0 {
			items = append(items, transcriptItem{
				Alternatives: []transcriptAlternative{{Content: "."}},
				Type:         "punctuation",
			})
		}

		startSec += dur + syntheticWordGap
	}

	doc := transcriptDocument{
		JobName:   jobName,
		AccountID: defaultAccountID,
		Results: transcriptResults{
			Transcripts: []transcriptEntry{{Transcript: transcriptText}},
			Items:       items,
		},
		Status: "COMPLETED",
	}

	b, _ := marshalJSON(doc)

	return b
}

// splitWords splits text into words by spaces, filtering empties.
func splitWords(text string) []string {
	var words []string
	current := ""

	for _, ch := range text {
		if ch == ' ' || ch == '\t' {
			if current != "" {
				words = append(words, current)
				current = ""
			}
		} else {
			current += string(ch)
		}
	}

	if current != "" {
		words = append(words, current)
	}

	return words
}

// transcriptDocument is the top-level structure of a Transcribe output JSON file.
type transcriptDocument struct {
	JobName   string            `json:"jobName"`
	AccountID string            `json:"accountId"`
	Status    string            `json:"status"`
	Results   transcriptResults `json:"results"`
}

// transcriptResults holds the transcript content.
type transcriptResults struct {
	Transcripts []transcriptEntry `json:"transcripts"`
	Items       []transcriptItem  `json:"items"`
}

// transcriptEntry is a full-transcript string.
type transcriptEntry struct {
	Transcript string `json:"transcript"`
}

// transcriptItem is a word or punctuation item with timing.
type transcriptItem struct {
	StartTime    string                  `json:"start_time,omitempty"`
	EndTime      string                  `json:"end_time,omitempty"`
	Type         string                  `json:"type"`
	Alternatives []transcriptAlternative `json:"alternatives"`
}

// transcriptAlternative holds a word alternative with confidence.
type transcriptAlternative struct {
	Content    string `json:"content"`
	Confidence string `json:"confidence,omitempty"`
}
