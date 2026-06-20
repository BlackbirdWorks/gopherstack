package translate_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_TranslateTextIncludesAppliedTerminologies verifies that TranslateText
// includes the AppliedTerminologies field in the response. Real AWS always returns
// this field; the emulator previously omitted it, causing SDK callers that access
// output.AppliedTerminologies to see nil and misreport that no terminology was used.
func TestParity_TranslateTextIncludesAppliedTerminologies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body          map[string]any
		name          string
		wantTermCount int
	}{
		{
			name: "no_terminology_names_returns_empty_slice",
			body: map[string]any{
				"Text":               "Hello world",
				"SourceLanguageCode": "en",
				"TargetLanguageCode": "es",
			},
			wantTermCount: 0,
		},
		{
			name: "unknown_terminology_name_omitted_from_applied",
			body: map[string]any{
				"Text":               "Hello world",
				"SourceLanguageCode": "en",
				"TargetLanguageCode": "es",
				"TerminologyNames":   []string{"nonexistent-term"},
			},
			wantTermCount: 0,
		},
		{
			name: "existing_terminology_appears_in_applied",
			body: map[string]any{
				"Text":               "Hello world",
				"SourceLanguageCode": "en",
				"TargetLanguageCode": "es",
				"TerminologyNames":   []string{"parity-term"},
			},
			wantTermCount: 1,
		},
	}

	h := newTestHandler(t)

	importRec := doRequest(t, h, "ImportTerminology", map[string]any{
		"Name":          "parity-term",
		"MergeStrategy": "OVERWRITE",
		"TerminologyData": map[string]any{
			"File":   "",
			"Format": "CSV",
		},
	})
	require.Equal(t, http.StatusOK, importRec.Code)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, "TranslateText", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			resp := unmarshalJSON(t, rec.Body.Bytes())

			applied, ok := resp["AppliedTerminologies"].([]any)
			require.True(t, ok, "AppliedTerminologies must be present as an array")
			assert.Len(t, applied, tt.wantTermCount,
				"AppliedTerminologies length must match expected count")
		})
	}
}

// TestParity_TranslateDocumentIncludesAppliedTerminologies verifies that
// TranslateDocument also includes the AppliedTerminologies field.
func TestParity_TranslateDocumentIncludesAppliedTerminologies(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	importRec := doRequest(t, h, "ImportTerminology", map[string]any{
		"Name":          "doc-parity-term",
		"MergeStrategy": "OVERWRITE",
		"TerminologyData": map[string]any{
			"File":   "",
			"Format": "CSV",
		},
	})
	require.Equal(t, http.StatusOK, importRec.Code)

	rec := doRequest(t, h, "TranslateDocument", map[string]any{
		"Document": map[string]any{
			"Content":     "Hello",
			"ContentType": "text/plain",
		},
		"SourceLanguageCode": "en",
		"TargetLanguageCode": "fr",
		"TerminologyNames":   []string{"doc-parity-term"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := unmarshalJSON(t, rec.Body.Bytes())

	applied, ok := resp["AppliedTerminologies"].([]any)
	require.True(t, ok, "AppliedTerminologies must be present as an array in TranslateDocument")
	assert.Len(t, applied, 1, "one matching terminology must appear in AppliedTerminologies")

	item, ok := applied[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "doc-parity-term", item["Name"])
}
