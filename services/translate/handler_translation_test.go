package translate_test

import (
	"encoding/base64"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandler_TranslateDocument_Base64RoundTrip verifies that Document.Content
// is base64-decoded before translation and TranslatedDocument.Content is
// base64-encoded in the response, matching the real SDK's blob handling (see
// awsAwsjson11_serializeDocumentDocument / _deserializeDocumentTranslatedDocument
// in aws-sdk-go-v2/service/translate). Previously the handler treated Content
// as literal text in both directions, so real SDK clients (which always
// base64-encode Document.Content and base64-decode the response) would send
// base64 garbage as translation input and fail decoding the response.
func TestHandler_TranslateDocument_Base64RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "TranslateDocument", map[string]any{
		"Document": map[string]any{
			"Content":     b64("Hello world"),
			"ContentType": "text/plain",
		},
		"SourceLanguageCode": "en",
		"TargetLanguageCode": "es",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	m := unmarshalJSON(t, rec.Body.Bytes())
	doc, ok := m["TranslatedDocument"].(map[string]any)
	require.True(t, ok, "TranslatedDocument must be present")

	contentB64, ok := doc["Content"].(string)
	require.True(t, ok, "TranslatedDocument.Content must be a string")

	decoded, err := base64.StdEncoding.DecodeString(contentB64)
	require.NoError(t, err, "TranslatedDocument.Content must be valid base64")
	assert.Contains(t, string(decoded), "Hello world")
}

// TestHandler_TranslateDocument_InvalidBase64Rejected verifies that
// non-base64 Document.Content is rejected as InvalidRequestException rather
// than silently passed through as literal text.
func TestHandler_TranslateDocument_InvalidBase64Rejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "TranslateDocument", map[string]any{
		"Document": map[string]any{
			"Content":     "not base64!!! ###",
			"ContentType": "text/plain",
		},
		"SourceLanguageCode": "en",
		"TargetLanguageCode": "es",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestHandler_TranslateDocument_MissingDocumentRejected verifies that a
// missing Document (a required member of TranslateDocumentInput) is rejected
// rather than silently translating empty content.
func TestHandler_TranslateDocument_MissingDocumentRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "TranslateDocument", map[string]any{
		"SourceLanguageCode": "en",
		"TargetLanguageCode": "es",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestHandler_TranslateText_AppliedTerminologiesReportsMatchedTerms verifies
// that AppliedTerminologies.Terms lists the source/target pairs actually
// substituted into the translated text, not a fabricated empty slice.
// Previously the handler always returned an empty Terms array regardless of
// which terms matched -- a disguised no-op the code's own comment described
// as wrong ("Terms slice lists matched pairs") without ever implementing it.
func TestHandler_TranslateText_AppliedTerminologiesReportsMatchedTerms(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	importRec := doRequest(t, h, "ImportTerminology", map[string]any{
		"Name":          "matched-term",
		"MergeStrategy": "OVERWRITE",
		"TerminologyData": map[string]any{
			// Header row (en,es) must NOT itself be treated as a term pair.
			"File":   b64("en,es\nhello,hola\nworld,mundo"),
			"Format": "CSV",
		},
	})
	require.Equal(t, http.StatusOK, importRec.Code)

	rec := doRequest(t, h, "TranslateText", map[string]any{
		"Text":               "hello there",
		"SourceLanguageCode": "en",
		"TargetLanguageCode": "es",
		"TerminologyNames":   []string{"matched-term"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	m := unmarshalJSON(t, rec.Body.Bytes())
	applied, ok := m["AppliedTerminologies"].([]any)
	require.True(t, ok)
	require.Len(t, applied, 1)

	entry := applied[0].(map[string]any)
	assert.Equal(t, "matched-term", entry["Name"])

	terms, ok := entry["Terms"].([]any)
	require.True(t, ok)
	require.Len(t, terms, 1, "only the matched 'hello' pair should be reported, not the unmatched 'world' pair")

	term := terms[0].(map[string]any)
	assert.Equal(t, "hello", term["SourceText"])
	assert.Equal(t, "hola", term["TargetText"])

	// The CSV header row's "en" must not have been substituted into the
	// translated text (it previously was, since the header row was wrongly
	// treated as a term pair alongside the real data rows).
	assert.Equal(t, "es: hola there", m["TranslatedText"])
}

func TestTranslateText(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		check    func(t *testing.T, body []byte)
		name     string
		wantCode int
	}{
		{
			name: "translates text returns source and target",
			body: map[string]any{
				"Text":               "Hello world",
				"SourceLanguageCode": "en",
				"TargetLanguageCode": "fr",
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()

				m := unmarshalJSON(t, body)
				// Translation must differ from source (real-ish transform, not echo)
				// and must contain the original text.
				translated, _ := m["TranslatedText"].(string)
				assert.Contains(t, translated, "Hello world")
				assert.NotEqual(t, "Hello world", translated, "translated text should differ from source")
				assert.Equal(t, "en", m["SourceLanguageCode"])
				assert.Equal(t, "fr", m["TargetLanguageCode"])
			},
		},
		{
			name: "auto-detect source language",
			body: map[string]any{
				"Text":               "Bonjour",
				"SourceLanguageCode": "auto",
				"TargetLanguageCode": "en",
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()

				m := unmarshalJSON(t, body)
				assert.NotEmpty(t, m["SourceLanguageCode"])
				assert.Equal(t, "en", m["TargetLanguageCode"])
			},
		},
		{
			name:     "missing Text returns error",
			body:     map[string]any{"SourceLanguageCode": "en", "TargetLanguageCode": "fr"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing TargetLanguageCode returns error",
			body:     map[string]any{"Text": "Hello", "SourceLanguageCode": "en"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "TranslateText", tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestTranslateDocument(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "TranslateDocument", map[string]any{
		"Document": map[string]any{
			"Content":     "SGVsbG8gd29ybGQ=",
			"ContentType": "text/plain",
		},
		"SourceLanguageCode": "en",
		"TargetLanguageCode": "fr",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	m := unmarshalJSON(t, rec.Body.Bytes())
	assert.NotNil(t, m["TranslatedDocument"])
	assert.Equal(t, "en", m["SourceLanguageCode"])
	assert.Equal(t, "fr", m["TargetLanguageCode"])
}

// TestTranslateText_OmittedSourceLangDefaultsToAuto verifies that omitting
// SourceLanguageCode defaults to "auto" in the response, matching AWS behavior.
func TestTranslateText_OmittedSourceLangDefaultsToAuto(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "TranslateText", map[string]any{
		"Text":               "Hello",
		"TargetLanguageCode": "fr",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	m := unmarshalJSON(t, rec.Body.Bytes())
	assert.Equal(t, "auto", m["SourceLanguageCode"])
}

// TestTranslateDocument_OmittedSourceLangDefaultsToAuto verifies that
// omitting SourceLanguageCode defaults to "auto" in the response.
func TestTranslateDocument_OmittedSourceLangDefaultsToAuto(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "TranslateDocument", map[string]any{
		"Document":           map[string]any{"Content": "SGVsbG8=", "ContentType": "text/plain"},
		"TargetLanguageCode": "de",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	m := unmarshalJSON(t, rec.Body.Bytes())
	assert.Equal(t, "auto", m["SourceLanguageCode"])
}

// TestTranslateTextIncludesAppliedTerminologies verifies that TranslateText
// includes the AppliedTerminologies field in the response. Real AWS always returns
// this field; the emulator previously omitted it, causing SDK callers that access
// output.AppliedTerminologies to see nil and misreport that no terminology was used.
func TestTranslateTextIncludesAppliedTerminologies(t *testing.T) {
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

// TestTranslateDocumentIncludesAppliedTerminologies verifies that
// TranslateDocument also includes the AppliedTerminologies field.
func TestTranslateDocumentIncludesAppliedTerminologies(t *testing.T) {
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
			"Content":     b64("Hello"),
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

// TestTranslateText_AppliedSettings verifies that TranslateText echoes
// back the input Settings in the response.
func TestTranslateText_AppliedSettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		settings    map[string]any
		name        string
		wantBriefly bool
	}{
		{
			name: "formality_echoed",
			settings: map[string]any{
				"Formality": "FORMAL",
			},
			wantBriefly: true,
		},
		{
			name:        "no_settings_returns_empty",
			settings:    nil,
			wantBriefly: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{
				"Text":               "Hello",
				"SourceLanguageCode": "en",
				"TargetLanguageCode": "es",
			}
			if tt.settings != nil {
				body["Settings"] = tt.settings
			}

			rec := doRequest(t, h, "TranslateText", body)
			require.Equal(t, http.StatusOK, rec.Code)

			resp := unmarshalJSON(t, rec.Body.Bytes())
			if tt.wantBriefly {
				appliedSettings, hasSettings := resp["AppliedSettings"]
				require.True(t, hasSettings, "AppliedSettings must be present when Settings provided")
				s := appliedSettings.(map[string]any)
				assert.Equal(t, "FORMAL", s["Formality"])
			}
			// When no Settings provided, AWS returns AppliedSettings: {} — presence is OK
		})
	}
}

// TestTranslateDocument_AppliedSettings verifies TranslateDocument echoes Settings.
func TestTranslateDocument_AppliedSettings(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "TranslateDocument", map[string]any{
		"Document": map[string]any{
			"Content":     b64("Hello"),
			"ContentType": "text/plain",
		},
		"SourceLanguageCode": "en",
		"TargetLanguageCode": "es",
		"Settings": map[string]any{
			"Profanity": "MASK",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := unmarshalJSON(t, rec.Body.Bytes())
	settings, ok := resp["AppliedSettings"].(map[string]any)
	require.True(t, ok, "AppliedSettings must be present when Settings provided")
	assert.Equal(t, "MASK", settings["Profanity"])
}
