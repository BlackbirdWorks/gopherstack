package translate_test

import (
	"encoding/base64"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// b64 base64-encodes s, matching what the real aws-sdk-go-v2 translate client
// does to every blob field (TerminologyData.File, Document.Content) before
// sending a request. Test bodies that populate those fields must use this
// helper rather than sending literal text, since the handler now decodes
// them the same way the real service does.
func b64(s string) string {
	return base64.StdEncoding.EncodeToString([]byte(s))
}

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

// TestHandler_ImportTerminology_Base64RoundTrip verifies that
// TerminologyData.File is base64-decoded before being parsed as CSV,
// matching the real SDK's blob handling (see
// awsAwsjson11_serializeDocumentTerminologyData in
// aws-sdk-go-v2/service/translate/serializers.go). Previously the handler
// stored the raw (base64) string as the terminology file, so a real SDK
// client's CSV content would be stored and parsed as base64 garbage instead
// of the actual terminology rows.
func TestHandler_ImportTerminology_Base64RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "ImportTerminology", map[string]any{
		"Name":          "b64-term",
		"MergeStrategy": "OVERWRITE",
		"TerminologyData": map[string]any{
			"File":   b64("en,es\nhello,hola"),
			"Format": "CSV",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	m := unmarshalJSON(t, rec.Body.Bytes())
	props := m["TerminologyProperties"].(map[string]any)
	assert.Equal(t, "en", props["SourceLanguageCode"])
	assert.Equal(t, []any{"es"}, props["TargetLanguageCodes"])
	assert.InDelta(t, float64(1), props["TermCount"], 0)
}

// TestHandler_ImportTerminology_InvalidBase64Rejected verifies that
// non-base64 TerminologyData.File is rejected rather than silently stored as
// literal (and un-parseable) CSV content.
func TestHandler_ImportTerminology_InvalidBase64Rejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "ImportTerminology", map[string]any{
		"Name":          "bad-b64-term",
		"MergeStrategy": "OVERWRITE",
		"TerminologyData": map[string]any{
			"File":   "en,es\nhello,hola", // not base64
			"Format": "CSV",
		},
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
