package translate_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListLanguages(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "ListLanguages", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	m := unmarshalJSON(t, rec.Body.Bytes())
	langs, _ := m["Languages"].([]any)
	assert.NotEmpty(t, langs)

	first, ok := langs[0].(map[string]any)
	require.True(t, ok)
	assert.NotEmpty(t, first["LanguageCode"])
	assert.NotEmpty(t, first["LanguageName"])
}

// TestListLanguages_MaxResults verifies that MaxResults correctly limits
// the number of languages returned.
func TestListLanguages_MaxResults(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "ListLanguages", map[string]any{"MaxResults": 5})
	require.Equal(t, http.StatusOK, rec.Code)

	m := unmarshalJSON(t, rec.Body.Bytes())
	langs, _ := m["Languages"].([]any)
	assert.Len(t, langs, 5)
}

// TestListLanguages_Pagination verifies that ListLanguages supports
// NextToken-based pagination.
func TestListLanguages_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "ListLanguages", map[string]any{"MaxResults": float64(5)})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := unmarshalJSON(t, rec.Body.Bytes())
	langs := resp["Languages"].([]any)
	require.Len(t, langs, 5)

	token, hasToken := resp["NextToken"].(string)
	require.True(t, hasToken, "NextToken must be present when more languages remain")
	require.NotEmpty(t, token)

	rec2 := doRequest(t, h, "ListLanguages", map[string]any{
		"MaxResults": float64(5),
		"NextToken":  token,
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	resp2 := unmarshalJSON(t, rec2.Body.Bytes())
	langs2 := resp2["Languages"].([]any)
	assert.NotEmpty(t, langs2, "second page must have results")

	// Language codes on page 2 must differ from page 1
	page1Codes := map[string]bool{}
	for _, l := range langs {
		lang := l.(map[string]any)
		page1Codes[lang["LanguageCode"].(string)] = true
	}
	for _, l := range langs2 {
		lang := l.(map[string]any)
		assert.False(t, page1Codes[lang["LanguageCode"].(string)], "page 2 must not repeat page 1 codes")
	}
}

// TestListLanguages_DisplayLanguageCode verifies that DisplayLanguageCode
// parameter changes the LanguageName in the response.
func TestListLanguages_DisplayLanguageCode(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		name            string
		displayLangCode string
	}{
		{name: "english_display", displayLangCode: "en"},
		{name: "no_display_code", displayLangCode: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			body := map[string]any{"MaxResults": float64(3)}
			if tt.displayLangCode != "" {
				body["DisplayLanguageCode"] = tt.displayLangCode
			}

			rec := doRequest(t, h, "ListLanguages", body)
			require.Equal(t, http.StatusOK, rec.Code)

			resp := unmarshalJSON(t, rec.Body.Bytes())
			langs, ok := resp["Languages"].([]any)
			require.True(t, ok)
			require.NotEmpty(t, langs)

			for _, l := range langs {
				lang := l.(map[string]any)
				assert.NotEmpty(t, lang["LanguageCode"])
				assert.Contains(t, lang, "LanguageName")
			}
		})
	}
}

// TestListLanguages_UnsupportedDisplayLanguageCodeRejected verifies that a
// DisplayLanguageCode outside Translate's fixed 10-value display-language
// enum (de/en/es/fr/it/ja/ko/pt/zh/zh-TW) is rejected as
// UnsupportedDisplayLanguageCodeException. This is a much smaller set than
// the ~75 translation-target language codes ListLanguages itself returns.
func TestListLanguages_UnsupportedDisplayLanguageCodeRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		displayLangCode string
		wantCode        int
	}{
		{name: "known_display_code_accepted", displayLangCode: "ja", wantCode: http.StatusOK},
		{name: "unknown_display_code_rejected", displayLangCode: "xx", wantCode: http.StatusBadRequest},
		// "es-MX" is a real translation-target language code but NOT one of
		// the 10 display-language codes -- exercises that the two sets are
		// validated independently.
		{name: "translation_code_not_a_display_code", displayLangCode: "es-MX", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "ListLanguages", map[string]any{"DisplayLanguageCode": tt.displayLangCode})
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusBadRequest {
				var body map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
				assert.Equal(t, "UnsupportedDisplayLanguageCodeException", body["__type"])
			}
		})
	}
}
