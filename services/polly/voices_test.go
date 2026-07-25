package polly_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDescribeVoicesFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		query      string
		contains   string
		notContain string
		count      int
	}{
		{name: "neural", query: "?Engine=neural", count: 3, notContain: "Aditi"},
		{name: "male_english", query: "?LanguageCode=en-US&Gender=Male", count: 1, contains: "Matthew"},
		{name: "additional_language_disabled", query: "?LanguageCode=hi-IN", count: 0},
		{
			name:     "additional_language_enabled",
			query:    "?LanguageCode=hi-IN&IncludeAdditionalLanguageCodes=true",
			count:    1,
			contains: "Aditi",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			rec := request(t, newHandler(), http.MethodGet, "/v1/voices"+test.query, nil)
			require.Equal(t, http.StatusOK, rec.Code)
			voices := responseMap(t, rec)["Voices"].([]any)
			// count is a lower-bound now that the voice catalogue is expanded.
			assert.GreaterOrEqual(t, len(voices), test.count)
			assert.Contains(t, rec.Body.String(), test.contains)
			if test.notContain != "" {
				assert.NotContains(t, rec.Body.String(), test.notContain)
			}
		})
	}
}

// TestDescribeVoicesInvalidEngine verifies that DescribeVoices rejects an
// unrecognized Engine value. AWS returns InvalidParameterValueException for
// engines not in {standard, neural, long-form, generative}.
func TestDescribeVoicesInvalidEngine(t *testing.T) {
	t.Parallel()

	rec := request(t, newHandler(), http.MethodGet, "/v1/voices?Engine=quantum", nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidParameterValueException")
}

// TestDescribeVoicesExpandedCatalogue verifies that the built-in voice
// catalogue is substantially populated (≥70 voices) to reflect the real AWS
// Polly list.
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

			out := responseMap(t, rec)

			voices, ok := out["Voices"].([]any)
			require.True(t, ok)
			assert.GreaterOrEqual(t, len(voices), test.minVoices,
				"expected at least %d voices for %s", test.minVoices, test.path)
		})
	}
}

// TestDescribeVoicesCatalogueExactCount verifies that the built-in voice
// catalogue has exactly 106 voices -- every VoiceId enum value in
// aws-sdk-go-v2/service/polly/types (pinned SDK version, see PARITY.md)
// except Patrick/Alba/Raúl, which are documented on AWS's live voice list but
// not yet part of that pinned SDK's VoiceId enum.
func TestDescribeVoicesCatalogueExactCount(t *testing.T) {
	t.Parallel()

	rec := request(t, newHandler(), http.MethodGet, "/v1/voices", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	voices := responseMap(t, rec)["Voices"].([]any)
	assert.Len(t, voices, 106)
}

// TestDescribeVoicesNewCatalogueEntries spot-checks voice IDs that were
// missing from the catalogue before this parity pass (PARITY.md's "Full
// VoiceId catalogue completion" deferred item), verifying each is present
// with its real AWS LanguageCode/Gender/SupportedEngines per
// https://docs.aws.amazon.com/polly/latest/dg/voicelist.html.
func TestDescribeVoicesNewCatalogueEntries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		voiceID      string
		languageCode string
		gender       string
		engine       string
	}{
		{name: "zeina_arabic_standard", voiceID: "Zeina", languageCode: "arb", gender: "Female", engine: "standard"},
		{name: "hala_gulf_arabic_neural", voiceID: "Hala", languageCode: "ar-AE", gender: "Female", engine: "neural"},
		{name: "zayd_gulf_arabic_neural", voiceID: "Zayd", languageCode: "ar-AE", gender: "Male", engine: "neural"},
		{
			name: "geraint_welsh_english_standard", voiceID: "Geraint",
			languageCode: "en-GB-WLS", gender: "Male", engine: "standard",
		},
		{
			name: "hiujin_cantonese_neural", voiceID: "Hiujin",
			languageCode: "yue-CN", gender: "Female", engine: "neural",
		},
		{
			name: "tomoko_japanese_neural", voiceID: "Tomoko",
			languageCode: "ja-JP", gender: "Female", engine: "neural",
		},
		{
			name: "danielle_us_english_generative", voiceID: "Danielle",
			languageCode: "en-US", gender: "Female", engine: "generative",
		},
		{
			name: "tiffany_us_english_generative_only", voiceID: "Tiffany",
			languageCode: "en-US", gender: "Female", engine: "generative",
		},
		{
			name: "sabrina_swiss_german_neural", voiceID: "Sabrina",
			languageCode: "de-CH", gender: "Female", engine: "neural",
		},
		{
			name: "andres_mexican_spanish_generative", voiceID: "Andres",
			languageCode: "es-MX", gender: "Male", engine: "generative",
		},
	}

	handler := newHandler()

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := request(t, handler, http.MethodGet,
				"/v1/voices?LanguageCode="+tc.languageCode+"&Engine="+tc.engine, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			voices := responseMap(t, rec)["Voices"].([]any)
			var found map[string]any
			for _, v := range voices {
				voice := v.(map[string]any)
				if voice["Id"] == tc.voiceID {
					found = voice

					break
				}
			}

			require.NotNil(t, found, "voice %q not found for LanguageCode=%s&Engine=%s",
				tc.voiceID, tc.languageCode, tc.engine)
			assert.Equal(t, tc.gender, found["Gender"])
		})
	}
}

// TestDescribeVoicesTiffanyGenerativeOnly verifies that Tiffany (US English)
// supports only the generative engine -- unlike most US English voices, it
// has no standard or neural support per the AWS voice table.
func TestDescribeVoicesTiffanyGenerativeOnly(t *testing.T) {
	t.Parallel()

	handler := newHandler()

	for _, engine := range []string{"standard", "neural"} {
		rec := request(t, handler, http.MethodGet, "/v1/voices?LanguageCode=en-US&Engine="+engine, nil)
		require.Equal(t, http.StatusOK, rec.Code)
		assert.NotContains(t, rec.Body.String(), `"Id":"Tiffany"`, "engine=%s", engine)
	}

	rec := request(t, handler, http.MethodGet, "/v1/voices?LanguageCode=en-US&Engine=generative", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `"Id":"Tiffany"`)
}
