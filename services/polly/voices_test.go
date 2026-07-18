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
