package translate_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/translate"
)

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

func TestImportTerminology(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		check    func(t *testing.T, body []byte)
		name     string
		wantCode int
	}{
		{
			name: "creates terminology returns properties",
			body: map[string]any{
				"Name":            "my-terminology",
				"MergeStrategy":   "OVERWRITE",
				"TerminologyData": map[string]any{"File": b64("en,fr\nhello,bonjour"), "Format": "CSV"},
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()

				m := unmarshalJSON(t, body)
				props, ok := m["TerminologyProperties"].(map[string]any)
				require.True(t, ok, "TerminologyProperties missing")
				assert.Equal(t, "my-terminology", props["Name"])
				assert.NotEmpty(t, props["Arn"])
				assert.NotEmpty(t, props["CreatedAt"])
			},
		},
		{
			name:     "missing Name returns error",
			body:     map[string]any{"MergeStrategy": "OVERWRITE", "TerminologyData": map[string]any{"Format": "CSV"}},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "overwrite existing terminology",
			body: map[string]any{
				"Name":            "my-terminology",
				"MergeStrategy":   "OVERWRITE",
				"Description":     "updated",
				"TerminologyData": map[string]any{"File": b64("en,de\nhello,hallo"), "Format": "CSV"},
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()

				m := unmarshalJSON(t, body)
				props := m["TerminologyProperties"].(map[string]any)
				assert.Equal(t, "updated", props["Description"])
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "ImportTerminology", tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestGetTerminology(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *translate.Handler)
		body     any
		name     string
		wantCode int
	}{
		{
			name: "returns existing terminology",
			setup: func(t *testing.T, h *translate.Handler) {
				t.Helper()

				rec := doRequest(t, h, "ImportTerminology", map[string]any{
					"Name": "my-term", "MergeStrategy": "OVERWRITE",
					"TerminologyData": map[string]any{"Format": "CSV"},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body:     map[string]any{"Name": "my-term"},
			wantCode: http.StatusOK,
		},
		{
			name:     "returns error for missing terminology",
			body:     map[string]any{"Name": "nonexistent"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tc.setup != nil {
				tc.setup(t, h)
			}

			rec := doRequest(t, h, "GetTerminology", tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

func TestDeleteTerminology(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
		preload  bool
	}{
		{
			name:     "deletes existing terminology",
			wantCode: http.StatusOK,
			preload:  true,
		},
		{
			name:     "error when terminology missing",
			wantCode: http.StatusBadRequest,
			preload:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tc.preload {
				rec := doRequest(t, h, "ImportTerminology", map[string]any{
					"Name": "to-delete", "MergeStrategy": "OVERWRITE",
					"TerminologyData": map[string]any{"Format": "CSV"},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "DeleteTerminology", map[string]any{"Name": "to-delete"})
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

func TestListTerminologies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		check    func(t *testing.T, body []byte)
		name     string
		wantCode int
		count    int
	}{
		{
			name:     "empty list returns empty array",
			body:     map[string]any{},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()

				m := unmarshalJSON(t, body)
				list, _ := m["TerminologyPropertiesList"].([]any)
				assert.Empty(t, list)
			},
		},
		{
			name:     "returns created terminologies",
			body:     map[string]any{},
			wantCode: http.StatusOK,
			count:    2,
			check: func(t *testing.T, body []byte) {
				t.Helper()

				m := unmarshalJSON(t, body)
				list, _ := m["TerminologyPropertiesList"].([]any)
				assert.Len(t, list, 2)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for i := range tc.count {
				rec := doRequest(t, h, "ImportTerminology", map[string]any{
					"Name":            "term-" + string(rune('a'+i)),
					"MergeStrategy":   "OVERWRITE",
					"TerminologyData": map[string]any{"Format": "CSV"},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "ListTerminologies", tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

// TestListTerminologies_Pagination verifies that MaxResults and NextToken
// paginate correctly through all terminologies.
func TestListTerminologies_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 5 {
		rec := doRequest(t, h, "ImportTerminology", map[string]any{
			"Name":            "term-" + string(rune('a'+i)),
			"MergeStrategy":   "OVERWRITE",
			"TerminologyData": map[string]any{"Format": "CSV"},
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doRequest(t, h, "ListTerminologies", map[string]any{"MaxResults": 2})
	require.Equal(t, http.StatusOK, rec.Code)

	m := unmarshalJSON(t, rec.Body.Bytes())
	page1, _ := m["TerminologyPropertiesList"].([]any)
	assert.Len(t, page1, 2)
	nextToken, _ := m["NextToken"].(string)
	assert.NotEmpty(t, nextToken)

	rec = doRequest(t, h, "ListTerminologies", map[string]any{"MaxResults": 2, "NextToken": nextToken})
	require.Equal(t, http.StatusOK, rec.Code)

	m = unmarshalJSON(t, rec.Body.Bytes())
	page2, _ := m["TerminologyPropertiesList"].([]any)
	assert.Len(t, page2, 2)

	rec = doRequest(t, h, "ListTerminologies", map[string]any{
		"MaxResults": 10,
		"NextToken":  m["NextToken"].(string),
	})
	require.Equal(t, http.StatusOK, rec.Code)

	m = unmarshalJSON(t, rec.Body.Bytes())
	page3, _ := m["TerminologyPropertiesList"].([]any)
	assert.Len(t, page3, 1)
	assert.Nil(t, m["NextToken"])
}

// TestGetTerminology_DataLocationField verifies that GetTerminology returns
// a TerminologyDataLocation with RepositoryType and Location fields, matching
// AWS behavior.
func TestGetTerminology_DataLocationField(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "ImportTerminology", map[string]any{
		"Name":            "loc-term",
		"MergeStrategy":   "OVERWRITE",
		"TerminologyData": map[string]any{"Format": "CSV"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "GetTerminology", map[string]any{"Name": "loc-term"})
	require.Equal(t, http.StatusOK, rec.Code)

	m := unmarshalJSON(t, rec.Body.Bytes())
	loc, ok := m["TerminologyDataLocation"].(map[string]any)
	require.True(t, ok, "TerminologyDataLocation must be present")
	assert.Equal(t, "S3", loc["RepositoryType"])
	assert.NotEmpty(t, loc["Location"])
}

// TestImportTerminology_ARNFormat verifies that the ARN returned by
// ImportTerminology is well-formed and includes the terminology name.
func TestImportTerminology_ARNFormat(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "ImportTerminology", map[string]any{
		"Name":            "arn-check-term",
		"MergeStrategy":   "OVERWRITE",
		"TerminologyData": map[string]any{"Format": "CSV"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	props := unmarshalJSON(t, rec.Body.Bytes())["TerminologyProperties"].(map[string]any)
	termARN, _ := props["Arn"].(string)
	assert.Contains(t, termARN, "arn:aws:translate:")
	assert.Contains(t, termARN, "terminology/arn-check-term")
}

// TestImportTerminology_SetsLanguagesFromCSV verifies that importing a CSV
// terminology correctly parses the header row to set SourceLanguage and TargetLanguages.
func TestImportTerminology_SetsLanguagesFromCSV(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		csv           string
		wantSource    string
		wantTargets   []any
		wantTermCount float64
	}{
		{
			name:          "two_target_languages",
			csv:           "en,es,fr\nhello,hola,bonjour\nworld,mundo,monde",
			wantSource:    "en",
			wantTargets:   []any{"es", "fr"},
			wantTermCount: 2,
		},
		{
			name:          "single_target_language",
			csv:           "en,de\ncat,Katze",
			wantSource:    "en",
			wantTargets:   []any{"de"},
			wantTermCount: 1,
		},
		{
			name:          "no_data_rows",
			csv:           "en,es",
			wantSource:    "en",
			wantTargets:   []any{"es"},
			wantTermCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "ImportTerminology", map[string]any{
				"Name":          "csv-term",
				"MergeStrategy": "OVERWRITE",
				"TerminologyData": map[string]any{
					"File":   b64(tt.csv),
					"Format": "CSV",
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			resp := unmarshalJSON(t, rec.Body.Bytes())
			props := resp["TerminologyProperties"].(map[string]any)

			assert.Equal(t, tt.wantSource, props["SourceLanguageCode"])
			assert.InDelta(t, tt.wantTermCount, props["TermCount"], 0)

			targets, ok := props["TargetLanguageCodes"].([]any)
			require.True(t, ok, "TargetLanguageCodes must be present")
			assert.Equal(t, tt.wantTargets, targets)
		})
	}
}

// TestImportTerminology_MergeStrategyValidation verifies that only OVERWRITE
// is accepted as MergeStrategy.
func TestImportTerminology_MergeStrategyValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		mergeStrategy string
		wantCode      int
	}{
		{name: "overwrite_accepted", mergeStrategy: "OVERWRITE", wantCode: http.StatusOK},
		{name: "empty_accepted", mergeStrategy: "", wantCode: http.StatusOK},
		{name: "merge_rejected", mergeStrategy: "MERGE", wantCode: http.StatusBadRequest},
		{name: "invalid_rejected", mergeStrategy: "REPLACE", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{
				"Name": "merge-test",
				"TerminologyData": map[string]any{
					"File":   b64("en,es"),
					"Format": "CSV",
				},
			}
			if tt.mergeStrategy != "" {
				body["MergeStrategy"] = tt.mergeStrategy
			}

			rec := doRequest(t, h, "ImportTerminology", body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestListTerminologies_FormatFilter verifies that TerminologyDataFormat
// filters the list to matching terminologies only.
func TestListTerminologies_FormatFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, name := range []string{"csv-term", "tmx-term"} {
		format := "CSV"
		if strings.HasPrefix(name, "tmx") {
			format = "TMX"
		}

		rec := doRequest(t, h, "ImportTerminology", map[string]any{
			"Name":          name,
			"MergeStrategy": "OVERWRITE",
			"TerminologyData": map[string]any{
				"File":   b64("en,es"),
				"Format": format,
			},
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	tests := []struct {
		name      string
		filter    string
		wantCount int
	}{
		{name: "csv_filter", filter: "CSV", wantCount: 1},
		{name: "tmx_filter", filter: "TMX", wantCount: 1},
		{name: "no_filter", filter: "", wantCount: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			body := map[string]any{}
			if tt.filter != "" {
				body["TerminologyDataFormat"] = tt.filter
			}

			rec := doRequest(t, h, "ListTerminologies", body)
			require.Equal(t, http.StatusOK, rec.Code)

			resp := unmarshalJSON(t, rec.Body.Bytes())
			terms := resp["TerminologyPropertiesList"].([]any)
			assert.Len(t, terms, tt.wantCount)
		})
	}
}

// TestTerminologyToMap_IncludesTargetLanguageCodes verifies GetTerminology
// response includes TargetLanguageCodes derived from the CSV header.
func TestTerminologyToMap_IncludesTargetLanguageCodes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "ImportTerminology", map[string]any{
		"Name":          "tgt-lang-term",
		"MergeStrategy": "OVERWRITE",
		"TerminologyData": map[string]any{
			"File":   b64("en,es,de\nhello,hola,hallo"),
			"Format": "CSV",
		},
	})

	rec := doRequest(t, h, "GetTerminology", map[string]any{"Name": "tgt-lang-term"})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := unmarshalJSON(t, rec.Body.Bytes())
	props := resp["TerminologyProperties"].(map[string]any)
	targets, ok := props["TargetLanguageCodes"].([]any)
	require.True(t, ok, "TargetLanguageCodes must be present")
	assert.Equal(t, []any{"es", "de"}, targets)
}

// TestImportTerminology_OverwriteUpdatesLanguages verifies that a second
// ImportTerminology with OVERWRITE updates SourceLanguage and TargetLanguages.
func TestImportTerminology_OverwriteUpdatesLanguages(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "ImportTerminology", map[string]any{
		"Name":          "overwrite-term",
		"MergeStrategy": "OVERWRITE",
		"TerminologyData": map[string]any{
			"File":   b64("en,es\nhello,hola"),
			"Format": "CSV",
		},
	})

	doRequest(t, h, "ImportTerminology", map[string]any{
		"Name":          "overwrite-term",
		"MergeStrategy": "OVERWRITE",
		"TerminologyData": map[string]any{
			"File":   b64("fr,de,it\nbonjour,hallo,ciao"),
			"Format": "CSV",
		},
	})

	rec := doRequest(t, h, "GetTerminology", map[string]any{"Name": "overwrite-term"})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := unmarshalJSON(t, rec.Body.Bytes())
	props := resp["TerminologyProperties"].(map[string]any)
	assert.Equal(t, "fr", props["SourceLanguageCode"])

	targets := props["TargetLanguageCodes"].([]any)
	assert.Equal(t, []any{"de", "it"}, targets)
}

// TestJSONEncoding_TermCount verifies TermCount is numeric not string in JSON.
func TestJSONEncoding_TermCount(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "ImportTerminology", map[string]any{
		"Name":          "count-term",
		"MergeStrategy": "OVERWRITE",
		"TerminologyData": map[string]any{
			"File":   b64("en,es\none,uno\ntwo,dos"),
			"Format": "CSV",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	m := unmarshalJSON(t, rec.Body.Bytes())
	props := m["TerminologyProperties"].(map[string]any)
	termCount, ok := props["TermCount"].(float64)
	require.True(t, ok, "TermCount must be a JSON number not a string")
	assert.InDelta(t, float64(2), termCount, 0)
}

// TestImportTerminology_MissingTerminologyDataRejected verifies that
// omitting TerminologyData entirely (a required top-level member of
// ImportTerminologyRequest) is rejected rather than silently defaulted to an
// empty CSV terminology.
func TestImportTerminology_MissingTerminologyDataRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "ImportTerminology", map[string]any{
		"Name":          "no-data-term",
		"MergeStrategy": "OVERWRITE",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	m := unmarshalJSON(t, rec.Body.Bytes())
	assert.Equal(t, "InvalidParameterValueException", m["__type"])
}

// TestImportTerminology_FileSizeLimitExceeded verifies that a
// TerminologyData.File larger than the 10 MB custom terminology file size
// quota is rejected as LimitExceededException.
func TestImportTerminology_FileSizeLimitExceeded(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	const overLimit = 10*1024*1024 + 1

	rec := doRequest(t, h, "ImportTerminology", map[string]any{
		"Name":          "big-term",
		"MergeStrategy": "OVERWRITE",
		"TerminologyData": map[string]any{
			"File":   b64(strings.Repeat("a", overLimit)),
			"Format": "CSV",
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	m := unmarshalJSON(t, rec.Body.Bytes())
	assert.Equal(t, "LimitExceededException", m["__type"])
}

// TestImportTerminology_TooManyTagsRejected verifies that importing a
// terminology with more than 50 tags is rejected as TooManyTagsException.
func TestImportTerminology_TooManyTagsRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	const tooMany = 51

	tags := make([]map[string]any, 0, tooMany)
	for i := range tooMany {
		tags = append(tags, map[string]any{"Key": "k" + string(rune('a'+i)), "Value": "v"})
	}

	rec := doRequest(t, h, "ImportTerminology", map[string]any{
		"Name":            "many-tags-term",
		"MergeStrategy":   "OVERWRITE",
		"TerminologyData": map[string]any{"Format": "CSV"},
		"Tags":            tags,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	m := unmarshalJSON(t, rec.Body.Bytes())
	assert.Equal(t, "TooManyTagsException", m["__type"])
}

// TestImportTerminology_FormatValidation verifies that TerminologyData.Format
// is restricted to the modeled CSV|TMX|TSV enum.
func TestImportTerminology_FormatValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		format   string
		wantCode int
	}{
		{name: "csv_accepted", format: "CSV", wantCode: http.StatusOK},
		{name: "tmx_accepted", format: "TMX", wantCode: http.StatusOK},
		{name: "tsv_accepted", format: "TSV", wantCode: http.StatusOK},
		{name: "invalid_rejected", format: "XML", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "ImportTerminology", map[string]any{
				"Name":            "format-test-" + tt.name,
				"MergeStrategy":   "OVERWRITE",
				"TerminologyData": map[string]any{"Format": tt.format},
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestGetTerminology_MissingNameIsInvalidParameter verifies that GetTerminology
// (which models InvalidParameterValueException, not InvalidRequestException)
// reports the correct __type for a missing Name.
func TestGetTerminology_MissingNameIsInvalidParameter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "GetTerminology", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	m := unmarshalJSON(t, rec.Body.Bytes())
	assert.Equal(t, "InvalidParameterValueException", m["__type"])
}
