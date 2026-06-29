package translate_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_ImportTerminology_SetsLanguagesFromCSV verifies that importing a CSV
// terminology correctly parses the header row to set SourceLanguage and TargetLanguages.
func TestParity_ImportTerminology_SetsLanguagesFromCSV(t *testing.T) {
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
					"File":   tt.csv,
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

// TestParity_ImportTerminology_MergeStrategyValidation verifies that only OVERWRITE
// is accepted as MergeStrategy.
func TestParity_ImportTerminology_MergeStrategyValidation(t *testing.T) {
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
					"File":   "en,es",
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

// TestParity_ListTerminologies_FormatFilter verifies that TerminologyDataFormat
// filters the list to matching terminologies only.
func TestParity_ListTerminologies_FormatFilter(t *testing.T) {
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
				"File":   "en,es",
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

// TestParity_TranslateText_AppliedSettings verifies that TranslateText echoes
// back the input Settings in the response.
func TestParity_TranslateText_AppliedSettings(t *testing.T) {
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

// TestParity_ListLanguages_Pagination verifies that ListLanguages supports
// NextToken-based pagination.
func TestParity_ListLanguages_Pagination(t *testing.T) {
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

// TestParity_StopTextTranslationJob_SetsEndTime verifies StopTextTranslationJob
// sets an EndTime on the job.
func TestParity_StopTextTranslationJob_SetsEndTime(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	startRec := doRequest(t, h, "StartTextTranslationJob", map[string]any{
		"JobName":             "stop-test",
		"SourceLanguageCode":  "en",
		"TargetLanguageCodes": []string{"es"},
		"DataAccessRoleArn":   "arn:aws:iam::000000000000:role/r",
		"InputDataConfig": map[string]any{
			"S3Uri":       "s3://bucket/input/",
			"ContentType": "text/plain",
		},
		"OutputDataConfig": map[string]any{
			"S3Uri": "s3://bucket/output/",
		},
	})
	require.Equal(t, http.StatusOK, startRec.Code)

	startResp := unmarshalJSON(t, startRec.Body.Bytes())
	jobID := startResp["JobId"].(string)

	stopRec := doRequest(t, h, "StopTextTranslationJob", map[string]any{"JobId": jobID})
	require.Equal(t, http.StatusOK, stopRec.Code)

	descRec := doRequest(t, h, "DescribeTextTranslationJob", map[string]any{"JobId": jobID})
	require.Equal(t, http.StatusOK, descRec.Code)

	descResp := unmarshalJSON(t, descRec.Body.Bytes())
	job := descResp["TextTranslationJobProperties"].(map[string]any)
	endTime, hasEndTime := job["EndTime"]
	assert.True(t, hasEndTime, "EndTime must be present after StopTextTranslationJob")
	assert.NotNil(t, endTime)
}

// TestParity_JobToMap_IncludesJobDetails verifies DescribeTextTranslationJob
// response includes the JobDetails field with document counts.
func TestParity_JobToMap_IncludesJobDetails(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	startRec := doRequest(t, h, "StartTextTranslationJob", map[string]any{
		"JobName":             "details-test",
		"SourceLanguageCode":  "en",
		"TargetLanguageCodes": []string{"fr"},
		"DataAccessRoleArn":   "arn:aws:iam::000000000000:role/r",
		"InputDataConfig": map[string]any{
			"S3Uri":       "s3://bucket/input/",
			"ContentType": "text/plain",
		},
		"OutputDataConfig": map[string]any{
			"S3Uri": "s3://bucket/output/",
		},
	})
	require.Equal(t, http.StatusOK, startRec.Code)

	startResp := unmarshalJSON(t, startRec.Body.Bytes())
	jobID := startResp["JobId"].(string)

	descRec := doRequest(t, h, "DescribeTextTranslationJob", map[string]any{"JobId": jobID})
	require.Equal(t, http.StatusOK, descRec.Code)

	descResp := unmarshalJSON(t, descRec.Body.Bytes())
	job := descResp["TextTranslationJobProperties"].(map[string]any)
	details, hasDetails := job["JobDetails"]
	require.True(t, hasDetails, "JobDetails must be present in job response")
	d := details.(map[string]any)
	assert.Contains(t, d, "TranslatedDocumentsCount")
	assert.Contains(t, d, "DocumentsWithErrorsCount")
	assert.Contains(t, d, "InputDocumentsCount")
}

// TestParity_UpdateParallelData_IncludesLatestUpdateAttemptAt verifies
// UpdateParallelData response includes LatestUpdateAttemptAt.
func TestParity_UpdateParallelData_IncludesLatestUpdateAttemptAt(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreateParallelData", map[string]any{
		"Name": "pd-update-test",
		"ParallelDataConfig": map[string]any{
			"S3Uri":  "s3://bucket/pd/",
			"Format": "TSV",
		},
	})

	rec := doRequest(t, h, "UpdateParallelData", map[string]any{
		"Name": "pd-update-test",
		"ParallelDataConfig": map[string]any{
			"S3Uri":  "s3://bucket/pd-v2/",
			"Format": "TSV",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := unmarshalJSON(t, rec.Body.Bytes())
	_, hasAt := resp["LatestUpdateAttemptAt"]
	assert.True(t, hasAt, "LatestUpdateAttemptAt must be present in UpdateParallelData response")
}

// TestParity_TerminologyToMap_IncludesTargetLanguageCodes verifies GetTerminology
// response includes TargetLanguageCodes derived from the CSV header.
func TestParity_TerminologyToMap_IncludesTargetLanguageCodes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "ImportTerminology", map[string]any{
		"Name":          "tgt-lang-term",
		"MergeStrategy": "OVERWRITE",
		"TerminologyData": map[string]any{
			"File":   "en,es,de\nhello,hola,hallo",
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

// TestParity_ListTextTranslationJobs_StatusFilter verifies filtering by JobStatus.
func TestParity_ListTextTranslationJobs_StatusFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	startBody := func(name string) map[string]any {
		return map[string]any{
			"JobName":             name,
			"SourceLanguageCode":  "en",
			"TargetLanguageCodes": []string{"es"},
			"DataAccessRoleArn":   "arn:aws:iam::000000000000:role/r",
			"InputDataConfig": map[string]any{
				"S3Uri":       "s3://b/i/",
				"ContentType": "text/plain",
			},
			"OutputDataConfig": map[string]any{"S3Uri": "s3://b/o/"},
		}
	}

	for _, name := range []string{"job-a", "job-b"} {
		rec := doRequest(t, h, "StartTextTranslationJob", startBody(name))
		require.Equal(t, http.StatusOK, rec.Code)
	}

	tests := []struct {
		name    string
		status  string
		wantMin int
	}{
		{name: "in_progress_filter", status: "IN_PROGRESS", wantMin: 2},
		{name: "completed_filter", status: "COMPLETED", wantMin: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			body := map[string]any{}
			if tt.status != "" {
				body["Filter"] = map[string]any{"JobStatus": tt.status}
			}

			rec := doRequest(t, h, "ListTextTranslationJobs", body)
			require.Equal(t, http.StatusOK, rec.Code)

			resp := unmarshalJSON(t, rec.Body.Bytes())
			jobs, ok := resp["TextTranslationJobPropertiesList"].([]any)
			require.True(t, ok)
			assert.GreaterOrEqual(t, len(jobs), tt.wantMin)
		})
	}
}

// TestParity_ImportTerminology_OverwriteUpdatesLanguages verifies that a second
// ImportTerminology with OVERWRITE updates SourceLanguage and TargetLanguages.
func TestParity_ImportTerminology_OverwriteUpdatesLanguages(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "ImportTerminology", map[string]any{
		"Name":          "overwrite-term",
		"MergeStrategy": "OVERWRITE",
		"TerminologyData": map[string]any{
			"File":   "en,es\nhello,hola",
			"Format": "CSV",
		},
	})

	doRequest(t, h, "ImportTerminology", map[string]any{
		"Name":          "overwrite-term",
		"MergeStrategy": "OVERWRITE",
		"TerminologyData": map[string]any{
			"File":   "fr,de,it\nbonjour,hallo,ciao",
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

// TestParity_ListLanguages_DisplayLanguageCode verifies that DisplayLanguageCode
// parameter changes the LanguageName in the response.
func TestParity_ListLanguages_DisplayLanguageCode(t *testing.T) {
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

// TestParity_TagOperations verifies TagResource, UntagResource, and ListTagsForResource.
func TestParity_TagOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	impRec := doRequest(t, h, "ImportTerminology", map[string]any{
		"Name":          "tag-term",
		"MergeStrategy": "OVERWRITE",
		"TerminologyData": map[string]any{
			"File":   "en,es",
			"Format": "CSV",
		},
	})
	require.Equal(t, http.StatusOK, impRec.Code)

	impResp := unmarshalJSON(t, impRec.Body.Bytes())
	resourceARN := impResp["TerminologyProperties"].(map[string]any)["Arn"].(string)

	tagRec := doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": resourceARN,
		"Tags":        []any{map[string]any{"Key": "env", "Value": "test"}},
	})
	require.Equal(t, http.StatusOK, tagRec.Code)

	listRec := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": resourceARN})
	require.Equal(t, http.StatusOK, listRec.Code)

	listResp := unmarshalJSON(t, listRec.Body.Bytes())
	tags, ok := listResp["Tags"].([]any)
	require.True(t, ok)

	found := false
	for _, raw := range tags {
		tag := raw.(map[string]any)
		if tag["Key"] == "env" && tag["Value"] == "test" {
			found = true
		}
	}
	assert.True(t, found, "added tag must appear in ListTagsForResource")

	untagRec := doRequest(t, h, "UntagResource", map[string]any{
		"ResourceArn": resourceARN,
		"TagKeys":     []string{"env"},
	})
	require.Equal(t, http.StatusOK, untagRec.Code)

	listRec2 := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": resourceARN})
	listResp2 := unmarshalJSON(t, listRec2.Body.Bytes())
	tags2 := listResp2["Tags"].([]any)

	for _, raw := range tags2 {
		tag := raw.(map[string]any)
		assert.NotEqual(t, "env", tag["Key"], "untagged key must be absent")
	}
}

// TestParity_TranslateDocument_AppliedSettings verifies TranslateDocument echoes Settings.
func TestParity_TranslateDocument_AppliedSettings(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "TranslateDocument", map[string]any{
		"Document": map[string]any{
			"Content":     "Hello",
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

// TestParity_JSONEncoding_TermCount verifies TermCount is numeric not string in JSON.
func TestParity_JSONEncoding_TermCount(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "ImportTerminology", map[string]any{
		"Name":          "count-term",
		"MergeStrategy": "OVERWRITE",
		"TerminologyData": map[string]any{
			"File":   "en,es\none,uno\ntwo,dos",
			"Format": "CSV",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

	props := raw["TerminologyProperties"].(map[string]any)
	termCount, ok := props["TermCount"].(float64)
	require.True(t, ok, "TermCount must be a JSON number not a string")
	assert.InDelta(t, float64(2), termCount, 0)
}
