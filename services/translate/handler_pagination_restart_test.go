package translate_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListTerminologies_Pagination_DeletedMidPage proves that deleting the
// terminology a cursor names does not restart pagination at page one. Prior
// coverage (TestListTerminologies_Pagination) only exercised the happy path
// where every named cursor still resolves.
func TestListTerminologies_Pagination_DeletedMidPage(t *testing.T) {
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
	nextToken, _ := m["NextToken"].(string)
	require.NotEmpty(t, nextToken)

	rec = doRequest(t, h, "DeleteTerminology", map[string]any{"Name": nextToken})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "ListTerminologies", map[string]any{"MaxResults": 2, "NextToken": nextToken})
	require.Equal(t, http.StatusOK, rec.Code)

	m2 := unmarshalJSON(t, rec.Body.Bytes())
	page2, _ := m2["TerminologyPropertiesList"].([]any)

	for _, item := range page2 {
		entry, _ := item.(map[string]any)
		name, _ := entry["Name"].(string)
		assert.NotEqual(t, nextToken, name, "deleted cursor item must not reappear")
	}

	// The bug under test resumes at the beginning of the whole collection,
	// which would reproduce the terminologies already served on page one.
	firstTwo := map[string]bool{"term-a": true, "term-b": true}
	restarted := false

	for _, item := range page2 {
		entry, _ := item.(map[string]any)
		name, _ := entry["Name"].(string)
		if firstTwo[name] {
			restarted = true
		}
	}

	assert.False(t, restarted, "cursor must not restart pagination at page one after its item is deleted")
}

// TestListParallelData_Pagination_DeletedMidPage proves that deleting the
// parallel-data resource a cursor names does not restart pagination.
func TestListParallelData_Pagination_DeletedMidPage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, name := range []string{"pd-a", "pd-b", "pd-c", "pd-d", "pd-e"} {
		rec := doRequest(t, h, "CreateParallelData", map[string]any{
			"Name":               name,
			"ParallelDataConfig": map[string]any{"S3Uri": "s3://b/f.tmx", "Format": "TMX"},
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doRequest(t, h, "ListParallelData", map[string]any{"MaxResults": 2})
	require.Equal(t, http.StatusOK, rec.Code)

	m := unmarshalJSON(t, rec.Body.Bytes())
	nextToken, _ := m["NextToken"].(string)
	require.NotEmpty(t, nextToken)

	rec = doRequest(t, h, "DeleteParallelData", map[string]any{"Name": nextToken})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "ListParallelData", map[string]any{"MaxResults": 2, "NextToken": nextToken})
	require.Equal(t, http.StatusOK, rec.Code)

	m2 := unmarshalJSON(t, rec.Body.Bytes())
	page2, _ := m2["ParallelDataPropertiesList"].([]any)

	restarted := false

	for _, item := range page2 {
		entry, _ := item.(map[string]any)
		name, _ := entry["Name"].(string)
		if name == "pd-a" || name == "pd-b" {
			restarted = true
		}
	}

	assert.False(t, restarted, "cursor must not restart pagination at page one after its item is deleted")
}

// TestListTextTranslationJobs_Pagination_StaleTokenDoesNotRestart proves that
// an unresolvable NextToken (e.g. from a job filtered out of the current
// view) does not restart pagination at page one. Jobs cannot be deleted in
// this API, so the hostile scenario is a forged/unresolvable token rather
// than deletion.
func TestListTextTranslationJobs_Pagination_StaleTokenDoesNotRestart(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 5 {
		rec := doRequest(t, h, "StartTextTranslationJob", map[string]any{
			"JobName":             "job-" + string(rune('a'+i)),
			"SourceLanguageCode":  "en",
			"TargetLanguageCodes": []string{"fr"},
			"DataAccessRoleArn":   "arn:aws:iam::000000000000:role/TranslateRole",
			"InputDataConfig":     map[string]any{"S3Uri": "s3://b/i/", "ContentType": "text/plain"},
			"OutputDataConfig":    map[string]any{"S3Uri": "s3://b/o/"},
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doRequest(t, h, "ListTextTranslationJobs", map[string]any{"MaxResults": 2})
	require.Equal(t, http.StatusOK, rec.Code)

	m := unmarshalJSON(t, rec.Body.Bytes())
	page1, _ := m["TextTranslationJobPropertiesList"].([]any)
	require.Len(t, page1, 2)

	page1IDs := map[string]bool{}
	for _, item := range page1 {
		entry, _ := item.(map[string]any)
		id, _ := entry["JobId"].(string)
		page1IDs[id] = true
	}

	rec = doRequest(t, h, "ListTextTranslationJobs", map[string]any{
		"MaxResults": 2,
		"NextToken":  "this-job-id-does-not-exist",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	m2 := unmarshalJSON(t, rec.Body.Bytes())
	page2, _ := m2["TextTranslationJobPropertiesList"].([]any)

	for _, item := range page2 {
		entry, _ := item.(map[string]any)
		id, _ := entry["JobId"].(string)
		assert.False(t, page1IDs[id], "an unresolvable NextToken must not restart pagination at page one")
	}
}

// TestListLanguages_Pagination_StaleTokenDoesNotRestart proves that a
// forged/unresolvable NextToken does not restart ListLanguages at page one.
// ListLanguages serves a fixed built-in list that cannot be mutated, so
// deletion between pages isn't possible here -- but a stale or forged token
// reaches the exact same miss-defaults-to-zero code path.
func TestListLanguages_Pagination_StaleTokenDoesNotRestart(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "ListLanguages", map[string]any{"MaxResults": float64(5)})
	require.Equal(t, http.StatusOK, rec.Code)

	m := unmarshalJSON(t, rec.Body.Bytes())
	page1, _ := m["Languages"].([]any)
	require.Len(t, page1, 5)

	page1Codes := map[string]bool{}
	for _, l := range page1 {
		lang, _ := l.(map[string]any)
		page1Codes[lang["LanguageCode"].(string)] = true
	}

	rec = doRequest(t, h, "ListLanguages", map[string]any{
		"MaxResults": float64(5),
		"NextToken":  "zz-not-a-real-code",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	m2 := unmarshalJSON(t, rec.Body.Bytes())
	page2, _ := m2["Languages"].([]any)

	for _, l := range page2 {
		lang, _ := l.(map[string]any)
		code, _ := lang["LanguageCode"].(string)
		assert.False(t, page1Codes[code], "a forged NextToken must not restart pagination at page one")
	}
}
