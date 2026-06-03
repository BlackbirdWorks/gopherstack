package translate_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/translate"
)

// ---------------------------------------------------------------------------
// StopTextTranslationJob state guard
// ---------------------------------------------------------------------------

// TestBatch2_StopTextTranslationJob_StateGuard verifies that StopTextTranslationJob
// rejects jobs that are not in a stoppable state. AWS returns InvalidRequestException
// for stop attempts on STOP_REQUESTED jobs.
func TestBatch2_StopTextTranslationJob_StateGuard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *translate.Handler, jobID string)
		name     string
		wantType string
		wantCode int
	}{
		{
			name:     "stop IN_PROGRESS job succeeds",
			setup:    func(_ *testing.T, _ *translate.Handler, _ string) {},
			wantCode: http.StatusOK,
		},
		{
			name: "stop STOP_REQUESTED job returns InvalidRequestException",
			setup: func(t *testing.T, h *translate.Handler, jobID string) {
				t.Helper()
				rec := doRequest(t, h, "StopTextTranslationJob", map[string]any{"JobId": jobID})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantCode: http.StatusBadRequest,
			wantType: "InvalidRequestException",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			jobID := createTestJob(t, h)
			tc.setup(t, h, jobID)

			rec := doRequest(t, h, "StopTextTranslationJob", map[string]any{"JobId": jobID})
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.wantType != "" {
				var body map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
				assert.Equal(t, tc.wantType, body["__type"])
			}
		})
	}
}

// TestBatch2_StopTextTranslationJob_NotFound verifies that stopping a nonexistent
// job returns ResourceNotFoundException.
func TestBatch2_StopTextTranslationJob_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "StopTextTranslationJob", map[string]any{"JobId": "nonexistent-job-id"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "ResourceNotFoundException", body["__type"])
}

// ---------------------------------------------------------------------------
// DescribeTextTranslationJob: not-found and field accuracy
// ---------------------------------------------------------------------------

// TestBatch2_DescribeTextTranslationJob_NotFound verifies that describing a
// nonexistent job returns ResourceNotFoundException.
func TestBatch2_DescribeTextTranslationJob_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "DescribeTextTranslationJob", map[string]any{"JobId": "no-such-job"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "ResourceNotFoundException", body["__type"])
}

// TestBatch2_DescribeTextTranslationJob_TerminologyAndParallelDataFields verifies
// that TerminologyNames and ParallelDataNames are preserved and returned in job details.
func TestBatch2_DescribeTextTranslationJob_TerminologyAndParallelDataFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "StartTextTranslationJob", map[string]any{
		"JobName":             "fields-test",
		"SourceLanguageCode":  "en",
		"TargetLanguageCodes": []string{"fr"},
		"DataAccessRoleArn":   "arn:aws:iam::000000000000:role/TranslateRole",
		"InputDataConfig":     map[string]any{"S3Uri": "s3://b/i/", "ContentType": "text/plain"},
		"OutputDataConfig":    map[string]any{"S3Uri": "s3://b/o/"},
		"TerminologyNames":    []string{"my-term"},
		"ParallelDataNames":   []string{"my-pd"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	jobID := unmarshalJSON(t, rec.Body.Bytes())["JobId"].(string)

	rec = doRequest(t, h, "DescribeTextTranslationJob", map[string]any{"JobId": jobID})
	require.Equal(t, http.StatusOK, rec.Code)

	props := unmarshalJSON(t, rec.Body.Bytes())["TextTranslationJobProperties"].(map[string]any)
	termNames, _ := props["TerminologyNames"].([]any)
	pdNames, _ := props["ParallelDataNames"].([]any)
	assert.Equal(t, []any{"my-term"}, termNames)
	assert.Equal(t, []any{"my-pd"}, pdNames)
}

// ---------------------------------------------------------------------------
// ListTextTranslationJobs: status filter accuracy
// ---------------------------------------------------------------------------

// TestBatch2_ListTextTranslationJobs_StatusFilter verifies that filtering by
// JobStatus returns only jobs with the matching status.
func TestBatch2_ListTextTranslationJobs_StatusFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 3 {
		rec := doRequest(t, h, "StartTextTranslationJob", map[string]any{
			"JobName":             "filter-job-" + string(rune('a'+i)),
			"SourceLanguageCode":  "en",
			"TargetLanguageCodes": []string{"fr"},
			"DataAccessRoleArn":   "arn:aws:iam::000000000000:role/TranslateRole",
			"InputDataConfig":     map[string]any{"S3Uri": "s3://b/i/", "ContentType": "text/plain"},
			"OutputDataConfig":    map[string]any{"S3Uri": "s3://b/o/"},
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doRequest(t, h, "ListTextTranslationJobs", map[string]any{
		"Filter": map[string]any{"JobStatus": "IN_PROGRESS"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	m := unmarshalJSON(t, rec.Body.Bytes())
	jobs, _ := m["TextTranslationJobPropertiesList"].([]any)
	assert.Len(t, jobs, 3)

	rec = doRequest(t, h, "ListTextTranslationJobs", map[string]any{
		"Filter": map[string]any{"JobStatus": "COMPLETED"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	m = unmarshalJSON(t, rec.Body.Bytes())
	jobs, _ = m["TextTranslationJobPropertiesList"].([]any)
	assert.Empty(t, jobs)
}

// ---------------------------------------------------------------------------
// Pagination: NextToken flow
// ---------------------------------------------------------------------------

// TestBatch2_ListTerminologies_Pagination verifies that MaxResults and NextToken
// paginate correctly through all terminologies.
func TestBatch2_ListTerminologies_Pagination(t *testing.T) {
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

// TestBatch2_ListParallelData_Pagination verifies that MaxResults and NextToken
// paginate correctly through all parallel data resources.
func TestBatch2_ListParallelData_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 4 {
		rec := doRequest(t, h, "CreateParallelData", map[string]any{
			"Name":               "pd-" + string(rune('a'+i)),
			"ParallelDataConfig": map[string]any{"S3Uri": "s3://b/f.tmx", "Format": "TMX"},
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doRequest(t, h, "ListParallelData", map[string]any{"MaxResults": 2})
	require.Equal(t, http.StatusOK, rec.Code)

	m := unmarshalJSON(t, rec.Body.Bytes())
	page1, _ := m["ParallelDataPropertiesList"].([]any)
	assert.Len(t, page1, 2)
	nextToken, _ := m["NextToken"].(string)
	assert.NotEmpty(t, nextToken)

	rec = doRequest(t, h, "ListParallelData", map[string]any{"MaxResults": 10, "NextToken": nextToken})
	require.Equal(t, http.StatusOK, rec.Code)

	m = unmarshalJSON(t, rec.Body.Bytes())
	page2, _ := m["ParallelDataPropertiesList"].([]any)
	assert.Len(t, page2, 2)
	assert.Nil(t, m["NextToken"])
}

// ---------------------------------------------------------------------------
// TagResource / UntagResource: nonexistent ARN
// ---------------------------------------------------------------------------

// TestBatch2_TagResource_NotFound verifies that tagging a nonexistent resource ARN
// returns ResourceNotFoundException.
func TestBatch2_TagResource_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": "arn:aws:translate:us-east-1:000000000000:terminology/nonexistent",
		"Tags":        []map[string]any{{"Key": "k", "Value": "v"}},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "ResourceNotFoundException", body["__type"])
}

// TestBatch2_UntagResource_NotFound verifies that untagging a nonexistent resource ARN
// returns ResourceNotFoundException.
func TestBatch2_UntagResource_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "UntagResource", map[string]any{
		"ResourceArn": "arn:aws:translate:us-east-1:000000000000:terminology/nonexistent",
		"TagKeys":     []string{"k"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "ResourceNotFoundException", body["__type"])
}

// TestBatch2_ListTagsForResource_NotFound verifies that listing tags for a nonexistent
// resource ARN returns ResourceNotFoundException.
func TestBatch2_ListTagsForResource_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "ListTagsForResource", map[string]any{
		"ResourceArn": "arn:aws:translate:us-east-1:000000000000:terminology/nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "ResourceNotFoundException", body["__type"])
}

// TestBatch2_TagResource_DeletedTerminologyARN verifies that tagging fails after
// the terminology has been deleted (ARN no longer valid).
func TestBatch2_TagResource_DeletedTerminologyARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "ImportTerminology", map[string]any{
		"Name": "to-delete-tag", "MergeStrategy": "OVERWRITE",
		"TerminologyData": map[string]any{"Format": "CSV"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	termARN := unmarshalJSON(t, rec.Body.Bytes())["TerminologyProperties"].(map[string]any)["Arn"].(string)

	rec = doRequest(t, h, "DeleteTerminology", map[string]any{"Name": "to-delete-tag"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": termARN,
		"Tags":        []map[string]any{{"Key": "k", "Value": "v"}},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// GetTerminology: response field accuracy
// ---------------------------------------------------------------------------

// TestBatch2_GetTerminology_DataLocationField verifies that GetTerminology returns
// a TerminologyDataLocation with RepositoryType and Location fields, matching
// AWS behavior.
func TestBatch2_GetTerminology_DataLocationField(t *testing.T) {
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

// ---------------------------------------------------------------------------
// GetParallelData: response field accuracy
// ---------------------------------------------------------------------------

// TestBatch2_GetParallelData_DataLocationField verifies that GetParallelData returns
// a DataLocation with RepositoryType and Location fields, matching AWS behavior.
func TestBatch2_GetParallelData_DataLocationField(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateParallelData", map[string]any{
		"Name":               "loc-pd",
		"ParallelDataConfig": map[string]any{"S3Uri": "s3://b/f.tmx", "Format": "TMX"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "GetParallelData", map[string]any{"Name": "loc-pd"})
	require.Equal(t, http.StatusOK, rec.Code)

	m := unmarshalJSON(t, rec.Body.Bytes())
	loc, ok := m["DataLocation"].(map[string]any)
	require.True(t, ok, "DataLocation must be present")
	assert.Equal(t, "S3", loc["RepositoryType"])
	assert.NotEmpty(t, loc["Location"])
}

// ---------------------------------------------------------------------------
// TranslateText: default source language
// ---------------------------------------------------------------------------

// TestBatch2_TranslateText_OmittedSourceLangDefaultsToAuto verifies that omitting
// SourceLanguageCode defaults to "auto" in the response, matching AWS behavior.
func TestBatch2_TranslateText_OmittedSourceLangDefaultsToAuto(t *testing.T) {
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

// ---------------------------------------------------------------------------
// TranslateDocument: default source language
// ---------------------------------------------------------------------------

// TestBatch2_TranslateDocument_OmittedSourceLangDefaultsToAuto verifies that
// omitting SourceLanguageCode defaults to "auto" in the response.
func TestBatch2_TranslateDocument_OmittedSourceLangDefaultsToAuto(t *testing.T) {
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

// ---------------------------------------------------------------------------
// ListLanguages: MaxResults boundary
// ---------------------------------------------------------------------------

// TestBatch2_ListLanguages_MaxResults verifies that MaxResults correctly limits
// the number of languages returned.
func TestBatch2_ListLanguages_MaxResults(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "ListLanguages", map[string]any{"MaxResults": 5})
	require.Equal(t, http.StatusOK, rec.Code)

	m := unmarshalJSON(t, rec.Body.Bytes())
	langs, _ := m["Languages"].([]any)
	assert.Len(t, langs, 5)
}

// ---------------------------------------------------------------------------
// UpdateParallelData: not-found guard
// ---------------------------------------------------------------------------

// TestBatch2_UpdateParallelData_NotFound verifies that updating a nonexistent
// parallel data resource returns ResourceNotFoundException.
func TestBatch2_UpdateParallelData_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "UpdateParallelData", map[string]any{
		"Name":               "nonexistent-pd",
		"ParallelDataConfig": map[string]any{"S3Uri": "s3://b/f.tmx", "Format": "TMX"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "ResourceNotFoundException", body["__type"])
}

// ---------------------------------------------------------------------------
// Terminology ARN accuracy
// ---------------------------------------------------------------------------

// TestBatch2_ImportTerminology_ARNFormat verifies that the ARN returned by
// ImportTerminology is well-formed and includes the terminology name.
func TestBatch2_ImportTerminology_ARNFormat(t *testing.T) {
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

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func createTestJob(t *testing.T, h *translate.Handler) string {
	t.Helper()

	rec := doRequest(t, h, "StartTextTranslationJob", map[string]any{
		"JobName":             "state-guard-job",
		"SourceLanguageCode":  "en",
		"TargetLanguageCodes": []string{"fr"},
		"DataAccessRoleArn":   "arn:aws:iam::000000000000:role/TranslateRole",
		"InputDataConfig":     map[string]any{"S3Uri": "s3://b/i/", "ContentType": "text/plain"},
		"OutputDataConfig":    map[string]any{"S3Uri": "s3://b/o/"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	return unmarshalJSON(t, rec.Body.Bytes())["JobId"].(string)
}
