package translate_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/translate"
)

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

func TestStartTextTranslationJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "StartTextTranslationJob", map[string]any{
		"JobName":             "batch-job-1",
		"SourceLanguageCode":  "en",
		"TargetLanguageCodes": []string{"fr", "de"},
		"DataAccessRoleArn":   "arn:aws:iam::000000000000:role/TranslateRole",
		"InputDataConfig": map[string]any{
			"S3Uri":       "s3://bucket/input/",
			"ContentType": "text/plain",
		},
		"OutputDataConfig": map[string]any{
			"S3Uri": "s3://bucket/output/",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	m := unmarshalJSON(t, rec.Body.Bytes())
	assert.NotEmpty(t, m["JobId"])
	assert.Equal(t, "SUBMITTED", m["JobStatus"])
}

func TestDescribeTextTranslationJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "StartTextTranslationJob", map[string]any{
		"JobName":             "describe-test",
		"SourceLanguageCode":  "en",
		"TargetLanguageCodes": []string{"es"},
		"DataAccessRoleArn":   "arn:aws:iam::000000000000:role/TranslateRole",
		"InputDataConfig":     map[string]any{"S3Uri": "s3://b/i/", "ContentType": "text/plain"},
		"OutputDataConfig":    map[string]any{"S3Uri": "s3://b/o/"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	jobID := unmarshalJSON(t, rec.Body.Bytes())["JobId"].(string)

	rec = doRequest(t, h, "DescribeTextTranslationJob", map[string]any{"JobId": jobID})
	assert.Equal(t, http.StatusOK, rec.Code)

	m := unmarshalJSON(t, rec.Body.Bytes())
	props, ok := m["TextTranslationJobProperties"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, jobID, props["JobId"])
	assert.Equal(t, "describe-test", props["JobName"])
}

func TestStopTextTranslationJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "StartTextTranslationJob", map[string]any{
		"JobName":             "stop-test",
		"SourceLanguageCode":  "en",
		"TargetLanguageCodes": []string{"fr"},
		"DataAccessRoleArn":   "arn:aws:iam::000000000000:role/TranslateRole",
		"InputDataConfig":     map[string]any{"S3Uri": "s3://b/i/", "ContentType": "text/plain"},
		"OutputDataConfig":    map[string]any{"S3Uri": "s3://b/o/"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	jobID := unmarshalJSON(t, rec.Body.Bytes())["JobId"].(string)

	rec = doRequest(t, h, "StopTextTranslationJob", map[string]any{"JobId": jobID})
	assert.Equal(t, http.StatusOK, rec.Code)

	m := unmarshalJSON(t, rec.Body.Bytes())
	assert.Equal(t, "STOP_REQUESTED", m["JobStatus"])
}

func TestListTextTranslationJobs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 3 {
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

	rec := doRequest(t, h, "ListTextTranslationJobs", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	m := unmarshalJSON(t, rec.Body.Bytes())
	jobs, _ := m["TextTranslationJobPropertiesList"].([]any)
	assert.Len(t, jobs, 3)
}

// TestStopTextTranslationJob_StateGuard verifies that StopTextTranslationJob
// rejects jobs that are not in a stoppable state. AWS returns InvalidRequestException
// for stop attempts on STOP_REQUESTED jobs.
func TestStopTextTranslationJob_StateGuard(t *testing.T) {
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

// TestStopTextTranslationJob_NotFound verifies that stopping a nonexistent
// job returns ResourceNotFoundException.
func TestStopTextTranslationJob_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "StopTextTranslationJob", map[string]any{"JobId": "nonexistent-job-id"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "ResourceNotFoundException", body["__type"])
}

// TestDescribeTextTranslationJob_NotFound verifies that describing a
// nonexistent job returns ResourceNotFoundException.
func TestDescribeTextTranslationJob_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "DescribeTextTranslationJob", map[string]any{"JobId": "no-such-job"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "ResourceNotFoundException", body["__type"])
}

// TestDescribeTextTranslationJob_TerminologyAndParallelDataFields verifies
// that TerminologyNames and ParallelDataNames are preserved and returned in job details.
func TestDescribeTextTranslationJob_TerminologyAndParallelDataFields(t *testing.T) {
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

// TestListTextTranslationJobs_StatusFilter verifies that filtering by
// JobStatus returns only jobs with the matching status.
func TestListTextTranslationJobs_StatusFilter(t *testing.T) {
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
		"Filter": map[string]any{"JobStatus": "SUBMITTED"},
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

// TestStopTextTranslationJob_SetsEndTime verifies StopTextTranslationJob
// sets an EndTime on the job.
func TestStopTextTranslationJob_SetsEndTime(t *testing.T) {
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

// TestJobToMap_IncludesJobDetails verifies DescribeTextTranslationJob
// response includes the JobDetails field with document counts.
func TestJobToMap_IncludesJobDetails(t *testing.T) {
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

// TestListTextTranslationJobs_StatusFilterMinCount verifies filtering by
// JobStatus using a threshold check (GreaterOrEqual) rather than an exact
// count, complementing TestListTextTranslationJobs_StatusFilter's exact-count
// assertions with a scenario using fewer seeded jobs and named subtests.
func TestListTextTranslationJobs_StatusFilterMinCount(t *testing.T) {
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
		{name: "submitted_filter", status: "SUBMITTED", wantMin: 2},
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
