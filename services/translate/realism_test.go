package translate_test

import (
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/translate"
)

func realismJobBody(mutate func(m map[string]any)) map[string]any {
	m := map[string]any{
		"JobName":             "realism-job",
		"SourceLanguageCode":  "en",
		"TargetLanguageCodes": []string{"es"},
		"DataAccessRoleArn":   "arn:aws:iam::000000000000:role/TranslateRole",
		"InputDataConfig":     map[string]any{"S3Uri": "s3://bkt/i/", "ContentType": "text/plain"},
		"OutputDataConfig":    map[string]any{"S3Uri": "s3://bkt/o/"},
	}
	if mutate != nil {
		mutate(m)
	}

	return m
}

func TestRealism_RequestValidation(t *testing.T) {
	t.Parallel()

	csv := "ZW4sZXMKaGVsbG8saG9sYQo="

	tests := []struct {
		body     map[string]any
		name     string
		action   string
		wantType string
		wantMsg  string
	}{
		{
			name: "terminology_name_pattern", action: "ImportTerminology", wantType: "InvalidParameterValueException",
			body: map[string]any{
				"Name": "bad name!", "MergeStrategy": "OVERWRITE",
				"TerminologyData": map[string]any{"File": csv, "Format": "CSV"},
			},
			wantMsg: "Name must match",
		},
		{
			name: "parallel_data_name_pattern", action: "CreateParallelData", wantType: "InvalidRequestException",
			body: map[string]any{
				"Name": "bad name!", "ParallelDataConfig": map[string]any{"S3Uri": "s3://bkt/pd.csv", "Format": "CSV"},
			},
			wantMsg: "Name must match",
		},
		{
			name: "parallel_data_s3_uri", action: "CreateParallelData", wantType: "InvalidRequestException",
			body: map[string]any{
				"Name": "pd", "ParallelDataConfig": map[string]any{"S3Uri": "notauri", "Format": "CSV"},
			},
			wantMsg: "ParallelDataConfig.S3Uri must be a valid s3:// URI",
		},
		{
			name: "job_name_pattern", action: "StartTextTranslationJob", wantType: "InvalidRequestException",
			body:    realismJobBody(func(m map[string]any) { m["JobName"] = "bad!name" }),
			wantMsg: "JobName contains invalid characters",
		},
		{
			name: "job_input_s3_uri", action: "StartTextTranslationJob", wantType: "InvalidRequestException",
			body: realismJobBody(func(m map[string]any) {
				m["InputDataConfig"] = map[string]any{"S3Uri": "notauri", "ContentType": "text/plain"}
			}),
			wantMsg: "InputDataConfig.S3Uri must be a valid s3:// URI",
		},
		{
			name: "job_output_s3_uri", action: "StartTextTranslationJob", wantType: "InvalidRequestException",
			body: realismJobBody(func(m map[string]any) {
				m["OutputDataConfig"] = map[string]any{"S3Uri": "http://bkt/o/"}
			}),
			wantMsg: "OutputDataConfig.S3Uri must be a valid s3:// URI",
		},
		{
			name: "job_content_type", action: "StartTextTranslationJob", wantType: "InvalidRequestException",
			body: realismJobBody(func(m map[string]any) {
				m["InputDataConfig"] = map[string]any{"S3Uri": "s3://bkt/i/", "ContentType": "application/foo"}
			}),
			wantMsg: "ContentType",
		},
		{
			name:     "list_terminologies_max_results",
			action:   "ListTerminologies",
			wantType: "InvalidParameterValueException",
			body:     map[string]any{"MaxResults": 501},
			wantMsg:  "MaxResults must be between 1 and 500",
		},
		{
			name: "list_languages_max_results", action: "ListLanguages", wantType: "InvalidParameterValueException",
			body: map[string]any{"MaxResults": 0}, wantMsg: "MaxResults must be between 1 and 500",
		},
		{
			name:     "list_jobs_max_results",
			action:   "ListTextTranslationJobs",
			wantType: "InvalidParameterValueException",
			body:     map[string]any{"MaxResults": -1},
			wantMsg:  "MaxResults must be between 1 and 500",
		},
		{
			name:     "list_parallel_data_max_results",
			action:   "ListParallelData",
			wantType: "InvalidParameterValueException",
			body:     map[string]any{"MaxResults": 1000},
			wantMsg:  "MaxResults must be between 1 and 500",
		},
		{
			name: "not_found_has_no_code_prefix", action: "GetTerminology", wantType: "ResourceNotFoundException",
			body: map[string]any{"Name": "nope"}, wantMsg: `terminology "nope" not found`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, newTestHandler(t), tt.action, tt.body)
			require.Equal(t, http.StatusBadRequest, rec.Code)

			m := unmarshalJSON(t, rec.Body.Bytes())
			assert.Equal(t, tt.wantType, m["__type"])
			assert.Contains(t, m["message"], tt.wantMsg)
			assert.NotContains(t, m["message"], "Exception: ")
		})
	}
}

func TestRealism_LifecycleDelay(t *testing.T) {
	t.Parallel()

	const delay = 10 * time.Second

	tests := []struct {
		observe func(t *testing.T, h *translate.Handler, id string) string
		start   func(t *testing.T, h *translate.Handler) string
		name    string
		want    []string
		sleeps  []time.Duration
	}{
		{
			name: "job_via_describe",
			start: func(t *testing.T, h *translate.Handler) string {
				t.Helper()

				rec := doRequest(t, h, "StartTextTranslationJob", realismJobBody(nil))

				return unmarshalJSON(t, rec.Body.Bytes())["JobId"].(string)
			},
			observe: func(t *testing.T, h *translate.Handler, id string) string {
				t.Helper()

				props := unmarshalJSON(t, doRequest(t, h, "DescribeTextTranslationJob",
					map[string]any{"JobId": id}).Body.Bytes())["TextTranslationJobProperties"].(map[string]any)

				return props["JobStatus"].(string)
			},
			sleeps: []time.Duration{0, delay, delay},
			want:   []string{"SUBMITTED", "IN_PROGRESS", "COMPLETED"},
		},
		{
			name: "job_via_list_without_describe",
			start: func(t *testing.T, h *translate.Handler) string {
				t.Helper()

				rec := doRequest(t, h, "StartTextTranslationJob", realismJobBody(nil))

				return unmarshalJSON(t, rec.Body.Bytes())["JobId"].(string)
			},
			observe: func(t *testing.T, h *translate.Handler, _ string) string {
				t.Helper()

				list := unmarshalJSON(t, doRequest(t, h, "ListTextTranslationJobs",
					map[string]any{}).Body.Bytes())["TextTranslationJobPropertiesList"].([]any)
				require.Len(t, list, 1)

				return list[0].(map[string]any)["JobStatus"].(string)
			},
			sleeps: []time.Duration{0, delay, delay},
			want:   []string{"SUBMITTED", "IN_PROGRESS", "COMPLETED"},
		},
		{
			name: "parallel_data",
			start: func(t *testing.T, h *translate.Handler) string {
				t.Helper()

				rec := doRequest(t, h, "CreateParallelData", map[string]any{
					"Name":               "pd",
					"ParallelDataConfig": map[string]any{"S3Uri": "s3://bkt/pd.csv", "Format": "CSV"},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				return "pd"
			},
			observe: func(t *testing.T, h *translate.Handler, _ string) string {
				t.Helper()

				list := unmarshalJSON(t, doRequest(t, h, "ListParallelData",
					map[string]any{}).Body.Bytes())["ParallelDataPropertiesList"].([]any)
				require.Len(t, list, 1)

				return list[0].(map[string]any)["Status"].(string)
			},
			sleeps: []time.Duration{0, delay},
			want:   []string{"CREATING", "ACTIVE"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := translate.NewInMemoryBackend("000000000000", "us-east-1")
				b.SetLifecycleDelay(delay)
				h := translate.NewHandler(b)
				id := tt.start(t, h)

				for i, want := range tt.want {
					time.Sleep(tt.sleeps[i])
					assert.Equal(t, want, tt.observe(t, h, id), "stage %d", i)
				}
			})
		})
	}
}
