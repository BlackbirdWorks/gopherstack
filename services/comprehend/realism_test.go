package comprehend_test

import (
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/comprehend"
)

func realismJob(name string) map[string]any {
	return map[string]any{
		"JobName":           name,
		"LanguageCode":      "en",
		"DataAccessRoleArn": "arn:aws:iam::123456789012:role/role",
		"InputDataConfig":   map[string]any{"S3Uri": "s3://bucket/input"},
		"OutputDataConfig":  map[string]any{"S3Uri": "s3://bucket/output"},
	}
}

func TestRealism_RequestValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		action   string
		wantType string
		wantMsg  string
	}{
		{
			name: "job_name_pattern", action: "StartEntitiesDetectionJob", wantType: "InvalidRequestException",
			body: realismJob("bad!name"), wantMsg: "JobName must match",
		},
		{
			name: "list_max_results_zero", action: "ListEntitiesDetectionJobs", wantType: "InvalidRequestException",
			body: map[string]any{"MaxResults": 0}, wantMsg: "MaxResults must be between 1 and 500",
		},
		{
			name: "list_max_results_over", action: "ListDocumentClassifiers", wantType: "InvalidRequestException",
			body: map[string]any{"MaxResults": 501}, wantMsg: "MaxResults must be between 1 and 500",
		},
		{
			name: "list_garbage_token", action: "ListEntitiesDetectionJobs", wantType: "InvalidRequestException",
			body: map[string]any{"NextToken": "garbage"}, wantMsg: "NextToken is invalid",
		},
		{
			name: "job_not_found_has_no_code_prefix", action: "DescribeEntitiesDetectionJob",
			wantType: "JobNotFoundException",
			body:     map[string]any{"JobId": "nope"}, wantMsg: `job "nope"`,
		},
		{
			name: "unsupported_language_has_no_code_prefix", action: "DetectSentiment",
			wantType: "UnsupportedLanguageException",
			body:     map[string]any{"Text": "hi", "LanguageCode": "xx"}, wantMsg: `language "xx"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := rawRequest(t, newHandler(), tt.action, toJSON(t, tt.body))
			require.Equal(t, http.StatusBadRequest, rec.Code)

			m := decodeBody(t, rec)
			assert.Equal(t, tt.wantType, m["__type"])
			assert.Contains(t, m["message"], tt.wantMsg)
			assert.NotContains(t, m["message"], "Exception: ")
		})
	}
}

func TestRealism_JobFailureMarkerStillAccepted(t *testing.T) {
	t.Parallel()

	rec := rawRequest(t, newHandler(), "StartEntitiesDetectionJob", toJSON(t, realismJob("job [fail]")))
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestRealism_LifecycleDelay(t *testing.T) {
	t.Parallel()

	const delay = 15 * time.Second

	tests := []struct {
		observe func(t *testing.T, h *comprehend.Handler, id string) string
		start   func(t *testing.T, h *comprehend.Handler) string
		name    string
		want    []string
	}{
		{
			name: "job_via_describe",
			start: func(t *testing.T, h *comprehend.Handler) string {
				t.Helper()

				return request(t, h, "StartEntitiesDetectionJob", realismJob("j"))["JobId"].(string)
			},
			observe: func(t *testing.T, h *comprehend.Handler, id string) string {
				t.Helper()

				out := request(t, h, "DescribeEntitiesDetectionJob", map[string]any{"JobId": id})

				return out["EntitiesDetectionJobProperties"].(map[string]any)["JobStatus"].(string)
			},
			want: []string{"SUBMITTED", "IN_PROGRESS", "COMPLETED"},
		},
		{
			name: "job_via_list_only",
			start: func(t *testing.T, h *comprehend.Handler) string {
				t.Helper()

				return request(t, h, "StartEntitiesDetectionJob", realismJob("j"))["JobId"].(string)
			},
			observe: func(t *testing.T, h *comprehend.Handler, _ string) string {
				t.Helper()

				out := request(t, h, "ListEntitiesDetectionJobs", map[string]any{})
				list := out["EntitiesDetectionJobPropertiesList"].([]any)
				require.Len(t, list, 1)

				return list[0].(map[string]any)["JobStatus"].(string)
			},
			want: []string{"SUBMITTED", "IN_PROGRESS", "COMPLETED"},
		},
		{
			name: "classifier_training",
			start: func(t *testing.T, h *comprehend.Handler) string {
				t.Helper()

				return request(t, h, "CreateDocumentClassifier", map[string]any{
					"DocumentClassifierName": "clf", "LanguageCode": "en",
				})["DocumentClassifierArn"].(string)
			},
			observe: func(t *testing.T, h *comprehend.Handler, arn string) string {
				t.Helper()

				out := request(t, h, "DescribeDocumentClassifier", map[string]any{"DocumentClassifierArn": arn})

				return out["DocumentClassifierProperties"].(map[string]any)["Status"].(string)
			},
			want: []string{"SUBMITTED", "TRAINING", "TRAINED"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := comprehend.NewInMemoryBackend("123456789012", "us-east-1")
				b.SetLifecycleDelay(delay)
				h := comprehend.NewHandler(b)
				id := tt.start(t, h)

				for i, want := range tt.want {
					if i > 0 {
						time.Sleep(delay)
					}

					assert.Equal(t, want, tt.observe(t, h, id), "stage %d", i)
				}
			})
		})
	}
}
