package transcribe_test

import (
	"encoding/json"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	transcribesdk "github.com/aws/aws-sdk-go-v2/service/transcribe"
	sdktypes "github.com/aws/aws-sdk-go-v2/service/transcribe/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/transcribe"
)

func TestRealism_RequestValidation(t *testing.T) {
	t.Parallel()

	media := map[string]any{"MediaFileUri": "s3://bkt/k.mp4"}
	roleInput := map[string]any{"S3Uri": "s3://bkt/t/", "DataAccessRoleArn": "arn:aws:iam::123456789012:role/r"}
	rule := []any{map[string]any{"InterruptionFilter": map[string]any{"Threshold": 1}}}

	tests := []struct {
		body     map[string]any
		name     string
		action   string
		wantType string
		wantMsg  string
	}{
		{
			name:     "vocabulary_name_pattern",
			action:   "CreateVocabulary",
			wantType: "BadRequestException",
			body: map[string]any{
				"VocabularyName": "bad name!",
				"LanguageCode":   "en-US",
				"Phrases":        []string{"hello"},
			},
			wantMsg: "VocabularyName must match",
		},
		{
			name:     "vocabulary_filter_name_pattern",
			action:   "CreateVocabularyFilter",
			wantType: "BadRequestException",
			body: map[string]any{
				"VocabularyFilterName": "bad name!",
				"LanguageCode":         "en-US",
				"Words":                []string{"x"},
			},
			wantMsg: "VocabularyFilterName must match",
		},
		{
			name: "language_model_name_pattern", action: "CreateLanguageModel", wantType: "BadRequestException",
			body: map[string]any{
				"ModelName": "bad name!", "BaseModelName": "WideBand", "LanguageCode": "en-US",
				"InputDataConfig": roleInput,
			},
			wantMsg: "ModelName must match",
		},
		{
			name: "medical_vocabulary_name_pattern", action: "CreateMedicalVocabulary", wantType: "BadRequestException",
			body: map[string]any{
				"VocabularyName": "bad name!", "LanguageCode": "en-US", "VocabularyFileUri": "s3://bkt/v.txt",
			},
			wantMsg: "VocabularyName must match",
		},
		{
			name: "category_name_pattern", action: "CreateCallAnalyticsCategory", wantType: "BadRequestException",
			body:    map[string]any{"CategoryName": "bad name!", "Rules": rule},
			wantMsg: "CategoryName must match",
		},
		{
			name: "call_analytics_job_name_pattern", action: "StartCallAnalyticsJob", wantType: "BadRequestException",
			body:    map[string]any{"CallAnalyticsJobName": "bad name!", "Media": media},
			wantMsg: "CallAnalyticsJobName must match",
		},
		{
			name: "list_jobs_status_enum", action: "ListTranscriptionJobs", wantType: "BadRequestException",
			body:    map[string]any{"Status": "FOO"},
			wantMsg: `Status "FOO" must be one of`,
		},
		{
			name: "list_vocabularies_state_enum", action: "ListVocabularies", wantType: "BadRequestException",
			body:    map[string]any{"StateEquals": "FOO"},
			wantMsg: `StateEquals "FOO" must be one of`,
		},
		{
			name: "list_models_status_enum", action: "ListLanguageModels", wantType: "BadRequestException",
			body:    map[string]any{"StatusEquals": "FOO"},
			wantMsg: `StatusEquals "FOO" must be one of`,
		},
		{
			name: "list_max_results_above_bound", action: "ListTranscriptionJobs", wantType: "BadRequestException",
			body:    map[string]any{"MaxResults": 101},
			wantMsg: "MaxResults must be between 1 and 100",
		},
		{
			name: "list_next_token_garbage", action: "ListVocabularyFilters", wantType: "BadRequestException",
			body:    map[string]any{"NextToken": "garbage"},
			wantMsg: "NextToken is invalid",
		},
		{
			name: "not_found_has_no_code_prefix", action: "GetTranscriptionJob", wantType: "NotFoundException",
			body:    map[string]any{"TranscriptionJobName": "nope"},
			wantMsg: "job nope not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doTranscribeRequest(t, newTestTranscribeHandler(t), tt.action, tt.body)

			var resp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tt.wantType, resp["__type"])
			assert.Contains(t, resp["message"], tt.wantMsg)
			assert.NotContains(t, resp["message"], "Exception: ")
		})
	}
}

func TestRealism_LifecycleDelay(t *testing.T) {
	t.Parallel()

	const delay = 30 * time.Second

	tests := []struct {
		run  func(t *testing.T, b *transcribe.InMemoryBackend) (status func() string)
		name string
		want [2]string
	}{
		{
			name: "transcription_job",
			want: [2]string{"IN_PROGRESS", "COMPLETED"},
			run: func(t *testing.T, b *transcribe.InMemoryBackend) func() string {
				t.Helper()

				_, err := b.StartTranscriptionJob(&transcribe.TranscriptionJob{
					JobName: "j", LanguageCode: "en-US",
					Media: transcribe.Media{MediaFileURI: "s3://bkt/k.mp4"},
				})
				require.NoError(t, err)

				return func() string {
					j, getErr := b.GetTranscriptionJob("j")
					require.NoError(t, getErr)

					return j.JobStatus
				}
			},
		},
		{
			name: "vocabulary",
			want: [2]string{"PENDING", "READY"},
			run: func(t *testing.T, b *transcribe.InMemoryBackend) func() string {
				t.Helper()

				_, err := b.CreateVocabulary(&transcribe.Vocabulary{
					VocabularyName: "v", LanguageCode: "en-US", Phrases: []string{"hello"},
				})
				require.NoError(t, err)

				return func() string {
					v, getErr := b.GetVocabulary("v")
					require.NoError(t, getErr)

					return v.VocabularyState
				}
			},
		},
		{
			name: "vocabulary_list_filter",
			want: [2]string{"PENDING", "READY"},
			run: func(t *testing.T, b *transcribe.InMemoryBackend) func() string {
				t.Helper()

				_, err := b.CreateVocabulary(&transcribe.Vocabulary{
					VocabularyName: "v", LanguageCode: "en-US", Phrases: []string{"hello"},
				})
				require.NoError(t, err)

				return func() string {
					for _, state := range []string{"PENDING", "READY"} {
						if got, _ := b.ListVocabularies(state, "", "", 0); len(got) == 1 {
							return state
						}
					}

					return ""
				}
			},
		},
		{
			name: "language_model",
			want: [2]string{"IN_PROGRESS", "COMPLETED"},
			run: func(t *testing.T, b *transcribe.InMemoryBackend) func() string {
				t.Helper()

				_, err := b.CreateLanguageModel(&transcribe.LanguageModel{
					ModelName: "m", BaseModelName: "WideBand", LanguageCode: "en-US",
					InputDataConfig: &transcribe.InputDataConfig{
						S3Uri: "s3://bkt/t/", DataAccessRoleArn: "arn:aws:iam::123456789012:role/r",
					},
				})
				require.NoError(t, err)

				return func() string {
					m, getErr := b.DescribeLanguageModel("m")
					require.NoError(t, getErr)

					return m.ModelStatus
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := transcribe.NewInMemoryBackend()
				b.SetLifecycleDelay(delay)
				status := tt.run(t, b)

				assert.Equal(t, tt.want[0], status())
				time.Sleep(delay - time.Second)
				assert.Equal(t, tt.want[0], status())
				time.Sleep(time.Second)
				assert.Equal(t, tt.want[1], status())
			})
		})
	}
}

func TestRealism_TranscriptHiddenWhileInProgress(t *testing.T) {
	t.Parallel()

	b := transcribe.NewInMemoryBackend()
	b.SetLifecycleDelay(time.Hour)
	client := newTranscribeSDKClient(t, transcribe.NewHandler(b))

	out, err := client.StartTranscriptionJob(t.Context(), &transcribesdk.StartTranscriptionJobInput{
		TranscriptionJobName: aws.String("j"),
		LanguageCode:         sdktypes.LanguageCodeEnUs,
		Media:                &sdktypes.Media{MediaFileUri: aws.String("s3://bkt/k.mp4")},
	})
	require.NoError(t, err)
	assert.Equal(t, sdktypes.TranscriptionJobStatusInProgress, out.TranscriptionJob.TranscriptionJobStatus)
	assert.Nil(t, out.TranscriptionJob.Transcript.TranscriptFileUri)
	assert.Nil(t, out.TranscriptionJob.CompletionTime)

	list, err := client.ListTranscriptionJobs(t.Context(), &transcribesdk.ListTranscriptionJobsInput{
		Status: sdktypes.TranscriptionJobStatusCompleted,
	})
	require.NoError(t, err)
	assert.Empty(t, list.TranscriptionJobSummaries)
}
