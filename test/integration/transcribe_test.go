//go:build integration
// +build integration

package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	transcribesdk "github.com/aws/aws-sdk-go-v2/service/transcribe"
	transcribetypes "github.com/aws/aws-sdk-go-v2/service/transcribe/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createTranscribeClient returns a Transcribe client pointed at the shared test container.
func createTranscribeClient(t *testing.T) *transcribesdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return transcribesdk.NewFromConfig(cfg, func(o *transcribesdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_Transcribe_TranscriptionJobLifecycle tests start, get, list, and delete
// of a transcription job.
func TestIntegration_Transcribe_TranscriptionJobLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name     string
		jobName  string
		mediaURI string
	}{
		{
			name:     "full_lifecycle",
			jobName:  "integ-job-" + uuid.NewString()[:8],
			mediaURI: "s3://integ-bucket/audio/test.mp3",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createTranscribeClient(t)

			// Start transcription job.
			startOut, err := client.StartTranscriptionJob(ctx, &transcribesdk.StartTranscriptionJobInput{
				TranscriptionJobName: aws.String(tt.jobName),
				LanguageCode:         transcribetypes.LanguageCodeEnUs,
				Media: &transcribetypes.Media{
					MediaFileUri: aws.String(tt.mediaURI),
				},
			})
			require.NoError(t, err, "StartTranscriptionJob should succeed")
			require.NotNil(t, startOut.TranscriptionJob)
			assert.Equal(t, tt.jobName, aws.ToString(startOut.TranscriptionJob.TranscriptionJobName))

			t.Cleanup(func() {
				cleanupCtx, cancel := cleanupContext(t)
				defer cancel()

				_, _ = client.DeleteTranscriptionJob(cleanupCtx, &transcribesdk.DeleteTranscriptionJobInput{
					TranscriptionJobName: aws.String(tt.jobName),
				})
			})

			// Get transcription job.
			getOut, err := client.GetTranscriptionJob(ctx, &transcribesdk.GetTranscriptionJobInput{
				TranscriptionJobName: aws.String(tt.jobName),
			})
			require.NoError(t, err, "GetTranscriptionJob should succeed")
			require.NotNil(t, getOut.TranscriptionJob)
			assert.Equal(t, tt.jobName, aws.ToString(getOut.TranscriptionJob.TranscriptionJobName))

			// List transcription jobs — should contain the created one.
			listOut, err := client.ListTranscriptionJobs(ctx, &transcribesdk.ListTranscriptionJobsInput{})
			require.NoError(t, err, "ListTranscriptionJobs should succeed")

			found := false
			for _, j := range listOut.TranscriptionJobSummaries {
				if aws.ToString(j.TranscriptionJobName) == tt.jobName {
					found = true

					break
				}
			}

			assert.True(t, found, "started job should appear in list")

			// Delete transcription job.
			_, err = client.DeleteTranscriptionJob(ctx, &transcribesdk.DeleteTranscriptionJobInput{
				TranscriptionJobName: aws.String(tt.jobName),
			})
			require.NoError(t, err, "DeleteTranscriptionJob should succeed")
		})
	}
}
