package transcribe_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	transcribesdk "github.com/aws/aws-sdk-go-v2/service/transcribe"
	sdktypes "github.com/aws/aws-sdk-go-v2/service/transcribe/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/transcribe"
)

// TestListJobs_EchoTopLevelStatus proves this pass's list-summary-shapes fix
// (gopherstack, 2026-09-19): ListTranscriptionJobs, ListMedicalTranscriptionJobs,
// ListMedicalScribeJobs and ListCallAnalyticsJobs each declare a top-level
// Status member on their real *Output shape (echoing the request's status
// filter, per api_op_List*.go) that these four handlers never populated --
// only ListVocabularies/ListMedicalVocabularies already had it. Each subtest
// drives the real aws-sdk-go-v2 client and also checks the raw response body.
func TestListJobs_EchoTopLevelStatus(t *testing.T) {
	t.Parallel()

	t.Run("transcription jobs", func(t *testing.T) {
		t.Parallel()

		h := transcribe.NewHandler(transcribe.NewInMemoryBackend())
		client := newTranscribeSDKClient(t, h)
		ctx := t.Context()

		_, err := client.StartTranscriptionJob(ctx, &transcribesdk.StartTranscriptionJobInput{
			TranscriptionJobName: aws.String("list-summary-tj"),
			LanguageCode:         sdktypes.LanguageCodeEnUs,
			Media:                &sdktypes.Media{MediaFileUri: aws.String("s3://bucket/audio.wav")},
		})
		require.NoError(t, err)

		out, err := client.ListTranscriptionJobs(ctx, &transcribesdk.ListTranscriptionJobsInput{
			Status: sdktypes.TranscriptionJobStatusCompleted,
		})
		require.NoError(t, err)
		require.Len(t, out.TranscriptionJobSummaries, 1)
		assert.Equal(t, sdktypes.TranscriptionJobStatusCompleted, out.Status)

		rec := doTranscribeRequest(t, h, "ListTranscriptionJobs", map[string]any{"Status": "COMPLETED"})
		assert.Contains(t, rec.Body.String(), `"Status":"COMPLETED"`)
	})

	t.Run("medical transcription jobs", func(t *testing.T) {
		t.Parallel()

		h := transcribe.NewHandler(transcribe.NewInMemoryBackend())
		client := newTranscribeSDKClient(t, h)
		ctx := t.Context()

		_, err := client.StartMedicalTranscriptionJob(ctx, &transcribesdk.StartMedicalTranscriptionJobInput{
			MedicalTranscriptionJobName: aws.String("list-summary-mtj"),
			LanguageCode:                sdktypes.LanguageCodeEnUs,
			Media:                       &sdktypes.Media{MediaFileUri: aws.String("s3://bucket/audio.wav")},
			Specialty:                   sdktypes.SpecialtyPrimarycare,
			Type:                        sdktypes.TypeConversation,
			OutputBucketName:            aws.String("output-bucket"),
		})
		require.NoError(t, err)

		out, err := client.ListMedicalTranscriptionJobs(ctx, &transcribesdk.ListMedicalTranscriptionJobsInput{
			Status: sdktypes.TranscriptionJobStatusCompleted,
		})
		require.NoError(t, err)
		require.Len(t, out.MedicalTranscriptionJobSummaries, 1)
		assert.Equal(t, sdktypes.TranscriptionJobStatusCompleted, out.Status)

		rec := doTranscribeRequest(t, h, "ListMedicalTranscriptionJobs", map[string]any{"Status": "COMPLETED"})
		assert.Contains(t, rec.Body.String(), `"Status":"COMPLETED"`)
	})

	t.Run("medical scribe jobs", func(t *testing.T) {
		t.Parallel()

		h := transcribe.NewHandler(transcribe.NewInMemoryBackend())
		client := newTranscribeSDKClient(t, h)
		ctx := t.Context()

		_, err := client.StartMedicalScribeJob(ctx, &transcribesdk.StartMedicalScribeJobInput{
			MedicalScribeJobName: aws.String("list-summary-msj"),
			Media:                &sdktypes.Media{MediaFileUri: aws.String("s3://bucket/audio.wav")},
			DataAccessRoleArn:    aws.String("arn:aws:iam::123456789012:role/transcribe"),
			OutputBucketName:     aws.String("output-bucket"),
			Settings: &sdktypes.MedicalScribeSettings{
				ShowSpeakerLabels: aws.Bool(true),
				MaxSpeakerLabels:  aws.Int32(2),
			},
		})
		require.NoError(t, err)

		out, err := client.ListMedicalScribeJobs(ctx, &transcribesdk.ListMedicalScribeJobsInput{
			Status: sdktypes.MedicalScribeJobStatusCompleted,
		})
		require.NoError(t, err)
		require.Len(t, out.MedicalScribeJobSummaries, 1)
		assert.Equal(t, sdktypes.MedicalScribeJobStatusCompleted, out.Status)

		rec := doTranscribeRequest(t, h, "ListMedicalScribeJobs", map[string]any{"Status": "COMPLETED"})
		assert.Contains(t, rec.Body.String(), `"Status":"COMPLETED"`)
	})

	t.Run("call analytics jobs", func(t *testing.T) {
		t.Parallel()

		h := transcribe.NewHandler(transcribe.NewInMemoryBackend())
		client := newTranscribeSDKClient(t, h)
		ctx := t.Context()

		_, err := client.StartCallAnalyticsJob(ctx, &transcribesdk.StartCallAnalyticsJobInput{
			CallAnalyticsJobName: aws.String("list-summary-caj"),
			Media:                &sdktypes.Media{MediaFileUri: aws.String("s3://bucket/call.wav")},
		})
		require.NoError(t, err)

		out, err := client.ListCallAnalyticsJobs(ctx, &transcribesdk.ListCallAnalyticsJobsInput{
			Status: sdktypes.CallAnalyticsJobStatusCompleted,
		})
		require.NoError(t, err)
		require.Len(t, out.CallAnalyticsJobSummaries, 1)
		assert.Equal(t, sdktypes.CallAnalyticsJobStatusCompleted, out.Status)

		rec := doTranscribeRequest(t, h, "ListCallAnalyticsJobs", map[string]any{"Status": "COMPLETED"})
		assert.Contains(t, rec.Body.String(), `"Status":"COMPLETED"`)
	})
}
