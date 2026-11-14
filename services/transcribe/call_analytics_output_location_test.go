package transcribe_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	transcribesdk "github.com/aws/aws-sdk-go-v2/service/transcribe"
	sdktypes "github.com/aws/aws-sdk-go-v2/service/transcribe/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/transcribe"
)

// StartCallAnalyticsJobInput.OutputLocation (transcribe@v1.64.0
// api_op_StartCallAnalyticsJob.go) was parsed nowhere, so every job's transcript
// landed at the hardcoded synthetic-transcripts bucket regardless of the caller's
// choice.
func TestStartCallAnalyticsJob_OutputLocation_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		outputLocation string
		jobName        string
		wantURI        string
	}{
		{
			name:           "bucket only gets the job name appended",
			outputLocation: "s3://doc-example-bucket",
			jobName:        "bucket-only-job",
			wantURI:        "s3://doc-example-bucket/bucket-only-job.json",
		},
		{
			name:           "folder gets the job name appended",
			outputLocation: "s3://doc-example-bucket/my-output-folder/",
			jobName:        "folder-job",
			wantURI:        "s3://doc-example-bucket/my-output-folder/folder-job.json",
		},
		{
			name:           "fully-specified file is used as-is",
			outputLocation: "s3://doc-example-bucket/my-output-folder/custom-name.json",
			jobName:        "fully-specified-job",
			wantURI:        "s3://doc-example-bucket/my-output-folder/custom-name.json",
		},
		{
			name:           "omitted falls back to the service-managed url",
			outputLocation: "",
			jobName:        "default-job",
			wantURI:        "http://localhost:8000/_gopherstack/transcribe/output/us-east-1/call-analytics/default-job/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := transcribe.NewHandler(transcribe.NewInMemoryBackend())
			client := newTranscribeSDKClient(t, h)
			ctx := t.Context()

			in := &transcribesdk.StartCallAnalyticsJobInput{
				CallAnalyticsJobName: aws.String(tt.jobName),
				Media:                &sdktypes.Media{MediaFileUri: aws.String("s3://input-bucket/audio.wav")},
			}
			if tt.outputLocation != "" {
				in.OutputLocation = aws.String(tt.outputLocation)
			}

			_, startErr := client.StartCallAnalyticsJob(ctx, in)
			require.NoError(t, startErr)

			out, getErr := client.GetCallAnalyticsJob(ctx, &transcribesdk.GetCallAnalyticsJobInput{
				CallAnalyticsJobName: aws.String(tt.jobName),
			})
			require.NoError(t, getErr)
			require.NotNil(t, out.CallAnalyticsJob.Transcript)
			got := aws.ToString(out.CallAnalyticsJob.Transcript.TranscriptFileUri)
			if tt.outputLocation == "" {
				require.True(t, strings.HasPrefix(got, tt.wantURI), got)
				require.True(t, strings.HasSuffix(got, "/asrOutput.json"), got)

				return
			}

			require.Equal(t, tt.wantURI, got)
		})
	}
}
