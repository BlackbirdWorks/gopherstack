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

func TestStartMedicalScribeJob_Context(t *testing.T) {
	t.Parallel()

	tests := []struct {
		context      *sdktypes.MedicalScribeContext
		name         string
		wantProvided bool
		wantErr      bool
	}{
		{name: "no context"},
		{
			name: "with pronouns", wantProvided: true,
			context: &sdktypes.MedicalScribeContext{
				PatientContext: &sdktypes.MedicalScribePatientContext{Pronouns: sdktypes.PronounsSheHer},
			},
		},
		{
			name: "bad pronouns", wantErr: true,
			context: &sdktypes.MedicalScribeContext{
				PatientContext: &sdktypes.MedicalScribePatientContext{Pronouns: "XE_XEM"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTranscribeSDKClient(t, transcribe.NewHandler(transcribe.NewInMemoryBackend()))

			_, err := client.StartMedicalScribeJob(t.Context(), &transcribesdk.StartMedicalScribeJobInput{
				MedicalScribeJobName: aws.String("ctx-job"),
				Media:                &sdktypes.Media{MediaFileUri: aws.String("s3://bucket/audio.wav")},
				DataAccessRoleArn:    aws.String("arn:aws:iam::123456789012:role/transcribe"),
				OutputBucketName:     aws.String("output-bucket"),
				MedicalScribeContext: tt.context,
				Settings: &sdktypes.MedicalScribeSettings{
					ShowSpeakerLabels: aws.Bool(true),
					MaxSpeakerLabels:  aws.Int32(2),
				},
			})
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			got, err := client.GetMedicalScribeJob(t.Context(), &transcribesdk.GetMedicalScribeJobInput{
				MedicalScribeJobName: aws.String("ctx-job"),
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantProvided, aws.ToBool(got.MedicalScribeJob.MedicalScribeContextProvided))
		})
	}
}
