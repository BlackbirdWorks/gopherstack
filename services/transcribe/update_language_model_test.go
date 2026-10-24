package transcribe_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	transcribesdk "github.com/aws/aws-sdk-go-v2/service/transcribe"
	sdktypes "github.com/aws/aws-sdk-go-v2/service/transcribe/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/transcribe"
)

const (
	updateLMKey  = "arn:aws:kms:us-east-1:123456789012:key/1234abcd-12ab-34cd-56ef-1234567890ab"
	updateLMRole = "arn:aws:iam::123456789012:role/transcribe-new"
)

func newUpdateLMClient(t *testing.T) (*transcribesdk.Client, *transcribe.InMemoryBackend) {
	t.Helper()

	b := transcribe.NewInMemoryBackend()
	client := newTranscribeSDKClient(t, transcribe.NewHandler(b))

	_, err := client.CreateLanguageModel(t.Context(), &transcribesdk.CreateLanguageModelInput{
		ModelName:     aws.String("lm"),
		BaseModelName: sdktypes.BaseModelNameWideBand,
		LanguageCode:  sdktypes.CLMLanguageCodeEnUs,
		InputDataConfig: &sdktypes.InputDataConfig{
			S3Uri:             aws.String("s3://bucket/training"),
			DataAccessRoleArn: aws.String("arn:aws:iam::123456789012:role/transcribe"),
		},
	})
	require.NoError(t, err)

	return client, b
}

func TestSDKRoundTrip_UpdateLanguageModel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in       *transcribesdk.UpdateLanguageModelInput
		check    func(t *testing.T, m *sdktypes.LanguageModel)
		name     string
		wantCode string
	}{
		{
			name: "encryption_configuration",
			in: &transcribesdk.UpdateLanguageModelInput{
				ModelName: aws.String("lm"),
				EncryptionConfiguration: &sdktypes.EncryptionConfiguration{
					KMSKey:               aws.String(updateLMKey),
					KMSEncryptionContext: map[string]string{"k": "v"},
				},
			},
			check: func(t *testing.T, m *sdktypes.LanguageModel) {
				t.Helper()
				require.NotNil(t, m.EncryptionConfiguration)
				assert.Equal(t, updateLMKey, aws.ToString(m.EncryptionConfiguration.KMSKey))
				assert.Equal(t, map[string]string{"k": "v"}, m.EncryptionConfiguration.KMSEncryptionContext)
			},
		},
		{
			name: "data_access_role",
			in: &transcribesdk.UpdateLanguageModelInput{
				ModelName: aws.String("lm"), DataAccessRoleArn: aws.String(updateLMRole),
			},
			check: func(t *testing.T, m *sdktypes.LanguageModel) {
				t.Helper()
				require.NotNil(t, m.InputDataConfig)
				assert.Equal(t, updateLMRole, aws.ToString(m.InputDataConfig.DataAccessRoleArn))
				assert.Equal(t, "s3://bucket/training", aws.ToString(m.InputDataConfig.S3Uri))
			},
		},
		{
			name: "unknown_model",
			in: &transcribesdk.UpdateLanguageModelInput{
				ModelName:         aws.String("nope"),
				DataAccessRoleArn: aws.String(updateLMRole),
			},
			wantCode: "NotFoundException",
		},
		{
			name:     "nothing_to_update",
			in:       &transcribesdk.UpdateLanguageModelInput{ModelName: aws.String("lm")},
			wantCode: "BadRequestException",
		},
		{
			name: "kms_key_not_an_arn",
			in: &transcribesdk.UpdateLanguageModelInput{
				ModelName:               aws.String("lm"),
				EncryptionConfiguration: &sdktypes.EncryptionConfiguration{KMSKey: aws.String("alias/foo")},
			},
			wantCode: "BadRequestException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, _ := newUpdateLMClient(t)
			before, err := client.DescribeLanguageModel(t.Context(), &transcribesdk.DescribeLanguageModelInput{
				ModelName: aws.String("lm"),
			})
			require.NoError(t, err)

			out, err := client.UpdateLanguageModel(t.Context(), tt.in)
			if tt.wantCode != "" {
				var apiErr smithy.APIError
				require.ErrorAs(t, err, &apiErr)
				assert.Equal(t, tt.wantCode, apiErr.ErrorCode())

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "lm", aws.ToString(out.ModelName))
			assert.Equal(t, sdktypes.ModelStatusCompleted, out.ModelStatus)
			require.NotNil(t, out.LastModifiedTime)
			assert.False(t, out.LastModifiedTime.Before(*before.LanguageModel.LastModifiedTime))

			desc, err := client.DescribeLanguageModel(t.Context(), &transcribesdk.DescribeLanguageModelInput{
				ModelName: aws.String("lm"),
			})
			require.NoError(t, err)
			tt.check(t, desc.LanguageModel)
		})
	}
}

func TestSDKRoundTrip_UpdateLanguageModel_InProgress(t *testing.T) {
	t.Parallel()

	b := transcribe.NewInMemoryBackend()
	client := newTranscribeSDKClient(t, transcribe.NewHandler(b))
	b.AddLanguageModelInternal(&transcribe.LanguageModel{ModelName: "lm", ModelStatus: "COMPLETED"})

	in := &transcribesdk.UpdateLanguageModelInput{
		ModelName:         aws.String("lm"),
		DataAccessRoleArn: aws.String(updateLMRole),
	}

	b.SetLifecycleDelay(time.Hour)

	out, err := client.UpdateLanguageModel(t.Context(), in)
	require.NoError(t, err)
	assert.Equal(t, sdktypes.ModelStatusInProgress, out.ModelStatus)

	_, err = client.UpdateLanguageModel(t.Context(), in)
	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "BadRequestException", apiErr.ErrorCode())

	b.SetLifecycleDelay(0)

	out, err = client.UpdateLanguageModel(t.Context(), in)
	require.NoError(t, err)
	assert.Equal(t, sdktypes.ModelStatusCompleted, out.ModelStatus)
}

func TestUpdateLanguageModel_Persistence(t *testing.T) {
	t.Parallel()

	client, src := newUpdateLMClient(t)
	_, err := client.UpdateLanguageModel(t.Context(), &transcribesdk.UpdateLanguageModelInput{
		ModelName:               aws.String("lm"),
		EncryptionConfiguration: &sdktypes.EncryptionConfiguration{KMSKey: aws.String(updateLMKey)},
	})
	require.NoError(t, err)

	dst := transcribe.NewInMemoryBackend()
	require.NoError(t, dst.Restore(t.Context(), src.Snapshot(t.Context())))

	desc, err := newTranscribeSDKClient(t, transcribe.NewHandler(dst)).DescribeLanguageModel(
		t.Context(), &transcribesdk.DescribeLanguageModelInput{ModelName: aws.String("lm")},
	)
	require.NoError(t, err)
	require.NotNil(t, desc.LanguageModel.EncryptionConfiguration)
	assert.Equal(t, updateLMKey, aws.ToString(desc.LanguageModel.EncryptionConfiguration.KMSKey))
}
