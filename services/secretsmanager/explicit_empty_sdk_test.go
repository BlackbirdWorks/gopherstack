package secretsmanager_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	secretsmanagersdk "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

func TestUpdateSecret_OmittedVsExplicitEmpty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		secretString *string
		token        *string
		desc         *string
		name         string
		wantValue    string
		wantErr      bool
	}{
		{name: "omitted_value_keeps", desc: aws.String("d"), wantValue: "orig"},
		{name: "value_applies", secretString: aws.String("new"), wantValue: "new"},
		{name: "explicit_empty_value_rejected", secretString: aws.String(""), wantErr: true},
		{name: "explicit_empty_token_rejected", secretString: aws.String("new"), token: aws.String(""), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestSecretsManagerClient(t, secretsmanager.NewHandler(secretsmanager.NewInMemoryBackend()))
			_, err := client.CreateSecret(t.Context(), &secretsmanagersdk.CreateSecretInput{
				Name: aws.String("s"), SecretString: aws.String("orig"),
			})
			require.NoError(t, err)

			_, err = client.UpdateSecret(t.Context(), &secretsmanagersdk.UpdateSecretInput{
				SecretId:           aws.String("s"),
				SecretString:       tt.secretString,
				ClientRequestToken: tt.token,
				Description:        tt.desc,
			})
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			got, err := client.GetSecretValue(t.Context(), &secretsmanagersdk.GetSecretValueInput{
				SecretId: aws.String("s"),
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantValue, aws.ToString(got.SecretString))
		})
	}
}

func TestPutSecretValue_ExplicitEmptyMembersRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		token         *string
		rotationToken *string
		name          string
		wantErr       bool
	}{
		{name: "omitted_ok"},
		{name: "explicit_empty_token_rejected", token: aws.String(""), wantErr: true},
		{name: "explicit_empty_rotation_token_rejected", rotationToken: aws.String(""), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestSecretsManagerClient(t, secretsmanager.NewHandler(secretsmanager.NewInMemoryBackend()))
			_, err := client.CreateSecret(t.Context(), &secretsmanagersdk.CreateSecretInput{
				Name: aws.String("s"), SecretString: aws.String("orig"),
			})
			require.NoError(t, err)

			_, err = client.PutSecretValue(t.Context(), &secretsmanagersdk.PutSecretValueInput{
				SecretId:           aws.String("s"),
				SecretString:       aws.String("v2"),
				ClientRequestToken: tt.token,
				RotationToken:      tt.rotationToken,
			})
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestUpdateSecretVersionStage_ExplicitEmptyVersionIDsRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		moveTo     *string
		removeFrom *string
		name       string
	}{
		{name: "explicit_empty_move_rejected", moveTo: aws.String("")},
		{name: "explicit_empty_remove_rejected", removeFrom: aws.String("")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestSecretsManagerClient(t, secretsmanager.NewHandler(secretsmanager.NewInMemoryBackend()))
			_, err := client.CreateSecret(t.Context(), &secretsmanagersdk.CreateSecretInput{
				Name: aws.String("s"), SecretString: aws.String("orig"),
			})
			require.NoError(t, err)

			_, err = client.UpdateSecretVersionStage(t.Context(), &secretsmanagersdk.UpdateSecretVersionStageInput{
				SecretId:            aws.String("s"),
				VersionStage:        aws.String("CUSTOM"),
				MoveToVersionId:     tt.moveTo,
				RemoveFromVersionId: tt.removeFrom,
			})
			require.Error(t, err)
		})
	}
}
