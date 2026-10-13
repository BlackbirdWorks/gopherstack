package secretsmanager_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	secretsmanagersdk "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

const realismToken = "22222222-2222-2222-2222-222222222222"

func TestPutSecretValue_TokenReuseDifferentValue_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		second  string
		wantErr bool
	}{
		{name: "same value idempotent", second: "v1"},
		{name: "different value rejected", second: "other", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestSecretsManagerClient(t, secretsmanager.NewHandler(secretsmanager.NewInMemoryBackend()))
			_, err := client.CreateSecret(t.Context(), &secretsmanagersdk.CreateSecretInput{
				Name: aws.String("tok"), SecretString: aws.String("seed"),
			})
			require.NoError(t, err)

			_, err = client.PutSecretValue(t.Context(), &secretsmanagersdk.PutSecretValueInput{
				SecretId: aws.String(
					"tok",
				),
				SecretString:       aws.String("v1"),
				ClientRequestToken: aws.String(realismToken),
			})
			require.NoError(t, err)

			_, err = client.PutSecretValue(t.Context(), &secretsmanagersdk.PutSecretValueInput{
				SecretId: aws.String(
					"tok",
				),
				SecretString:       aws.String(tc.second),
				ClientRequestToken: aws.String(realismToken),
			})
			if tc.wantErr {
				var target *types.ResourceExistsException
				require.ErrorAs(t, err, &target)

				got, getErr := client.GetSecretValue(
					t.Context(),
					&secretsmanagersdk.GetSecretValueInput{SecretId: aws.String("tok")},
				)
				require.NoError(t, getErr)
				assert.Equal(t, "v1", aws.ToString(got.SecretString))

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestListCalls_InvalidNextToken_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(c *secretsmanagersdk.Client) error
		name string
	}{
		{
			name: "list secrets",
			run: func(c *secretsmanagersdk.Client) error {
				_, err := c.ListSecrets(
					t.Context(),
					&secretsmanagersdk.ListSecretsInput{NextToken: aws.String("bogus")},
				)

				return err
			},
		},
		{
			name: "list versions",
			run: func(c *secretsmanagersdk.Client) error {
				_, err := c.ListSecretVersionIds(t.Context(), &secretsmanagersdk.ListSecretVersionIdsInput{
					SecretId: aws.String("tok"), NextToken: aws.String("bogus"),
				})

				return err
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestSecretsManagerClient(t, secretsmanager.NewHandler(secretsmanager.NewInMemoryBackend()))
			_, err := client.CreateSecret(t.Context(), &secretsmanagersdk.CreateSecretInput{
				Name: aws.String("tok"), SecretString: aws.String("seed"),
			})
			require.NoError(t, err)

			var target *types.InvalidNextTokenException
			assert.ErrorAs(t, tc.run(client), &target)
		})
	}
}
