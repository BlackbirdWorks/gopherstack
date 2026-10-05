package glacier_test

import (
	"bytes"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	glaciersdk "github.com/aws/aws-sdk-go-v2/service/glacier"
	gtypes "github.com/aws/aws-sdk-go-v2/service/glacier/types"
	"github.com/stretchr/testify/require"
)

func TestDeleteVault_NonEmptyTypedError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "invalid_parameter_value"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newWireTestClient(t)
			_, err := client.CreateVault(t.Context(), &glaciersdk.CreateVaultInput{
				AccountId: aws.String("-"), VaultName: aws.String("v-typed"),
			})
			require.NoError(t, err)

			_, err = client.CreateVault(t.Context(), &glaciersdk.CreateVaultInput{
				AccountId: aws.String("-"), VaultName: aws.String("v-typed"),
			})
			require.NoError(t, err, "CreateVault is idempotent")

			_, err = client.UploadArchive(t.Context(), &glaciersdk.UploadArchiveInput{
				AccountId: aws.String("-"), VaultName: aws.String("v-typed"),
				Body: bytes.NewReader([]byte("data")),
			})
			require.NoError(t, err)

			_, err = client.DeleteVault(t.Context(), &glaciersdk.DeleteVaultInput{
				AccountId: aws.String("-"), VaultName: aws.String("v-typed"),
			})

			var apiErr *gtypes.InvalidParameterValueException
			require.ErrorAs(t, err, &apiErr)
		})
	}
}
