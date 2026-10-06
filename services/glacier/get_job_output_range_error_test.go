package glacier_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	glaciersdk "github.com/aws/aws-sdk-go-v2/service/glacier"
	glaciertypes "github.com/aws/aws-sdk-go-v2/service/glacier/types"
	"github.com/stretchr/testify/require"
)

func TestGetJobOutput_BadRangeIsModelledError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
	}{
		{name: "no_bytes_prefix", input: "0-4"},
		{name: "no_dash", input: "bytes=5"},
		{name: "past_end", input: "bytes=9999-10000"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newVaultLockTestClient(t)
			createLockTestVault(t, client, "range-vault")
			uploadLockTestArchive(t, client, "range-vault")

			job, err := client.InitiateJob(t.Context(), &glaciersdk.InitiateJobInput{
				AccountId: aws.String("-"),
				VaultName: aws.String("range-vault"),
				JobParameters: &glaciertypes.JobParameters{
					Type: aws.String("inventory-retrieval"),
				},
			})
			require.NoError(t, err)

			_, err = client.GetJobOutput(t.Context(), &glaciersdk.GetJobOutputInput{
				AccountId: aws.String("-"),
				VaultName: aws.String("range-vault"),
				JobId:     job.JobId,
				Range:     aws.String(tt.input),
			})
			require.Error(t, err)

			var want *glaciertypes.InvalidParameterValueException
			require.ErrorAs(t, err, &want)
		})
	}
}
