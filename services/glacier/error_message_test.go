package glacier_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	glaciersdk "github.com/aws/aws-sdk-go-v2/service/glacier"
	gtypes "github.com/aws/aws-sdk-go-v2/service/glacier/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestErrorMessages_NoCodePrefix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call func(*glaciersdk.Client) error
		name string
		want string
	}{
		{
			name: "describe missing vault",
			call: func(c *glaciersdk.Client) error {
				_, err := c.DescribeVault(t.Context(), &glaciersdk.DescribeVaultInput{
					AccountId: aws.String("-"), VaultName: aws.String("nope"),
				})

				return err
			},
			want: "Vault not found",
		},
		{
			name: "bad job type",
			call: func(c *glaciersdk.Client) error {
				if _, err := c.CreateVault(t.Context(), &glaciersdk.CreateVaultInput{
					AccountId: aws.String("-"), VaultName: aws.String("msg-v"),
				}); err != nil {
					return err
				}
				_, err := c.InitiateJob(t.Context(), &glaciersdk.InitiateJobInput{
					AccountId: aws.String("-"), VaultName: aws.String("msg-v"),
					JobParameters: &gtypes.JobParameters{Type: aws.String("bogus")},
				})

				return err
			},
			want: "unsupported job Type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.call(newWireTestClient(t))
			require.Error(t, err)

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Contains(t, apiErr.ErrorMessage(), tt.want)
			assert.NotContains(t, apiErr.ErrorMessage(), "Exception")
		})
	}
}
