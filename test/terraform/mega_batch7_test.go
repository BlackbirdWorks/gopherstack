package terraform_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	accountsvc "github.com/aws/aws-sdk-go-v2/service/account"
	accounttypes "github.com/aws/aws-sdk-go-v2/service/account/types"
	lightsailsvc "github.com/aws/aws-sdk-go-v2/service/lightsail"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTerraform_MegaBatch7 provisions an Account alternate contact and a
// Lightsail key pair via Terraform and verifies each through its own SDK
// client's Get path.
func TestTerraform_MegaBatch7(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "mega-batch-7",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{}
			},
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()
				cfg := megaConfig(t)

				acctClient := accountsvc.NewFromConfig(cfg, func(o *accountsvc.Options) {
					o.BaseEndpoint = aws.String(endpoint)
				})
				acctOut, err := acctClient.GetAlternateContact(ctx, &accountsvc.GetAlternateContactInput{
					AlternateContactType: accounttypes.AlternateContactTypeOperations,
				})
				require.NoError(t, err, "GetAlternateContact should succeed")
				require.NotNil(t, acctOut.AlternateContact)
				assert.Equal(t, "Example Contact", aws.ToString(acctOut.AlternateContact.Name))

				lsClient := lightsailsvc.NewFromConfig(cfg, func(o *lightsailsvc.Options) {
					o.BaseEndpoint = aws.String(endpoint)
				})
				lsOut, err := lsClient.GetKeyPair(ctx, &lightsailsvc.GetKeyPairInput{
					KeyPairName: aws.String("mega-batch-7-keypair"),
				})
				require.NoError(t, err, "GetKeyPair should succeed")
				require.NotNil(t, lsOut.KeyPair)
				assert.Equal(t, "mega-batch-7-keypair", aws.ToString(lsOut.KeyPair.Name))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}
