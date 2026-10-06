package dsql_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dsqlsdk "github.com/aws/aws-sdk-go-v2/service/dsql"
	"github.com/aws/aws-sdk-go-v2/service/dsql/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCluster_KmsKeyReportedAsARN(t *testing.T) {
	t.Parallel()

	const keyARN = "arn:aws:kms:" + testRegion + ":" + testAccountID

	const custom = types.EncryptionTypeCustomerManagedKmsKey

	tests := []struct {
		name    string
		key     string
		wantARN string
		wantTyp types.EncryptionType
	}{
		{name: "key id", key: "1234abcd", wantARN: keyARN + ":key/1234abcd", wantTyp: custom},
		{name: "alias", key: "alias/mine", wantARN: keyARN + ":alias/mine", wantTyp: custom},
		{name: "arn", key: keyARN + ":key/abc", wantARN: keyARN + ":key/abc", wantTyp: custom},
		{name: "owned", key: "", wantARN: "", wantTyp: types.EncryptionTypeAwsOwnedKmsKey},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := newTestClient(t, newTestHandler())

			created, err := client.CreateCluster(ctx, &dsqlsdk.CreateClusterInput{KmsEncryptionKey: aws.String(tt.key)})
			require.NoError(t, err)

			got, err := client.GetCluster(ctx, &dsqlsdk.GetClusterInput{Identifier: created.Identifier})
			require.NoError(t, err)
			require.NotNil(t, got.EncryptionDetails)
			assert.Equal(t, tt.wantARN, aws.ToString(got.EncryptionDetails.KmsKeyArn))
			assert.Equal(t, tt.wantTyp, got.EncryptionDetails.EncryptionType)

			_, err = client.UpdateCluster(ctx, &dsqlsdk.UpdateClusterInput{
				Identifier: created.Identifier, DeletionProtectionEnabled: aws.Bool(true),
			})
			require.NoError(t, err)

			got, err = client.GetCluster(ctx, &dsqlsdk.GetClusterInput{Identifier: created.Identifier})
			require.NoError(t, err)
			assert.Equal(t, tt.wantARN, aws.ToString(got.EncryptionDetails.KmsKeyArn))
		})
	}
}
