package docdb_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	docdbsdk "github.com/aws/aws-sdk-go-v2/service/docdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRealClient_ClusterMasterUserSecret(t *testing.T) {
	t.Parallel()

	const kmsKey = "arn:aws:kms:us-east-1:123456789012:key/11111111-2222-3333-4444-555555555555"

	tests := []struct {
		in      docdbsdk.CreateDBClusterInput
		name    string
		errCode string
		wantKMS string
	}{
		{name: "managed_with_kms", wantKMS: kmsKey, in: docdbsdk.CreateDBClusterInput{
			ManageMasterUserPassword: aws.Bool(true), MasterUserSecretKmsKeyId: aws.String(kmsKey),
		}},
		{name: "managed_default_key", in: docdbsdk.CreateDBClusterInput{ManageMasterUserPassword: aws.Bool(true)}},
		{name: "kms_without_manage", errCode: "InvalidParameterCombination", in: docdbsdk.CreateDBClusterInput{
			MasterUserSecretKmsKeyId: aws.String(kmsKey),
		}},
		{name: "manage_with_password", errCode: "InvalidParameterCombination", in: docdbsdk.CreateDBClusterInput{
			ManageMasterUserPassword: aws.Bool(true), MasterUserPassword: aws.String("hunter2hunter2"),
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			in := tt.in
			in.DBClusterIdentifier = aws.String("sec-c")
			in.Engine = aws.String("docdb")
			in.MasterUsername = aws.String("admin")
			out, err := client.CreateDBCluster(t.Context(), &in)
			if tt.errCode != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errCode)

				return
			}
			require.NoError(t, err)
			require.NotNil(t, out.DBCluster.MasterUserSecret)
			assert.Equal(t, tt.wantKMS, aws.ToString(out.DBCluster.MasterUserSecret.KmsKeyId))
			assert.Equal(t, "active", aws.ToString(out.DBCluster.MasterUserSecret.SecretStatus))
			assert.Contains(t, aws.ToString(out.DBCluster.MasterUserSecret.SecretArn), ":secret:rds!cluster-")

			desc, err := client.DescribeDBClusters(t.Context(), &docdbsdk.DescribeDBClustersInput{
				DBClusterIdentifier: aws.String("sec-c"),
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantKMS, aws.ToString(desc.DBClusters[0].MasterUserSecret.KmsKeyId))
		})
	}
}

func TestRealClient_ModifyClusterMasterUserSecret(t *testing.T) {
	t.Parallel()

	const kmsKey = "alias/docdb-secret"

	tests := []struct {
		name       string
		errCode    string
		wantKMS    string
		in         docdbsdk.ModifyDBClusterInput
		preManaged bool
		wantNil    bool
	}{
		{name: "enable_with_kms", wantKMS: kmsKey, in: docdbsdk.ModifyDBClusterInput{
			ManageMasterUserPassword: aws.Bool(true), MasterUserSecretKmsKeyId: aws.String(kmsKey),
		}},
		{name: "kms_without_enabling", errCode: "InvalidParameterCombination", in: docdbsdk.ModifyDBClusterInput{
			MasterUserSecretKmsKeyId: aws.String(kmsKey),
		}},
		{name: "kms_change_when_managed", preManaged: true, errCode: "InvalidParameterCombination",
			in: docdbsdk.ModifyDBClusterInput{MasterUserSecretKmsKeyId: aws.String(kmsKey)}},
		{name: "disable_needs_password", preManaged: true, errCode: "InvalidParameterCombination",
			in: docdbsdk.ModifyDBClusterInput{ManageMasterUserPassword: aws.Bool(false)}},
		{name: "disable_with_password", preManaged: true, wantNil: true, in: docdbsdk.ModifyDBClusterInput{
			ManageMasterUserPassword: aws.Bool(false), MasterUserPassword: aws.String("hunter2hunter2"),
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			_, err := client.CreateDBCluster(t.Context(), &docdbsdk.CreateDBClusterInput{
				DBClusterIdentifier: aws.String("sec-m"), Engine: aws.String("docdb"),
				MasterUsername: aws.String("admin"), ManageMasterUserPassword: aws.Bool(tt.preManaged),
			})
			require.NoError(t, err)

			in := tt.in
			in.DBClusterIdentifier = aws.String("sec-m")
			out, err := client.ModifyDBCluster(t.Context(), &in)
			if tt.errCode != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errCode)

				return
			}
			require.NoError(t, err)
			if tt.wantNil {
				assert.Nil(t, out.DBCluster.MasterUserSecret)

				return
			}
			require.NotNil(t, out.DBCluster.MasterUserSecret)
			assert.Equal(t, tt.wantKMS, aws.ToString(out.DBCluster.MasterUserSecret.KmsKeyId))
		})
	}
}
