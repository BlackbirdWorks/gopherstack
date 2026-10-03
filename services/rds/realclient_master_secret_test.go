package rds_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdssdk "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rds"
)

const secretKMSKey = "arn:aws:kms:us-east-1:123456789012:key/11111111-2222-3333-4444-555555555555"

func apiErrCode(t *testing.T, err error) string {
	t.Helper()

	var ae smithy.APIError
	require.ErrorAs(t, err, &ae)

	return ae.ErrorCode()
}

func seedInstance(t *testing.T, backend *rds.InMemoryBackend, id string) {
	t.Helper()

	_, err := backend.CreateDBInstance(id, "mysql", "db.t3.micro", "db", "admin", "", 20, rds.DBInstanceOptions{})
	require.NoError(t, err)
}

func TestRealClient_MasterUserSecret(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, backend *rds.InMemoryBackend, c *rdssdk.Client)
		name string
	}{
		{name: "create_instance_with_kms", run: func(t *testing.T, _ *rds.InMemoryBackend, c *rdssdk.Client) {
			t.Helper()

			out, err := c.CreateDBInstance(t.Context(), &rdssdk.CreateDBInstanceInput{
				DBInstanceIdentifier: aws.String("sec-i1"), Engine: aws.String("mysql"),
				DBInstanceClass: aws.String("db.t3.micro"), MasterUsername: aws.String("admin"),
				AllocatedStorage: aws.Int32(20), ManageMasterUserPassword: aws.Bool(true),
				MasterUserSecretKmsKeyId: aws.String(secretKMSKey),
			})
			require.NoError(t, err)
			require.NotNil(t, out.DBInstance.MasterUserSecret)
			assert.Equal(t, secretKMSKey, aws.ToString(out.DBInstance.MasterUserSecret.KmsKeyId))
			assert.Equal(t, "active", aws.ToString(out.DBInstance.MasterUserSecret.SecretStatus))
			assert.Contains(t, aws.ToString(out.DBInstance.MasterUserSecret.SecretArn), ":secret:rds!db-")

			desc, err := c.DescribeDBInstances(t.Context(), &rdssdk.DescribeDBInstancesInput{
				DBInstanceIdentifier: aws.String("sec-i1"),
			})
			require.NoError(t, err)
			assert.Equal(t, secretKMSKey, aws.ToString(desc.DBInstances[0].MasterUserSecret.KmsKeyId))
		}},
		{name: "create_instance_kms_without_manage", run: func(t *testing.T, _ *rds.InMemoryBackend, c *rdssdk.Client) {
			t.Helper()

			_, err := c.CreateDBInstance(t.Context(), &rdssdk.CreateDBInstanceInput{
				DBInstanceIdentifier: aws.String("sec-i2"), Engine: aws.String("mysql"),
				DBInstanceClass: aws.String("db.t3.micro"), MasterUsername: aws.String("admin"),
				AllocatedStorage: aws.Int32(20), MasterUserSecretKmsKeyId: aws.String(secretKMSKey),
			})
			require.Error(t, err)
			assert.Equal(t, "InvalidParameterCombination", apiErrCode(t, err))
		}},
		{
			name: "create_instance_manage_with_password",
			run: func(t *testing.T, _ *rds.InMemoryBackend, c *rdssdk.Client) {
				t.Helper()

				_, err := c.CreateDBInstance(t.Context(), &rdssdk.CreateDBInstanceInput{
					DBInstanceIdentifier: aws.String("sec-i3"), Engine: aws.String("mysql"),
					DBInstanceClass: aws.String("db.t3.micro"), MasterUsername: aws.String("admin"),
					AllocatedStorage: aws.Int32(20), ManageMasterUserPassword: aws.Bool(true),
					MasterUserPassword: aws.String("hunter2hunter2"),
				})
				require.Error(t, err)
				assert.Equal(t, "InvalidParameterCombination", apiErrCode(t, err))
			},
		},
		{name: "modify_instance_lifecycle", run: func(t *testing.T, b *rds.InMemoryBackend, c *rdssdk.Client) {
			t.Helper()

			seedInstance(t, b, "sec-i4")
			on, err := c.ModifyDBInstance(t.Context(), &rdssdk.ModifyDBInstanceInput{
				DBInstanceIdentifier: aws.String("sec-i4"), ManageMasterUserPassword: aws.Bool(true),
				MasterUserSecretKmsKeyId: aws.String(secretKMSKey),
			})
			require.NoError(t, err)
			require.NotNil(t, on.DBInstance.MasterUserSecret)
			assert.Equal(t, secretKMSKey, aws.ToString(on.DBInstance.MasterUserSecret.KmsKeyId))

			const other = "alias/other"
			re, err := c.ModifyDBInstance(t.Context(), &rdssdk.ModifyDBInstanceInput{
				DBInstanceIdentifier: aws.String("sec-i4"), MasterUserSecretKmsKeyId: aws.String(other),
			})
			require.NoError(t, err)
			assert.Equal(t, other, aws.ToString(re.DBInstance.MasterUserSecret.KmsKeyId))
			assert.Equal(t, aws.ToString(on.DBInstance.MasterUserSecret.SecretArn),
				aws.ToString(re.DBInstance.MasterUserSecret.SecretArn))

			_, err = c.ModifyDBInstance(t.Context(), &rdssdk.ModifyDBInstanceInput{
				DBInstanceIdentifier: aws.String("sec-i4"), ManageMasterUserPassword: aws.Bool(false),
			})
			require.Error(t, err)
			assert.Equal(t, "InvalidParameterCombination", apiErrCode(t, err))

			off, err := c.ModifyDBInstance(t.Context(), &rdssdk.ModifyDBInstanceInput{
				DBInstanceIdentifier: aws.String("sec-i4"), ManageMasterUserPassword: aws.Bool(false),
				MasterUserPassword: aws.String("hunter2hunter2"),
			})
			require.NoError(t, err)
			assert.Nil(t, off.DBInstance.MasterUserSecret)
		}},
		{name: "modify_instance_kms_without_secret", run: func(t *testing.T, b *rds.InMemoryBackend, c *rdssdk.Client) {
			t.Helper()

			seedInstance(t, b, "sec-i5")
			_, err := c.ModifyDBInstance(t.Context(), &rdssdk.ModifyDBInstanceInput{
				DBInstanceIdentifier: aws.String("sec-i5"), MasterUserSecretKmsKeyId: aws.String(secretKMSKey),
			})
			require.Error(t, err)
			assert.Equal(t, "InvalidParameterCombination", apiErrCode(t, err))
		}},
		{name: "cluster_create_and_modify", run: func(t *testing.T, _ *rds.InMemoryBackend, c *rdssdk.Client) {
			t.Helper()

			out, err := c.CreateDBCluster(t.Context(), &rdssdk.CreateDBClusterInput{
				DBClusterIdentifier: aws.String("sec-c1"), Engine: aws.String("aurora-postgresql"),
				MasterUsername: aws.String("admin"), ManageMasterUserPassword: aws.Bool(true),
				MasterUserSecretKmsKeyId: aws.String(secretKMSKey),
			})
			require.NoError(t, err)
			require.NotNil(t, out.DBCluster.MasterUserSecret)
			assert.Equal(t, secretKMSKey, aws.ToString(out.DBCluster.MasterUserSecret.KmsKeyId))
			assert.Contains(t, aws.ToString(out.DBCluster.MasterUserSecret.SecretArn), ":secret:rds!cluster-")

			mod, err := c.ModifyDBCluster(t.Context(), &rdssdk.ModifyDBClusterInput{
				DBClusterIdentifier: aws.String("sec-c1"), MasterUserSecretKmsKeyId: aws.String("alias/other"),
			})
			require.NoError(t, err)
			assert.Equal(t, "alias/other", aws.ToString(mod.DBCluster.MasterUserSecret.KmsKeyId))

			_, err = c.CreateDBCluster(t.Context(), &rdssdk.CreateDBClusterInput{
				DBClusterIdentifier: aws.String("sec-c2"), Engine: aws.String("aurora-postgresql"),
				MasterUsername: aws.String("admin"), MasterUserSecretKmsKeyId: aws.String(secretKMSKey),
			})
			require.Error(t, err)
			assert.Equal(t, "InvalidParameterCombination", apiErrCode(t, err))
		}},
		{name: "cluster_restore_from_s3", run: func(t *testing.T, _ *rds.InMemoryBackend, c *rdssdk.Client) {
			t.Helper()

			out, err := c.RestoreDBClusterFromS3(t.Context(), &rdssdk.RestoreDBClusterFromS3Input{
				DBClusterIdentifier: aws.String("sec-c3"), Engine: aws.String("aurora-mysql"),
				MasterUsername: aws.String("admin"), S3BucketName: aws.String("b"),
				S3IngestionRoleArn: aws.String("arn:aws:iam::123456789012:role/r"),
				SourceEngine:       aws.String("mysql"), SourceEngineVersion: aws.String("8.0.28"),
				ManageMasterUserPassword: aws.Bool(true), MasterUserSecretKmsKeyId: aws.String(secretKMSKey),
			})
			require.NoError(t, err)
			assert.Equal(t, secretKMSKey, aws.ToString(out.DBCluster.MasterUserSecret.KmsKeyId))
		}},
		{name: "instance_restore_from_s3", run: func(t *testing.T, _ *rds.InMemoryBackend, c *rdssdk.Client) {
			t.Helper()

			out, err := c.RestoreDBInstanceFromS3(t.Context(), &rdssdk.RestoreDBInstanceFromS3Input{
				DBInstanceIdentifier: aws.String("sec-i6"), Engine: aws.String("mysql"),
				DBInstanceClass: aws.String("db.t3.micro"), S3BucketName: aws.String("b"),
				S3IngestionRoleArn: aws.String("arn:aws:iam::123456789012:role/r"),
				SourceEngine:       aws.String("mysql"), SourceEngineVersion: aws.String("8.0.28"),
				ManageMasterUserPassword: aws.Bool(true), MasterUserSecretKmsKeyId: aws.String(secretKMSKey),
			})
			require.NoError(t, err)
			assert.Equal(t, secretKMSKey, aws.ToString(out.DBInstance.MasterUserSecret.KmsKeyId))
		}},
		{
			name: "instance_restore_from_snapshot_and_pitr",
			run: func(t *testing.T, b *rds.InMemoryBackend, c *rdssdk.Client) {
				t.Helper()

				seedInstance(t, b, "sec-src")
				_, err := b.CreateDBSnapshot("sec-snap", "sec-src")
				require.NoError(t, err)

				snap, err := c.RestoreDBInstanceFromDBSnapshot(
					t.Context(),
					&rdssdk.RestoreDBInstanceFromDBSnapshotInput{
						DBInstanceIdentifier: aws.String("sec-i7"), DBSnapshotIdentifier: aws.String("sec-snap"),
						ManageMasterUserPassword: aws.Bool(true), MasterUserSecretKmsKeyId: aws.String(secretKMSKey),
					},
				)
				require.NoError(t, err)
				assert.Equal(t, secretKMSKey, aws.ToString(snap.DBInstance.MasterUserSecret.KmsKeyId))

				pitr, err := c.RestoreDBInstanceToPointInTime(t.Context(), &rdssdk.RestoreDBInstanceToPointInTimeInput{
					SourceDBInstanceIdentifier: aws.String("sec-src"), TargetDBInstanceIdentifier: aws.String("sec-i8"),
					UseLatestRestorableTime:  aws.Bool(true),
					ManageMasterUserPassword: aws.Bool(true), MasterUserSecretKmsKeyId: aws.String(secretKMSKey),
				})
				require.NoError(t, err)
				assert.Equal(t, secretKMSKey, aws.ToString(pitr.DBInstance.MasterUserSecret.KmsKeyId))
			},
		},
		{name: "tenant_create_and_modify", run: func(t *testing.T, b *rds.InMemoryBackend, c *rdssdk.Client) {
			t.Helper()

			seedInstance(t, b, "sec-i9")
			out, err := c.CreateTenantDatabase(t.Context(), &rdssdk.CreateTenantDatabaseInput{
				DBInstanceIdentifier: aws.String("sec-i9"), TenantDBName: aws.String("t1"),
				MasterUsername: aws.String("admin"), ManageMasterUserPassword: aws.Bool(true),
				MasterUserSecretKmsKeyId: aws.String(secretKMSKey),
			})
			require.NoError(t, err)
			require.NotNil(t, out.TenantDatabase.MasterUserSecret)
			assert.Equal(t, secretKMSKey, aws.ToString(out.TenantDatabase.MasterUserSecret.KmsKeyId))

			mod, err := c.ModifyTenantDatabase(t.Context(), &rdssdk.ModifyTenantDatabaseInput{
				DBInstanceIdentifier: aws.String("sec-i9"), TenantDBName: aws.String("t1"),
				MasterUserSecretKmsKeyId: aws.String("alias/other"),
			})
			require.NoError(t, err)
			assert.Equal(t, "alias/other", aws.ToString(mod.TenantDatabase.MasterUserSecret.KmsKeyId))
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend, client := newRealClientBackendAndClient(t)
			tt.run(t, backend, client)
		})
	}
}
