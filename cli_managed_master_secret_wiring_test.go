package main

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/docdb"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManagedMasterSecretWiring(t *testing.T) {
	t.Parallel()

	tests := []struct {
		create func(t *testing.T, fx *sfnFixture) string
		remove func(t *testing.T, fx *sfnFixture)
		name   string
	}{
		{
			name: "rds_instance",
			create: func(t *testing.T, fx *sfnFixture) string {
				t.Helper()

				out, err := rds.NewFromConfig(fx.cfg).CreateDBInstance(t.Context(), &rds.CreateDBInstanceInput{
					DBInstanceIdentifier: aws.String("wired-i"), Engine: aws.String("mysql"),
					DBInstanceClass: aws.String("db.t3.micro"), MasterUsername: aws.String("admin"),
					AllocatedStorage: aws.Int32(20), ManageMasterUserPassword: aws.Bool(true),
				})
				require.NoError(t, err)

				return aws.ToString(out.DBInstance.MasterUserSecret.SecretArn)
			},
			remove: func(t *testing.T, fx *sfnFixture) {
				t.Helper()

				_, err := rds.NewFromConfig(fx.cfg).DeleteDBInstance(t.Context(), &rds.DeleteDBInstanceInput{
					DBInstanceIdentifier: aws.String("wired-i"), SkipFinalSnapshot: aws.Bool(true),
				})
				require.NoError(t, err)
			},
		},
		{
			name: "rds_cluster",
			create: func(t *testing.T, fx *sfnFixture) string {
				t.Helper()

				out, err := rds.NewFromConfig(fx.cfg).CreateDBCluster(t.Context(), &rds.CreateDBClusterInput{
					DBClusterIdentifier: aws.String("wired-c"), Engine: aws.String("aurora-postgresql"),
					MasterUsername: aws.String("admin"), ManageMasterUserPassword: aws.Bool(true),
				})
				require.NoError(t, err)

				return aws.ToString(out.DBCluster.MasterUserSecret.SecretArn)
			},
			remove: func(t *testing.T, fx *sfnFixture) {
				t.Helper()

				_, err := rds.NewFromConfig(fx.cfg).DeleteDBCluster(t.Context(), &rds.DeleteDBClusterInput{
					DBClusterIdentifier: aws.String("wired-c"), SkipFinalSnapshot: aws.Bool(true),
				})
				require.NoError(t, err)
			},
		},
		{
			name: "docdb_cluster",
			create: func(t *testing.T, fx *sfnFixture) string {
				t.Helper()

				out, err := docdb.NewFromConfig(fx.cfg).CreateDBCluster(t.Context(), &docdb.CreateDBClusterInput{
					DBClusterIdentifier: aws.String("wired-d"), Engine: aws.String("docdb"),
					MasterUsername: aws.String("admin"), ManageMasterUserPassword: aws.Bool(true),
				})
				require.NoError(t, err)

				return aws.ToString(out.DBCluster.MasterUserSecret.SecretArn)
			},
			remove: func(t *testing.T, fx *sfnFixture) {
				t.Helper()

				_, err := docdb.NewFromConfig(fx.cfg).DeleteDBCluster(t.Context(), &docdb.DeleteDBClusterInput{
					DBClusterIdentifier: aws.String("wired-d"), SkipFinalSnapshot: aws.Bool(true),
				})
				require.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixture(t)
			sm := secretsmanager.NewFromConfig(fx.cfg)

			secretARN := tt.create(t, fx)

			got, err := sm.GetSecretValue(
				t.Context(),
				&secretsmanager.GetSecretValueInput{SecretId: aws.String(secretARN)},
			)
			require.NoError(t, err)
			assert.Contains(t, aws.ToString(got.SecretString), `"username":"admin"`)
			assert.Contains(t, aws.ToString(got.SecretString), `"password":"`)

			tt.remove(t, fx)

			_, err = sm.GetSecretValue(
				t.Context(),
				&secretsmanager.GetSecretValueInput{SecretId: aws.String(secretARN)},
			)
			require.Error(t, err)
		})
	}
}
