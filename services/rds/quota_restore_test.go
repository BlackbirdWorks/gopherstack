package rds_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdssdk "github.com/aws/aws-sdk-go-v2/service/rds"
	rdstypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rds"
)

func TestQuotaLimiterRestorePaths(t *testing.T) {
	t.Parallel()

	instanceOps := map[string]func(b *rds.InMemoryBackend) error{
		"replica": func(b *rds.InMemoryBackend) error {
			_, err := b.CreateDBInstanceReadReplica("n", "src", "", "", "", rds.DBInstanceOptions{})

			return err
		},
		"from snapshot": func(b *rds.InMemoryBackend) error {
			_, err := b.RestoreDBInstanceFromDBSnapshot("n", "snap", rds.DBInstanceOptions{})

			return err
		},
		"point in time": func(b *rds.InMemoryBackend) error {
			_, err := b.RestoreDBInstanceToPointInTime("n", "src", rds.DBInstanceOptions{})

			return err
		},
		"from s3": func(b *rds.InMemoryBackend) error {
			_, err := b.RestoreDBInstanceFromS3(
				"n", "mysql", "db.t3.micro", "bkt", "arn:aws:iam::000000000000:role/r", "mysql", "8.0.0",
				"", "", rds.DBInstanceOptions{})

			return err
		},
	}

	clusterOps := map[string]func(b *rds.InMemoryBackend) error{
		"from snapshot": func(b *rds.InMemoryBackend) error {
			_, err := b.RestoreDBClusterFromSnapshot("n", "csnap", "aurora-postgresql", rds.DBClusterOptions{})

			return err
		},
		"point in time": func(b *rds.InMemoryBackend) error {
			_, err := b.RestoreDBClusterToPointInTime("n", "src", rds.DBClusterOptions{})

			return err
		},
		"from s3": func(b *rds.InMemoryBackend) error {
			_, err := b.RestoreDBClusterFromS3(
				"n", "aurora-mysql", "admin", "bkt", "arn:aws:iam::000000000000:role/r", "mysql", "8.0.0",
				rds.DBClusterOptions{})

			return err
		},
	}

	for name, op := range instanceOps {
		t.Run("instance "+name, func(t *testing.T) {
			t.Parallel()

			h := newBatch3Handler(t)
			_, err := h.Backend.CreateDBInstance("src", "postgres", "db.t3.micro", "", "admin", "", 20,
				rds.DBInstanceOptions{})
			require.NoError(t, err)
			_, err = h.Backend.CreateDBSnapshot("snap", "src")
			require.NoError(t, err)

			h.SetQuotaLimiter(quotaFunc("L-7B6409FD", 1, true))

			require.ErrorIs(t, op(h.Backend), rds.ErrInstanceQuotaExceeded)
			assert.Equal(t, 1, h.InstanceCount("us-east-1"))
		})
	}

	for name, op := range clusterOps {
		t.Run("cluster "+name, func(t *testing.T) {
			t.Parallel()

			h := newBatch3Handler(t)
			_, err := h.Backend.CreateDBCluster("src", "aurora-postgresql", "admin", "", "", 5432, nil,
				rds.DBClusterOptions{})
			require.NoError(t, err)
			_, err = h.Backend.CreateDBClusterSnapshot("csnap", "src")
			require.NoError(t, err)

			h.SetQuotaLimiter(quotaFunc("L-952B80B8", 1, true))

			require.ErrorIs(t, op(h.Backend), rds.ErrClusterQuotaExceeded)
			assert.Equal(t, 1, h.ClusterCount("us-east-1"))
		})
	}
}

func TestQuotaLimitWireErrors(t *testing.T) {
	t.Parallel()

	h := newTestRDSHandler(t)
	client := newTestRDSClient(t, h)

	createFlowInstance(t, client, "wire-src")
	_, err := client.CreateDBCluster(t.Context(), &rdssdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String("wire-c"), Engine: aws.String("aurora-postgresql"),
		MasterUsername: aws.String("admin"), MasterUserPassword: aws.String("Passw0rd!x"),
	})
	require.NoError(t, err)

	h.SetQuotaLimiter(func(_, _, _, code string) (float64, bool) {
		return 1, code == "L-7B6409FD" || code == "L-952B80B8"
	})

	_, err = client.CreateDBInstance(t.Context(), &rdssdk.CreateDBInstanceInput{
		DBInstanceIdentifier: aws.String("wire-2"), Engine: aws.String("postgres"),
		DBInstanceClass: aws.String("db.t3.micro"), MasterUsername: aws.String("admin"),
		MasterUserPassword: aws.String("Passw0rd!x"), AllocatedStorage: aws.Int32(20),
	})

	var instErr *rdstypes.InstanceQuotaExceededFault

	require.ErrorAs(t, err, &instErr)

	_, err = client.CreateDBCluster(t.Context(), &rdssdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String("wire-c2"), Engine: aws.String("aurora-postgresql"),
		MasterUsername: aws.String("admin"), MasterUserPassword: aws.String("Passw0rd!x"),
	})

	var clusterErr *rdstypes.DBClusterQuotaExceededFault

	require.ErrorAs(t, err, &clusterErr)
}
