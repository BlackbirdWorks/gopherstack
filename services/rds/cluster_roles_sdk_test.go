package rds_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdssdk "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rds"
)

// TestAddRoleToDBCluster_FeatureNameKeepsRolesSeparate_RealSDKClient drives
// AddRoleToDBCluster/RemoveRoleFromDBCluster/DescribeDBClusters through the
// real aws-sdk-go-v2 client to prove the same-ARN-different-FeatureName
// collapse (gopherstack-1jkv, the cluster-side analogue of the
// gopherstack-i101 instance fix) no longer loses an association. Before the
// fix, AddRoleToDBCluster deduped purely on RoleArn (db_clusters.go's old
// slices.Contains(b.clusterRoles[id], roleARN)), so associating the same
// role with a second, different, explicitly-supplied FeatureName was
// silently dropped as a no-op duplicate -- and DescribeDBClusters never
// emitted AssociatedRoles at all, so the loss was invisible either way.
func TestAddRoleToDBCluster_FeatureNameKeepsRolesSeparate_RealSDKClient(t *testing.T) {
	t.Parallel()

	h := rds.NewHandler(rds.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestRDSClient(t, h)

	_, err := client.CreateDBCluster(t.Context(), &rdssdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String("multi-feature-cluster"),
		Engine:              aws.String("aurora-mysql"),
		MasterUsername:      aws.String("admin"),
	})
	require.NoError(t, err)

	roleARN := "arn:aws:iam::000000000000:role/SharedRole"

	_, err = client.AddRoleToDBCluster(t.Context(), &rdssdk.AddRoleToDBClusterInput{
		DBClusterIdentifier: aws.String("multi-feature-cluster"),
		RoleArn:             aws.String(roleARN),
		FeatureName:         aws.String("S3_INTEGRATION"),
	})
	require.NoError(t, err)

	_, err = client.AddRoleToDBCluster(t.Context(), &rdssdk.AddRoleToDBClusterInput{
		DBClusterIdentifier: aws.String("multi-feature-cluster"),
		RoleArn:             aws.String(roleARN),
		FeatureName:         aws.String("SQLSERVER_AUDIT"),
	})
	require.NoError(t, err)

	out, err := client.DescribeDBClusters(t.Context(), &rdssdk.DescribeDBClustersInput{
		DBClusterIdentifier: aws.String("multi-feature-cluster"),
	})
	require.NoError(t, err)
	require.Len(t, out.DBClusters, 1)

	roles := out.DBClusters[0].AssociatedRoles
	require.Len(t, roles, 2,
		"both FeatureName associations for the same RoleArn must be retained, not collapsed onto one")

	byFeature := make(map[string]string, len(roles))
	for _, r := range roles {
		byFeature[aws.ToString(r.FeatureName)] = aws.ToString(r.RoleArn)
	}
	assert.Equal(t, roleARN, byFeature["S3_INTEGRATION"])
	assert.Equal(t, roleARN, byFeature["SQLSERVER_AUDIT"])

	// Removing one FeatureName's association must leave the other intact.
	_, err = client.RemoveRoleFromDBCluster(t.Context(), &rdssdk.RemoveRoleFromDBClusterInput{
		DBClusterIdentifier: aws.String("multi-feature-cluster"),
		RoleArn:             aws.String(roleARN),
		FeatureName:         aws.String("S3_INTEGRATION"),
	})
	require.NoError(t, err)

	out, err = client.DescribeDBClusters(t.Context(), &rdssdk.DescribeDBClustersInput{
		DBClusterIdentifier: aws.String("multi-feature-cluster"),
	})
	require.NoError(t, err)
	require.Len(t, out.DBClusters, 1)
	require.Len(t, out.DBClusters[0].AssociatedRoles, 1,
		"removing the S3_INTEGRATION association must not remove SQLSERVER_AUDIT's")
	assert.Equal(t, "SQLSERVER_AUDIT", aws.ToString(out.DBClusters[0].AssociatedRoles[0].FeatureName))
	assert.Equal(t, roleARN, aws.ToString(out.DBClusters[0].AssociatedRoles[0].RoleArn))
}

// TestAddRoleToDBCluster_OmittedFeatureNamePlaceholder_RealSDKClient drives
// AddRoleToDBCluster through the real client leaving the optional
// FeatureName entirely unset (rds@v1.124.1 api_op_AddRoleToDBCluster.go:39-43
// does not mark it required, unlike the instance-side member fixed in
// gopherstack-i101). Real AWS's behavior for two such adds is unverified --
// this test pins and documents this repo's placeholder rather than silently
// guessing: two different roles both added without FeatureName both persist,
// matching this emulator's pre-fix behavior for that specific bucket (see
// upsertClusterRole in db_clusters.go and gopherstack-1jkv/PARITY.md).
func TestAddRoleToDBCluster_OmittedFeatureNamePlaceholder_RealSDKClient(t *testing.T) {
	t.Parallel()

	h := rds.NewHandler(rds.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestRDSClient(t, h)

	_, err := client.CreateDBCluster(t.Context(), &rdssdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String("no-feature-cluster"),
		Engine:              aws.String("aurora-mysql"),
		MasterUsername:      aws.String("admin"),
	})
	require.NoError(t, err)

	_, err = client.AddRoleToDBCluster(t.Context(), &rdssdk.AddRoleToDBClusterInput{
		DBClusterIdentifier: aws.String("no-feature-cluster"),
		RoleArn:             aws.String("arn:aws:iam::000000000000:role/R1"),
	})
	require.NoError(t, err)

	_, err = client.AddRoleToDBCluster(t.Context(), &rdssdk.AddRoleToDBClusterInput{
		DBClusterIdentifier: aws.String("no-feature-cluster"),
		RoleArn:             aws.String("arn:aws:iam::000000000000:role/R2"),
	})
	require.NoError(t, err)

	out, err := client.DescribeDBClusters(t.Context(), &rdssdk.DescribeDBClustersInput{
		DBClusterIdentifier: aws.String("no-feature-cluster"),
	})
	require.NoError(t, err)
	require.Len(t, out.DBClusters, 1)
	require.Len(t, out.DBClusters[0].AssociatedRoles, 2,
		"placeholder pinned: distinct roles added without FeatureName both persist -- see gopherstack-1jkv")
}

// TestAddRoleToDBCluster_SameFeatureNameReplaces_RealSDKClient mirrors
// gopherstack-i101's "adding a different role for a feature already in use
// replaces it" semantics, extended to the cluster side's optional
// FeatureName.
func TestAddRoleToDBCluster_SameFeatureNameReplaces_RealSDKClient(t *testing.T) {
	t.Parallel()

	h := rds.NewHandler(rds.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestRDSClient(t, h)

	_, err := client.CreateDBCluster(t.Context(), &rdssdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String("replace-cluster"),
		Engine:              aws.String("aurora-mysql"),
		MasterUsername:      aws.String("admin"),
	})
	require.NoError(t, err)

	_, err = client.AddRoleToDBCluster(t.Context(), &rdssdk.AddRoleToDBClusterInput{
		DBClusterIdentifier: aws.String("replace-cluster"),
		RoleArn:             aws.String("arn:aws:iam::000000000000:role/R1"),
		FeatureName:         aws.String("S3_INTEGRATION"),
	})
	require.NoError(t, err)

	_, err = client.AddRoleToDBCluster(t.Context(), &rdssdk.AddRoleToDBClusterInput{
		DBClusterIdentifier: aws.String("replace-cluster"),
		RoleArn:             aws.String("arn:aws:iam::000000000000:role/R2"),
		FeatureName:         aws.String("S3_INTEGRATION"),
	})
	require.NoError(t, err)

	out, err := client.DescribeDBClusters(t.Context(), &rdssdk.DescribeDBClustersInput{
		DBClusterIdentifier: aws.String("replace-cluster"),
	})
	require.NoError(t, err)
	require.Len(t, out.DBClusters, 1)
	require.Len(t, out.DBClusters[0].AssociatedRoles, 1,
		"a second role added for a feature already in use must replace, not duplicate")
	assert.Equal(t, "arn:aws:iam::000000000000:role/R2", aws.ToString(out.DBClusters[0].AssociatedRoles[0].RoleArn))
}
