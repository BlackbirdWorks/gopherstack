package neptune_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	neptunesdk "github.com/aws/aws-sdk-go-v2/service/neptune"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune"
)

// TestCreateDBCluster_JoinsExistingGlobalCluster proves CreateDBCluster now
// accepts and applies its real, previously-unparsed GlobalClusterIdentifier
// member (api_op_CreateDBCluster.go:129): a cluster created with it set joins
// that global cluster as a member (writer, since the global cluster starts
// with none), and DescribeDBClusters echoes the real DBCluster.
// GlobalClusterIdentifier response member -- also previously entirely
// unmodeled (zero struct field), so a real client always decoded it as nil
// regardless of what CreateGlobalCluster/CreateDBCluster had actually done.
func TestCreateDBCluster_JoinsExistingGlobalCluster(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("123456789012", testRegion)
	h := neptune.NewHandler(backend)
	client := newTestNeptuneClient(t, h)

	_, err := client.CreateGlobalCluster(t.Context(), &neptunesdk.CreateGlobalClusterInput{
		GlobalClusterIdentifier: aws.String("gfix-global"),
	})
	require.NoError(t, err)

	_, err = client.CreateDBCluster(t.Context(), &neptunesdk.CreateDBClusterInput{
		DBClusterIdentifier:     aws.String("gfix-cluster"),
		Engine:                  aws.String("neptune"),
		GlobalClusterIdentifier: aws.String("gfix-global"),
	})
	require.NoError(t, err)

	described, err := client.DescribeDBClusters(t.Context(), &neptunesdk.DescribeDBClustersInput{
		DBClusterIdentifier: aws.String("gfix-cluster"),
	})
	require.NoError(t, err)
	require.Len(t, described.DBClusters, 1)
	assert.Equal(t, "gfix-global", aws.ToString(described.DBClusters[0].GlobalClusterIdentifier),
		"GlobalClusterIdentifier supplied at CreateDBCluster was silently dropped before this fix")

	globals, err := client.DescribeGlobalClusters(t.Context(), &neptunesdk.DescribeGlobalClustersInput{
		GlobalClusterIdentifier: aws.String("gfix-global"),
	})
	require.NoError(t, err)
	require.Len(t, globals.GlobalClusters, 1)
	require.Len(t, globals.GlobalClusters[0].GlobalClusterMembers, 1)
	member := globals.GlobalClusters[0].GlobalClusterMembers[0]
	assert.Contains(t, aws.ToString(member.DBClusterArn), "gfix-cluster")
	assert.True(t, aws.ToBool(member.IsWriter), "the first member joining an empty global cluster should be the writer")
}

// TestCreateDBCluster_JoinNonexistentGlobalCluster proves an unresolvable
// GlobalClusterIdentifier at CreateDBCluster is rejected rather than silently
// ignored (the pre-fix behavior, since the field was never read at all).
func TestCreateDBCluster_JoinNonexistentGlobalCluster(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("123456789012", testRegion)
	h := neptune.NewHandler(backend)
	client := newTestNeptuneClient(t, h)

	_, err := client.CreateDBCluster(t.Context(), &neptunesdk.CreateDBClusterInput{
		DBClusterIdentifier:     aws.String("orphan-cluster"),
		Engine:                  aws.String("neptune"),
		GlobalClusterIdentifier: aws.String("does-not-exist"),
	})
	require.Error(t, err)
}

// TestCreateGlobalCluster_WithSource_SetsMemberClusterField proves
// CreateGlobalCluster's SourceDBClusterIdentifier path -- which promotes an
// existing DB cluster to writer on the GlobalCluster side -- now also sets
// that DB cluster's own GlobalClusterIdentifier reciprocally, so
// DescribeDBClusters on the source cluster reflects its global-cluster
// membership too, not just DescribeGlobalClusters.
func TestCreateGlobalCluster_WithSource_SetsMemberClusterField(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("123456789012", testRegion)
	h := neptune.NewHandler(backend)
	client := newTestNeptuneClient(t, h)

	_, err := client.CreateDBCluster(t.Context(), &neptunesdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String("src-cluster"),
		Engine:              aws.String("neptune"),
	})
	require.NoError(t, err)

	_, err = client.CreateGlobalCluster(t.Context(), &neptunesdk.CreateGlobalClusterInput{
		GlobalClusterIdentifier:   aws.String("gfix-global-src"),
		SourceDBClusterIdentifier: aws.String("src-cluster"),
	})
	require.NoError(t, err)

	described, err := client.DescribeDBClusters(t.Context(), &neptunesdk.DescribeDBClustersInput{
		DBClusterIdentifier: aws.String("src-cluster"),
	})
	require.NoError(t, err)
	require.Len(t, described.DBClusters, 1)
	assert.Equal(
		t,
		"gfix-global-src",
		aws.ToString(described.DBClusters[0].GlobalClusterIdentifier),
		"CreateGlobalCluster's SourceDBClusterIdentifier never reciprocally set the DB cluster's own field before this fix",
	)
}

// TestRemoveFromGlobalCluster_ClearsMemberClusterField proves
// RemoveFromGlobalCluster clears the departing cluster's own
// GlobalClusterIdentifier, mirroring the attach-side fix above.
func TestRemoveFromGlobalCluster_ClearsMemberClusterField(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("123456789012", testRegion)
	h := neptune.NewHandler(backend)
	client := newTestNeptuneClient(t, h)

	_, err := client.CreateGlobalCluster(t.Context(), &neptunesdk.CreateGlobalClusterInput{
		GlobalClusterIdentifier: aws.String("gfix-global-remove"),
	})
	require.NoError(t, err)

	created, err := client.CreateDBCluster(t.Context(), &neptunesdk.CreateDBClusterInput{
		DBClusterIdentifier:     aws.String("leaving-cluster"),
		Engine:                  aws.String("neptune"),
		GlobalClusterIdentifier: aws.String("gfix-global-remove"),
	})
	require.NoError(t, err)

	beforeRemoval, err := client.DescribeDBClusters(t.Context(), &neptunesdk.DescribeDBClustersInput{
		DBClusterIdentifier: aws.String("leaving-cluster"),
	})
	require.NoError(t, err)
	require.Len(t, beforeRemoval.DBClusters, 1)
	require.Equal(t, "gfix-global-remove", aws.ToString(beforeRemoval.DBClusters[0].GlobalClusterIdentifier),
		"sanity check: the cluster must actually show membership before RemoveFromGlobalCluster can prove it clears it")

	_, err = client.RemoveFromGlobalCluster(t.Context(), &neptunesdk.RemoveFromGlobalClusterInput{
		GlobalClusterIdentifier: aws.String("gfix-global-remove"),
		DbClusterIdentifier:     created.DBCluster.DBClusterArn,
	})
	require.NoError(t, err)

	described, err := client.DescribeDBClusters(t.Context(), &neptunesdk.DescribeDBClustersInput{
		DBClusterIdentifier: aws.String("leaving-cluster"),
	})
	require.NoError(t, err)
	require.Len(t, described.DBClusters, 1)
	assert.Empty(t, aws.ToString(described.DBClusters[0].GlobalClusterIdentifier),
		"GlobalClusterIdentifier should be cleared after RemoveFromGlobalCluster")
}
