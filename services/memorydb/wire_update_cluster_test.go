package memorydb_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	memorydbsdk "github.com/aws/aws-sdk-go-v2/service/memorydb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestUpdateCluster_EngineParameterGroupSecurityGroups_RoundTrip drives
// UpdateCluster through the real SDK client for three members the decode
// struct never recognized (memorydb@v1.36.4 api_op_UpdateCluster.go:44,87,93:
// Engine, ParameterGroupName, SecurityGroupIds are all real
// UpdateClusterInput members): every real client call setting any of them
// silently no-opped. This test fails against unfixed code (see hand-revert
// note in PARITY.md).
func TestUpdateCluster_EngineParameterGroupSecurityGroups_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newMemorydbSDKClient(t, h)
	ctx := t.Context()

	_, err := client.CreateParameterGroup(ctx, &memorydbsdk.CreateParameterGroupInput{
		ParameterGroupName: aws.String("wire-update-pg"),
		Family:             aws.String("memorydb_redis7"),
	})
	require.NoError(t, err)

	_, err = client.CreateCluster(ctx, &memorydbsdk.CreateClusterInput{
		ClusterName: aws.String("wire-update-cluster"),
		NodeType:    aws.String("db.r6g.large"),
		ACLName:     aws.String("open-access"),
	})
	require.NoError(t, err)

	_, err = client.UpdateCluster(ctx, &memorydbsdk.UpdateClusterInput{
		ClusterName:        aws.String("wire-update-cluster"),
		Engine:             aws.String("valkey"),
		ParameterGroupName: aws.String("wire-update-pg"),
		SecurityGroupIds:   []string{"sg-abc123"},
	})
	require.NoError(t, err)

	out, err := client.DescribeClusters(ctx, &memorydbsdk.DescribeClustersInput{
		ClusterName: aws.String("wire-update-cluster"),
	})
	require.NoError(t, err)
	require.Len(t, out.Clusters, 1)

	got := out.Clusters[0]
	assert.Equal(t, "valkey", aws.ToString(got.Engine))
	assert.Equal(t, "wire-update-pg", aws.ToString(got.ParameterGroupName))
	require.Len(t, got.SecurityGroups, 1)
	assert.Equal(t, "sg-abc123", aws.ToString(got.SecurityGroups[0].SecurityGroupId))
}
