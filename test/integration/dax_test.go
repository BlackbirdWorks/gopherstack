package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	daxsdk "github.com/aws/aws-sdk-go-v2/service/dax"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_DAX_ClusterLifecycle exercises the core Amazon DAX
// control-plane workflow via the AWS SDK v2: create a cluster, describe it,
// then delete. Primary integration coverage for the DAX JSON-RPC handler.
func TestIntegration_DAX_ClusterLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createDAXClient(t)
	ctx := t.Context()

	const (
		clusterName = "it-dax-cluster"
		nodeType    = "dax.t3.small"
		roleArn     = "arn:aws:iam::000000000000:role/DAXAccessRole"
	)

	// CreateCluster.
	createOut, err := client.CreateCluster(ctx, &daxsdk.CreateClusterInput{
		ClusterName:       aws.String(clusterName),
		NodeType:          aws.String(nodeType),
		ReplicationFactor: 1,
		IamRoleArn:        aws.String(roleArn),
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.Cluster)
	assert.Equal(t, clusterName, aws.ToString(createOut.Cluster.ClusterName))
	assert.Equal(t, nodeType, aws.ToString(createOut.Cluster.NodeType))

	t.Cleanup(func() {
		_, _ = client.DeleteCluster(ctx, &daxsdk.DeleteClusterInput{
			ClusterName: aws.String(clusterName),
		})
	})

	// DescribeClusters with the new cluster's name.
	descOut, err := client.DescribeClusters(ctx, &daxsdk.DescribeClustersInput{
		ClusterNames: []string{clusterName},
	})
	require.NoError(t, err)
	require.Len(t, descOut.Clusters, 1)
	assert.Equal(t, clusterName, aws.ToString(descOut.Clusters[0].ClusterName))
}
