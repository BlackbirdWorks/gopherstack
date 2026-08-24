package dax_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	daxsdk "github.com/aws/aws-sdk-go-v2/service/dax"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dax"
)

// TestDecreaseReplicationFactor_AvailabilityZones_SDKRoundTrip proves that
// DecreaseReplicationFactorInput.AvailabilityZones is honored: gopherstack's
// decode struct previously dropped it entirely, so a client selecting nodes
// to remove by AZ (rather than by NodeIdsToRemove) always fell through to
// removing whichever nodes happened to be last in the cluster's internal
// node list -- silently ignoring the client's actual selection.
func TestDecreaseReplicationFactor_AvailabilityZones_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := dax.NewInMemoryBackend("123456789012", "us-east-1")
	h := dax.NewHandler(backend)
	client := newTestDAXSDKClient(t, h)
	ctx := t.Context()

	const clusterName = "az-decrease-cluster"

	_, err := client.CreateCluster(ctx, &daxsdk.CreateClusterInput{
		ClusterName:       aws.String(clusterName),
		NodeType:          aws.String("dax.r4.large"),
		IamRoleArn:        aws.String("arn:aws:iam::123456789012:role/dax"),
		ReplicationFactor: 3,
		AvailabilityZones: []string{"us-east-1a", "us-east-1b", "us-east-1c"},
	})
	require.NoError(t, err)

	// Target the node in us-east-1b for removal by AZ, not by node ID.
	decOut, err := client.DecreaseReplicationFactor(ctx, &daxsdk.DecreaseReplicationFactorInput{
		ClusterName:          aws.String(clusterName),
		NewReplicationFactor: 2,
		AvailabilityZones:    []string{"us-east-1b"},
	})
	require.NoError(t, err)
	require.NotNil(t, decOut.Cluster)

	remainingAZs := make([]string, 0, len(decOut.Cluster.Nodes))
	for _, n := range decOut.Cluster.Nodes {
		remainingAZs = append(remainingAZs, aws.ToString(n.AvailabilityZone))
	}

	require.ElementsMatch(t, []string{"us-east-1a", "us-east-1c"}, remainingAZs,
		"DecreaseReplicationFactorInput.AvailabilityZones must select which node is removed")
}
