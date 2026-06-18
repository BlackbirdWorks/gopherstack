package kafka //nolint:testpackage // needs access to the unexported region context key.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ctxRegion returns a context carrying the given AWS region under regionContextKey,
// mirroring what the HTTP handler injects from the SigV4 credential scope.
func ctxRegion(region string) context.Context {
	return context.WithValue(context.Background(), regionContextKey{}, region)
}

func newKafkaBrokerInfo() BrokerNodeGroupInfo {
	return BrokerNodeGroupInfo{
		InstanceType:  "kafka.m5.large",
		ClientSubnets: []string{"subnet-00000000"},
	}
}

// TestKafkaClusterRegionIsolation proves that same-named clusters created in two
// regions are fully isolated: each region sees only its own cluster, ARNs carry
// the correct region, and deleting in one region leaves the other intact.
func TestKafkaClusterRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := ctxRegion("us-east-1")
	ctxWest := ctxRegion("us-west-2")

	// 1. Create a cluster named "shared" in us-east-1.
	eastCluster, err := backend.CreateCluster(
		ctxEast, "shared", "3.5.1", 3, newKafkaBrokerInfo(), nil, map[string]string{"env": "east"},
	)
	require.NoError(t, err)
	assert.Contains(t, eastCluster.ClusterArn, "us-east-1")

	// 2. Create a cluster with the SAME NAME in us-west-2; no conflict across regions.
	westCluster, err := backend.CreateCluster(
		ctxWest, "shared", "3.6.0", 2, newKafkaBrokerInfo(), nil, map[string]string{"env": "west"},
	)
	require.NoError(t, err)
	assert.Contains(t, westCluster.ClusterArn, "us-west-2")
	assert.NotEqual(t, eastCluster.ClusterArn, westCluster.ClusterArn)

	// 3. Each region lists only its own cluster with its own attributes.
	eastList := backend.ListClusters(ctxEast)
	require.Len(t, eastList, 1)
	assert.Equal(t, "shared", eastList[0].ClusterName)
	assert.Equal(t, "3.5.1", eastList[0].KafkaVersion)
	assert.Contains(t, eastList[0].ClusterArn, "us-east-1")

	westList := backend.ListClusters(ctxWest)
	require.Len(t, westList, 1)
	assert.Equal(t, "shared", westList[0].ClusterName)
	assert.Equal(t, "3.6.0", westList[0].KafkaVersion)
	assert.Contains(t, westList[0].ClusterArn, "us-west-2")

	// 4. Describe-by-ARN resolves the region from the ARN regardless of ctx region.
	got, err := backend.DescribeCluster(ctxEast, westCluster.ClusterArn)
	require.NoError(t, err)
	assert.Equal(t, "3.6.0", got.KafkaVersion)

	// 5. The east cluster ARN is not visible from the west region's nested store
	//    when looked up with a mismatched ctx — ARN region wins, so it still resolves.
	got, err = backend.DescribeCluster(ctxWest, eastCluster.ClusterArn)
	require.NoError(t, err)
	assert.Equal(t, "3.5.1", got.KafkaVersion)

	// 6. Deleting in us-east-1 leaves us-west-2 intact.
	require.NoError(t, backend.DeleteCluster(ctxEast, eastCluster.ClusterArn))

	assert.Empty(t, backend.ListClusters(ctxEast))
	assert.Len(t, backend.ListClusters(ctxWest), 1)

	// The deleted east cluster is gone; the west cluster is still describable.
	_, err = backend.DescribeCluster(ctxEast, eastCluster.ClusterArn)
	require.ErrorIs(t, err, ErrNotFound)

	_, err = backend.DescribeCluster(ctxWest, westCluster.ClusterArn)
	require.NoError(t, err)
}

// TestKafkaTopicAndPolicyRegionIsolation proves that resources hanging off a
// cluster (topics, cluster policies) and tags are isolated per region: a topic
// or policy created against a cluster in one region is invisible from the other.
func TestKafkaTopicAndPolicyRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := ctxRegion("us-east-1")
	ctxWest := ctxRegion("us-west-2")

	eastCluster, err := backend.CreateCluster(
		ctxEast, "c1", "3.5.1", 3, newKafkaBrokerInfo(), nil, nil,
	)
	require.NoError(t, err)

	westCluster, err := backend.CreateCluster(
		ctxWest, "c1", "3.5.1", 3, newKafkaBrokerInfo(), nil, nil,
	)
	require.NoError(t, err)

	// Topic created on the east cluster (region resolved from the cluster ARN).
	_, err = backend.CreateTopic(ctxEast, eastCluster.ClusterArn, "orders", 3, 6, nil)
	require.NoError(t, err)

	eastTopics, err := backend.ListTopics(ctxEast, eastCluster.ClusterArn)
	require.NoError(t, err)
	assert.Len(t, eastTopics, 1)

	// The west cluster (same name) has no topics.
	westTopics, err := backend.ListTopics(ctxWest, westCluster.ClusterArn)
	require.NoError(t, err)
	assert.Empty(t, westTopics)

	// Cluster policy isolation.
	require.NoError(t, backend.PutClusterPolicy(ctxEast, eastCluster.ClusterArn, `{"east":true}`))

	_, err = backend.GetClusterPolicy(ctxWest, westCluster.ClusterArn)
	require.ErrorIs(t, err, ErrNotFound)

	policy, err := backend.GetClusterPolicy(ctxEast, eastCluster.ClusterArn)
	require.NoError(t, err)
	assert.Equal(t, `{"east":true}`, policy)

	// Tag isolation: tagging the east cluster does not leak to the west cluster.
	require.NoError(t, backend.TagResource(ctxEast, eastCluster.ClusterArn, map[string]string{"team": "data"}))

	westTags, err := backend.GetTags(ctxWest, westCluster.ClusterArn)
	require.NoError(t, err)
	assert.Empty(t, westTags)

	eastTags, err := backend.GetTags(ctxEast, eastCluster.ClusterArn)
	require.NoError(t, err)
	assert.Equal(t, "data", eastTags["team"])
}

// TestKafkaConfigurationRegionIsolation proves configurations (which are not tied
// to a cluster) are isolated per request-context region.
func TestKafkaConfigurationRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := ctxRegion("us-east-1")
	ctxWest := ctxRegion("us-west-2")

	eastCfg, err := backend.CreateConfiguration(ctxEast, "cfg", "east cfg", []string{"3.5.1"}, "auto.create=true")
	require.NoError(t, err)
	assert.Contains(t, eastCfg.Arn, "us-east-1")

	westCfg, err := backend.CreateConfiguration(ctxWest, "cfg", "west cfg", []string{"3.6.0"}, "auto.create=false")
	require.NoError(t, err)
	assert.Contains(t, westCfg.Arn, "us-west-2")

	assert.Len(t, backend.ListConfigurations(ctxEast), 1)
	assert.Len(t, backend.ListConfigurations(ctxWest), 1)

	require.NoError(t, backend.DeleteConfiguration(ctxEast, eastCfg.Arn))
	assert.Empty(t, backend.ListConfigurations(ctxEast))
	assert.Len(t, backend.ListConfigurations(ctxWest), 1)
}
