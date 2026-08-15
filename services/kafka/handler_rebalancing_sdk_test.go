package kafka_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kafkasdk "github.com/aws/aws-sdk-go-v2/service/kafka"
	"github.com/aws/aws-sdk-go-v2/service/kafka/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kafka"
)

// TestUpdateRebalancing_RoundTrip drives UpdateRebalancing through a real SDK
// client and proves CurrentVersion is enforced (optimistic-lock guard, like
// every sibling Update op) and Rebalancing.Status is actually persisted and
// echoed back by DescribeClusterV2, instead of the pre-fix behavior where
// both were dropped and a false comment claimed AWS exposes no per-field
// rebalancing configuration (gopherstack-h910).
func TestUpdateRebalancing_RoundTrip(t *testing.T) {
	t.Parallel()

	newClusterClient := func(t *testing.T) (*kafkasdk.Client, string, string) {
		t.Helper()

		h := kafka.NewHandler(kafka.NewInMemoryBackend("123456789012", "us-east-1"))
		client := newTestKafkaClient(t, h)

		created, err := client.CreateCluster(t.Context(), &kafkasdk.CreateClusterInput{
			ClusterName:         aws.String("rebalancing-cluster"),
			KafkaVersion:        aws.String("3.5.1"),
			NumberOfBrokerNodes: aws.Int32(3),
			BrokerNodeGroupInfo: &types.BrokerNodeGroupInfo{
				ClientSubnets: []string{"subnet-1", "subnet-2"},
				InstanceType:  aws.String("kafka.m5.large"),
			},
		})
		require.NoError(t, err)

		described, err := client.DescribeClusterV2(t.Context(), &kafkasdk.DescribeClusterV2Input{
			ClusterArn: created.ClusterArn,
		})
		require.NoError(t, err)

		return client, aws.ToString(created.ClusterArn), aws.ToString(described.ClusterInfo.CurrentVersion)
	}

	t.Run("stale_current_version_errors", func(t *testing.T) {
		t.Parallel()

		client, clusterArn, _ := newClusterClient(t)

		_, err := client.UpdateRebalancing(t.Context(), &kafkasdk.UpdateRebalancingInput{
			ClusterArn:     aws.String(clusterArn),
			CurrentVersion: aws.String("stale-version"),
			Rebalancing:    &types.Rebalancing{Status: types.RebalancingStatusPaused},
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "BadRequestException")
	})

	t.Run("status_persists_and_is_echoed", func(t *testing.T) {
		t.Parallel()

		client, clusterArn, currentVersion := newClusterClient(t)

		_, err := client.UpdateRebalancing(t.Context(), &kafkasdk.UpdateRebalancingInput{
			ClusterArn:     aws.String(clusterArn),
			CurrentVersion: aws.String(currentVersion),
			Rebalancing:    &types.Rebalancing{Status: types.RebalancingStatusPaused},
		})
		require.NoError(t, err)

		described, err := client.DescribeClusterV2(t.Context(), &kafkasdk.DescribeClusterV2Input{
			ClusterArn: aws.String(clusterArn),
		})
		require.NoError(t, err)
		require.NotNil(t, described.ClusterInfo.Provisioned)
		require.NotNil(t, described.ClusterInfo.Provisioned.Rebalancing)
		assert.Equal(t, types.RebalancingStatusPaused, described.ClusterInfo.Provisioned.Rebalancing.Status)
	})
}
