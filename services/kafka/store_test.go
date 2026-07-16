package kafka_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kafka"
)

const (
	testAccountID = "000000000000"
	testRegion    = "us-east-1"
)

func newTestBackend(t *testing.T) *kafka.InMemoryBackend {
	t.Helper()

	return kafka.NewInMemoryBackend(testAccountID, testRegion)
}

func TestRegion(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	assert.Equal(t, testRegion, b.Region())
}

func TestAccountID(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	assert.Equal(t, testAccountID, b.AccountID())
}

func TestReset(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	b.AddClusterInternal("c1", "2.8.0")
	b.AddConfigurationInternal("cfg1")

	require.Equal(t, 1, kafka.ClusterCount(b))
	require.Equal(t, 1, kafka.ConfigurationCount(b))

	b.Reset()

	assert.Equal(t, 0, kafka.ClusterCount(b))
	assert.Equal(t, 0, kafka.ConfigurationCount(b))
}

func TestMultipleResetCycle(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)

	for range 3 {
		b.AddClusterInternal("c1", "2.8.0")
		b.Reset()
		assert.Equal(t, 0, kafka.ClusterCount(b))
	}
}

func TestSeedHelpers(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)

	cl := b.AddClusterInternal("my-cluster", "2.8.0")
	require.NotNil(t, cl)
	assert.Equal(t, "my-cluster", cl.ClusterName)
	assert.Equal(t, kafka.ClusterStateActive, cl.State)

	cfg := b.AddConfigurationInternal("my-cfg")
	require.NotNil(t, cfg)
	assert.Equal(t, "my-cfg", cfg.Name)

	rep := b.AddReplicatorInternal("my-replicator")
	require.NotNil(t, rep)
	assert.Equal(t, "my-replicator", rep.ReplicatorName)

	topic := b.AddTopicInternal(cl.ClusterArn, "my-topic")
	require.NotNil(t, topic)
	assert.Equal(t, "my-topic", topic.TopicName)

	vpc := b.AddVpcConnectionInternal(cl.ClusterArn, "vpc-12345")
	require.NotNil(t, vpc)
	assert.Equal(t, "vpc-12345", vpc.VpcID)

	op := b.AddClusterOperationInternal(cl.ClusterArn, "UPDATE_BROKER_COUNT")
	require.NotNil(t, op)
	assert.Equal(t, "UPDATE_BROKER_COUNT", op.OperationType)
}

func TestExportCountHelpers(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)

	assert.Equal(t, 0, kafka.ClusterCount(b))
	assert.Equal(t, 0, kafka.ConfigurationCount(b))
	assert.Equal(t, 0, kafka.ReplicatorCount(b))
	assert.Equal(t, 0, kafka.TopicCount(b))
	assert.Equal(t, 0, kafka.VpcConnectionCount(b))
	assert.Equal(t, 0, kafka.ClusterOperationCount(b))
	assert.Equal(t, 0, kafka.ScramSecretCount(b))

	cl := b.AddClusterInternal("c1", "2.8.0")
	b.AddConfigurationInternal("cfg1")
	b.AddReplicatorInternal("rep1")
	b.AddTopicInternal(cl.ClusterArn, "t1")
	b.AddVpcConnectionInternal(cl.ClusterArn, "vpc-1")
	b.AddClusterOperationInternal(cl.ClusterArn, "UPDATE")

	assert.Equal(t, 1, kafka.ClusterCount(b))
	assert.Equal(t, 1, kafka.ConfigurationCount(b))
	assert.Equal(t, 1, kafka.ReplicatorCount(b))
	assert.Equal(t, 1, kafka.TopicCount(b))
	assert.Equal(t, 1, kafka.VpcConnectionCount(b))
	assert.Equal(t, 1, kafka.ClusterOperationCount(b))
}
