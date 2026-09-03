package kafka_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kafkasdk "github.com/aws/aws-sdk-go-v2/service/kafka"
	"github.com/aws/aws-sdk-go-v2/service/kafka/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kafka"
)

func newTestClusterForErrorSweep(t *testing.T, b *kafka.InMemoryBackend) string {
	t.Helper()

	c, err := b.CreateCluster(
		context.Background(),
		"error-sweep-cluster",
		"3.5.1",
		2,
		kafka.BrokerNodeGroupInfo{InstanceType: "kafka.m5.large"},
		nil,
		nil,
	)
	require.NoError(t, err)

	return c.ClusterArn
}

// Real AWS: CreateTopic's own error switch models the specific
// TopicExistsException in addition to the generic ConflictException -- a
// duplicate topic name must raise the specific type, not the generic one.
func Test_SDKRoundTrip_CreateTopic_DuplicateName_TopicExistsException(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend("123456789012", "us-east-1")
	h := kafka.NewHandler(b)
	client := newTestKafkaClient(t, h)
	clusterArn := newTestClusterForErrorSweep(t, b)

	in := &kafkasdk.CreateTopicInput{
		ClusterArn:        aws.String(clusterArn),
		TopicName:         aws.String("dup-topic"),
		PartitionCount:    aws.Int32(1),
		ReplicationFactor: aws.Int32(1),
	}

	_, err := client.CreateTopic(t.Context(), in)
	require.NoError(t, err)

	_, err = client.CreateTopic(t.Context(), in)
	require.Error(t, err)

	var te *types.TopicExistsException
	require.ErrorAs(t, err, &te, "expected a real TopicExistsException from the SDK deserializer")
}

// Real AWS: DeleteTopic's own error switch models the specific
// UnknownTopicOrPartitionException (the real Kafka protocol's own name for a
// missing topic) in addition to the generic NotFoundException -- deleting an
// unknown topic on a cluster that exists must raise the specific type.
func Test_SDKRoundTrip_DeleteTopic_UnknownTopic_UnknownTopicOrPartitionException(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend("123456789012", "us-east-1")
	h := kafka.NewHandler(b)
	client := newTestKafkaClient(t, h)
	clusterArn := newTestClusterForErrorSweep(t, b)

	_, err := client.DeleteTopic(t.Context(), &kafkasdk.DeleteTopicInput{
		ClusterArn: aws.String(clusterArn),
		TopicName:  aws.String("no-such-topic"),
	})
	require.Error(t, err)

	var ue *types.UnknownTopicOrPartitionException
	require.ErrorAs(t, err, &ue, "expected a real UnknownTopicOrPartitionException from the SDK deserializer")
}

// Real AWS: UpdateTopic's own error switch models the specific
// UnknownTopicOrPartitionException in addition to the generic
// NotFoundException.
func Test_SDKRoundTrip_UpdateTopic_UnknownTopic_UnknownTopicOrPartitionException(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend("123456789012", "us-east-1")
	h := kafka.NewHandler(b)
	client := newTestKafkaClient(t, h)
	clusterArn := newTestClusterForErrorSweep(t, b)

	_, err := client.UpdateTopic(t.Context(), &kafkasdk.UpdateTopicInput{
		ClusterArn:     aws.String(clusterArn),
		TopicName:      aws.String("no-such-topic"),
		PartitionCount: aws.Int32(3),
	})
	require.Error(t, err)

	var ue *types.UnknownTopicOrPartitionException
	require.ErrorAs(t, err, &ue, "expected a real UnknownTopicOrPartitionException from the SDK deserializer")
}
