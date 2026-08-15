package kafka_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kafkasdk "github.com/aws/aws-sdk-go-v2/service/kafka"
	"github.com/aws/aws-sdk-go-v2/service/kafka/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateReplicator_LogDelivery_RoundTrip drives CreateReplicator through
// a real SDK client with LogDelivery set and proves it reaches
// DescribeReplicator. CreateReplicatorInput.LogDelivery is a real, optional
// request member (kafka@v1.57.2 api_op_CreateReplicator.go) that gopherstack
// previously parsed nowhere -- discarded entirely, never stored, never
// echoed by DescribeReplicator (whose real output also carries it, field-
// diffed against deserializers.go's awsRestjson1_deserializeOpDocument
// DescribeReplicatorOutput, case "logDelivery").
func TestCreateReplicator_LogDelivery_RoundTrip(t *testing.T) {
	t.Parallel()

	h, backend := newTestHandlerWithBackend(t)
	client := newTestKafkaClient(t, h)

	source := backend.AddClusterInternal("log-delivery-source", "3.5.1")
	target := backend.AddClusterInternal("log-delivery-target", "3.5.1")

	created, err := client.CreateReplicator(t.Context(), &kafkasdk.CreateReplicatorInput{
		ReplicatorName:          aws.String("log-delivery-replicator"),
		ServiceExecutionRoleArn: aws.String("arn:aws:iam::123456789012:role/replicator-role"),
		KafkaClusters: []types.KafkaCluster{
			{AmazonMskCluster: &types.AmazonMskCluster{MskClusterArn: aws.String(source.ClusterArn)}},
			{AmazonMskCluster: &types.AmazonMskCluster{MskClusterArn: aws.String(target.ClusterArn)}},
		},
		ReplicationInfoList: []types.ReplicationInfo{
			{
				SourceKafkaClusterArn: aws.String(source.ClusterArn),
				TargetKafkaClusterArn: aws.String(target.ClusterArn),
				TargetCompressionType: types.TargetCompressionTypeNone,
				TopicReplication: &types.TopicReplication{
					TopicsToReplicate: []string{".*"},
				},
				ConsumerGroupReplication: &types.ConsumerGroupReplication{
					ConsumerGroupsToReplicate: []string{".*"},
				},
			},
		},
		LogDelivery: &types.LogDelivery{
			ReplicatorLogDelivery: &types.ReplicatorLogDelivery{
				CloudWatchLogs: &types.ReplicatorCloudWatchLogs{
					Enabled:  aws.Bool(true),
					LogGroup: aws.String("/aws/msk/replicator/log-delivery"),
				},
			},
		},
	})
	require.NoError(t, err)

	described, err := client.DescribeReplicator(t.Context(), &kafkasdk.DescribeReplicatorInput{
		ReplicatorArn: created.ReplicatorArn,
	})
	require.NoError(t, err)
	require.NotNil(t, described.LogDelivery, "LogDelivery was discarded entirely before this fix")
	require.NotNil(t, described.LogDelivery.ReplicatorLogDelivery)
	require.NotNil(t, described.LogDelivery.ReplicatorLogDelivery.CloudWatchLogs)
	assert.True(t, aws.ToBool(described.LogDelivery.ReplicatorLogDelivery.CloudWatchLogs.Enabled))
	assert.Equal(t, "/aws/msk/replicator/log-delivery",
		aws.ToString(described.LogDelivery.ReplicatorLogDelivery.CloudWatchLogs.LogGroup))
}
