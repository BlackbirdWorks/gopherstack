package kafka_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kafkasdk "github.com/aws/aws-sdk-go-v2/service/kafka"
	"github.com/aws/aws-sdk-go-v2/service/kafka/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReplicator_ApacheKafkaCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		sourceID string
		wantErr  bool
	}{
		{name: "update by cluster id", sourceID: "apache-src"},
		{name: "unknown cluster id", sourceID: "other", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, b := newMemberClient(t)
			target := b.AddClusterInternal("msk-target", "3.5.1")

			created, err := client.CreateReplicator(t.Context(), &kafkasdk.CreateReplicatorInput{
				ReplicatorName:          aws.String("apache-rep"),
				ServiceExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/r"),
				KafkaClusters: []types.KafkaCluster{
					{
						ApacheKafkaCluster: &types.ApacheKafkaCluster{
							ApacheKafkaClusterId:  aws.String("apache-src"),
							BootstrapBrokerString: aws.String("b-1:9094"),
						},
						ClientAuthentication: &types.KafkaClusterClientAuthentication{
							SaslScram: &types.KafkaClusterSaslScramAuthentication{
								Mechanism: types.KafkaClusterSaslScramMechanismSha512,
								SecretArn: aws.String("arn:aws:secretsmanager:us-east-1:000000000000:secret:s"),
							},
						},
						EncryptionInTransit: &types.KafkaClusterEncryptionInTransit{
							EncryptionType: types.KafkaClusterEncryptionInTransitTypeTls,
						},
						VpcConfig: &types.KafkaClusterClientVpcConfig{SubnetIds: []string{"subnet-1"}},
					},
					{
						AmazonMskCluster: &types.AmazonMskCluster{MskClusterArn: aws.String(target.ClusterArn)},
						VpcConfig:        &types.KafkaClusterClientVpcConfig{SubnetIds: []string{"subnet-2"}},
					},
				},
				ReplicationInfoList: []types.ReplicationInfo{{
					SourceKafkaClusterId:  aws.String("apache-src"),
					TargetKafkaClusterArn: aws.String(target.ClusterArn),
					TargetCompressionType: types.TargetCompressionTypeNone,
					TopicReplication:      &types.TopicReplication{TopicsToReplicate: []string{".*"}},
					ConsumerGroupReplication: &types.ConsumerGroupReplication{
						ConsumerGroupsToReplicate: []string{".*"},
					},
				}},
			})
			require.NoError(t, err)

			got, err := client.DescribeReplicator(
				t.Context(),
				&kafkasdk.DescribeReplicatorInput{ReplicatorArn: created.ReplicatorArn},
			)
			require.NoError(t, err)
			require.Len(t, got.KafkaClusters, 2)
			require.NotNil(t, got.KafkaClusters[0].ApacheKafkaCluster)
			assert.Equal(t, "apache-src", aws.ToString(got.KafkaClusters[0].ApacheKafkaCluster.ApacheKafkaClusterId))
			assert.Equal(t, "b-1:9094", aws.ToString(got.KafkaClusters[0].ApacheKafkaCluster.BootstrapBrokerString))
			assert.Nil(t, got.KafkaClusters[0].AmazonMskCluster)
			require.NotNil(t, got.KafkaClusters[0].ClientAuthentication.SaslScram)
			assert.Equal(
				t,
				types.KafkaClusterSaslScramMechanismSha512,
				got.KafkaClusters[0].ClientAuthentication.SaslScram.Mechanism,
			)
			assert.Equal(
				t,
				types.KafkaClusterEncryptionInTransitTypeTls,
				got.KafkaClusters[0].EncryptionInTransit.EncryptionType,
			)
			assert.Equal(t, "apache-src", aws.ToString(got.KafkaClusters[0].KafkaClusterAlias))
			assert.Equal(t, "apache-src", aws.ToString(got.ReplicationInfoList[0].SourceKafkaClusterAlias))
			assert.Equal(t, "msk-target", aws.ToString(got.ReplicationInfoList[0].TargetKafkaClusterAlias))

			_, err = client.UpdateReplicationInfo(t.Context(), &kafkasdk.UpdateReplicationInfoInput{
				ReplicatorArn:         created.ReplicatorArn,
				CurrentVersion:        got.CurrentVersion,
				SourceKafkaClusterId:  aws.String(tt.sourceID),
				TargetKafkaClusterArn: aws.String(target.ClusterArn),
				TopicReplication: &types.TopicReplicationUpdate{
					TopicsToReplicate:               []string{"orders.*"},
					TopicsToExclude:                 []string{},
					CopyAccessControlListsForTopics: aws.Bool(true),
					CopyTopicConfigurations:         aws.Bool(true),
					DetectAndCopyNewTopics:          aws.Bool(true),
				},
			})
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			after, err := client.DescribeReplicator(
				t.Context(),
				&kafkasdk.DescribeReplicatorInput{ReplicatorArn: created.ReplicatorArn},
			)
			require.NoError(t, err)
			assert.Equal(t, []string{"orders.*"}, after.ReplicationInfoList[0].TopicReplication.TopicsToReplicate)
		})
	}
}
