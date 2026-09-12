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

// TestTypedSlice35RealClient drives kafka's typed-coverage-blind ops
// (gopherstack-n3zi slice 35) through the real aws-sdk-go-v2 client.
func TestTypedSlice35RealClient(t *testing.T) {
	t.Parallel()

	newClient := func(t *testing.T) *kafkasdk.Client {
		t.Helper()

		h := kafka.NewHandler(kafka.NewInMemoryBackend("123456789012", "us-east-1"))

		return newTestKafkaClient(t, h)
	}

	createCluster := func(t *testing.T, client *kafkasdk.Client, name string) *string {
		t.Helper()

		out, err := client.CreateCluster(t.Context(), &kafkasdk.CreateClusterInput{
			ClusterName:         aws.String(name),
			KafkaVersion:        aws.String("3.5.1"),
			NumberOfBrokerNodes: aws.Int32(3),
			BrokerNodeGroupInfo: &types.BrokerNodeGroupInfo{
				ClientSubnets: []string{"subnet-1", "subnet-2"},
				InstanceType:  aws.String("kafka.m5.large"),
			},
		})
		require.NoError(t, err)

		return out.ClusterArn
	}

	currentVersion := func(t *testing.T, client *kafkasdk.Client, clusterArn *string) *string {
		t.Helper()

		out, err := client.DescribeCluster(t.Context(), &kafkasdk.DescribeClusterInput{ClusterArn: clusterArn})
		require.NoError(t, err)

		return out.ClusterInfo.CurrentVersion
	}

	t.Run("cluster update ops", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)
		ctx := t.Context()
		clusterArn := createCluster(t, client, "s35-update-cluster")

		rebootOut, err := client.RebootBroker(ctx, &kafkasdk.RebootBrokerInput{
			ClusterArn: clusterArn,
			BrokerIds:  []string{"1"},
		})
		require.NoError(t, err)
		assert.NotEmpty(t, aws.ToString(rebootOut.ClusterOperationArn))

		storageOut, err := client.UpdateBrokerStorage(ctx, &kafkasdk.UpdateBrokerStorageInput{
			ClusterArn:     clusterArn,
			CurrentVersion: currentVersion(t, client, clusterArn),
			TargetBrokerEBSVolumeInfo: []types.BrokerEBSVolumeInfo{
				{KafkaBrokerNodeId: aws.String("All"), VolumeSizeGB: aws.Int32(200)},
			},
		})
		require.NoError(t, err)
		assert.NotEmpty(t, aws.ToString(storageOut.ClusterOperationArn))

		typeOut, err := client.UpdateBrokerType(ctx, &kafkasdk.UpdateBrokerTypeInput{
			ClusterArn:         clusterArn,
			CurrentVersion:     currentVersion(t, client, clusterArn),
			TargetInstanceType: aws.String("kafka.m5.xlarge"),
		})
		require.NoError(t, err)
		assert.NotEmpty(t, aws.ToString(typeOut.ClusterOperationArn))

		configOut, err := client.CreateConfiguration(ctx, &kafkasdk.CreateConfigurationInput{
			Name:             aws.String("s35-config"),
			ServerProperties: []byte("auto.create.topics.enable=true"),
			KafkaVersions:    []string{"3.5.1"},
		})
		require.NoError(t, err)

		clusterConfigOut, err := client.UpdateClusterConfiguration(ctx, &kafkasdk.UpdateClusterConfigurationInput{
			ClusterArn:     clusterArn,
			CurrentVersion: currentVersion(t, client, clusterArn),
			ConfigurationInfo: &types.ConfigurationInfo{
				Arn:      configOut.Arn,
				Revision: aws.Int64(1),
			},
		})
		require.NoError(t, err)
		assert.NotEmpty(t, aws.ToString(clusterConfigOut.ClusterOperationArn))

		versionOut, err := client.UpdateClusterKafkaVersion(ctx, &kafkasdk.UpdateClusterKafkaVersionInput{
			ClusterArn:         clusterArn,
			CurrentVersion:     currentVersion(t, client, clusterArn),
			TargetKafkaVersion: aws.String("3.6.0"),
		})
		require.NoError(t, err)
		assert.NotEmpty(t, aws.ToString(versionOut.ClusterOperationArn))

		connOut, err := client.UpdateConnectivity(ctx, &kafkasdk.UpdateConnectivityInput{
			ClusterArn:     clusterArn,
			CurrentVersion: currentVersion(t, client, clusterArn),
			ConnectivityInfo: &types.ConnectivityInfo{
				PublicAccess: &types.PublicAccess{Type: aws.String("DISABLED")},
			},
		})
		require.NoError(t, err)
		assert.NotEmpty(t, aws.ToString(connOut.ClusterOperationArn))

		monOut, err := client.UpdateMonitoring(ctx, &kafkasdk.UpdateMonitoringInput{
			ClusterArn:         clusterArn,
			CurrentVersion:     currentVersion(t, client, clusterArn),
			EnhancedMonitoring: types.EnhancedMonitoringPerBroker,
		})
		require.NoError(t, err)
		assert.NotEmpty(t, aws.ToString(monOut.ClusterOperationArn))

		secOut, err := client.UpdateSecurity(ctx, &kafkasdk.UpdateSecurityInput{
			ClusterArn:     clusterArn,
			CurrentVersion: currentVersion(t, client, clusterArn),
			ClientAuthentication: &types.ClientAuthentication{
				Unauthenticated: &types.Unauthenticated{Enabled: aws.Bool(true)},
			},
		})
		require.NoError(t, err)
		assert.NotEmpty(t, aws.ToString(secOut.ClusterOperationArn))
	})

	t.Run("cluster listing v2 and operations", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)
		ctx := t.Context()
		clusterArn := createCluster(t, client, "s35-listing-cluster")

		_, err := client.UpdateClusterKafkaVersion(ctx, &kafkasdk.UpdateClusterKafkaVersionInput{
			ClusterArn:         clusterArn,
			CurrentVersion:     currentVersion(t, client, clusterArn),
			TargetKafkaVersion: aws.String("3.6.0"),
		})
		require.NoError(t, err)

		listOut, err := client.ListClustersV2(ctx, &kafkasdk.ListClustersV2Input{})
		require.NoError(t, err)
		require.NotEmpty(t, listOut.ClusterInfoList)

		var found bool
		for _, c := range listOut.ClusterInfoList {
			if aws.ToString(c.ClusterArn) == aws.ToString(clusterArn) {
				found = true
			}
		}
		assert.True(t, found)

		opsOut, err := client.ListClusterOperations(ctx, &kafkasdk.ListClusterOperationsInput{
			ClusterArn: clusterArn,
		})
		require.NoError(t, err)
		require.NotEmpty(t, opsOut.ClusterOperationInfoList)
		assert.NotEmpty(t, aws.ToString(opsOut.ClusterOperationInfoList[0].OperationType))
	})

	t.Run("configuration update and revision", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)
		ctx := t.Context()

		createOut, err := client.CreateConfiguration(ctx, &kafkasdk.CreateConfigurationInput{
			Name:             aws.String("s35-update-config"),
			ServerProperties: []byte("num.partitions=3"),
			KafkaVersions:    []string{"3.5.1"},
		})
		require.NoError(t, err)
		require.NotNil(t, createOut.LatestRevision)
		require.NotNil(t, createOut.LatestRevision.CreationTime, "LatestRevision.CreationTime must round-trip")

		updateOut, err := client.UpdateConfiguration(ctx, &kafkasdk.UpdateConfigurationInput{
			Arn:              createOut.Arn,
			ServerProperties: []byte("num.partitions=6"),
		})
		require.NoError(t, err)
		require.NotNil(t, updateOut.LatestRevision)
		assert.NotNil(t, updateOut.LatestRevision.CreationTime)

		revOut, err := client.DescribeConfigurationRevision(ctx, &kafkasdk.DescribeConfigurationRevisionInput{
			Arn:      createOut.Arn,
			Revision: aws.Int64(1),
		})
		require.NoError(t, err)
		assert.Equal(t, aws.ToString(createOut.Arn), aws.ToString(revOut.Arn))
		assert.NotNil(t, revOut.CreationTime)
	})

	t.Run("cluster policy", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)
		ctx := t.Context()
		clusterArn := createCluster(t, client, "s35-policy-cluster")

		policy := `{"Version":"2012-10-17","Statement":[]}`

		_, err := client.PutClusterPolicy(ctx, &kafkasdk.PutClusterPolicyInput{
			ClusterArn: clusterArn,
			Policy:     aws.String(policy),
		})
		require.NoError(t, err)

		getOut, err := client.GetClusterPolicy(ctx, &kafkasdk.GetClusterPolicyInput{
			ClusterArn: clusterArn,
		})
		require.NoError(t, err)
		assert.Equal(t, policy, aws.ToString(getOut.Policy))

		_, err = client.DeleteClusterPolicy(ctx, &kafkasdk.DeleteClusterPolicyInput{
			ClusterArn: clusterArn,
		})
		require.NoError(t, err)

		_, err = client.GetClusterPolicy(ctx, &kafkasdk.GetClusterPolicyInput{
			ClusterArn: clusterArn,
		})
		require.Error(t, err)
	})

	t.Run("scram secrets", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)
		ctx := t.Context()
		clusterArn := createCluster(t, client, "s35-scram-cluster")

		secretArn := "arn:aws:secretsmanager:us-east-1:123456789012:secret:AmazonMSK_s35-secret"

		assocOut, err := client.BatchAssociateScramSecret(ctx, &kafkasdk.BatchAssociateScramSecretInput{
			ClusterArn:    clusterArn,
			SecretArnList: []string{secretArn},
		})
		require.NoError(t, err)
		assert.Empty(t, assocOut.UnprocessedScramSecrets)

		listOut, err := client.ListScramSecrets(ctx, &kafkasdk.ListScramSecretsInput{
			ClusterArn: clusterArn,
		})
		require.NoError(t, err)
		assert.Equal(t, []string{secretArn}, listOut.SecretArnList)

		disassocOut, err := client.BatchDisassociateScramSecret(ctx, &kafkasdk.BatchDisassociateScramSecretInput{
			ClusterArn:    clusterArn,
			SecretArnList: []string{secretArn},
		})
		require.NoError(t, err)
		assert.Empty(t, disassocOut.UnprocessedScramSecrets)

		listOut2, err := client.ListScramSecrets(ctx, &kafkasdk.ListScramSecretsInput{
			ClusterArn: clusterArn,
		})
		require.NoError(t, err)
		assert.Empty(t, listOut2.SecretArnList)
	})

	t.Run("topics", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)
		ctx := t.Context()
		clusterArn := createCluster(t, client, "s35-topics-cluster")

		_, err := client.CreateTopic(ctx, &kafkasdk.CreateTopicInput{
			ClusterArn:        clusterArn,
			TopicName:         aws.String("s35-topic"),
			PartitionCount:    aws.Int32(3),
			ReplicationFactor: aws.Int32(2),
		})
		require.NoError(t, err)

		descOut, err := client.DescribeTopic(ctx, &kafkasdk.DescribeTopicInput{
			ClusterArn: clusterArn,
			TopicName:  aws.String("s35-topic"),
		})
		require.NoError(t, err)
		assert.Equal(t, int32(3), aws.ToInt32(descOut.PartitionCount))
		assert.Equal(t, int32(2), aws.ToInt32(descOut.ReplicationFactor))

		partOut, err := client.DescribeTopicPartitions(ctx, &kafkasdk.DescribeTopicPartitionsInput{
			ClusterArn: clusterArn,
			TopicName:  aws.String("s35-topic"),
		})
		require.NoError(t, err)
		require.Len(t, partOut.Partitions, 3)
		assert.NotEmpty(t, partOut.Partitions[0].Replicas)

		listOut, err := client.ListTopics(ctx, &kafkasdk.ListTopicsInput{
			ClusterArn: clusterArn,
		})
		require.NoError(t, err)
		require.Len(t, listOut.Topics, 1)
		assert.Equal(t, "s35-topic", aws.ToString(listOut.Topics[0].TopicName))
	})

	t.Run("replicator", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)
		ctx := t.Context()

		sourceArn := createCluster(t, client, "s35-repl-source")
		targetArn := createCluster(t, client, "s35-repl-target")

		createOut, err := client.CreateReplicator(ctx, &kafkasdk.CreateReplicatorInput{
			ReplicatorName:          aws.String("s35-replicator"),
			ServiceExecutionRoleArn: aws.String("arn:aws:iam::123456789012:role/s35-role"),
			KafkaClusters: []types.KafkaCluster{
				{
					AmazonMskCluster: &types.AmazonMskCluster{MskClusterArn: sourceArn},
					VpcConfig:        &types.KafkaClusterClientVpcConfig{SubnetIds: []string{"subnet-1"}},
				},
				{
					AmazonMskCluster: &types.AmazonMskCluster{MskClusterArn: targetArn},
					VpcConfig:        &types.KafkaClusterClientVpcConfig{SubnetIds: []string{"subnet-2"}},
				},
			},
			ReplicationInfoList: []types.ReplicationInfo{
				{
					SourceKafkaClusterArn: sourceArn,
					TargetKafkaClusterArn: targetArn,
					TargetCompressionType: types.TargetCompressionTypeNone,
					TopicReplication: &types.TopicReplication{
						TopicsToReplicate: []string{".*"},
					},
					ConsumerGroupReplication: &types.ConsumerGroupReplication{
						ConsumerGroupsToReplicate: []string{".*"},
					},
				},
			},
		})
		require.NoError(t, err)
		require.NotEmpty(t, aws.ToString(createOut.ReplicatorArn))

		listOut, err := client.ListReplicators(ctx, &kafkasdk.ListReplicatorsInput{})
		require.NoError(t, err)
		require.NotEmpty(t, listOut.Replicators)

		descBefore, err := client.DescribeReplicator(ctx, &kafkasdk.DescribeReplicatorInput{
			ReplicatorArn: createOut.ReplicatorArn,
		})
		require.NoError(t, err)

		updateOut, err := client.UpdateReplicationInfo(ctx, &kafkasdk.UpdateReplicationInfoInput{
			ReplicatorArn:         createOut.ReplicatorArn,
			CurrentVersion:        descBefore.CurrentVersion,
			SourceKafkaClusterArn: sourceArn,
			TargetKafkaClusterArn: targetArn,
			ConsumerGroupReplication: &types.ConsumerGroupReplicationUpdate{
				ConsumerGroupsToReplicate:       []string{".*", "s35-.*"},
				ConsumerGroupsToExclude:         []string{"s35-excluded"},
				DetectAndCopyNewConsumerGroups:  aws.Bool(true),
				SynchroniseConsumerGroupOffsets: aws.Bool(false),
			},
		})
		require.NoError(t, err)
		assert.NotEmpty(t, aws.ToString(updateOut.ReplicatorArn))

		deleteOut, err := client.DeleteReplicator(ctx, &kafkasdk.DeleteReplicatorInput{
			ReplicatorArn: createOut.ReplicatorArn,
		})
		require.NoError(t, err)
		assert.Equal(t, aws.ToString(createOut.ReplicatorArn), aws.ToString(deleteOut.ReplicatorArn))
		assert.NotEmpty(t, deleteOut.ReplicatorState)
	})

	t.Run("vpc connections", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)
		ctx := t.Context()
		clusterArn := createCluster(t, client, "s35-vpcconn-cluster")

		createOut, err := client.CreateVpcConnection(ctx, &kafkasdk.CreateVpcConnectionInput{
			TargetClusterArn: clusterArn,
			VpcId:            aws.String("vpc-s35"),
			Authentication:   aws.String("SASL_IAM"),
			ClientSubnets:    []string{"subnet-1", "subnet-2"},
			SecurityGroups:   []string{"sg-1"},
		})
		require.NoError(t, err)
		vpcConnArn := createOut.VpcConnectionArn

		descOut, err := client.DescribeVpcConnection(ctx, &kafkasdk.DescribeVpcConnectionInput{
			Arn: vpcConnArn,
		})
		require.NoError(t, err)
		assert.Equal(t, aws.ToString(clusterArn), aws.ToString(descOut.TargetClusterArn))

		listOut, err := client.ListVpcConnections(ctx, &kafkasdk.ListVpcConnectionsInput{})
		require.NoError(t, err)
		require.NotEmpty(t, listOut.VpcConnections)

		clientListOut, err := client.ListClientVpcConnections(ctx, &kafkasdk.ListClientVpcConnectionsInput{
			ClusterArn: clusterArn,
		})
		require.NoError(t, err)
		require.Len(t, clientListOut.ClientVpcConnections, 1)
		assert.Equal(t, aws.ToString(vpcConnArn), aws.ToString(clientListOut.ClientVpcConnections[0].VpcConnectionArn))

		_, err = client.RejectClientVpcConnection(ctx, &kafkasdk.RejectClientVpcConnectionInput{
			ClusterArn:       clusterArn,
			VpcConnectionArn: vpcConnArn,
		})
		require.NoError(t, err)

		_, err = client.DescribeVpcConnection(ctx, &kafkasdk.DescribeVpcConnectionInput{Arn: vpcConnArn})
		require.Error(t, err)

		createOut2, err := client.CreateVpcConnection(ctx, &kafkasdk.CreateVpcConnectionInput{
			TargetClusterArn: clusterArn,
			VpcId:            aws.String("vpc-s35-2"),
			Authentication:   aws.String("SASL_IAM"),
			ClientSubnets:    []string{"subnet-1", "subnet-2"},
			SecurityGroups:   []string{"sg-1"},
		})
		require.NoError(t, err)

		deleteOut, err := client.DeleteVpcConnection(ctx, &kafkasdk.DeleteVpcConnectionInput{
			Arn: createOut2.VpcConnectionArn,
		})
		require.NoError(t, err)
		assert.Equal(t, aws.ToString(createOut2.VpcConnectionArn), aws.ToString(deleteOut.VpcConnectionArn))
		assert.NotEmpty(t, deleteOut.State)
	})

	t.Run("channels", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)
		ctx := t.Context()
		clusterArn := createCluster(t, client, "s35-channel-cluster")

		createOut, err := client.CreateChannel(ctx, &kafkasdk.CreateChannelInput{
			ClusterArn:  clusterArn,
			ChannelName: aws.String("s35-channel"),
			TopicConfigurationList: []types.TopicConfiguration{
				{
					TopicArn:        aws.String("arn:aws:kafka:us-east-1:123456789012:topic/s35-channel-topic"),
					RecordConverter: &types.RecordConverter{ValueConverter: types.ValueConverterJson},
				},
			},
			S3DestinationConfiguration: &types.S3DestinationConfiguration{
				ServiceExecutionRoleArn: aws.String("arn:aws:iam::123456789012:role/s35-channel-role"),
				Storage: &types.S3Storage{
					BucketArn:       aws.String("arn:aws:s3:::s35-bucket"),
					CompressionType: types.S3CompressionTypeNone,
					StorageClass:    types.S3StorageClassStandard,
				},
				DeadLetterQueueS3: &types.DeadLetterQueueS3{
					BucketArn: aws.String("arn:aws:s3:::s35-bucket-dlq"),
				},
			},
		})
		require.NoError(t, err)
		require.NotEmpty(t, aws.ToString(createOut.ChannelArn))

		descOut, err := client.DescribeChannel(ctx, &kafkasdk.DescribeChannelInput{
			ClusterArn: clusterArn,
			ChannelArn: createOut.ChannelArn,
		})
		require.NoError(t, err)
		assert.Equal(t, "s35-channel", aws.ToString(descOut.ChannelName))

		listOut, err := client.ListChannels(ctx, &kafkasdk.ListChannelsInput{
			ClusterArn: clusterArn,
		})
		require.NoError(t, err)
		require.Len(t, listOut.Channels, 1)

		updateOut, err := client.UpdateChannel(ctx, &kafkasdk.UpdateChannelInput{
			ClusterArn: clusterArn,
			ChannelArn: createOut.ChannelArn,
			S3DestinationUpdate: &types.S3DestinationUpdate{
				DataFreshnessInSeconds: aws.Int32(600),
			},
		})
		require.NoError(t, err)
		assert.Equal(t, aws.ToString(createOut.ChannelArn), aws.ToString(updateOut.ChannelArn))

		deleteOut, err := client.DeleteChannel(ctx, &kafkasdk.DeleteChannelInput{
			ClusterArn: clusterArn,
			ChannelArn: createOut.ChannelArn,
		})
		require.NoError(t, err)
		assert.Equal(t, aws.ToString(createOut.ChannelArn), aws.ToString(deleteOut.ChannelArn))
	})
}
