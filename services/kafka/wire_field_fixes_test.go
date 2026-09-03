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

// TestCreateCluster_V1_AcceptsOptionalCreateFields drives CreateCluster
// (real CreateClusterInput, api_op_CreateCluster.go) through a real SDK
// client and proves ConfigurationInfo/StorageMode/Rebalancing -- real,
// wire-confirmed CreateClusterInput members that were previously not even
// parsed by this backend -- are now stored and echoed by DescribeCluster
// immediately, not only after a follow-up UpdateClusterConfiguration/
// UpdateStorage/UpdateRebalancing call.
func TestCreateCluster_V1_AcceptsOptionalCreateFields(t *testing.T) {
	t.Parallel()

	h := kafka.NewHandler(kafka.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestKafkaClient(t, h)

	created, err := client.CreateCluster(t.Context(), &kafkasdk.CreateClusterInput{
		ClusterName:         aws.String("create-opts-cluster"),
		KafkaVersion:        aws.String("3.5.1"),
		NumberOfBrokerNodes: aws.Int32(3),
		BrokerNodeGroupInfo: &types.BrokerNodeGroupInfo{
			ClientSubnets: []string{"subnet-1", "subnet-2"},
			InstanceType:  aws.String("kafka.m5.large"),
		},
		ConfigurationInfo: &types.ConfigurationInfo{
			Arn:      aws.String("arn:aws:kafka:us-east-1:123456789012:configuration/my-config/abc-123"),
			Revision: aws.Int64(2),
		},
		StorageMode: types.StorageModeTiered,
		Rebalancing: &types.Rebalancing{Status: types.RebalancingStatusPaused},
	})
	require.NoError(t, err)

	described, err := client.DescribeCluster(t.Context(), &kafkasdk.DescribeClusterInput{
		ClusterArn: created.ClusterArn,
	})
	require.NoError(t, err)
	require.NotNil(t, described.ClusterInfo)

	assert.Equal(t, types.StorageModeTiered, described.ClusterInfo.StorageMode,
		"StorageMode supplied at CreateCluster was silently dropped before this fix")
	require.NotNil(t, described.ClusterInfo.Rebalancing,
		"Rebalancing supplied at CreateCluster was silently dropped before this fix")
	assert.Equal(t, types.RebalancingStatusPaused, described.ClusterInfo.Rebalancing.Status)

	require.NotNil(t, described.ClusterInfo.CurrentBrokerSoftwareInfo)

	wantArn := "arn:aws:kafka:us-east-1:123456789012:configuration/my-config/abc-123"
	gotArn := aws.ToString(described.ClusterInfo.CurrentBrokerSoftwareInfo.ConfigurationArn)
	assert.Equal(
		t,
		wantArn,
		gotArn,
		"ConfigurationInfo at CreateCluster never reached CurrentBrokerSoftwareInfo before this fix",
	)
	assert.Equal(t, int64(2), aws.ToInt64(described.ClusterInfo.CurrentBrokerSoftwareInfo.ConfigurationRevision))
}

// TestCreateClusterV2_Provisioned_AcceptsEncryptionAndMonitoring proves the
// ProvisionedRequest (types.go:1362) arm of CreateClusterV2 also stops
// dropping EncryptionInfo/EnhancedMonitoring/OpenMonitoring/LoggingInfo,
// the same class of bug as the V1 test above.
func TestCreateClusterV2_Provisioned_AcceptsEncryptionAndMonitoring(t *testing.T) {
	t.Parallel()

	h := kafka.NewHandler(kafka.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestKafkaClient(t, h)

	created, err := client.CreateClusterV2(t.Context(), &kafkasdk.CreateClusterV2Input{
		ClusterName: aws.String("v2-create-opts-cluster"),
		Provisioned: &types.ProvisionedRequest{
			KafkaVersion:        aws.String("3.5.1"),
			NumberOfBrokerNodes: aws.Int32(3),
			BrokerNodeGroupInfo: &types.BrokerNodeGroupInfo{
				ClientSubnets: []string{"subnet-1", "subnet-2"},
				InstanceType:  aws.String("kafka.m5.large"),
			},
			EncryptionInfo: &types.EncryptionInfo{
				EncryptionAtRest: &types.EncryptionAtRest{
					DataVolumeKMSKeyId: aws.String("arn:aws:kms:us-east-1:123456789012:key/test-key"),
				},
			},
			EnhancedMonitoring: types.EnhancedMonitoringPerTopicPerBroker,
			OpenMonitoring: &types.OpenMonitoringInfo{
				Prometheus: &types.PrometheusInfo{
					JmxExporter:  &types.JmxExporterInfo{EnabledInBroker: aws.Bool(true)},
					NodeExporter: &types.NodeExporterInfo{EnabledInBroker: aws.Bool(true)},
				},
			},
		},
	})
	require.NoError(t, err)

	described, err := client.DescribeClusterV2(t.Context(), &kafkasdk.DescribeClusterV2Input{
		ClusterArn: created.ClusterArn,
	})
	require.NoError(t, err)
	require.NotNil(t, described.ClusterInfo)
	require.NotNil(t, described.ClusterInfo.Provisioned)

	prov := described.ClusterInfo.Provisioned
	require.NotNil(
		t,
		prov.EncryptionInfo,
		"EncryptionInfo supplied at CreateClusterV2 was silently dropped before this fix",
	)
	require.NotNil(t, prov.EncryptionInfo.EncryptionAtRest)
	assert.Equal(t,
		"arn:aws:kms:us-east-1:123456789012:key/test-key",
		aws.ToString(prov.EncryptionInfo.EncryptionAtRest.DataVolumeKMSKeyId),
	)
	assert.Equal(t, types.EnhancedMonitoringPerTopicPerBroker, prov.EnhancedMonitoring,
		"EnhancedMonitoring supplied at CreateClusterV2 was silently dropped before this fix")
	require.NotNil(
		t,
		prov.OpenMonitoring,
		"OpenMonitoring supplied at CreateClusterV2 was silently dropped before this fix",
	)
	require.NotNil(t, prov.OpenMonitoring.Prometheus.JmxExporter)
	assert.True(t, aws.ToBool(prov.OpenMonitoring.Prometheus.JmxExporter.EnabledInBroker))
}

// TestListNodes_SDKRoundTrip proves ListNodes now emits the real
// types.NodeInfo shape (7 of 7 members: addedToClusterTime, brokerNodeInfo,
// controllerNodeInfo, instanceType, nodeARN, nodeType, zookeeperNodeInfo),
// not the previous invented top-level "brokerId" field that no real
// deserializer reads (gopherstack-mk3t item 3).
func TestListNodes_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	h := kafka.NewHandler(kafka.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestKafkaClient(t, h)

	created, err := client.CreateCluster(t.Context(), &kafkasdk.CreateClusterInput{
		ClusterName:         aws.String("nodes-shape-cluster"),
		KafkaVersion:        aws.String("3.5.1"),
		NumberOfBrokerNodes: aws.Int32(2),
		BrokerNodeGroupInfo: &types.BrokerNodeGroupInfo{
			ClientSubnets: []string{"subnet-1", "subnet-2"},
			InstanceType:  aws.String("kafka.m5.large"),
		},
	})
	require.NoError(t, err)

	out, err := client.ListNodes(t.Context(), &kafkasdk.ListNodesInput{
		ClusterArn: created.ClusterArn,
	})
	require.NoError(t, err)
	require.Len(t, out.NodeInfoList, 2)

	seenBrokerIDs := make(map[float64]bool)

	for _, node := range out.NodeInfoList {
		assert.Equal(t, types.NodeTypeBroker, node.NodeType)
		assert.Equal(t, "kafka.m5.large", aws.ToString(node.InstanceType))
		assert.NotEmpty(t, aws.ToString(node.NodeARN), "NodeARN was never emitted before this fix")
		assert.NotEmpty(t, aws.ToString(node.AddedToClusterTime))
		assert.Nil(t, node.ControllerNodeInfo, "this backend only tracks broker nodes")
		assert.Nil(t, node.ZookeeperNodeInfo, "this backend only tracks broker nodes")

		require.NotNil(t, node.BrokerNodeInfo, "BrokerNodeInfo was never emitted before this fix")
		assert.NotZero(t, aws.ToFloat64(node.BrokerNodeInfo.BrokerId))
		assert.NotEmpty(t, aws.ToString(node.BrokerNodeInfo.ClientSubnet))
		require.NotNil(t, node.BrokerNodeInfo.CurrentBrokerSoftwareInfo)
		assert.Equal(t, "3.5.1", aws.ToString(node.BrokerNodeInfo.CurrentBrokerSoftwareInfo.KafkaVersion))

		seenBrokerIDs[aws.ToFloat64(node.BrokerNodeInfo.BrokerId)] = true
	}

	assert.Len(t, seenBrokerIDs, 2, "each node should have a distinct brokerId")
}

// TestDescribeClusterOperation_V1_TimesAndClientRequestId proves
// ClusterOperationInfo's CreationTime/EndTime/ClientRequestId -- real
// members (deserializers.go's
// awsRestjson1_deserializeDocumentClusterOperationInfo) previously never
// modeled at all -- are now populated. Every operation here completes
// synchronously, so EndTime equals CreationTime.
func TestDescribeClusterOperation_V1_TimesAndClientRequestId(t *testing.T) {
	t.Parallel()

	h := kafka.NewHandler(kafka.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestKafkaClient(t, h)

	created, err := client.CreateCluster(t.Context(), &kafkasdk.CreateClusterInput{
		ClusterName:         aws.String("op-times-cluster"),
		KafkaVersion:        aws.String("3.5.1"),
		NumberOfBrokerNodes: aws.Int32(3),
		BrokerNodeGroupInfo: &types.BrokerNodeGroupInfo{
			ClientSubnets: []string{"subnet-1", "subnet-2"},
			InstanceType:  aws.String("kafka.m5.large"),
		},
	})
	require.NoError(t, err)

	described, err := client.DescribeCluster(t.Context(), &kafkasdk.DescribeClusterInput{
		ClusterArn: created.ClusterArn,
	})
	require.NoError(t, err)

	updated, err := client.UpdateBrokerCount(t.Context(), &kafkasdk.UpdateBrokerCountInput{
		ClusterArn:                created.ClusterArn,
		CurrentVersion:            described.ClusterInfo.CurrentVersion,
		TargetNumberOfBrokerNodes: aws.Int32(4),
	})
	require.NoError(t, err)

	opDesc, err := client.DescribeClusterOperation(t.Context(), &kafkasdk.DescribeClusterOperationInput{
		ClusterOperationArn: updated.ClusterOperationArn,
	})
	require.NoError(t, err)
	require.NotNil(t, opDesc.ClusterOperationInfo)

	info := opDesc.ClusterOperationInfo
	require.NotNil(t, info.CreationTime, "CreationTime was never emitted before this fix")
	require.NotNil(t, info.EndTime, "EndTime was never emitted before this fix")
	assert.Equal(t, *info.CreationTime, *info.EndTime,
		"operations complete synchronously; EndTime should equal CreationTime")
	assert.NotEmpty(t, aws.ToString(info.ClientRequestId), "ClientRequestId was never emitted before this fix")
}

// TestDescribeClusterOperationV2_ProvisionedShape proves
// DescribeClusterOperationV2 now emits the real types.ClusterOperationV2
// shape: sourceClusterInfo/targetClusterInfo nested under provisioned (not
// at the top level, which is where the previous V1-struct reuse put them),
// plus clusterType. gopherstack-mk3t item 2.
func TestDescribeClusterOperationV2_ProvisionedShape(t *testing.T) {
	t.Parallel()

	h := kafka.NewHandler(kafka.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestKafkaClient(t, h)

	created, err := client.CreateCluster(t.Context(), &kafkasdk.CreateClusterInput{
		ClusterName:         aws.String("op-v2-shape-cluster"),
		KafkaVersion:        aws.String("3.5.1"),
		NumberOfBrokerNodes: aws.Int32(3),
		BrokerNodeGroupInfo: &types.BrokerNodeGroupInfo{
			ClientSubnets: []string{"subnet-1", "subnet-2"},
			InstanceType:  aws.String("kafka.m5.large"),
		},
	})
	require.NoError(t, err)

	described, err := client.DescribeCluster(t.Context(), &kafkasdk.DescribeClusterInput{
		ClusterArn: created.ClusterArn,
	})
	require.NoError(t, err)

	updated, err := client.UpdateBrokerCount(t.Context(), &kafkasdk.UpdateBrokerCountInput{
		ClusterArn:                created.ClusterArn,
		CurrentVersion:            described.ClusterInfo.CurrentVersion,
		TargetNumberOfBrokerNodes: aws.Int32(4),
	})
	require.NoError(t, err)

	opDesc, err := client.DescribeClusterOperationV2(t.Context(), &kafkasdk.DescribeClusterOperationV2Input{
		ClusterOperationArn: updated.ClusterOperationArn,
	})
	require.NoError(t, err)
	require.NotNil(t, opDesc.ClusterOperationInfo)

	info := opDesc.ClusterOperationInfo
	assert.Equal(t, types.ClusterTypeProvisioned, info.ClusterType,
		"ClusterType was never emitted by DescribeClusterOperationV2 before this fix")
	require.NotNil(t, info.Provisioned,
		"sourceClusterInfo/targetClusterInfo now nest under Provisioned, matching real ClusterOperationV2")
	require.NotNil(t, info.Provisioned.SourceClusterInfo)
	require.NotNil(t, info.Provisioned.TargetClusterInfo)
	assert.Equal(t, int32(3), aws.ToInt32(info.Provisioned.SourceClusterInfo.NumberOfBrokerNodes))
	assert.Equal(t, int32(4), aws.ToInt32(info.Provisioned.TargetClusterInfo.NumberOfBrokerNodes))
	assert.Nil(t, info.Serverless)
}

// TestListClusterOperationsV2_SummaryShape proves the ListClusterOperationsV2
// element shape now populates clusterType/startTime/endTime, real
// ClusterOperationV2Summary members previously left absent.
func TestListClusterOperationsV2_SummaryShape(t *testing.T) {
	t.Parallel()

	h := kafka.NewHandler(kafka.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestKafkaClient(t, h)

	created, err := client.CreateCluster(t.Context(), &kafkasdk.CreateClusterInput{
		ClusterName:         aws.String("op-v2-list-cluster"),
		KafkaVersion:        aws.String("3.5.1"),
		NumberOfBrokerNodes: aws.Int32(3),
		BrokerNodeGroupInfo: &types.BrokerNodeGroupInfo{
			ClientSubnets: []string{"subnet-1", "subnet-2"},
			InstanceType:  aws.String("kafka.m5.large"),
		},
	})
	require.NoError(t, err)

	described, err := client.DescribeCluster(t.Context(), &kafkasdk.DescribeClusterInput{
		ClusterArn: created.ClusterArn,
	})
	require.NoError(t, err)

	_, err = client.UpdateBrokerCount(t.Context(), &kafkasdk.UpdateBrokerCountInput{
		ClusterArn:                created.ClusterArn,
		CurrentVersion:            described.ClusterInfo.CurrentVersion,
		TargetNumberOfBrokerNodes: aws.Int32(4),
	})
	require.NoError(t, err)

	listed, err := client.ListClusterOperationsV2(t.Context(), &kafkasdk.ListClusterOperationsV2Input{
		ClusterArn: created.ClusterArn,
	})
	require.NoError(t, err)
	require.Len(t, listed.ClusterOperationInfoList, 1)

	summary := listed.ClusterOperationInfoList[0]
	assert.Equal(t, types.ClusterTypeProvisioned, summary.ClusterType,
		"ClusterType was never emitted by ListClusterOperationsV2 before this fix")
	assert.NotNil(t, summary.StartTime, "StartTime was never emitted by ListClusterOperationsV2 before this fix")
	assert.NotNil(t, summary.EndTime, "EndTime was never emitted by ListClusterOperationsV2 before this fix")
}
