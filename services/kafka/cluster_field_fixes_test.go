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

// TestDescribeCluster_V1_StorageModeAndCreationTime drives DescribeCluster
// through a real SDK client and proves StorageMode/CreationTime -- both
// tracked by the backend (StorageMode via UpdateStorage, CreationTime set at
// CreateCluster) -- actually reach the wire. Field-diffed against
// kafka@v1.57.2 deserializers.go's awsRestjson1_deserializeDocumentClusterInfo:
// both are real, non-required ClusterInfo members this DTO previously never
// emitted.
func TestDescribeCluster_V1_StorageModeAndCreationTime(t *testing.T) {
	t.Parallel()

	h := kafka.NewHandler(kafka.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestKafkaClient(t, h)

	created, err := client.CreateCluster(t.Context(), &kafkasdk.CreateClusterInput{
		ClusterName:         aws.String("storage-mode-cluster"),
		KafkaVersion:        aws.String("3.5.1"),
		NumberOfBrokerNodes: aws.Int32(3),
		BrokerNodeGroupInfo: &types.BrokerNodeGroupInfo{
			ClientSubnets: []string{"subnet-1", "subnet-2"},
			InstanceType:  aws.String("kafka.m5.large"),
			StorageInfo: &types.StorageInfo{
				EbsStorageInfo: &types.EBSStorageInfo{VolumeSize: aws.Int32(100)},
			},
		},
	})
	require.NoError(t, err)

	described, err := client.DescribeCluster(t.Context(), &kafkasdk.DescribeClusterInput{
		ClusterArn: created.ClusterArn,
	})
	require.NoError(t, err)
	require.NotNil(t, described.ClusterInfo)
	assert.NotNil(t, described.ClusterInfo.CreationTime, "CreationTime should be set at creation")

	_, err = client.UpdateStorage(t.Context(), &kafkasdk.UpdateStorageInput{
		ClusterArn:     created.ClusterArn,
		CurrentVersion: described.ClusterInfo.CurrentVersion,
		StorageMode:    types.StorageModeTiered,
		ProvisionedThroughput: &types.ProvisionedThroughput{
			Enabled: aws.Bool(false),
		},
	})
	require.NoError(t, err)

	described, err = client.DescribeCluster(t.Context(), &kafkasdk.DescribeClusterInput{
		ClusterArn: created.ClusterArn,
	})
	require.NoError(t, err)
	assert.Equal(t, types.StorageModeTiered, described.ClusterInfo.StorageMode,
		"StorageMode is tracked by UpdateStorage but was never echoed by DescribeCluster before this fix")
}

// TestDescribeCluster_V1_ZookeeperConnectStringTls proves the TLS ZooKeeper
// endpoint -- a real ClusterInfo member alongside the already-correct plain
// ZookeeperConnectString -- is now populated and uses a distinct port.
func TestDescribeCluster_V1_ZookeeperConnectStringTls(t *testing.T) {
	t.Parallel()

	h := kafka.NewHandler(kafka.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestKafkaClient(t, h)

	created, err := client.CreateCluster(t.Context(), &kafkasdk.CreateClusterInput{
		ClusterName:         aws.String("zk-tls-cluster"),
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

	plain := aws.ToString(described.ClusterInfo.ZookeeperConnectString)
	tls := aws.ToString(described.ClusterInfo.ZookeeperConnectStringTls)
	require.NotEmpty(t, plain)
	require.NotEmpty(t, tls, "ZookeeperConnectStringTls was never emitted before this fix")
	assert.Contains(t, plain, ":2181")
	assert.Contains(t, tls, ":2182")
	assert.NotEqual(t, plain, tls)
}

// TestDescribeClusterV2_TopLevel_CreationTimeAndStateInfo proves
// DescribeClusterV2's top-level Cluster shape now echoes CreationTime and
// StateInfo -- both already correctly emitted by the V1 sibling
// (DescribeCluster) but missing here before this fix. Field-diffed against
// awsRestjson1_deserializeDocumentCluster.
func TestDescribeClusterV2_TopLevel_CreationTimeAndStateInfo(t *testing.T) {
	t.Parallel()

	h, backend := newTestHandlerWithBackend(t)
	client := newTestKafkaClient(t, h)

	created, err := client.CreateCluster(t.Context(), &kafkasdk.CreateClusterInput{
		ClusterName:         aws.String("v2-top-fields-cluster"),
		KafkaVersion:        aws.String("3.5.1"),
		NumberOfBrokerNodes: aws.Int32(3),
		BrokerNodeGroupInfo: &types.BrokerNodeGroupInfo{
			ClientSubnets: []string{"subnet-1", "subnet-2"},
			InstanceType:  aws.String("kafka.m5.large"),
		},
	})
	require.NoError(t, err)

	stored := kafka.GetStoredCluster(backend, aws.ToString(created.ClusterArn))
	stored.StateInfo = &kafka.StateInfo{Code: "BROKER_STORAGE_FAILURE", Message: "EBS volume ran out of space"}

	described, err := client.DescribeClusterV2(t.Context(), &kafkasdk.DescribeClusterV2Input{
		ClusterArn: created.ClusterArn,
	})
	require.NoError(t, err)
	require.NotNil(t, described.ClusterInfo)
	assert.NotNil(t, described.ClusterInfo.CreationTime, "CreationTime should be set at creation")
	require.NotNil(
		t,
		described.ClusterInfo.StateInfo,
		"StateInfo was never emitted by DescribeClusterV2 before this fix",
	)
	assert.Equal(t, "BROKER_STORAGE_FAILURE", aws.ToString(described.ClusterInfo.StateInfo.Code))
}

// TestDescribeClusterV2_Provisioned_ZookeeperConnectString proves the
// Provisioned arm now echoes zookeeperConnectString/zookeeperConnectStringTls,
// real types.Provisioned members that were entirely absent before this fix
// (this backend's zookeeperConnectStringFor helper was only ever wired to
// the V1 response).
func TestDescribeClusterV2_Provisioned_ZookeeperConnectString(t *testing.T) {
	t.Parallel()

	h := kafka.NewHandler(kafka.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestKafkaClient(t, h)

	created, err := client.CreateClusterV2(t.Context(), &kafkasdk.CreateClusterV2Input{
		ClusterName: aws.String("v2-provisioned-zk-cluster"),
		Provisioned: &types.ProvisionedRequest{
			KafkaVersion:        aws.String("3.5.1"),
			NumberOfBrokerNodes: aws.Int32(3),
			BrokerNodeGroupInfo: &types.BrokerNodeGroupInfo{
				ClientSubnets: []string{"subnet-1", "subnet-2"},
				InstanceType:  aws.String("kafka.m5.large"),
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

	plain := aws.ToString(described.ClusterInfo.Provisioned.ZookeeperConnectString)
	tls := aws.ToString(described.ClusterInfo.Provisioned.ZookeeperConnectStringTls)
	assert.NotEmpty(t, plain, "Provisioned.ZookeeperConnectString was never emitted before this fix")
	assert.NotEmpty(t, tls, "Provisioned.ZookeeperConnectStringTls was never emitted before this fix")
}
