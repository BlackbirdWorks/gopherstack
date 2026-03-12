package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	kafkasvc "github.com/aws/aws-sdk-go-v2/service/kafka"
	kafkatypes "github.com/aws/aws-sdk-go-v2/service/kafka/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createKafkaSDKClient returns an MSK Kafka client pointed at the shared test container.
func createKafkaSDKClient(t *testing.T) *kafkasvc.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return kafkasvc.NewFromConfig(cfg, func(o *kafkasvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_Kafka_ClusterLifecycle tests the full cluster CRUD lifecycle via the SDK.
func TestIntegration_Kafka_ClusterLifecycle(t *testing.T) {
	t.Parallel()

	client := createKafkaSDKClient(t)
	clusterName := "integration-kafka-" + t.Name()

	numBrokers := int32(1)

	// Create cluster.
	createOut, err := client.CreateCluster(t.Context(), &kafkasvc.CreateClusterInput{
		ClusterName:         aws.String(clusterName),
		KafkaVersion:        aws.String("3.5.1"),
		NumberOfBrokerNodes: &numBrokers,
		BrokerNodeGroupInfo: &kafkatypes.BrokerNodeGroupInfo{
			ClientSubnets: []string{"subnet-00000000"},
			InstanceType:  aws.String("kafka.m5.large"),
		},
		Tags: map[string]string{"env": "test"},
	})
	require.NoError(t, err, "CreateCluster should succeed")
	assert.NotEmpty(t, aws.ToString(createOut.ClusterArn))
	assert.Equal(t, clusterName, aws.ToString(createOut.ClusterName))

	clusterArn := aws.ToString(createOut.ClusterArn)

	// Describe cluster.
	descOut, err := client.DescribeCluster(t.Context(), &kafkasvc.DescribeClusterInput{
		ClusterArn: aws.String(clusterArn),
	})
	require.NoError(t, err, "DescribeCluster should succeed")
	assert.Equal(t, clusterName, aws.ToString(descOut.ClusterInfo.ClusterName))
	assert.Equal(t, "ACTIVE", string(descOut.ClusterInfo.State))

	// GetBootstrapBrokers.
	brokersOut, err := client.GetBootstrapBrokers(t.Context(), &kafkasvc.GetBootstrapBrokersInput{
		ClusterArn: aws.String(clusterArn),
	})
	require.NoError(t, err, "GetBootstrapBrokers should succeed")
	assert.NotEmpty(t, aws.ToString(brokersOut.BootstrapBrokerString))

	// List clusters.
	listOut, err := client.ListClusters(t.Context(), &kafkasvc.ListClustersInput{
		ClusterNameFilter: aws.String(clusterName),
	})
	require.NoError(t, err, "ListClusters should succeed")
	assert.NotEmpty(t, listOut.ClusterInfoList, "cluster should appear in list")

	// ListTagsForResource.
	tagsOut, err := client.ListTagsForResource(t.Context(), &kafkasvc.ListTagsForResourceInput{
		ResourceArn: aws.String(clusterArn),
	})
	require.NoError(t, err, "ListTagsForResource should succeed")
	assert.Equal(t, "test", tagsOut.Tags["env"])

	// Delete cluster.
	_, err = client.DeleteCluster(t.Context(), &kafkasvc.DeleteClusterInput{
		ClusterArn: aws.String(clusterArn),
	})
	require.NoError(t, err, "DeleteCluster should succeed")
}

// TestIntegration_Kafka_ConfigurationLifecycle tests configuration CRUD.
func TestIntegration_Kafka_ConfigurationLifecycle(t *testing.T) {
	t.Parallel()

	client := createKafkaSDKClient(t)
	cfgName := "integration-kafka-config-" + t.Name()

	// Create configuration.
	createOut, err := client.CreateConfiguration(t.Context(), &kafkasvc.CreateConfigurationInput{
		Name:             aws.String(cfgName),
		KafkaVersions:    []string{"3.5.1"},
		ServerProperties: []byte("auto.create.topics.enable=true"),
	})
	require.NoError(t, err, "CreateConfiguration should succeed")
	assert.NotEmpty(t, aws.ToString(createOut.Arn))
	assert.Equal(t, cfgName, aws.ToString(createOut.Name))

	cfgArn := aws.ToString(createOut.Arn)

	// Describe configuration.
	descOut, err := client.DescribeConfiguration(t.Context(), &kafkasvc.DescribeConfigurationInput{
		Arn: aws.String(cfgArn),
	})
	require.NoError(t, err, "DescribeConfiguration should succeed")
	assert.Equal(t, cfgName, aws.ToString(descOut.Name))

	// List configurations.
	listOut, err := client.ListConfigurations(t.Context(), &kafkasvc.ListConfigurationsInput{})
	require.NoError(t, err, "ListConfigurations should succeed")
	assert.NotEmpty(t, listOut.Configurations)

	// Delete configuration.
	_, err = client.DeleteConfiguration(t.Context(), &kafkasvc.DeleteConfigurationInput{
		Arn: aws.String(cfgArn),
	})
	require.NoError(t, err, "DeleteConfiguration should succeed")
}

// TestIntegration_Kafka_Tags tests tagging and untagging.
func TestIntegration_Kafka_Tags(t *testing.T) {
	t.Parallel()

	client := createKafkaSDKClient(t)

	numBrokers := int32(1)
	createOut, err := client.CreateCluster(t.Context(), &kafkasvc.CreateClusterInput{
		ClusterName:         aws.String("tag-test-cluster-" + t.Name()),
		KafkaVersion:        aws.String("3.5.1"),
		NumberOfBrokerNodes: &numBrokers,
		BrokerNodeGroupInfo: &kafkatypes.BrokerNodeGroupInfo{
			ClientSubnets: []string{"subnet-00000000"},
			InstanceType:  aws.String("kafka.m5.large"),
		},
	})
	require.NoError(t, err)

	clusterArn := aws.ToString(createOut.ClusterArn)

	// Tag resource.
	_, err = client.TagResource(t.Context(), &kafkasvc.TagResourceInput{
		ResourceArn: aws.String(clusterArn),
		Tags:        map[string]string{"team": "platform"},
	})
	require.NoError(t, err, "TagResource should succeed")

	// Verify tag was added.
	tagsOut, err := client.ListTagsForResource(t.Context(), &kafkasvc.ListTagsForResourceInput{
		ResourceArn: aws.String(clusterArn),
	})
	require.NoError(t, err)
	assert.Equal(t, "platform", tagsOut.Tags["team"])

	// Untag resource.
	_, err = client.UntagResource(t.Context(), &kafkasvc.UntagResourceInput{
		ResourceArn: aws.String(clusterArn),
		TagKeys:     []string{"team"},
	})
	require.NoError(t, err, "UntagResource should succeed")

	// Verify tag was removed.
	tagsOut2, err := client.ListTagsForResource(t.Context(), &kafkasvc.ListTagsForResourceInput{
		ResourceArn: aws.String(clusterArn),
	})
	require.NoError(t, err)
	assert.Empty(t, tagsOut2.Tags["team"])
}
