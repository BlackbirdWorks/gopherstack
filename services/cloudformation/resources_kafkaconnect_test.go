package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	kafkaconnectbackend "github.com/blackbirdworks/gopherstack/services/kafkaconnect"
)

// newKafkaConnectTestClient wires a real aws-sdk-go-v2 CloudFormation client
// against a backend with KafkaConnect (among the other backends
// newMoreTypesServiceBackends already wires) set to a real in-memory service
// backend.
func newKafkaConnectTestClient(t *testing.T) (*cloudformation.ServiceBackends, *cfnsdk.Client) {
	t.Helper()

	backends := newMoreTypesServiceBackends(t)
	backends.KafkaConnect = kafkaconnectbackend.NewHandler(kafkaconnectbackend.NewInMemoryBackend())

	creator := cloudformation.NewResourceCreator(backends)
	backend := cloudformation.NewInMemoryBackendWithConfig("000000000000", "us-east-1", creator)
	client := newTestClientForBackend(t, backend)

	return backends, client
}

func TestCreateStack_KafkaConnectTypes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{testKafkaConnectConnector, "connector"},
		{testKafkaConnectCustomPlugin, "custom_plugin"},
		{testKafkaConnectWorkerConfiguration, "worker_configuration"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func testKafkaConnectConnector(t *testing.T) {
	t.Helper()

	backends, client := newKafkaConnectTestClient(t)

	tmpl := `{
"Resources": {"Connector": {"Type": "AWS::KafkaConnect::Connector", "Properties": {
  "Capacity": {"ProvisionedCapacity": {"McuCount": 1, "WorkerCount": 1}},
  "ConnectorConfiguration": {"connector.class": "com.example.Connector"},
  "ConnectorName": "test-connector",
  "KafkaCluster": {"ApacheKafkaCluster": {
    "BootstrapServers": "broker1:9092,broker2:9092",
    "Vpc": {"SecurityGroups": ["sg-1"], "Subnets": ["subnet-1", "subnet-2"]}
  }},
  "KafkaClusterClientAuthentication": {"AuthenticationType": "NONE"},
  "KafkaClusterEncryptionInTransit": {"EncryptionType": "PLAINTEXT"},
  "KafkaConnectVersion": "2.7.1",
  "Plugins": [{"CustomPlugin": {
    "CustomPluginArn": "arn:aws:kafkaconnect:us-east-1:000000000000:custom-plugin/p/abc",
    "Revision": 1
  }}],
  "ServiceExecutionRoleArn": "arn:aws:iam::000000000000:role/connect-role"
}}},
"Outputs": {
  "Ref": {"Value": {"Ref": "Connector"}},
  "ConnectorArn": {"Value": {"Fn::GetAtt": ["Connector", "ConnectorArn"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "kc-connector-stack", tmpl)
	assert.Equal(t, outputs["Ref"], outputs["ConnectorArn"])
	assert.Contains(t, outputs["ConnectorArn"], "connector/test-connector/")

	c, err := backends.KafkaConnect.Backend.DescribeConnector(outputs["ConnectorArn"])
	require.NoError(t, err)
	assert.Equal(t, "broker1:9092,broker2:9092", c.ApacheKafkaCluster.BootstrapServers)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("kc-connector-stack")})
	require.NoError(t, err)

	_, err = backends.KafkaConnect.Backend.DescribeConnector(outputs["ConnectorArn"])
	require.Error(t, err)
}

func testKafkaConnectCustomPlugin(t *testing.T) {
	t.Helper()

	backends, client := newKafkaConnectTestClient(t)

	tmpl := `{
"Resources": {"Plugin": {"Type": "AWS::KafkaConnect::CustomPlugin", "Properties": {
  "ContentType": "ZIP",
  "Name": "test-plugin",
  "Location": {"S3Location": {"BucketArn": "arn:aws:s3:::my-bucket", "FileKey": "plugin.zip"}}
}}},
"Outputs": {
  "Ref": {"Value": {"Ref": "Plugin"}},
  "CustomPluginArn": {"Value": {"Fn::GetAtt": ["Plugin", "CustomPluginArn"]}},
  "Revision": {"Value": {"Fn::GetAtt": ["Plugin", "Revision"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "kc-plugin-stack", tmpl)
	assert.Equal(t, outputs["Ref"], outputs["CustomPluginArn"])
	assert.Contains(t, outputs["CustomPluginArn"], "custom-plugin/test-plugin/")
	assert.Equal(t, "1", outputs["Revision"])

	p, err := backends.KafkaConnect.Backend.DescribeCustomPlugin(outputs["CustomPluginArn"])
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:s3:::my-bucket", p.BucketArn)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("kc-plugin-stack")})
	require.NoError(t, err)

	_, err = backends.KafkaConnect.Backend.DescribeCustomPlugin(outputs["CustomPluginArn"])
	require.Error(t, err)
}

func testKafkaConnectWorkerConfiguration(t *testing.T) {
	t.Helper()

	backends, client := newKafkaConnectTestClient(t)

	tmpl := `{
"Resources": {"WC": {"Type": "AWS::KafkaConnect::WorkerConfiguration", "Properties": {
  "Name": "test-worker-config",
  "PropertiesFileContent": "a2V5PXZhbHVl"
}}},
"Outputs": {
  "Ref": {"Value": {"Ref": "WC"}},
  "WorkerConfigurationArn": {"Value": {"Fn::GetAtt": ["WC", "WorkerConfigurationArn"]}},
  "Revision": {"Value": {"Fn::GetAtt": ["WC", "Revision"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "kc-workerconfig-stack", tmpl)
	assert.Equal(t, outputs["Ref"], outputs["WorkerConfigurationArn"])
	assert.Contains(t, outputs["WorkerConfigurationArn"], "worker-configuration/test-worker-config/")
	assert.Equal(t, "1", outputs["Revision"])

	w, err := backends.KafkaConnect.Backend.DescribeWorkerConfiguration(outputs["WorkerConfigurationArn"])
	require.NoError(t, err)
	assert.Equal(t, "a2V5PXZhbHVl", w.LatestRevision.PropertiesFileContent)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("kc-workerconfig-stack")})
	require.NoError(t, err)

	_, err = backends.KafkaConnect.Backend.DescribeWorkerConfiguration(outputs["WorkerConfigurationArn"])
	require.Error(t, err)
}
