package terraform_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kafkaconnectsdk "github.com/aws/aws-sdk-go-v2/service/kafkaconnect"
	"github.com/aws/aws-sdk-go-v2/service/kafkaconnect/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createKafkaConnectClient returns an MSK Connect client pointed at the shared test container.
func createKafkaConnectClient(t *testing.T) *kafkaconnectsdk.Client {
	t.Helper()

	return createClientWithEndpoint(t, kafkaconnectsdk.NewFromConfig, endpoint)
}

// findConnectorByName lists connectors filtered by namePrefix and returns the one matching name exactly.
func findConnectorByName(
	ctx context.Context,
	t *testing.T,
	client *kafkaconnectsdk.Client,
	name string,
) types.ConnectorSummary {
	t.Helper()

	out, err := client.ListConnectors(ctx, &kafkaconnectsdk.ListConnectorsInput{ConnectorNamePrefix: aws.String(name)})
	require.NoError(t, err, "ListConnectors should succeed after terraform apply")

	for _, c := range out.Connectors {
		if aws.ToString(c.ConnectorName) == name {
			return c
		}
	}

	t.Fatalf("connector %q not found in ListConnectors output", name)

	return types.ConnectorSummary{}
}

// findCustomPluginByName lists custom plugins filtered by namePrefix and returns the one matching name exactly.
func findCustomPluginByName(
	ctx context.Context,
	t *testing.T,
	client *kafkaconnectsdk.Client,
	name string,
) types.CustomPluginSummary {
	t.Helper()

	out, err := client.ListCustomPlugins(ctx, &kafkaconnectsdk.ListCustomPluginsInput{NamePrefix: aws.String(name)})
	require.NoError(t, err, "ListCustomPlugins should succeed after terraform apply")

	for _, p := range out.CustomPlugins {
		if aws.ToString(p.Name) == name {
			return p
		}
	}

	t.Fatalf("custom plugin %q not found in ListCustomPlugins output", name)

	return types.CustomPluginSummary{}
}

// findWorkerConfigurationByName lists worker configurations filtered by namePrefix and returns
// the one matching name exactly.
func findWorkerConfigurationByName(
	ctx context.Context,
	t *testing.T,
	client *kafkaconnectsdk.Client,
	name string,
) types.WorkerConfigurationSummary {
	t.Helper()

	out, err := client.ListWorkerConfigurations(
		ctx, &kafkaconnectsdk.ListWorkerConfigurationsInput{NamePrefix: aws.String(name)},
	)
	require.NoError(t, err, "ListWorkerConfigurations should succeed after terraform apply")

	for _, w := range out.WorkerConfigurations {
		if aws.ToString(w.Name) == name {
			return w
		}
	}

	t.Fatalf("worker configuration %q not found in ListWorkerConfigurations output", name)

	return types.WorkerConfigurationSummary{}
}

// TestTerraform_MskConnect provisions an aws_mskconnect_custom_plugin, an
// aws_mskconnect_worker_configuration, and an aws_mskconnect_connector (with
// its VPC/subnets/security group/IAM role and a literal Apache Kafka
// bootstrap string) via Terraform, then verifies all three are visible via
// the MSK Connect SDK, selecting each by name.
func TestTerraform_MskConnect(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "msk-connect",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				suffix := uuid.NewString()[:8]

				return map[string]any{
					"BucketName":       "tf-mskc-bucket-" + suffix,
					"PluginName":       "tf-mskc-plugin-" + suffix,
					"WorkerConfigName": "tf-mskc-wc-" + suffix,
					"ConnectorName":    "tf-mskc-connector-" + suffix,
				}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				client := createKafkaConnectClient(t)

				pluginName := vars["PluginName"].(string)
				plugin := findCustomPluginByName(ctx, t, client, pluginName)
				assert.Equal(t, types.CustomPluginStateActive, plugin.CustomPluginState)

				wcName := vars["WorkerConfigName"].(string)
				wc := findWorkerConfigurationByName(ctx, t, client, wcName)
				assert.Equal(t, types.WorkerConfigurationStateActive, wc.WorkerConfigurationState)

				connectorName := vars["ConnectorName"].(string)
				connector := findConnectorByName(ctx, t, client, connectorName)
				assert.Equal(t, types.ConnectorStateRunning, connector.ConnectorState)
				assert.Equal(t, "2.7.1", aws.ToString(connector.KafkaConnectVersion))
				require.NotNil(t, connector.Capacity)
				require.NotNil(t, connector.Capacity.ProvisionedCapacity)
				assert.EqualValues(t, 1, connector.Capacity.ProvisionedCapacity.WorkerCount)
				require.NotNil(t, connector.KafkaCluster)
				require.NotNil(t, connector.KafkaCluster.ApacheKafkaCluster)
				assert.Equal(
					t,
					"broker1.example.com:9092,broker2.example.com:9092",
					aws.ToString(connector.KafkaCluster.ApacheKafkaCluster.BootstrapServers),
				)

				describeOut, err := client.DescribeConnector(ctx, &kafkaconnectsdk.DescribeConnectorInput{
					ConnectorArn: connector.ConnectorArn,
				})
				require.NoError(t, err, "DescribeConnector should succeed after terraform apply")
				assert.Equal(t, connectorName, aws.ToString(describeOut.ConnectorName))

				tagsOut, err := client.ListTagsForResource(ctx, &kafkaconnectsdk.ListTagsForResourceInput{
					ResourceArn: connector.ConnectorArn,
				})
				require.NoError(t, err, "ListTagsForResource should succeed after terraform apply")
				assert.Equal(t, map[string]string{"Environment": "test", "Owner": "terraform"}, tagsOut.Tags)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}
