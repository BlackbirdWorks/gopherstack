package kafkaconnect_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kafkaconnectsdk "github.com/aws/aws-sdk-go-v2/service/kafkaconnect"
	"github.com/aws/aws-sdk-go-v2/service/kafkaconnect/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// connectorInput registers a plugin for the connector (CreateConnector rejects unknown plugin ARNs).
func connectorInput(
	t *testing.T, client *kafkaconnectsdk.Client, name string,
) *kafkaconnectsdk.CreateConnectorInput {
	t.Helper()

	return minimalCreateConnectorInput(name, createPlugin(t, client, "plugin-for-"+name))
}

func minimalCreateConnectorInput(name, pluginArn string) *kafkaconnectsdk.CreateConnectorInput {
	return &kafkaconnectsdk.CreateConnectorInput{
		Capacity: &types.Capacity{
			ProvisionedCapacity: &types.ProvisionedCapacity{McuCount: 1, WorkerCount: 1},
		},
		ConnectorConfiguration: map[string]string{"connector.class": "com.example.Connector"},
		ConnectorName:          aws.String(name),
		KafkaCluster: &types.KafkaCluster{
			ApacheKafkaCluster: &types.ApacheKafkaCluster{
				BootstrapServers: aws.String("broker1:9092,broker2:9092"),
				Vpc: &types.Vpc{
					SecurityGroups: []string{"sg-12345"},
					Subnets:        []string{"subnet-1", "subnet-2"},
				},
			},
		},
		KafkaClusterClientAuthentication: &types.KafkaClusterClientAuthentication{
			AuthenticationType: types.KafkaClusterClientAuthenticationTypeNone,
		},
		KafkaClusterEncryptionInTransit: &types.KafkaClusterEncryptionInTransit{
			EncryptionType: types.KafkaClusterEncryptionInTransitTypePlaintext,
		},
		KafkaConnectVersion: aws.String("2.7.1"),
		Plugins: []types.Plugin{
			{
				CustomPlugin: &types.CustomPlugin{
					CustomPluginArn: aws.String(pluginArn),
					Revision:        1,
				},
			},
		},
		ServiceExecutionRoleArn: aws.String("arn:aws:iam::123456789012:role/connect-role"),
	}
}

func TestCreateConnector(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "minimal"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestClient(t, newTestHandler())

			out, err := client.CreateConnector(t.Context(), connectorInput(t, client, "conn-"+tt.name))
			require.NoError(t, err)
			assert.Contains(t, aws.ToString(out.ConnectorArn), "connector/conn-"+tt.name+"/")
			assert.Equal(t, types.ConnectorStateCreating, out.ConnectorState)
			waitConnectorRunning(t, client, out.ConnectorArn)
		})
	}
}

func TestCreateConnector_DuplicateNameReturnsConflict(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	input := connectorInput(t, client, "dup-connector")

	_, err := client.CreateConnector(ctx, input)
	require.NoError(t, err)

	_, err = client.CreateConnector(ctx, input)
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "ConflictException", apiErr.ErrorCode())
}

func TestDescribeConnector(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	created, err := client.CreateConnector(ctx, connectorInput(t, client, "describe-me"))
	require.NoError(t, err)

	out, err := client.DescribeConnector(
		ctx,
		&kafkaconnectsdk.DescribeConnectorInput{ConnectorArn: created.ConnectorArn},
	)
	require.NoError(t, err)
	assert.Equal(t, "describe-me", aws.ToString(out.ConnectorName))
	assert.Equal(t, types.ConnectorStateCreating, out.ConnectorState)
	require.NotNil(t, out.Capacity)
	require.NotNil(t, out.Capacity.ProvisionedCapacity)
	assert.EqualValues(t, 1, out.Capacity.ProvisionedCapacity.WorkerCount)
	require.NotNil(t, out.KafkaCluster)
	require.NotNil(t, out.KafkaCluster.ApacheKafkaCluster)
	assert.Equal(t, "broker1:9092,broker2:9092", aws.ToString(out.KafkaCluster.ApacheKafkaCluster.BootstrapServers))
	assert.NotEmpty(t, aws.ToString(out.CurrentVersion))
}

func TestDescribeConnector_NotFound(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())

	_, err := client.DescribeConnector(t.Context(), &kafkaconnectsdk.DescribeConnectorInput{
		ConnectorArn: aws.String("arn:aws:kafkaconnect:us-east-1:123456789012:connector/nope/abc"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "NotFoundException", apiErr.ErrorCode())
}

func TestListConnectors(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	_, err := client.CreateConnector(ctx, connectorInput(t, client, "list-a"))
	require.NoError(t, err)
	_, err = client.CreateConnector(ctx, connectorInput(t, client, "list-b"))
	require.NoError(t, err)

	out, err := client.ListConnectors(ctx, &kafkaconnectsdk.ListConnectorsInput{})
	require.NoError(t, err)
	assert.Len(t, out.Connectors, 2)
}

func TestUpdateConnector_Capacity(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	created, err := client.CreateConnector(ctx, connectorInput(t, client, "update-me"))
	require.NoError(t, err)
	waitConnectorRunning(t, client, created.ConnectorArn)

	described, err := client.DescribeConnector(
		ctx,
		&kafkaconnectsdk.DescribeConnectorInput{ConnectorArn: created.ConnectorArn},
	)
	require.NoError(t, err)

	out, err := client.UpdateConnector(ctx, &kafkaconnectsdk.UpdateConnectorInput{
		ConnectorArn:   created.ConnectorArn,
		CurrentVersion: described.CurrentVersion,
		Capacity: &types.CapacityUpdate{
			ProvisionedCapacity: &types.ProvisionedCapacityUpdate{McuCount: 2, WorkerCount: 2},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, types.ConnectorStateUpdating, out.ConnectorState)
	assert.NotEmpty(t, aws.ToString(out.ConnectorOperationArn))
	waitConnectorRunning(t, client, created.ConnectorArn)

	describedAfter, err := client.DescribeConnector(
		ctx,
		&kafkaconnectsdk.DescribeConnectorInput{ConnectorArn: created.ConnectorArn},
	)
	require.NoError(t, err)
	require.NotNil(t, describedAfter.Capacity.ProvisionedCapacity)
	assert.EqualValues(t, 2, describedAfter.Capacity.ProvisionedCapacity.WorkerCount)
	assert.NotEqual(t, aws.ToString(described.CurrentVersion), aws.ToString(describedAfter.CurrentVersion))

	opOut, err := client.DescribeConnectorOperation(ctx, &kafkaconnectsdk.DescribeConnectorOperationInput{
		ConnectorOperationArn: out.ConnectorOperationArn,
	})
	require.NoError(t, err)
	assert.Equal(t, types.ConnectorOperationStateUpdateComplete, opOut.ConnectorOperationState)
	assert.NotNil(t, opOut.EndTime)
	assert.Equal(t, types.ConnectorOperationTypeUpdateWorkerSetting, opOut.ConnectorOperationType)

	listOpsOut, err := client.ListConnectorOperations(ctx, &kafkaconnectsdk.ListConnectorOperationsInput{
		ConnectorArn: created.ConnectorArn,
	})
	require.NoError(t, err)
	assert.Len(t, listOpsOut.ConnectorOperations, 1)
}

func TestUpdateConnector_VersionMismatch(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	created, err := client.CreateConnector(ctx, connectorInput(t, client, "stale-version"))
	require.NoError(t, err)

	_, err = client.UpdateConnector(ctx, &kafkaconnectsdk.UpdateConnectorInput{
		ConnectorArn:           created.ConnectorArn,
		CurrentVersion:         aws.String("stale"),
		ConnectorConfiguration: map[string]string{"foo": "bar"},
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "ConflictException", apiErr.ErrorCode())
}

func TestDeleteConnector(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	created, err := client.CreateConnector(ctx, connectorInput(t, client, "delete-me"))
	require.NoError(t, err)

	out, err := client.DeleteConnector(ctx, &kafkaconnectsdk.DeleteConnectorInput{ConnectorArn: created.ConnectorArn})
	require.NoError(t, err)
	assert.Equal(t, types.ConnectorStateDeleting, out.ConnectorState)

	waitConnectorGone(t, client, created.ConnectorArn)
}

func TestRestartConnector(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		onlyFailedTasks bool
	}{
		{name: "full"},
		{name: "only_failed_tasks", onlyFailedTasks: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestClient(t, newTestHandler())
			ctx := t.Context()

			created, err := client.CreateConnector(ctx, connectorInput(t, client, "restart-"+tt.name))
			require.NoError(t, err)

			out, err := client.RestartConnector(ctx, &kafkaconnectsdk.RestartConnectorInput{
				ConnectorArn:    created.ConnectorArn,
				OnlyFailedTasks: tt.onlyFailedTasks,
			})
			require.NoError(t, err)
			assert.Equal(t, aws.ToString(created.ConnectorArn), aws.ToString(out.ConnectorArn))
			assert.NotEmpty(t, aws.ToString(out.ConnectorOperationArn))

			opOut, err := client.DescribeConnectorOperation(ctx, &kafkaconnectsdk.DescribeConnectorOperationInput{
				ConnectorOperationArn: out.ConnectorOperationArn,
			})
			require.NoError(t, err)
			assert.Equal(t, types.ConnectorOperationStateRestartInProgress, opOut.ConnectorOperationState)
			assert.Equal(t, types.ConnectorOperationTypeRestartConnector, opOut.ConnectorOperationType)

			waitConnectorRunning(t, client, created.ConnectorArn)

			opOut, err = client.DescribeConnectorOperation(ctx, &kafkaconnectsdk.DescribeConnectorOperationInput{
				ConnectorOperationArn: out.ConnectorOperationArn,
			})
			require.NoError(t, err)
			assert.Equal(t, types.ConnectorOperationStateRestartComplete, opOut.ConnectorOperationState)
			assert.NotNil(t, opOut.EndTime)

			described, err := client.DescribeConnector(ctx, &kafkaconnectsdk.DescribeConnectorInput{
				ConnectorArn: created.ConnectorArn,
			})
			require.NoError(t, err)
			assert.Equal(t, types.ConnectorStateRunning, described.ConnectorState)
		})
	}
}

func TestRestartConnector_NotFound(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())

	_, err := client.RestartConnector(t.Context(), &kafkaconnectsdk.RestartConnectorInput{
		ConnectorArn: aws.String("arn:aws:kafkaconnect:us-east-1:123456789012:connector/nope/abc"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "NotFoundException", apiErr.ErrorCode())
}

func TestDescribeConnectorOperation_NotFound(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())

	_, err := client.DescribeConnectorOperation(t.Context(), &kafkaconnectsdk.DescribeConnectorOperationInput{
		ConnectorOperationArn: aws.String(
			"arn:aws:kafkaconnect:us-east-1:123456789012:connector/nope/abc/operation/def",
		),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "NotFoundException", apiErr.ErrorCode())
}
