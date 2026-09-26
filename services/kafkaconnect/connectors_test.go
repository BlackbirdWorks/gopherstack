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

func minimalCreateConnectorInput(name string) *kafkaconnectsdk.CreateConnectorInput {
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
					CustomPluginArn: aws.String("arn:aws:kafkaconnect:us-east-1:123456789012:custom-plugin/p/abc"),
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

			out, err := client.CreateConnector(t.Context(), minimalCreateConnectorInput("conn-"+tt.name))
			require.NoError(t, err)
			assert.Contains(t, aws.ToString(out.ConnectorArn), "connector/conn-"+tt.name+"/")
			assert.Equal(t, types.ConnectorStateRunning, out.ConnectorState)
		})
	}
}

func TestCreateConnector_DuplicateNameReturnsConflict(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	_, err := client.CreateConnector(ctx, minimalCreateConnectorInput("dup-connector"))
	require.NoError(t, err)

	_, err = client.CreateConnector(ctx, minimalCreateConnectorInput("dup-connector"))
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "ConflictException", apiErr.ErrorCode())
}

func TestDescribeConnector(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	created, err := client.CreateConnector(ctx, minimalCreateConnectorInput("describe-me"))
	require.NoError(t, err)

	out, err := client.DescribeConnector(
		ctx,
		&kafkaconnectsdk.DescribeConnectorInput{ConnectorArn: created.ConnectorArn},
	)
	require.NoError(t, err)
	assert.Equal(t, "describe-me", aws.ToString(out.ConnectorName))
	assert.Equal(t, types.ConnectorStateRunning, out.ConnectorState)
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

	_, err := client.CreateConnector(ctx, minimalCreateConnectorInput("list-a"))
	require.NoError(t, err)
	_, err = client.CreateConnector(ctx, minimalCreateConnectorInput("list-b"))
	require.NoError(t, err)

	out, err := client.ListConnectors(ctx, &kafkaconnectsdk.ListConnectorsInput{})
	require.NoError(t, err)
	assert.Len(t, out.Connectors, 2)
}

func TestUpdateConnector_Capacity(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	created, err := client.CreateConnector(ctx, minimalCreateConnectorInput("update-me"))
	require.NoError(t, err)

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
	assert.Equal(t, types.ConnectorStateRunning, out.ConnectorState)
	assert.NotEmpty(t, aws.ToString(out.ConnectorOperationArn))

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

	created, err := client.CreateConnector(ctx, minimalCreateConnectorInput("stale-version"))
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

	created, err := client.CreateConnector(ctx, minimalCreateConnectorInput("delete-me"))
	require.NoError(t, err)

	out, err := client.DeleteConnector(ctx, &kafkaconnectsdk.DeleteConnectorInput{ConnectorArn: created.ConnectorArn})
	require.NoError(t, err)
	assert.Equal(t, types.ConnectorStateDeleting, out.ConnectorState)

	_, err = client.DescribeConnector(ctx, &kafkaconnectsdk.DescribeConnectorInput{ConnectorArn: created.ConnectorArn})
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
