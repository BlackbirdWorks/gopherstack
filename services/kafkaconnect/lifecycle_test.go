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

const (
	unknownPluginArn       = "arn:aws:kafkaconnect:us-east-1:123456789012:custom-plugin/x/y"
	unknownWorkerConfigArn = "arn:aws:kafkaconnect:us-east-1:123456789012:worker-configuration/x/y"
)

func TestConnectorLifecycleTransitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		act       func(t *testing.T, c *kafkaconnectsdk.Client, arn *string) types.ConnectorState
		name      string
		wantOpTyp types.ConnectorOperationType
		wantOpEnd types.ConnectorOperationState
	}{
		{
			name: "restart",
			act: func(t *testing.T, c *kafkaconnectsdk.Client, arn *string) types.ConnectorState {
				t.Helper()

				_, err := c.RestartConnector(t.Context(), &kafkaconnectsdk.RestartConnectorInput{ConnectorArn: arn})
				require.NoError(t, err)

				return types.ConnectorStateRestarting
			},
			wantOpTyp: types.ConnectorOperationTypeRestartConnector,
			wantOpEnd: types.ConnectorOperationStateRestartComplete,
		},
		{
			name: "update_configuration",
			act: func(t *testing.T, c *kafkaconnectsdk.Client, arn *string) types.ConnectorState {
				t.Helper()

				desc, err := c.DescribeConnector(
					t.Context(), &kafkaconnectsdk.DescribeConnectorInput{ConnectorArn: arn},
				)
				require.NoError(t, err)

				_, err = c.UpdateConnector(t.Context(), &kafkaconnectsdk.UpdateConnectorInput{
					ConnectorArn:           arn,
					CurrentVersion:         desc.CurrentVersion,
					ConnectorConfiguration: map[string]string{"tasks.max": "2"},
				})
				require.NoError(t, err)

				return types.ConnectorStateUpdating
			},
			wantOpTyp: types.ConnectorOperationTypeUpdateConnectorConfiguration,
			wantOpEnd: types.ConnectorOperationStateUpdateComplete,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestClient(t, newTestHandler())
			created, err := client.CreateConnector(t.Context(), connectorInput(t, client, "life-"+tt.name))
			require.NoError(t, err)
			waitConnectorRunning(t, client, created.ConnectorArn)

			transient := tt.act(t, client, created.ConnectorArn)
			desc, err := client.DescribeConnector(t.Context(), &kafkaconnectsdk.DescribeConnectorInput{
				ConnectorArn: created.ConnectorArn,
			})
			require.NoError(t, err)
			assert.Equal(t, transient, desc.ConnectorState)

			waitConnectorRunning(t, client, created.ConnectorArn)

			ops, err := client.ListConnectorOperations(t.Context(), &kafkaconnectsdk.ListConnectorOperationsInput{
				ConnectorArn: created.ConnectorArn,
			})
			require.NoError(t, err)
			require.Len(t, ops.ConnectorOperations, 1)
			assert.Equal(t, tt.wantOpTyp, ops.ConnectorOperations[0].ConnectorOperationType)
			assert.Equal(t, tt.wantOpEnd, ops.ConnectorOperations[0].ConnectorOperationState)
		})
	}
}

func TestConnectorDeleteMutationsConflict(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	created, err := client.CreateConnector(t.Context(), connectorInput(t, client, "deleting"))
	require.NoError(t, err)

	_, err = client.DeleteConnector(
		t.Context(), &kafkaconnectsdk.DeleteConnectorInput{ConnectorArn: created.ConnectorArn},
	)
	require.NoError(t, err)

	_, err = client.RestartConnector(
		t.Context(), &kafkaconnectsdk.RestartConnectorInput{ConnectorArn: created.ConnectorArn},
	)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "ConflictException", apiErr.ErrorCode())
	waitConnectorGone(t, client, created.ConnectorArn)
}

func TestCreateConnectorRejectsUnknownReferences(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(in *kafkaconnectsdk.CreateConnectorInput)
		name   string
	}{
		{
			name: "plugin",
			mutate: func(in *kafkaconnectsdk.CreateConnectorInput) {
				in.Plugins[0].CustomPlugin.CustomPluginArn = aws.String(unknownPluginArn)
			},
		},
		{
			name: "worker_configuration",
			mutate: func(in *kafkaconnectsdk.CreateConnectorInput) {
				in.WorkerConfiguration = &types.WorkerConfiguration{
					WorkerConfigurationArn: aws.String(unknownWorkerConfigArn),
					Revision:               1,
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestClient(t, newTestHandler())
			in := connectorInput(t, client, "bad-ref-"+tt.name)
			tt.mutate(in)

			_, err := client.CreateConnector(t.Context(), in)

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, "NotFoundException", apiErr.ErrorCode())
		})
	}
}

func TestCustomPluginSettlesActive(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	created, err := client.CreateCustomPlugin(t.Context(), minimalCreateCustomPluginInput("settle"))
	require.NoError(t, err)
	assert.Equal(t, types.CustomPluginStateCreating, created.CustomPluginState)

	require.Eventually(t, func() bool {
		out, descErr := client.DescribeCustomPlugin(t.Context(), &kafkaconnectsdk.DescribeCustomPluginInput{
			CustomPluginArn: created.CustomPluginArn,
		})

		return descErr == nil && out.CustomPluginState == types.CustomPluginStateActive
	}, waitTimeout, waitTick)
}
