package kafkaconnect_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	kafkaconnectsdk "github.com/aws/aws-sdk-go-v2/service/kafkaconnect"
	"github.com/aws/aws-sdk-go-v2/service/kafkaconnect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kafkaconnect"
)

func TestRealism_CreateConnectorValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(in *kafkaconnectsdk.CreateConnectorInput)
		name   string
	}{
		{name: "mcu count", mutate: func(in *kafkaconnectsdk.CreateConnectorInput) {
			in.Capacity.ProvisionedCapacity.McuCount = 3
		}},
		{name: "zero workers", mutate: func(in *kafkaconnectsdk.CreateConnectorInput) {
			in.Capacity.ProvisionedCapacity.WorkerCount = 0
		}},
		{name: "min above max", mutate: func(in *kafkaconnectsdk.CreateConnectorInput) {
			in.Capacity = &types.Capacity{AutoScaling: &types.AutoScaling{
				McuCount: 1, MinWorkerCount: 5, MaxWorkerCount: 2,
				ScaleInPolicy:  &types.ScaleInPolicy{CpuUtilizationPercentage: 20},
				ScaleOutPolicy: &types.ScaleOutPolicy{CpuUtilizationPercentage: 80},
			}}
		}},
		{name: "task count below max workers", mutate: func(in *kafkaconnectsdk.CreateConnectorInput) {
			in.Capacity = &types.Capacity{AutoScaling: &types.AutoScaling{
				McuCount: 1, MinWorkerCount: 1, MaxWorkerCount: 4, MaxAutoscalingTaskCount: 2,
				ScaleInPolicy:  &types.ScaleInPolicy{CpuUtilizationPercentage: 20},
				ScaleOutPolicy: &types.ScaleOutPolicy{CpuUtilizationPercentage: 80},
			}}
		}},
		{name: "role arn", mutate: func(in *kafkaconnectsdk.CreateConnectorInput) {
			in.ServiceExecutionRoleArn = aws.String("not-an-arn")
		}},
		{name: "name too long", mutate: func(in *kafkaconnectsdk.CreateConnectorInput) {
			in.ConnectorName = aws.String(string(make([]byte, 129)))
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestClient(t, newTestHandler())
			in := connectorInput(t, client, "val-conn")
			tc.mutate(in)

			_, err := client.CreateConnector(t.Context(), in)
			require.Error(t, err)

			var bad *types.BadRequestException
			require.ErrorAs(t, err, &bad)
			assert.NotEmpty(t, aws.ToString(bad.Message))
		})
	}
}

func TestRealism_WorkerConfigAndListValidation(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())

	_, err := client.CreateWorkerConfiguration(t.Context(), &kafkaconnectsdk.CreateWorkerConfigurationInput{
		Name:                  aws.String("wc"),
		PropertiesFileContent: aws.String("not base64!!"),
	})

	var bad *types.BadRequestException
	require.ErrorAs(t, err, &bad)

	_, err = client.ListConnectors(t.Context(), &kafkaconnectsdk.ListConnectorsInput{NextToken: aws.String("%%%")})
	require.ErrorAs(t, err, &bad)

	_, err = client.ListCustomPlugins(t.Context(), &kafkaconnectsdk.ListCustomPluginsInput{MaxResults: aws.Int32(101)})
	require.ErrorAs(t, err, &bad)
}

func TestRealism_LifecycleDelayHoldsCreating(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	h.Backend.(*kafkaconnect.InMemoryBackend).SetLifecycleDelay(time.Hour)
	client := newTestClient(t, h)

	out, err := client.CreateConnector(t.Context(), connectorInput(t, client, "slow-conn"))
	require.NoError(t, err)

	desc, err := client.DescribeConnector(
		t.Context(),
		&kafkaconnectsdk.DescribeConnectorInput{ConnectorArn: out.ConnectorArn},
	)
	require.NoError(t, err)
	assert.Equal(t, types.ConnectorStateCreating, desc.ConnectorState)
}
