//go:build integration
// +build integration

package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	mqsdk "github.com/aws/aws-sdk-go-v2/service/mq"
	mqtypes "github.com/aws/aws-sdk-go-v2/service/mq/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createMQClient returns an Amazon MQ client pointed at the shared test container.
func createMQClient(t *testing.T) *mqsdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return mqsdk.NewFromConfig(cfg, func(o *mqsdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_MQ_BrokerLifecycle tests the full broker CRUD lifecycle.
func TestIntegration_MQ_BrokerLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		brokerName string
		engineType string
	}{
		{
			name:       "activemq_full_lifecycle",
			brokerName: "integration-test-broker",
			engineType: "ACTIVEMQ",
		},
		{
			name:       "rabbitmq_full_lifecycle",
			brokerName: "integration-test-rabbitmq",
			engineType: "RABBITMQ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createMQClient(t)
			brokerName := tt.brokerName + "-" + t.Name()

			// Create broker.
			createOut, err := client.CreateBroker(ctx, &mqsdk.CreateBrokerInput{
				BrokerName:       aws.String(brokerName),
				EngineType:       mqtypes.EngineType(tt.engineType),
				EngineVersion:    aws.String("5.15.14"),
				HostInstanceType: aws.String("mq.m5.large"),
				DeploymentMode:   mqtypes.DeploymentModeSingleInstance,
				Users: []mqtypes.User{
					{Username: aws.String("admin"), Password: aws.String("adminpassword1234")},
				},
			})
			require.NoError(t, err, "CreateBroker should succeed")
			require.NotNil(t, createOut.BrokerId)
			assert.NotEmpty(t, aws.ToString(createOut.BrokerId))
			assert.NotEmpty(t, aws.ToString(createOut.BrokerArn))

			brokerID := aws.ToString(createOut.BrokerId)

			// Describe broker.
			descOut, err := client.DescribeBroker(ctx, &mqsdk.DescribeBrokerInput{
				BrokerId: aws.String(brokerID),
			})
			require.NoError(t, err, "DescribeBroker should succeed")
			assert.Equal(t, brokerName, aws.ToString(descOut.BrokerName))

			// List brokers — should contain the created one.
			listOut, err := client.ListBrokers(ctx, &mqsdk.ListBrokersInput{})
			require.NoError(t, err, "ListBrokers should succeed")

			found := false

			for _, br := range listOut.BrokerSummaries {
				if aws.ToString(br.BrokerId) == brokerID {
					found = true

					break
				}
			}

			assert.True(t, found, "created broker should appear in list")

			// Delete broker.
			_, err = client.DeleteBroker(ctx, &mqsdk.DeleteBrokerInput{
				BrokerId: aws.String(brokerID),
			})
			require.NoError(t, err, "DeleteBroker should succeed")

			// Verify deletion.
			_, err = client.DescribeBroker(ctx, &mqsdk.DescribeBrokerInput{
				BrokerId: aws.String(brokerID),
			})
			require.Error(t, err, "DescribeBroker after delete should fail")
		})
	}
}
