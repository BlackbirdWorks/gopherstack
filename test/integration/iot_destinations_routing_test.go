package integration_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotsdk "github.com/aws/aws-sdk-go-v2/service/iot"
	iottypes "github.com/aws/aws-sdk-go-v2/service/iot/types"
	iotwirelesssdk "github.com/aws/aws-sdk-go-v2/service/iotwireless"
	iotwirelesstypes "github.com/aws/aws-sdk-go-v2/service/iotwireless/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func iotWirelessDestinationLifecycle(ctx context.Context, t *testing.T) {
	t.Helper()

	client := createIoTWirelessClient(t)
	name := "route-dest-" + uuid.NewString()[:8]

	_, err := client.CreateDestination(ctx, &iotwirelesssdk.CreateDestinationInput{
		Name:           aws.String(name),
		Expression:     aws.String("rule-" + name),
		ExpressionType: iotwirelesstypes.ExpressionTypeRuleName,
		RoleArn:        aws.String("arn:aws:iam::000000000000:role/wireless"),
	})
	require.NoError(t, err)

	got, err := client.GetDestination(ctx, &iotwirelesssdk.GetDestinationInput{Name: aws.String(name)})
	require.NoError(t, err)
	assert.Equal(t, name, aws.ToString(got.Name))

	list, err := client.ListDestinations(ctx, &iotwirelesssdk.ListDestinationsInput{})
	require.NoError(t, err)

	names := make([]string, 0, len(list.DestinationList))
	for _, d := range list.DestinationList {
		names = append(names, aws.ToString(d.Name))
	}

	assert.Contains(t, names, name)

	_, err = client.UpdateDestination(ctx, &iotwirelesssdk.UpdateDestinationInput{
		Name:       aws.String(name),
		Expression: aws.String("rule-updated"),
	})
	require.NoError(t, err)

	_, err = client.DeleteDestination(ctx, &iotwirelesssdk.DeleteDestinationInput{Name: aws.String(name)})
	require.NoError(t, err)
}

func iotTopicRuleDestinationLifecycle(ctx context.Context, t *testing.T) {
	t.Helper()

	client := createIoTClient(t)
	url := "https://example.com/" + uuid.NewString()[:8]

	created, err := client.CreateTopicRuleDestination(ctx, &iotsdk.CreateTopicRuleDestinationInput{
		DestinationConfiguration: &iottypes.TopicRuleDestinationConfiguration{
			HttpUrlConfiguration: &iottypes.HttpUrlDestinationConfiguration{ConfirmationUrl: aws.String(url)},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, created.TopicRuleDestination)

	arn := aws.ToString(created.TopicRuleDestination.Arn)
	require.NotEmpty(t, arn)

	got, err := client.GetTopicRuleDestination(ctx, &iotsdk.GetTopicRuleDestinationInput{Arn: aws.String(arn)})
	require.NoError(t, err)
	assert.Equal(t, arn, aws.ToString(got.TopicRuleDestination.Arn))

	list, err := client.ListTopicRuleDestinations(ctx, &iotsdk.ListTopicRuleDestinationsInput{})
	require.NoError(t, err)

	arns := make([]string, 0, len(list.DestinationSummaries))
	for _, d := range list.DestinationSummaries {
		arns = append(arns, aws.ToString(d.Arn))
	}

	assert.Contains(t, arns, arn)

	_, err = client.DeleteTopicRuleDestination(ctx, &iotsdk.DeleteTopicRuleDestinationInput{Arn: aws.String(arn)})
	require.NoError(t, err)
}

// TestIntegration_IoTWireless_DestinationsNotCapturedByIoT drives both services' /destinations
// operations through one server so a route collision surfaces as a wrong-service response.
func TestIntegration_IoTWireless_DestinationsNotCapturedByIoT(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		run  func(ctx context.Context, t *testing.T)
		name string
	}{
		{name: "iotwireless", run: iotWirelessDestinationLifecycle},
		{name: "iot_topic_rule", run: iotTopicRuleDestinationLifecycle},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.run(t.Context(), t)
		})
	}
}
