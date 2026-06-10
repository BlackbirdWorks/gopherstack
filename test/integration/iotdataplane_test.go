package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/iotdataplane"
	"github.com/stretchr/testify/require"
)

func createIoTDataPlaneClient(t *testing.T) *iotdataplane.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	return iotdataplane.NewFromConfig(cfg, func(o *iotdataplane.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

func TestIntegration_IoTDataPlane_GetRetainedMessage(t *testing.T) {
	t.Parallel()
	client := createIoTDataPlaneClient(t)

	_, err := client.GetRetainedMessage(t.Context(), &iotdataplane.GetRetainedMessageInput{
		Topic: aws.String("some/topic"),
	})
	require.Error(t, err) // Expect an error since the message doesn't exist
}
