package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sagemakerruntime"
	"github.com/stretchr/testify/require"
)

func createSageMakerRuntimeClient(t *testing.T) *sagemakerruntime.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	return sagemakerruntime.NewFromConfig(cfg, func(o *sagemakerruntime.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

func TestIntegration_SageMakerRuntime_InvokeEndpoint(t *testing.T) {
	t.Parallel()
	client := createSageMakerRuntimeClient(t)

	_, err := client.InvokeEndpoint(t.Context(), &sagemakerruntime.InvokeEndpointInput{
		EndpointName: aws.String("some-endpoint"),
		Body:         []byte("{}"),
	})
	require.NoError(t, err)
}
