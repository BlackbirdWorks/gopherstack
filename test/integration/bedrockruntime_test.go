package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/stretchr/testify/require"
)

func createBedrockRuntimeClient(t *testing.T) *bedrockruntime.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	return bedrockruntime.NewFromConfig(cfg, func(o *bedrockruntime.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

func TestIntegration_BedrockRuntime_InvokeModel(t *testing.T) {
	t.Parallel()
	client := createBedrockRuntimeClient(t)

	_, err := client.InvokeModel(t.Context(), &bedrockruntime.InvokeModelInput{
		ModelId: aws.String("some-model"),
		Body:    []byte("{}"),
	})
	require.NoError(t, err)
}
