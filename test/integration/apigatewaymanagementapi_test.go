package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/apigatewaymanagementapi"
	"github.com/stretchr/testify/require"
)

func createAPIGatewayManagementAPIClient(t *testing.T) *apigatewaymanagementapi.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	return apigatewaymanagementapi.NewFromConfig(cfg, func(o *apigatewaymanagementapi.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

func TestIntegration_APIGatewayManagementAPI_PostToConnection(t *testing.T) {
	t.Parallel()
	client := createAPIGatewayManagementAPIClient(t)

	_, err := client.PostToConnection(t.Context(), &apigatewaymanagementapi.PostToConnectionInput{
		ConnectionId: aws.String("some-conn"),
		Data:         []byte("{}"),
	})
	require.Error(t, err)
}
