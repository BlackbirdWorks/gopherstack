package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/appconfigdata"
	"github.com/stretchr/testify/require"
)

func createAppConfigDataClient(t *testing.T) *appconfigdata.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	return appconfigdata.NewFromConfig(cfg, func(o *appconfigdata.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

func TestIntegration_AppConfigData_StartConfigurationSession(t *testing.T) {
	t.Parallel()
	client := createAppConfigDataClient(t)

	_, err := client.StartConfigurationSession(t.Context(), &appconfigdata.StartConfigurationSessionInput{
		ApplicationIdentifier:          aws.String("app-id"),
		ConfigurationProfileIdentifier: aws.String("profile-id"),
		EnvironmentIdentifier:          aws.String("env-id"),
	})
	require.Error(t, err)
}
