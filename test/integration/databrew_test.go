package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/databrew"
	"github.com/stretchr/testify/require"
)

func createDataBrewClient(t *testing.T) *databrew.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	return databrew.NewFromConfig(cfg, func(o *databrew.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

func TestIntegration_DataBrew_ListRecipes(t *testing.T) {
	t.Parallel()
	client := createDataBrewClient(t)

	res, err := client.ListRecipes(t.Context(), &databrew.ListRecipesInput{})
	require.NoError(t, err)
	require.NotNil(t, res)
}
