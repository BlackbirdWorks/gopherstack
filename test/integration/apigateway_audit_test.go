package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	apigwsdk "github.com/aws/aws-sdk-go-v2/service/apigateway"
	apigwtypes "github.com/aws/aws-sdk-go-v2/service/apigateway/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createAPIGatewayAuditClient returns an API Gateway (v1) client pointed at the
// shared test container. Named uniquely to avoid colliding with any helper that
// may later be added to main_test.go.
func createAPIGatewayAuditClient(t *testing.T) *apigwsdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return apigwsdk.NewFromConfig(cfg, func(o *apigwsdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_APIGatewayAudit_ImportApiKeysThenGetApiKeys verifies that
// ImportApiKeys actually creates API keys from the supplied CSV payload and that
// GetApiKeys subsequently returns the imported key.
func TestIntegration_APIGatewayAudit_ImportApiKeysThenGetApiKeys(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := t.Context()
	client := createAPIGatewayAuditClient(t)

	const keyName = "audit-imported-key"

	// AWS API key CSV file format: a header row naming columns, then one row per
	// key. The "key" column holds the secret value; "name" the key name.
	csvPayload := []byte("name,key,enabled\n" + keyName + ",auditsecretvalue123,true\n")

	importOut, err := client.ImportApiKeys(ctx, &apigwsdk.ImportApiKeysInput{
		Body:   csvPayload,
		Format: apigwtypes.ApiKeysFormatCsv,
	})
	require.NoError(t, err, "ImportApiKeys should succeed")
	require.NotEmpty(t, importOut.Ids, "ImportApiKeys should return the id of the created key")

	importedID := importOut.Ids[0]

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteApiKey(cleanupCtx, &apigwsdk.DeleteApiKeyInput{ApiKey: aws.String(importedID)})
	})

	// GetApiKeys must reflect the imported key.
	getOut, err := client.GetApiKeys(ctx, &apigwsdk.GetApiKeysInput{})
	require.NoError(t, err, "GetApiKeys should succeed")

	var found bool
	for _, k := range getOut.Items {
		if aws.ToString(k.Id) == importedID {
			found = true

			assert.Equal(t, keyName, aws.ToString(k.Name), "imported key should keep its name")

			break
		}
	}

	assert.True(t, found, "GetApiKeys should contain the imported key %q", importedID)
}

// TestIntegration_APIGatewayAudit_GetSdkValidatesInput verifies that GetSdk
// performs AWS-accurate input validation: a non-existent REST API yields a
// NotFoundException rather than a silent empty success.
func TestIntegration_APIGatewayAudit_GetSdkValidatesInput(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := t.Context()
	client := createAPIGatewayAuditClient(t)

	_, err := client.GetSdk(ctx, &apigwsdk.GetSdkInput{
		RestApiId: aws.String("does-not-exist"),
		StageName: aws.String("prod"),
		SdkType:   aws.String("javascript"),
	})
	require.Error(t, err, "GetSdk against a missing REST API should fail validation")
}
