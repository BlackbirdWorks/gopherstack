package apigateway_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	apigatewaysdk "github.com/aws/aws-sdk-go-v2/service/apigateway"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

const apigwSweep1OpenAPI = `{
  "openapi": "3.0.0",
  "info": {"title": "sweep1-api", "version": "2.1.0"},
  "paths": {}
}`

// TestImportRestAPI_Version_RealClient drives ImportRestApi through the real
// aws-sdk-go-v2 client (gopherstack-7185). The real CreateRestApi/
// ImportRestApi output carries Version (from the OpenAPI document's
// info.version) alongside SecurityPolicy and Warnings (apigateway@v1.42.4
// deserializers.go: awsRestjson1_deserializeOpDocumentImportRestApiOutput);
// gopherstack's RestApi model had no Version field at all, confirmed by
// hand-reverting. GetRestApi is checked too since it shares the same model.
func TestImportRestAPI_Version_RealClient(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	client := newTestAPIGatewayClient(t, apigateway.NewHandler(backend))

	created, err := client.ImportRestApi(t.Context(), &apigatewaysdk.ImportRestApiInput{
		Body: []byte(apigwSweep1OpenAPI),
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(created.Id))
	assert.Equal(t, "2.1.0", aws.ToString(created.Version), "ImportRestApi: Version empty or wrong")

	got, err := client.GetRestApi(t.Context(), &apigatewaysdk.GetRestApiInput{RestApiId: created.Id})
	require.NoError(t, err)
	assert.Equal(t, "2.1.0", aws.ToString(got.Version), "GetRestApi: Version did not match the imported API")
}

// TestCreateVpcLink_StatusMessage_RealClient drives CreateVpcLink through the
// real client. The real output carries StatusMessage alongside Status
// (apigateway@v1.42.4 deserializers.go:
// awsRestjson1_deserializeOpDocumentCreateVpcLinkOutput); gopherstack's
// VpcLink model had no StatusMessage field at all, confirmed by
// hand-reverting. It's expected to be empty for a freshly created (non-
// failed) link, so this only checks the field round-trips through the SDK
// without error -- the presence of the JSON key is what gopherstack lacked.
func TestCreateVpcLink_StatusMessage_RealClient(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	client := newTestAPIGatewayClient(t, apigateway.NewHandler(backend))

	out, err := client.CreateVpcLink(t.Context(), &apigatewaysdk.CreateVpcLinkInput{
		Name:       aws.String("sweep1-vpclink"),
		TargetArns: []string{"arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/net/sweep1/abc"},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, string(out.Status))
	assert.Empty(t, aws.ToString(out.StatusMessage))
}
