package apigatewayv2_test

import (
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	apigatewayv2sdk "github.com/aws/aws-sdk-go-v2/service/apigatewayv2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
)

// TestExportApi_OutputType_RoundTrip drives ExportApi through a real SDK
// client and proves OutputType=YAML actually returns YAML instead of the
// pre-fix behavior where the field was ignored and JSON was always returned
// regardless of what was requested (gopherstack-h910).
func TestExportApi_OutputType_RoundTrip(t *testing.T) {
	t.Parallel()

	h := apigatewayv2.NewHandler(apigatewayv2.NewInMemoryBackend())
	client := newTestAPIGatewayV2Client(t, h)

	created, err := client.CreateApi(t.Context(), &apigatewayv2sdk.CreateApiInput{
		Name:         aws.String("export-rt-api"),
		ProtocolType: "HTTP",
	})
	require.NoError(t, err)

	jsonOut, err := client.ExportApi(t.Context(), &apigatewayv2sdk.ExportApiInput{
		ApiId:         created.ApiId,
		Specification: aws.String("OAS30"),
		OutputType:    aws.String("JSON"),
	})
	require.NoError(t, err)

	var asJSON map[string]any
	require.NoError(t, json.Unmarshal(jsonOut.Body, &asJSON), "OutputType=JSON must return valid JSON")
	assert.Equal(t, "3.0.1", asJSON["openapi"])

	yamlOut, err := client.ExportApi(t.Context(), &apigatewayv2sdk.ExportApiInput{
		ApiId:         created.ApiId,
		Specification: aws.String("OAS30"),
		OutputType:    aws.String("YAML"),
	})
	require.NoError(t, err)

	require.Error(
		t, json.Unmarshal(yamlOut.Body, &asJSON),
		"OutputType=YAML must not return the same JSON syntax as OutputType=JSON",
	)
	assert.Contains(t, string(yamlOut.Body), "openapi:")
}

// TestExportApi_ClientRequiresOutputType proves the real SDK client itself
// refuses to send ExportApi without OutputType, confirming it is genuinely a
// required member on the pinned SDK, not an assumption (gopherstack-h910).
func TestExportApi_ClientRequiresOutputType(t *testing.T) {
	t.Parallel()

	h := apigatewayv2.NewHandler(apigatewayv2.NewInMemoryBackend())
	client := newTestAPIGatewayV2Client(t, h)

	_, err := client.ExportApi(t.Context(), &apigatewayv2sdk.ExportApiInput{
		ApiId:         aws.String("some-api-id"),
		Specification: aws.String("OAS30"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "OutputType")
}
