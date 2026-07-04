package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	apigwv2sdk "github.com/aws/aws-sdk-go-v2/service/apigatewayv2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_APIGWv2Audit_ImportApiCreatesRoutes verifies that ImportApi
// parses an OpenAPI body and creates a route for each path+method, so that
// GetRoutes reflects the imported structure.
func TestIntegration_APIGWv2Audit_ImportApiCreatesRoutes(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := t.Context()
	client := createAPIGatewayV2Client(t)

	const openAPIBody = `{
		"openapi": "3.0.1",
		"info": {"title": "audit-imported-api", "version": "1.0"},
		"paths": {
			"/items": {
				"get": {
					"x-amazon-apigateway-integration": {
						"type": "http_proxy",
						"httpMethod": "GET",
						"uri": "https://example.com/items",
						"payloadFormatVersion": "1.0"
					}
				}
			}
		}
	}`

	importOut, err := client.ImportApi(ctx, &apigwv2sdk.ImportApiInput{
		Body: aws.String(openAPIBody),
	})
	require.NoError(t, err, "ImportApi should succeed")
	require.NotNil(t, importOut.ApiId)

	apiID := aws.ToString(importOut.ApiId)
	assert.Equal(t, "audit-imported-api", aws.ToString(importOut.Name), "API name should come from info.title")

	t.Cleanup(func() {
		_, _ = client.DeleteApi(ctx, &apigwv2sdk.DeleteApiInput{ApiId: aws.String(apiID)})
	})

	routesOut, err := client.GetRoutes(ctx, &apigwv2sdk.GetRoutesInput{ApiId: aws.String(apiID)})
	require.NoError(t, err, "GetRoutes should succeed")

	var found bool
	for _, r := range routesOut.Items {
		if aws.ToString(r.RouteKey) == "GET /items" {
			found = true

			break
		}
	}

	assert.True(t, found, "GetRoutes should contain a route with key %q", "GET /items")
}

// TestIntegration_APIGWv2Audit_ReimportApiReplacesRoutes verifies that
// ReimportApi parses an OpenAPI body and replaces the existing routes of an API.
func TestIntegration_APIGWv2Audit_ReimportApiReplacesRoutes(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := t.Context()
	client := createAPIGatewayV2Client(t)

	const initialBody = `{
		"openapi": "3.0.1",
		"info": {"title": "audit-reimport-api"},
		"paths": {
			"/items": {"get": {}}
		}
	}`

	importOut, err := client.ImportApi(ctx, &apigwv2sdk.ImportApiInput{
		Body: aws.String(initialBody),
	})
	require.NoError(t, err, "ImportApi should succeed")
	require.NotNil(t, importOut.ApiId)

	apiID := aws.ToString(importOut.ApiId)

	t.Cleanup(func() {
		_, _ = client.DeleteApi(ctx, &apigwv2sdk.DeleteApiInput{ApiId: aws.String(apiID)})
	})

	const reimportBody = `{
		"openapi": "3.0.1",
		"info": {"title": "audit-reimport-api"},
		"paths": {
			"/widgets": {"post": {}}
		}
	}`

	_, err = client.ReimportApi(ctx, &apigwv2sdk.ReimportApiInput{
		ApiId: aws.String(apiID),
		Body:  aws.String(reimportBody),
	})
	require.NoError(t, err, "ReimportApi should succeed")

	routesOut, err := client.GetRoutes(ctx, &apigwv2sdk.GetRoutesInput{ApiId: aws.String(apiID)})
	require.NoError(t, err, "GetRoutes should succeed")

	keys := make([]string, 0, len(routesOut.Items))
	for _, r := range routesOut.Items {
		keys = append(keys, aws.ToString(r.RouteKey))
	}

	assert.Contains(t, keys, "POST /widgets", "ReimportApi should add the new route")
	assert.NotContains(t, keys, "GET /items", "ReimportApi should replace the old route")
}
