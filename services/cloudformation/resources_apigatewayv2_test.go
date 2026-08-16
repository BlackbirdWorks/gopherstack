package cloudformation_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apigatewayv2backend "github.com/blackbirdworks/gopherstack/services/apigatewayv2"
	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

// TestResourceCreator_Extra_APIGatewayV2Children verifies Integration, Route, and Authorizer are
// created against a real HTTP API and removed on delete.
func TestResourceCreator_Extra_APIGatewayV2Children(t *testing.T) {
	t.Parallel()

	backends := newDependentServiceBackends(t)
	rc := cloudformation.NewResourceCreator(backends)
	ctx := t.Context()
	apigw, ok := backends.APIGatewayV2.Backend.(*apigatewayv2backend.InMemoryBackend)
	require.True(t, ok)

	apiID, err := rc.Create(ctx, "Api", "AWS::ApiGatewayV2::Api",
		map[string]any{"Name": "phase5-http", "ProtocolType": "HTTP"}, nil, nil)
	require.NoError(t, err)
	physIDs := map[string]string{"Api": apiID}

	authPhys, err := rc.Create(ctx, "Authz", "AWS::ApiGatewayV2::Authorizer",
		map[string]any{"ApiId": apiID, "Name": "jwt-less", "AuthorizerType": "REQUEST"}, nil, physIDs)
	require.NoError(t, err)

	intPhys, err := rc.Create(ctx, "Integ", "AWS::ApiGatewayV2::Integration",
		map[string]any{"ApiId": apiID, "IntegrationType": "HTTP_PROXY", "IntegrationUri": "https://example.com"},
		nil, physIDs)
	require.NoError(t, err)

	routePhys, err := rc.Create(ctx, "Route", "AWS::ApiGatewayV2::Route",
		map[string]any{"ApiId": apiID, "RouteKey": "GET /items"}, nil, physIDs)
	require.NoError(t, err)

	routes, err := apigw.GetRoutes(apiID)
	require.NoError(t, err)
	require.Len(t, routes, 1)
	assert.Equal(t, "GET /items", routes[0].RouteKey)

	require.NoError(t, rc.Delete(ctx, "AWS::ApiGatewayV2::Route", routePhys, nil))
	require.NoError(t, rc.Delete(ctx, "AWS::ApiGatewayV2::Integration", intPhys, nil))
	require.NoError(t, rc.Delete(ctx, "AWS::ApiGatewayV2::Authorizer", authPhys, nil))

	routes, err = apigw.GetRoutes(apiID)
	require.NoError(t, err)
	assert.Empty(t, routes)
}
