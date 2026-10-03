package cloudformation_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apigatewayv2backend "github.com/blackbirdworks/gopherstack/services/apigatewayv2"
	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

func TestResourceCreator_APIGatewayV2Properties(t *testing.T) {
	t.Parallel()

	backends := newDependentServiceBackends(t)
	rc := cloudformation.NewResourceCreator(backends)
	bk, ok := backends.APIGatewayV2.Backend.(*apigatewayv2backend.InMemoryBackend)
	require.True(t, ok)
	ctx := t.Context()

	apiID, err := rc.Create(ctx, "Api", "AWS::ApiGatewayV2::Api", map[string]any{
		"Name": "props", "Description": "d",
		"CorsConfiguration": map[string]any{
			"AllowOrigins": []any{"https://a.example"}, "AllowMethods": []any{"GET"}, "MaxAge": 30,
		},
	}, nil, nil)
	require.NoError(t, err)
	physIDs := map[string]string{"Api": apiID}

	integPhys, err := rc.Create(ctx, "Integ", "AWS::ApiGatewayV2::Integration", map[string]any{
		"ApiId": map[string]any{"Ref": "Api"}, "IntegrationType": "AWS_PROXY",
		"IntegrationUri": "arn:aws:lambda:us-east-1:000000000000:function:f", "PayloadFormatVersion": "2.0",
		"TimeoutInMillis": 4000, "IntegrationMethod": "POST",
	}, nil, physIDs)
	require.NoError(t, err)
	physIDs["Integ"] = integPhys

	_, err = rc.Create(ctx, "Route", "AWS::ApiGatewayV2::Route", map[string]any{
		"ApiId": map[string]any{"Ref": "Api"}, "RouteKey": "GET /x", "OperationName": "getX", "ApiKeyRequired": true,
		"Target": map[string]any{"Fn::Sub": "integrations/${Integ}"},
	}, nil, physIDs)
	require.NoError(t, err)

	_, err = rc.Create(ctx, "Stage", "AWS::ApiGatewayV2::Stage", map[string]any{
		"ApiId": map[string]any{"Ref": "Api"}, "StageName": "live", "AutoDeploy": true,
		"StageVariables": map[string]any{"k": "v"}, "Description": "stage",
		"DefaultRouteSettings": map[string]any{"ThrottlingBurstLimit": 7},
	}, nil, physIDs)
	require.NoError(t, err)

	api, err := bk.GetAPI(apiID)
	require.NoError(t, err)
	assert.Equal(t, "d", api.Description)
	require.NotNil(t, api.CorsConfiguration)
	assert.Equal(t, []string{"https://a.example"}, api.CorsConfiguration.AllowOrigins)
	assert.EqualValues(t, 30, api.CorsConfiguration.MaxAge)

	integs, err := bk.GetIntegrations(apiID)
	require.NoError(t, err)
	require.Len(t, integs, 1)
	assert.EqualValues(t, 4000, integs[0].TimeoutInMillis)
	assert.Equal(t, "POST", integs[0].IntegrationMethod)

	routes, err := bk.GetRoutes(apiID)
	require.NoError(t, err)
	require.Len(t, routes, 1)
	assert.Equal(t, "integrations/"+integs[0].IntegrationID, routes[0].Target)
	assert.Equal(t, "getX", routes[0].OperationName)
	assert.True(t, routes[0].APIKeyRequired)

	stage, err := bk.GetStage(apiID, "live")
	require.NoError(t, err)
	assert.Equal(t, "v", stage.StageVariables["k"])
	assert.Equal(t, "stage", stage.Description)
	require.NotNil(t, stage.DefaultRouteSettings)
	assert.EqualValues(t, 7, stage.DefaultRouteSettings.ThrottlingBurstLimit)
}
