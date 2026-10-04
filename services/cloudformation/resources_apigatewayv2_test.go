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

	require.NoError(t, rc.Delete(ctx, "AWS::ApiGatewayV2::Route", routePhys, nil, nil))
	require.NoError(t, rc.Delete(ctx, "AWS::ApiGatewayV2::Integration", intPhys, nil, nil))
	require.NoError(t, rc.Delete(ctx, "AWS::ApiGatewayV2::Authorizer", authPhys, nil, nil))

	routes, err = apigw.GetRoutes(apiID)
	require.NoError(t, err)
	assert.Empty(t, routes)
}

// TestDeleteStack_APIGatewayV2ChildAlreadyDeleted reproduces the CI failure on
// the apigatewayv2 terraform fixture: a child resource (Integration, Route or
// Stage) deleted directly through its own service before DeleteStack runs
// must not fail the stack delete with the underlying NotFoundException. Real
// CloudFormation treats an already-gone resource as DELETE_COMPLETE.
func TestDeleteStack_APIGatewayV2ChildAlreadyDeleted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		deleteDirect func(t *testing.T, apigw *apigatewayv2backend.InMemoryBackend, apiID string)
		name         string
		resourceType string
	}{
		{
			name:         "integration",
			resourceType: "AWS::ApiGatewayV2::Integration",
			deleteDirect: func(t *testing.T, apigw *apigatewayv2backend.InMemoryBackend, apiID string) {
				t.Helper()
				integs, err := apigw.GetIntegrations(apiID)
				require.NoError(t, err)
				require.Len(t, integs, 1)
				require.NoError(t, apigw.DeleteIntegration(apiID, integs[0].IntegrationID))
			},
		},
		{
			name:         "route",
			resourceType: "AWS::ApiGatewayV2::Route",
			deleteDirect: func(t *testing.T, apigw *apigatewayv2backend.InMemoryBackend, apiID string) {
				t.Helper()
				routes, err := apigw.GetRoutes(apiID)
				require.NoError(t, err)
				require.Len(t, routes, 1)
				require.NoError(t, apigw.DeleteRoute(apiID, routes[0].RouteID))
			},
		},
		{
			name:         "stage",
			resourceType: "AWS::ApiGatewayV2::Stage",
			deleteDirect: func(t *testing.T, apigw *apigatewayv2backend.InMemoryBackend, apiID string) {
				t.Helper()
				require.NoError(t, apigw.DeleteStage(apiID, "prod"))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backends := newDependentServiceBackends(t)
			apigw, ok := backends.APIGatewayV2.Backend.(*apigatewayv2backend.InMemoryBackend)
			require.True(t, ok)

			b := cloudformation.NewInMemoryBackendWithConfig(
				"000000000000", "us-east-1", cloudformation.NewResourceCreator(backends),
			)

			tmpl := `{"AWSTemplateFormatVersion":"2010-09-09","Resources":{` +
				`"MyApi":{"Type":"AWS::ApiGatewayV2::Api","Properties":{"Name":"stackapi","ProtocolType":"HTTP"}},` +
				`"MyChild":{"Type":"` + tc.resourceType + `","DependsOn":"MyApi","Properties":{` +
				apiGatewayV2ChildProperties(tc.resourceType) + `}}}}`

			stack, err := b.CreateStack(t.Context(), "apigwv2-stack", tmpl, nil, cloudformation.StackOptions{})
			require.NoError(t, err)
			require.Equal(t, "CREATE_COMPLETE", stack.StackStatus)

			apiRes, err := b.DescribeStackResource("apigwv2-stack", "MyApi")
			require.NoError(t, err)
			apiID := apiRes.PhysicalID

			tc.deleteDirect(t, apigw, apiID)

			require.NoError(t, b.DeleteStack(t.Context(), "apigwv2-stack"))

			final, err := b.DescribeStack("apigwv2-stack")
			require.NoError(t, err)
			assert.Equal(t, "DELETE_COMPLETE", final.StackStatus)
			assert.Empty(t, final.StackStatusReason)
		})
	}
}

// apiGatewayV2ChildProperties returns the CFN Properties JSON body for the
// given APIGatewayV2 child resource type, referencing "MyApi" as its parent.
func apiGatewayV2ChildProperties(resourceType string) string {
	switch resourceType {
	case "AWS::ApiGatewayV2::Integration":
		return `"ApiId":{"Ref":"MyApi"},"IntegrationType":"HTTP_PROXY","IntegrationUri":"https://example.com"`
	case "AWS::ApiGatewayV2::Route":
		return `"ApiId":{"Ref":"MyApi"},"RouteKey":"GET /items"`
	default: // AWS::ApiGatewayV2::Stage
		return `"ApiId":{"Ref":"MyApi"},"StageName":"prod"`
	}
}

// TestDeleteStack_APIGatewayV2FullStack: an Api stack reaches DELETE_COMPLETE even when its
// Integration, Route and Stage were already cascade-deleted.
func TestDeleteStack_APIGatewayV2FullStack(t *testing.T) {
	t.Parallel()

	tests := []struct {
		predelete func(t *testing.T, apigw *apigatewayv2backend.InMemoryBackend, apiID string)
		name      string
	}{
		{name: "clean_delete"},
		{
			name: "children_already_gone",
			predelete: func(t *testing.T, apigw *apigatewayv2backend.InMemoryBackend, apiID string) {
				t.Helper()

				integs, err := apigw.GetIntegrations(apiID)
				require.NoError(t, err)
				require.Len(t, integs, 1)
				require.NoError(t, apigw.DeleteIntegration(apiID, integs[0].IntegrationID))

				routes, err := apigw.GetRoutes(apiID)
				require.NoError(t, err)
				require.Len(t, routes, 1)
				require.NoError(t, apigw.DeleteRoute(apiID, routes[0].RouteID))

				require.NoError(t, apigw.DeleteStage(apiID, "prod"))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backends := newDependentServiceBackends(t)
			apigw, ok := backends.APIGatewayV2.Backend.(*apigatewayv2backend.InMemoryBackend)
			require.True(t, ok)

			b := cloudformation.NewInMemoryBackendWithConfig(
				"000000000000", "us-east-1", cloudformation.NewResourceCreator(backends),
			)

			tmpl := `{"AWSTemplateFormatVersion":"2010-09-09","Resources":{` +
				`"MyApi":{"Type":"AWS::ApiGatewayV2::Api","Properties":{"Name":"fullstack","ProtocolType":"HTTP"}},` +
				`"Integration":{"Type":"AWS::ApiGatewayV2::Integration","Properties":{` +
				apiGatewayV2ChildProperties("AWS::ApiGatewayV2::Integration") + `}},` +
				`"Route":{"Type":"AWS::ApiGatewayV2::Route","Properties":{` +
				apiGatewayV2ChildProperties("AWS::ApiGatewayV2::Route") + `}},` +
				`"Stage":{"Type":"AWS::ApiGatewayV2::Stage","Properties":{` +
				apiGatewayV2ChildProperties("AWS::ApiGatewayV2::Stage") + `}}` +
				`}}`

			stackName := "apigwv2-fullstack-" + tc.name

			stack, err := b.CreateStack(t.Context(), stackName, tmpl, nil, cloudformation.StackOptions{})
			require.NoError(t, err)
			require.Equal(t, "CREATE_COMPLETE", stack.StackStatus)

			apiRes, err := b.DescribeStackResource(stackName, "MyApi")
			require.NoError(t, err)

			if tc.predelete != nil {
				tc.predelete(t, apigw, apiRes.PhysicalID)
			}

			require.NoError(t, b.DeleteStack(t.Context(), stackName))

			final, err := b.DescribeStack(stackName)
			require.NoError(t, err)
			assert.Equal(t, "DELETE_COMPLETE", final.StackStatus)
			assert.Empty(t, final.StackStatusReason)
		})
	}
}
