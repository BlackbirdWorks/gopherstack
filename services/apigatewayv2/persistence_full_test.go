package apigatewayv2_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
)

// TestInMemoryBackend_SnapshotRestore_FullState exercises every resource
// family touched by the Phase 3.3 pkgs/store conversion, in particular the
// ones whose composite key (apiID#childID, domainName#id,
// portalProductID#pageID) depends on a field tagged json:"-" on the live type
// (Stage, Route, Integration, Deployment, Authorizer, Model,
// IntegrationResponse, RouteResponse, APIMapping, ProductPage,
// ProductRestEndpointPage, RoutingRule -- see store_setup.go and
// persistence.go). If the DTO-registry snapshot/restore path in
// persistence.go ever dropped that field, every one of these lookups would
// fail post-restore even though the resource "exists" in the restored table
// under the wrong (unprefixed) key. This is a single sequential lifecycle
// (create everything once, snapshot, restore, verify everything), not a
// table of independent cases, so it does not need t.Run subtests.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()
	ctx := t.Context()

	api, err := b.CreateAPI(ctx, apigatewayv2.CreateAPIInput{Name: "full-api", ProtocolType: "HTTP"})
	require.NoError(t, err)

	stage, err := b.CreateStage(api.APIID, apigatewayv2.CreateStageInput{StageName: "prod"})
	require.NoError(t, err)

	route, err := b.CreateRoute(api.APIID, apigatewayv2.CreateRouteInput{RouteKey: "GET /widgets"})
	require.NoError(t, err)

	integration, err := b.CreateIntegration(api.APIID, apigatewayv2.CreateIntegrationInput{
		IntegrationType: "AWS_PROXY",
		IntegrationURI:  "arn:aws:lambda:us-east-1:1:function:f",
		CredentialsArn:  "arn:aws:iam::123456789012:role/apigw-role",
	})
	require.NoError(t, err)

	deployment, err := b.CreateDeployment(api.APIID, apigatewayv2.CreateDeploymentInput{Description: "initial"})
	require.NoError(t, err)

	authorizer, err := b.CreateAuthorizer(api.APIID, apigatewayv2.CreateAuthorizerInput{
		Name: "auth1", AuthorizerType: "REQUEST", AuthorizerURI: "arn:aws:lambda:us-east-1:1:function:auth",
	})
	require.NoError(t, err)

	model, err := b.CreateModel(api.APIID, apigatewayv2.CreateModelInput{
		Name: "Widget", ContentType: "application/json", Schema: `{"type":"object"}`,
	})
	require.NoError(t, err)

	integrationResponse, err := b.CreateIntegrationResponse(
		api.APIID, integration.IntegrationID,
		apigatewayv2.CreateIntegrationResponseInput{IntegrationResponseKey: "/200/"},
	)
	require.NoError(t, err)

	routeResponse, err := b.CreateRouteResponse(
		api.APIID, route.RouteID,
		apigatewayv2.CreateRouteResponseInput{RouteResponseKey: "$default"},
	)
	require.NoError(t, err)

	domainName, err := b.CreateDomainName(ctx, apigatewayv2.CreateDomainNameInput{DomainNameValue: "api.example.com"})
	require.NoError(t, err)

	apiMapping, err := b.CreateAPIMapping(domainName.DomainNameValue, apigatewayv2.CreateAPIMappingInput{
		APIID: api.APIID, Stage: stage.StageName,
	})
	require.NoError(t, err)

	portal, err := b.CreatePortal(apigatewayv2.CreatePortalInput{
		LogoURI:               "https://example.com/logo.png",
		Authorization:         &apigatewayv2.Authorization{None: &apigatewayv2.None{}},
		EndpointConfiguration: &apigatewayv2.EndpointConfigurationRequest{None: &apigatewayv2.None{}},
		PortalContent: &apigatewayv2.PortalContent{
			DisplayName: "persist-portal",
			Theme: &apigatewayv2.PortalTheme{
				CustomColors: &apigatewayv2.CustomColors{
					AccentColor:          "#000000",
					BackgroundColor:      "#ffffff",
					ErrorValidationColor: "#ff0000",
					HeaderColor:          "#111111",
					NavigationColor:      "#222222",
					TextColor:            "#333333",
				},
			},
		},
	})
	require.NoError(t, err)

	portalProduct, err := b.CreatePortalProduct(apigatewayv2.CreatePortalProductInput{DisplayName: "product1"})
	require.NoError(t, err)

	productPage, err := b.CreateProductPage(portalProduct.PortalProductID, apigatewayv2.CreateProductPageInput{})
	require.NoError(t, err)

	productREPage, err := b.CreateProductRestEndpointPage(
		portalProduct.PortalProductID, apigatewayv2.CreateProductRestEndpointPageInput{
			RestEndpointIdentifier: &apigatewayv2.RestEndpointIdentifier{
				IdentifierParts: &apigatewayv2.IdentifierParts{
					Method: "GET", Path: "/widgets", RestAPIID: "abc123", Stage: "prod",
				},
			},
		},
	)
	require.NoError(t, err)

	sharingPolicy, err := b.PutPortalProductSharingPolicy(portalProduct.PortalProductID, `{"policy":"doc"}`)
	require.NoError(t, err)

	vpcLink, err := b.CreateVpcLink(apigatewayv2.CreateVpcLinkInput{Name: "link1", SubnetIDs: []string{"subnet-1"}})
	require.NoError(t, err)

	routingRule, err := b.CreateRoutingRule(
		ctx, domainName.DomainNameValue, apigatewayv2.CreateRoutingRuleInput{
			Priority: 1,
			Actions: []apigatewayv2.RoutingRuleAction{
				{InvokeAPI: &apigatewayv2.RoutingRuleActionInvokeAPI{APIID: api.APIID, Stage: stage.StageName}},
			},
			Conditions: []apigatewayv2.RoutingRuleCondition{
				{MatchBasePaths: &apigatewayv2.RoutingRuleMatchBasePaths{AnyOf: []string{"/widgets"}}},
			},
		},
	)
	require.NoError(t, err)

	snap := b.Snapshot(ctx)
	require.NotNil(t, snap)

	fresh := apigatewayv2.NewInMemoryBackend()
	require.NoError(t, fresh.Restore(ctx, snap))

	gotAPI, err := fresh.GetAPI(api.APIID)
	require.NoError(t, err)
	assert.Equal(t, api.Name, gotAPI.Name)

	gotStage, err := fresh.GetStage(api.APIID, stage.StageName)
	require.NoError(t, err)
	assert.Equal(t, stage.StageName, gotStage.StageName)

	gotRoute, err := fresh.GetRoute(api.APIID, route.RouteID)
	require.NoError(t, err)
	assert.Equal(t, route.RouteKey, gotRoute.RouteKey)

	gotIntegration, err := fresh.GetIntegration(api.APIID, integration.IntegrationID)
	require.NoError(t, err)
	assert.Equal(t, integration.IntegrationURI, gotIntegration.IntegrationURI)
	assert.Equal(t, integration.CredentialsArn, gotIntegration.CredentialsArn)

	gotDeployment, err := fresh.GetDeployment(api.APIID, deployment.DeploymentID)
	require.NoError(t, err)
	assert.Equal(t, deployment.Description, gotDeployment.Description)

	gotAuthorizer, err := fresh.GetAuthorizer(api.APIID, authorizer.AuthorizerID)
	require.NoError(t, err)
	assert.Equal(t, authorizer.Name, gotAuthorizer.Name)

	gotModel, err := fresh.GetModel(api.APIID, model.ModelID)
	require.NoError(t, err)
	assert.Equal(t, model.Schema, gotModel.Schema)

	gotIntegrationResponse, err := fresh.GetIntegrationResponse(
		api.APIID, integration.IntegrationID, integrationResponse.IntegrationResponseID,
	)
	require.NoError(t, err)
	assert.Equal(t, integrationResponse.IntegrationResponseKey, gotIntegrationResponse.IntegrationResponseKey)

	gotRouteResponse, err := fresh.GetRouteResponse(api.APIID, route.RouteID, routeResponse.RouteResponseID)
	require.NoError(t, err)
	assert.Equal(t, routeResponse.RouteResponseKey, gotRouteResponse.RouteResponseKey)

	gotDomainName, err := fresh.GetDomainName(domainName.DomainNameValue)
	require.NoError(t, err)
	assert.Equal(t, domainName.DomainNameValue, gotDomainName.DomainNameValue)

	gotAPIMapping, err := fresh.GetAPIMapping(domainName.DomainNameValue, apiMapping.APIMappingID)
	require.NoError(t, err)
	assert.Equal(t, apiMapping.Stage, gotAPIMapping.Stage)

	gotPortal, err := fresh.GetPortal(portal.PortalID)
	require.NoError(t, err)
	assert.Equal(t, portal.LogoURI, gotPortal.LogoURI)
	require.NotNil(t, gotPortal.PortalContent)
	assert.Equal(t, "persist-portal", gotPortal.PortalContent.DisplayName)
	require.NotNil(t, gotPortal.EndpointConfiguration)
	assert.NotEmpty(t, gotPortal.EndpointConfiguration.PortalDefaultDomainName)

	gotPortalProduct, err := fresh.GetPortalProduct(portalProduct.PortalProductID)
	require.NoError(t, err)
	assert.Equal(t, portalProduct.DisplayName, gotPortalProduct.DisplayName)

	gotProductPage, err := fresh.GetProductPage(portalProduct.PortalProductID, productPage.ProductPageID)
	require.NoError(t, err)
	assert.Equal(t, productPage.ProductPageID, gotProductPage.ProductPageID)

	gotProductREPage, err := fresh.GetProductRestEndpointPage(
		portalProduct.PortalProductID, productREPage.ProductRestEndpointPageID,
	)
	require.NoError(t, err)
	assert.Equal(t, productREPage.ProductRestEndpointPageID, gotProductREPage.ProductRestEndpointPageID)
	require.NotNil(t, gotProductREPage.RestEndpointIdentifier)
	require.NotNil(t, gotProductREPage.RestEndpointIdentifier.IdentifierParts)
	assert.Equal(t, "abc123", gotProductREPage.RestEndpointIdentifier.IdentifierParts.RestAPIID)

	gotSharingPolicy, err := fresh.GetPortalProductSharingPolicy(portalProduct.PortalProductID)
	require.NoError(t, err)
	assert.Equal(t, sharingPolicy.PolicyDocument, gotSharingPolicy.PolicyDocument)

	gotVpcLink, err := fresh.GetVpcLink(vpcLink.VpcLinkID)
	require.NoError(t, err)
	assert.Equal(t, vpcLink.Name, gotVpcLink.Name)

	gotRoutingRule, err := fresh.GetRoutingRule(domainName.DomainNameValue, routingRule.RoutingRuleID)
	require.NoError(t, err)
	assert.Equal(t, routingRule.Priority, gotRoutingRule.Priority)
	assert.Equal(t, routingRule.Actions, gotRoutingRule.Actions)
	assert.Equal(t, routingRule.Conditions, gotRoutingRule.Conditions)
	require.Len(t, gotRoutingRule.Actions, 1)
	require.NotNil(t, gotRoutingRule.Actions[0].InvokeAPI)
	assert.Equal(t, api.APIID, gotRoutingRule.Actions[0].InvokeAPI.APIID)
}

// TestInMemoryBackend_SnapshotRestore_EmptyBackend verifies an empty backend
// round-trips to another empty backend without error, matching
// services/apigateway's equivalent case.
func TestInMemoryBackend_SnapshotRestore_EmptyBackend(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := apigatewayv2.NewInMemoryBackend()
	require.NoError(t, fresh.Restore(t.Context(), snap))

	apis, err := fresh.GetAPIs()
	require.NoError(t, err)
	assert.Empty(t, apis)
}
