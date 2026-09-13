package apigatewayv2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	apigatewayv2sdk "github.com/aws/aws-sdk-go-v2/service/apigatewayv2"
	apigatewayv2types "github.com/aws/aws-sdk-go-v2/service/apigatewayv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
)

// newSlice9APIGatewayV2Client is a top-level (not closure-local) client
// constructor: cmd/clientcoverage's census only traces a var's SDK-module
// binding through a named top-level function with a declared *pkg.Client
// return type, not a local func-literal variable, so keeping this as a
// top-level func is load-bearing for accurate typed-coverage measurement.
func newSlice9APIGatewayV2Client(t *testing.T) *apigatewayv2sdk.Client {
	t.Helper()

	return newTestAPIGatewayV2Client(
		t,
		apigatewayv2.NewHandler(apigatewayv2.NewInMemoryBackend()),
	)
}

// TestTypedSlice9RealClient drives apigatewayv2's typed-coverage-blind ops
// (gopherstack-n3zi slice 9) through the real aws-sdk-go-v2 apigatewayv2
// client, one subtest per named priority family, asserting decoded values.
func TestTypedSlice9RealClient(t *testing.T) {
	t.Parallel()

	t.Run("models", func(t *testing.T) {
		t.Parallel()

		client := newSlice9APIGatewayV2Client(t)

		api, err := client.CreateApi(t.Context(), &apigatewayv2sdk.CreateApiInput{
			Name:         aws.String("s9-model-api"),
			ProtocolType: apigatewayv2types.ProtocolTypeHttp,
		})
		require.NoError(t, err)

		createOut, err := client.CreateModel(t.Context(), &apigatewayv2sdk.CreateModelInput{
			ApiId: api.ApiId,
			Name:  aws.String("S9Model"),
			Schema: aws.String(
				`{"$schema":"http://json-schema.org/draft-04/schema#","title":"S9Model","type":"object"}`,
			),
		})
		require.NoError(t, err)

		getOut, err := client.GetModel(t.Context(), &apigatewayv2sdk.GetModelInput{
			ApiId:   api.ApiId,
			ModelId: createOut.ModelId,
		})
		require.NoError(t, err)
		assert.Equal(t, "S9Model", aws.ToString(getOut.Name))

		listOut, err := client.GetModels(
			t.Context(),
			&apigatewayv2sdk.GetModelsInput{ApiId: api.ApiId},
		)
		require.NoError(t, err)
		require.Len(t, listOut.Items, 1)

		templateOut, err := client.GetModelTemplate(
			t.Context(),
			&apigatewayv2sdk.GetModelTemplateInput{
				ApiId:   api.ApiId,
				ModelId: createOut.ModelId,
			},
		)
		require.NoError(t, err)
		assert.NotNil(t, templateOut.Value)

		updOut, err := client.UpdateModel(t.Context(), &apigatewayv2sdk.UpdateModelInput{
			ApiId:       api.ApiId,
			ModelId:     createOut.ModelId,
			Description: aws.String("s9 model desc"),
		})
		require.NoError(t, err)
		assert.Equal(t, "s9 model desc", aws.ToString(updOut.Description))

		_, err = client.DeleteModel(t.Context(), &apigatewayv2sdk.DeleteModelInput{
			ApiId:   api.ApiId,
			ModelId: createOut.ModelId,
		})
		require.NoError(t, err)
	})

	t.Run("integration responses", func(t *testing.T) {
		t.Parallel()

		client := newSlice9APIGatewayV2Client(t)

		api, err := client.CreateApi(t.Context(), &apigatewayv2sdk.CreateApiInput{
			Name:         aws.String("s9-ir-api"),
			ProtocolType: apigatewayv2types.ProtocolTypeHttp,
		})
		require.NoError(t, err)

		integration, err := client.CreateIntegration(
			t.Context(),
			&apigatewayv2sdk.CreateIntegrationInput{
				ApiId:           api.ApiId,
				IntegrationType: apigatewayv2types.IntegrationTypeHttpProxy,
				IntegrationUri:  aws.String("https://example.com"),
			},
		)
		require.NoError(t, err)

		createIROut, err := client.CreateIntegrationResponse(
			t.Context(),
			&apigatewayv2sdk.CreateIntegrationResponseInput{
				ApiId:                  api.ApiId,
				IntegrationId:          integration.IntegrationId,
				IntegrationResponseKey: aws.String("$default"),
			},
		)
		require.NoError(t, err)
		irID := createIROut.IntegrationResponseId

		getIROut, err := client.GetIntegrationResponse(
			t.Context(),
			&apigatewayv2sdk.GetIntegrationResponseInput{
				ApiId:                 api.ApiId,
				IntegrationId:         integration.IntegrationId,
				IntegrationResponseId: irID,
			},
		)
		require.NoError(t, err)
		assert.Equal(t, "$default", aws.ToString(getIROut.IntegrationResponseKey))

		updIROut, err := client.UpdateIntegrationResponse(
			t.Context(),
			&apigatewayv2sdk.UpdateIntegrationResponseInput{
				ApiId:                       api.ApiId,
				IntegrationId:               integration.IntegrationId,
				IntegrationResponseId:       irID,
				TemplateSelectionExpression: aws.String("$default"),
			},
		)
		require.NoError(t, err)
		assert.Equal(t, "$default", aws.ToString(updIROut.TemplateSelectionExpression))

		_, err = client.DeleteIntegrationResponse(
			t.Context(),
			&apigatewayv2sdk.DeleteIntegrationResponseInput{
				ApiId:                 api.ApiId,
				IntegrationId:         integration.IntegrationId,
				IntegrationResponseId: irID,
			},
		)
		require.NoError(t, err)
	})

	t.Run("route responses", func(t *testing.T) {
		t.Parallel()

		client := newSlice9APIGatewayV2Client(t)

		api, err := client.CreateApi(t.Context(), &apigatewayv2sdk.CreateApiInput{
			Name:         aws.String("s9-rr-api"),
			ProtocolType: apigatewayv2types.ProtocolTypeHttp,
		})
		require.NoError(t, err)

		route, err := client.CreateRoute(t.Context(), &apigatewayv2sdk.CreateRouteInput{
			ApiId:    api.ApiId,
			RouteKey: aws.String("GET /s9"),
		})
		require.NoError(t, err)

		createRROut, err := client.CreateRouteResponse(
			t.Context(),
			&apigatewayv2sdk.CreateRouteResponseInput{
				ApiId:            api.ApiId,
				RouteId:          route.RouteId,
				RouteResponseKey: aws.String("$default"),
			},
		)
		require.NoError(t, err)
		rrID := createRROut.RouteResponseId

		getRROut, err := client.GetRouteResponse(
			t.Context(),
			&apigatewayv2sdk.GetRouteResponseInput{
				ApiId:           api.ApiId,
				RouteId:         route.RouteId,
				RouteResponseId: rrID,
			},
		)
		require.NoError(t, err)
		assert.Equal(t, "$default", aws.ToString(getRROut.RouteResponseKey))

		updRROut, err := client.UpdateRouteResponse(
			t.Context(),
			&apigatewayv2sdk.UpdateRouteResponseInput{
				ApiId:                    api.ApiId,
				RouteId:                  route.RouteId,
				RouteResponseId:          rrID,
				ModelSelectionExpression: aws.String("$default"),
			},
		)
		require.NoError(t, err)
		assert.Equal(t, "$default", aws.ToString(updRROut.ModelSelectionExpression))

		_, err = client.DeleteRouteResponse(t.Context(), &apigatewayv2sdk.DeleteRouteResponseInput{
			ApiId:           api.ApiId,
			RouteId:         route.RouteId,
			RouteResponseId: rrID,
		})
		require.NoError(t, err)
	})

	t.Run("deployments and domain names", func(t *testing.T) {
		t.Parallel()

		client := newSlice9APIGatewayV2Client(t)

		api, err := client.CreateApi(t.Context(), &apigatewayv2sdk.CreateApiInput{
			Name:         aws.String("s9-dep-api"),
			ProtocolType: apigatewayv2types.ProtocolTypeHttp,
		})
		require.NoError(t, err)

		deployment, err := client.CreateDeployment(
			t.Context(),
			&apigatewayv2sdk.CreateDeploymentInput{
				ApiId: api.ApiId,
			},
		)
		require.NoError(t, err)

		updDepOut, err := client.UpdateDeployment(
			t.Context(),
			&apigatewayv2sdk.UpdateDeploymentInput{
				ApiId:        api.ApiId,
				DeploymentId: deployment.DeploymentId,
				Description:  aws.String("s9 deployment"),
			},
		)
		require.NoError(t, err)
		assert.Equal(t, "s9 deployment", aws.ToString(updDepOut.Description))

		_, err = client.CreateDomainName(t.Context(), &apigatewayv2sdk.CreateDomainNameInput{
			DomainName: aws.String("s9.example.com"),
		})
		require.NoError(t, err)

		updDomainOut, err := client.UpdateDomainName(
			t.Context(),
			&apigatewayv2sdk.UpdateDomainNameInput{
				DomainName: aws.String("s9.example.com"),
			},
		)
		require.NoError(t, err)
		assert.Equal(t, "s9.example.com", aws.ToString(updDomainOut.DomainName))

		_, err = client.CreateStage(t.Context(), &apigatewayv2sdk.CreateStageInput{
			ApiId:     api.ApiId,
			StageName: aws.String("s9-stage"),
		})
		require.NoError(t, err)

		mapping, err := client.CreateApiMapping(t.Context(), &apigatewayv2sdk.CreateApiMappingInput{
			ApiId:      api.ApiId,
			DomainName: aws.String("s9.example.com"),
			Stage:      aws.String("s9-stage"),
		})
		require.NoError(t, err)

		updMappingOut, err := client.UpdateApiMapping(
			t.Context(),
			&apigatewayv2sdk.UpdateApiMappingInput{
				ApiId:         api.ApiId,
				ApiMappingId:  mapping.ApiMappingId,
				DomainName:    aws.String("s9.example.com"),
				ApiMappingKey: aws.String("v1"),
			},
		)
		require.NoError(t, err)
		assert.Equal(t, "v1", aws.ToString(updMappingOut.ApiMappingKey))
	})

	t.Run("delete settings families", func(t *testing.T) {
		t.Parallel()

		client := newSlice9APIGatewayV2Client(t)

		api, err := client.CreateApi(t.Context(), &apigatewayv2sdk.CreateApiInput{
			Name:         aws.String("s9-del-settings-api"),
			ProtocolType: apigatewayv2types.ProtocolTypeHttp,
			CorsConfiguration: &apigatewayv2types.Cors{
				AllowOrigins: []string{"https://s9.example.com"},
			},
		})
		require.NoError(t, err)

		_, err = client.DeleteCorsConfiguration(
			t.Context(),
			&apigatewayv2sdk.DeleteCorsConfigurationInput{
				ApiId: api.ApiId,
			},
		)
		require.NoError(t, err)

		getAfterCORS, err := client.GetApi(
			t.Context(),
			&apigatewayv2sdk.GetApiInput{ApiId: api.ApiId},
		)
		require.NoError(t, err)
		assert.Nil(t, getAfterCORS.CorsConfiguration)

		_, err = client.CreateStage(t.Context(), &apigatewayv2sdk.CreateStageInput{
			ApiId:     api.ApiId,
			StageName: aws.String("s9-settings-stage"),
			AccessLogSettings: &apigatewayv2types.AccessLogSettings{
				DestinationArn: aws.String(
					"arn:aws:logs:us-east-1:000000000000:log-group:/s9/accesslogs",
				),
				Format: aws.String("$context.requestId"),
			},
			RouteSettings: map[string]apigatewayv2types.RouteSettings{
				"GET /s9": {ThrottlingBurstLimit: aws.Int32(10)},
			},
		})
		require.NoError(t, err)

		route, err := client.CreateRoute(t.Context(), &apigatewayv2sdk.CreateRouteInput{
			ApiId:    api.ApiId,
			RouteKey: aws.String("GET /s9"),
			RequestParameters: map[string]apigatewayv2types.ParameterConstraints{
				"route.request.querystring.q": {Required: aws.Bool(true)},
			},
		})
		require.NoError(t, err)

		_, err = client.DeleteRouteRequestParameter(
			t.Context(),
			&apigatewayv2sdk.DeleteRouteRequestParameterInput{
				ApiId:               api.ApiId,
				RouteId:             route.RouteId,
				RequestParameterKey: aws.String("route.request.querystring.q"),
			},
		)
		require.NoError(t, err)

		_, err = client.DeleteRouteSettings(t.Context(), &apigatewayv2sdk.DeleteRouteSettingsInput{
			ApiId:     api.ApiId,
			StageName: aws.String("s9-settings-stage"),
			RouteKey:  aws.String("GET /s9"),
		})
		require.NoError(t, err)

		_, err = client.ResetAuthorizersCache(
			t.Context(),
			&apigatewayv2sdk.ResetAuthorizersCacheInput{
				ApiId:     api.ApiId,
				StageName: aws.String("s9-settings-stage"),
			},
		)
		require.NoError(t, err)

		_, err = client.DeleteAccessLogSettings(
			t.Context(),
			&apigatewayv2sdk.DeleteAccessLogSettingsInput{
				ApiId:     api.ApiId,
				StageName: aws.String("s9-settings-stage"),
			},
		)
		require.NoError(t, err)
	})

	t.Run("routing rules", func(t *testing.T) {
		t.Parallel()

		client := newSlice9APIGatewayV2Client(t)

		_, err := client.CreateDomainName(t.Context(), &apigatewayv2sdk.CreateDomainNameInput{
			DomainName: aws.String("s9-routing.example.com"),
		})
		require.NoError(t, err)

		api, err := client.CreateApi(t.Context(), &apigatewayv2sdk.CreateApiInput{
			Name:         aws.String("s9-routing-api"),
			ProtocolType: apigatewayv2types.ProtocolTypeHttp,
		})
		require.NoError(t, err)

		_, err = client.CreateStage(t.Context(), &apigatewayv2sdk.CreateStageInput{
			ApiId:     api.ApiId,
			StageName: aws.String("s9-routing-stage"),
		})
		require.NoError(t, err)

		createOut, err := client.CreateRoutingRule(
			t.Context(),
			&apigatewayv2sdk.CreateRoutingRuleInput{
				DomainName: aws.String("s9-routing.example.com"),
				Priority:   aws.Int32(1),
				Actions: []apigatewayv2types.RoutingRuleAction{
					{InvokeApi: &apigatewayv2types.RoutingRuleActionInvokeApi{
						ApiId: api.ApiId,
						Stage: aws.String("s9-routing-stage"),
					}},
				},
				Conditions: []apigatewayv2types.RoutingRuleCondition{
					{
						MatchBasePaths: &apigatewayv2types.RoutingRuleMatchBasePaths{
							AnyOf: []string{"s9"},
						},
					},
				},
			},
		)
		require.NoError(t, err)
		routingRuleID := createOut.RoutingRuleId

		updOut, err := client.PutRoutingRule(t.Context(), &apigatewayv2sdk.PutRoutingRuleInput{
			DomainName:    aws.String("s9-routing.example.com"),
			RoutingRuleId: routingRuleID,
			Priority:      aws.Int32(2),
			Actions: []apigatewayv2types.RoutingRuleAction{
				{InvokeApi: &apigatewayv2types.RoutingRuleActionInvokeApi{
					ApiId: api.ApiId,
					Stage: aws.String("s9-routing-stage"),
				}},
			},
			Conditions: []apigatewayv2types.RoutingRuleCondition{
				{
					MatchBasePaths: &apigatewayv2types.RoutingRuleMatchBasePaths{
						AnyOf: []string{"s9-updated"},
					},
				},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, int32(2), aws.ToInt32(updOut.Priority))

		_, err = client.DeleteRoutingRule(t.Context(), &apigatewayv2sdk.DeleteRoutingRuleInput{
			DomainName:    aws.String("s9-routing.example.com"),
			RoutingRuleId: routingRuleID,
		})
		require.NoError(t, err)
	})

	t.Run("portals", func(t *testing.T) {
		t.Parallel()

		client := newSlice9APIGatewayV2Client(t)

		createPortalOut, err := client.CreatePortal(t.Context(), &apigatewayv2sdk.CreatePortalInput{
			Authorization: &apigatewayv2types.Authorization{
				None: &apigatewayv2types.None{},
			},
			EndpointConfiguration: &apigatewayv2types.EndpointConfigurationRequest{
				None: &apigatewayv2types.None{},
			},
			PortalContent: testPortalContent(),
		})
		require.NoError(t, err)
		portalID := createPortalOut.PortalId

		_, err = client.PreviewPortal(
			t.Context(),
			&apigatewayv2sdk.PreviewPortalInput{PortalId: portalID},
		)
		require.NoError(t, err)

		listPortalsOut, err := client.ListPortals(t.Context(), &apigatewayv2sdk.ListPortalsInput{})
		require.NoError(t, err)
		assert.NotEmpty(t, listPortalsOut.Items)

		createProductOut, err := client.CreatePortalProduct(
			t.Context(),
			&apigatewayv2sdk.CreatePortalProductInput{
				DisplayName: aws.String("s9-product"),
			},
		)
		require.NoError(t, err)
		portalProductID := createProductOut.PortalProductId

		listProductsOut, err := client.ListPortalProducts(
			t.Context(),
			&apigatewayv2sdk.ListPortalProductsInput{},
		)
		require.NoError(t, err)
		assert.NotEmpty(t, listProductsOut.Items)

		updProductOut, err := client.UpdatePortalProduct(
			t.Context(),
			&apigatewayv2sdk.UpdatePortalProductInput{
				PortalProductId: portalProductID,
				DisplayName:     aws.String("s9-product-renamed"),
			},
		)
		require.NoError(t, err)
		assert.Equal(t, "s9-product-renamed", aws.ToString(updProductOut.DisplayName))

		_, err = client.PutPortalProductSharingPolicy(
			t.Context(),
			&apigatewayv2sdk.PutPortalProductSharingPolicyInput{
				PortalProductId: portalProductID,
				PolicyDocument:  aws.String(`{"Version":"2012-10-17","Statement":[]}`),
			},
		)
		require.NoError(t, err)

		getPolicyOut, err := client.GetPortalProductSharingPolicy(
			t.Context(),
			&apigatewayv2sdk.GetPortalProductSharingPolicyInput{PortalProductId: portalProductID},
		)
		require.NoError(t, err)
		assert.NotEmpty(t, aws.ToString(getPolicyOut.PolicyDocument))

		_, err = client.DeletePortalProductSharingPolicy(
			t.Context(),
			&apigatewayv2sdk.DeletePortalProductSharingPolicyInput{
				PortalProductId: portalProductID,
			},
		)
		require.NoError(t, err)

		createPageOut, err := client.CreateProductPage(
			t.Context(),
			&apigatewayv2sdk.CreateProductPageInput{
				PortalProductId: portalProductID,
				DisplayContent: &apigatewayv2types.DisplayContent{
					Title: aws.String("s9 page"),
					Body:  aws.String("s9 page body"),
				},
			},
		)
		require.NoError(t, err)
		productPageID := createPageOut.ProductPageId

		listPagesOut, err := client.ListProductPages(
			t.Context(),
			&apigatewayv2sdk.ListProductPagesInput{
				PortalProductId: portalProductID,
			},
		)
		require.NoError(t, err)
		require.Len(t, listPagesOut.Items, 1)

		updPageOut, err := client.UpdateProductPage(
			t.Context(),
			&apigatewayv2sdk.UpdateProductPageInput{
				PortalProductId: portalProductID,
				ProductPageId:   productPageID,
				DisplayContent: &apigatewayv2types.DisplayContent{
					Title: aws.String("s9 page updated"),
					Body:  aws.String("s9 page body updated"),
				},
			},
		)
		require.NoError(t, err)
		assert.Equal(t, "s9 page updated", aws.ToString(updPageOut.DisplayContent.Title))

		_, err = client.DeleteProductPage(t.Context(), &apigatewayv2sdk.DeleteProductPageInput{
			PortalProductId: portalProductID,
			ProductPageId:   productPageID,
		})
		require.NoError(t, err)

		createEndpointPageOut, err := client.CreateProductRestEndpointPage(
			t.Context(),
			&apigatewayv2sdk.CreateProductRestEndpointPageInput{
				PortalProductId: portalProductID,
				RestEndpointIdentifier: &apigatewayv2types.RestEndpointIdentifier{
					IdentifierParts: &apigatewayv2types.IdentifierParts{
						RestApiId: aws.String("s9restapi"),
						Stage:     aws.String("prod"),
						Method:    aws.String("GET"),
						Path:      aws.String("/s9"),
					},
				},
				TryItState: apigatewayv2types.TryItStateEnabled,
			},
		)
		require.NoError(t, err)
		endpointPageID := createEndpointPageOut.ProductRestEndpointPageId

		listEndpointPagesOut, err := client.ListProductRestEndpointPages(
			t.Context(),
			&apigatewayv2sdk.ListProductRestEndpointPagesInput{PortalProductId: portalProductID},
		)
		require.NoError(t, err)
		require.Len(t, listEndpointPagesOut.Items, 1)

		getEndpointPageOut, err := client.GetProductRestEndpointPage(
			t.Context(),
			&apigatewayv2sdk.GetProductRestEndpointPageInput{
				PortalProductId:           portalProductID,
				ProductRestEndpointPageId: endpointPageID,
			},
		)
		require.NoError(t, err)
		assert.Equal(t, apigatewayv2types.TryItStateEnabled, getEndpointPageOut.TryItState)

		updEndpointPageOut, err := client.UpdateProductRestEndpointPage(
			t.Context(),
			&apigatewayv2sdk.UpdateProductRestEndpointPageInput{
				PortalProductId:           portalProductID,
				ProductRestEndpointPageId: endpointPageID,
				TryItState:                apigatewayv2types.TryItStateDisabled,
			},
		)
		require.NoError(t, err)
		assert.Equal(t, apigatewayv2types.TryItStateDisabled, updEndpointPageOut.TryItState)

		_, err = client.DeleteProductRestEndpointPage(
			t.Context(),
			&apigatewayv2sdk.DeleteProductRestEndpointPageInput{
				PortalProductId:           portalProductID,
				ProductRestEndpointPageId: endpointPageID,
			},
		)
		require.NoError(t, err)

		_, err = client.DeletePortalProduct(t.Context(), &apigatewayv2sdk.DeletePortalProductInput{
			PortalProductId: portalProductID,
		})
		require.NoError(t, err)
	})
}
