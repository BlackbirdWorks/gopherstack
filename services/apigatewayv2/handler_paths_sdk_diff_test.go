package apigatewayv2_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
)

// sdkRouteCases is the authoritative method+path for every real apigatewayv2
// operation, extracted from apigatewayv2@v1.37.4 serializers.go: each entry's
// "request.Method" and the string passed to httpbinding.SplitURI in that op's
// awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands in for any
// {Param} URI label -- the router does not validate ID shape, so the literal
// value doesn't matter here, only that the path matches Op.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"CreateApi", "POST", "/v2/apis"},
		{"CreateApiMapping", "POST", "/v2/domainnames/PLACEHOLDER/apimappings"},
		{"CreateAuthorizer", "POST", "/v2/apis/PLACEHOLDER/authorizers"},
		{"CreateDeployment", "POST", "/v2/apis/PLACEHOLDER/deployments"},
		{"CreateDomainName", "POST", "/v2/domainnames"},
		{"CreateIntegration", "POST", "/v2/apis/PLACEHOLDER/integrations"},
		{"CreateIntegrationResponse", "POST", "/v2/apis/PLACEHOLDER/integrations/PLACEHOLDER/integrationresponses"},
		{"CreateModel", "POST", "/v2/apis/PLACEHOLDER/models"},
		{"CreatePortal", "POST", "/v2/portals"},
		{"CreatePortalProduct", "POST", "/v2/portalproducts"},
		{"CreateProductPage", "POST", "/v2/portalproducts/PLACEHOLDER/productpages"},
		{"CreateProductRestEndpointPage", "POST", "/v2/portalproducts/PLACEHOLDER/productrestendpointpages"},
		{"CreateRoute", "POST", "/v2/apis/PLACEHOLDER/routes"},
		{"CreateRouteResponse", "POST", "/v2/apis/PLACEHOLDER/routes/PLACEHOLDER/routeresponses"},
		{"CreateRoutingRule", "POST", "/v2/domainnames/PLACEHOLDER/routingrules"},
		{"CreateStage", "POST", "/v2/apis/PLACEHOLDER/stages"},
		{"CreateVpcLink", "POST", "/v2/vpclinks"},
		{"DeleteAccessLogSettings", "DELETE", "/v2/apis/PLACEHOLDER/stages/PLACEHOLDER/accesslogsettings"},
		{"DeleteApi", "DELETE", "/v2/apis/PLACEHOLDER"},
		{"DeleteApiMapping", "DELETE", "/v2/domainnames/PLACEHOLDER/apimappings/PLACEHOLDER"},
		{"DeleteAuthorizer", "DELETE", "/v2/apis/PLACEHOLDER/authorizers/PLACEHOLDER"},
		{"DeleteCorsConfiguration", "DELETE", "/v2/apis/PLACEHOLDER/cors"},
		{"DeleteDeployment", "DELETE", "/v2/apis/PLACEHOLDER/deployments/PLACEHOLDER"},
		{"DeleteDomainName", "DELETE", "/v2/domainnames/PLACEHOLDER"},
		{"DeleteIntegration", "DELETE", "/v2/apis/PLACEHOLDER/integrations/PLACEHOLDER"},
		{
			"DeleteIntegrationResponse", "DELETE",
			"/v2/apis/PLACEHOLDER/integrations/PLACEHOLDER/integrationresponses/PLACEHOLDER",
		},
		{"DeleteModel", "DELETE", "/v2/apis/PLACEHOLDER/models/PLACEHOLDER"},
		{"DeletePortal", "DELETE", "/v2/portals/PLACEHOLDER"},
		{"DeletePortalProduct", "DELETE", "/v2/portalproducts/PLACEHOLDER"},
		{"DeletePortalProductSharingPolicy", "DELETE", "/v2/portalproducts/PLACEHOLDER/sharingpolicy"},
		{"DeleteProductPage", "DELETE", "/v2/portalproducts/PLACEHOLDER/productpages/PLACEHOLDER"},
		{
			"DeleteProductRestEndpointPage", "DELETE",
			"/v2/portalproducts/PLACEHOLDER/productrestendpointpages/PLACEHOLDER",
		},
		{"DeleteRoute", "DELETE", "/v2/apis/PLACEHOLDER/routes/PLACEHOLDER"},
		{
			"DeleteRouteRequestParameter", "DELETE",
			"/v2/apis/PLACEHOLDER/routes/PLACEHOLDER/requestparameters/PLACEHOLDER",
		},
		{"DeleteRouteResponse", "DELETE", "/v2/apis/PLACEHOLDER/routes/PLACEHOLDER/routeresponses/PLACEHOLDER"},
		{"DeleteRouteSettings", "DELETE", "/v2/apis/PLACEHOLDER/stages/PLACEHOLDER/routesettings/PLACEHOLDER"},
		{"DeleteRoutingRule", "DELETE", "/v2/domainnames/PLACEHOLDER/routingrules/PLACEHOLDER"},
		{"DeleteStage", "DELETE", "/v2/apis/PLACEHOLDER/stages/PLACEHOLDER"},
		{"DeleteVpcLink", "DELETE", "/v2/vpclinks/PLACEHOLDER"},
		{"DisablePortal", "DELETE", "/v2/portals/PLACEHOLDER/publish"},
		{"ExportApi", "GET", "/v2/apis/PLACEHOLDER/exports/PLACEHOLDER"},
		{"GetApi", "GET", "/v2/apis/PLACEHOLDER"},
		{"GetApiMapping", "GET", "/v2/domainnames/PLACEHOLDER/apimappings/PLACEHOLDER"},
		{"GetApiMappings", "GET", "/v2/domainnames/PLACEHOLDER/apimappings"},
		{"GetApis", "GET", "/v2/apis"},
		{"GetAuthorizer", "GET", "/v2/apis/PLACEHOLDER/authorizers/PLACEHOLDER"},
		{"GetAuthorizers", "GET", "/v2/apis/PLACEHOLDER/authorizers"},
		{"GetDeployment", "GET", "/v2/apis/PLACEHOLDER/deployments/PLACEHOLDER"},
		{"GetDeployments", "GET", "/v2/apis/PLACEHOLDER/deployments"},
		{"GetDomainName", "GET", "/v2/domainnames/PLACEHOLDER"},
		{"GetDomainNames", "GET", "/v2/domainnames"},
		{"GetIntegration", "GET", "/v2/apis/PLACEHOLDER/integrations/PLACEHOLDER"},
		{
			"GetIntegrationResponse", "GET",
			"/v2/apis/PLACEHOLDER/integrations/PLACEHOLDER/integrationresponses/PLACEHOLDER",
		},
		{"GetIntegrationResponses", "GET", "/v2/apis/PLACEHOLDER/integrations/PLACEHOLDER/integrationresponses"},
		{"GetIntegrations", "GET", "/v2/apis/PLACEHOLDER/integrations"},
		{"GetModel", "GET", "/v2/apis/PLACEHOLDER/models/PLACEHOLDER"},
		{"GetModelTemplate", "GET", "/v2/apis/PLACEHOLDER/models/PLACEHOLDER/template"},
		{"GetModels", "GET", "/v2/apis/PLACEHOLDER/models"},
		{"GetPortal", "GET", "/v2/portals/PLACEHOLDER"},
		{"GetPortalProduct", "GET", "/v2/portalproducts/PLACEHOLDER"},
		{"GetPortalProductSharingPolicy", "GET", "/v2/portalproducts/PLACEHOLDER/sharingpolicy"},
		{"GetProductPage", "GET", "/v2/portalproducts/PLACEHOLDER/productpages/PLACEHOLDER"},
		{"GetProductRestEndpointPage", "GET", "/v2/portalproducts/PLACEHOLDER/productrestendpointpages/PLACEHOLDER"},
		{"GetRoute", "GET", "/v2/apis/PLACEHOLDER/routes/PLACEHOLDER"},
		{"GetRouteResponse", "GET", "/v2/apis/PLACEHOLDER/routes/PLACEHOLDER/routeresponses/PLACEHOLDER"},
		{"GetRouteResponses", "GET", "/v2/apis/PLACEHOLDER/routes/PLACEHOLDER/routeresponses"},
		{"GetRoutes", "GET", "/v2/apis/PLACEHOLDER/routes"},
		{"GetRoutingRule", "GET", "/v2/domainnames/PLACEHOLDER/routingrules/PLACEHOLDER"},
		{"GetStage", "GET", "/v2/apis/PLACEHOLDER/stages/PLACEHOLDER"},
		{"GetStages", "GET", "/v2/apis/PLACEHOLDER/stages"},
		{"GetTags", "GET", "/v2/tags/PLACEHOLDER"},
		{"GetVpcLink", "GET", "/v2/vpclinks/PLACEHOLDER"},
		{"GetVpcLinks", "GET", "/v2/vpclinks"},
		{"ImportApi", "PUT", "/v2/apis"},
		{"ListPortalProducts", "GET", "/v2/portalproducts"},
		{"ListPortals", "GET", "/v2/portals"},
		{"ListProductPages", "GET", "/v2/portalproducts/PLACEHOLDER/productpages"},
		{"ListProductRestEndpointPages", "GET", "/v2/portalproducts/PLACEHOLDER/productrestendpointpages"},
		{"ListRoutingRules", "GET", "/v2/domainnames/PLACEHOLDER/routingrules"},
		{"PreviewPortal", "POST", "/v2/portals/PLACEHOLDER/preview"},
		{"PublishPortal", "POST", "/v2/portals/PLACEHOLDER/publish"},
		{"PutPortalProductSharingPolicy", "PUT", "/v2/portalproducts/PLACEHOLDER/sharingpolicy"},
		{"PutRoutingRule", "PUT", "/v2/domainnames/PLACEHOLDER/routingrules/PLACEHOLDER"},
		{"ReimportApi", "PUT", "/v2/apis/PLACEHOLDER"},
		{"ResetAuthorizersCache", "DELETE", "/v2/apis/PLACEHOLDER/stages/PLACEHOLDER/cache/authorizers"},
		{"TagResource", "POST", "/v2/tags/PLACEHOLDER"},
		{"UntagResource", "DELETE", "/v2/tags/PLACEHOLDER"},
		{"UpdateApi", "PATCH", "/v2/apis/PLACEHOLDER"},
		{"UpdateApiMapping", "PATCH", "/v2/domainnames/PLACEHOLDER/apimappings/PLACEHOLDER"},
		{"UpdateAuthorizer", "PATCH", "/v2/apis/PLACEHOLDER/authorizers/PLACEHOLDER"},
		{"UpdateDeployment", "PATCH", "/v2/apis/PLACEHOLDER/deployments/PLACEHOLDER"},
		{"UpdateDomainName", "PATCH", "/v2/domainnames/PLACEHOLDER"},
		{"UpdateIntegration", "PATCH", "/v2/apis/PLACEHOLDER/integrations/PLACEHOLDER"},
		{
			"UpdateIntegrationResponse", "PATCH",
			"/v2/apis/PLACEHOLDER/integrations/PLACEHOLDER/integrationresponses/PLACEHOLDER",
		},
		{"UpdateModel", "PATCH", "/v2/apis/PLACEHOLDER/models/PLACEHOLDER"},
		{"UpdatePortal", "PATCH", "/v2/portals/PLACEHOLDER"},
		{"UpdatePortalProduct", "PATCH", "/v2/portalproducts/PLACEHOLDER"},
		{"UpdateProductPage", "PATCH", "/v2/portalproducts/PLACEHOLDER/productpages/PLACEHOLDER"},
		{
			"UpdateProductRestEndpointPage", "PATCH",
			"/v2/portalproducts/PLACEHOLDER/productrestendpointpages/PLACEHOLDER",
		},
		{"UpdateRoute", "PATCH", "/v2/apis/PLACEHOLDER/routes/PLACEHOLDER"},
		{"UpdateRouteResponse", "PATCH", "/v2/apis/PLACEHOLDER/routes/PLACEHOLDER/routeresponses/PLACEHOLDER"},
		{"UpdateStage", "PATCH", "/v2/apis/PLACEHOLDER/stages/PLACEHOLDER"},
		{"UpdateVpcLink", "PATCH", "/v2/vpclinks/PLACEHOLDER"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real apigatewayv2 op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts the route table resolves it to the right op. gopherstack-l5ir: none
// of these 103 ops was previously covered by a routing-verification test.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())

			got := h.ExtractOperation(c)
			if got != tc.op {
				t.Errorf("method=%s path=%s: got op %q, want %q", tc.method, tc.path, got, tc.op)
			}
		})
	}
}
