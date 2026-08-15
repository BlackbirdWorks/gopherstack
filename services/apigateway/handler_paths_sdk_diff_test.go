package apigateway_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative method+path for every real apigateway
// operation in the apikeys/domainnames/domainnameaccessassociations/
// usageplans/vpclinks/clientcertificates families -- the ~30-40 ops
// gopherstack-4nek left unchecked when it verified the /restapis subtree
// (~90 ops). Extracted from apigateway@v1.42.4 serializers.go: each entry's
// "request.Method" and the string passed to httpbinding.SplitURI in that
// op's awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands in
// for any {Param} URI label -- the router does not validate ID shape, so the
// literal value doesn't matter here, only that the path matches Op.
// ImportApiKeys carries its real ?mode=import query flag, the load-bearing
// signal that distinguishes it from CreateApiKey sharing the same bare
// /apikeys path.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"CreateApiKey", "POST", "/apikeys"},
		{"CreateBasePathMapping", "POST", "/domainnames/PLACEHOLDER/basepathmappings"},
		{"CreateDomainName", "POST", "/domainnames"},
		{"CreateDomainNameAccessAssociation", "POST", "/domainnameaccessassociations"},
		{"CreateUsagePlan", "POST", "/usageplans"},
		{"CreateUsagePlanKey", "POST", "/usageplans/PLACEHOLDER/keys"},
		{"CreateVpcLink", "POST", "/vpclinks"},
		{"DeleteApiKey", "DELETE", "/apikeys/PLACEHOLDER"},
		{"DeleteBasePathMapping", "DELETE", "/domainnames/PLACEHOLDER/basepathmappings/PLACEHOLDER"},
		{"DeleteClientCertificate", "DELETE", "/clientcertificates/PLACEHOLDER"},
		{"DeleteDomainName", "DELETE", "/domainnames/PLACEHOLDER"},
		{"DeleteDomainNameAccessAssociation", "DELETE", "/domainnameaccessassociations/PLACEHOLDER"},
		{"DeleteUsagePlan", "DELETE", "/usageplans/PLACEHOLDER"},
		{"DeleteUsagePlanKey", "DELETE", "/usageplans/PLACEHOLDER/keys/PLACEHOLDER"},
		{"DeleteVpcLink", "DELETE", "/vpclinks/PLACEHOLDER"},
		{"GenerateClientCertificate", "POST", "/clientcertificates"},
		{"GetApiKey", "GET", "/apikeys/PLACEHOLDER"},
		{"GetApiKeys", "GET", "/apikeys"},
		{"GetBasePathMapping", "GET", "/domainnames/PLACEHOLDER/basepathmappings/PLACEHOLDER"},
		{"GetBasePathMappings", "GET", "/domainnames/PLACEHOLDER/basepathmappings"},
		{"GetClientCertificate", "GET", "/clientcertificates/PLACEHOLDER"},
		{"GetClientCertificates", "GET", "/clientcertificates"},
		{"GetDomainName", "GET", "/domainnames/PLACEHOLDER"},
		{"GetDomainNameAccessAssociations", "GET", "/domainnameaccessassociations"},
		{"GetDomainNames", "GET", "/domainnames"},
		{"GetUsage", "GET", "/usageplans/PLACEHOLDER/usage"},
		{"GetUsagePlan", "GET", "/usageplans/PLACEHOLDER"},
		{"GetUsagePlanKey", "GET", "/usageplans/PLACEHOLDER/keys/PLACEHOLDER"},
		{"GetUsagePlanKeys", "GET", "/usageplans/PLACEHOLDER/keys"},
		{"GetUsagePlans", "GET", "/usageplans"},
		{"GetVpcLink", "GET", "/vpclinks/PLACEHOLDER"},
		{"GetVpcLinks", "GET", "/vpclinks"},
		{"ImportApiKeys", "POST", "/apikeys?mode=import"},
		{"RejectDomainNameAccessAssociation", "POST", "/rejectdomainnameaccessassociations"},
		{"UpdateApiKey", "PATCH", "/apikeys/PLACEHOLDER"},
		{"UpdateBasePathMapping", "PATCH", "/domainnames/PLACEHOLDER/basepathmappings/PLACEHOLDER"},
		{"UpdateClientCertificate", "PATCH", "/clientcertificates/PLACEHOLDER"},
		{"UpdateDomainName", "PATCH", "/domainnames/PLACEHOLDER"},
		{"UpdateUsage", "PATCH", "/usageplans/PLACEHOLDER/keys/PLACEHOLDER/usage"},
		{"UpdateUsagePlan", "PATCH", "/usageplans/PLACEHOLDER"},
		{"UpdateVpcLink", "PATCH", "/vpclinks/PLACEHOLDER"},

		// The /restapis subtree (83 ops below), plus /account, /sdktypes and
		// /tags -- gopherstack-4nek/l5ir verified this subtree only with a
		// same-path collision check (do the router's own shared paths agree
		// on method?), not a per-op diff against the real SDK method+path.
		// gopherstack-0bq8 closes that gap: every op here is cross-checked
		// against apigateway@v1.42.4 serializers.go directly. Zero
		// mismatches found -- TestInvokeAuthorizer/TestInvokeMethod (POST,
		// sharing their bare resource path with Get/Update/Delete) were
		// already fixed by the time of this pass and are included here for
		// permanent regression coverage.
		{"CreateAuthorizer", "POST", "/restapis/PLACEHOLDER/authorizers"},
		{"CreateDeployment", "POST", "/restapis/PLACEHOLDER/deployments"},
		{"CreateDocumentationPart", "POST", "/restapis/PLACEHOLDER/documentation/parts"},
		{"CreateDocumentationVersion", "POST", "/restapis/PLACEHOLDER/documentation/versions"},
		{"CreateModel", "POST", "/restapis/PLACEHOLDER/models"},
		{"CreateRequestValidator", "POST", "/restapis/PLACEHOLDER/requestvalidators"},
		{"CreateResource", "POST", "/restapis/PLACEHOLDER/resources/PLACEHOLDER"},
		{"CreateRestApi", "POST", "/restapis"},
		{"CreateStage", "POST", "/restapis/PLACEHOLDER/stages"},
		{"DeleteAuthorizer", "DELETE", "/restapis/PLACEHOLDER/authorizers/PLACEHOLDER"},
		{"DeleteDeployment", "DELETE", "/restapis/PLACEHOLDER/deployments/PLACEHOLDER"},
		{"DeleteDocumentationPart", "DELETE", "/restapis/PLACEHOLDER/documentation/parts/PLACEHOLDER"},
		{"DeleteDocumentationVersion", "DELETE", "/restapis/PLACEHOLDER/documentation/versions/PLACEHOLDER"},
		{"DeleteGatewayResponse", "DELETE", "/restapis/PLACEHOLDER/gatewayresponses/PLACEHOLDER"},
		{"DeleteIntegration", "DELETE", "/restapis/PLACEHOLDER/resources/PLACEHOLDER/methods/PLACEHOLDER/integration"},
		{
			"DeleteIntegrationResponse", "DELETE",
			"/restapis/PLACEHOLDER/resources/PLACEHOLDER/methods/PLACEHOLDER/integration/responses/PLACEHOLDER",
		},
		{"DeleteMethod", "DELETE", "/restapis/PLACEHOLDER/resources/PLACEHOLDER/methods/PLACEHOLDER"},
		{
			"DeleteMethodResponse", "DELETE",
			"/restapis/PLACEHOLDER/resources/PLACEHOLDER/methods/PLACEHOLDER/responses/PLACEHOLDER",
		},
		{"DeleteModel", "DELETE", "/restapis/PLACEHOLDER/models/PLACEHOLDER"},
		{"DeleteRequestValidator", "DELETE", "/restapis/PLACEHOLDER/requestvalidators/PLACEHOLDER"},
		{"DeleteResource", "DELETE", "/restapis/PLACEHOLDER/resources/PLACEHOLDER"},
		{"DeleteRestApi", "DELETE", "/restapis/PLACEHOLDER"},
		{"DeleteStage", "DELETE", "/restapis/PLACEHOLDER/stages/PLACEHOLDER"},
		{"FlushStageAuthorizersCache", "DELETE", "/restapis/PLACEHOLDER/stages/PLACEHOLDER/cache/authorizers"},
		{"FlushStageCache", "DELETE", "/restapis/PLACEHOLDER/stages/PLACEHOLDER/cache/data"},
		{"GetAccount", "GET", "/account"},
		{"GetAuthorizer", "GET", "/restapis/PLACEHOLDER/authorizers/PLACEHOLDER"},
		{"GetAuthorizers", "GET", "/restapis/PLACEHOLDER/authorizers"},
		{"GetDeployment", "GET", "/restapis/PLACEHOLDER/deployments/PLACEHOLDER"},
		{"GetDeployments", "GET", "/restapis/PLACEHOLDER/deployments"},
		{"GetDocumentationPart", "GET", "/restapis/PLACEHOLDER/documentation/parts/PLACEHOLDER"},
		{"GetDocumentationParts", "GET", "/restapis/PLACEHOLDER/documentation/parts"},
		{"GetDocumentationVersion", "GET", "/restapis/PLACEHOLDER/documentation/versions/PLACEHOLDER"},
		{"GetDocumentationVersions", "GET", "/restapis/PLACEHOLDER/documentation/versions"},
		{"GetExport", "GET", "/restapis/PLACEHOLDER/stages/PLACEHOLDER/exports/PLACEHOLDER"},
		{"GetGatewayResponse", "GET", "/restapis/PLACEHOLDER/gatewayresponses/PLACEHOLDER"},
		{"GetGatewayResponses", "GET", "/restapis/PLACEHOLDER/gatewayresponses"},
		{"GetIntegration", "GET", "/restapis/PLACEHOLDER/resources/PLACEHOLDER/methods/PLACEHOLDER/integration"},
		{
			"GetIntegrationResponse", "GET",
			"/restapis/PLACEHOLDER/resources/PLACEHOLDER/methods/PLACEHOLDER/integration/responses/PLACEHOLDER",
		},
		{"GetMethod", "GET", "/restapis/PLACEHOLDER/resources/PLACEHOLDER/methods/PLACEHOLDER"},
		{
			"GetMethodResponse", "GET",
			"/restapis/PLACEHOLDER/resources/PLACEHOLDER/methods/PLACEHOLDER/responses/PLACEHOLDER",
		},
		{"GetModel", "GET", "/restapis/PLACEHOLDER/models/PLACEHOLDER"},
		{"GetModelTemplate", "GET", "/restapis/PLACEHOLDER/models/PLACEHOLDER/default_template"},
		{"GetModels", "GET", "/restapis/PLACEHOLDER/models"},
		{"GetRequestValidator", "GET", "/restapis/PLACEHOLDER/requestvalidators/PLACEHOLDER"},
		{"GetRequestValidators", "GET", "/restapis/PLACEHOLDER/requestvalidators"},
		{"GetResource", "GET", "/restapis/PLACEHOLDER/resources/PLACEHOLDER"},
		{"GetResources", "GET", "/restapis/PLACEHOLDER/resources"},
		{"GetRestApi", "GET", "/restapis/PLACEHOLDER"},
		{"GetRestApis", "GET", "/restapis"},
		{"GetSdk", "GET", "/restapis/PLACEHOLDER/stages/PLACEHOLDER/sdks/PLACEHOLDER"},
		{"GetSdkType", "GET", "/sdktypes/PLACEHOLDER"},
		{"GetSdkTypes", "GET", "/sdktypes"},
		{"GetStage", "GET", "/restapis/PLACEHOLDER/stages/PLACEHOLDER"},
		{"GetStages", "GET", "/restapis/PLACEHOLDER/stages"},
		{"GetTags", "GET", "/tags/PLACEHOLDER"},
		{"ImportDocumentationParts", "PUT", "/restapis/PLACEHOLDER/documentation/parts"},
		{"ImportRestApi", "POST", "/restapis?mode=import"},
		{"PutGatewayResponse", "PUT", "/restapis/PLACEHOLDER/gatewayresponses/PLACEHOLDER"},
		{"PutIntegration", "PUT", "/restapis/PLACEHOLDER/resources/PLACEHOLDER/methods/PLACEHOLDER/integration"},
		{
			"PutIntegrationResponse", "PUT",
			"/restapis/PLACEHOLDER/resources/PLACEHOLDER/methods/PLACEHOLDER/integration/responses/PLACEHOLDER",
		},
		{"PutMethod", "PUT", "/restapis/PLACEHOLDER/resources/PLACEHOLDER/methods/PLACEHOLDER"},
		{
			"PutMethodResponse", "PUT",
			"/restapis/PLACEHOLDER/resources/PLACEHOLDER/methods/PLACEHOLDER/responses/PLACEHOLDER",
		},
		{"PutRestApi", "PUT", "/restapis/PLACEHOLDER"},
		{"TagResource", "PUT", "/tags/PLACEHOLDER"},
		{"TestInvokeAuthorizer", "POST", "/restapis/PLACEHOLDER/authorizers/PLACEHOLDER"},
		{"TestInvokeMethod", "POST", "/restapis/PLACEHOLDER/resources/PLACEHOLDER/methods/PLACEHOLDER"},
		{"UntagResource", "DELETE", "/tags/PLACEHOLDER"},
		{"UpdateAccount", "PATCH", "/account"},
		{"UpdateAuthorizer", "PATCH", "/restapis/PLACEHOLDER/authorizers/PLACEHOLDER"},
		{"UpdateDeployment", "PATCH", "/restapis/PLACEHOLDER/deployments/PLACEHOLDER"},
		{"UpdateDocumentationPart", "PATCH", "/restapis/PLACEHOLDER/documentation/parts/PLACEHOLDER"},
		{"UpdateDocumentationVersion", "PATCH", "/restapis/PLACEHOLDER/documentation/versions/PLACEHOLDER"},
		{"UpdateGatewayResponse", "PATCH", "/restapis/PLACEHOLDER/gatewayresponses/PLACEHOLDER"},
		{"UpdateIntegration", "PATCH", "/restapis/PLACEHOLDER/resources/PLACEHOLDER/methods/PLACEHOLDER/integration"},
		{
			"UpdateIntegrationResponse", "PATCH",
			"/restapis/PLACEHOLDER/resources/PLACEHOLDER/methods/PLACEHOLDER/integration/responses/PLACEHOLDER",
		},
		{"UpdateMethod", "PATCH", "/restapis/PLACEHOLDER/resources/PLACEHOLDER/methods/PLACEHOLDER"},
		{
			"UpdateMethodResponse", "PATCH",
			"/restapis/PLACEHOLDER/resources/PLACEHOLDER/methods/PLACEHOLDER/responses/PLACEHOLDER",
		},
		{"UpdateModel", "PATCH", "/restapis/PLACEHOLDER/models/PLACEHOLDER"},
		{"UpdateRequestValidator", "PATCH", "/restapis/PLACEHOLDER/requestvalidators/PLACEHOLDER"},
		{"UpdateResource", "PATCH", "/restapis/PLACEHOLDER/resources/PLACEHOLDER"},
		{"UpdateRestApi", "PATCH", "/restapis/PLACEHOLDER"},
		{"UpdateStage", "PATCH", "/restapis/PLACEHOLDER/stages/PLACEHOLDER"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real apikeys/domainnames/
// usageplans/vpclinks/clientcertificates op's authoritative method+path (see
// sdkRouteCases) through ExtractOperation and asserts the route table
// resolves it to the right op. gopherstack-l5ir: zero mismatches found across
// all 41 ops, including ImportApiKeys/CreateApiKey sharing the bare /apikeys
// path and correctly disambiguated by the real ?mode=import query flag (not
// a bare flag or Operation=-style discriminator gopherstack got wrong -- the
// existing handler_router.go code already implemented this correctly).
// ExtractOperation calls parseAPIGWRESTPath directly -- the same function
// that performs real request dispatch -- so there is no separate,
// independently-maintained op-name-resolution path to drift out of sync,
// unlike several other services' ExtractOperation.
//
// It then drives the same request through the real Handler() and asserts it
// did not fall through to the "UnknownOperationException" errType that
// dispatch's map-lookup miss produces (handler.go:700-704) -- guarding
// against an action name that resolves correctly but has no entry in
// dispatchTable (gopherstack-ey26).
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	h := newAPIGWHandler()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			if got != tc.op {
				t.Errorf("method=%s path=%s: got op %q, want %q", tc.method, tc.path, got, tc.op)
			}

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "UnknownOperationException",
				"method=%s path=%s op=%s: dispatched to the unmatched-route handler", tc.method, tc.path, tc.op)
		})
	}
}
