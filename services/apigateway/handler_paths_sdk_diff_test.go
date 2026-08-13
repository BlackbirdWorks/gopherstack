package apigateway_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
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
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	h := newAPIGWHandler()

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
