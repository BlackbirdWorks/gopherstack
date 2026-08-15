package elasticsearch_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative method+path for every real
// Elasticsearch (elasticsearchservice) operation, extracted from
// elasticsearchservice@v1.45.4 serializers.go: each entry's "request.Method"
// and the string passed to httpbinding.SplitURI in that op's
// awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands in for
// any {DomainName}/{PackageID}/{ElasticsearchVersion}/{...} URI label --
// ExtractOperation and ServeHTTP's own routing (handler.go/
// handler_routing.go) do not validate ID shape, so the literal value
// doesn't matter here, only that the path matches Op. 51 real ops here,
// matching elasticsearchservice's real op count exactly.
//
// A systematic check for a shared method+path across all 51 ops found zero
// collisions, so no *required dynamic* (non-template) member -- the
// s3/glacier vacuity-trap class -- was needed to disambiguate any route in
// this table.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"AcceptInboundCrossClusterSearchConnection", "PUT", "/2015-01-01/es/ccs/inboundConnection/PLACEHOLDER/accept"},
		{"AddTags", "POST", "/2015-01-01/tags"},
		{"AssociatePackage", "POST", "/2015-01-01/packages/associate/PLACEHOLDER/PLACEHOLDER"},
		{"AuthorizeVpcEndpointAccess", "POST", "/2015-01-01/es/domain/PLACEHOLDER/authorizeVpcEndpointAccess"},
		{"CancelDomainConfigChange", "POST", "/2015-01-01/es/domain/PLACEHOLDER/config/cancel"},
		{"CancelElasticsearchServiceSoftwareUpdate", "POST", "/2015-01-01/es/serviceSoftwareUpdate/cancel"},
		{"CreateElasticsearchDomain", "POST", "/2015-01-01/es/domain"},
		{"CreateOutboundCrossClusterSearchConnection", "POST", "/2015-01-01/es/ccs/outboundConnection"},
		{"CreatePackage", "POST", "/2015-01-01/packages"},
		{"CreateVpcEndpoint", "POST", "/2015-01-01/es/vpcEndpoints"},
		{"DeleteElasticsearchDomain", "DELETE", "/2015-01-01/es/domain/PLACEHOLDER"},
		{"DeleteElasticsearchServiceRole", "DELETE", "/2015-01-01/es/role"},
		{"DeleteInboundCrossClusterSearchConnection", "DELETE", "/2015-01-01/es/ccs/inboundConnection/PLACEHOLDER"},
		{"DeleteOutboundCrossClusterSearchConnection", "DELETE", "/2015-01-01/es/ccs/outboundConnection/PLACEHOLDER"},
		{"DeletePackage", "DELETE", "/2015-01-01/packages/PLACEHOLDER"},
		{"DeleteVpcEndpoint", "DELETE", "/2015-01-01/es/vpcEndpoints/PLACEHOLDER"},
		{"DescribeDomainAutoTunes", "GET", "/2015-01-01/es/domain/PLACEHOLDER/autoTunes"},
		{"DescribeDomainChangeProgress", "GET", "/2015-01-01/es/domain/PLACEHOLDER/progress"},
		{"DescribeElasticsearchDomain", "GET", "/2015-01-01/es/domain/PLACEHOLDER"},
		{"DescribeElasticsearchDomainConfig", "GET", "/2015-01-01/es/domain/PLACEHOLDER/config"},
		{"DescribeElasticsearchDomains", "POST", "/2015-01-01/es/domain-info"},
		{"DescribeElasticsearchInstanceTypeLimits", "GET", "/2015-01-01/es/instanceTypeLimits/PLACEHOLDER/PLACEHOLDER"},
		{"DescribeInboundCrossClusterSearchConnections", "POST", "/2015-01-01/es/ccs/inboundConnection/search"},
		{"DescribeOutboundCrossClusterSearchConnections", "POST", "/2015-01-01/es/ccs/outboundConnection/search"},
		{"DescribePackages", "POST", "/2015-01-01/packages/describe"},
		{"DescribeReservedElasticsearchInstanceOfferings", "GET", "/2015-01-01/es/reservedInstanceOfferings"},
		{"DescribeReservedElasticsearchInstances", "GET", "/2015-01-01/es/reservedInstances"},
		{"DescribeVpcEndpoints", "POST", "/2015-01-01/es/vpcEndpoints/describe"},
		{"DissociatePackage", "POST", "/2015-01-01/packages/dissociate/PLACEHOLDER/PLACEHOLDER"},
		{"GetCompatibleElasticsearchVersions", "GET", "/2015-01-01/es/compatibleVersions"},
		{"GetPackageVersionHistory", "GET", "/2015-01-01/packages/PLACEHOLDER/history"},
		{"GetUpgradeHistory", "GET", "/2015-01-01/es/upgradeDomain/PLACEHOLDER/history"},
		{"GetUpgradeStatus", "GET", "/2015-01-01/es/upgradeDomain/PLACEHOLDER/status"},
		{"ListDomainNames", "GET", "/2015-01-01/domain"},
		{"ListDomainsForPackage", "GET", "/2015-01-01/packages/PLACEHOLDER/domains"},
		{"ListElasticsearchInstanceTypes", "GET", "/2015-01-01/es/instanceTypes/PLACEHOLDER"},
		{"ListElasticsearchVersions", "GET", "/2015-01-01/es/versions"},
		{"ListPackagesForDomain", "GET", "/2015-01-01/domain/PLACEHOLDER/packages"},
		{"ListTags", "GET", "/2015-01-01/tags"},
		{"ListVpcEndpointAccess", "GET", "/2015-01-01/es/domain/PLACEHOLDER/listVpcEndpointAccess"},
		{"ListVpcEndpoints", "GET", "/2015-01-01/es/vpcEndpoints"},
		{"ListVpcEndpointsForDomain", "GET", "/2015-01-01/es/domain/PLACEHOLDER/vpcEndpoints"},
		{"PurchaseReservedElasticsearchInstanceOffering", "POST", "/2015-01-01/es/purchaseReservedInstanceOffering"},
		{"RejectInboundCrossClusterSearchConnection", "PUT", "/2015-01-01/es/ccs/inboundConnection/PLACEHOLDER/reject"},
		{"RemoveTags", "POST", "/2015-01-01/tags-removal"},
		{"RevokeVpcEndpointAccess", "POST", "/2015-01-01/es/domain/PLACEHOLDER/revokeVpcEndpointAccess"},
		{"StartElasticsearchServiceSoftwareUpdate", "POST", "/2015-01-01/es/serviceSoftwareUpdate/start"},
		{"UpdateElasticsearchDomainConfig", "POST", "/2015-01-01/es/domain/PLACEHOLDER/config"},
		{"UpdatePackage", "POST", "/2015-01-01/packages/update"},
		{"UpdateVpcEndpoint", "POST", "/2015-01-01/es/vpcEndpoints/update"},
		{"UpgradeElasticsearchDomain", "POST", "/2015-01-01/es/upgradeDomain"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Elasticsearch op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts it resolves to the right op, all 51 ops against
// elasticsearchservice's real op count. It then drives the same request
// through the real Handler() (which wraps ServeHTTP -- handler.go's Handle
// method) and asserts the response did not fall through to the literal
// "route not found" message that handleDomainRoutes's and
// handlePostDomainRoute's default cases (handler.go) both emit under
// ResourceNotFoundException when no case matches -- distinct from every
// domain-specific ResourceNotFoundException this service writes elsewhere
// (via writeOperationError/writeError with a dynamic err.Error() message
// naming the missing resource, e.g. "domain xyz not found"), none of which
// produce this exact literal.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			e := echo.New()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			require.Equal(t, tc.op, got, "method=%s path=%s", tc.method, tc.path)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "route not found",
				"method=%s path=%s op=%s: dispatched to the unmatched-route default", tc.method, tc.path, tc.op)
		})
	}
}
