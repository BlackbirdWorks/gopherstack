package opensearch_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
)

// sdkRouteCases is the authoritative method+path for every real classic
// opensearch (control-plane) operation, extracted from opensearch@v1.75.4
// serializers.go: each entry's "request.Method" and the string passed to
// httpbinding.SplitURI in that op's awsRestjson1_serializeOp<Op>.HandleSerialize.
// PLACEHOLDER stands in for any {Param} URI label -- the router does not
// validate ID shape, so the literal value doesn't matter here, only that the
// path matches Op.
//
// Excludes the 19 OpenSearch Serverless (AOSS) ops this Handler also
// advertises: those belong to the separate opensearchserverless SDK
// client/protocol, not this one -- see serverlessOperations' doc comment in
// handler_operations.go.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"AcceptInboundConnection", "PUT", "/2021-01-01/opensearch/cc/inboundConnection/PLACEHOLDER/accept"},
		{"AddDataSource", "POST", "/2021-01-01/opensearch/domain/PLACEHOLDER/dataSource"},
		{"AddDirectQueryDataSource", "POST", "/2021-01-01/opensearch/directQueryDataSource"},
		{"AddTags", "POST", "/2021-01-01/tags"},
		{"AssociatePackage", "POST", "/2021-01-01/packages/associate/PLACEHOLDER/PLACEHOLDER"},
		{"AssociatePackages", "POST", "/2021-01-01/packages/associateMultiple"},
		{"AttachDataSource", "POST", "/2021-01-01/opensearch/application/PLACEHOLDER/attachDataSource"},
		{"AuthorizeVpcEndpointAccess", "POST", "/2021-01-01/opensearch/domain/PLACEHOLDER/authorizeVpcEndpointAccess"},
		{"CancelDomainConfigChange", "POST", "/2021-01-01/opensearch/domain/PLACEHOLDER/config/cancel"},
		{"CancelServiceSoftwareUpdate", "POST", "/2021-01-01/opensearch/serviceSoftwareUpdate/cancel"},
		{"CreateApplication", "POST", "/2021-01-01/opensearch/application"},
		{"CreateDomain", "POST", "/2021-01-01/opensearch/domain"},
		{"CreateIndex", "POST", "/2021-01-01/opensearch/domain/PLACEHOLDER/index"},
		{"CreateOutboundConnection", "POST", "/2021-01-01/opensearch/cc/outboundConnection"},
		{"CreatePackage", "POST", "/2021-01-01/packages"},
		{"CreateVpcEndpoint", "POST", "/2021-01-01/opensearch/vpcEndpoints"},
		{"DeleteApplication", "DELETE", "/2021-01-01/opensearch/application/PLACEHOLDER"},
		{"DeleteDataSource", "DELETE", "/2021-01-01/opensearch/domain/PLACEHOLDER/dataSource/PLACEHOLDER"},
		{"DeleteDirectQueryDataSource", "DELETE", "/2021-01-01/opensearch/directQueryDataSource/PLACEHOLDER"},
		{"DeleteDomain", "DELETE", "/2021-01-01/opensearch/domain/PLACEHOLDER"},
		{"DeleteInboundConnection", "DELETE", "/2021-01-01/opensearch/cc/inboundConnection/PLACEHOLDER"},
		{"DeleteIndex", "DELETE", "/2021-01-01/opensearch/domain/PLACEHOLDER/index/PLACEHOLDER"},
		{"DeleteOutboundConnection", "DELETE", "/2021-01-01/opensearch/cc/outboundConnection/PLACEHOLDER"},
		{"DeletePackage", "DELETE", "/2021-01-01/packages/PLACEHOLDER"},
		{"DeleteVpcEndpoint", "DELETE", "/2021-01-01/opensearch/vpcEndpoints/PLACEHOLDER"},
		{
			"DeregisterCapability", "DELETE",
			"/2021-01-01/opensearch/application/PLACEHOLDER/capability/deregister/PLACEHOLDER",
		},
		{
			"DescribeDataSourceAttachment", "POST",
			"/2021-01-01/opensearch/application/PLACEHOLDER/describeDataSourceAttachment",
		},
		{"DescribeDomain", "GET", "/2021-01-01/opensearch/domain/PLACEHOLDER"},
		{"DescribeDomainAutoTunes", "GET", "/2021-01-01/opensearch/domain/PLACEHOLDER/autoTunes"},
		{"DescribeDomainChangeProgress", "GET", "/2021-01-01/opensearch/domain/PLACEHOLDER/progress"},
		{"DescribeDomainConfig", "GET", "/2021-01-01/opensearch/domain/PLACEHOLDER/config"},
		{"DescribeDomainHealth", "GET", "/2021-01-01/opensearch/domain/PLACEHOLDER/health"},
		{"DescribeDomainNodes", "GET", "/2021-01-01/opensearch/domain/PLACEHOLDER/nodes"},
		{"DescribeDomains", "POST", "/2021-01-01/opensearch/domain-info"},
		{"DescribeDryRunProgress", "GET", "/2021-01-01/opensearch/domain/PLACEHOLDER/dryRun"},
		{"DescribeInboundConnections", "POST", "/2021-01-01/opensearch/cc/inboundConnection/search"},
		{"DescribeInsightDetails", "POST", "/2021-01-01/opensearch/insight-details"},
		{"DescribeInstanceTypeLimits", "GET", "/2021-01-01/opensearch/instanceTypeLimits/PLACEHOLDER/PLACEHOLDER"},
		{"DescribeOutboundConnections", "POST", "/2021-01-01/opensearch/cc/outboundConnection/search"},
		{"DescribePackages", "POST", "/2021-01-01/packages/describe"},
		{"DescribeReservedInstanceOfferings", "GET", "/2021-01-01/opensearch/reservedInstanceOfferings"},
		{"DescribeReservedInstances", "GET", "/2021-01-01/opensearch/reservedInstances"},
		{"DescribeVpcEndpoints", "POST", "/2021-01-01/opensearch/vpcEndpoints/describe"},
		{"DetachDataSource", "POST", "/2021-01-01/opensearch/application/PLACEHOLDER/detachDataSource"},
		{"DissociatePackage", "POST", "/2021-01-01/packages/dissociate/PLACEHOLDER/PLACEHOLDER"},
		{"DissociatePackages", "POST", "/2021-01-01/packages/dissociateMultiple"},
		{"GetApplication", "GET", "/2021-01-01/opensearch/application/PLACEHOLDER"},
		{"GetCapability", "GET", "/2021-01-01/opensearch/application/PLACEHOLDER/capability/PLACEHOLDER"},
		{"GetCompatibleVersions", "GET", "/2021-01-01/opensearch/compatibleVersions"},
		{"GetDataSource", "GET", "/2021-01-01/opensearch/domain/PLACEHOLDER/dataSource/PLACEHOLDER"},
		{"GetDefaultApplicationSetting", "GET", "/2021-01-01/opensearch/defaultApplicationSetting"},
		{"GetDirectQueryDataSource", "GET", "/2021-01-01/opensearch/directQueryDataSource/PLACEHOLDER"},
		{"GetDomainMaintenanceStatus", "GET", "/2021-01-01/opensearch/domain/PLACEHOLDER/domainMaintenance"},
		{"GetIndex", "GET", "/2021-01-01/opensearch/domain/PLACEHOLDER/index/PLACEHOLDER"},
		{"GetMigration", "GET", "/2021-01-01/opensearch/app-migrations/PLACEHOLDER"},
		{"GetPackageVersionHistory", "GET", "/2021-01-01/packages/PLACEHOLDER/history"},
		{"GetUpgradeHistory", "GET", "/2021-01-01/opensearch/upgradeDomain/PLACEHOLDER/history"},
		{"GetUpgradeStatus", "GET", "/2021-01-01/opensearch/upgradeDomain/PLACEHOLDER/status"},
		{"InsightFeedback", "POST", "/2021-01-01/opensearch/insight-feedback"},
		{"ListApplications", "GET", "/2021-01-01/opensearch/list-applications"},
		{
			"ListDataSourceAttachments", "POST",
			"/2021-01-01/opensearch/application/PLACEHOLDER/listDataSourceAttachments",
		},
		{"ListDataSources", "GET", "/2021-01-01/opensearch/domain/PLACEHOLDER/dataSource"},
		{"ListDirectQueryDataSources", "GET", "/2021-01-01/opensearch/directQueryDataSource"},
		{"ListDomainMaintenances", "GET", "/2021-01-01/opensearch/domain/PLACEHOLDER/domainMaintenances"},
		{"ListDomainNames", "GET", "/2021-01-01/domain"},
		{"ListDomainsForPackage", "GET", "/2021-01-01/packages/PLACEHOLDER/domains"},
		{"ListInsights", "POST", "/2021-01-01/opensearch/insights"},
		{"ListInstanceTypeDetails", "GET", "/2021-01-01/opensearch/instanceTypeDetails/PLACEHOLDER"},
		{"ListMigrations", "GET", "/2021-01-01/opensearch/app-migrations"},
		{"ListPackagesForDomain", "GET", "/2021-01-01/domain/PLACEHOLDER/packages"},
		{"ListScheduledActions", "GET", "/2021-01-01/opensearch/domain/PLACEHOLDER/scheduledActions"},
		{"ListTags", "GET", "/2021-01-01/tags"},
		{"ListVersions", "GET", "/2021-01-01/opensearch/versions"},
		{"ListVpcEndpointAccess", "GET", "/2021-01-01/opensearch/domain/PLACEHOLDER/listVpcEndpointAccess"},
		{"ListVpcEndpoints", "GET", "/2021-01-01/opensearch/vpcEndpoints"},
		{"ListVpcEndpointsForDomain", "GET", "/2021-01-01/opensearch/domain/PLACEHOLDER/vpcEndpoints"},
		{"PurchaseReservedInstanceOffering", "POST", "/2021-01-01/opensearch/purchaseReservedInstanceOffering"},
		{"PutDefaultApplicationSetting", "PUT", "/2021-01-01/opensearch/defaultApplicationSetting"},
		{"RegisterCapability", "POST", "/2021-01-01/opensearch/application/PLACEHOLDER/capability/register"},
		{"RejectInboundConnection", "PUT", "/2021-01-01/opensearch/cc/inboundConnection/PLACEHOLDER/reject"},
		{"RemoveTags", "POST", "/2021-01-01/tags-removal"},
		{"RevokeVpcEndpointAccess", "POST", "/2021-01-01/opensearch/domain/PLACEHOLDER/revokeVpcEndpointAccess"},
		{"RollbackServiceSoftwareUpdate", "POST", "/2021-01-01/opensearch/serviceSoftwareUpdate/rollback"},
		{"StartDomainMaintenance", "POST", "/2021-01-01/opensearch/domain/PLACEHOLDER/domainMaintenance"},
		{"StartMigration", "POST", "/2021-01-01/opensearch/app-migrations"},
		{"StartServiceSoftwareUpdate", "POST", "/2021-01-01/opensearch/serviceSoftwareUpdate/start"},
		{"UpdateApplication", "PUT", "/2021-01-01/opensearch/application/PLACEHOLDER"},
		{"UpdateDataSource", "PUT", "/2021-01-01/opensearch/domain/PLACEHOLDER/dataSource/PLACEHOLDER"},
		{"UpdateDirectQueryDataSource", "PUT", "/2021-01-01/opensearch/directQueryDataSource/PLACEHOLDER"},
		{"UpdateDomainConfig", "POST", "/2021-01-01/opensearch/domain/PLACEHOLDER/config"},
		{"UpdateIndex", "PUT", "/2021-01-01/opensearch/domain/PLACEHOLDER/index/PLACEHOLDER"},
		{"UpdatePackage", "POST", "/2021-01-01/packages/update"},
		{"UpdatePackageScope", "POST", "/2021-01-01/packages/updateScope"},
		{"UpdateScheduledAction", "PUT", "/2021-01-01/opensearch/domain/PLACEHOLDER/scheduledAction/update"},
		{"UpdateVpcEndpoint", "POST", "/2021-01-01/opensearch/vpcEndpoints/update"},
		{"UpgradeDomain", "POST", "/2021-01-01/opensearch/upgradeDomain"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real classic-opensearch
// op's authoritative method+path (see sdkRouteCases) through ExtractOperation
// and asserts the route table resolves it to the right op. gopherstack-l5ir:
// this audit found and fixed 22 unreachable/misrouted ops (UpdateDomainConfig,
// UpdateVpcEndpoint, DescribeInboundConnections, DescribeOutboundConnections,
// StartServiceSoftwareUpdate, DescribeDomains, ListDomainNames,
// ListPackagesForDomain, GetUpgradeHistory, GetUpgradeStatus,
// DissociatePackage, DescribePackages, UpdatePackage, UpdatePackageScope,
// ListApplications, DescribeReservedInstanceOfferings,
// PurchaseReservedInstanceOffering, ListInstanceTypeDetails,
// StartDomainMaintenance, ListDomainMaintenances, GetDomainMaintenanceStatus,
// CreateIndex) -- see PARITY.md for the full account.
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
