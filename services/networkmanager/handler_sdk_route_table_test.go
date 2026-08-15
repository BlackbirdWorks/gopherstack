package networkmanager_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/networkmanager"
)

// sdkRouteCases is the authoritative method+path for every real Network
// Manager operation, extracted from networkmanager@v1.44.4 serializers.go:
// each entry's "request.Method" and the string passed to
// httpbinding.SplitURI in that op's awsRestjson1_serializeOp<Op>.
// HandleSerialize. PLACEHOLDER stands in for any {Param} URI label -- the
// router does not validate ID shape, so the literal value doesn't matter
// here, only that the path matches Op.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"AcceptAttachment", "POST", "/attachments/PLACEHOLDER/accept"},
		{"AssociateConnectPeer", "POST", "/global-networks/PLACEHOLDER/connect-peer-associations"},
		{"AssociateCustomerGateway", "POST", "/global-networks/PLACEHOLDER/customer-gateway-associations"},
		{"AssociateLink", "POST", "/global-networks/PLACEHOLDER/link-associations"},
		{
			"AssociateTransitGatewayConnectPeer", "POST",
			"/global-networks/PLACEHOLDER/transit-gateway-connect-peer-associations",
		},
		{"CreateConnectAttachment", "POST", "/connect-attachments"},
		{"CreateConnectPeer", "POST", "/connect-peers"},
		{"CreateConnection", "POST", "/global-networks/PLACEHOLDER/connections"},
		{"CreateCoreNetwork", "POST", "/core-networks"},
		{"CreateCoreNetworkPrefixListAssociation", "POST", "/prefix-list"},
		{"CreateDevice", "POST", "/global-networks/PLACEHOLDER/devices"},
		{"CreateDirectConnectGatewayAttachment", "POST", "/direct-connect-gateway-attachments"},
		{"CreateGlobalNetwork", "POST", "/global-networks"},
		{"CreateLink", "POST", "/global-networks/PLACEHOLDER/links"},
		{"CreateSite", "POST", "/global-networks/PLACEHOLDER/sites"},
		{"CreateSiteToSiteVpnAttachment", "POST", "/site-to-site-vpn-attachments"},
		{"CreateTransitGatewayPeering", "POST", "/transit-gateway-peerings"},
		{"CreateTransitGatewayRouteTableAttachment", "POST", "/transit-gateway-route-table-attachments"},
		{"CreateVpcAttachment", "POST", "/vpc-attachments"},
		{"DeleteAttachment", "DELETE", "/attachments/PLACEHOLDER"},
		{"DeleteConnectPeer", "DELETE", "/connect-peers/PLACEHOLDER"},
		{"DeleteConnection", "DELETE", "/global-networks/PLACEHOLDER/connections/PLACEHOLDER"},
		{"DeleteCoreNetwork", "DELETE", "/core-networks/PLACEHOLDER"},
		{
			"DeleteCoreNetworkPolicyVersion", "DELETE",
			"/core-networks/PLACEHOLDER/core-network-policy-versions/PLACEHOLDER",
		},
		{"DeleteCoreNetworkPrefixListAssociation", "DELETE", "/prefix-list/PLACEHOLDER/core-network/PLACEHOLDER"},
		{"DeleteDevice", "DELETE", "/global-networks/PLACEHOLDER/devices/PLACEHOLDER"},
		{"DeleteGlobalNetwork", "DELETE", "/global-networks/PLACEHOLDER"},
		{"DeleteLink", "DELETE", "/global-networks/PLACEHOLDER/links/PLACEHOLDER"},
		{"DeletePeering", "DELETE", "/peerings/PLACEHOLDER"},
		{"DeleteResourcePolicy", "DELETE", "/resource-policy/PLACEHOLDER"},
		{"DeleteSite", "DELETE", "/global-networks/PLACEHOLDER/sites/PLACEHOLDER"},
		{
			"DeregisterTransitGateway", "DELETE",
			"/global-networks/PLACEHOLDER/transit-gateway-registrations/PLACEHOLDER",
		},
		{"DescribeGlobalNetworks", "GET", "/global-networks"},
		{
			"DisassociateConnectPeer", "DELETE",
			"/global-networks/PLACEHOLDER/connect-peer-associations/PLACEHOLDER",
		},
		{
			"DisassociateCustomerGateway", "DELETE",
			"/global-networks/PLACEHOLDER/customer-gateway-associations/PLACEHOLDER",
		},
		{"DisassociateLink", "DELETE", "/global-networks/PLACEHOLDER/link-associations"},
		{
			"DisassociateTransitGatewayConnectPeer", "DELETE",
			"/global-networks/PLACEHOLDER/transit-gateway-connect-peer-associations/PLACEHOLDER",
		},
		{
			"ExecuteCoreNetworkChangeSet", "POST",
			"/core-networks/PLACEHOLDER/core-network-change-sets/PLACEHOLDER/execute",
		},
		{"GetConnectAttachment", "GET", "/connect-attachments/PLACEHOLDER"},
		{"GetConnectPeer", "GET", "/connect-peers/PLACEHOLDER"},
		{"GetConnectPeerAssociations", "GET", "/global-networks/PLACEHOLDER/connect-peer-associations"},
		{"GetConnections", "GET", "/global-networks/PLACEHOLDER/connections"},
		{"GetCoreNetwork", "GET", "/core-networks/PLACEHOLDER"},
		{
			"GetCoreNetworkChangeEvents", "GET",
			"/core-networks/PLACEHOLDER/core-network-change-events/PLACEHOLDER",
		},
		{"GetCoreNetworkChangeSet", "GET", "/core-networks/PLACEHOLDER/core-network-change-sets/PLACEHOLDER"},
		{"GetCoreNetworkPolicy", "GET", "/core-networks/PLACEHOLDER/core-network-policy"},
		{"GetCustomerGatewayAssociations", "GET", "/global-networks/PLACEHOLDER/customer-gateway-associations"},
		{"GetDevices", "GET", "/global-networks/PLACEHOLDER/devices"},
		{"GetDirectConnectGatewayAttachment", "GET", "/direct-connect-gateway-attachments/PLACEHOLDER"},
		{"GetLinkAssociations", "GET", "/global-networks/PLACEHOLDER/link-associations"},
		{"GetLinks", "GET", "/global-networks/PLACEHOLDER/links"},
		{"GetNetworkResourceCounts", "GET", "/global-networks/PLACEHOLDER/network-resource-count"},
		{"GetNetworkResourceRelationships", "GET", "/global-networks/PLACEHOLDER/network-resource-relationships"},
		{"GetNetworkResources", "GET", "/global-networks/PLACEHOLDER/network-resources"},
		{"GetNetworkRoutes", "POST", "/global-networks/PLACEHOLDER/network-routes"},
		{"GetNetworkTelemetry", "GET", "/global-networks/PLACEHOLDER/network-telemetry"},
		{"GetResourcePolicy", "GET", "/resource-policy/PLACEHOLDER"},
		{"GetRouteAnalysis", "GET", "/global-networks/PLACEHOLDER/route-analyses/PLACEHOLDER"},
		{"GetSiteToSiteVpnAttachment", "GET", "/site-to-site-vpn-attachments/PLACEHOLDER"},
		{"GetSites", "GET", "/global-networks/PLACEHOLDER/sites"},
		{
			"GetTransitGatewayConnectPeerAssociations", "GET",
			"/global-networks/PLACEHOLDER/transit-gateway-connect-peer-associations",
		},
		{"GetTransitGatewayPeering", "GET", "/transit-gateway-peerings/PLACEHOLDER"},
		{"GetTransitGatewayRegistrations", "GET", "/global-networks/PLACEHOLDER/transit-gateway-registrations"},
		{"GetTransitGatewayRouteTableAttachment", "GET", "/transit-gateway-route-table-attachments/PLACEHOLDER"},
		{"GetVpcAttachment", "GET", "/vpc-attachments/PLACEHOLDER"},
		{"ListAttachmentRoutingPolicyAssociations", "GET", "/routing-policy-label/core-network/PLACEHOLDER"},
		{"ListAttachments", "GET", "/attachments"},
		{"ListConnectPeers", "GET", "/connect-peers"},
		{"ListCoreNetworkPolicyVersions", "GET", "/core-networks/PLACEHOLDER/core-network-policy-versions"},
		{"ListCoreNetworkPrefixListAssociations", "GET", "/prefix-list/core-network/PLACEHOLDER"},
		{"ListCoreNetworkRoutingInformation", "POST", "/core-networks/PLACEHOLDER/core-network-routing-information"},
		{"ListCoreNetworks", "GET", "/core-networks"},
		{"ListOrganizationServiceAccessStatus", "GET", "/organizations/service-access"},
		{"ListPeerings", "GET", "/peerings"},
		{"ListTagsForResource", "GET", "/tags/PLACEHOLDER"},
		{"PutAttachmentRoutingPolicyLabel", "POST", "/routing-policy-label"},
		{"PutCoreNetworkPolicy", "POST", "/core-networks/PLACEHOLDER/core-network-policy"},
		{"PutResourcePolicy", "POST", "/resource-policy/PLACEHOLDER"},
		{"RegisterTransitGateway", "POST", "/global-networks/PLACEHOLDER/transit-gateway-registrations"},
		{"RejectAttachment", "POST", "/attachments/PLACEHOLDER/reject"},
		{
			"RemoveAttachmentRoutingPolicyLabel", "DELETE",
			"/routing-policy-label/core-network/PLACEHOLDER/attachment/PLACEHOLDER",
		},
		{
			"RestoreCoreNetworkPolicyVersion", "POST",
			"/core-networks/PLACEHOLDER/core-network-policy-versions/PLACEHOLDER/restore",
		},
		{"StartOrganizationServiceAccessUpdate", "POST", "/organizations/service-access"},
		{"StartRouteAnalysis", "POST", "/global-networks/PLACEHOLDER/route-analyses"},
		{"TagResource", "POST", "/tags/PLACEHOLDER"},
		{"UntagResource", "DELETE", "/tags/PLACEHOLDER"},
		{"UpdateConnection", "PATCH", "/global-networks/PLACEHOLDER/connections/PLACEHOLDER"},
		{"UpdateCoreNetwork", "PATCH", "/core-networks/PLACEHOLDER"},
		{"UpdateDevice", "PATCH", "/global-networks/PLACEHOLDER/devices/PLACEHOLDER"},
		{"UpdateDirectConnectGatewayAttachment", "PATCH", "/direct-connect-gateway-attachments/PLACEHOLDER"},
		{"UpdateGlobalNetwork", "PATCH", "/global-networks/PLACEHOLDER"},
		{"UpdateLink", "PATCH", "/global-networks/PLACEHOLDER/links/PLACEHOLDER"},
		{
			"UpdateNetworkResourceMetadata", "PATCH",
			"/global-networks/PLACEHOLDER/network-resources/PLACEHOLDER/metadata",
		},
		{"UpdateSite", "PATCH", "/global-networks/PLACEHOLDER/sites/PLACEHOLDER"},
		{"UpdateVpcAttachment", "PATCH", "/vpc-attachments/PLACEHOLDER"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Network Manager op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts the route table resolves it to the right op. gopherstack-jqh2 pass
// 2: re-extracted all 95 networkmanager ops from the pinned SDK and found
// the existing routeTable (handler.go) already correct -- no bugs.
//
// It then drives the same request through the real Handler() and asserts it
// did not fall through to the "unknown path" error handler.go:198-201 emits
// when matchRoute (the same lookup ExtractOperation calls) reports !ok --
// guarding against a request whose route lookup diverges between the two
// calls (gopherstack-ey26).
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	backend := networkmanager.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	t.Cleanup(backend.Close)

	h := networkmanager.NewHandler(backend)

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			require.Equal(t, tc.op, got, "method=%s path=%s", tc.method, tc.path)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "unknown path",
				"method=%s path=%s op=%s: dispatched to the unmatched-route handler", tc.method, tc.path, tc.op)
		})
	}
}
