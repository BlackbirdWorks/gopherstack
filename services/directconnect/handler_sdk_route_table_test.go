package directconnect_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/directconnect"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real AWS Direct
// Connect operation, extracted from directconnect@v1.44.1 serializers.go:
// each op's awsAwsjson11_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String("OvertureService.<Op>")
// and always POSTs to "/" -- Direct Connect is JSON-RPC 1.1
// (services/_PROTOCOLS.md), so unlike a REST-family service there is no
// path template to get wrong: dispatch is entirely by this one header.
// "OvertureService" is Direct Connect's real internal AWS codename
// (confirmed directly from serializers.go, matching handler.go's own doc
// comment). ExtractOperation and Handler() (via h.dispatch's opTable() map,
// assembled by merging 6 op-family fragments) both derive the action the
// same way (TrimPrefix on "OvertureService."), so the class of bug this
// table catches is a dispatch-table key that doesn't exactly match the
// real op name (typo, wrong case -- Direct Connect is case-sensitive
// JSON-RPC), not a route-template mismatch.
//
// This table covers all 64 real Direct Connect ops (directconnect@v1.44.1)
// -- confirmed by diffing this SDK-extracted list against both
// GetSupportedOperations() (a hand-written literal) and the actual dispatch
// map assembled from all 6 opTable() family functions (connectionOps,
// lagAndInterconnectOps, vifOps, bgpOps, gatewayOps, staticAndTagOps -- each
// also a hand-written literal, not built by ranging over anything): zero
// mismatches in either direction, no dead, duplicate, or excluded keys
// across the 6 families. The two diffs are genuinely independent -- neither
// GetSupportedOperations nor opTable is derived from the other.
//
// STALE COMMENT, NOT FIXED (per task instructions): handler.go's own
// GetSupportedOperations doc comment and the directConnectTargetPrefix
// const's doc comment both say "63 operations" -- the pinned SDK actually
// has 64. The list itself is complete and correct (all 64 present,
// confirmed above); only the count in the prose is stale. Recorded here,
// not corrected, since routing is right.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("OvertureService.` and pulling the
// suffix after the last dot.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{
			"AcceptDirectConnectGatewayAssociationProposal",
			"OvertureService.AcceptDirectConnectGatewayAssociationProposal",
		},
		{"AllocateConnectionOnInterconnect", "OvertureService.AllocateConnectionOnInterconnect"},
		{"AllocateHostedConnection", "OvertureService.AllocateHostedConnection"},
		{"AllocatePrivateVirtualInterface", "OvertureService.AllocatePrivateVirtualInterface"},
		{"AllocatePublicVirtualInterface", "OvertureService.AllocatePublicVirtualInterface"},
		{"AllocateTransitVirtualInterface", "OvertureService.AllocateTransitVirtualInterface"},
		{"AssociateConnectionWithLag", "OvertureService.AssociateConnectionWithLag"},
		{"AssociateHostedConnection", "OvertureService.AssociateHostedConnection"},
		{"AssociateMacSecKey", "OvertureService.AssociateMacSecKey"},
		{"AssociateVirtualInterface", "OvertureService.AssociateVirtualInterface"},
		{"ConfirmConnection", "OvertureService.ConfirmConnection"},
		{"ConfirmCustomerAgreement", "OvertureService.ConfirmCustomerAgreement"},
		{"ConfirmPrivateVirtualInterface", "OvertureService.ConfirmPrivateVirtualInterface"},
		{"ConfirmPublicVirtualInterface", "OvertureService.ConfirmPublicVirtualInterface"},
		{"ConfirmTransitVirtualInterface", "OvertureService.ConfirmTransitVirtualInterface"},
		{"CreateBGPPeer", "OvertureService.CreateBGPPeer"},
		{"CreateConnection", "OvertureService.CreateConnection"},
		{"CreateDirectConnectGateway", "OvertureService.CreateDirectConnectGateway"},
		{"CreateDirectConnectGatewayAssociation", "OvertureService.CreateDirectConnectGatewayAssociation"},
		{
			"CreateDirectConnectGatewayAssociationProposal",
			"OvertureService.CreateDirectConnectGatewayAssociationProposal",
		},
		{"CreateInterconnect", "OvertureService.CreateInterconnect"},
		{"CreateLag", "OvertureService.CreateLag"},
		{"CreatePrivateVirtualInterface", "OvertureService.CreatePrivateVirtualInterface"},
		{"CreatePublicVirtualInterface", "OvertureService.CreatePublicVirtualInterface"},
		{"CreateTransitVirtualInterface", "OvertureService.CreateTransitVirtualInterface"},
		{"DeleteBGPPeer", "OvertureService.DeleteBGPPeer"},
		{"DeleteConnection", "OvertureService.DeleteConnection"},
		{"DeleteDirectConnectGateway", "OvertureService.DeleteDirectConnectGateway"},
		{"DeleteDirectConnectGatewayAssociation", "OvertureService.DeleteDirectConnectGatewayAssociation"},
		{
			"DeleteDirectConnectGatewayAssociationProposal",
			"OvertureService.DeleteDirectConnectGatewayAssociationProposal",
		},
		{"DeleteInterconnect", "OvertureService.DeleteInterconnect"},
		{"DeleteLag", "OvertureService.DeleteLag"},
		{"DeleteVirtualInterface", "OvertureService.DeleteVirtualInterface"},
		{"DescribeConnectionLoa", "OvertureService.DescribeConnectionLoa"},
		{"DescribeConnections", "OvertureService.DescribeConnections"},
		{"DescribeConnectionsOnInterconnect", "OvertureService.DescribeConnectionsOnInterconnect"},
		{"DescribeCustomerMetadata", "OvertureService.DescribeCustomerMetadata"},
		{
			"DescribeDirectConnectGatewayAssociationProposals",
			"OvertureService.DescribeDirectConnectGatewayAssociationProposals",
		},
		{"DescribeDirectConnectGatewayAssociations", "OvertureService.DescribeDirectConnectGatewayAssociations"},
		{"DescribeDirectConnectGatewayAttachments", "OvertureService.DescribeDirectConnectGatewayAttachments"},
		{"DescribeDirectConnectGateways", "OvertureService.DescribeDirectConnectGateways"},
		{"DescribeHostedConnections", "OvertureService.DescribeHostedConnections"},
		{"DescribeInterconnectLoa", "OvertureService.DescribeInterconnectLoa"},
		{"DescribeInterconnects", "OvertureService.DescribeInterconnects"},
		{"DescribeLags", "OvertureService.DescribeLags"},
		{"DescribeLoa", "OvertureService.DescribeLoa"},
		{"DescribeLocations", "OvertureService.DescribeLocations"},
		{"DescribeRouterConfiguration", "OvertureService.DescribeRouterConfiguration"},
		{"DescribeTags", "OvertureService.DescribeTags"},
		{"DescribeVirtualGateways", "OvertureService.DescribeVirtualGateways"},
		{"DescribeVirtualInterfaces", "OvertureService.DescribeVirtualInterfaces"},
		{"DisassociateConnectionFromLag", "OvertureService.DisassociateConnectionFromLag"},
		{"DisassociateMacSecKey", "OvertureService.DisassociateMacSecKey"},
		{"ListVirtualInterfaceRoutes", "OvertureService.ListVirtualInterfaceRoutes"},
		{"ListVirtualInterfaceTestHistory", "OvertureService.ListVirtualInterfaceTestHistory"},
		{"StartBgpFailoverTest", "OvertureService.StartBgpFailoverTest"},
		{"StopBgpFailoverTest", "OvertureService.StopBgpFailoverTest"},
		{"TagResource", "OvertureService.TagResource"},
		{"UntagResource", "OvertureService.UntagResource"},
		{"UpdateConnection", "OvertureService.UpdateConnection"},
		{"UpdateDirectConnectGateway", "OvertureService.UpdateDirectConnectGateway"},
		{"UpdateDirectConnectGatewayAssociation", "OvertureService.UpdateDirectConnectGatewayAssociation"},
		{"UpdateLag", "OvertureService.UpdateLag"},
		{"UpdateVirtualInterfaceAttributes", "OvertureService.UpdateVirtualInterfaceAttributes"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Direct Connect
// operation's authoritative X-Amz-Target through ExtractOperation and
// Handler(), asserting the header resolves to the right op name and that
// Handler() does not fall through to h.dispatch's single unmatched-route
// return (fmt.Errorf("%w: %s", errUnknownOperation, action), handler.go's
// dispatch() single production call site).
//
// This asserts on MESSAGE TEXT ("unknown Direct Connect operation"), not
// wire type -- classifyDirectConnectError's default case maps
// errUnknownOperation to "DirectConnectClientException", the generic
// catch-all this service uses for essentially every bad-input, not-found,
// and conflict condition (there is no dedicated ValidationException in this
// service's 5-shape error model -- see errors.go's own doc comments), so
// asserting on __type would be structurally unsafe here. errUnknownOperation's
// message ("unknown Direct Connect operation: <action>") has exactly one
// production call site (grepped) and is not produced by any other error
// path, so asserting on message text is safe.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			h := directconnect.NewHandler(directconnect.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1"))

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
			req.Header.Set("X-Amz-Target", tc.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, tc.op, got)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "unknown Direct Connect operation",
				"target=%s op=%s: dispatched to the unmatched-route handler", tc.target, tc.op)
		})
	}
}
