package ec2_test

import (
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- Route Server HTTP dispatch integration tests ----

func TestRouteServer_HTTP_CreateDescribeModifyDelete(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler()

	createResp, err := dispatchHandler(h, url.Values{
		"Action":        []string{"CreateRouteServer"},
		"AmazonSideAsn": []string{"65000"},
	})
	require.NoError(t, err)
	assert.Contains(t, createResp, "<CreateRouteServerResponse")
	assert.Contains(t, createResp, "<routeServerId>rs-")
	assert.NotContains(t, createResp, "StubResponse")

	rsID := extractRouteServerID(t, createResp)

	describeResp, err := dispatchHandler(h, url.Values{
		"Action":          []string{"DescribeRouteServers"},
		"RouteServerId.1": []string{rsID},
	})
	require.NoError(t, err)
	assert.Contains(t, describeResp, "<DescribeRouteServersResponse")
	assert.Contains(t, describeResp, rsID)

	modifyResp, err := dispatchHandler(h, url.Values{
		"Action":             []string{"ModifyRouteServer"},
		"RouteServerId":      []string{rsID},
		"PersistRoutesState": []string{"disabled"},
	})
	require.NoError(t, err)
	assert.Contains(t, modifyResp, "<ModifyRouteServerResponse")
	assert.Contains(t, modifyResp, "<persistRoutesState>disabled")

	deleteResp, err := dispatchHandler(h, url.Values{
		"Action":        []string{"DeleteRouteServer"},
		"RouteServerId": []string{rsID},
	})
	require.NoError(t, err)
	assert.Contains(t, deleteResp, "<DeleteRouteServerResponse")

	// Missing AmazonSideAsn should error, not silently succeed as a stub would.
	_, err = dispatchHandler(h, url.Values{"Action": []string{"CreateRouteServer"}})
	require.Error(t, err)
}

func TestRouteServer_HTTP_EndpointAndPeer(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler()

	createResp, err := dispatchHandler(h, url.Values{
		"Action":        []string{"CreateRouteServer"},
		"AmazonSideAsn": []string{"65000"},
	})
	require.NoError(t, err)
	rsID := extractRouteServerID(t, createResp)

	epResp, err := dispatchHandler(h, url.Values{
		"Action":        []string{"CreateRouteServerEndpoint"},
		"RouteServerId": []string{rsID},
		"SubnetId":      []string{"subnet-default"},
	})
	require.NoError(t, err)
	assert.Contains(t, epResp, "<CreateRouteServerEndpointResponse")
	assert.Contains(t, epResp, "<routeServerEndpointId>rse-")
	assert.NotContains(t, epResp, "StubResponse")

	epID := extractBetween(t, epResp, "<routeServerEndpointId>", "</routeServerEndpointId>")

	describeEpResp, err := dispatchHandler(h, url.Values{"Action": []string{"DescribeRouteServerEndpoints"}})
	require.NoError(t, err)
	assert.Contains(t, describeEpResp, epID)

	peerResp, err := dispatchHandler(h, url.Values{
		"Action":                []string{"CreateRouteServerPeer"},
		"RouteServerEndpointId": []string{epID},
		"PeerAddress":           []string{"10.0.0.5"},
		"BgpOptions.PeerAsn":    []string{"65001"},
	})
	require.NoError(t, err)
	assert.Contains(t, peerResp, "<CreateRouteServerPeerResponse")
	assert.Contains(t, peerResp, "<routeServerPeerId>rsp-")
	assert.Contains(t, peerResp, "<peerAddress>10.0.0.5")
	assert.NotContains(t, peerResp, "StubResponse")

	peerID := extractBetween(t, peerResp, "<routeServerPeerId>", "</routeServerPeerId>")

	deletePeerResp, err := dispatchHandler(h, url.Values{
		"Action":            []string{"DeleteRouteServerPeer"},
		"RouteServerPeerId": []string{peerID},
	})
	require.NoError(t, err)
	assert.Contains(t, deletePeerResp, "<DeleteRouteServerPeerResponse")

	deleteEpResp, err := dispatchHandler(h, url.Values{
		"Action":                []string{"DeleteRouteServerEndpoint"},
		"RouteServerEndpointId": []string{epID},
	})
	require.NoError(t, err)
	assert.Contains(t, deleteEpResp, "<DeleteRouteServerEndpointResponse")
}

func TestRouteServer_HTTP_AssociationAndPropagation(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler()

	createResp, err := dispatchHandler(h, url.Values{
		"Action":        []string{"CreateRouteServer"},
		"AmazonSideAsn": []string{"65000"},
	})
	require.NoError(t, err)
	rsID := extractRouteServerID(t, createResp)

	assocResp, err := dispatchHandler(h, url.Values{
		"Action":        []string{"AssociateRouteServer"},
		"RouteServerId": []string{rsID},
		"VpcId":         []string{"vpc-default"},
	})
	require.NoError(t, err)
	assert.Contains(t, assocResp, "<AssociateRouteServerResponse")
	assert.Contains(t, assocResp, "<state>associated")

	getAssocResp, err := dispatchHandler(h, url.Values{
		"Action":        []string{"GetRouteServerAssociations"},
		"RouteServerId": []string{rsID},
	})
	require.NoError(t, err)
	assert.Contains(t, getAssocResp, "vpc-default")

	disassocResp, err := dispatchHandler(h, url.Values{
		"Action":        []string{"DisassociateRouteServer"},
		"RouteServerId": []string{rsID},
		"VpcId":         []string{"vpc-default"},
	})
	require.NoError(t, err)
	assert.Contains(t, disassocResp, "<DisassociateRouteServerResponse")

	rtResp, err := dispatchHandler(h, url.Values{
		"Action": []string{"CreateRouteTable"},
		"VpcId":  []string{"vpc-default"},
	})
	require.NoError(t, err)
	rtID := extractBetween(t, rtResp, "<routeTableId>", "</routeTableId>")
	require.NotEmpty(t, rtID)

	enableResp, err := dispatchHandler(h, url.Values{
		"Action":        []string{"EnableRouteServerPropagation"},
		"RouteServerId": []string{rsID},
		"RouteTableId":  []string{rtID},
	})
	require.NoError(t, err)
	assert.Contains(t, enableResp, "<EnableRouteServerPropagationResponse")
	assert.Contains(t, enableResp, "<state>enabled")

	getPropResp, err := dispatchHandler(h, url.Values{
		"Action":        []string{"GetRouteServerPropagations"},
		"RouteServerId": []string{rsID},
	})
	require.NoError(t, err)
	assert.Contains(t, getPropResp, rtID)

	disableResp, err := dispatchHandler(h, url.Values{
		"Action":        []string{"DisableRouteServerPropagation"},
		"RouteServerId": []string{rsID},
		"RouteTableId":  []string{rtID},
	})
	require.NoError(t, err)
	assert.Contains(t, disableResp, "<DisableRouteServerPropagationResponse")

	routingDBResp, err := dispatchHandler(h, url.Values{
		"Action":        []string{"GetRouteServerRoutingDatabase"},
		"RouteServerId": []string{rsID},
	})
	require.NoError(t, err)
	assert.Contains(t, routingDBResp, "<GetRouteServerRoutingDatabaseResponse")
}

func extractRouteServerID(t *testing.T, xmlResp string) string {
	t.Helper()

	return extractBetween(t, xmlResp, "<routeServerId>", "</routeServerId>")
}

func extractBetween(t *testing.T, s, start, end string) string {
	t.Helper()

	i := strings.Index(s, start)
	require.GreaterOrEqual(t, i, 0, "expected %q in %q", start, s)
	rest := s[i+len(start):]
	j := strings.Index(rest, end)
	require.GreaterOrEqual(t, j, 0, "expected %q in %q", end, rest)

	return rest[:j]
}

// TestUnshadowedBatch5Ops verifies the systemic stub-shadowing fix: these ops
// have real handlers registered in handler_batch5.go, and previously the
// stub registered after them in buildOps silently won the dispatch-table
// slot. They must now dispatch to the real handler and return the real
// response shape rather than the generic StubResponse.
func TestUnshadowedBatch5Ops(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler()

	tests := []struct {
		vals     url.Values
		wantRoot string
	}{
		{
			vals: url.Values{
				"Action": []string{"CreateFleet"},
				"Type":   []string{"instant"},
			},
			wantRoot: "<CreateFleetResponse",
		},
		{
			vals: url.Values{
				"Action": []string{"CreateCarrierGateway"},
				"VpcId":  []string{"vpc-default"},
			},
			wantRoot: "<CreateCarrierGatewayResponse",
		},
		{
			vals: url.Values{
				"Action": []string{"DescribeReservedInstancesOfferings"},
			},
			wantRoot: "<DescribeReservedInstancesOfferingsResponse",
		},
		{
			vals: url.Values{
				"Action": []string{"DescribeFleets"},
			},
			wantRoot: "<DescribeFleetsResponse",
		},
		{
			vals: url.Values{
				"Action": []string{"DescribeCarrierGateways"},
			},
			wantRoot: "<DescribeCarrierGatewaysResponse",
		},
	}

	for _, tt := range tests {
		resp, err := dispatchHandler(h, tt.vals)
		require.NoError(t, err)
		assert.Contains(t, resp, tt.wantRoot)
		assert.NotContains(t, resp, "StubResponse")
	}
}

// TestRegisterStubOpsIfAbsent_DoesNotShadowRealHandler is a focused
// regression test for the buildOps dispatch-table bug: a real handler
// registered before registerStubOpsIfAbsent must win, regardless of whether
// a stub for the same action name also exists.
func TestRegisterStubOpsIfAbsent_DoesNotShadowRealHandler(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler()

	// CreateRouteServer has both a real handler (registerRouteServerOps) and
	// a would-be stub entry in registerStubOps for the same action name.
	// The real handler must win.
	resp, err := dispatchHandler(h, url.Values{
		"Action":        []string{"CreateRouteServer"},
		"AmazonSideAsn": []string{"65000"},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<CreateRouteServerResponse")
	assert.NotContains(t, resp, "StubResponse")
}
