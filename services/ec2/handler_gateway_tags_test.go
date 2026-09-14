package ec2_test

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreate_Tags_RoundTrip covers gopherstack-wjlrn: these Create handlers
// never called parseTagSpecification, so TagSpecifications were silently
// dropped. Each case creates a resource with a tag, then confirms the tag
// comes back on the matching Describe.
func TestCreate_Tags_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{name: "vpn gateway", run: testVpnGatewayCreateTags},
		{name: "customer gateway", run: testCustomerGatewayCreateTags},
		{name: "internet gateway", run: testInternetGatewayCreateTags},
		{name: "carrier gateway", run: testCarrierGatewayCreateTags},
		{name: "egress only internet gateway", run: testEgressOnlyIGWCreateTags},
		{name: "route table", run: testRouteTableCreateTags},
		{name: "vpn connection", run: testVpnConnectionCreateTags},
		{name: "network acl", run: testNetworkACLCreateTags},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

func testVpnGatewayCreateTags(t *testing.T) {
	t.Helper()

	h := newTestHandler()

	createResp, err := dispatchHandler(h, url.Values{
		"Action":                          []string{"CreateVpnGateway"},
		"Type":                            []string{"ipsec.1"},
		"TagSpecification.1.ResourceType": []string{"vpn-gateway"},
		"TagSpecification.1.Tag.1.Key":    []string{"Name"},
		"TagSpecification.1.Tag.1.Value":  []string{"demo"},
	})
	require.NoError(t, err)
	assert.Contains(t, createResp, "<key>Name</key><value>demo</value>")

	id := extractBetween(t, createResp, "<vpnGatewayId>", "</vpnGatewayId>")
	require.NotEmpty(t, id)

	describeResp, err := dispatchHandler(h, url.Values{
		"Action":         []string{"DescribeVpnGateways"},
		"VpnGatewayId.1": []string{id},
	})
	require.NoError(t, err)
	assert.Contains(t, describeResp, "<key>Name</key><value>demo</value>")
}

func testCustomerGatewayCreateTags(t *testing.T) {
	t.Helper()

	h := newTestHandler()

	createResp, err := dispatchHandler(h, url.Values{
		"Action":                          []string{"CreateCustomerGateway"},
		"Type":                            []string{"ipsec.1"},
		"IpAddress":                       []string{"203.0.113.1"},
		"BgpAsn":                          []string{"65000"},
		"TagSpecification.1.ResourceType": []string{"customer-gateway"},
		"TagSpecification.1.Tag.1.Key":    []string{"Name"},
		"TagSpecification.1.Tag.1.Value":  []string{"demo"},
	})
	require.NoError(t, err)
	assert.Contains(t, createResp, "<key>Name</key><value>demo</value>")

	id := extractBetween(t, createResp, "<customerGatewayId>", "</customerGatewayId>")
	require.NotEmpty(t, id)

	describeResp, err := dispatchHandler(h, url.Values{
		"Action":              []string{"DescribeCustomerGateways"},
		"CustomerGatewayId.1": []string{id},
	})
	require.NoError(t, err)
	assert.Contains(t, describeResp, "<key>Name</key><value>demo</value>")
}

func testInternetGatewayCreateTags(t *testing.T) {
	t.Helper()

	h := newTestHandler()

	createResp, err := dispatchHandler(h, url.Values{
		"Action":                          []string{"CreateInternetGateway"},
		"TagSpecification.1.ResourceType": []string{"internet-gateway"},
		"TagSpecification.1.Tag.1.Key":    []string{"Name"},
		"TagSpecification.1.Tag.1.Value":  []string{"demo"},
	})
	require.NoError(t, err)
	assert.Contains(t, createResp, "<key>Name</key><value>demo</value>")

	id := extractBetween(t, createResp, "<internetGatewayId>", "</internetGatewayId>")
	require.NotEmpty(t, id)

	describeResp, err := dispatchHandler(h, url.Values{
		"Action":              []string{"DescribeInternetGateways"},
		"InternetGatewayId.1": []string{id},
	})
	require.NoError(t, err)
	assert.Contains(t, describeResp, "<key>Name</key><value>demo</value>")
}

func testCarrierGatewayCreateTags(t *testing.T) {
	t.Helper()

	h := newTestHandler()

	createResp, err := dispatchHandler(h, url.Values{
		"Action":                          []string{"CreateCarrierGateway"},
		"VpcId":                           []string{"vpc-default"},
		"TagSpecification.1.ResourceType": []string{"carrier-gateway"},
		"TagSpecification.1.Tag.1.Key":    []string{"Name"},
		"TagSpecification.1.Tag.1.Value":  []string{"demo"},
	})
	require.NoError(t, err)
	assert.Contains(t, createResp, "<key>Name</key><value>demo</value>")

	id := extractBetween(t, createResp, "<carrierGatewayId>", "</carrierGatewayId>")
	require.NotEmpty(t, id)

	describeResp, err := dispatchHandler(h, url.Values{
		"Action":             []string{"DescribeCarrierGateways"},
		"CarrierGatewayId.1": []string{id},
	})
	require.NoError(t, err)
	assert.Contains(t, describeResp, "<key>Name</key><value>demo</value>")
}

func testEgressOnlyIGWCreateTags(t *testing.T) {
	t.Helper()

	h := newTestHandler()

	createResp, err := dispatchHandler(h, url.Values{
		"Action":                          []string{"CreateEgressOnlyInternetGateway"},
		"VpcId":                           []string{"vpc-default"},
		"TagSpecification.1.ResourceType": []string{"egress-only-internet-gateway"},
		"TagSpecification.1.Tag.1.Key":    []string{"Name"},
		"TagSpecification.1.Tag.1.Value":  []string{"demo"},
	})
	require.NoError(t, err)
	assert.Contains(t, createResp, "<key>Name</key><value>demo</value>")

	id := extractBetween(t, createResp, "<egressOnlyInternetGatewayId>", "</egressOnlyInternetGatewayId>")
	require.NotEmpty(t, id)

	describeResp, err := dispatchHandler(h, url.Values{
		"Action":                        []string{"DescribeEgressOnlyInternetGateways"},
		"EgressOnlyInternetGatewayId.1": []string{id},
	})
	require.NoError(t, err)
	assert.Contains(t, describeResp, "<key>Name</key><value>demo</value>")
}

func testRouteTableCreateTags(t *testing.T) {
	t.Helper()

	h := newTestHandler()

	createResp, err := dispatchHandler(h, url.Values{
		"Action":                          []string{"CreateRouteTable"},
		"VpcId":                           []string{"vpc-default"},
		"TagSpecification.1.ResourceType": []string{"route-table"},
		"TagSpecification.1.Tag.1.Key":    []string{"Name"},
		"TagSpecification.1.Tag.1.Value":  []string{"demo"},
	})
	require.NoError(t, err)
	assert.Contains(t, createResp, "<key>Name</key><value>demo</value>")

	id := extractBetween(t, createResp, "<routeTableId>", "</routeTableId>")
	require.NotEmpty(t, id)

	describeResp, err := dispatchHandler(h, url.Values{
		"Action":         []string{"DescribeRouteTables"},
		"RouteTableId.1": []string{id},
	})
	require.NoError(t, err)
	assert.Contains(t, describeResp, "<key>Name</key><value>demo</value>")
}

func testVpnConnectionCreateTags(t *testing.T) {
	t.Helper()

	h := newTestHandler()

	cgwResp, err := dispatchHandler(h, url.Values{
		"Action":    []string{"CreateCustomerGateway"},
		"Type":      []string{"ipsec.1"},
		"IpAddress": []string{"203.0.113.1"},
		"BgpAsn":    []string{"65000"},
	})
	require.NoError(t, err)
	cgwID := extractBetween(t, cgwResp, "<customerGatewayId>", "</customerGatewayId>")
	require.NotEmpty(t, cgwID)

	vgwResp, err := dispatchHandler(h, url.Values{
		"Action": []string{"CreateVpnGateway"},
		"Type":   []string{"ipsec.1"},
	})
	require.NoError(t, err)
	vgwID := extractBetween(t, vgwResp, "<vpnGatewayId>", "</vpnGatewayId>")
	require.NotEmpty(t, vgwID)

	createResp, err := dispatchHandler(h, url.Values{
		"Action":                          []string{"CreateVpnConnection"},
		"Type":                            []string{"ipsec.1"},
		"CustomerGatewayId":               []string{cgwID},
		"VpnGatewayId":                    []string{vgwID},
		"TagSpecification.1.ResourceType": []string{"vpn-connection"},
		"TagSpecification.1.Tag.1.Key":    []string{"Name"},
		"TagSpecification.1.Tag.1.Value":  []string{"demo"},
	})
	require.NoError(t, err)
	assert.Contains(t, createResp, "<key>Name</key><value>demo</value>")

	id := extractBetween(t, createResp, "<vpnConnectionId>", "</vpnConnectionId>")
	require.NotEmpty(t, id)

	describeResp, err := dispatchHandler(h, url.Values{
		"Action":            []string{"DescribeVpnConnections"},
		"VpnConnectionId.1": []string{id},
	})
	require.NoError(t, err)
	assert.Contains(t, describeResp, "<key>Name</key><value>demo</value>")
}

func testNetworkACLCreateTags(t *testing.T) {
	t.Helper()

	h := newTestHandler()

	createResp, err := dispatchHandler(h, url.Values{
		"Action":                          []string{"CreateNetworkAcl"},
		"VpcId":                           []string{"vpc-default"},
		"TagSpecification.1.ResourceType": []string{"network-acl"},
		"TagSpecification.1.Tag.1.Key":    []string{"Name"},
		"TagSpecification.1.Tag.1.Value":  []string{"demo"},
	})
	require.NoError(t, err)
	assert.Contains(t, createResp, "<key>Name</key><value>demo</value>")

	id := extractBetween(t, createResp, "<networkAclId>", "</networkAclId>")
	require.NotEmpty(t, id)

	describeResp, err := dispatchHandler(h, url.Values{
		"Action":         []string{"DescribeNetworkAcls"},
		"NetworkAclId.1": []string{id},
	})
	require.NoError(t, err)
	assert.Contains(t, describeResp, "<key>Name</key><value>demo</value>")
}
