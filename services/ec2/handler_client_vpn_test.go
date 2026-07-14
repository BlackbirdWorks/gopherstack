package ec2_test

import (
	"net/url"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClientVpnEndpoint(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	var epID string

	t.Run("create endpoint", func(t *testing.T) { //nolint:paralleltest // existing issue.
		ep, err := b.CreateClientVpnEndpoint("10.0.0.0/22", "test vpn", nil)
		require.NoError(t, err)
		assert.NotEmpty(t, ep.ClientVpnEndpointID)
		assert.Equal(t, "available", ep.Status)
		assert.Equal(t, "10.0.0.0/22", ep.ClientCidrBlock)
		epID = ep.ClientVpnEndpointID
	})

	t.Run("describe returns endpoint", func(t *testing.T) { //nolint:paralleltest // existing issue.
		eps := b.DescribeClientVpnEndpoints([]string{epID})
		require.Len(t, eps, 1)
		assert.Equal(t, "10.0.0.0/22", eps[0].ClientCidrBlock)
	})

	t.Run("associate target network", func(t *testing.T) { //nolint:paralleltest // existing issue.
		assocID, assocErr := b.AssociateClientVpnTargetNetwork(epID, "subnet-default")
		require.NoError(t, assocErr)
		assert.NotEmpty(t, assocID)
		networks, err := b.DescribeClientVpnTargetNetworks(epID)
		require.NoError(t, err)
		require.Len(t, networks, 1)
		assert.Equal(t, "subnet-default", networks[0].SubnetID)
		assert.Equal(t, assocID, networks[0].AssociationID)
	})

	t.Run("disassociate target network", func(t *testing.T) { //nolint:paralleltest // existing issue.
		networks, err := b.DescribeClientVpnTargetNetworks(epID)
		require.NoError(t, err)
		require.Len(t, networks, 1)
		require.NoError(t, b.DisassociateClientVpnTargetNetwork(epID, networks[0].AssociationID))
		networks, err = b.DescribeClientVpnTargetNetworks(epID)
		require.NoError(t, err)
		assert.Empty(t, networks)
	})

	t.Run("create route", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.CreateClientVpnRoute(epID, "0.0.0.0/0", "default route"))
		routes, err := b.DescribeClientVpnRoutes(epID)
		require.NoError(t, err)
		require.Len(t, routes, 1)
		assert.Equal(t, "0.0.0.0/0", routes[0].DestinationCidr)
	})

	t.Run("delete route", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.DeleteClientVpnRoute(epID, "0.0.0.0/0"))
		routes, err := b.DescribeClientVpnRoutes(epID)
		require.NoError(t, err)
		assert.Empty(t, routes)
	})

	t.Run("authorize ingress", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.AuthorizeClientVpnIngress(epID, "10.0.0.0/8", "private"))
		rules, err := b.DescribeClientVpnAuthorizationRules(epID)
		require.NoError(t, err)
		require.Len(t, rules, 1)
		assert.Equal(t, "10.0.0.0/8", rules[0].Cidr)
	})

	t.Run("revoke ingress", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.RevokeClientVpnIngress(epID, "10.0.0.0/8"))
		rules, err := b.DescribeClientVpnAuthorizationRules(epID)
		require.NoError(t, err)
		assert.Empty(t, rules)
	})

	t.Run("modify endpoint", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.ModifyClientVpnEndpoint(epID, "updated vpn", nil))
		eps := b.DescribeClientVpnEndpoints([]string{epID})
		require.Len(t, eps, 1)
		assert.Equal(t, "updated vpn", eps[0].Description)
	})

	t.Run("describe connections returns empty", func(t *testing.T) { //nolint:paralleltest // existing issue.
		conns, err := b.DescribeClientVpnConnections(epID)
		require.NoError(t, err)
		assert.Empty(t, conns)
	})

	t.Run("terminate connections", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.TerminateClientVpnConnections(epID))
	})

	t.Run("apply security groups", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.ApplySecurityGroupsToClientVpnTargetNetwork(epID, []string{"sg-default"}))
	})

	t.Run("delete endpoint", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.DeleteClientVpnEndpoint(epID))
		eps := b.DescribeClientVpnEndpoints(nil)
		assert.Empty(t, eps)
	})

	t.Run("create with empty cidr returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.CreateClientVpnEndpoint("", "test", nil)
		require.Error(t, err)
	})
}

// TestClientVpnEndpointDNSNameRegion verifies the Client VPN endpoint
// DNS name reflects the backend's region rather than a hardcoded us-east-1
// (parity §C).

// TestClientVpnEndpointDNSNameRegion verifies the Client VPN endpoint
// DNS name reflects the backend's region rather than a hardcoded us-east-1
// (parity §C).
func TestClientVpnEndpointDNSNameRegion(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		region string
	}{
		{name: "us-west-2", region: "us-west-2"},
		{name: "eu-central-1", region: "eu-central-1"},
		{name: "ap-southeast-1", region: "ap-southeast-1"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := ec2.NewInMemoryBackend("000000000000", tc.region)
			ep, err := b.CreateClientVpnEndpoint("10.0.0.0/22", "region vpn", nil)
			require.NoError(t, err)
			assert.Contains(t, ep.DNSName, ".clientvpn."+tc.region+".amazonaws.com",
				"DNS name must reflect the request region")
			assert.NotContains(t, ep.DNSName, "us-east-1")
		})
	}
}

// ---- TransitGatewayPeeringAttachment ----.

// TestClientVPN_TargetNetworkHasAssociationID verifies that
// DescribeClientVpnTargetNetworks now returns associationId and status
// (was missing per parity.md §R).
func TestClientVPN_TargetNetworkHasAssociationID(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	ep, err := b.CreateClientVpnEndpoint("10.0.0.0/22", "test vpn", nil)
	require.NoError(t, err)

	// associate via handler
	assocResp, err := ec2.ExportDispatch(h, url.Values{
		"Action":              {"AssociateClientVpnTargetNetwork"},
		"ClientVpnEndpointId": {ep.ClientVpnEndpointID},
		"SubnetId":            {"subnet-default"},
	})
	require.NoError(t, err)
	assert.Contains(t, assocResp, "<associationId>cvpn-assoc-",
		"AssociateClientVpnTargetNetwork must return associationId")
	assert.Contains(t, assocResp, "<status>associating</status>",
		"AssociateClientVpnTargetNetwork must return status")

	// describe target networks
	descResp, err := ec2.ExportDispatch(h, url.Values{
		"Action":              {"DescribeClientVpnTargetNetworks"},
		"ClientVpnEndpointId": {ep.ClientVpnEndpointID},
	})
	require.NoError(t, err)
	assert.Contains(t, descResp, "<associationId>cvpn-assoc-",
		"DescribeClientVpnTargetNetworks must return associationId")
	assert.Contains(t, descResp, "<subnetId>subnet-default</subnetId>",
		"DescribeClientVpnTargetNetworks must return subnetId")
	assert.Contains(t, descResp, "<status>associated</status>",
		"DescribeClientVpnTargetNetworks must return status=associated")
}

// TestClientVPN_DisassociateByAssocID verifies DisassociateClientVpnTargetNetwork
// uses AssociationId (not SubnetId) as the key.

// TestClientVPN_DisassociateByAssocID verifies DisassociateClientVpnTargetNetwork
// uses AssociationId (not SubnetId) as the key.
func TestClientVPN_DisassociateByAssocID(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	ep, err := b.CreateClientVpnEndpoint("10.0.0.0/22", "test vpn", nil)
	require.NoError(t, err)

	assocID, err := b.AssociateClientVpnTargetNetwork(ep.ClientVpnEndpointID, "subnet-default")
	require.NoError(t, err)

	// disassociate by association ID
	_, err = ec2.ExportDispatch(h, url.Values{
		"Action":              {"DisassociateClientVpnTargetNetwork"},
		"ClientVpnEndpointId": {ep.ClientVpnEndpointID},
		"AssociationId":       {assocID},
	})
	require.NoError(t, err)

	// verify network is gone
	networks, err := b.DescribeClientVpnTargetNetworks(ep.ClientVpnEndpointID)
	require.NoError(t, err)
	assert.Empty(t, networks, "target network must be removed after disassociate")
}

// TestClientVPN_DisassociateWrongIDReturnsError verifies that
// disassociating with a non-existent association ID returns an error.

// TestClientVPN_DisassociateWrongIDReturnsError verifies that
// disassociating with a non-existent association ID returns an error.
func TestClientVPN_DisassociateWrongIDReturnsError(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	ep, err := b.CreateClientVpnEndpoint("10.0.0.0/22", "test vpn", nil)
	require.NoError(t, err)

	require.Error(t, b.DisassociateClientVpnTargetNetwork(ep.ClientVpnEndpointID, "cvpn-assoc-nonexistent"),
		"non-existent association ID must return error")
}

// TestClientVPN_RoutesXMLElementName verifies that
// DescribeClientVpnRoutes wraps the route list in <routes>, matching the real
// aws-sdk-go-v2 wire format (see DescribeClientVpnRoutesOutput's
// "routes" case in deserializers.go). A prior version of this test asserted
// the opposite (<clientVpnRouteSet>), which was itself the bug parity.md
// flagged under "EC2 — sub-resource ops (ClientVPN...)" ("ClientVPN routes
// use `routes` not `clientVpnRouteSet`") — this corrects the assertion to
// match the documented, SDK-verified fix rather than the bug.

// TestClientVPN_RoutesXMLElementName verifies that
// DescribeClientVpnRoutes wraps the route list in <routes>, matching the real
// aws-sdk-go-v2 wire format (see DescribeClientVpnRoutesOutput's
// "routes" case in deserializers.go). A prior version of this test asserted
// the opposite (<clientVpnRouteSet>), which was itself the bug parity.md
// flagged under "EC2 — sub-resource ops (ClientVPN...)" ("ClientVPN routes
// use `routes` not `clientVpnRouteSet`") — this corrects the assertion to
// match the documented, SDK-verified fix rather than the bug.
func TestClientVPN_RoutesXMLElementName(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	ep, err := b.CreateClientVpnEndpoint("10.0.0.0/22", "test vpn", nil)
	require.NoError(t, err)

	require.NoError(t, b.CreateClientVpnRoute(ep.ClientVpnEndpointID, "0.0.0.0/0", "default"))

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":              {"DescribeClientVpnRoutes"},
		"ClientVpnEndpointId": {ep.ClientVpnEndpointID},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<routes>",
		"routes must be wrapped in <routes> per the real aws-sdk-go-v2 wire format")
	assert.NotContains(t, resp, "<clientVpnRouteSet>",
		"must NOT use the incorrect <clientVpnRouteSet> element name")
}

// TestClientVPN_FullRouteCycle tests create/describe/delete routes.

// TestClientVPN_FullRouteCycle tests create/describe/delete routes.
func TestClientVPN_FullRouteCycle(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	ep, err := b.CreateClientVpnEndpoint("10.0.0.0/22", "test", nil)
	require.NoError(t, err)

	routes := []struct {
		cidr        string
		description string
	}{
		{cidr: "0.0.0.0/0", description: "internet"},
		{cidr: "10.0.0.0/8", description: "private"},
	}

	for _, r := range routes {
		createResp, createErr := ec2.ExportDispatch(h, url.Values{
			"Action":               {"CreateClientVpnRoute"},
			"ClientVpnEndpointId":  {ep.ClientVpnEndpointID},
			"DestinationCidrBlock": {r.cidr},
			"Description":          {r.description},
		})
		require.NoError(t, createErr)
		assert.Contains(t, createResp, "<return>true</return>")
	}

	// describe shows both routes
	descResp, err := ec2.ExportDispatch(h, url.Values{
		"Action":              {"DescribeClientVpnRoutes"},
		"ClientVpnEndpointId": {ep.ClientVpnEndpointID},
	})
	require.NoError(t, err)
	assert.Contains(t, descResp, "0.0.0.0/0")
	assert.Contains(t, descResp, "10.0.0.0/8")

	// delete one route
	_, err = ec2.ExportDispatch(h, url.Values{
		"Action":               {"DeleteClientVpnRoute"},
		"ClientVpnEndpointId":  {ep.ClientVpnEndpointID},
		"DestinationCidrBlock": {"10.0.0.0/8"},
	})
	require.NoError(t, err)

	// verify only one remains
	descResp2, err := ec2.ExportDispatch(h, url.Values{
		"Action":              {"DescribeClientVpnRoutes"},
		"ClientVpnEndpointId": {ep.ClientVpnEndpointID},
	})
	require.NoError(t, err)
	assert.Contains(t, descResp2, "0.0.0.0/0")
	assert.NotContains(t, descResp2, "10.0.0.0/8")
}

// TestClientVPN_MultipleTargetNetworks verifies multiple subnets
// can be associated to the same endpoint.

// TestClientVPN_MultipleTargetNetworks verifies multiple subnets
// can be associated to the same endpoint.
func TestClientVPN_MultipleTargetNetworks(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	ep, err := b.CreateClientVpnEndpoint("10.0.0.0/22", "test", nil)
	require.NoError(t, err)

	subnet2, err := b.CreateSubnet("vpc-default", "172.31.48.0/24", "us-east-1b")
	require.NoError(t, err)

	assocID1, err := b.AssociateClientVpnTargetNetwork(ep.ClientVpnEndpointID, "subnet-default")
	require.NoError(t, err)
	assert.NotEmpty(t, assocID1)

	assocID2, err := b.AssociateClientVpnTargetNetwork(ep.ClientVpnEndpointID, subnet2.ID)
	require.NoError(t, err)
	assert.NotEmpty(t, assocID2)
	assert.NotEqual(t, assocID1, assocID2, "each association must get a unique ID")

	networks, err := b.DescribeClientVpnTargetNetworks(ep.ClientVpnEndpointID)
	require.NoError(t, err)
	require.Len(t, networks, 2)

	subnetIDs := []string{networks[0].SubnetID, networks[1].SubnetID}
	assert.Contains(t, subnetIDs, "subnet-default")
	assert.Contains(t, subnetIDs, subnet2.ID)
}

// TestClientVPN_IdempotentAssociate verifies that associating
// the same subnet twice returns the same association ID.

// TestClientVPN_IdempotentAssociate verifies that associating
// the same subnet twice returns the same association ID.
func TestClientVPN_IdempotentAssociate(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	ep, err := b.CreateClientVpnEndpoint("10.0.0.0/22", "test", nil)
	require.NoError(t, err)

	assocID1, err := b.AssociateClientVpnTargetNetwork(ep.ClientVpnEndpointID, "subnet-default")
	require.NoError(t, err)

	assocID2, err := b.AssociateClientVpnTargetNetwork(ep.ClientVpnEndpointID, "subnet-default")
	require.NoError(t, err)

	assert.Equal(t, assocID1, assocID2, "idempotent associate must return same assocID")

	// only one network should exist
	networks, err := b.DescribeClientVpnTargetNetworks(ep.ClientVpnEndpointID)
	require.NoError(t, err)
	assert.Len(t, networks, 1)
}

// ============================================================================
// Transit Gateway peering / connect accuracy
// ============================================================================

// TestTGW_PeeringAttachmentCRUD verifies TGW peering full CRUD cycle.
