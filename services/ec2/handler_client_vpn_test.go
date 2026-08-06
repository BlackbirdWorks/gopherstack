package ec2_test

import (
	"net/http"
	"net/url"
	"strings"
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

// TestClientVPN_RoutesXMLElementName verifies DescribeClientVpnRoutes wraps the
// route list in <routes>, per aws-sdk-go-v2's DescribeClientVpnRoutesOutput
// "routes" case in deserializers.go (not <clientVpnRouteSet>).
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

// TestClientVpn_ExportImportCertificateRevocationList covers the
// Export/Import CRL round trip that used to be a static stub. Each subtest
// uses its own endpoint so they can safely run in parallel.
func TestClientVpn_ExportImportCertificateRevocationList(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	t.Run("export before import returns a well-formed empty CRL", func(t *testing.T) {
		t.Parallel()

		ep, createErr := b.CreateClientVpnEndpoint("10.0.0.0/22", "test", nil)
		require.NoError(t, createErr)

		crl, exportErr := b.ExportClientVpnClientCertificateRevocationList(ep.ClientVpnEndpointID)
		require.NoError(t, exportErr)
		assert.Contains(t, crl, "BEGIN X509 CRL")
	})

	t.Run("import then export round-trips the CRL body", func(t *testing.T) {
		t.Parallel()

		ep, createErr := b.CreateClientVpnEndpoint("10.0.0.0/22", "test", nil)
		require.NoError(t, createErr)

		body := "-----BEGIN X509 CRL-----\nMIIB...fake...\n-----END X509 CRL-----\n"
		require.NoError(t, b.ImportClientVpnClientCertificateRevocationList(ep.ClientVpnEndpointID, body))

		crl, exportErr := b.ExportClientVpnClientCertificateRevocationList(ep.ClientVpnEndpointID)
		require.NoError(t, exportErr)
		assert.Equal(t, body, crl)
	})

	t.Run("import on unknown endpoint returns not-found", func(t *testing.T) {
		t.Parallel()

		importErr := b.ImportClientVpnClientCertificateRevocationList("cvpn-endpoint-nonexistent", "body")
		require.Error(t, importErr)
	})

	t.Run("import with empty body returns invalid parameter", func(t *testing.T) {
		t.Parallel()

		ep, createErr := b.CreateClientVpnEndpoint("10.0.0.0/22", "test", nil)
		require.NoError(t, createErr)

		importErr := b.ImportClientVpnClientCertificateRevocationList(ep.ClientVpnEndpointID, "")
		require.Error(t, importErr)
	})

	t.Run("export on unknown endpoint returns not-found", func(t *testing.T) {
		t.Parallel()

		_, exportErr := b.ExportClientVpnClientCertificateRevocationList("cvpn-endpoint-nonexistent")
		require.Error(t, exportErr)
	})
}

// TestClientVpn_ExportClientConfiguration verifies the generated OpenVPN
// config references the endpoint's real DNS name and transport settings.
func TestClientVpn_ExportClientConfiguration(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	ep, err := b.CreateClientVpnEndpoint("10.0.0.0/22", "test", nil)
	require.NoError(t, err)

	config, err := b.ExportClientVpnClientConfiguration(ep.ClientVpnEndpointID)
	require.NoError(t, err)
	assert.Contains(t, config, ep.DNSName)
	assert.Contains(t, config, "proto udp")

	_, err = b.ExportClientVpnClientConfiguration("cvpn-endpoint-nonexistent")
	require.Error(t, err, "unknown endpoint must return not-found")

	_, err = b.ExportClientVpnClientConfiguration("")
	require.Error(t, err, "empty endpoint id must return invalid parameter")
}

// TestClientVpn_HTTPNotFoundIsBadRequest verifies that a not-found
// ClientVpnEndpointId maps to a 400 InvalidClientVpnEndpointId.NotFound error,
// not a 500 InternalFailure (the errCodeLookup table previously had no entry
// for ErrClientVpnEndpointNotFound).
func TestClientVpn_HTTPNotFoundIsBadRequest(t *testing.T) {
	t.Parallel()

	h := newHandler()

	rec := postForm(t, h, "Action=DeleteClientVpnEndpoint&Version=2016-11-15&ClientVpnEndpointId=cvpn-endpoint-missing")
	assert.Equal(t, 400, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidClientVpnEndpointId.NotFound")
}

// TestClientVpn_DescribeEndpointsXMLShape verifies the corrected wire shapes:
// the endpoint list is wrapped in <clientVpnEndpoint> (not the invented
// <clientVpnEndpointSet>), and the endpoint status is a nested <status><code>.
func TestClientVpn_DescribeEndpointsXMLShape(t *testing.T) {
	t.Parallel()

	h := newHandler()

	createResp, err := ec2.ExportDispatch(h, url.Values{
		"Action":          {"CreateClientVpnEndpoint"},
		"ClientCidrBlock": {"10.0.0.0/22"},
	})
	require.NoError(t, err)
	assert.Contains(t, createResp, "<clientVpnEndpoint>")
	assert.NotContains(t, createResp, "<clientVpnEndpointSet>")
	assert.Contains(t, createResp, "<status><code>available</code></status>")

	descResp, err := ec2.ExportDispatch(h, url.Values{"Action": {"DescribeClientVpnEndpoints"}})
	require.NoError(t, err)
	assert.Contains(t, descResp, "<clientVpnEndpoint>")
	assert.NotContains(t, descResp, "<clientVpnEndpointSet>")
}

// TestClientVpn_AssociateResponseIsFlat verifies AssociateClientVpnTargetNetwork's
// associationId/status are direct children of the response, not nested under
// an invented <associationStatus> wrapper.
func TestClientVpn_AssociateResponseIsFlat(t *testing.T) {
	t.Parallel()

	h := newHandler()

	ep, err := ec2.ExportDispatch(h, url.Values{
		"Action":          {"CreateClientVpnEndpoint"},
		"ClientCidrBlock": {"10.0.0.0/22"},
	})
	require.NoError(t, err)

	epID := extractClientVpnEndpointID(t, ep)

	assocResp, err := ec2.ExportDispatch(h, url.Values{
		"Action":              {"AssociateClientVpnTargetNetwork"},
		"ClientVpnEndpointId": {epID},
		"SubnetId":            {"subnet-default"},
	})
	require.NoError(t, err)
	assert.NotContains(t, assocResp, "<associationStatus>")
	assert.Contains(t, assocResp, "<associationId>cvpn-assoc-")
	assert.Contains(t, assocResp, "<status>associating</status>")
}

// TestClientVpn_AuthorizationRulesXMLShape verifies the corrected wrapper
// element name (<authorizationRule>, singular) and field name
// (<destinationCidr>, not <cidr>).
func TestClientVpn_AuthorizationRulesXMLShape(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	ep, err := b.CreateClientVpnEndpoint("10.0.0.0/22", "test", nil)
	require.NoError(t, err)

	require.NoError(t, b.AuthorizeClientVpnIngress(ep.ClientVpnEndpointID, "10.0.0.0/8", "private"))

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":              {"DescribeClientVpnAuthorizationRules"},
		"ClientVpnEndpointId": {ep.ClientVpnEndpointID},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<authorizationRule>")
	assert.NotContains(t, resp, "<authorizationRules>")
	assert.Contains(t, resp, "<destinationCidr>10.0.0.0/8</destinationCidr>")
}

// TestClientVpn_AuthorizeIngressHTTPUsesTargetNetworkCidr verifies the HTTP
// handler reads the CIDR from TargetNetworkCidr (the real AWS request field),
// not from the AuthorizeAllGroups boolean flag.
func TestClientVpn_AuthorizeIngressHTTPUsesTargetNetworkCidr(t *testing.T) {
	t.Parallel()

	h := newHandler()

	ep, err := ec2.ExportDispatch(h, url.Values{
		"Action":          {"CreateClientVpnEndpoint"},
		"ClientCidrBlock": {"10.0.0.0/22"},
	})
	require.NoError(t, err)
	epID := extractClientVpnEndpointID(t, ep)

	_, err = ec2.ExportDispatch(h, url.Values{
		"Action":              {"AuthorizeClientVpnIngress"},
		"ClientVpnEndpointId": {epID},
		"TargetNetworkCidr":   {"192.168.0.0/16"},
		"AuthorizeAllGroups":  {"true"},
	})
	require.NoError(t, err)

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":              {"DescribeClientVpnAuthorizationRules"},
		"ClientVpnEndpointId": {epID},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<destinationCidr>192.168.0.0/16</destinationCidr>")
}

// TestClientVpn_ApplySecurityGroupsPropagatesToTargetNetworks verifies that
// ApplySecurityGroupsToClientVpnTargetNetwork actually stores the security
// groups (previously a no-op that discarded its argument) and that they show
// up on the associated target networks.
func TestClientVpn_ApplySecurityGroupsPropagatesToTargetNetworks(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	ep, err := b.CreateClientVpnEndpoint("10.0.0.0/22", "test", nil)
	require.NoError(t, err)

	_, err = b.AssociateClientVpnTargetNetwork(ep.ClientVpnEndpointID, "subnet-default")
	require.NoError(t, err)

	require.NoError(t, b.ApplySecurityGroupsToClientVpnTargetNetwork(ep.ClientVpnEndpointID, []string{"sg-1", "sg-2"}))

	networks, err := b.DescribeClientVpnTargetNetworks(ep.ClientVpnEndpointID)
	require.NoError(t, err)
	require.Len(t, networks, 1)
	assert.Equal(t, []string{"sg-1", "sg-2"}, networks[0].SecurityGroups)

	// VPC ID should be derived from the associated subnet's known VPC.
	assert.Equal(t, "vpc-default", networks[0].VPCID)
}

// TestClientVpn_CreateWithOptionsCapturesAdvancedFields verifies the
// options-based create/modify path threads real values (transport protocol,
// VPC, security groups, split tunnel, ports) into the stored endpoint and
// that the HTTP handler surfaces them.
func TestClientVpn_CreateWithOptionsCapturesAdvancedFields(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	splitTunnel := true
	ep, err := b.CreateClientVpnEndpointWithOptions("10.0.0.0/22", "test", nil, ec2.ClientVpnEndpointOptions{
		ServerCertificateArn: "arn:aws:acm:us-east-1:123456789012:certificate/abc",
		TransportProtocol:    "tcp",
		VpcID:                "vpc-123",
		VpnPort:              1194,
		SplitTunnel:          &splitTunnel,
		SecurityGroupIDs:     []string{"sg-1"},
		SessionTimeoutHours:  8,
	})
	require.NoError(t, err)
	assert.Equal(t, "tcp", ep.TransportProtocol)
	assert.Equal(t, "vpc-123", ep.VPCID)
	assert.Equal(t, int32(1194), ep.VpnPort)
	assert.True(t, ep.SplitTunnel)
	assert.Equal(t, []string{"sg-1"}, ep.SecurityGroupIDs)
	assert.Equal(t, int32(8), ep.SessionTimeoutHours)

	// Modifying without SplitTunnel set must not reset it back to false.
	require.NoError(t, b.ModifyClientVpnEndpointWithOptions(
		ep.ClientVpnEndpointID, "new description", nil, ec2.ClientVpnEndpointOptions{},
	))
	eps := b.DescribeClientVpnEndpoints([]string{ep.ClientVpnEndpointID})
	require.Len(t, eps, 1)
	assert.True(t, eps[0].SplitTunnel, "SplitTunnel must be preserved when not explicitly modified")
	assert.Equal(t, "new description", eps[0].Description)
}

// TestClientVpn_TerminateConnectionsResponseShape verifies the response has
// no top-level <return> field (unlike most mutating EC2 ops) and instead
// echoes back the endpoint id with an empty connectionStatuses list, matching
// the real TerminateClientVpnConnectionsOutput shape.
func TestClientVpn_TerminateConnectionsResponseShape(t *testing.T) {
	t.Parallel()

	h := newHandler()

	ep, err := ec2.ExportDispatch(h, url.Values{
		"Action":          {"CreateClientVpnEndpoint"},
		"ClientCidrBlock": {"10.0.0.0/22"},
	})
	require.NoError(t, err)
	epID := extractClientVpnEndpointID(t, ep)

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":              {"TerminateClientVpnConnections"},
		"ClientVpnEndpointId": {epID},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<clientVpnEndpointId>"+epID+"</clientVpnEndpointId>")
	assert.Contains(t, resp, "<connectionStatuses></connectionStatuses>")
	assert.NotContains(t, resp, "<return>")
}

// TestClientVpn_DescribeConnectionsEmptyShape verifies the correct empty AWS
// shape for a resource family with no create path in this backend (no API
// here can establish an actual client connection).
func TestClientVpn_DescribeConnectionsEmptyShape(t *testing.T) {
	t.Parallel()

	h := newHandler()

	ep, err := ec2.ExportDispatch(h, url.Values{
		"Action":          {"CreateClientVpnEndpoint"},
		"ClientCidrBlock": {"10.0.0.0/22"},
	})
	require.NoError(t, err)
	epID := extractClientVpnEndpointID(t, ep)

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":              {"DescribeClientVpnConnections"},
		"ClientVpnEndpointId": {epID},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<connections></connections>")

	_, err = ec2.ExportDispatch(h, url.Values{
		"Action":              {"DescribeClientVpnConnections"},
		"ClientVpnEndpointId": {"cvpn-endpoint-missing"},
	})
	require.Error(t, err, "unknown endpoint must return not-found, not an empty list")
}

// TestClientVpn_ExportImportOpsViaHTTP exercises the three ops that used to
// be static stubs end-to-end through the HTTP handler.
func TestClientVpn_ExportImportOpsViaHTTP(t *testing.T) {
	t.Parallel()

	h := newHandler()

	ep, err := ec2.ExportDispatch(h, url.Values{
		"Action":          {"CreateClientVpnEndpoint"},
		"ClientCidrBlock": {"10.0.0.0/22"},
	})
	require.NoError(t, err)
	epID := extractClientVpnEndpointID(t, ep)

	t.Run("export client configuration", func(t *testing.T) {
		t.Parallel()

		resp, dispatchErr := ec2.ExportDispatch(h, url.Values{
			"Action":              {"ExportClientVpnClientConfiguration"},
			"ClientVpnEndpointId": {epID},
		})
		require.NoError(t, dispatchErr)
		assert.Contains(t, resp, "<clientConfiguration>")
	})

	t.Run("export CRL", func(t *testing.T) {
		t.Parallel()

		resp, dispatchErr := ec2.ExportDispatch(h, url.Values{
			"Action":              {"ExportClientVpnClientCertificateRevocationList"},
			"ClientVpnEndpointId": {epID},
		})
		require.NoError(t, dispatchErr)
		assert.Contains(t, resp, "<certificateRevocationList>")
	})

	t.Run("import CRL then re-export returns the imported body", func(t *testing.T) {
		t.Parallel()

		crlBody := "-----BEGIN X509 CRL-----\nfake\n-----END X509 CRL-----\n"
		_, dispatchErr := ec2.ExportDispatch(h, url.Values{
			"Action":                    {"ImportClientVpnClientCertificateRevocationList"},
			"ClientVpnEndpointId":       {epID},
			"CertificateRevocationList": {crlBody},
		})
		require.NoError(t, dispatchErr)

		resp, dispatchErr := ec2.ExportDispatch(h, url.Values{
			"Action":              {"ExportClientVpnClientCertificateRevocationList"},
			"ClientVpnEndpointId": {epID},
		})
		require.NoError(t, dispatchErr)
		assert.Contains(t, resp, "fake")
	})

	t.Run("missing endpoint id returns error", func(t *testing.T) {
		t.Parallel()

		_, dispatchErr := ec2.ExportDispatch(h, url.Values{
			"Action": {"ExportClientVpnClientConfiguration"},
		})
		require.Error(t, dispatchErr)
	})
}

// TestClientVpn_MultipleEndpointsListAndFilter is a basic list/filter
// round-trip covering more than one endpoint at a time.
func TestClientVpn_MultipleEndpointsListAndFilter(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	ep1, err := b.CreateClientVpnEndpoint("10.0.0.0/22", "first", nil)
	require.NoError(t, err)
	ep2, err := b.CreateClientVpnEndpoint("10.1.0.0/22", "second", nil)
	require.NoError(t, err)

	all := b.DescribeClientVpnEndpoints(nil)
	assert.Len(t, all, 2)

	filtered := b.DescribeClientVpnEndpoints([]string{ep2.ClientVpnEndpointID})
	require.Len(t, filtered, 1)
	assert.Equal(t, ep2.ClientVpnEndpointID, filtered[0].ClientVpnEndpointID)
	assert.NotEqual(t, ep1.ClientVpnEndpointID, filtered[0].ClientVpnEndpointID)

	none := b.DescribeClientVpnEndpoints([]string{"cvpn-endpoint-missing"})
	assert.Empty(t, none)
}

// extractClientVpnEndpointID pulls the text content of the first
// <clientVpnEndpointId> element out of an EC2 response body.
func extractClientVpnEndpointID(t *testing.T, body string) string {
	t.Helper()

	const open = "<clientVpnEndpointId>"
	const closeTag = "</clientVpnEndpointId>"

	start := strings.Index(body, open)
	require.GreaterOrEqual(t, start, 0, "clientVpnEndpointId tag not found in body: %s", body)
	start += len(open)

	end := strings.Index(body[start:], closeTag)
	require.GreaterOrEqual(t, end, 0, "clientVpnEndpointId closing tag not found in body: %s", body)

	return body[start : start+end]
}

// TestClientVpn_TransitGatewayAttachment verifies the 3 new parity-4
// operations (Accept/Reject/DeleteTransitGatewayClientVpnAttachment) against
// an attachment created implicitly by CreateClientVpnEndpoint's
// TransitGatewayConfiguration.TransitGatewayId, and that
// DescribeTransitGatewayAttachments surfaces it as a "client-vpn" resource.
func TestClientVpn_TransitGatewayAttachment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, b *ec2.InMemoryBackend) string
		name     string
		action   string
		wantBody []string
		wantCode int
	}{
		{
			name:   "accept_missing_attachment_not_found",
			action: "AcceptTransitGatewayClientVpnAttachment",
			setup: func(*testing.T, *ec2.InMemoryBackend) string {
				return "tgw-attach-missing"
			},
			wantCode: http.StatusBadRequest,
			wantBody: []string{"InvalidTransitGatewayAttachmentID.NotFound"},
		},
		{
			name:     "accept_transitions_pending_to_available",
			action:   "AcceptTransitGatewayClientVpnAttachment",
			setup:    seedTGWClientVpnAttachment,
			wantCode: http.StatusOK,
			wantBody: []string{
				"AcceptTransitGatewayClientVpnAttachmentResponse",
				"<state>available</state>",
			},
		},
		{
			name:     "reject_transitions_pending_to_rejected",
			action:   "RejectTransitGatewayClientVpnAttachment",
			setup:    seedTGWClientVpnAttachment,
			wantCode: http.StatusOK,
			wantBody: []string{
				"RejectTransitGatewayClientVpnAttachmentResponse",
				"<state>rejected</state>",
			},
		},
		{
			name:     "delete_removes_attachment",
			action:   "DeleteTransitGatewayClientVpnAttachment",
			setup:    seedTGWClientVpnAttachment,
			wantCode: http.StatusOK,
			wantBody: []string{
				"DeleteTransitGatewayClientVpnAttachmentResponse",
				"<state>deleted</state>",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
			h := ec2.NewHandler(b)

			attachmentID := tt.setup(t, b)

			rec := postForm(t, h, "Action="+tt.action+
				"&Version=2016-11-15&TransitGatewayAttachmentId="+attachmentID)
			assert.Equal(t, tt.wantCode, rec.Code)
			for _, want := range tt.wantBody {
				assert.Contains(t, rec.Body.String(), want)
			}
		})
	}
}

// seedTGWClientVpnAttachment creates a Transit Gateway and a Client VPN
// endpoint configured with TransitGatewayConfiguration, which implicitly
// creates a pending TransitGatewayClientVpnAttachment, and returns its ID
// (read back via DescribeTransitGatewayAttachments, since there is no
// standalone Describe op for this attachment type).
func seedTGWClientVpnAttachment(t *testing.T, b *ec2.InMemoryBackend) string {
	t.Helper()

	tgw, err := b.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "test"})
	require.NoError(t, err)

	_, err = b.CreateClientVpnEndpointWithOptions(
		"10.0.0.0/22", "tgw-backed", nil,
		ec2.ClientVpnEndpointOptions{TransitGatewayID: tgw.ID},
	)
	require.NoError(t, err)

	atts := b.DescribeTransitGatewayAttachments(nil)
	require.Len(t, atts, 1)
	assert.Equal(t, "client-vpn", atts[0].ResourceType)
	assert.Equal(t, "pending-acceptance", atts[0].State)

	return atts[0].TransitGatewayAttachmentID
}

// TestClientVpn_CreateWithUnknownTransitGatewayFails verifies
// CreateClientVpnEndpoint validates TransitGatewayConfiguration.TransitGatewayId
// against real transit gateway state rather than silently accepting any ID.
func TestClientVpn_CreateWithUnknownTransitGatewayFails(t *testing.T) {
	t.Parallel()

	h := newHandler()

	rec := postForm(t, h, "Action=CreateClientVpnEndpoint&Version=2016-11-15&"+
		"ClientCidrBlock=10.0.0.0/22&TransitGatewayConfiguration.TransitGatewayId=tgw-missing")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidTransitGatewayID.NotFound")
}
