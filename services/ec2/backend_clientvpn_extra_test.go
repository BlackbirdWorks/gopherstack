package ec2_test

import (
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ec2 "github.com/blackbirdworks/gopherstack/services/ec2"
)

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
