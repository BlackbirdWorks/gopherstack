package ec2_test

import (
	"testing"

	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestDescribeVpcEndpointServices_ServiceDetails_RealClient covers
// handleDescribeVpcEndpointServices, which had two pre-fix bugs. First,
// serviceNameSet's <item> elements wrapped the value in a nested
// <serviceName> child instead of holding it as plain text, so the real
// client's ValueStringList decoder (ec2@v1.319.1 deserializers.go,
// awsEc2query_deserializeDocumentValueStringList expects decoder.Value() at
// "item") failed outright rather than silently dropping data. Second, the
// real DescribeVpcEndpointServicesOutput also carries ServiceDetails
// (awsEc2query_deserializeOpDocumentDescribeVpcEndpointServicesOutput
// matches both "serviceNameSet" and "serviceDetailSet"), which is the field
// real client code reads for ServiceType/Owner/etc; gopherstack never
// emitted serviceDetailSet at all, so that field was always an empty slice
// despite HTTP 200/err==nil.
func TestDescribeVpcEndpointServices_ServiceDetails_RealClient(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	client := newTestEC2Client(t, h)

	out, err := client.DescribeVpcEndpointServices(t.Context(), &ec2sdk.DescribeVpcEndpointServicesInput{})
	require.NoError(t, err)
	require.NotEmpty(t, out.ServiceNames)
	require.NotEmpty(t, out.ServiceDetails, "ServiceDetails empty - pre-fix serviceDetailSet was never rendered")
	assert.Len(t, out.ServiceDetails, len(out.ServiceNames))

	var s3Detail *string
	for i := range out.ServiceDetails {
		d := out.ServiceDetails[i]
		require.NotNil(t, d.ServiceName, "ServiceName is nil")
		require.NotEmpty(t, d.ServiceType, "ServiceType empty for %s", *d.ServiceName)

		if d.ServiceName != nil && *d.ServiceName == "com.amazonaws.us-east-1.s3" {
			s3Detail = d.ServiceId
			assert.Equal(t, "Gateway", string(d.ServiceType[0].ServiceType))
		}
	}
	require.NotNil(t, s3Detail, "expected an s3 service in the built-in catalog")
}

// TestDescribeVpcEndpoints_SubnetAndRouteTableIds_RealClient covers
// toVpcEndpointItem's SubnetIds/RouteTableIds fields, which pre-fix wrapped
// each string in a nested <subnetId>/<routeTableId> child element. Both
// fields are plain ValueStringLists on the real VpcEndpoint shape
// (ec2@v1.319.1 deserializers.go, awsEc2query_deserializeDocumentVpcEndpoint
// matches "subnetIdSet"/"routeTableIdSet" via
// awsEc2query_deserializeDocumentValueStringList, which reads a bare text
// value at each <item>), so the real client's decoder failed outright
// instead of silently dropping data.
func TestDescribeVpcEndpoints_SubnetAndRouteTableIds_RealClient(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	client := newTestEC2Client(t, h)

	vpc, err := b.CreateVpc("10.70.0.0/16", "default")
	require.NoError(t, err)
	subnet, err := b.CreateSubnet(vpc.ID, "10.70.1.0/24", "us-east-1a")
	require.NoError(t, err)
	rt, err := b.CreateRouteTable(vpc.ID)
	require.NoError(t, err)

	_, err = b.CreateVpcEndpointWithRouteTableIDs(
		vpc.ID, "com.amazonaws.us-east-1.s3", "Gateway",
		[]string{subnet.ID}, []string{rt.ID},
	)
	require.NoError(t, err)

	out, err := client.DescribeVpcEndpoints(t.Context(), &ec2sdk.DescribeVpcEndpointsInput{})
	require.NoError(t, err)
	require.Len(t, out.VpcEndpoints, 1)
	assert.Equal(t, []string{subnet.ID}, out.VpcEndpoints[0].SubnetIds)
	assert.Equal(t, []string{rt.ID}, out.VpcEndpoints[0].RouteTableIds)
}

// TestDescribePrefixLists_CidrSet_RealClient covers prefixListItem.CidrsSet,
// which pre-fix wrapped each CIDR in a nested <cidrIp> child element. The
// real PrefixList.Cidrs field is a plain ValueStringList (ec2@v1.319.1
// deserializers.go, awsEc2query_deserializeDocumentPrefixList matches
// "cidrSet" via awsEc2query_deserializeDocumentValueStringList), so the
// real client's decoder failed outright instead of silently dropping data.
func TestDescribePrefixLists_CidrSet_RealClient(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	client := newTestEC2Client(t, h)

	out, err := client.DescribePrefixLists(t.Context(), &ec2sdk.DescribePrefixListsInput{})
	require.NoError(t, err)
	require.NotEmpty(t, out.PrefixLists)
	require.NotEmpty(t, out.PrefixLists[0].Cidrs, "Cidrs empty - pre-fix cidrSet items were nested, not plain text")
	assert.Equal(t, "52.216.0.0/15", out.PrefixLists[0].Cidrs[0])
}

// TestDescribeVpcEndpointConnectionNotifications_ConnectionEvents_RealClient
// covers connectionNotifItem.ConnectionEvents, which pre-fix double-wrapped
// each event in a nested <item> under the already-list-wrapping <item> (two
// levels of "item" instead of one). The real ConnectionEvents field is a
// plain ValueStringList (ec2@v1.319.1 deserializers.go, the
// VpcEndpointConnectionNotification deserializer matches "connectionEvents"
// via awsEc2query_deserializeDocumentValueStringList, one <item> per
// string), so the real client's decoder failed outright instead of silently
// dropping data.
func TestDescribeVpcEndpointConnectionNotifications_ConnectionEvents_RealClient(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	client := newTestEC2Client(t, h)

	_, err := b.CreateVpcEndpointConnectionNotification(
		"vpce-svc-sweep26", "", "arn:aws:sns:us-east-1:000000000000:sweep26",
		[]string{"Accept", "Reject"},
	)
	require.NoError(t, err)

	out, err := client.DescribeVpcEndpointConnectionNotifications(
		t.Context(), &ec2sdk.DescribeVpcEndpointConnectionNotificationsInput{},
	)
	require.NoError(t, err)
	require.Len(t, out.ConnectionNotificationSet, 1)
	assert.ElementsMatch(
		t, []string{"Accept", "Reject"}, out.ConnectionNotificationSet[0].ConnectionEvents,
		"ConnectionEvents empty - pre-fix items were double-nested",
	)
}

// TestVpnGatewayFamily_TagSet_RealClient covers VpnConnection, VpnGateway,
// and CustomerGateway, whose item shapes had no TagSet field at all
// pre-fix, even though tags applied via the shared/generic CreateTags op
// (ec2.InMemoryBackend.TagsForResource) are genuinely tracked for any known
// resource ID (resourceExistsGatewayLocked recognizes all three). The real
// deserializers all match "tagSet" for these three types (ec2@v1.319.1
// deserializers.go: awsEc2query_deserializeDocumentVpnConnection,
// awsEc2query_deserializeDocumentVpnGateway,
// awsEc2query_deserializeDocumentCustomerGateway), so a client reading Tags
// on any of the three always saw an empty slice despite the tags genuinely
// existing in the backend.
func TestVpnGatewayFamily_TagSet_RealClient(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	client := newTestEC2Client(t, h)

	vgw, err := b.CreateVpnGateway("ipsec.1")
	require.NoError(t, err)
	cgw, err := b.CreateCustomerGateway("ipsec.1", "203.0.113.1", "65000")
	require.NoError(t, err)
	conn, err := b.CreateVpnConnection("ipsec.1", cgw.CustomerGatewayID, vgw.VpnGatewayID)
	require.NoError(t, err)

	require.NoError(t, b.CreateTags([]string{vgw.VpnGatewayID}, map[string]string{"Name": "vgw-sweep26"}))
	require.NoError(t, b.CreateTags([]string{cgw.CustomerGatewayID}, map[string]string{"Name": "cgw-sweep26"}))
	require.NoError(t, b.CreateTags([]string{conn.VpnConnectionID}, map[string]string{"Name": "vpn-sweep26"}))

	vgwOut, err := client.DescribeVpnGateways(t.Context(), &ec2sdk.DescribeVpnGatewaysInput{})
	require.NoError(t, err)
	require.Len(t, vgwOut.VpnGateways, 1)
	require.NotEmpty(t, vgwOut.VpnGateways[0].Tags, "VpnGateway tags empty - pre-fix tagSet was never rendered")

	cgwOut, err := client.DescribeCustomerGateways(t.Context(), &ec2sdk.DescribeCustomerGatewaysInput{})
	require.NoError(t, err)
	require.Len(t, cgwOut.CustomerGateways, 1)
	require.NotEmpty(
		t, cgwOut.CustomerGateways[0].Tags, "CustomerGateway tags empty - pre-fix tagSet was never rendered",
	)

	connOut, err := client.DescribeVpnConnections(t.Context(), &ec2sdk.DescribeVpnConnectionsInput{})
	require.NoError(t, err)
	require.Len(t, connOut.VpnConnections, 1)
	require.NotEmpty(
		t, connOut.VpnConnections[0].Tags, "VpnConnection tags empty - pre-fix tagSet was never rendered",
	)
}

// TestVerifiedAccessFamily_TagSet_RealClient covers VerifiedAccessInstance,
// VerifiedAccessGroup, VerifiedAccessTrustProvider, and
// VerifiedAccessEndpoint, none of which rendered a TagSet field at all
// pre-fix, even though tags applied via the shared CreateTags op are
// genuinely tracked for these resource IDs
// (resourceExistsVerifiedAccessAndMirrorLocked). The real deserializers all
// match "tagSet" for these four types (ec2@v1.319.1 deserializers.go:
// awsEc2query_deserializeDocumentVerifiedAccessInstance,
// ...VerifiedAccessGroup, ...VerifiedAccessTrustProvider,
// ...VerifiedAccessEndpoint), so a client reading Tags on any of the four
// always saw an empty slice despite the tags genuinely existing.
func TestVerifiedAccessFamily_TagSet_RealClient(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	client := newTestEC2Client(t, h)

	inst, err := b.CreateVerifiedAccessInstance("sweep26 instance")
	require.NoError(t, err)
	grp, err := b.CreateVerifiedAccessGroup(inst.VerifiedAccessInstanceID, "sweep26 group")
	require.NoError(t, err)
	tp, err := b.CreateVerifiedAccessTrustProvider("user", "sweep26 trust provider")
	require.NoError(t, err)
	ep, err := b.CreateVerifiedAccessEndpoint(grp.VerifiedAccessGroupID, "network-interface", "sweep26 endpoint")
	require.NoError(t, err)

	require.NoError(t, b.CreateTags([]string{inst.VerifiedAccessInstanceID}, map[string]string{"Name": "inst"}))
	require.NoError(t, b.CreateTags([]string{grp.VerifiedAccessGroupID}, map[string]string{"Name": "grp"}))
	require.NoError(t, b.CreateTags([]string{tp.VerifiedAccessTrustProviderID}, map[string]string{"Name": "tp"}))
	require.NoError(t, b.CreateTags([]string{ep.VerifiedAccessEndpointID}, map[string]string{"Name": "ep"}))

	instOut, err := client.DescribeVerifiedAccessInstances(t.Context(), &ec2sdk.DescribeVerifiedAccessInstancesInput{})
	require.NoError(t, err)
	require.Len(t, instOut.VerifiedAccessInstances, 1)
	assert.NotEmpty(t, instOut.VerifiedAccessInstances[0].Tags, "VerifiedAccessInstance tags empty")

	grpOut, err := client.DescribeVerifiedAccessGroups(t.Context(), &ec2sdk.DescribeVerifiedAccessGroupsInput{})
	require.NoError(t, err)
	require.Len(t, grpOut.VerifiedAccessGroups, 1)
	assert.NotEmpty(t, grpOut.VerifiedAccessGroups[0].Tags, "VerifiedAccessGroup tags empty")

	tpOut, err := client.DescribeVerifiedAccessTrustProviders(
		t.Context(), &ec2sdk.DescribeVerifiedAccessTrustProvidersInput{},
	)
	require.NoError(t, err)
	require.Len(t, tpOut.VerifiedAccessTrustProviders, 1)
	assert.NotEmpty(t, tpOut.VerifiedAccessTrustProviders[0].Tags, "VerifiedAccessTrustProvider tags empty")

	epOut, err := client.DescribeVerifiedAccessEndpoints(t.Context(), &ec2sdk.DescribeVerifiedAccessEndpointsInput{})
	require.NoError(t, err)
	require.Len(t, epOut.VerifiedAccessEndpoints, 1)
	assert.NotEmpty(t, epOut.VerifiedAccessEndpoints[0].Tags, "VerifiedAccessEndpoint tags empty")
}

// TestIpamFamily_TagSet_RealClient covers Ipam, IpamScope, and IpamPool,
// none of which rendered a TagSet field at all pre-fix, even though tags
// applied via the shared CreateTags op are genuinely tracked for these
// resource IDs (resourceExistsIpamLocked). The real deserializers all match
// "tagSet" for these three types (ec2@v1.319.1 deserializers.go:
// awsEc2query_deserializeDocumentIpam, ...IpamScope, ...IpamPool), so a
// client reading Tags on any of the three always saw an empty slice despite
// the tags genuinely existing.
func TestIpamFamily_TagSet_RealClient(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	client := newTestEC2Client(t, h)

	ipam, err := b.CreateIpam()
	require.NoError(t, err)
	scope, err := b.CreateIpamScope(ipam.IpamID, "sweep26 scope")
	require.NoError(t, err)
	pool, err := b.CreateIpamPool(ipam.IpamID, "ipv4", "", "")
	require.NoError(t, err)

	require.NoError(t, b.CreateTags([]string{ipam.IpamID}, map[string]string{"Name": "ipam"}))
	require.NoError(t, b.CreateTags([]string{scope.IpamScopeID}, map[string]string{"Name": "scope"}))
	require.NoError(t, b.CreateTags([]string{pool.IpamPoolID}, map[string]string{"Name": "pool"}))

	ipamOut, err := client.DescribeIpams(t.Context(), &ec2sdk.DescribeIpamsInput{})
	require.NoError(t, err)
	require.Len(t, ipamOut.Ipams, 1)
	assert.NotEmpty(t, ipamOut.Ipams[0].Tags, "Ipam tags empty - pre-fix tagSet was never rendered")

	scopeOut, err := client.DescribeIpamScopes(t.Context(), &ec2sdk.DescribeIpamScopesInput{
		IpamScopeIds: []string{scope.IpamScopeID},
	})
	require.NoError(t, err)
	require.Len(t, scopeOut.IpamScopes, 1)
	assert.NotEmpty(t, scopeOut.IpamScopes[0].Tags, "IpamScope tags empty - pre-fix tagSet was never rendered")

	poolOut, err := client.DescribeIpamPools(t.Context(), &ec2sdk.DescribeIpamPoolsInput{})
	require.NoError(t, err)
	require.Len(t, poolOut.IpamPools, 1)
	assert.NotEmpty(t, poolOut.IpamPools[0].Tags, "IpamPool tags empty - pre-fix tagSet was never rendered")
}
