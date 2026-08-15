package ec2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestDescribeIpams_OperatingRegions_RealClient covers gopherstack-6flj:
// ipamItem.OperatingRegionSet was emitted under "operatingRegions", but the
// real Ipam deserializer (ec2@v1.319.1 deserializers.go:
// awsEc2query_deserializeDocumentIpam) reads "operatingRegionSet" -- the
// sibling IpamResourceDiscovery type already used the correct name, making
// this a sibling-style mismatch within the same file. A real client's
// Ipam.OperatingRegions was always empty regardless of what CreateIpam set.
func TestDescribeIpams_OperatingRegions_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestEC2Client(t, ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1")))
	ctx := t.Context()

	createOut, err := client.CreateIpam(ctx, &ec2sdk.CreateIpamInput{
		Description: aws.String("test-ipam"),
		OperatingRegions: []types.AddIpamOperatingRegion{
			{RegionName: aws.String("us-east-1")},
			{RegionName: aws.String("us-west-2")},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.Ipam)

	out, err := client.DescribeIpams(ctx, &ec2sdk.DescribeIpamsInput{
		IpamIds: []string{aws.ToString(createOut.Ipam.IpamId)},
	})
	require.NoError(t, err)
	require.Len(t, out.Ipams, 1)

	require.Len(t, out.Ipams[0].OperatingRegions, 2,
		"OperatingRegions must round-trip; pre-fix the real deserializer's element name never "+
			"matched the emitted one, so this was always empty")

	regions := make([]string, len(out.Ipams[0].OperatingRegions))
	for i, r := range out.Ipams[0].OperatingRegions {
		regions[i] = aws.ToString(r.RegionName)
	}
	assert.ElementsMatch(t, []string{"us-east-1", "us-west-2"}, regions)
}

// TestDescribeRouteServerPeers_EndpointEni_RealClient covers gopherstack-6flj:
// routeServerPeerItem emitted the peer's ENI under "eniId"/"eniAddress", but
// the real RouteServerPeer deserializer (ec2@v1.319.1 deserializers.go:
// awsEc2query_deserializeDocumentRouteServerPeer) reads
// "endpointEniId"/"endpointEniAddress" -- a sibling trap, since
// RouteServerEndpoint (a neighbouring type) legitimately uses the plain
// "eniId"/"eniAddress" names. A real client's peer ENI fields were always
// empty regardless of what the backend generated.
func TestDescribeRouteServerPeers_EndpointEni_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestEC2Client(t, ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1")))
	ctx := t.Context()

	rsOut, err := client.CreateRouteServer(ctx, &ec2sdk.CreateRouteServerInput{
		AmazonSideAsn: aws.Int64(4200000000),
	})
	require.NoError(t, err)

	epOut, err := client.CreateRouteServerEndpoint(ctx, &ec2sdk.CreateRouteServerEndpointInput{
		RouteServerId: rsOut.RouteServer.RouteServerId,
		SubnetId:      aws.String("subnet-default"),
	})
	require.NoError(t, err)

	peerOut, err := client.CreateRouteServerPeer(ctx, &ec2sdk.CreateRouteServerPeerInput{
		RouteServerEndpointId: epOut.RouteServerEndpoint.RouteServerEndpointId,
		PeerAddress:           aws.String("10.0.0.5"),
		BgpOptions: &types.RouteServerBgpOptionsRequest{
			PeerAsn: aws.Int64(65001),
		},
	})
	require.NoError(t, err)

	out, err := client.DescribeRouteServerPeers(ctx, &ec2sdk.DescribeRouteServerPeersInput{
		RouteServerPeerIds: []string{aws.ToString(peerOut.RouteServerPeer.RouteServerPeerId)},
	})
	require.NoError(t, err)
	require.Len(t, out.RouteServerPeers, 1)

	assert.NotEmpty(t, aws.ToString(out.RouteServerPeers[0].EndpointEniId),
		"EndpointEniId must round-trip; pre-fix it was emitted under the wrong \"eniId\" key")
	assert.NotEmpty(t, aws.ToString(out.RouteServerPeers[0].EndpointEniAddress),
		"EndpointEniAddress must round-trip; pre-fix it was emitted under the wrong \"eniAddress\" key")
}

// TestClientVpnTargetNetworks_StatusAndTargetNetworkId_RealClient covers
// gopherstack-6flj: clientVpnTargetNetworkItem emitted the subnet ID under
// "subnetId" (not a real field on this type at all) and Status as a flat
// string, but the real TargetNetwork deserializer (ec2@v1.319.1
// deserializers.go: awsEc2query_deserializeDocumentTargetNetwork) reads the
// subnet under "targetNetworkId" and Status as a nested
// AssociationStatus{Code,Message} struct
// (awsEc2query_deserializeDocumentAssociationStatus). A real client's
// TargetNetworkId and Status.Code were always empty. The same Status nesting
// bug affected AssociateClientVpnTargetNetworkOutput, ClientVpnRoute, and
// ClientVpnAuthorizationRule -- all four are covered below since they share
// one root cause and one fix.
func TestClientVpnTargetNetworks_StatusAndTargetNetworkId_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestEC2Client(t, ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1")))
	ctx := t.Context()

	epOut, err := client.CreateClientVpnEndpoint(ctx, &ec2sdk.CreateClientVpnEndpointInput{
		ClientCidrBlock:      aws.String("10.100.0.0/16"),
		ServerCertificateArn: aws.String("arn:aws:acm:us-east-1:000000000000:certificate/test"),
		ConnectionLogOptions: &types.ConnectionLogOptions{Enabled: aws.Bool(false)},
		AuthenticationOptions: []types.ClientVpnAuthenticationRequest{
			{Type: types.ClientVpnAuthenticationTypeCertificateAuthentication},
		},
	})
	require.NoError(t, err)

	assocOut, err := client.AssociateClientVpnTargetNetwork(ctx, &ec2sdk.AssociateClientVpnTargetNetworkInput{
		ClientVpnEndpointId: epOut.ClientVpnEndpointId,
		SubnetId:            aws.String("subnet-default"),
	})
	require.NoError(t, err)
	require.NotNil(t, assocOut.Status)
	assert.NotEmpty(t, string(assocOut.Status.Code),
		"AssociateClientVpnTargetNetwork's Status.Code must round-trip; pre-fix Status was a flat "+
			"string the real client's nested-struct decoder never populated")

	netOut, err := client.DescribeClientVpnTargetNetworks(ctx, &ec2sdk.DescribeClientVpnTargetNetworksInput{
		ClientVpnEndpointId: epOut.ClientVpnEndpointId,
	})
	require.NoError(t, err)
	require.Len(t, netOut.ClientVpnTargetNetworks, 1)
	assert.Equal(t, "subnet-default", aws.ToString(netOut.ClientVpnTargetNetworks[0].TargetNetworkId),
		"TargetNetworkId must round-trip; pre-fix it was emitted under the invented \"subnetId\" key")
	assert.NotEmpty(t, string(netOut.ClientVpnTargetNetworks[0].Status.Code),
		"Status.Code must round-trip; pre-fix Status was a flat string")

	require.NoError(t, err)
	authOut, err := client.AuthorizeClientVpnIngress(ctx, &ec2sdk.AuthorizeClientVpnIngressInput{
		ClientVpnEndpointId: epOut.ClientVpnEndpointId,
		TargetNetworkCidr:   aws.String("192.168.0.0/16"),
	})
	require.NoError(t, err)
	require.NotNil(t, authOut.Status)

	rulesOut, err := client.DescribeClientVpnAuthorizationRules(
		ctx, &ec2sdk.DescribeClientVpnAuthorizationRulesInput{ClientVpnEndpointId: epOut.ClientVpnEndpointId},
	)
	require.NoError(t, err)
	require.Len(t, rulesOut.AuthorizationRules, 1)
	assert.NotEmpty(t, string(rulesOut.AuthorizationRules[0].Status.Code),
		"AuthorizationRule.Status.Code must round-trip; pre-fix Status was a flat string")

	_, err = client.CreateClientVpnRoute(ctx, &ec2sdk.CreateClientVpnRouteInput{
		ClientVpnEndpointId:  epOut.ClientVpnEndpointId,
		DestinationCidrBlock: aws.String("172.16.0.0/16"),
		TargetVpcSubnetId:    aws.String("subnet-default"),
	})
	require.NoError(t, err)

	routesOut, err := client.DescribeClientVpnRoutes(
		ctx, &ec2sdk.DescribeClientVpnRoutesInput{ClientVpnEndpointId: epOut.ClientVpnEndpointId},
	)
	require.NoError(t, err)
	require.NotEmpty(t, routesOut.Routes)
	assert.NotEmpty(t, string(routesOut.Routes[0].Status.Code),
		"ClientVpnRoute.Status.Code must round-trip; pre-fix Status was a flat string")
}
