package terraform_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2svc "github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTerraform_MegaBatch45 provisions IPAM, traffic mirroring, client VPN,
// instance connect endpoint, network insights, and transit gateway
// connect/peering/multicast/policy-table resources, then verifies each
// through the EC2 SDK.
func TestTerraform_MegaBatch45(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "mega-batch-45",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return twoVPCCIDRVars(t)
			},
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()
				client := createEC2Client(t)

				verifyMegaBatch45Ipam(ctx, t, client)
				verifyMegaBatch45TrafficMirroring(ctx, t, client)
				verifyMegaBatch45ClientVpn(ctx, t, client)
				verifyMegaBatch45InstanceConnectAndInsights(ctx, t, client)
				verifyMegaBatch45TransitGatewayExtras(ctx, t, client)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

// verifyMegaBatch45Ipam checks the IPAM, scope, pool, pool CIDR, pool CIDR
// allocation, resource discovery, and resource discovery association.
func verifyMegaBatch45Ipam(ctx context.Context, t *testing.T, client *ec2svc.Client) {
	t.Helper()

	ipamOut, err := client.DescribeIpams(ctx, &ec2svc.DescribeIpamsInput{
		Filters: []ec2types.Filter{
			{Name: aws.String("tag:Name"), Values: []string{"mega-batch-45-ipam"}},
		},
	})
	require.NoError(t, err, "DescribeIpams should succeed")
	require.Len(t, ipamOut.Ipams, 1)
	ipamID := aws.ToString(ipamOut.Ipams[0].IpamId)

	scopeOut, err := client.DescribeIpamScopes(ctx, &ec2svc.DescribeIpamScopesInput{
		Filters: []ec2types.Filter{
			{Name: aws.String("ipam-id"), Values: []string{ipamID}},
			{Name: aws.String("is-default"), Values: []string{"false"}},
		},
	})
	require.NoError(t, err, "DescribeIpamScopes should succeed")
	require.Len(t, scopeOut.IpamScopes, 1)

	poolOut, err := client.DescribeIpamPools(ctx, &ec2svc.DescribeIpamPoolsInput{
		Filters: []ec2types.Filter{
			{Name: aws.String("tag:Name"), Values: []string{"mega-batch-45-ipam-pool"}},
		},
	})
	require.NoError(t, err, "DescribeIpamPools should succeed")
	require.Len(t, poolOut.IpamPools, 1)
	poolID := aws.ToString(poolOut.IpamPools[0].IpamPoolId)

	cidrOut, err := client.GetIpamPoolCidrs(ctx, &ec2svc.GetIpamPoolCidrsInput{
		IpamPoolId: aws.String(poolID),
	})
	require.NoError(t, err, "GetIpamPoolCidrs should succeed")

	var foundPoolCidr bool

	for _, c := range cidrOut.IpamPoolCidrs {
		if aws.ToString(c.Cidr) == "10.90.0.0/16" {
			foundPoolCidr = true
		}
	}

	assert.True(t, foundPoolCidr, "aws_vpc_ipam_pool_cidr should provision the CIDR")

	allocOut, err := client.GetIpamPoolAllocations(ctx, &ec2svc.GetIpamPoolAllocationsInput{
		IpamPoolId: aws.String(poolID),
	})
	require.NoError(t, err, "GetIpamPoolAllocations should succeed")

	var foundAllocation bool

	for _, a := range allocOut.IpamPoolAllocations {
		if aws.ToString(a.Cidr) == "10.90.1.0/24" {
			foundAllocation = true
		}
	}

	assert.True(t, foundAllocation, "aws_vpc_ipam_pool_cidr_allocation should allocate the CIDR")

	rdOut, err := client.DescribeIpamResourceDiscoveries(ctx, &ec2svc.DescribeIpamResourceDiscoveriesInput{
		Filters: []ec2types.Filter{
			{Name: aws.String("tag:Name"), Values: []string{"mega-batch-45-ipam-rd"}},
		},
	})
	require.NoError(t, err, "DescribeIpamResourceDiscoveries should succeed")
	require.Len(t, rdOut.IpamResourceDiscoveries, 1)
	rdID := aws.ToString(rdOut.IpamResourceDiscoveries[0].IpamResourceDiscoveryId)

	rdAssocOut, err := client.DescribeIpamResourceDiscoveryAssociations(
		ctx,
		&ec2svc.DescribeIpamResourceDiscoveryAssociationsInput{
			Filters: []ec2types.Filter{
				{Name: aws.String("ipam-resource-discovery-id"), Values: []string{rdID}},
			},
		},
	)
	require.NoError(t, err, "DescribeIpamResourceDiscoveryAssociations should succeed")
	assert.NotEmpty(
		t,
		rdAssocOut.IpamResourceDiscoveryAssociations,
		"aws_vpc_ipam_resource_discovery_association should associate the discovery",
	)
}

// verifyMegaBatch45TrafficMirroring checks the traffic mirror filter, its
// rule, target, and session.
func verifyMegaBatch45TrafficMirroring(ctx context.Context, t *testing.T, client *ec2svc.Client) {
	t.Helper()

	filterOut, err := client.DescribeTrafficMirrorFilters(ctx, &ec2svc.DescribeTrafficMirrorFiltersInput{})
	require.NoError(t, err, "DescribeTrafficMirrorFilters should succeed")

	var filterID string

	for _, f := range filterOut.TrafficMirrorFilters {
		if aws.ToString(f.Description) == "mega-batch-45 filter" {
			filterID = aws.ToString(f.TrafficMirrorFilterId)
		}
	}

	require.NotEmpty(t, filterID, "aws_ec2_traffic_mirror_filter should exist")

	var foundRule bool

	for _, f := range filterOut.TrafficMirrorFilters {
		if aws.ToString(f.TrafficMirrorFilterId) != filterID {
			continue
		}

		for _, r := range f.IngressFilterRules {
			if aws.ToInt32(r.RuleNumber) == 1 {
				foundRule = true
			}
		}
	}

	assert.True(t, foundRule, "aws_ec2_traffic_mirror_filter_rule should add the ingress rule")

	targetOut, err := client.DescribeTrafficMirrorTargets(ctx, &ec2svc.DescribeTrafficMirrorTargetsInput{})
	require.NoError(t, err, "DescribeTrafficMirrorTargets should succeed")

	var targetID string

	for _, tg := range targetOut.TrafficMirrorTargets {
		if aws.ToString(tg.Description) == "mega-batch-45 target" {
			targetID = aws.ToString(tg.TrafficMirrorTargetId)
		}
	}

	require.NotEmpty(t, targetID, "aws_ec2_traffic_mirror_target should exist")

	sessionOut, err := client.DescribeTrafficMirrorSessions(ctx, &ec2svc.DescribeTrafficMirrorSessionsInput{})
	require.NoError(t, err, "DescribeTrafficMirrorSessions should succeed")

	var foundSession bool

	for _, s := range sessionOut.TrafficMirrorSessions {
		if aws.ToString(s.TrafficMirrorTargetId) == targetID && aws.ToString(s.TrafficMirrorFilterId) == filterID {
			foundSession = true
		}
	}

	assert.True(t, foundSession, "aws_ec2_traffic_mirror_session should exist")
}

// verifyMegaBatch45ClientVpn checks the Client VPN endpoint, its network
// association, authorization rule, and route.
func verifyMegaBatch45ClientVpn(ctx context.Context, t *testing.T, client *ec2svc.Client) {
	t.Helper()

	epOut, err := client.DescribeClientVpnEndpoints(ctx, &ec2svc.DescribeClientVpnEndpointsInput{
		Filters: []ec2types.Filter{
			{Name: aws.String("tag:Name"), Values: []string{"mega-batch-45-cvpn"}},
		},
	})
	require.NoError(t, err, "DescribeClientVpnEndpoints should succeed")
	require.Len(t, epOut.ClientVpnEndpoints, 1)
	endpointID := aws.ToString(epOut.ClientVpnEndpoints[0].ClientVpnEndpointId)

	netOut, err := client.DescribeClientVpnTargetNetworks(ctx, &ec2svc.DescribeClientVpnTargetNetworksInput{
		ClientVpnEndpointId: aws.String(endpointID),
	})
	require.NoError(t, err, "DescribeClientVpnTargetNetworks should succeed")
	assert.NotEmpty(
		t,
		netOut.ClientVpnTargetNetworks,
		"aws_ec2_client_vpn_network_association should associate a subnet",
	)

	authOut, err := client.DescribeClientVpnAuthorizationRules(ctx, &ec2svc.DescribeClientVpnAuthorizationRulesInput{
		ClientVpnEndpointId: aws.String(endpointID),
	})
	require.NoError(t, err, "DescribeClientVpnAuthorizationRules should succeed")
	assert.NotEmpty(t, authOut.AuthorizationRules, "aws_ec2_client_vpn_authorization_rule should exist")

	routeOut, err := client.DescribeClientVpnRoutes(ctx, &ec2svc.DescribeClientVpnRoutesInput{
		ClientVpnEndpointId: aws.String(endpointID),
	})
	require.NoError(t, err, "DescribeClientVpnRoutes should succeed")

	var foundRoute bool

	for _, r := range routeOut.Routes {
		if aws.ToString(r.DestinationCidr) == "0.0.0.0/0" {
			foundRoute = true
		}
	}

	assert.True(t, foundRoute, "aws_ec2_client_vpn_route should add the route")
}

// verifyMegaBatch45InstanceConnectAndInsights checks the instance connect
// endpoint plus the network insights path and analysis.
func verifyMegaBatch45InstanceConnectAndInsights(ctx context.Context, t *testing.T, client *ec2svc.Client) {
	t.Helper()

	iceOut, err := client.DescribeInstanceConnectEndpoints(ctx, &ec2svc.DescribeInstanceConnectEndpointsInput{
		Filters: []ec2types.Filter{
			{Name: aws.String("tag:Name"), Values: []string{"mega-batch-45-ice"}},
		},
	})
	require.NoError(t, err, "DescribeInstanceConnectEndpoints should succeed")
	require.Len(t, iceOut.InstanceConnectEndpoints, 1)

	destOut, err := client.DescribeInstances(ctx, &ec2svc.DescribeInstancesInput{
		Filters: []ec2types.Filter{
			{Name: aws.String("tag:Name"), Values: []string{"mega-batch-45-destination"}},
		},
	})
	require.NoError(t, err, "DescribeInstances should succeed")
	require.Len(t, destOut.Reservations, 1)
	require.Len(t, destOut.Reservations[0].Instances, 1)
	destInstanceID := aws.ToString(destOut.Reservations[0].Instances[0].InstanceId)

	// DescribeNetworkInsightsPaths has no per-fixture tag/ID filter available
	// at this point (the path's own ID is only known after creation), so
	// filter server-side by our destination instance to avoid picking up
	// another fixture's path in this CI shard's shared emulator.
	pathOut, err := client.DescribeNetworkInsightsPaths(ctx, &ec2svc.DescribeNetworkInsightsPathsInput{
		Filters: []ec2types.Filter{
			{Name: aws.String("destination"), Values: []string{destInstanceID}},
		},
	})
	require.NoError(t, err, "DescribeNetworkInsightsPaths should succeed")
	path := findBy(t, pathOut.NetworkInsightsPaths, func(p ec2types.NetworkInsightsPath) bool {
		return aws.ToString(p.Destination) == destInstanceID && p.Protocol == ec2types.ProtocolTcp
	}, "mega-batch-45 network insights path")
	pathID := aws.ToString(path.NetworkInsightsPathId)

	analysisOut, err := client.DescribeNetworkInsightsAnalyses(ctx, &ec2svc.DescribeNetworkInsightsAnalysesInput{
		Filters: []ec2types.Filter{
			{Name: aws.String("path-id"), Values: []string{pathID}},
		},
	})
	require.NoError(t, err, "DescribeNetworkInsightsAnalyses should succeed")
	assert.NotEmpty(t, analysisOut.NetworkInsightsAnalyses, "aws_ec2_network_insights_analysis should exist")
}

// verifyMegaBatch45TransitGatewayExtras checks the TGW Connect attachment,
// peering attachment, multicast domain, and policy table.
func verifyMegaBatch45TransitGatewayExtras(ctx context.Context, t *testing.T, client *ec2svc.Client) {
	t.Helper()

	connectOut, err := client.DescribeTransitGatewayConnects(ctx, &ec2svc.DescribeTransitGatewayConnectsInput{
		Filters: []ec2types.Filter{
			{Name: aws.String("tag:Name"), Values: []string{"mega-batch-45-tgw-connect"}},
		},
	})
	require.NoError(t, err, "DescribeTransitGatewayConnects should succeed")
	assert.Len(t, connectOut.TransitGatewayConnects, 1)

	peeringOut, err := client.DescribeTransitGatewayPeeringAttachments(
		ctx,
		&ec2svc.DescribeTransitGatewayPeeringAttachmentsInput{
			Filters: []ec2types.Filter{
				{Name: aws.String("tag:Name"), Values: []string{"mega-batch-45-tgw-peering"}},
			},
		},
	)
	require.NoError(t, err, "DescribeTransitGatewayPeeringAttachments should succeed")
	assert.Len(t, peeringOut.TransitGatewayPeeringAttachments, 1)

	multicastOut, err := client.DescribeTransitGatewayMulticastDomains(
		ctx,
		&ec2svc.DescribeTransitGatewayMulticastDomainsInput{
			Filters: []ec2types.Filter{
				{Name: aws.String("tag:Name"), Values: []string{"mega-batch-45-tgw-multicast"}},
			},
		},
	)
	require.NoError(t, err, "DescribeTransitGatewayMulticastDomains should succeed")
	assert.Len(t, multicastOut.TransitGatewayMulticastDomains, 1)

	policyTableOut, err := client.DescribeTransitGatewayPolicyTables(
		ctx,
		&ec2svc.DescribeTransitGatewayPolicyTablesInput{
			Filters: []ec2types.Filter{
				{Name: aws.String("tag:Name"), Values: []string{"mega-batch-45-tgw-policy-table"}},
			},
		},
	)
	require.NoError(t, err, "DescribeTransitGatewayPolicyTables should succeed")
	assert.Len(t, policyTableOut.TransitGatewayPolicyTables, 1)
}
