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

// TestRealClient_IPAMAndTrafficMirror covers ec2's largest remaining uncovered-op
// families (gopherstack-n3zi): IPAM core/pool/resource-discovery/
// verification-token, BYOIP/COIP/public IPv4 pools, Traffic Mirror, Route
// Server and Local Gateway. Each row gets its own fresh handler+backend so
// rows can run in parallel without shared state.
func TestRealClient_IPAMAndTrafficMirror(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, backend *ec2.InMemoryBackend, client *ec2sdk.Client)
		name string
	}{
		{runIpamCore, "ipam_core"},
		{runIpamPoolCidrAndResourceDiscovery, "ipam_pool_cidr_and_resource_discovery"},
		{runByoipAndIPPools, "byoip_and_ip_pools"},
		{runTrafficMirror, "traffic_mirror"},
		{runRouteServer, "route_server"},
		{runLocalGateway, "local_gateway"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
			h := ec2.NewHandler(backend)
			client := newTestEC2Client(t, h)
			tt.run(t, backend, client)
		})
	}
}

// runIpamCore covers CreateIpam, DescribeIpams, ModifyIpam, CreateIpamScope,
// ModifyIpamScope, DeleteIpamScope, CreateIpamPool, ModifyIpamPool,
// DeleteIpamPool, DeleteIpam.
func runIpamCore(t *testing.T, _ *ec2.InMemoryBackend, client *ec2sdk.Client) {
	t.Helper()

	createOut, err := client.CreateIpam(t.Context(), &ec2sdk.CreateIpamInput{
		Description: aws.String("test ipam"),
		Tier:        types.IpamTierFree,
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.Ipam)
	ipamID := aws.ToString(createOut.Ipam.IpamId)
	assert.Equal(t, "test ipam", aws.ToString(createOut.Ipam.Description))
	assert.Equal(t, types.IpamTierFree, createOut.Ipam.Tier)

	descOut, err := client.DescribeIpams(t.Context(), &ec2sdk.DescribeIpamsInput{
		IpamIds: []string{ipamID},
	})
	require.NoError(t, err)
	require.Len(t, descOut.Ipams, 1)
	assert.Equal(t, ipamID, aws.ToString(descOut.Ipams[0].IpamId))

	modOut, err := client.ModifyIpam(t.Context(), &ec2sdk.ModifyIpamInput{
		IpamId:      aws.String(ipamID),
		Description: aws.String("updated ipam"),
	})
	require.NoError(t, err)
	assert.Equal(t, "updated ipam", aws.ToString(modOut.Ipam.Description))

	require.NotEmpty(t, createOut.Ipam.PublicDefaultScopeId)
	scopeID := aws.ToString(createOut.Ipam.PrivateDefaultScopeId)
	require.NotEmpty(t, scopeID)

	createScopeOut, err := client.CreateIpamScope(t.Context(), &ec2sdk.CreateIpamScopeInput{
		IpamId:      aws.String(ipamID),
		Description: aws.String("extra scope"),
	})
	require.NoError(t, err)
	extraScopeID := aws.ToString(createScopeOut.IpamScope.IpamScopeId)
	assert.Equal(t, "extra scope", aws.ToString(createScopeOut.IpamScope.Description))

	modScopeOut, err := client.ModifyIpamScope(t.Context(), &ec2sdk.ModifyIpamScopeInput{
		IpamScopeId: aws.String(extraScopeID),
		Description: aws.String("renamed scope"),
	})
	require.NoError(t, err)
	assert.Equal(t, "renamed scope", aws.ToString(modScopeOut.IpamScope.Description))

	_, err = client.DeleteIpamScope(t.Context(), &ec2sdk.DeleteIpamScopeInput{
		IpamScopeId: aws.String(extraScopeID),
	})
	require.NoError(t, err)

	createPoolOut, err := client.CreateIpamPool(t.Context(), &ec2sdk.CreateIpamPoolInput{
		IpamScopeId:   aws.String(scopeID),
		AddressFamily: types.AddressFamilyIpv4,
		Description:   aws.String("test pool"),
	})
	require.NoError(t, err)
	poolID := aws.ToString(createPoolOut.IpamPool.IpamPoolId)
	assert.Equal(t, types.AddressFamilyIpv4, createPoolOut.IpamPool.AddressFamily)

	modPoolOut, err := client.ModifyIpamPool(t.Context(), &ec2sdk.ModifyIpamPoolInput{
		IpamPoolId:  aws.String(poolID),
		Description: aws.String("updated pool"),
	})
	require.NoError(t, err)
	assert.Equal(t, "updated pool", aws.ToString(modPoolOut.IpamPool.Description))

	_, err = client.DeleteIpamPool(t.Context(), &ec2sdk.DeleteIpamPoolInput{
		IpamPoolId: aws.String(poolID),
	})
	require.NoError(t, err)

	deleteIpamOut, err := client.DeleteIpam(t.Context(), &ec2sdk.DeleteIpamInput{
		IpamId:  aws.String(ipamID),
		Cascade: aws.Bool(true),
	})
	require.NoError(t, err)
	assert.Equal(t, ipamID, aws.ToString(deleteIpamOut.Ipam.IpamId))
}

// runIpamPoolCidrAndResourceDiscovery covers ProvisionIpamPoolCidr,
// GetIpamPoolCidrs, AllocateIpamPoolCidr, GetIpamPoolAllocations,
// DeprovisionIpamPoolCidr, CreateIpamResourceDiscovery,
// AssociateIpamResourceDiscovery, DisassociateIpamResourceDiscovery,
// DeleteIpamResourceDiscovery, CreateIpamExternalResourceVerificationToken,
// DescribeIpamExternalResourceVerificationTokens,
// DeleteIpamExternalResourceVerificationToken.
func runIpamPoolCidrAndResourceDiscovery(t *testing.T, _ *ec2.InMemoryBackend, client *ec2sdk.Client) {
	t.Helper()

	ipamOut, err := client.CreateIpam(t.Context(), &ec2sdk.CreateIpamInput{})
	require.NoError(t, err)
	scopeID := aws.ToString(ipamOut.Ipam.PrivateDefaultScopeId)

	poolOut, err := client.CreateIpamPool(t.Context(), &ec2sdk.CreateIpamPoolInput{
		IpamScopeId:   aws.String(scopeID),
		AddressFamily: types.AddressFamilyIpv4,
	})
	require.NoError(t, err)
	poolID := aws.ToString(poolOut.IpamPool.IpamPoolId)

	provOut, err := client.ProvisionIpamPoolCidr(t.Context(), &ec2sdk.ProvisionIpamPoolCidrInput{
		IpamPoolId: aws.String(poolID),
		Cidr:       aws.String("10.20.0.0/16"),
	})
	require.NoError(t, err)
	require.NotNil(t, provOut.IpamPoolCidr)
	assert.Equal(t, "10.20.0.0/16", aws.ToString(provOut.IpamPoolCidr.Cidr))

	getCidrsOut, err := client.GetIpamPoolCidrs(t.Context(), &ec2sdk.GetIpamPoolCidrsInput{
		IpamPoolId: aws.String(poolID),
	})
	require.NoError(t, err)
	require.Len(t, getCidrsOut.IpamPoolCidrs, 1)
	assert.Equal(t, "10.20.0.0/16", aws.ToString(getCidrsOut.IpamPoolCidrs[0].Cidr))

	allocOut, err := client.AllocateIpamPoolCidr(t.Context(), &ec2sdk.AllocateIpamPoolCidrInput{
		IpamPoolId:    aws.String(poolID),
		NetmaskLength: aws.Int32(24),
	})
	require.NoError(t, err)
	require.NotNil(t, allocOut.IpamPoolAllocation)
	allocID := aws.ToString(allocOut.IpamPoolAllocation.IpamPoolAllocationId)
	require.NotEmpty(t, allocID)
	assert.Contains(t, aws.ToString(allocOut.IpamPoolAllocation.Cidr), "/24")

	getAllocOut, err := client.GetIpamPoolAllocations(t.Context(), &ec2sdk.GetIpamPoolAllocationsInput{
		IpamPoolId: aws.String(poolID),
	})
	require.NoError(t, err)
	require.Len(t, getAllocOut.IpamPoolAllocations, 1)
	assert.Equal(t, allocID, aws.ToString(getAllocOut.IpamPoolAllocations[0].IpamPoolAllocationId))

	_, err = client.ReleaseIpamPoolAllocation(t.Context(), &ec2sdk.ReleaseIpamPoolAllocationInput{
		IpamPoolId:           aws.String(poolID),
		IpamPoolAllocationId: aws.String(allocID),
		Cidr:                 allocOut.IpamPoolAllocation.Cidr,
	})
	require.NoError(t, err)

	deprovOut, err := client.DeprovisionIpamPoolCidr(t.Context(), &ec2sdk.DeprovisionIpamPoolCidrInput{
		IpamPoolId: aws.String(poolID),
		Cidr:       aws.String("10.20.0.0/16"),
	})
	require.NoError(t, err)
	assert.Equal(t, "10.20.0.0/16", aws.ToString(deprovOut.IpamPoolCidr.Cidr))

	rdOut, err := client.CreateIpamResourceDiscovery(t.Context(), &ec2sdk.CreateIpamResourceDiscoveryInput{
		Description: aws.String("test discovery"),
	})
	require.NoError(t, err)
	rdID := aws.ToString(rdOut.IpamResourceDiscovery.IpamResourceDiscoveryId)
	assert.Equal(t, "test discovery", aws.ToString(rdOut.IpamResourceDiscovery.Description))

	assocOut, err := client.AssociateIpamResourceDiscovery(t.Context(), &ec2sdk.AssociateIpamResourceDiscoveryInput{
		IpamId:                  ipamOut.Ipam.IpamId,
		IpamResourceDiscoveryId: aws.String(rdID),
	})
	require.NoError(t, err)
	assocID := aws.ToString(assocOut.IpamResourceDiscoveryAssociation.IpamResourceDiscoveryAssociationId)
	require.NotEmpty(t, assocID)

	_, err = client.DisassociateIpamResourceDiscovery(t.Context(), &ec2sdk.DisassociateIpamResourceDiscoveryInput{
		IpamResourceDiscoveryAssociationId: aws.String(assocID),
	})
	require.NoError(t, err)

	_, err = client.DeleteIpamResourceDiscovery(t.Context(), &ec2sdk.DeleteIpamResourceDiscoveryInput{
		IpamResourceDiscoveryId: aws.String(rdID),
	})
	require.NoError(t, err)

	tokenOut, err := client.CreateIpamExternalResourceVerificationToken(
		t.Context(), &ec2sdk.CreateIpamExternalResourceVerificationTokenInput{
			IpamId: ipamOut.Ipam.IpamId,
		},
	)
	require.NoError(t, err)
	require.NotNil(t, tokenOut.IpamExternalResourceVerificationToken)
	tokenID := aws.ToString(tokenOut.IpamExternalResourceVerificationToken.IpamExternalResourceVerificationTokenId)
	require.NotEmpty(t, tokenID)
	require.NotEmpty(t, aws.ToString(tokenOut.IpamExternalResourceVerificationToken.TokenValue))

	descTokensOut, err := client.DescribeIpamExternalResourceVerificationTokens(
		t.Context(), &ec2sdk.DescribeIpamExternalResourceVerificationTokensInput{
			IpamExternalResourceVerificationTokenIds: []string{tokenID},
		},
	)
	require.NoError(t, err)
	require.Len(t, descTokensOut.IpamExternalResourceVerificationTokens, 1)
	assert.Equal(
		t, tokenID,
		aws.ToString(descTokensOut.IpamExternalResourceVerificationTokens[0].IpamExternalResourceVerificationTokenId),
	)

	_, err = client.DeleteIpamExternalResourceVerificationToken(
		t.Context(), &ec2sdk.DeleteIpamExternalResourceVerificationTokenInput{
			IpamExternalResourceVerificationTokenId: aws.String(tokenID),
		},
	)
	require.NoError(t, err)
}

// runByoipAndIPPools covers AdvertiseByoipCidr, ProvisionByoipCidr,
// WithdrawByoipCidr, DeprovisionByoipCidr, DescribeByoipCidrs,
// CreatePublicIpv4Pool, ProvisionPublicIpv4PoolCidr,
// DeprovisionPublicIpv4PoolCidr, DeletePublicIpv4Pool,
// CreateLocalGatewayRouteTable (setup for COIP), CreateCoipPool,
// CreateCoipCidr, DescribeCoipPools, GetCoipPoolUsage, DeleteCoipCidr,
// DeleteCoipPool.
func runByoipAndIPPools(t *testing.T, backend *ec2.InMemoryBackend, client *ec2sdk.Client) {
	t.Helper()

	provOut, err := client.ProvisionByoipCidr(t.Context(), &ec2sdk.ProvisionByoipCidrInput{
		Cidr:        aws.String("203.0.113.0/24"),
		Description: aws.String("test byoip range"),
	})
	require.NoError(t, err)
	require.NotNil(t, provOut.ByoipCidr)
	assert.Equal(t, "203.0.113.0/24", aws.ToString(provOut.ByoipCidr.Cidr))

	advOut, err := client.AdvertiseByoipCidr(t.Context(), &ec2sdk.AdvertiseByoipCidrInput{
		Cidr: aws.String("203.0.113.0/24"),
	})
	require.NoError(t, err)
	assert.Equal(t, "203.0.113.0/24", aws.ToString(advOut.ByoipCidr.Cidr))

	descOut, err := client.DescribeByoipCidrs(t.Context(), &ec2sdk.DescribeByoipCidrsInput{
		MaxResults: aws.Int32(10),
	})
	require.NoError(t, err)
	require.Len(t, descOut.ByoipCidrs, 1)
	assert.Equal(t, "203.0.113.0/24", aws.ToString(descOut.ByoipCidrs[0].Cidr))

	wOut, err := client.WithdrawByoipCidr(t.Context(), &ec2sdk.WithdrawByoipCidrInput{
		Cidr: aws.String("203.0.113.0/24"),
	})
	require.NoError(t, err)
	assert.Equal(t, "203.0.113.0/24", aws.ToString(wOut.ByoipCidr.Cidr))

	deprovOut, err := client.DeprovisionByoipCidr(t.Context(), &ec2sdk.DeprovisionByoipCidrInput{
		Cidr: aws.String("203.0.113.0/24"),
	})
	require.NoError(t, err)
	assert.Equal(t, "203.0.113.0/24", aws.ToString(deprovOut.ByoipCidr.Cidr))

	poolOut, err := client.CreatePublicIpv4Pool(t.Context(), &ec2sdk.CreatePublicIpv4PoolInput{
		NetworkBorderGroup: aws.String("us-east-1"),
	})
	require.NoError(t, err)
	poolID := aws.ToString(poolOut.PoolId)
	require.NotEmpty(t, poolID)

	rangeOut, err := client.ProvisionPublicIpv4PoolCidr(t.Context(), &ec2sdk.ProvisionPublicIpv4PoolCidrInput{
		PoolId:        aws.String(poolID),
		NetmaskLength: aws.Int32(28),
		// IpamPoolId is a real, required member of this wire request. This
		// backend's public-IPv4-pool model tracks pools standing alone, not
		// sourced from an IPAM pool, so the value is accepted and dropped
		// (accept-and-drop finding, not fabrication -- see report).
		IpamPoolId: aws.String("ipam-pool-unused"),
	})
	require.NoError(t, err)
	require.NotNil(t, rangeOut.PoolAddressRange)
	assert.NotEmpty(t, aws.ToString(rangeOut.PoolAddressRange.FirstAddress))

	deprovIpv4Out, err := client.DeprovisionPublicIpv4PoolCidr(t.Context(), &ec2sdk.DeprovisionPublicIpv4PoolCidrInput{
		PoolId: aws.String(poolID),
		Cidr:   rangeOut.PoolAddressRange.FirstAddress,
	})
	require.NoError(t, err)
	assert.Equal(t, poolID, aws.ToString(deprovIpv4Out.PoolId))

	_, err = client.DeletePublicIpv4Pool(t.Context(), &ec2sdk.DeletePublicIpv4PoolInput{
		PoolId: aws.String(poolID),
	})
	require.NoError(t, err)

	lg, err := backend.SeedLocalGateway(ec2.LocalGateway{})
	require.NoError(t, err)

	rt, err := backend.CreateLocalGatewayRouteTable(lg.LocalGatewayID, "direct-vpc-routing")
	require.NoError(t, err)

	coipOut, err := client.CreateCoipPool(t.Context(), &ec2sdk.CreateCoipPoolInput{
		LocalGatewayRouteTableId: aws.String(rt.LocalGatewayRouteTableID),
	})
	require.NoError(t, err)
	coipID := aws.ToString(coipOut.CoipPool.PoolId)
	require.NotEmpty(t, coipID)

	_, err = client.CreateCoipCidr(t.Context(), &ec2sdk.CreateCoipCidrInput{
		CoipPoolId: aws.String(coipID),
		Cidr:       aws.String("198.51.100.0/24"),
	})
	require.NoError(t, err)

	descCoipOut, err := client.DescribeCoipPools(t.Context(), &ec2sdk.DescribeCoipPoolsInput{
		PoolIds: []string{coipID},
	})
	require.NoError(t, err)
	require.Len(t, descCoipOut.CoipPools, 1)
	assert.Equal(t, rt.LocalGatewayRouteTableID, aws.ToString(descCoipOut.CoipPools[0].LocalGatewayRouteTableId))

	usageOut, err := client.GetCoipPoolUsage(t.Context(), &ec2sdk.GetCoipPoolUsageInput{
		PoolId: aws.String(coipID),
	})
	require.NoError(t, err)
	assert.Equal(t, coipID, aws.ToString(usageOut.CoipPoolId))

	_, err = client.DeleteCoipCidr(t.Context(), &ec2sdk.DeleteCoipCidrInput{
		CoipPoolId: aws.String(coipID),
		Cidr:       aws.String("198.51.100.0/24"),
	})
	require.NoError(t, err)

	_, err = client.DeleteCoipPool(t.Context(), &ec2sdk.DeleteCoipPoolInput{
		CoipPoolId: aws.String(coipID),
	})
	require.NoError(t, err)
}

// runTrafficMirror covers CreateTrafficMirrorFilter,
// ModifyTrafficMirrorFilterNetworkServices, CreateTrafficMirrorFilterRule,
// DescribeTrafficMirrorFilterRules, ModifyTrafficMirrorFilterRule,
// DeleteTrafficMirrorFilterRule, CreateTrafficMirrorTarget,
// CreateTrafficMirrorSession, ModifyTrafficMirrorSession,
// DeleteTrafficMirrorSession, DeleteTrafficMirrorTarget,
// DeleteTrafficMirrorFilter.
func runTrafficMirror(t *testing.T, _ *ec2.InMemoryBackend, client *ec2sdk.Client) {
	t.Helper()

	vpcOut, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.0.0.0/16")})
	require.NoError(t, err)
	subnetOut, err := client.CreateSubnet(t.Context(), &ec2sdk.CreateSubnetInput{
		VpcId:     vpcOut.Vpc.VpcId,
		CidrBlock: aws.String("10.0.0.0/24"),
	})
	require.NoError(t, err)
	eniOut, err := client.CreateNetworkInterface(t.Context(), &ec2sdk.CreateNetworkInterfaceInput{
		SubnetId: subnetOut.Subnet.SubnetId,
	})
	require.NoError(t, err)
	eniID := aws.ToString(eniOut.NetworkInterface.NetworkInterfaceId)

	filterOut, err := client.CreateTrafficMirrorFilter(t.Context(), &ec2sdk.CreateTrafficMirrorFilterInput{
		Description: aws.String("test filter"),
	})
	require.NoError(t, err)
	filterID := aws.ToString(filterOut.TrafficMirrorFilter.TrafficMirrorFilterId)

	svcOut, err := client.ModifyTrafficMirrorFilterNetworkServices(
		t.Context(), &ec2sdk.ModifyTrafficMirrorFilterNetworkServicesInput{
			TrafficMirrorFilterId: aws.String(filterID),
			AddNetworkServices:    []types.TrafficMirrorNetworkService{types.TrafficMirrorNetworkServiceAmazonDns},
		},
	)
	require.NoError(t, err)
	require.Contains(t, svcOut.TrafficMirrorFilter.NetworkServices, types.TrafficMirrorNetworkServiceAmazonDns)

	ruleOut, err := client.CreateTrafficMirrorFilterRule(t.Context(), &ec2sdk.CreateTrafficMirrorFilterRuleInput{
		TrafficMirrorFilterId: aws.String(filterID),
		TrafficDirection:      types.TrafficDirectionIngress,
		RuleAction:            types.TrafficMirrorRuleActionAccept,
		RuleNumber:            aws.Int32(1),
		SourceCidrBlock:       aws.String("0.0.0.0/0"),
		DestinationCidrBlock:  aws.String("0.0.0.0/0"),
	})
	require.NoError(t, err)
	ruleID := aws.ToString(ruleOut.TrafficMirrorFilterRule.TrafficMirrorFilterRuleId)

	descRulesOut, err := client.DescribeTrafficMirrorFilterRules(
		t.Context(), &ec2sdk.DescribeTrafficMirrorFilterRulesInput{TrafficMirrorFilterId: aws.String(filterID)},
	)
	require.NoError(t, err)
	require.Len(t, descRulesOut.TrafficMirrorFilterRules, 1)
	assert.Equal(t, ruleID, aws.ToString(descRulesOut.TrafficMirrorFilterRules[0].TrafficMirrorFilterRuleId))

	modRuleOut, err := client.ModifyTrafficMirrorFilterRule(t.Context(), &ec2sdk.ModifyTrafficMirrorFilterRuleInput{
		TrafficMirrorFilterRuleId: aws.String(ruleID),
		Description:               aws.String("updated rule"),
	})
	require.NoError(t, err)
	assert.Equal(t, "updated rule", aws.ToString(modRuleOut.TrafficMirrorFilterRule.Description))

	targetOut, err := client.CreateTrafficMirrorTarget(t.Context(), &ec2sdk.CreateTrafficMirrorTargetInput{
		NetworkInterfaceId: aws.String(eniID),
		Description:        aws.String("test target"),
	})
	require.NoError(t, err)
	targetID := aws.ToString(targetOut.TrafficMirrorTarget.TrafficMirrorTargetId)
	assert.Equal(t, eniID, aws.ToString(targetOut.TrafficMirrorTarget.NetworkInterfaceId))

	sessOut, err := client.CreateTrafficMirrorSession(t.Context(), &ec2sdk.CreateTrafficMirrorSessionInput{
		NetworkInterfaceId:    aws.String(eniID),
		TrafficMirrorTargetId: aws.String(targetID),
		TrafficMirrorFilterId: aws.String(filterID),
		SessionNumber:         aws.Int32(1),
	})
	require.NoError(t, err)
	sessID := aws.ToString(sessOut.TrafficMirrorSession.TrafficMirrorSessionId)
	assert.NotZero(t, aws.ToInt32(sessOut.TrafficMirrorSession.VirtualNetworkId))

	modSessOut, err := client.ModifyTrafficMirrorSession(t.Context(), &ec2sdk.ModifyTrafficMirrorSessionInput{
		TrafficMirrorSessionId: aws.String(sessID),
		Description:            aws.String("updated session"),
	})
	require.NoError(t, err)
	assert.Equal(t, "updated session", aws.ToString(modSessOut.TrafficMirrorSession.Description))

	_, err = client.DeleteTrafficMirrorSession(t.Context(), &ec2sdk.DeleteTrafficMirrorSessionInput{
		TrafficMirrorSessionId: aws.String(sessID),
	})
	require.NoError(t, err)

	_, err = client.DeleteTrafficMirrorFilterRule(t.Context(), &ec2sdk.DeleteTrafficMirrorFilterRuleInput{
		TrafficMirrorFilterRuleId: aws.String(ruleID),
	})
	require.NoError(t, err)

	_, err = client.DeleteTrafficMirrorTarget(t.Context(), &ec2sdk.DeleteTrafficMirrorTargetInput{
		TrafficMirrorTargetId: aws.String(targetID),
	})
	require.NoError(t, err)

	_, err = client.DeleteTrafficMirrorFilter(t.Context(), &ec2sdk.DeleteTrafficMirrorFilterInput{
		TrafficMirrorFilterId: aws.String(filterID),
	})
	require.NoError(t, err)
}

// runRouteServer covers CreateRouteServer, DescribeRouteServers,
// ModifyRouteServer, CreateRouteServerEndpoint,
// DescribeRouteServerEndpoints, CreateRouteServerPeer,
// DescribeRouteServerPeers, AssociateRouteServer, GetRouteServerAssociations,
// DisassociateRouteServer, EnableRouteServerPropagation,
// GetTransitGatewayAttachmentPropagations (n/a), DisableRouteServerPropagation,
// DeleteRouteServerPeer, DeleteRouteServerEndpoint, DeleteRouteServer.
func runRouteServer(t *testing.T, _ *ec2.InMemoryBackend, client *ec2sdk.Client) {
	t.Helper()

	vpcOut, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.1.0.0/16")})
	require.NoError(t, err)
	vpcID := aws.ToString(vpcOut.Vpc.VpcId)
	subnetOut, err := client.CreateSubnet(t.Context(), &ec2sdk.CreateSubnetInput{
		VpcId:     vpcOut.Vpc.VpcId,
		CidrBlock: aws.String("10.1.0.0/24"),
	})
	require.NoError(t, err)
	rtOut, err := client.CreateRouteTable(t.Context(), &ec2sdk.CreateRouteTableInput{VpcId: vpcOut.Vpc.VpcId})
	require.NoError(t, err)

	rsOut, err := client.CreateRouteServer(t.Context(), &ec2sdk.CreateRouteServerInput{
		AmazonSideAsn: aws.Int64(4294967294),
	})
	require.NoError(t, err)
	rsID := aws.ToString(rsOut.RouteServer.RouteServerId)
	assert.Equal(t, int64(4294967294), aws.ToInt64(rsOut.RouteServer.AmazonSideAsn))

	descOut, err := client.DescribeRouteServers(t.Context(), &ec2sdk.DescribeRouteServersInput{
		RouteServerIds: []string{rsID},
	})
	require.NoError(t, err)
	require.Len(t, descOut.RouteServers, 1)

	modOut, err := client.ModifyRouteServer(t.Context(), &ec2sdk.ModifyRouteServerInput{
		RouteServerId:           aws.String(rsID),
		SnsNotificationsEnabled: aws.Bool(true),
	})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(modOut.RouteServer.SnsNotificationsEnabled))

	epOut, err := client.CreateRouteServerEndpoint(t.Context(), &ec2sdk.CreateRouteServerEndpointInput{
		RouteServerId: aws.String(rsID),
		SubnetId:      subnetOut.Subnet.SubnetId,
	})
	require.NoError(t, err)
	epID := aws.ToString(epOut.RouteServerEndpoint.RouteServerEndpointId)

	descEpOut, err := client.DescribeRouteServerEndpoints(
		t.Context(), &ec2sdk.DescribeRouteServerEndpointsInput{RouteServerEndpointIds: []string{epID}},
	)
	require.NoError(t, err)
	require.Len(t, descEpOut.RouteServerEndpoints, 1)

	peerOut, err := client.CreateRouteServerPeer(t.Context(), &ec2sdk.CreateRouteServerPeerInput{
		RouteServerEndpointId: aws.String(epID),
		PeerAddress:           aws.String("10.1.0.10"),
		BgpOptions:            &types.RouteServerBgpOptionsRequest{PeerAsn: aws.Int64(65001)},
	})
	require.NoError(t, err)
	peerID := aws.ToString(peerOut.RouteServerPeer.RouteServerPeerId)
	assert.Equal(t, "10.1.0.10", aws.ToString(peerOut.RouteServerPeer.PeerAddress))

	descPeerOut, err := client.DescribeRouteServerPeers(
		t.Context(), &ec2sdk.DescribeRouteServerPeersInput{RouteServerPeerIds: []string{peerID}},
	)
	require.NoError(t, err)
	require.Len(t, descPeerOut.RouteServerPeers, 1)

	assocOut, err := client.AssociateRouteServer(t.Context(), &ec2sdk.AssociateRouteServerInput{
		RouteServerId: aws.String(rsID),
		VpcId:         aws.String(vpcID),
	})
	require.NoError(t, err)
	assert.Equal(t, vpcID, aws.ToString(assocOut.RouteServerAssociation.VpcId))

	getAssocOut, err := client.GetRouteServerAssociations(
		t.Context(), &ec2sdk.GetRouteServerAssociationsInput{RouteServerId: aws.String(rsID)},
	)
	require.NoError(t, err)
	require.Len(t, getAssocOut.RouteServerAssociations, 1)

	propOut, err := client.EnableRouteServerPropagation(t.Context(), &ec2sdk.EnableRouteServerPropagationInput{
		RouteServerId: aws.String(rsID),
		RouteTableId:  rtOut.RouteTable.RouteTableId,
	})
	require.NoError(t, err)
	assert.Equal(
		t,
		aws.ToString(rtOut.RouteTable.RouteTableId),
		aws.ToString(propOut.RouteServerPropagation.RouteTableId),
	)

	_, err = client.DisableRouteServerPropagation(t.Context(), &ec2sdk.DisableRouteServerPropagationInput{
		RouteServerId: aws.String(rsID),
		RouteTableId:  rtOut.RouteTable.RouteTableId,
	})
	require.NoError(t, err)

	_, err = client.DisassociateRouteServer(t.Context(), &ec2sdk.DisassociateRouteServerInput{
		RouteServerId: aws.String(rsID),
		VpcId:         aws.String(vpcID),
	})
	require.NoError(t, err)

	_, err = client.DeleteRouteServerPeer(t.Context(), &ec2sdk.DeleteRouteServerPeerInput{
		RouteServerPeerId: aws.String(peerID),
	})
	require.NoError(t, err)

	_, err = client.DeleteRouteServerEndpoint(t.Context(), &ec2sdk.DeleteRouteServerEndpointInput{
		RouteServerEndpointId: aws.String(epID),
	})
	require.NoError(t, err)

	_, err = client.DeleteRouteServer(t.Context(), &ec2sdk.DeleteRouteServerInput{
		RouteServerId: aws.String(rsID),
	})
	require.NoError(t, err)
}

// runLocalGateway covers DescribeLocalGateways,
// CreateLocalGatewayVirtualInterfaceGroup, CreateLocalGatewayVirtualInterface,
// DescribeLocalGatewayVirtualInterfaces,
// DescribeLocalGatewayVirtualInterfaceGroups, CreateLocalGatewayRouteTable,
// DescribeLocalGatewayRouteTables, CreateLocalGatewayRoute,
// ModifyLocalGatewayRoute, DeleteLocalGatewayRoute,
// CreateLocalGatewayRouteTableVpcAssociation,
// DescribeLocalGatewayRouteTableVpcAssociations,
// DeleteLocalGatewayRouteTableVpcAssociation,
// CreateLocalGatewayRouteTableVirtualInterfaceGroupAssociation,
// DescribeLocalGatewayRouteTableVirtualInterfaceGroupAssociations,
// DeleteLocalGatewayRouteTableVirtualInterfaceGroupAssociation,
// DeleteLocalGatewayVirtualInterface, DeleteLocalGatewayVirtualInterfaceGroup,
// DeleteLocalGatewayRouteTable.
func runLocalGateway(t *testing.T, backend *ec2.InMemoryBackend, client *ec2sdk.Client) {
	t.Helper()

	lg, err := backend.SeedLocalGateway(ec2.LocalGateway{})
	require.NoError(t, err)

	vpcOut, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.2.0.0/16")})
	require.NoError(t, err)

	descLGOut, err := client.DescribeLocalGateways(
		t.Context(), &ec2sdk.DescribeLocalGatewaysInput{LocalGatewayIds: []string{lg.LocalGatewayID}},
	)
	require.NoError(t, err)
	require.Len(t, descLGOut.LocalGateways, 1)

	vifGroupOut, err := client.CreateLocalGatewayVirtualInterfaceGroup(
		t.Context(), &ec2sdk.CreateLocalGatewayVirtualInterfaceGroupInput{
			LocalGatewayId: aws.String(lg.LocalGatewayID),
		},
	)
	require.NoError(t, err)
	vifGroupID := aws.ToString(vifGroupOut.LocalGatewayVirtualInterfaceGroup.LocalGatewayVirtualInterfaceGroupId)

	vifOut, err := client.CreateLocalGatewayVirtualInterface(
		t.Context(), &ec2sdk.CreateLocalGatewayVirtualInterfaceInput{
			LocalGatewayVirtualInterfaceGroupId: aws.String(vifGroupID),
			LocalAddress:                        aws.String("169.254.0.1"),
			PeerAddress:                         aws.String("169.254.0.2"),
			Vlan:                                aws.Int32(100),
			OutpostLagId:                        aws.String("lgw-lag-test"),
		},
	)
	require.NoError(t, err)
	vifID := aws.ToString(vifOut.LocalGatewayVirtualInterface.LocalGatewayVirtualInterfaceId)

	descVifsOut, err := client.DescribeLocalGatewayVirtualInterfaces(
		t.Context(), &ec2sdk.DescribeLocalGatewayVirtualInterfacesInput{
			LocalGatewayVirtualInterfaceIds: []string{vifID},
		},
	)
	require.NoError(t, err)
	require.Len(t, descVifsOut.LocalGatewayVirtualInterfaces, 1)

	descVifGroupsOut, err := client.DescribeLocalGatewayVirtualInterfaceGroups(
		t.Context(), &ec2sdk.DescribeLocalGatewayVirtualInterfaceGroupsInput{
			LocalGatewayVirtualInterfaceGroupIds: []string{vifGroupID},
		},
	)
	require.NoError(t, err)
	require.Len(t, descVifGroupsOut.LocalGatewayVirtualInterfaceGroups, 1)

	rtOut, err := client.CreateLocalGatewayRouteTable(
		t.Context(), &ec2sdk.CreateLocalGatewayRouteTableInput{LocalGatewayId: aws.String(lg.LocalGatewayID)},
	)
	require.NoError(t, err)
	rtID := aws.ToString(rtOut.LocalGatewayRouteTable.LocalGatewayRouteTableId)

	descRTOut, err := client.DescribeLocalGatewayRouteTables(
		t.Context(), &ec2sdk.DescribeLocalGatewayRouteTablesInput{LocalGatewayRouteTableIds: []string{rtID}},
	)
	require.NoError(t, err)
	require.Len(t, descRTOut.LocalGatewayRouteTables, 1)

	routeOut, err := client.CreateLocalGatewayRoute(t.Context(), &ec2sdk.CreateLocalGatewayRouteInput{
		LocalGatewayRouteTableId:            aws.String(rtID),
		DestinationCidrBlock:                aws.String("192.168.0.0/24"),
		LocalGatewayVirtualInterfaceGroupId: aws.String(vifGroupID),
	})
	require.NoError(t, err)
	assert.Equal(t, "192.168.0.0/24", aws.ToString(routeOut.Route.DestinationCidrBlock))

	modRouteOut, err := client.ModifyLocalGatewayRoute(t.Context(), &ec2sdk.ModifyLocalGatewayRouteInput{
		LocalGatewayRouteTableId:            aws.String(rtID),
		DestinationCidrBlock:                aws.String("192.168.0.0/24"),
		LocalGatewayVirtualInterfaceGroupId: aws.String(vifGroupID),
	})
	require.NoError(t, err)
	assert.Equal(t, "192.168.0.0/24", aws.ToString(modRouteOut.Route.DestinationCidrBlock))

	_, err = client.DeleteLocalGatewayRoute(t.Context(), &ec2sdk.DeleteLocalGatewayRouteInput{
		LocalGatewayRouteTableId: aws.String(rtID),
		DestinationCidrBlock:     aws.String("192.168.0.0/24"),
	})
	require.NoError(t, err)

	vpcAssocOut, err := client.CreateLocalGatewayRouteTableVpcAssociation(
		t.Context(), &ec2sdk.CreateLocalGatewayRouteTableVpcAssociationInput{
			LocalGatewayRouteTableId: aws.String(rtID),
			VpcId:                    vpcOut.Vpc.VpcId,
		},
	)
	require.NoError(t, err)
	vpcAssocID := aws.ToString(vpcAssocOut.LocalGatewayRouteTableVpcAssociation.LocalGatewayRouteTableVpcAssociationId)

	descVpcAssocOut, err := client.DescribeLocalGatewayRouteTableVpcAssociations(
		t.Context(), &ec2sdk.DescribeLocalGatewayRouteTableVpcAssociationsInput{
			LocalGatewayRouteTableVpcAssociationIds: []string{vpcAssocID},
		},
	)
	require.NoError(t, err)
	require.Len(t, descVpcAssocOut.LocalGatewayRouteTableVpcAssociations, 1)

	vifGroupAssocOut, err := client.CreateLocalGatewayRouteTableVirtualInterfaceGroupAssociation(
		t.Context(), &ec2sdk.CreateLocalGatewayRouteTableVirtualInterfaceGroupAssociationInput{
			LocalGatewayRouteTableId:            aws.String(rtID),
			LocalGatewayVirtualInterfaceGroupId: aws.String(vifGroupID),
		},
	)
	require.NoError(t, err)
	vifGroupAssoc := vifGroupAssocOut.LocalGatewayRouteTableVirtualInterfaceGroupAssociation
	vifGroupAssocID := aws.ToString(vifGroupAssoc.LocalGatewayRouteTableVirtualInterfaceGroupAssociationId)

	descVifGroupAssocOut, err := client.DescribeLocalGatewayRouteTableVirtualInterfaceGroupAssociations(
		t.Context(), &ec2sdk.DescribeLocalGatewayRouteTableVirtualInterfaceGroupAssociationsInput{
			LocalGatewayRouteTableVirtualInterfaceGroupAssociationIds: []string{vifGroupAssocID},
		},
	)
	require.NoError(t, err)
	require.Len(t, descVifGroupAssocOut.LocalGatewayRouteTableVirtualInterfaceGroupAssociations, 1)

	_, err = client.DeleteLocalGatewayRouteTableVirtualInterfaceGroupAssociation(
		t.Context(), &ec2sdk.DeleteLocalGatewayRouteTableVirtualInterfaceGroupAssociationInput{
			LocalGatewayRouteTableVirtualInterfaceGroupAssociationId: aws.String(vifGroupAssocID),
		},
	)
	require.NoError(t, err)

	_, err = client.DeleteLocalGatewayRouteTableVpcAssociation(
		t.Context(), &ec2sdk.DeleteLocalGatewayRouteTableVpcAssociationInput{
			LocalGatewayRouteTableVpcAssociationId: aws.String(vpcAssocID),
		},
	)
	require.NoError(t, err)

	_, err = client.DeleteLocalGatewayVirtualInterface(
		t.Context(), &ec2sdk.DeleteLocalGatewayVirtualInterfaceInput{
			LocalGatewayVirtualInterfaceId: aws.String(vifID),
		},
	)
	require.NoError(t, err)

	_, err = client.DeleteLocalGatewayVirtualInterfaceGroup(
		t.Context(), &ec2sdk.DeleteLocalGatewayVirtualInterfaceGroupInput{
			LocalGatewayVirtualInterfaceGroupId: aws.String(vifGroupID),
		},
	)
	require.NoError(t, err)

	_, err = client.DeleteLocalGatewayRouteTable(
		t.Context(), &ec2sdk.DeleteLocalGatewayRouteTableInput{LocalGatewayRouteTableId: aws.String(rtID)},
	)
	require.NoError(t, err)
}
