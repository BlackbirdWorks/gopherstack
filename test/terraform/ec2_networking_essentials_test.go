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

// TestTerraform_Ec2NetworkingEssentials provisions everyday EC2 networking resources
// (EIP family, NAT gateway, routes, security group / network ACL rules,
// DHCP options, VPC CIDR associations, subnet CIDR reservations, account
// defaults, EBS/AMI block-public-access singletons) plus the VPC endpoint
// service/connection/association family, then verifies each through the
// EC2 SDK.
func TestTerraform_Ec2NetworkingEssentials(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "ec2-networking-essentials",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return vpcCIDRVars(t)
			},
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()
				client := createEC2Client(t)

				verifyEc2NetworkingEssentialsNetworking(ctx, t, client)
				verifyEc2NetworkingEssentialsElasticIPsAndNAT(ctx, t, client)
				verifyEc2NetworkingEssentialsSGAndNACLRules(ctx, t, client)
				verifyEc2NetworkingEssentialsAccountSettings(ctx, t, client)
				verifyEc2NetworkingEssentialsVpcEndpointFamily(ctx, t, client)
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

// verifyEc2NetworkingEssentialsNetworking checks the VPC's secondary IPv4/IPv6 CIDR
// associations, DHCP options association, and subnet CIDR reservation.
func verifyEc2NetworkingEssentialsNetworking(ctx context.Context, t *testing.T, client *ec2svc.Client) {
	t.Helper()

	vpcsOut, err := client.DescribeVpcs(ctx, &ec2svc.DescribeVpcsInput{
		Filters: []ec2types.Filter{
			{Name: aws.String("tag:Name"), Values: []string{"ecne-vpc"}},
		},
	})
	require.NoError(t, err, "DescribeVpcs should succeed")
	require.Len(t, vpcsOut.Vpcs, 1)
	vpc := vpcsOut.Vpcs[0]

	var foundIPv4Assoc, foundIPv6Assoc bool

	for _, a := range vpc.CidrBlockAssociationSet {
		if aws.ToString(a.CidrBlock) == "10.99.0.0/16" {
			foundIPv4Assoc = true
		}
	}

	for range vpc.Ipv6CidrBlockAssociationSet {
		foundIPv6Assoc = true
	}

	assert.True(t, foundIPv4Assoc, "aws_vpc_ipv4_cidr_block_association should add the secondary CIDR")
	assert.True(t, foundIPv6Assoc, "aws_vpc_ipv6_cidr_block_association should assign an IPv6 CIDR")

	dhcpOut, err := client.DescribeDhcpOptions(ctx, &ec2svc.DescribeDhcpOptionsInput{
		Filters: []ec2types.Filter{
			{Name: aws.String("tag:Name"), Values: []string{"ecne-dhcp-opts"}},
		},
	})
	require.NoError(t, err, "DescribeDhcpOptions should succeed")
	require.Len(t, dhcpOut.DhcpOptions, 1)

	subnetsOut, err := client.DescribeSubnets(ctx, &ec2svc.DescribeSubnetsInput{
		Filters: []ec2types.Filter{
			{Name: aws.String("tag:Name"), Values: []string{"ecne-subnet"}},
		},
	})
	require.NoError(t, err, "DescribeSubnets should succeed")
	require.Len(t, subnetsOut.Subnets, 1)
	subnetID := aws.ToString(subnetsOut.Subnets[0].SubnetId)

	resOut, err := client.GetSubnetCidrReservations(ctx, &ec2svc.GetSubnetCidrReservationsInput{
		SubnetId: aws.String(subnetID),
	})
	require.NoError(t, err, "GetSubnetCidrReservations should succeed")
	assert.NotEmpty(t, resOut.SubnetIpv4CidrReservations, "aws_ec2_subnet_cidr_reservation should create a reservation")
}

// verifyEc2NetworkingEssentialsElasticIPsAndNAT checks the EIP, its reverse-DNS domain
// name, its association to an instance, and the NAT gateway plus route.
func verifyEc2NetworkingEssentialsElasticIPsAndNAT(ctx context.Context, t *testing.T, client *ec2svc.Client) {
	t.Helper()

	addrOut, err := client.DescribeAddresses(ctx, &ec2svc.DescribeAddressesInput{
		Filters: []ec2types.Filter{
			{Name: aws.String("tag:Name"), Values: []string{"ecne-eip"}},
		},
	})
	require.NoError(t, err, "DescribeAddresses should succeed")
	require.Len(t, addrOut.Addresses, 1)
	allocationID := aws.ToString(addrOut.Addresses[0].AllocationId)
	assert.NotEmpty(t, aws.ToString(addrOut.Addresses[0].InstanceId),
		"aws_eip_association should associate the EIP to the instance")

	attrOut, err := client.DescribeAddressesAttribute(ctx, &ec2svc.DescribeAddressesAttributeInput{
		AllocationIds: []string{allocationID},
	})
	require.NoError(t, err, "DescribeAddressesAttribute should succeed")
	require.Len(t, attrOut.Addresses, 1)
	assert.Equal(t, "ecne.example.com", aws.ToString(attrOut.Addresses[0].PtrRecord),
		"aws_eip_domain_name should set the PTR record")

	natOut, err := client.DescribeNatGateways(ctx, &ec2svc.DescribeNatGatewaysInput{
		Filter: []ec2types.Filter{
			{Name: aws.String("tag:Name"), Values: []string{"ecne-natgw"}},
		},
	})
	require.NoError(t, err, "DescribeNatGateways should succeed")
	require.Len(t, natOut.NatGateways, 1)
	assert.Equal(t, ec2types.NatGatewayStateAvailable, natOut.NatGateways[0].State)

	rtOut, err := client.DescribeRouteTables(ctx, &ec2svc.DescribeRouteTablesInput{
		Filters: []ec2types.Filter{
			{Name: aws.String("tag:Name"), Values: []string{"ecne-rt"}},
		},
	})
	require.NoError(t, err, "DescribeRouteTables should succeed")
	require.Len(t, rtOut.RouteTables, 1)
	rt := rtOut.RouteTables[0]

	var foundRoute, foundAssoc bool

	for _, r := range rt.Routes {
		if aws.ToString(r.DestinationCidrBlock) == "0.0.0.0/0" && r.GatewayId != nil {
			foundRoute = true
		}
	}

	for _, a := range rt.Associations {
		if a.SubnetId != nil {
			foundAssoc = true
		}
	}

	assert.True(t, foundRoute, "aws_route should add the default route")
	assert.True(t, foundAssoc, "aws_route_table_association should associate the subnet")
}

// verifyEc2NetworkingEssentialsSGAndNACLRules checks the security group rule family and
// the network ACL rule.
func verifyEc2NetworkingEssentialsSGAndNACLRules(ctx context.Context, t *testing.T, client *ec2svc.Client) {
	t.Helper()

	sgOut, err := client.DescribeSecurityGroups(ctx, &ec2svc.DescribeSecurityGroupsInput{
		Filters: []ec2types.Filter{
			{Name: aws.String("group-name"), Values: []string{"ecne-sg-rule"}},
		},
	})
	require.NoError(t, err, "DescribeSecurityGroups should succeed")
	require.Len(t, sgOut.SecurityGroups, 1)

	var foundSGRule bool

	for _, p := range sgOut.SecurityGroups[0].IpPermissions {
		if aws.ToInt32(p.FromPort) == 22 && aws.ToInt32(p.ToPort) == 22 {
			foundSGRule = true
		}
	}

	assert.True(t, foundSGRule, "aws_security_group_rule should add the ingress rule")

	sgOut2, err := client.DescribeSecurityGroups(ctx, &ec2svc.DescribeSecurityGroupsInput{
		Filters: []ec2types.Filter{
			{Name: aws.String("group-name"), Values: []string{"ecne-sg"}},
		},
	})
	require.NoError(t, err, "DescribeSecurityGroups should succeed")
	require.Len(t, sgOut2.SecurityGroups, 1)

	var foundIngressRule, foundEgressRule bool

	for _, p := range sgOut2.SecurityGroups[0].IpPermissions {
		for _, r := range p.IpRanges {
			if aws.ToString(r.CidrIp) == "192.168.0.0/16" {
				foundIngressRule = true
			}
		}
	}

	for _, p := range sgOut2.SecurityGroups[0].IpPermissionsEgress {
		for _, r := range p.IpRanges {
			if aws.ToString(r.CidrIp) == "0.0.0.0/0" && aws.ToString(p.IpProtocol) == "-1" {
				foundEgressRule = true
			}
		}
	}

	assert.True(t, foundIngressRule, "aws_vpc_security_group_ingress_rule should add the ingress rule")
	assert.True(t, foundEgressRule, "aws_vpc_security_group_egress_rule should add the egress rule")

	naclOut, err := client.DescribeNetworkAcls(ctx, &ec2svc.DescribeNetworkAclsInput{
		Filters: []ec2types.Filter{
			{Name: aws.String("tag:Name"), Values: []string{"ecne-nacl"}},
		},
	})
	require.NoError(t, err, "DescribeNetworkAcls should succeed")
	require.Len(t, naclOut.NetworkAcls, 1)

	var foundNACLRule bool

	for _, e := range naclOut.NetworkAcls[0].Entries {
		if aws.ToInt32(e.RuleNumber) == 100 && !aws.ToBool(e.Egress) {
			foundNACLRule = true
		}
	}

	assert.True(t, foundNACLRule, "aws_network_acl_rule should add the ingress entry")
}

// verifyEc2NetworkingEssentialsAccountSettings checks the account-wide singletons: EBS
// snapshot block-public-access, VPC block-public-access options/exclusion,
// instance metadata defaults, and default credit specification.
func verifyEc2NetworkingEssentialsAccountSettings(ctx context.Context, t *testing.T, client *ec2svc.Client) {
	t.Helper()

	snapBPAOut, err := client.GetSnapshotBlockPublicAccessState(ctx, &ec2svc.GetSnapshotBlockPublicAccessStateInput{})
	require.NoError(t, err, "GetSnapshotBlockPublicAccessState should succeed")
	assert.Equal(t, "block-all-sharing", string(snapBPAOut.State))

	vpcBPAOut, err := client.DescribeVpcBlockPublicAccessOptions(
		ctx,
		&ec2svc.DescribeVpcBlockPublicAccessOptionsInput{},
	)
	require.NoError(t, err, "DescribeVpcBlockPublicAccessOptions should succeed")
	require.NotNil(t, vpcBPAOut.VpcBlockPublicAccessOptions)
	assert.Equal(
		t,
		ec2types.InternetGatewayBlockModeBlockBidirectional,
		vpcBPAOut.VpcBlockPublicAccessOptions.InternetGatewayBlockMode,
	)

	// DescribeVpcBlockPublicAccessExclusions has no filter that scopes to a
	// single VPC (ResourceArn is an opaque exclusion-object ARN, not the
	// excluded VPC's own ARN), and other fixtures in this CI shard may have
	// their own exclusions in the unfiltered account-wide list -- find ours
	// by mode rather than indexing [0] or asserting an exact count.
	exclOut, err := client.DescribeVpcBlockPublicAccessExclusions(
		ctx,
		&ec2svc.DescribeVpcBlockPublicAccessExclusionsInput{},
	)
	require.NoError(t, err, "DescribeVpcBlockPublicAccessExclusions should succeed")
	findBy(t, exclOut.VpcBlockPublicAccessExclusions, func(e ec2types.VpcBlockPublicAccessExclusion) bool {
		return e.InternetGatewayExclusionMode == ec2types.InternetGatewayExclusionModeAllowBidirectional
	}, "ecne VPC block-public-access exclusion")

	imdOut, err := client.GetInstanceMetadataDefaults(ctx, &ec2svc.GetInstanceMetadataDefaultsInput{})
	require.NoError(t, err, "GetInstanceMetadataDefaults should succeed")
	require.NotNil(t, imdOut.AccountLevel)
	assert.Equal(t, ec2types.HttpTokensStateRequired, imdOut.AccountLevel.HttpTokens)

	creditOut, err := client.GetDefaultCreditSpecification(ctx, &ec2svc.GetDefaultCreditSpecificationInput{
		InstanceFamily: ec2types.UnlimitedSupportedInstanceFamilyT2,
	})
	require.NoError(t, err, "GetDefaultCreditSpecification should succeed")
	require.NotNil(t, creditOut.InstanceFamilyCreditSpecification)
	assert.Equal(t, "standard", aws.ToString(creditOut.InstanceFamilyCreditSpecification.CpuCredits))
}

// verifyEc2NetworkingEssentialsVpcEndpointFamily checks the VPC endpoint service, its
// allowed principal, connection notification, and the gateway/interface
// endpoints with their policy, route table, subnet, security group, and
// private DNS associations.
func verifyEc2NetworkingEssentialsVpcEndpointFamily(ctx context.Context, t *testing.T, client *ec2svc.Client) {
	t.Helper()

	svcOut, err := client.DescribeVpcEndpointServiceConfigurations(
		ctx,
		&ec2svc.DescribeVpcEndpointServiceConfigurationsInput{
			Filters: []ec2types.Filter{
				{Name: aws.String("tag:Name"), Values: []string{"ecne-vpces"}},
			},
		},
	)
	require.NoError(t, err, "DescribeVpcEndpointServiceConfigurations should succeed")
	require.Len(t, svcOut.ServiceConfigurations, 1)
	svcID := aws.ToString(svcOut.ServiceConfigurations[0].ServiceId)

	permOut, err := client.DescribeVpcEndpointServicePermissions(
		ctx,
		&ec2svc.DescribeVpcEndpointServicePermissionsInput{ServiceId: aws.String(svcID)},
	)
	require.NoError(t, err, "DescribeVpcEndpointServicePermissions should succeed")

	var foundPrincipal bool

	for _, p := range permOut.AllowedPrincipals {
		if aws.ToString(p.Principal) == "arn:aws:iam::123456789012:root" {
			foundPrincipal = true
		}
	}

	assert.True(t, foundPrincipal, "aws_vpc_endpoint_service_allowed_principal should allow the principal")

	notifOut, err := client.DescribeVpcEndpointConnectionNotifications(
		ctx,
		&ec2svc.DescribeVpcEndpointConnectionNotificationsInput{
			Filters: []ec2types.Filter{
				{Name: aws.String("service-id"), Values: []string{svcID}},
			},
		},
	)
	require.NoError(t, err, "DescribeVpcEndpointConnectionNotifications should succeed")
	require.Len(t, notifOut.ConnectionNotificationSet, 1)

	gwOut, err := client.DescribeVpcEndpoints(ctx, &ec2svc.DescribeVpcEndpointsInput{
		Filters: []ec2types.Filter{
			{Name: aws.String("tag:Name"), Values: []string{"ecne-vpce-gateway"}},
		},
	})
	require.NoError(t, err, "DescribeVpcEndpoints (gateway) should succeed")
	require.Len(t, gwOut.VpcEndpoints, 1)
	gwEndpoint := gwOut.VpcEndpoints[0]
	assert.NotEmpty(t, aws.ToString(gwEndpoint.PolicyDocument), "aws_vpc_endpoint_policy should set the policy")
	assert.NotEmpty(
		t,
		gwEndpoint.RouteTableIds,
		"aws_vpc_endpoint_route_table_association should associate a route table",
	)

	ifaceOut, err := client.DescribeVpcEndpoints(ctx, &ec2svc.DescribeVpcEndpointsInput{
		Filters: []ec2types.Filter{
			{Name: aws.String("tag:Name"), Values: []string{"ecne-vpce-interface"}},
		},
	})
	require.NoError(t, err, "DescribeVpcEndpoints (interface) should succeed")
	require.Len(t, ifaceOut.VpcEndpoints, 1)
	iface := ifaceOut.VpcEndpoints[0]
	assert.GreaterOrEqual(t, len(iface.SubnetIds), 2,
		"aws_vpc_endpoint_subnet_association should add the second subnet")
	assert.NotEmpty(t, iface.Groups, "aws_vpc_endpoint_security_group_association should attach the security group")
	assert.True(t, aws.ToBool(iface.PrivateDnsEnabled), "aws_vpc_endpoint_private_dns should enable private DNS")

	connOut, err := client.DescribeVpcEndpointConnections(ctx, &ec2svc.DescribeVpcEndpointConnectionsInput{
		Filters: []ec2types.Filter{
			{Name: aws.String("service-id"), Values: []string{svcID}},
		},
	})
	require.NoError(t, err, "DescribeVpcEndpointConnections should succeed")

	var foundAcceptedConn bool

	for _, c := range connOut.VpcEndpointConnections {
		if aws.ToString(c.VpcEndpointId) == aws.ToString(iface.VpcEndpointId) &&
			c.VpcEndpointState == ec2types.StateAvailable {
			foundAcceptedConn = true
		}
	}

	assert.True(t, foundAcceptedConn, "aws_vpc_endpoint_connection_accepter should accept the connection")
}
