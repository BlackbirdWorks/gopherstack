package ec2_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestRealClient_VpnEndpointsAndSpot covers the gopherstack-xhu2t/99nj
// ec2query filter/field sweep's 2026-09-13 continuation: Client VPN
// (DisconnectOnSessionTimeout/EndpointIpAddressType/TrafficIpAddressType),
// VPN connection options (LocalIpv6NetworkCidr/RemoteIpv6NetworkCidr/
// TunnelBandwidth), Traffic Mirror (ModifyTrafficMirrorFilterRule/Session's
// RemoveFields), VPC Endpoints (PolicyDocument/PrivateDnsEnabled/
// SecurityGroupIds/ServiceRegion, ModifyVpcEndpoint.ResetPolicy), Instance
// Connect Endpoint (IpAddressType, ModifyInstanceConnectEndpoint's
// SecurityGroupIds), and Spot Instances (InstanceCount/
// AvailabilityZoneGroup/LaunchGroup/InstanceInterruptionBehavior/
// ValidUntil). Each row gets its own fresh handler+backend so rows can run
// in parallel without shared state.
func TestRealClient_VpnEndpointsAndSpot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, client *ec2sdk.Client)
		name string
	}{
		{runClientVpnFields, "client_vpn_fields"},
		{runVpnConnectionOptionsFields, "vpn_connection_options_fields"},
		{runTrafficMirrorRemoveFields, "traffic_mirror_remove_fields"},
		{runVpcEndpointFields, "vpc_endpoint_fields"},
		{runInstanceConnectEndpointFields, "instance_connect_endpoint_fields"},
		{runSpotInstancesFields, "spot_instances_fields"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
			h.AccountID = "000000000000"
			client := newTestEC2Client(t, h)
			tt.run(t, client)
		})
	}
}

// runClientVpnFields covers CreateClientVpnEndpoint's
// DisconnectOnSessionTimeout/EndpointIpAddressType/TrafficIpAddressType and
// ModifyClientVpnEndpoint's DisconnectOnSessionTimeout.
func runClientVpnFields(t *testing.T, client *ec2sdk.Client) {
	t.Helper()

	authOptions := []types.ClientVpnAuthenticationRequest{
		{Type: types.ClientVpnAuthenticationTypeCertificateAuthentication},
	}

	createOut, err := client.CreateClientVpnEndpoint(t.Context(), &ec2sdk.CreateClientVpnEndpointInput{
		ClientCidrBlock:            aws.String("10.100.0.0/16"),
		ServerCertificateArn:       aws.String("arn:aws:acm:us-east-1:000000000000:certificate/test"),
		ConnectionLogOptions:       &types.ConnectionLogOptions{Enabled: aws.Bool(false)},
		AuthenticationOptions:      authOptions,
		DisconnectOnSessionTimeout: aws.Bool(false),
		EndpointIpAddressType:      types.EndpointIpAddressTypeIpv4,
		TrafficIpAddressType:       types.TrafficIpAddressTypeIpv4,
	})
	require.NoError(t, err)
	endpointID := aws.ToString(createOut.ClientVpnEndpointId)

	descOut, err := client.DescribeClientVpnEndpoints(t.Context(), &ec2sdk.DescribeClientVpnEndpointsInput{
		ClientVpnEndpointIds: []string{endpointID},
	})
	require.NoError(t, err)
	require.Len(t, descOut.ClientVpnEndpoints, 1)
	ep := descOut.ClientVpnEndpoints[0]
	assert.False(t, aws.ToBool(ep.DisconnectOnSessionTimeout))
	assert.Equal(t, types.EndpointIpAddressTypeIpv4, ep.EndpointIpAddressType)
	assert.Equal(t, types.TrafficIpAddressTypeIpv4, ep.TrafficIpAddressType)

	modOut, err := client.ModifyClientVpnEndpoint(t.Context(), &ec2sdk.ModifyClientVpnEndpointInput{
		ClientVpnEndpointId:        aws.String(endpointID),
		DisconnectOnSessionTimeout: aws.Bool(true),
	})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(modOut.Return))

	descOut2, err := client.DescribeClientVpnEndpoints(t.Context(), &ec2sdk.DescribeClientVpnEndpointsInput{
		ClientVpnEndpointIds: []string{endpointID},
	})
	require.NoError(t, err)
	require.Len(t, descOut2.ClientVpnEndpoints, 1)
	assert.True(t, aws.ToBool(descOut2.ClientVpnEndpoints[0].DisconnectOnSessionTimeout))
}

// runVpnConnectionOptionsFields covers ModifyVpnConnectionOptions'
// LocalIpv6NetworkCidr/RemoteIpv6NetworkCidr/TunnelBandwidth.
func runVpnConnectionOptionsFields(t *testing.T, client *ec2sdk.Client) {
	t.Helper()

	cgwOut, err := client.CreateCustomerGateway(t.Context(), &ec2sdk.CreateCustomerGatewayInput{
		Type: types.GatewayTypeIpsec1, BgpAsn: aws.Int32(65000), PublicIp: aws.String("203.0.113.1"),
	})
	require.NoError(t, err)

	vgwOut, err := client.CreateVpnGateway(t.Context(), &ec2sdk.CreateVpnGatewayInput{Type: types.GatewayTypeIpsec1})
	require.NoError(t, err)

	connOut, err := client.CreateVpnConnection(t.Context(), &ec2sdk.CreateVpnConnectionInput{
		Type:              aws.String("ipsec.1"),
		CustomerGatewayId: cgwOut.CustomerGateway.CustomerGatewayId,
		VpnGatewayId:      vgwOut.VpnGateway.VpnGatewayId,
	})
	require.NoError(t, err)
	connID := aws.ToString(connOut.VpnConnection.VpnConnectionId)

	// Default TunnelBandwidth at creation.
	assert.Equal(t, types.VpnTunnelBandwidthStandard, connOut.VpnConnection.Options.TunnelBandwidth)

	modOut, err := client.ModifyVpnConnectionOptions(t.Context(), &ec2sdk.ModifyVpnConnectionOptionsInput{
		VpnConnectionId:       aws.String(connID),
		LocalIpv6NetworkCidr:  aws.String("2001:db8:1::/64"),
		RemoteIpv6NetworkCidr: aws.String("2001:db8:2::/64"),
		TunnelBandwidth:       types.VpnTunnelBandwidthLarge,
	})
	require.NoError(t, err)
	require.NotNil(t, modOut.VpnConnection)
	assert.Equal(t, "2001:db8:1::/64", aws.ToString(modOut.VpnConnection.Options.LocalIpv6NetworkCidr))
	assert.Equal(t, "2001:db8:2::/64", aws.ToString(modOut.VpnConnection.Options.RemoteIpv6NetworkCidr))
	assert.Equal(t, types.VpnTunnelBandwidthLarge, modOut.VpnConnection.Options.TunnelBandwidth)
}

// runTrafficMirrorRemoveFields covers ModifyTrafficMirrorFilterRule and
// ModifyTrafficMirrorSession's RemoveFields, which reset a named property to
// its default.
func runTrafficMirrorRemoveFields(t *testing.T, client *ec2sdk.Client) {
	t.Helper()

	vpcOut, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.0.0.0/16")})
	require.NoError(t, err)
	subnetOut, err := client.CreateSubnet(t.Context(), &ec2sdk.CreateSubnetInput{
		VpcId: vpcOut.Vpc.VpcId, CidrBlock: aws.String("10.0.0.0/24"),
	})
	require.NoError(t, err)
	eniOut, err := client.CreateNetworkInterface(t.Context(), &ec2sdk.CreateNetworkInterfaceInput{
		SubnetId: subnetOut.Subnet.SubnetId,
	})
	require.NoError(t, err)
	eniID := aws.ToString(eniOut.NetworkInterface.NetworkInterfaceId)

	filterOut, err := client.CreateTrafficMirrorFilter(t.Context(), &ec2sdk.CreateTrafficMirrorFilterInput{
		Description: aws.String("rm-fields filter"),
	})
	require.NoError(t, err)
	filterID := aws.ToString(filterOut.TrafficMirrorFilter.TrafficMirrorFilterId)

	ruleOut, err := client.CreateTrafficMirrorFilterRule(t.Context(), &ec2sdk.CreateTrafficMirrorFilterRuleInput{
		TrafficMirrorFilterId: aws.String(filterID),
		TrafficDirection:      types.TrafficDirectionIngress,
		RuleAction:            types.TrafficMirrorRuleActionAccept,
		RuleNumber:            aws.Int32(1),
		SourceCidrBlock:       aws.String("0.0.0.0/0"),
		DestinationCidrBlock:  aws.String("0.0.0.0/0"),
		Protocol:              aws.Int32(6),
		Description:           aws.String("rule with protocol"),
	})
	require.NoError(t, err)
	ruleID := aws.ToString(ruleOut.TrafficMirrorFilterRule.TrafficMirrorFilterRuleId)
	require.Equal(t, int32(6), aws.ToInt32(ruleOut.TrafficMirrorFilterRule.Protocol))

	modRuleOut, err := client.ModifyTrafficMirrorFilterRule(t.Context(), &ec2sdk.ModifyTrafficMirrorFilterRuleInput{
		TrafficMirrorFilterRuleId: aws.String(ruleID),
		RemoveFields:              []types.TrafficMirrorFilterRuleField{types.TrafficMirrorFilterRuleFieldProtocol},
	})
	require.NoError(t, err)
	assert.Zero(t, aws.ToInt32(modRuleOut.TrafficMirrorFilterRule.Protocol), "RemoveFields=protocol must reset it")

	targetOut, err := client.CreateTrafficMirrorTarget(t.Context(), &ec2sdk.CreateTrafficMirrorTargetInput{
		NetworkInterfaceId: aws.String(eniID),
	})
	require.NoError(t, err)
	targetID := aws.ToString(targetOut.TrafficMirrorTarget.TrafficMirrorTargetId)

	sessOut, err := client.CreateTrafficMirrorSession(t.Context(), &ec2sdk.CreateTrafficMirrorSessionInput{
		NetworkInterfaceId:    aws.String(eniID),
		TrafficMirrorTargetId: aws.String(targetID),
		TrafficMirrorFilterId: aws.String(filterID),
		SessionNumber:         aws.Int32(1),
		PacketLength:          aws.Int32(200),
	})
	require.NoError(t, err)
	sessID := aws.ToString(sessOut.TrafficMirrorSession.TrafficMirrorSessionId)
	require.Equal(t, int32(200), aws.ToInt32(sessOut.TrafficMirrorSession.PacketLength))

	modSessOut, err := client.ModifyTrafficMirrorSession(t.Context(), &ec2sdk.ModifyTrafficMirrorSessionInput{
		TrafficMirrorSessionId: aws.String(sessID),
		RemoveFields:           []types.TrafficMirrorSessionField{types.TrafficMirrorSessionFieldPacketLength},
	})
	require.NoError(t, err)
	assert.Zero(
		t, aws.ToInt32(modSessOut.TrafficMirrorSession.PacketLength), "RemoveFields=packet-length must reset it",
	)
}

// runVpcEndpointFields covers CreateVpcEndpoint's PolicyDocument/
// PrivateDnsEnabled/SecurityGroupIds/ServiceRegion and
// ModifyVpcEndpoint.ResetPolicy.
func runVpcEndpointFields(t *testing.T, client *ec2sdk.Client) {
	t.Helper()

	vpcOut, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.0.0.0/16")})
	require.NoError(t, err)
	vpcID := aws.ToString(vpcOut.Vpc.VpcId)

	sgOut, err := client.CreateSecurityGroup(t.Context(), &ec2sdk.CreateSecurityGroupInput{
		VpcId: aws.String(vpcID), GroupName: aws.String("vpce-sg"), Description: aws.String("vpce test sg"),
	})
	require.NoError(t, err)
	sgID := aws.ToString(sgOut.GroupId)

	policy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"*","Resource":"*"}]}`

	epOut, err := client.CreateVpcEndpoint(t.Context(), &ec2sdk.CreateVpcEndpointInput{
		VpcId: aws.String(vpcID), ServiceName: aws.String("com.amazonaws.us-east-1.execute-api"),
		VpcEndpointType:   types.VpcEndpointTypeInterface,
		PolicyDocument:    aws.String(policy),
		SecurityGroupIds:  []string{sgID},
		ServiceRegion:     aws.String("us-west-2"),
		PrivateDnsEnabled: aws.Bool(true),
	})
	require.NoError(t, err)
	require.NotNil(t, epOut.VpcEndpoint)
	endpointID := aws.ToString(epOut.VpcEndpoint.VpcEndpointId)
	assert.Equal(t, policy, aws.ToString(epOut.VpcEndpoint.PolicyDocument))
	assert.True(t, aws.ToBool(epOut.VpcEndpoint.PrivateDnsEnabled))
	assert.Equal(t, "us-west-2", aws.ToString(epOut.VpcEndpoint.ServiceRegion))
	require.Len(t, epOut.VpcEndpoint.Groups, 1)
	assert.Equal(t, sgID, aws.ToString(epOut.VpcEndpoint.Groups[0].GroupId))

	// Interface endpoints default PrivateDnsEnabled to true when omitted.
	epOut2, err := client.CreateVpcEndpoint(t.Context(), &ec2sdk.CreateVpcEndpointInput{
		VpcId: aws.String(vpcID), ServiceName: aws.String("com.amazonaws.us-east-1.sqs"),
		VpcEndpointType: types.VpcEndpointTypeInterface,
	})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(epOut2.VpcEndpoint.PrivateDnsEnabled))

	// ResetPolicy resets PolicyDocument to the default (empty).
	_, err = client.ModifyVpcEndpoint(t.Context(), &ec2sdk.ModifyVpcEndpointInput{
		VpcEndpointId: aws.String(endpointID), ResetPolicy: aws.Bool(true),
	})
	require.NoError(t, err)

	descOut, err := client.DescribeVpcEndpoints(t.Context(), &ec2sdk.DescribeVpcEndpointsInput{
		VpcEndpointIds: []string{endpointID},
	})
	require.NoError(t, err)
	require.Len(t, descOut.VpcEndpoints, 1)
	assert.Empty(t, aws.ToString(descOut.VpcEndpoints[0].PolicyDocument), "ResetPolicy must clear PolicyDocument")
}

// runInstanceConnectEndpointFields covers CreateInstanceConnectEndpoint's
// IpAddressType and ModifyInstanceConnectEndpoint's SecurityGroupIds.
func runInstanceConnectEndpointFields(t *testing.T, client *ec2sdk.Client) {
	t.Helper()

	vpcOut, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.0.0.0/16")})
	require.NoError(t, err)
	vpcID := aws.ToString(vpcOut.Vpc.VpcId)
	subnetOut, err := client.CreateSubnet(t.Context(), &ec2sdk.CreateSubnetInput{
		VpcId: aws.String(vpcID), CidrBlock: aws.String("10.0.0.0/24"),
	})
	require.NoError(t, err)
	subnetID := aws.ToString(subnetOut.Subnet.SubnetId)

	sg1Out, err := client.CreateSecurityGroup(t.Context(), &ec2sdk.CreateSecurityGroupInput{
		VpcId: aws.String(vpcID), GroupName: aws.String("eice-sg-1"), Description: aws.String("eice sg 1"),
	})
	require.NoError(t, err)
	sg2Out, err := client.CreateSecurityGroup(t.Context(), &ec2sdk.CreateSecurityGroupInput{
		VpcId: aws.String(vpcID), GroupName: aws.String("eice-sg-2"), Description: aws.String("eice sg 2"),
	})
	require.NoError(t, err)

	createOut, err := client.CreateInstanceConnectEndpoint(t.Context(), &ec2sdk.CreateInstanceConnectEndpointInput{
		SubnetId:         aws.String(subnetID),
		SecurityGroupIds: []string{aws.ToString(sg1Out.GroupId)},
		IpAddressType:    types.IpAddressTypeIpv4,
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.InstanceConnectEndpoint)
	endpointID := aws.ToString(createOut.InstanceConnectEndpoint.InstanceConnectEndpointId)
	assert.Equal(t, types.IpAddressTypeIpv4, createOut.InstanceConnectEndpoint.IpAddressType)

	_, err = client.ModifyInstanceConnectEndpoint(t.Context(), &ec2sdk.ModifyInstanceConnectEndpointInput{
		InstanceConnectEndpointId: aws.String(endpointID),
		SecurityGroupIds:          []string{aws.ToString(sg2Out.GroupId)},
	})
	require.NoError(t, err)

	descOut, err := client.DescribeInstanceConnectEndpoints(t.Context(), &ec2sdk.DescribeInstanceConnectEndpointsInput{
		InstanceConnectEndpointIds: []string{endpointID},
	})
	require.NoError(t, err)
	require.Len(t, descOut.InstanceConnectEndpoints, 1)
	require.Len(t, descOut.InstanceConnectEndpoints[0].SecurityGroupIds, 1)
	assert.Equal(t, aws.ToString(sg2Out.GroupId), descOut.InstanceConnectEndpoints[0].SecurityGroupIds[0])
}

// runSpotInstancesFields covers RequestSpotInstances' InstanceCount (one
// SpotInstanceRequest per instance), AvailabilityZoneGroup, LaunchGroup,
// InstanceInterruptionBehavior, and ValidUntil.
func runSpotInstancesFields(t *testing.T, client *ec2sdk.Client) {
	t.Helper()

	validUntil := aws.Time(time.Now().Add(time.Hour).Truncate(time.Second))

	out, err := client.RequestSpotInstances(t.Context(), &ec2sdk.RequestSpotInstancesInput{
		InstanceCount: aws.Int32(3),
		LaunchSpecification: &types.RequestSpotLaunchSpecification{
			ImageId: aws.String("ami-spot-test"), InstanceType: types.InstanceTypeT3Micro,
		},
		AvailabilityZoneGroup:        aws.String("test-az-group"),
		LaunchGroup:                  aws.String("test-launch-group"),
		InstanceInterruptionBehavior: types.InstanceInterruptionBehaviorStop,
		ValidUntil:                   validUntil,
	})
	require.NoError(t, err)
	require.Len(t, out.SpotInstanceRequests, 3, "InstanceCount=3 must create 3 separate spot requests")

	for _, req := range out.SpotInstanceRequests {
		assert.Equal(t, "test-az-group", aws.ToString(req.AvailabilityZoneGroup))
		assert.Equal(t, "test-launch-group", aws.ToString(req.LaunchGroup))
		assert.Equal(t, types.InstanceInterruptionBehaviorStop, req.InstanceInterruptionBehavior)
		require.NotNil(t, req.ValidUntil)
		assert.WithinDuration(t, *validUntil, *req.ValidUntil, 0)
	}

	seen := make(map[string]bool, len(out.SpotInstanceRequests))
	for _, req := range out.SpotInstanceRequests {
		seen[aws.ToString(req.SpotInstanceRequestId)] = true
	}
	assert.Len(t, seen, 3, "each spot request must have a distinct ID")
}
