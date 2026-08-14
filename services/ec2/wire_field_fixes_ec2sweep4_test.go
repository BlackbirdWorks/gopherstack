package ec2_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestMutatingOps_DeleteReturnsObject_RealClient drives Delete ops that were
// found (gopherstack-7185, ec2sweep4) returning an empty envelope where the
// real ec2@v1.319.1 deserializer emits the deleted resource. Each case
// creates the resource, deletes it through the real client, and asserts the
// returned object -- not just a bare boolean -- carries the resource's ID.
func TestMutatingOps_DeleteReturnsObject_RealClient(t *testing.T) {
	t.Parallel()

	t.Run("carrier gateway", func(t *testing.T) {
		t.Parallel()

		h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
		client := newTestEC2Client(t, h)

		vpc, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.60.0.0/16")})
		require.NoError(t, err)

		gw, err := client.CreateCarrierGateway(t.Context(), &ec2sdk.CreateCarrierGatewayInput{
			VpcId: vpc.Vpc.VpcId,
		})
		require.NoError(t, err)
		gwID := gw.CarrierGateway.CarrierGatewayId

		out, err := client.DeleteCarrierGateway(t.Context(), &ec2sdk.DeleteCarrierGatewayInput{
			CarrierGatewayId: gwID,
		})
		require.NoError(t, err)
		require.NotNil(t, out.CarrierGateway, "CarrierGateway nil - DeleteCarrierGateway returned an empty envelope")
		assert.Equal(t, aws.ToString(gwID), aws.ToString(out.CarrierGateway.CarrierGatewayId))
	})

	t.Run("verified access endpoint", func(t *testing.T) {
		t.Parallel()

		h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
		client := newTestEC2Client(t, h)

		inst, err := client.CreateVerifiedAccessInstance(t.Context(), &ec2sdk.CreateVerifiedAccessInstanceInput{})
		require.NoError(t, err)

		grp, err := client.CreateVerifiedAccessGroup(t.Context(), &ec2sdk.CreateVerifiedAccessGroupInput{
			VerifiedAccessInstanceId: inst.VerifiedAccessInstance.VerifiedAccessInstanceId,
		})
		require.NoError(t, err)

		ep, err := client.CreateVerifiedAccessEndpoint(t.Context(), &ec2sdk.CreateVerifiedAccessEndpointInput{
			VerifiedAccessGroupId: grp.VerifiedAccessGroup.VerifiedAccessGroupId,
			EndpointType:          types.VerifiedAccessEndpointTypeNetworkInterface,
			AttachmentType:        types.VerifiedAccessEndpointAttachmentTypeVpc,
		})
		require.NoError(t, err)
		epID := ep.VerifiedAccessEndpoint.VerifiedAccessEndpointId

		out, err := client.DeleteVerifiedAccessEndpoint(t.Context(), &ec2sdk.DeleteVerifiedAccessEndpointInput{
			VerifiedAccessEndpointId: epID,
		})
		require.NoError(t, err)
		require.NotNil(t, out.VerifiedAccessEndpoint,
			"VerifiedAccessEndpoint nil - DeleteVerifiedAccessEndpoint returned an empty envelope")
		assert.Equal(t, aws.ToString(epID), aws.ToString(out.VerifiedAccessEndpoint.VerifiedAccessEndpointId))
	})

	t.Run("transit gateway connect peer", func(t *testing.T) {
		t.Parallel()

		h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
		client := newTestEC2Client(t, h)

		vpc, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.61.0.0/16")})
		require.NoError(t, err)
		subnet, err := client.CreateSubnet(t.Context(), &ec2sdk.CreateSubnetInput{
			VpcId: vpc.Vpc.VpcId, CidrBlock: aws.String("10.61.1.0/24"),
		})
		require.NoError(t, err)

		tgw, err := client.CreateTransitGateway(t.Context(), &ec2sdk.CreateTransitGatewayInput{})
		require.NoError(t, err)

		attInput := &ec2sdk.CreateTransitGatewayVpcAttachmentInput{
			TransitGatewayId: tgw.TransitGateway.TransitGatewayId,
			VpcId:            vpc.Vpc.VpcId,
			SubnetIds:        []string{aws.ToString(subnet.Subnet.SubnetId)},
		}
		att, err := client.CreateTransitGatewayVpcAttachment(t.Context(), attInput)
		require.NoError(t, err)

		connOptions := &types.CreateTransitGatewayConnectRequestOptions{Protocol: types.ProtocolValueGre}
		conn, err := client.CreateTransitGatewayConnect(t.Context(), &ec2sdk.CreateTransitGatewayConnectInput{
			TransportTransitGatewayAttachmentId: att.TransitGatewayVpcAttachment.TransitGatewayAttachmentId,
			Options:                             connOptions,
		})
		require.NoError(t, err)

		peer, err := client.CreateTransitGatewayConnectPeer(t.Context(), &ec2sdk.CreateTransitGatewayConnectPeerInput{
			TransitGatewayAttachmentId: conn.TransitGatewayConnect.TransitGatewayAttachmentId,
			PeerAddress:                aws.String("10.0.0.1"),
			InsideCidrBlocks:           []string{"169.254.6.0/29"},
		})
		require.NoError(t, err)
		peerID := peer.TransitGatewayConnectPeer.TransitGatewayConnectPeerId

		out, err := client.DeleteTransitGatewayConnectPeer(t.Context(), &ec2sdk.DeleteTransitGatewayConnectPeerInput{
			TransitGatewayConnectPeerId: peerID,
		})
		require.NoError(t, err)
		require.NotNil(t, out.TransitGatewayConnectPeer,
			"TransitGatewayConnectPeer nil - DeleteTransitGatewayConnectPeer returned an empty envelope")
		assert.Equal(t, aws.ToString(peerID), aws.ToString(out.TransitGatewayConnectPeer.TransitGatewayConnectPeerId))
	})
}

// TestModifyTrafficMirrorFilterRule_ReturnsUpdatedRule_RealClient covers the
// Modify-returns-object class: the real ModifyTrafficMirrorFilterRuleOutput
// carries the updated rule under trafficMirrorFilterRule, where gopherstack
// returned only {requestId, return}.
func TestModifyTrafficMirrorFilterRule_ReturnsUpdatedRule_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	filter, err := client.CreateTrafficMirrorFilter(t.Context(), &ec2sdk.CreateTrafficMirrorFilterInput{})
	require.NoError(t, err)

	rule, err := client.CreateTrafficMirrorFilterRule(t.Context(), &ec2sdk.CreateTrafficMirrorFilterRuleInput{
		TrafficMirrorFilterId: filter.TrafficMirrorFilter.TrafficMirrorFilterId,
		TrafficDirection:      types.TrafficDirectionIngress,
		RuleAction:            types.TrafficMirrorRuleActionAccept,
		DestinationCidrBlock:  aws.String("0.0.0.0/0"),
		SourceCidrBlock:       aws.String("10.0.0.0/8"),
		RuleNumber:            aws.Int32(100),
	})
	require.NoError(t, err)

	out, err := client.ModifyTrafficMirrorFilterRule(t.Context(), &ec2sdk.ModifyTrafficMirrorFilterRuleInput{
		TrafficMirrorFilterRuleId: rule.TrafficMirrorFilterRule.TrafficMirrorFilterRuleId,
		RuleAction:                types.TrafficMirrorRuleActionReject,
	})
	require.NoError(t, err)
	require.NotNil(t, out.TrafficMirrorFilterRule,
		"TrafficMirrorFilterRule nil - ModifyTrafficMirrorFilterRule returned an empty envelope")
	assert.Equal(t, types.TrafficMirrorRuleActionReject, out.TrafficMirrorFilterRule.RuleAction)
}

// TestCreateClientVpnEndpoint_FlatFields_RealClient covers the input/output
// split: CreateClientVpnEndpoint wrapped its whole result in an invented
// <clientVpnEndpoint> element - a shape that belongs to Describe, not Create.
// The real CreateClientVpnEndpointOutput has flat ClientVpnEndpointId, DnsName
// and Status members.
func TestCreateClientVpnEndpoint_FlatFields_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	authOptions := []types.ClientVpnAuthenticationRequest{
		{Type: types.ClientVpnAuthenticationTypeCertificateAuthentication},
	}

	out, err := client.CreateClientVpnEndpoint(t.Context(), &ec2sdk.CreateClientVpnEndpointInput{
		ClientCidrBlock:       aws.String("10.100.0.0/16"),
		ServerCertificateArn:  aws.String("arn:aws:acm:us-east-1:000000000000:certificate/test"),
		ConnectionLogOptions:  &types.ConnectionLogOptions{Enabled: aws.Bool(false)},
		AuthenticationOptions: authOptions,
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(out.ClientVpnEndpointId),
		"ClientVpnEndpointId empty - CreateClientVpnEndpoint nested it under an invented clientVpnEndpoint wrapper")
	assert.Contains(t, aws.ToString(out.ClientVpnEndpointId), "cvpn-endpoint-")
}

// TestModifyVpcEndpointServicePermissions_AddedPrincipals_RealClient covers
// the missing addedPrincipalSet: the real output reports which principals
// were newly granted access, not just a bare boolean.
func TestModifyVpcEndpointServicePermissions_AddedPrincipals_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	nlbArn := "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/net/test/abc"
	svcInput := &ec2sdk.CreateVpcEndpointServiceConfigurationInput{
		NetworkLoadBalancerArns: []string{nlbArn},
	}
	svc, err := client.CreateVpcEndpointServiceConfiguration(t.Context(), svcInput)
	require.NoError(t, err)

	const principal = "arn:aws:iam::111111111111:root"

	permInput := &ec2sdk.ModifyVpcEndpointServicePermissionsInput{
		ServiceId:            svc.ServiceConfiguration.ServiceId,
		AddAllowedPrincipals: []string{principal},
	}
	out, err := client.ModifyVpcEndpointServicePermissions(t.Context(), permInput)
	require.NoError(t, err)
	require.Len(t, out.AddedPrincipals, 1,
		"AddedPrincipals empty - ModifyVpcEndpointServicePermissions dropped the addedPrincipalSet")
	assert.Equal(t, principal, aws.ToString(out.AddedPrincipals[0].Principal))
}

// TestDeleteFlowLogs_UnsuccessfulKey_RealClient covers a fabricated field: the
// real DeleteFlowLogsOutput has only "unsuccessful" - gopherstack emitted a
// "return" boolean that does not exist on the wire at all.
func TestDeleteFlowLogs_UnsuccessfulKey_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	vpc, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.62.0.0/16")})
	require.NoError(t, err)

	createFlowLog := func() string {
		created, cerr := client.CreateFlowLogs(t.Context(), &ec2sdk.CreateFlowLogsInput{
			ResourceIds:        []string{aws.ToString(vpc.Vpc.VpcId)},
			ResourceType:       types.FlowLogsResourceTypeVpc,
			TrafficType:        types.TrafficTypeAll,
			LogDestinationType: types.LogDestinationTypeS3,
			LogDestination:     aws.String("arn:aws:s3:::wire-field-fixes-flow-logs-2"),
		})
		require.NoError(t, cerr)
		require.Len(t, created.FlowLogIds, 1)

		return created.FlowLogIds[0]
	}

	flowLogID1 := createFlowLog()

	out, err := client.DeleteFlowLogs(t.Context(), &ec2sdk.DeleteFlowLogsInput{
		FlowLogIds: []string{flowLogID1},
	})
	require.NoError(t, err)
	assert.Empty(t, out.Unsuccessful, "Unsuccessful should be empty (not nil-vs-populated) on a clean delete")

	flowLogID2 := createFlowLog()

	raw, err := ec2.ExportDispatch(h, map[string][]string{
		"Action":      {"DeleteFlowLogs"},
		"Version":     {"2016-11-15"},
		"FlowLogId.1": {flowLogID2},
	})
	require.NoError(t, err)
	assert.Contains(t, raw, "<unsuccessful", "real DeleteFlowLogsOutput has no return field, only unsuccessful")
	assert.NotContains(t, raw, "<return>", "return is not a real DeleteFlowLogsOutput member")
}

// TestCreateFleet_ErrorSetKey_RealClient covers a wrong-key bug: gopherstack
// emitted <errors> and an invented <type>, where the real CreateFleetOutput
// deserializer (ec2@v1.319.1 deserializers.go) only matches errorSet,
// fleetId and fleetInstanceSet.
func TestCreateFleet_ErrorSetKey_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))

	raw, err := ec2.ExportDispatch(h, map[string][]string{
		"Action":  {"CreateFleet"},
		"Version": {"2016-11-15"},
		"TargetCapacitySpecification.TotalTargetCapacity": {"1"},
	})
	require.NoError(t, err)
	assert.True(t, strings.Contains(raw, "<errorSet>") || strings.Contains(raw, "<errorSet/>"),
		"real CreateFleetOutput carries errorSet, not errors")
	assert.NotContains(t, raw, "<errors>", "errors is not a real CreateFleetOutput member")
	assert.NotContains(t, raw, "<type>", "type is not a real CreateFleetOutput member")
}
