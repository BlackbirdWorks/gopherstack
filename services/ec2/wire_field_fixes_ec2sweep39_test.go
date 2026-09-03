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

// TestDescribeIdFormat_ResourceFilter_RealClient covers handleDescribeIDFormat.
// Resource is a scalar field serialized as the bare key "Resource"
// (ec2@v1.319.1 serializers.go:77885, api_op_DescribeIdFormat.go:57,
// Resource *string), not an indexed list. The handler read it via
// parseMemberList, which looks for "Resource.1" -- a key a real client's
// single-resource-type lookup never sends -- so the filter was always
// silently ignored and every call returned every resource type's setting.
func TestDescribeIdFormat_ResourceFilter_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestEC2Client(t, ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1")))
	ctx := t.Context()

	_, err := client.ModifyIdFormat(ctx, &ec2sdk.ModifyIdFormatInput{
		Resource:   aws.String("instance"),
		UseLongIds: aws.Bool(true),
	})
	require.NoError(t, err)

	out, err := client.DescribeIdFormat(ctx, &ec2sdk.DescribeIdFormatInput{
		Resource: aws.String("volume"),
	})
	require.NoError(t, err)
	require.Len(t, out.Statuses, 1, "Resource filter ignored - returned every resource type")
	assert.Equal(t, "volume", aws.ToString(out.Statuses[0].Resource))
}

// TestDescribeIdentityIdFormat_ResourceFilter_RealClient covers
// handleDescribeIdentityIDFormat. Resource is a scalar field serialized as
// the bare key "Resource" (ec2@v1.319.1 serializers.go:77873,
// api_op_DescribeIdentityIdFormat.go:62, Resource *string), not an indexed
// list. Same bug as DescribeIdFormat: parseMemberList hunted for
// "Resource.1" and always came back empty.
func TestDescribeIdentityIdFormat_ResourceFilter_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestEC2Client(t, ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1")))
	ctx := t.Context()

	out, err := client.DescribeIdentityIdFormat(ctx, &ec2sdk.DescribeIdentityIdFormatInput{
		PrincipalArn: aws.String("arn:aws:iam::000000000000:role/sweep39"),
		Resource:     aws.String("snapshot"),
	})
	require.NoError(t, err)
	require.Len(t, out.Statuses, 1, "Resource filter ignored - returned every resource type")
	assert.Equal(t, "snapshot", aws.ToString(out.Statuses[0].Resource))
}

// TestModifyClientVpnEndpoint_DnsServers_RealClient covers
// handleModifyClientVpnEndpoint. Unlike CreateClientVpnEndpointInput (where
// DnsServers []string is a flat list), ModifyClientVpnEndpointInput.DnsServers
// is *types.DnsServersOptionsModifyStructure, a nested object whose
// CustomDnsServers field is the actual list (ec2@v1.319.1 serializers.go:87142-87146,
// api_op_ModifyClientVpnEndpoint.go, DnsServersOptionsModifyStructure.CustomDnsServers
// []string). The wire key is "DnsServers.CustomDnsServers.N", not "DnsServers.N" --
// the handler read the latter, so Modify never picked up new DNS servers.
func TestModifyClientVpnEndpoint_DnsServers_RealClient(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	ep, err := backend.CreateClientVpnEndpoint("10.10.0.0/22", "sweep39 vpn", nil)
	require.NoError(t, err)

	client := newTestEC2Client(t, ec2.NewHandler(backend))
	ctx := t.Context()

	_, err = client.ModifyClientVpnEndpoint(ctx, &ec2sdk.ModifyClientVpnEndpointInput{
		ClientVpnEndpointId: aws.String(ep.ClientVpnEndpointID),
		DnsServers: &types.DnsServersOptionsModifyStructure{
			CustomDnsServers: []string{"10.10.0.10", "10.10.0.11"},
			Enabled:          aws.Bool(true),
		},
	})
	require.NoError(t, err)

	out, err := client.DescribeClientVpnEndpoints(ctx, &ec2sdk.DescribeClientVpnEndpointsInput{
		ClientVpnEndpointIds: []string{ep.ClientVpnEndpointID},
	})
	require.NoError(t, err)
	require.Len(t, out.ClientVpnEndpoints, 1)
	assert.ElementsMatch(
		t, []string{"10.10.0.10", "10.10.0.11"}, out.ClientVpnEndpoints[0].DnsServers,
		"DnsServers empty - Modify read the wrong wire key (DnsServers.N instead of DnsServers.CustomDnsServers.N)",
	)
}

// TestModifyTransitGatewayMeteringPolicy_MiddleboxAttachmentIds_RealClient
// covers handleModifyTransitGatewayMeteringPolicy. AddMiddleboxAttachmentIds
// and RemoveMiddleboxAttachmentIds are Go fields on
// ModifyTransitGatewayMeteringPolicyInput, but each serializes under the
// SINGULAR wire key "AddMiddleboxAttachmentId"/"RemoveMiddleboxAttachmentId"
// (ec2@v1.319.1 serializers.go:89067-89084,
// awsEc2query_serializeOpDocumentModifyTransitGatewayMeteringPolicyInput).
// The handler read the plural Go field name as the wire key, which a real
// client never sends, so adds/removes were always silently dropped.
func TestModifyTransitGatewayMeteringPolicy_MiddleboxAttachmentIds_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestEC2Client(t, ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1")))
	ctx := t.Context()

	tgw, err := client.CreateTransitGateway(ctx, &ec2sdk.CreateTransitGatewayInput{})
	require.NoError(t, err)

	policy, err := client.CreateTransitGatewayMeteringPolicy(ctx, &ec2sdk.CreateTransitGatewayMeteringPolicyInput{
		TransitGatewayId: tgw.TransitGateway.TransitGatewayId,
	})
	require.NoError(t, err)

	out, err := client.ModifyTransitGatewayMeteringPolicy(ctx, &ec2sdk.ModifyTransitGatewayMeteringPolicyInput{
		TransitGatewayMeteringPolicyId: policy.TransitGatewayMeteringPolicy.TransitGatewayMeteringPolicyId,
		AddMiddleboxAttachmentIds:      []string{"tgw-attach-sweep39a", "tgw-attach-sweep39b"},
	})
	require.NoError(t, err)
	assert.ElementsMatch(
		t, []string{"tgw-attach-sweep39a", "tgw-attach-sweep39b"},
		out.TransitGatewayMeteringPolicy.MiddleboxAttachmentIds,
		"MiddleboxAttachmentIds empty - Add/Remove read the wrong wire key (plural Ids instead of singular Id)",
	)
}

// TestModifyVpcEndpointConnectionNotification_ConnectionEvents_RealClient
// covers handleModifyVpcEndpointConnectionNotification. ConnectionEvents
// serializes as the flat wire key "ConnectionEvents" (ec2@v1.319.1
// serializers.go:89688-89693, api_op_ModifyVpcEndpointConnectionNotification.go),
// not "ConnectionEvents.member". Unlike CreateVpcEndpointConnectionNotification's
// handler, which falls back to the correct key, Modify only ever tried the
// wrong one, so a real client's updated event list was always dropped.
func TestModifyVpcEndpointConnectionNotification_ConnectionEvents_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestEC2Client(t, ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1")))
	ctx := t.Context()

	created, err := client.CreateVpcEndpointConnectionNotification(
		ctx, &ec2sdk.CreateVpcEndpointConnectionNotificationInput{
			ConnectionNotificationArn: aws.String("arn:aws:sns:us-east-1:000000000000:sweep39-topic"),
			ConnectionEvents:          []string{"Accept"},
		},
	)
	require.NoError(t, err)

	_, err = client.ModifyVpcEndpointConnectionNotification(
		ctx, &ec2sdk.ModifyVpcEndpointConnectionNotificationInput{
			ConnectionNotificationId: created.ConnectionNotification.ConnectionNotificationId,
			ConnectionEvents:         []string{"Reject", "Delete"},
		},
	)
	require.NoError(t, err)

	out, err := client.DescribeVpcEndpointConnectionNotifications(
		ctx, &ec2sdk.DescribeVpcEndpointConnectionNotificationsInput{
			ConnectionNotificationId: created.ConnectionNotification.ConnectionNotificationId,
		},
	)
	require.NoError(t, err)
	require.Len(t, out.ConnectionNotificationSet, 1)
	assert.ElementsMatch(
		t, []string{"Reject", "Delete"}, out.ConnectionNotificationSet[0].ConnectionEvents,
		"ConnectionEvents unchanged - Modify read the wrong wire key "+
			"(ConnectionEvents.member instead of ConnectionEvents)",
	)
}
