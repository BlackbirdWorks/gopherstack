package directconnect_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	directconnectsdk "github.com/aws/aws-sdk-go-v2/service/directconnect"
	"github.com/aws/aws-sdk-go-v2/service/directconnect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/directconnect"
)

func TestEndpointDeviceIdentity(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	conn := createTestConnection(t, client)
	require.NotEmpty(t, aws.ToString(conn.AwsDeviceV2))
	assert.Equal(t, aws.ToString(conn.AwsDeviceV2), legacyConnectionDevice(conn))
	assert.Equal(t, conn.AwsDeviceV2, conn.AwsLogicalDeviceId)
	assert.Contains(t, aws.ToString(conn.AwsDeviceV2), "EqDC2-")

	lag, err := client.CreateLag(ctx, &directconnectsdk.CreateLagInput{
		ConnectionsBandwidth: aws.String("1Gbps"), LagName: aws.String("dev-lag"),
		Location: aws.String("EqDC2"), NumberOfConnections: 1,
	})
	require.NoError(t, err)
	assert.Equal(t, conn.AwsDeviceV2, lag.AwsDeviceV2)
	assert.Equal(t, aws.ToString(lag.AwsDeviceV2), legacyLagDevice(lag))

	ic, err := client.CreateInterconnect(ctx, &directconnectsdk.CreateInterconnectInput{
		Bandwidth: aws.String("1Gbps"), InterconnectName: aws.String("dev-ic"), Location: aws.String("EqDC2"),
	})
	require.NoError(t, err)
	assert.Equal(t, conn.AwsDeviceV2, ic.AwsDeviceV2)
	assert.Equal(t, aws.ToString(ic.AwsDeviceV2), legacyInterconnectDevice(ic))

	other, err := client.CreateConnection(ctx, &directconnectsdk.CreateConnectionInput{
		Bandwidth: aws.String("1Gbps"), ConnectionName: aws.String("other"), Location: aws.String("CSVA1"),
	})
	require.NoError(t, err)
	assert.NotEqual(t, conn.AwsDeviceV2, other.AwsDeviceV2)

	vif, err := client.CreatePrivateVirtualInterface(ctx, &directconnectsdk.CreatePrivateVirtualInterfaceInput{
		ConnectionId: conn.ConnectionId,
		NewPrivateVirtualInterface: &types.NewPrivateVirtualInterface{
			VirtualInterfaceName: aws.String("dev-vif"), Vlan: 200, Asn: 65001,
			AddressFamily: types.AddressFamilyIPv4,
		},
	})
	require.NoError(t, err)
	assert.Equal(t, conn.AwsDeviceV2, vif.AwsDeviceV2)

	peer, err := client.CreateBGPPeer(ctx, &directconnectsdk.CreateBGPPeerInput{
		VirtualInterfaceId: vif.VirtualInterfaceId,
		NewBGPPeer:         &types.NewBGPPeer{AddressFamily: types.AddressFamilyIPv4, Asn: 65002},
	})
	require.NoError(t, err)
	require.NotEmpty(t, peer.VirtualInterface.BgpPeers)
	assert.Equal(t, conn.AwsDeviceV2, peer.VirtualInterface.BgpPeers[0].AwsDeviceV2)
}

func TestAssociateConnectionWithLag_EndpointMismatch(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	lag, err := client.CreateLag(ctx, &directconnectsdk.CreateLagInput{
		ConnectionsBandwidth: aws.String("1Gbps"), LagName: aws.String("mismatch-lag"),
		Location: aws.String("EqDC2"), NumberOfConnections: 1,
	})
	require.NoError(t, err)

	tests := []struct {
		name     string
		location string
		wantErr  bool
	}{
		{name: "same_endpoint", location: "EqDC2"},
		{name: "other_endpoint", location: "CSVA1", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			conn, connErr := client.CreateConnection(ctx, &directconnectsdk.CreateConnectionInput{
				Bandwidth: aws.String("1Gbps"), ConnectionName: aws.String("c-" + tt.name),
				Location: aws.String(tt.location),
			})
			require.NoError(t, connErr)

			_, assocErr := client.AssociateConnectionWithLag(ctx, &directconnectsdk.AssociateConnectionWithLagInput{
				ConnectionId: conn.ConnectionId, LagId: lag.LagId,
			})
			if !tt.wantErr {
				require.NoError(t, assocErr)

				return
			}

			var clientErr *types.DirectConnectClientException
			require.ErrorAs(t, assocErr, &clientErr, "got %v", assocErr)
		})
	}
}

func TestGatewayAssociation_VirtualGatewayRegion(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	gw, err := client.CreateDirectConnectGateway(ctx, &directconnectsdk.CreateDirectConnectGatewayInput{
		DirectConnectGatewayName: aws.String("vgw-region-gw"),
	})
	require.NoError(t, err)

	assoc, err := client.CreateDirectConnectGatewayAssociation(
		ctx, &directconnectsdk.CreateDirectConnectGatewayAssociationInput{
			DirectConnectGatewayId: gw.DirectConnectGateway.DirectConnectGatewayId,
			GatewayId:              aws.String("vgw-0123456789abcdef0"),
		})
	require.NoError(t, err)
	require.NotNil(t, assoc.DirectConnectGatewayAssociation)
	assert.Equal(t, rtTestRegion, legacyGatewayRegion(assoc.DirectConnectGatewayAssociation))
}

type fakeSecretCreator struct {
	names   []string
	payload []string
}

func (f *fakeSecretCreator) CreateMacSecSecret(region, name, secretString string) (string, error) {
	f.names = append(f.names, name)
	f.payload = append(f.payload, secretString)

	return "arn:aws:secretsmanager:" + region + ":000000000000:secret:" + name + "-AbCdEf", nil
}

func TestAssociateMacSecKey_CreatesBackingSecret(t *testing.T) {
	t.Parallel()

	h, client := newTestHandlerAndClient(t)
	fake := &fakeSecretCreator{}
	h.Backend.SetMacSecSecretCreator(fake)

	conn := createTestConnection(t, client)

	out, err := client.AssociateMacSecKey(t.Context(), &directconnectsdk.AssociateMacSecKeyInput{
		ConnectionId: conn.ConnectionId,
		Cak:          aws.String("0123456789abcdef0123456789abcdef"),
		Ckn:          aws.String("fedcba9876543210fedcba9876543210"),
	})
	require.NoError(t, err)
	require.Len(t, out.MacSecKeys, 1)
	require.Len(t, fake.names, 1)
	assert.Contains(t, aws.ToString(out.MacSecKeys[0].SecretARN), fake.names[0])
	assert.JSONEq(t,
		`{"cak":"0123456789abcdef0123456789abcdef","ckn":"fedcba9876543210fedcba9876543210"}`,
		fake.payload[0])
}

var _ directconnect.MacSecSecretCreator = (*fakeSecretCreator)(nil)

// The deprecated members are the subject of these tests, so the SA1019 lint is silenced here only.

//nolint:staticcheck // SA1019: deprecated AwsDevice mirror
func legacyConnectionDevice(c *directconnectsdk.CreateConnectionOutput) string {
	return aws.ToString(c.AwsDevice)
}

//nolint:staticcheck // SA1019: deprecated AwsDevice mirror
func legacyLagDevice(l *directconnectsdk.CreateLagOutput) string { return aws.ToString(l.AwsDevice) }

//nolint:staticcheck // SA1019: deprecated AwsDevice mirror
func legacyInterconnectDevice(i *directconnectsdk.CreateInterconnectOutput) string {
	return aws.ToString(i.AwsDevice)
}

//nolint:staticcheck // SA1019: deprecated VirtualGatewayRegion
func legacyGatewayRegion(a *types.DirectConnectGatewayAssociation) string {
	return aws.ToString(a.VirtualGatewayRegion)
}
