package ec2_test

import (
	"testing"

	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestDeleteKeyPair_SurfacesKeyPairID_RealClient covers handler_key_pairs.go's
// handleDeleteKeyPair, which pre-fix returned a bare stubResponse{Return: true}.
// The real DeleteKeyPairOutput also has a keyPairId field
// (deserializers.go, awsEc2query_deserializeOpDocumentDeleteKeyPairOutput), so
// a client confirming which key pair it just deleted by ID got an empty
// string pre-fix.
func TestDeleteKeyPair_SurfacesKeyPairID_RealClient(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	client := newTestEC2Client(t, h)

	kp, err := b.CreateKeyPair("sweep13-key", nil)
	require.NoError(t, err)

	out, err := client.DeleteKeyPair(t.Context(), &ec2sdk.DeleteKeyPairInput{
		KeyName: &kp.Name,
	})
	require.NoError(t, err)
	require.NotNil(t, out.KeyPairId, "pre-fix this field was never rendered, only a bare Return bool")
	assert.Equal(t, kp.KeyPairID, *out.KeyPairId)
}

// TestDisassociateClientVpnTargetNetwork_SurfacesAssociation_RealClient covers
// handler_client_vpn.go's handleDisassociateClientVpnTargetNetwork, which
// pre-fix returned a bare stubResponse{Return: true}. The real
// DisassociateClientVpnTargetNetworkOutput has no Return field at all -- only
// associationId and status (deserializers.go,
// awsEc2query_deserializeOpDocumentDisassociateClientVpnTargetNetworkOutput)
// -- so a client polling for the association to leave "associated" state got
// zero values pre-fix.
func TestDisassociateClientVpnTargetNetwork_SurfacesAssociation_RealClient(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	client := newTestEC2Client(t, h)

	ep, err := b.CreateClientVpnEndpoint("10.0.0.0/22", "sweep13 vpn", nil)
	require.NoError(t, err)
	assocID, err := b.AssociateClientVpnTargetNetwork(ep.ClientVpnEndpointID, "subnet-default")
	require.NoError(t, err)

	out, err := client.DisassociateClientVpnTargetNetwork(t.Context(), &ec2sdk.DisassociateClientVpnTargetNetworkInput{
		ClientVpnEndpointId: &ep.ClientVpnEndpointID,
		AssociationId:       &assocID,
	})
	require.NoError(t, err)
	require.NotNil(t, out.AssociationId, "pre-fix this field was never rendered, only a bare Return bool")
	assert.Equal(t, assocID, *out.AssociationId)
	require.NotNil(t, out.Status)
	assert.Equal(t, "disassociating", string(out.Status.Code))
}

// TestReleaseIpamPoolAllocation_WireField_RealClient covers handler_ipam.go's
// handleReleaseIpamPoolAllocation, which pre-fix rendered <return>true</return>.
// The real ReleaseIpamPoolAllocationOutput's only member decodes off
// <success>, not <return> (deserializers.go,
// awsEc2query_deserializeOpDocumentReleaseIpamPoolAllocationOutput) -- the
// real SDK's deserializer has no case for "return" at all, so it silently
// skipped the emulator's tag and Success was always nil pre-fix, even though
// the release genuinely succeeded.
func TestReleaseIpamPoolAllocation_WireField_RealClient(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	client := newTestEC2Client(t, h)

	ipam, err := b.CreateIpam()
	require.NoError(t, err)
	pool, err := b.CreateIpamPool(ipam.IpamID, "ipv4", "", "10.0.0.0/16")
	require.NoError(t, err)
	alloc, err := b.AllocateIpamPoolCidr(pool.IpamPoolID, "", 24)
	require.NoError(t, err)

	out, err := client.ReleaseIpamPoolAllocation(t.Context(), &ec2sdk.ReleaseIpamPoolAllocationInput{
		IpamPoolId:           &pool.IpamPoolID,
		IpamPoolAllocationId: &alloc.IpamPoolAllocationID,
		Cidr:                 &alloc.Cidr,
	})
	require.NoError(t, err)
	require.NotNil(t, out.Success, "pre-fix the real deserializer has no case for <return>, so this stayed nil")
	assert.True(t, *out.Success)
}
