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

// TestApplySecurityGroupsToClientVpnTargetNetwork_WireShape_RealClient covers
// handleApplySecurityGroupsToClientVpnTargetNetwork, which pre-fix rendered a
// bare <return>true</return> via stubResponse. The real
// ApplySecurityGroupsToClientVpnTargetNetworkOutput has no Return member at
// all -- only SecurityGroupIds (ec2@v1.319.1 deserializers.go,
// awsEc2query_deserializeOpDocumentApplySecurityGroupsToClientVpnTargetNetworkOutput
// has no case for "return") -- so a client confirming which security groups
// were just applied saw an empty slice pre-fix.
func TestApplySecurityGroupsToClientVpnTargetNetwork_WireShape_RealClient(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	client := newTestEC2Client(t, h)

	ep, err := b.CreateClientVpnEndpoint("10.10.0.0/22", "sweep20 vpn", nil)
	require.NoError(t, err)

	out, err := client.ApplySecurityGroupsToClientVpnTargetNetwork(
		t.Context(),
		&ec2sdk.ApplySecurityGroupsToClientVpnTargetNetworkInput{
			ClientVpnEndpointId: aws.String(ep.ClientVpnEndpointID),
			VpcId:               aws.String("vpc-default"),
			SecurityGroupIds:    []string{"sg-sweep20a", "sg-sweep20b"},
		},
	)
	require.NoError(t, err)
	assert.ElementsMatch(
		t, []string{"sg-sweep20a", "sg-sweep20b"}, out.SecurityGroupIds,
		"SecurityGroupIds empty - pre-fix this field was never rendered, only a bare Return bool",
	)
}

// TestCancelReservedInstancesListing_SurfacesListing_RealClient covers
// handleCancelReservedInstancesListing, which pre-fix returned only Return.
// The real CancelReservedInstancesListingOutput deserializer matches
// "reservedInstancesListingsSet" (ec2@v1.319.1 deserializers.go,
// awsEc2query_deserializeOpDocumentCancelReservedInstancesListingOutput) and
// has no case for "return" at all -- so a client confirming the listing's
// cancelled status saw an empty slice pre-fix, even though the cancellation
// genuinely happened (DescribeReservedInstancesListings already reflected
// it).
func TestCancelReservedInstancesListing_SurfacesListing_RealClient(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	client := newTestEC2Client(t, h)

	b.SeedReservedInstancesOffering(
		"rio-sweep20", "t3.medium", "us-east-1a", "Linux/UNIX", "All Upfront", 94608000, 500.0, 0.0,
	)
	ri, err := b.PurchaseReservedInstancesOffering("rio-sweep20", 1)
	require.NoError(t, err)
	listing, err := b.CreateReservedInstancesListing(ri.ReservedInstancesID, 1)
	require.NoError(t, err)

	out, err := client.CancelReservedInstancesListing(t.Context(), &ec2sdk.CancelReservedInstancesListingInput{
		ReservedInstancesListingId: aws.String(listing.ReservedInstancesListingID),
	})
	require.NoError(t, err)
	require.Len(
		t, out.ReservedInstancesListings, 1,
		"pre-fix this field was never rendered, only a bare Return bool",
	)
	assert.Equal(t, "cancelled", string(out.ReservedInstancesListings[0].Status))
}

// TestDisassociateVpcCidrBlock_WireShape_RealClient covers
// handleDisassociateVpcCidrBlock, which pre-fix rendered a bare
// <return>true</return>. The real DisassociateVpcCidrBlockOutput has no
// Return member -- only a nested CidrBlockAssociation and VpcId
// (ec2@v1.319.1 deserializers.go,
// awsEc2query_deserializeOpDocumentDisassociateVpcCidrBlockOutput has no
// case for "return") -- so a client confirming which CIDR block was just
// disassociated saw a nil CidrBlockAssociation pre-fix.
func TestDisassociateVpcCidrBlock_WireShape_RealClient(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	client := newTestEC2Client(t, h)

	vpc, err := b.CreateVpc("10.61.0.0/16")
	require.NoError(t, err)

	// Set up the association directly on the backend rather than through
	// client.AssociateVpcCidrBlock: that op's response wraps the association
	// under <ipv4CidrBlockAssociation> (handler_ec2core.go's
	// associateVpcCidrBlockResponse), but the real deserializer only matches
	// <cidrBlockAssociation> -- a pre-existing bug in a different op, not in
	// this task's queue.
	assoc, err := b.AssociateVpcCidrBlock(vpc.ID, "10.62.0.0/16")
	require.NoError(t, err)

	out, err := client.DisassociateVpcCidrBlock(t.Context(), &ec2sdk.DisassociateVpcCidrBlockInput{
		AssociationId: aws.String(assoc.AssociationID),
	})
	require.NoError(t, err)
	require.NotNil(t, out.CidrBlockAssociation, "pre-fix this field was never rendered, only a bare Return bool")
	assert.Equal(t, assoc.AssociationID, aws.ToString(out.CidrBlockAssociation.AssociationId))
	require.NotNil(t, out.CidrBlockAssociation.CidrBlockState)
	assert.Equal(t, types.VpcCidrBlockStateCodeDisassociated, out.CidrBlockAssociation.CidrBlockState.State)
	assert.Equal(t, vpc.ID, aws.ToString(out.VpcId))
}
