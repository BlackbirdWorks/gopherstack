package directconnect_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	directconnectsdk "github.com/aws/aws-sdk-go-v2/service/directconnect"
	"github.com/aws/aws-sdk-go-v2/service/directconnect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTypedSlice23RealClient drives directconnect's remaining typed-
// coverage-blind ops (gopherstack-n3zi slice 23) through the real
// aws-sdk-go-v2 client: AllocatePublicVirtualInterface,
// AllocateTransitVirtualInterface, AssociateVirtualInterface,
// ConfirmConnection, ConfirmCustomerAgreement, ConfirmPublicVirtualInterface,
// ConfirmTransitVirtualInterface, DeleteDirectConnectGatewayAssociationProposal,
// DeleteInterconnect, DescribeDirectConnectGatewayAssociationProposals,
// DescribeDirectConnectGatewayAssociations, DescribeDirectConnectGateways,
// DescribeInterconnectLoa, DescribeInterconnects, UpdateDirectConnectGateway.
func TestTypedSlice23RealClient(t *testing.T) {
	t.Parallel()

	t.Run("hosted connection confirm and ConfirmCustomerAgreement", func(t *testing.T) {
		t.Parallel()

		_, client := newTestHandlerAndClient(t)
		ctx := t.Context()

		ic, err := client.CreateInterconnect(ctx, &directconnectsdk.CreateInterconnectInput{
			Bandwidth:        aws.String("1Gbps"),
			InterconnectName: aws.String("s23-ic"),
			Location:         aws.String("EqDC2"),
		})
		require.NoError(t, err)

		//nolint:staticcheck // SA1019: deprecated but real, matches hosted_connections_test.go's precedent.
		allocated, err := client.AllocateConnectionOnInterconnect(
			ctx,
			&directconnectsdk.AllocateConnectionOnInterconnectInput{
				Bandwidth:      aws.String("500Mbps"),
				ConnectionName: aws.String("s23-hosted-conn"),
				InterconnectId: ic.InterconnectId,
				OwnerAccount:   aws.String("999999999999"),
				Vlan:           101,
			},
		)
		require.NoError(t, err)
		require.Equal(t, types.ConnectionStateOrdering, allocated.ConnectionState)

		confirmOut, err := client.ConfirmConnection(ctx, &directconnectsdk.ConfirmConnectionInput{
			ConnectionId: allocated.ConnectionId,
		})
		require.NoError(t, err)
		assert.Equal(t, types.ConnectionStatePending, confirmOut.ConnectionState)

		require.Eventually(t, func() bool {
			out, descErr := client.DescribeConnections(ctx, &directconnectsdk.DescribeConnectionsInput{
				ConnectionId: allocated.ConnectionId,
			})

			return descErr == nil && len(out.Connections) == 1 &&
				out.Connections[0].ConnectionState == types.ConnectionStateAvailable
		}, defaultAsyncWait, defaultAsyncPoll)

		agreementOut, err := client.ConfirmCustomerAgreement(ctx, &directconnectsdk.ConfirmCustomerAgreementInput{
			AgreementName: aws.String("s23-agreement"),
		})
		require.NoError(t, err)
		assert.Equal(t, "signed", aws.ToString(agreementOut.Status))
	})

	t.Run("allocate and confirm public and transit VIFs", func(t *testing.T) {
		t.Parallel()

		_, client := newTestHandlerAndClient(t)
		ctx := t.Context()

		conn := createTestConnection(t, client)

		pubOut, err := client.AllocatePublicVirtualInterface(ctx, &directconnectsdk.AllocatePublicVirtualInterfaceInput{
			ConnectionId: conn.ConnectionId,
			OwnerAccount: aws.String("999999999999"),
			NewPublicVirtualInterfaceAllocation: &types.NewPublicVirtualInterfaceAllocation{
				VirtualInterfaceName: aws.String("s23-pub-vif"),
				Vlan:                 300,
			},
		})
		require.NoError(t, err)
		require.Equal(t, types.VirtualInterfaceStateConfirming, pubOut.VirtualInterfaceState)

		confirmPubOut, err := client.ConfirmPublicVirtualInterface(
			ctx,
			&directconnectsdk.ConfirmPublicVirtualInterfaceInput{
				VirtualInterfaceId: pubOut.VirtualInterfaceId,
			},
		)
		require.NoError(t, err)
		assert.Equal(t, types.VirtualInterfaceStatePending, confirmPubOut.VirtualInterfaceState)

		gw, err := client.CreateDirectConnectGateway(ctx, &directconnectsdk.CreateDirectConnectGatewayInput{
			DirectConnectGatewayName: aws.String("s23-vif-gw"),
		})
		require.NoError(t, err)

		transitOut, err := client.AllocateTransitVirtualInterface(
			ctx,
			&directconnectsdk.AllocateTransitVirtualInterfaceInput{
				ConnectionId: conn.ConnectionId,
				OwnerAccount: aws.String("999999999999"),
				NewTransitVirtualInterfaceAllocation: &types.NewTransitVirtualInterfaceAllocation{
					VirtualInterfaceName: aws.String("s23-transit-vif"),
					Vlan:                 400,
				},
			},
		)
		require.NoError(t, err)
		require.NotNil(t, transitOut.VirtualInterface)
		require.Equal(t, types.VirtualInterfaceStateConfirming, transitOut.VirtualInterface.VirtualInterfaceState)

		confirmTransitOut, err := client.ConfirmTransitVirtualInterface(
			ctx,
			&directconnectsdk.ConfirmTransitVirtualInterfaceInput{
				VirtualInterfaceId:     transitOut.VirtualInterface.VirtualInterfaceId,
				DirectConnectGatewayId: gw.DirectConnectGateway.DirectConnectGatewayId,
			},
		)
		require.NoError(t, err)
		assert.Equal(t, types.VirtualInterfaceStatePending, confirmTransitOut.VirtualInterfaceState)

		conn2 := createTestConnection(t, client)

		assocOut, err := client.AssociateVirtualInterface(ctx, &directconnectsdk.AssociateVirtualInterfaceInput{
			VirtualInterfaceId: pubOut.VirtualInterfaceId,
			ConnectionId:       conn2.ConnectionId,
		})
		require.NoError(t, err)
		assert.Equal(t, aws.ToString(conn2.ConnectionId), aws.ToString(assocOut.ConnectionId))
	})

	t.Run("direct connect gateway association proposal lifecycle", func(t *testing.T) {
		t.Parallel()

		_, client := newTestHandlerAndClient(t)
		ctx := t.Context()

		gw, err := client.CreateDirectConnectGateway(ctx, &directconnectsdk.CreateDirectConnectGatewayInput{
			DirectConnectGatewayName: aws.String("s23-proposal-gw"),
		})
		require.NoError(t, err)
		gwID := gw.DirectConnectGateway.DirectConnectGatewayId

		updateOut, err := client.UpdateDirectConnectGateway(ctx, &directconnectsdk.UpdateDirectConnectGatewayInput{
			DirectConnectGatewayId:      gwID,
			NewDirectConnectGatewayName: aws.String("s23-proposal-gw-renamed"),
		})
		require.NoError(t, err)
		assert.Equal(
			t,
			"s23-proposal-gw-renamed",
			aws.ToString(updateOut.DirectConnectGateway.DirectConnectGatewayName),
		)

		descGwOut, err := client.DescribeDirectConnectGateways(
			ctx,
			&directconnectsdk.DescribeDirectConnectGatewaysInput{
				DirectConnectGatewayId: gwID,
			},
		)
		require.NoError(t, err)
		require.Len(t, descGwOut.DirectConnectGateways, 1)
		assert.Equal(
			t,
			"s23-proposal-gw-renamed",
			aws.ToString(descGwOut.DirectConnectGateways[0].DirectConnectGatewayName),
		)

		proposalOut, err := client.CreateDirectConnectGatewayAssociationProposal(ctx,
			&directconnectsdk.CreateDirectConnectGatewayAssociationProposalInput{
				DirectConnectGatewayId:           gwID,
				DirectConnectGatewayOwnerAccount: aws.String(rtTestAccountID),
				GatewayId:                        aws.String("tgw-s23proposal1"),
			},
		)
		require.NoError(t, err)
		require.NotNil(t, proposalOut.DirectConnectGatewayAssociationProposal)
		proposalID := proposalOut.DirectConnectGatewayAssociationProposal.ProposalId

		descProposalsOut, err := client.DescribeDirectConnectGatewayAssociationProposals(ctx,
			&directconnectsdk.DescribeDirectConnectGatewayAssociationProposalsInput{ProposalId: proposalID},
		)
		require.NoError(t, err)
		require.Len(t, descProposalsOut.DirectConnectGatewayAssociationProposals, 1)
		assert.Equal(
			t, gwID, descProposalsOut.DirectConnectGatewayAssociationProposals[0].DirectConnectGatewayId,
		)

		acceptOut, err := client.AcceptDirectConnectGatewayAssociationProposal(ctx,
			&directconnectsdk.AcceptDirectConnectGatewayAssociationProposalInput{
				AssociatedGatewayOwnerAccount: aws.String(rtTestAccountID),
				DirectConnectGatewayId:        gwID,
				ProposalId:                    proposalID,
			},
		)
		require.NoError(t, err)
		require.NotNil(t, acceptOut.DirectConnectGatewayAssociation)

		descAssocOut, err := client.DescribeDirectConnectGatewayAssociations(ctx,
			&directconnectsdk.DescribeDirectConnectGatewayAssociationsInput{DirectConnectGatewayId: gwID},
		)
		require.NoError(t, err)
		require.Len(t, descAssocOut.DirectConnectGatewayAssociations, 1)
		assert.Equal(
			t,
			aws.ToString(gwID),
			aws.ToString(descAssocOut.DirectConnectGatewayAssociations[0].DirectConnectGatewayId),
		)

		proposal2Out, err := client.CreateDirectConnectGatewayAssociationProposal(ctx,
			&directconnectsdk.CreateDirectConnectGatewayAssociationProposalInput{
				DirectConnectGatewayId:           gwID,
				DirectConnectGatewayOwnerAccount: aws.String(rtTestAccountID),
				GatewayId:                        aws.String("tgw-s23proposal2"),
			},
		)
		require.NoError(t, err)
		proposal2ID := proposal2Out.DirectConnectGatewayAssociationProposal.ProposalId

		_, err = client.DeleteDirectConnectGatewayAssociationProposal(ctx,
			&directconnectsdk.DeleteDirectConnectGatewayAssociationProposalInput{ProposalId: proposal2ID},
		)
		require.NoError(t, err)

		descAfterDelete, err := client.DescribeDirectConnectGatewayAssociationProposals(ctx,
			&directconnectsdk.DescribeDirectConnectGatewayAssociationProposalsInput{ProposalId: proposal2ID},
		)
		require.NoError(t, err)
		require.Len(t, descAfterDelete.DirectConnectGatewayAssociationProposals, 1)
		assert.Equal(
			t, types.DirectConnectGatewayAssociationProposalStateDeleted,
			descAfterDelete.DirectConnectGatewayAssociationProposals[0].ProposalState,
		)
	})

	t.Run("interconnect describe, LOA, delete", func(t *testing.T) {
		t.Parallel()

		_, client := newTestHandlerAndClient(t)
		ctx := t.Context()

		ic, err := client.CreateInterconnect(ctx, &directconnectsdk.CreateInterconnectInput{
			Bandwidth:        aws.String("1Gbps"),
			InterconnectName: aws.String("s23-ic-lifecycle"),
			Location:         aws.String("EqDC2"),
		})
		require.NoError(t, err)

		descOut, err := client.DescribeInterconnects(ctx, &directconnectsdk.DescribeInterconnectsInput{
			InterconnectId: ic.InterconnectId,
		})
		require.NoError(t, err)
		require.Len(t, descOut.Interconnects, 1)
		assert.Equal(t, aws.ToString(ic.InterconnectId), aws.ToString(descOut.Interconnects[0].InterconnectId))

		//nolint:staticcheck // SA1019: DescribeInterconnectLoa is deprecated but still a real, listed op.
		loaOut, err := client.DescribeInterconnectLoa(ctx, &directconnectsdk.DescribeInterconnectLoaInput{
			InterconnectId: ic.InterconnectId,
		})
		require.NoError(t, err)
		require.NotNil(t, loaOut.Loa)
		require.NotEmpty(t, loaOut.Loa.LoaContent)
		assert.Equal(t, types.LoaContentTypePdf, loaOut.Loa.LoaContentType)

		delOut, err := client.DeleteInterconnect(ctx, &directconnectsdk.DeleteInterconnectInput{
			InterconnectId: ic.InterconnectId,
		})
		require.NoError(t, err)
		assert.Equal(t, types.InterconnectStateDeleting, delOut.InterconnectState)
	})
}
