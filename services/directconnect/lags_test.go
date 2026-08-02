package directconnect_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	directconnectsdk "github.com/aws/aws-sdk-go-v2/service/directconnect"
	"github.com/aws/aws-sdk-go-v2/service/directconnect/types"
	"github.com/stretchr/testify/require"
)

// TestCreateLag_NumberOfConnectionsExceedsBandwidthCap verifies the real,
// checkable Lag.NumberOfConnections cap from its own doc comment: at most
// 4 connections at 1Gbps/10Gbps, or 2 at 100Gbps/400Gbps.
func TestCreateLag_NumberOfConnectionsExceedsBandwidthCap(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	_, err := client.CreateLag(ctx, &directconnectsdk.CreateLagInput{
		ConnectionsBandwidth: aws.String("100Gbps"),
		LagName:              aws.String("too-big-lag"),
		Location:             aws.String("EqDC2"),
		NumberOfConnections:  3, // cap for 100Gbps is 2
	})
	require.Error(t, err)

	var clientErr *types.DirectConnectClientException
	require.ErrorAs(t, err, &clientErr)
}

// TestAssociateConnectionWithLag_CapacityLimitExceeded verifies
// AssociateConnectionWithLag is the one op where LimitExceededException is
// reachable without any Tags input at all (PARITY.md wire-trap #8): a LAG
// already at its bandwidth-derived connection cap rejects one more.
func TestAssociateConnectionWithLag_CapacityLimitExceeded(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	lag, err := client.CreateLag(ctx, &directconnectsdk.CreateLagInput{
		ConnectionsBandwidth: aws.String("100Gbps"),
		LagName:              aws.String("full-lag"),
		Location:             aws.String("EqDC2"),
		NumberOfConnections:  2, // already at the 100Gbps cap
	})
	require.NoError(t, err)
	require.Len(t, lag.Connections, 2)

	extra, err := client.CreateConnection(ctx, &directconnectsdk.CreateConnectionInput{
		Bandwidth:      aws.String("100Gbps"),
		ConnectionName: aws.String("extra-conn"),
		Location:       aws.String("EqDC2"),
	})
	require.NoError(t, err)

	_, err = client.AssociateConnectionWithLag(ctx, &directconnectsdk.AssociateConnectionWithLagInput{
		ConnectionId: extra.ConnectionId,
		LagId:        lag.LagId,
	})
	require.Error(t, err)

	var limitErr *types.LimitExceededException
	require.ErrorAs(t, err, &limitErr)
}

// TestDisassociateConnectionFromLag_MismatchIsClientError verifies
// disassociating a connection from a LAG it does not belong to is
// rejected, not silently accepted.
func TestDisassociateConnectionFromLag_MismatchIsClientError(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	lag, err := client.CreateLag(ctx, &directconnectsdk.CreateLagInput{
		ConnectionsBandwidth: aws.String("1Gbps"),
		LagName:              aws.String("lag-a"),
		Location:             aws.String("EqDC2"),
		NumberOfConnections:  1,
	})
	require.NoError(t, err)

	standalone, err := client.CreateConnection(ctx, &directconnectsdk.CreateConnectionInput{
		Bandwidth:      aws.String("1Gbps"),
		ConnectionName: aws.String("standalone-conn"),
		Location:       aws.String("EqDC2"),
	})
	require.NoError(t, err)

	_, err = client.DisassociateConnectionFromLag(ctx, &directconnectsdk.DisassociateConnectionFromLagInput{
		ConnectionId: standalone.ConnectionId,
		LagId:        lag.LagId,
	})
	require.Error(t, err)
}
