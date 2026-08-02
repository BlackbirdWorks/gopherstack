package networkmanager_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	networkmanagersdk "github.com/aws/aws-sdk-go-v2/service/networkmanager"
	"github.com/aws/aws-sdk-go-v2/service/networkmanager/types"
	"github.com/stretchr/testify/require"
)

// TestRoundTrip_RouteAnalysis drives family S. This backend does not model
// real cross-service EC2 Transit Gateway route-table state (see
// routeanalysis.go's doc comment), so it never fabricates a CONNECTED
// verdict -- every analysis honestly resolves to NOT_CONNECTED with a real,
// deterministic reason code once the async RUNNING -> COMPLETED transition
// finishes.
func TestRoundTrip_RouteAnalysis(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	gn, err := client.CreateGlobalNetwork(ctx, &networkmanagersdk.CreateGlobalNetworkInput{})
	require.NoError(t, err)

	started, err := client.StartRouteAnalysis(ctx, &networkmanagersdk.StartRouteAnalysisInput{
		GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId,
		Source:          &types.RouteAnalysisEndpointOptionsSpecification{IpAddress: aws.String("10.0.0.1")},
		Destination: &types.RouteAnalysisEndpointOptionsSpecification{
			TransitGatewayAttachmentArn: aws.String(
				"arn:aws:ec2:us-east-1:000000000000:transit-gateway-attachment/tgw-attach-1",
			),
		},
		IncludeReturnPath: true,
	})
	require.NoError(t, err)
	require.Equal(t, types.RouteAnalysisStatusRunning, started.RouteAnalysis.Status)

	analysisID := started.RouteAnalysis.RouteAnalysisId

	require.Eventually(t, func() bool {
		g, getErr := client.GetRouteAnalysis(ctx, &networkmanagersdk.GetRouteAnalysisInput{
			GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId, RouteAnalysisId: analysisID,
		})

		return getErr == nil && g.RouteAnalysis.Status == types.RouteAnalysisStatusCompleted
	}, defaultAsyncWait, defaultAsyncPoll)

	final, err := client.GetRouteAnalysis(ctx, &networkmanagersdk.GetRouteAnalysisInput{
		GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId, RouteAnalysisId: analysisID,
	})
	require.NoError(t, err)
	require.Equal(
		t,
		types.RouteAnalysisCompletionResultCodeNotConnected,
		final.RouteAnalysis.ForwardPath.CompletionStatus.ResultCode,
	)
	require.Equal(
		t, types.RouteAnalysisCompletionReasonCodeTransitGatewayAttachmentNotFound,
		final.RouteAnalysis.ForwardPath.CompletionStatus.ReasonCode,
	)
	require.NotNil(t, final.RouteAnalysis.ReturnPath)

	// No destination ARN/IP at all gets the distinct NO_DESTINATION_ARN_PROVIDED reason.
	started2, err := client.StartRouteAnalysis(ctx, &networkmanagersdk.StartRouteAnalysisInput{
		GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId,
		Source:          &types.RouteAnalysisEndpointOptionsSpecification{IpAddress: aws.String("10.0.0.1")},
		Destination:     &types.RouteAnalysisEndpointOptionsSpecification{},
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		g, getErr := client.GetRouteAnalysis(ctx, &networkmanagersdk.GetRouteAnalysisInput{
			GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId, RouteAnalysisId: started2.RouteAnalysis.RouteAnalysisId,
		})

		wantReason := types.RouteAnalysisCompletionReasonCodeNoDestinationArnProvided

		return getErr == nil && g.RouteAnalysis.Status == types.RouteAnalysisStatusCompleted &&
			g.RouteAnalysis.ForwardPath.CompletionStatus.ReasonCode == wantReason
	}, defaultAsyncWait, defaultAsyncPoll)
}
