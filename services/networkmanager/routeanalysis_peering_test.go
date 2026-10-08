package networkmanager_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	networkmanagersdk "github.com/aws/aws-sdk-go-v2/service/networkmanager"
	"github.com/aws/aws-sdk-go-v2/service/networkmanager/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/networkmanager"
)

const peeringAttachArnFmt = "arn:aws:ec2:us-east-1:000000000000:transit-gateway-attachment/"

type peeringResolver struct {
	fakeEC2Resolver
	routeTables map[string]string
	routes      map[string][]networkmanager.EC2TransitGatewayRoute
	peers       map[string]string
}

func (p *peeringResolver) TransitGatewayRouteTableForAttachment(arn string) (string, bool) {
	id, ok := p.routeTables[arn]

	return id, ok
}

func (p *peeringResolver) TransitGatewayRoutes(rt string) []networkmanager.EC2TransitGatewayRoute {
	return p.routes[rt]
}

func (p *peeringResolver) TransitGatewayPeerAttachment(id string) (string, bool) {
	arn, ok := p.peers[id]

	return arn, ok
}

func TestRoundTrip_RouteAnalysisAcrossPeering(t *testing.T) {
	t.Parallel()

	a, b := peeringAttachArnFmt+"tgw-attach-a", peeringAttachArnFmt+"tgw-attach-b"
	active := func(attach string) []networkmanager.EC2TransitGatewayRoute {
		return []networkmanager.EC2TransitGatewayRoute{
			{DestinationCIDRBlock: "10.1.0.0/16", State: "active", AttachmentID: attach},
		}
	}

	tests := []struct {
		routes     map[string][]networkmanager.EC2TransitGatewayRoute
		peers      map[string]string
		name       string
		wantReason types.RouteAnalysisCompletionReasonCode
		wantResult types.RouteAnalysisCompletionResultCode
		wantHops   int
	}{
		{
			name: "peering_then_vpc",
			routes: map[string][]networkmanager.EC2TransitGatewayRoute{
				"rtb-a": active("peer-a"),
				"rtb-b": active("vpc-b"),
			},
			peers:      map[string]string{"peer-a": b},
			wantResult: types.RouteAnalysisCompletionResultCodeConnected,
			wantHops:   2,
		},
		{
			name: "peering_cycle",
			routes: map[string][]networkmanager.EC2TransitGatewayRoute{
				"rtb-a": active("peer-a"),
				"rtb-b": active("peer-b"),
			},
			peers:      map[string]string{"peer-a": b, "peer-b": a},
			wantResult: types.RouteAnalysisCompletionResultCodeNotConnected,
			wantReason: types.RouteAnalysisCompletionReasonCodeCyclicPathDetected,
			wantHops:   2,
		},
		{
			name:       "no_route_on_peer",
			routes:     map[string][]networkmanager.EC2TransitGatewayRoute{"rtb-a": active("peer-a")},
			peers:      map[string]string{"peer-a": b},
			wantResult: types.RouteAnalysisCompletionResultCodeNotConnected,
			wantReason: types.RouteAnalysisCompletionReasonCodeRouteNotFound,
			wantHops:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, client := newTestHandlerAndClient(t)
			ctx := t.Context()

			h.Backend.SetEC2Resolver(&peeringResolver{
				routeTables: map[string]string{a: "rtb-a", b: "rtb-b"},
				routes:      tt.routes,
				peers:       tt.peers,
			})

			gn, err := client.CreateGlobalNetwork(ctx, &networkmanagersdk.CreateGlobalNetworkInput{})
			require.NoError(t, err)

			started, err := client.StartRouteAnalysis(ctx, &networkmanagersdk.StartRouteAnalysisInput{
				GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId,
				Source: &types.RouteAnalysisEndpointOptionsSpecification{
					TransitGatewayAttachmentArn: aws.String(a),
				},
				Destination: &types.RouteAnalysisEndpointOptionsSpecification{IpAddress: aws.String("10.1.2.3")},
			})
			require.NoError(t, err)

			var got *types.RouteAnalysis

			require.Eventually(t, func() bool {
				g, getErr := client.GetRouteAnalysis(ctx, &networkmanagersdk.GetRouteAnalysisInput{
					GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId,
					RouteAnalysisId: started.RouteAnalysis.RouteAnalysisId,
				})
				if getErr != nil || g.RouteAnalysis.Status != types.RouteAnalysisStatusCompleted {
					return false
				}

				got = g.RouteAnalysis

				return true
			}, defaultAsyncWait, defaultAsyncPoll)

			assert.Equal(t, tt.wantResult, got.ForwardPath.CompletionStatus.ResultCode)
			assert.Equal(t, tt.wantReason, got.ForwardPath.CompletionStatus.ReasonCode)
			assert.Len(t, got.ForwardPath.Path, tt.wantHops)
		})
	}
}
