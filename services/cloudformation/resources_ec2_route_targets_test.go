package cloudformation_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
)

func TestResourceCreator_EC2RouteTargets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		props func(rtID string) map[string]any
		check func(t *testing.T, r *ec2backend.Route)
		name  string
	}{
		{
			name: "transit gateway",
			props: func(rtID string) map[string]any {
				return map[string]any{
					"RouteTableId":         rtID,
					"DestinationCidrBlock": "10.9.0.0/16",
					"TransitGatewayId":     "tgw-0123456789abcdef0",
				}
			},
			check: func(t *testing.T, r *ec2backend.Route) {
				t.Helper()
				assert.Equal(t, "tgw-0123456789abcdef0", r.TransitGatewayID)
			},
		},
		{
			name: "peering connection",
			props: func(rtID string) map[string]any {
				return map[string]any{
					"RouteTableId":           rtID,
					"DestinationCidrBlock":   "10.8.0.0/16",
					"VpcPeeringConnectionId": "pcx-0123456789abcdef0",
				}
			},
			check: func(t *testing.T, r *ec2backend.Route) {
				t.Helper()
				assert.Equal(t, "pcx-0123456789abcdef0", r.VpcPeeringConnectionID)
			},
		},
		{
			name: "ipv6 egress only",
			props: func(rtID string) map[string]any {
				return map[string]any{
					"RouteTableId":                rtID,
					"DestinationIpv6CidrBlock":    "::/0",
					"EgressOnlyInternetGatewayId": "eigw-0123456789abcdef0",
				}
			},
			check: func(t *testing.T, r *ec2backend.Route) {
				t.Helper()
				assert.Equal(t, "::/0", r.DestinationIPv6CIDR)
				assert.Equal(t, "eigw-0123456789abcdef0", r.EgressOnlyInternetGatewayID)
			},
		},
		{
			name: "prefix list",
			props: func(rtID string) map[string]any {
				return map[string]any{
					"RouteTableId":            rtID,
					"DestinationPrefixListId": "pl-0123456789abcdef0",
					"TransitGatewayId":        "tgw-0123456789abcdef0",
				}
			},
			check: func(t *testing.T, r *ec2backend.Route) {
				t.Helper()
				assert.Equal(t, "pl-0123456789abcdef0", r.DestinationPrefixListID)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newExtendedServiceBackends(t)
			rc := cloudformation.NewResourceCreator(backends)

			vpcID, err := rc.Create(
				t.Context(),
				"V",
				"AWS::EC2::VPC",
				map[string]any{"CidrBlock": "10.0.0.0/16"},
				nil,
				nil,
			)
			require.NoError(t, err)
			rtID, err := rc.Create(t.Context(), "RT", "AWS::EC2::RouteTable", map[string]any{"VpcId": vpcID}, nil, nil)
			require.NoError(t, err)

			_, err = rc.Create(t.Context(), "R", "AWS::EC2::Route", tt.props(rtID), nil, nil)
			require.NoError(t, err)

			rts, err := backends.EC2.Backend.DescribeRouteTables([]string{rtID})
			require.NoError(t, err)
			require.Len(t, rts, 1)

			var found *ec2backend.Route
			for i := range rts[0].Routes {
				r := &rts[0].Routes[i]
				if r.Origin == "CreateRoute" {
					found = r
				}
			}
			require.NotNil(t, found, "route created by the stack must exist")
			tt.check(t, found)
		})
	}
}
