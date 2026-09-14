package ec2

import (
	"encoding/xml"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestToRouteServerRouteItem_WireShape verifies RouteServerRoute serializes
// against the real fields (ec2@v1.329.0 types/types.go:20601) and no longer
// carries the fabricated routeInstalled boolean.
func TestToRouteServerRouteItem_WireShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   *RouteServerRoute
		want []string
		bad  []string
	}{
		{
			name: "populated route",
			in: &RouteServerRoute{
				RouteServerEndpointID: "rse-1",
				RouteServerPeerID:     "rsp-1",
				Prefix:                "10.0.0.0/24",
				NextHopIP:             "10.0.0.1",
				RouteStatus:           "in-rib",
				AsPaths:               []string{"65000", "65001"},
				Med:                   100,
				RouteInstallationDetails: []RouteServerRouteInstallationDetail{
					{
						RouteTableID:            "rtb-1",
						RouteInstallationStatus: "installed",
					},
				},
			},
			want: []string{
				"<routeServerEndpointId>rse-1</routeServerEndpointId>",
				"<routeServerPeerId>rsp-1</routeServerPeerId>",
				"<prefix>10.0.0.0/24</prefix>",
				"<nextHopIp>10.0.0.1</nextHopIp>",
				"<routeStatus>in-rib</routeStatus>",
				"<asPathSet><item>65000</item><item>65001</item></asPathSet>",
				"<med>100</med>",
				"<routeInstallationDetailSet><item>" +
					"<routeTableId>rtb-1</routeTableId>" +
					"<routeInstallationStatus>installed</routeInstallationStatus>" +
					"</item></routeInstallationDetailSet>",
			},
			bad: []string{"routeInstalled"},
		},
		{
			name: "empty route has no scalar fields",
			in:   &RouteServerRoute{},
			want: []string{"<asPathSet></asPathSet>", "<routeInstallationDetailSet></routeInstallationDetailSet>"},
			bad:  []string{"routeInstalled"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			item := toRouteServerRouteItem(tt.in)
			raw, err := xml.Marshal(item)
			require.NoError(t, err)

			got := string(raw)
			for _, w := range tt.want {
				assert.Contains(t, got, w)
			}

			for _, b := range tt.bad {
				assert.NotContains(t, got, b)
			}
		})
	}
}
