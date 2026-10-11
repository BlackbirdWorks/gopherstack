package directconnect_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	directconnectsdk "github.com/aws/aws-sdk-go-v2/service/directconnect"
	"github.com/aws/aws-sdk-go-v2/service/directconnect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRealism_InputValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call    func(c *directconnectsdk.Client) error
		name    string
		wantErr string
	}{
		{
			name: "connection_bandwidth",
			call: func(c *directconnectsdk.Client) error {
				_, err := c.CreateConnection(t.Context(), &directconnectsdk.CreateConnectionInput{
					Bandwidth: aws.String("7Gbps"), ConnectionName: aws.String("c"), Location: aws.String("EqDC2"),
				})

				return err
			},
			wantErr: "Invalid bandwidth 7Gbps",
		},
		{
			name: "lag_bandwidth",
			call: func(c *directconnectsdk.Client) error {
				_, err := c.CreateLag(t.Context(), &directconnectsdk.CreateLagInput{
					ConnectionsBandwidth: aws.String("500Mbps"), LagName: aws.String("l"),
					Location: aws.String("EqDC2"), NumberOfConnections: 1,
				})

				return err
			},
			wantErr: "Invalid connectionsBandwidth",
		},
		{
			name: "gateway_asn_low",
			call: func(c *directconnectsdk.Client) error {
				_, err := c.CreateDirectConnectGateway(t.Context(), &directconnectsdk.CreateDirectConnectGatewayInput{
					DirectConnectGatewayName: aws.String("g"), AmazonSideAsn: aws.Int64(64000),
				})

				return err
			},
			wantErr: "amazonSideAsn must be in the range",
		},
		{
			name: "gateway_asn_32bit_ok",
			call: func(c *directconnectsdk.Client) error {
				_, err := c.CreateDirectConnectGateway(t.Context(), &directconnectsdk.CreateDirectConnectGatewayInput{
					DirectConnectGatewayName: aws.String("g"), AmazonSideAsn: aws.Int64(4200000001),
				})

				return err
			},
		},
		{
			name: "describe_connection_unknown",
			call: func(c *directconnectsdk.Client) error {
				_, err := c.DescribeConnections(t.Context(), &directconnectsdk.DescribeConnectionsInput{
					ConnectionId: aws.String("dxcon-nope"),
				})

				return err
			},
			wantErr: "Could not find Connection with ID: dxcon-nope",
		},
		{
			name: "describe_lag_unknown",
			call: func(c *directconnectsdk.Client) error {
				in := &directconnectsdk.DescribeLagsInput{LagId: aws.String("dxlag-nope")}
				_, err := c.DescribeLags(t.Context(), in)

				return err
			},
			wantErr: "Could not find Lag with ID: dxlag-nope",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newTestHandlerAndClient(t)
			err := tt.call(client)

			if tt.wantErr == "" {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestRealism_VlanValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantErr string
		vlans   []int32
	}{
		{name: "too_high", vlans: []int32{4095}, wantErr: "vlan must be between 1 and 4094"},
		{name: "negative", vlans: []int32{-1}, wantErr: "vlan must be between 1 and 4094"},
		{name: "duplicate_names_vlan", vlans: []int32{100, 100}, wantErr: "VLAN 100 is already in use"},
		{name: "distinct", vlans: []int32{100, 4094}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newTestHandlerAndClient(t)
			conn := createTestConnection(t, client)

			var err error

			for _, v := range tt.vlans {
				in := &directconnectsdk.CreatePrivateVirtualInterfaceInput{
					ConnectionId: conn.ConnectionId,
					NewPrivateVirtualInterface: &types.NewPrivateVirtualInterface{
						VirtualInterfaceName: aws.String("v"), Vlan: v, Asn: 65001,
					},
				}
				_, err = client.CreatePrivateVirtualInterface(t.Context(), in)
			}

			if tt.wantErr == "" {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestRealism_LagChildConnectionsProgress(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)

	lag, err := client.CreateLag(t.Context(), &directconnectsdk.CreateLagInput{
		ConnectionsBandwidth: aws.String("1Gbps"), LagName: aws.String("l"),
		Location: aws.String("EqDC2"), NumberOfConnections: 2,
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		out, dErr := client.DescribeConnections(t.Context(), &directconnectsdk.DescribeConnectionsInput{})
		if dErr != nil {
			return false
		}

		n := 0

		for _, c := range out.Connections {
			if aws.ToString(c.LagId) == aws.ToString(lag.LagId) && c.ConnectionState == types.ConnectionStateAvailable {
				n++
			}
		}

		return n == 2
	}, 5*time.Second, 20*time.Millisecond)
}
