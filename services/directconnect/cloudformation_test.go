package directconnect_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/directconnect"
)

func TestCreateCFNResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		props    map[string]any
		name     string
		typ      string
		wantAttr string
		wantErr  bool
	}{
		{
			name: "connection", typ: directconnect.CFNConnection, wantAttr: "ConnectionArn",
			props: map[string]any{"ConnectionName": "c", "Bandwidth": "1Gbps", "Location": "EqDC2"},
		},
		{
			name: "lag", typ: directconnect.CFNLag, wantAttr: "LagArn",
			props: map[string]any{"LagName": "l", "ConnectionsBandwidth": "1Gbps", "Location": "EqDC2"},
		},
		{
			name: "lag_minimum_links_on_create", typ: directconnect.CFNLag, wantErr: true,
			props: map[string]any{
				"LagName": "l", "ConnectionsBandwidth": "1Gbps", "Location": "EqDC2", "MinimumLinks": 1,
			},
		},
		{
			name: "lag_number_of_connections", typ: directconnect.CFNLag, wantErr: true,
			props: map[string]any{
				"LagName": "l", "ConnectionsBandwidth": "1Gbps", "Location": "EqDC2", "NumberOfConnections": 1,
			},
		},
		{
			name: "gateway_string_asn", typ: directconnect.CFNDirectConnectGateway, wantAttr: "DirectConnectGatewayArn",
			props: map[string]any{"DirectConnectGatewayName": "g", "AmazonSideAsn": "64512"},
		},
		{
			name: "unknown_property", typ: directconnect.CFNConnection, wantErr: true,
			props: map[string]any{"ConnectionName": "c", "Bandwidth": "1Gbps", "Location": "EqDC2", "Bogus": 1},
		},
		{
			name: "missing_required", typ: directconnect.CFNConnection, wantErr: true,
			props: map[string]any{"ConnectionName": "c"},
		},
		{
			name: "vif_flat_asn", typ: directconnect.CFNTransitVirtualInterface, wantErr: true,
			props: map[string]any{"ConnectionId": "dxcon-x", "VirtualInterfaceName": "v", "Vlan": 1, "Asn": 65000},
		},
		{name: "unknown_type", typ: "AWS::DirectConnect::Nope", wantErr: true, props: map[string]any{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := directconnect.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")

			id, attrs, err := b.CreateCFNResource(tt.typ, tt.props)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, id)
			assert.NotEmpty(t, attrs[tt.wantAttr])

			_, found := b.CFNResourceState(tt.typ, id)
			assert.True(t, found)

			if tt.typ == directconnect.CFNLag {
				assert.Empty(t, b.DescribeConnections(""))
			}
		})
	}
}

func TestCreateCFNResourceSpecShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check   func(t *testing.T, b *directconnect.InMemoryBackend, id string)
		props   func(connARN, gwARN string) (string, map[string]any)
		name    string
		wantErr bool
	}{
		{
			name: "public_vif_bgp_peers",
			props: func(connARN, _ string) (string, map[string]any) {
				return directconnect.CFNPublicVirtualInterface, map[string]any{
					"ConnectionId": connARN, "VirtualInterfaceName": "v", "Vlan": 101,
					"RouteFilterPrefixes": []any{"50.0.0.0/30"},
					"BgpPeers": []any{
						map[string]any{
							"AddressFamily": "ipv4", "AmazonAddress": "50.0.0.1/30",
							"CustomerAddress": "50.0.0.2/30", "Asn": "65000", "AuthKey": "example-auth-key",
						},
						map[string]any{"AddressFamily": "ipv6", "Asn": "65000"},
					},
					"AllocatePublicVirtualInterfaceRoleArn": "arn:aws:iam::123456789012:role/r",
				}
			},
			check: func(t *testing.T, b *directconnect.InMemoryBackend, id string) {
				t.Helper()

				vs := b.DescribeVirtualInterfaces("", id[strings.LastIndex(id, "/")+1:])
				require.Len(t, vs, 1)
				assert.Equal(t, "ipv4", vs[0].AddressFamily)
				assert.Equal(t, "50.0.0.2/30", vs[0].CustomerAddress)
				assert.Equal(t, int64(65000), vs[0].AsnValue)
				require.Len(t, vs[0].BgpPeers, 1)
				assert.Equal(t, "ipv6", vs[0].BgpPeers[0].AddressFamily)
				assert.Equal(t, []directconnect.RouteFilterPrefix{{Cidr: "50.0.0.0/30"}}, vs[0].RouteFilterPrefixes)
			},
		},
		{
			name: "transit_vif_gateway_arn",
			props: func(connARN, gwARN string) (string, map[string]any) {
				return directconnect.CFNTransitVirtualInterface, map[string]any{
					"ConnectionId": connARN, "DirectConnectGatewayId": gwARN, "VirtualInterfaceName": "v",
					"Vlan": 102, "BgpPeers": []any{map[string]any{"AddressFamily": "ipv4", "Asn": "65000"}},
				}
			},
		},
		{
			name: "private_vif_flat_bgp_rejected",
			props: func(connARN, _ string) (string, map[string]any) {
				return directconnect.CFNPrivateVirtualInterface, map[string]any{
					"ConnectionId": connARN, "VirtualInterfaceName": "v", "Vlan": 103, "Bogus": 1,
				}
			},
			wantErr: true,
		},
		{
			name: "association_spec_names",
			props: func(_, gwARN string) (string, map[string]any) {
				return directconnect.CFNGatewayAssociation, map[string]any{
					"DirectConnectGatewayId":                gwARN,
					"AssociatedGatewayId":                   "vgw-0123456789ab",
					"AllowedPrefixesToDirectConnectGateway": []any{"10.0.0.0/24"},
				}
			},
			check: func(t *testing.T, b *directconnect.InMemoryBackend, id string) {
				t.Helper()

				as := b.DescribeDirectConnectGatewayAssociations("", id, "", "")
				require.Len(t, as, 1)
				assert.Equal(t, "vgw-0123456789ab", as[0].GatewayID)
				assert.Equal(t, []directconnect.RouteFilterPrefix{{Cidr: "10.0.0.0/24"}}, as[0].AllowedPrefixes)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := directconnect.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")

			connARN, _, err := b.CreateCFNResource(directconnect.CFNConnection, map[string]any{
				"ConnectionName": "c", "Bandwidth": "1Gbps", "Location": "EqDC2",
			})
			require.NoError(t, err)
			assert.Contains(t, connARN, ":dxcon/dxcon-")

			gwARN, _, err := b.CreateCFNResource(directconnect.CFNDirectConnectGateway, map[string]any{
				"DirectConnectGatewayName": "g",
			})
			require.NoError(t, err)
			assert.Contains(t, gwARN, ":dx-gateway/")

			typ, props := tt.props(connARN, gwARN)

			id, _, err := b.CreateCFNResource(typ, props)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			if typ != directconnect.CFNGatewayAssociation {
				assert.Contains(t, id, "arn:")
			}

			if tt.check != nil {
				tt.check(t, b, id)
			}

			_, found := b.CFNResourceState(typ, id)
			assert.True(t, found)
		})
	}
}
