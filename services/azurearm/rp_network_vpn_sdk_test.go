package azurearm_test

import (
	"net/http"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const vpnAPI = "?api-version=2023-09-01"

func TestNetworkVPNGatewaysAndConnections(t *testing.T) {
	t.Parallel()

	s := newNetStack(t)
	v := s.vnet(
		t,
		"gwvnet",
		[]string{"10.0.0.0/16"},
		map[string]string{"GatewaySubnet": "10.0.255.0/27", "other": "10.0.1.0/24"},
	)
	pip, err := s.pip(t, "gwpip", armnetwork.PublicIPAddressSKUNameStandard, "")
	require.NoError(t, err)

	pip2, err := s.pip(t, "gwpip2", armnetwork.PublicIPAddressSKUNameStandard, "")
	require.NoError(t, err)

	put := func(typ, name, body string) (int, map[string]any) {
		return s.rawJSON(t, http.MethodPut, netProv("Microsoft.Network", typ, name)+vpnAPI, body)
	}

	gw := func(subnet, pipID, sku string) string {
		return `{"location":"westus","properties":{"gatewayType":"Vpn","vpnType":"RouteBased","sku":{"name":"` + sku +
			`"},"ipConfigurations":[{"name":"c","properties":{"subnet":{"id":"` + subnet +
			`"},"publicIPAddress":{"id":"` + pipID + `"}}}]}}`
	}

	subnets := map[string]string{}
	for _, sn := range v.Properties.Subnets {
		subnets[*sn.Name] = *sn.ID
	}

	lngs := []struct {
		name, body string
		want       int
	}{
		{
			"lng1",
			`{"location":"westus","properties":{"gatewayIpAddress":"203.0.113.9",` +
				`"localNetworkAddressSpace":{"addressPrefixes":["192.168.0.0/24"]}}}`,
			201,
		},
		{
			"lng-bad-cidr",
			`{"location":"westus","properties":{"gatewayIpAddress":"203.0.113.9",` +
				`"localNetworkAddressSpace":{"addressPrefixes":["192.168.0.5/24"]}}}`,
			400,
		},
		{
			"lng-both",
			`{"location":"westus","properties":{"gatewayIpAddress":"203.0.113.9","fqdn":"x.example.com"}}`,
			400,
		},
		{"lng-asn", `{"location":"westus","properties":{"fqdn":"x.example.com","bgpSettings":{"asn":65515}}}`, 400},
	}

	for _, tc := range lngs {
		code, out := put(netLNGsType, tc.name, tc.body)
		assert.Equal(t, tc.want, code, tc.name+" %v", out)
	}

	gws := []struct {
		name, body string
		want       int
	}{
		{"vng-wrong-subnet", gw(subnets["other"], *pip.ID, "VpnGw1"), 400},
		{"vng-missing-pip", gw(subnets["GatewaySubnet"], *pip.ID+"x", "VpnGw1"), 400},
		{"vng-bad-sku", gw(subnets["GatewaySubnet"], *pip.ID, "Gold"), 400},
		{"vng1", gw(subnets["GatewaySubnet"], *pip.ID, "VpnGw1"), 201},
		{"vng-same-pip", gw(subnets["GatewaySubnet"], *pip.ID, "VpnGw1"), 400},
		{"vng-second-vpn-gateway", gw(subnets["GatewaySubnet"], *pip2.ID, "VpnGw1"), 400},
	}

	for _, tc := range gws {
		code, out := put("virtualNetworkGateways", tc.name, tc.body)
		assert.Equal(t, tc.want, code, tc.name+" %v", out)
	}

	vng, lng := netProv(
		"Microsoft.Network",
		"virtualNetworkGateways",
		"vng1",
	), netProv(
		"Microsoft.Network",
		netLNGsType,
		"lng1",
	)
	conn := func(extra string) string {
		return `{"location":"westus","properties":{"connectionType":"IPsec","virtualNetworkGateway1":{"id":"` + vng +
			`"},"localNetworkGateway2":{"id":"` + lng + `"}` + extra + `}}`
	}

	code, _ := put("connections", "c-nokey", conn(""))
	assert.Equal(t, http.StatusBadRequest, code, "IPsec needs a shared key")

	code, out := put("connections", "c1", conn(`,"sharedKey":"abc123"`))
	require.Equal(t, http.StatusCreated, code, "%v", out)
	assert.Equal(t, "Unknown", out["properties"].(map[string]any)["connectionStatus"])

	resp := s.rawDo(t, http.MethodDelete, netProv("Microsoft.Network", netLNGsType, "lng1")+vpnAPI, "")
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "a local network gateway in use cannot be deleted")

	resp = s.rawDo(t, http.MethodDelete, netProv("Microsoft.Network", "connections", "c1")+vpnAPI, "")
	assert.Less(t, resp.StatusCode, 300)

	resp = s.rawDo(t, http.MethodDelete, netProv("Microsoft.Network", netLNGsType, "lng1")+vpnAPI, "")
	assert.Less(t, resp.StatusCode, 300)
}

const netLNGsType = "localNetworkGateways"

func TestNetworkExpressRouteCircuits(t *testing.T) {
	t.Parallel()

	s := newNetStack(t)
	circuit := netProv("Microsoft.Network", "expressRouteCircuits", "erc1")

	code, out := s.rawJSON(
		t,
		http.MethodPut,
		circuit+vpnAPI,
		`{"location":"westus","sku":{"name":"Standard_MeteredData"},`+
			`"properties":{"serviceProviderProperties":{"serviceProviderName":"Equinix",`+
			`"peeringLocation":"Silicon Valley","bandwidthInMbps":200}}}`,
	)
	require.Equal(t, http.StatusCreated, code, "%v", out)
	assert.Equal(t, "Standard", out["sku"].(map[string]any)["tier"])
	assert.NotEmpty(t, out["properties"].(map[string]any)["serviceKey"])

	code, _ = s.rawJSON(t, http.MethodPut, netProv("Microsoft.Network", "expressRouteCircuits", "erc-bad")+vpnAPI,
		`{"location":"westus","sku":{"name":"Gold"},"properties":{"serviceProviderProperties":{}}}`)
	assert.Equal(t, http.StatusBadRequest, code)

	peering := func(name, vlan, extra string) (int, map[string]any) {
		return s.rawJSON(t, http.MethodPut, circuit+"/peerings/"+name+vpnAPI, `{"properties":{"peerASN":65010,`+
			`"primaryPeerAddressPrefix":"192.168.1.0/30",`+
			`"secondaryPeerAddressPrefix":"192.168.2.0/30","vlanId":`+vlan+extra+`}}`)
	}

	code, out = peering("AzurePrivatePeering", "100", "")
	require.Equal(t, http.StatusCreated, code, "%v", out)
	assert.Equal(t, "Enabled", out["properties"].(map[string]any)["state"])

	code, _ = peering(
		"MicrosoftPeering",
		"100",
		`,"microsoftPeeringConfig":{"advertisedPublicPrefixes":["203.0.113.0/24"]}`,
	)
	assert.Equal(t, http.StatusBadRequest, code, "duplicate vlan")

	code, _ = peering("MicrosoftPeering", "200", "")
	assert.Equal(t, http.StatusBadRequest, code, "Microsoft peering needs advertised prefixes")

	code, _ = peering("custom", "300", "")
	assert.Equal(t, http.StatusBadRequest, code, "peering name is the peering type")

	code, out = s.rawJSON(t, http.MethodPut, circuit+"/authorizations/partner"+vpnAPI, `{"properties":{}}`)
	require.Equal(t, http.StatusCreated, code, "%v", out)
	assert.NotEmpty(t, out["properties"].(map[string]any)["authorizationKey"])

	code, out = s.rawJSON(t, http.MethodGet, circuit+vpnAPI, "")
	require.Equal(t, http.StatusOK, code)
	assert.Len(t, out["properties"].(map[string]any)["peerings"], 1)
	assert.Len(t, out["properties"].(map[string]any)["authorizations"], 1)
}
