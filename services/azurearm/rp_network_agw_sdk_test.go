package azurearm_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pkcs12 "software.sslmate.com/src/go-pkcs12"
)

const agwName = "agw1"

func agwChild(gw, kind, name string) string {
	return "/subscriptions/" + aciSub + "/resourceGroups/" + netRG + "/providers/Microsoft.Network/applicationGateways/" +
		gw + "/" + kind + "/" + name
}

func agwRef(gw, kind, name string) *armnetwork.SubResource {
	return &armnetwork.SubResource{ID: new(agwChild(gw, kind, name))}
}

// agwFixture is a vnet with a gateway subnet and a workload subnet, plus a Standard static public IP.
type agwFixture struct {
	*netStack
	gwSubnet, otherSubnet, pipID string
}

func newAGWFixture(t *testing.T) *agwFixture {
	t.Helper()

	s := newNetStack(t)
	v := s.vnet(t, "vnet1", []string{"10.0.0.0/16"}, map[string]string{"appgw": "10.0.1.0/24", "other": "10.0.2.0/24"})

	pip, err := s.pip(t, "agwpip", armnetwork.PublicIPAddressSKUNameStandard, "")
	require.NoError(t, err)

	f := &agwFixture{netStack: s, pipID: *pip.ID}

	for _, sn := range v.Properties.Subnets {
		if *sn.Name == "appgw" {
			f.gwSubnet = *sn.ID
		} else {
			f.otherSubnet = *sn.ID
		}
	}

	return f
}

func (f *agwFixture) gateway(name string) armnetwork.ApplicationGateway {
	return armnetwork.ApplicationGateway{
		Location: new("westus"),
		Properties: &armnetwork.ApplicationGatewayPropertiesFormat{
			SKU: &armnetwork.ApplicationGatewaySKU{
				Name: new(armnetwork.ApplicationGatewaySKUNameStandardV2),
				Tier: new(armnetwork.ApplicationGatewayTierStandardV2), Capacity: new(int32(2)),
			},
			GatewayIPConfigurations: []*armnetwork.ApplicationGatewayIPConfiguration{{
				Name: new("gwip"),
				Properties: &armnetwork.ApplicationGatewayIPConfigurationPropertiesFormat{
					Subnet: &armnetwork.SubResource{ID: &f.gwSubnet},
				},
			}},
			FrontendIPConfigurations: []*armnetwork.ApplicationGatewayFrontendIPConfiguration{{
				Name: new("fe"),
				Properties: &armnetwork.ApplicationGatewayFrontendIPConfigurationPropertiesFormat{
					PublicIPAddress: &armnetwork.SubResource{ID: &f.pipID},
				},
			}},
			FrontendPorts: []*armnetwork.ApplicationGatewayFrontendPort{{
				Name: new(
					"port80",
				), Properties: &armnetwork.ApplicationGatewayFrontendPortPropertiesFormat{Port: new(int32(80))},
			}},
			BackendAddressPools: []*armnetwork.ApplicationGatewayBackendAddressPool{{
				Name: new("pool"),
				Properties: &armnetwork.ApplicationGatewayBackendAddressPoolPropertiesFormat{
					BackendAddresses: []*armnetwork.ApplicationGatewayBackendAddress{{IPAddress: new("10.0.2.10")}},
				},
			}},
			BackendHTTPSettingsCollection: []*armnetwork.ApplicationGatewayBackendHTTPSettings{{
				Name: new("settings"),
				Properties: &armnetwork.ApplicationGatewayBackendHTTPSettingsPropertiesFormat{
					Port: new(int32(80)), Protocol: new(armnetwork.ApplicationGatewayProtocolHTTP),
				},
			}},
			HTTPListeners: []*armnetwork.ApplicationGatewayHTTPListener{{
				Name: new("listener"),
				Properties: &armnetwork.ApplicationGatewayHTTPListenerPropertiesFormat{
					FrontendIPConfiguration: agwRef(name, "frontendIPConfigurations", "fe"),
					FrontendPort:            agwRef(name, "frontendPorts", "port80"),
					Protocol:                new(armnetwork.ApplicationGatewayProtocolHTTP),
				},
			}},
			RequestRoutingRules: []*armnetwork.ApplicationGatewayRequestRoutingRule{{
				Name: new("rule"),
				Properties: &armnetwork.ApplicationGatewayRequestRoutingRulePropertiesFormat{
					RuleType:            new(armnetwork.ApplicationGatewayRequestRoutingRuleTypeBasic),
					Priority:            new(int32(10)),
					HTTPListener:        agwRef(name, "httpListeners", "listener"),
					BackendAddressPool:  agwRef(name, "backendAddressPools", "pool"),
					BackendHTTPSettings: agwRef(name, "backendHttpSettingsCollection", "settings"),
				},
			}},
		},
	}
}

func (s *netStack) putGateway(
	t *testing.T,
	name string,
	gw armnetwork.ApplicationGateway,
) (armnetwork.ApplicationGateway, error) {
	t.Helper()

	p, err := s.net.NewApplicationGatewaysClient().BeginCreateOrUpdate(t.Context(), netRG, name, gw, nil)
	if err != nil {
		return armnetwork.ApplicationGateway{}, err
	}

	res, err := p.PollUntilDone(t.Context(), pollFast())

	return res.ApplicationGateway, err
}

func TestNetworkSDK_AppGatewayLifecycle(t *testing.T) {
	t.Parallel()

	f := newAGWFixture(t)
	ctx := t.Context()

	got, err := f.putGateway(t, agwName, f.gateway(agwName))
	require.NoError(t, err)
	assert.Equal(t, armnetwork.ApplicationGatewayOperationalStateRunning, *got.Properties.OperationalState)
	assert.Equal(t, armnetwork.ProvisioningStateSucceeded, *got.Properties.ProvisioningState)
	assert.Equal(t, agwChild(agwName, "httpListeners", "listener"), *got.Properties.HTTPListeners[0].ID)
	assert.Equal(t, int32(10), *got.Properties.RequestRoutingRules[0].Properties.Priority)
	assert.Equal(t, armnetwork.ApplicationGatewayCookieBasedAffinityDisabled,
		*got.Properties.BackendHTTPSettingsCollection[0].Properties.CookieBasedAffinity)

	c := f.net.NewApplicationGatewaysClient()

	one, err := c.Get(ctx, netRG, agwName, nil)
	require.NoError(t, err)
	assert.Equal(t, "10.0.2.10", *one.Properties.BackendAddressPools[0].Properties.BackendAddresses[0].IPAddress)

	pager := c.NewListPager(netRG, nil)
	require.True(t, pager.More())

	page, err := pager.NextPage(ctx)
	require.NoError(t, err)
	assert.Len(t, page.Value, 1)

	all := c.NewListAllPager(nil)
	require.True(t, all.More())

	// The gateway holds the public IP and the subnet.
	_, err = f.net.NewPublicIPAddressesClient().BeginDelete(ctx, netRG, "agwpip", nil)
	assert.Equal(t, "PublicIPAddressCannotBeDeleted", sdkErrCode(err))

	pubIP, err := f.net.NewPublicIPAddressesClient().Get(ctx, netRG, "agwpip", nil)
	require.NoError(t, err)
	assert.Contains(t, *pubIP.Properties.IPConfiguration.ID, "applicationGateways/agw1/frontendIPConfigurations/fe")

	_, err = f.net.NewSubnetsClient().BeginDelete(ctx, netRG, "vnet1", "appgw", nil)
	assert.Equal(t, "InUseSubnetCannotBeDeleted", sdkErrCode(err))

	del, err := c.BeginDelete(ctx, netRG, agwName, nil)
	require.NoError(t, err)
	_, err = del.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)

	_, err = c.Get(ctx, netRG, agwName, nil)
	assert.Equal(t, "ResourceNotFound", sdkErrCode(err))

	pubIP, err = f.net.NewPublicIPAddressesClient().Get(ctx, netRG, "agwpip", nil)
	require.NoError(t, err)
	assert.Nil(t, pubIP.Properties.IPConfiguration)
}

func TestNetworkSDK_AppGatewayValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate   func(f *agwFixture, gw *armnetwork.ApplicationGateway)
		name     string
		wantCode string
	}{
		{name: "valid"},
		{
			name: "v1 sku retired", wantCode: "ApplicationGatewayV1SkuRetired",
			mutate: func(_ *agwFixture, gw *armnetwork.ApplicationGateway) {
				gw.Properties.SKU = &armnetwork.ApplicationGatewaySKU{
					Name: new(armnetwork.ApplicationGatewaySKUNameStandardMedium), Capacity: new(int32(2)),
				}
			},
		},
		{
			name: "capacity and autoscale", wantCode: "InvalidRequestFormat",
			mutate: func(_ *agwFixture, gw *armnetwork.ApplicationGateway) {
				gw.Properties.AutoscaleConfiguration = &armnetwork.ApplicationGatewayAutoscaleConfiguration{
					MinCapacity: new(int32(1)),
				}
			},
		},
		{
			name: "autoscale only",
			mutate: func(_ *agwFixture, gw *armnetwork.ApplicationGateway) {
				gw.Properties.SKU.Capacity = nil
				gw.Properties.AutoscaleConfiguration = &armnetwork.ApplicationGatewayAutoscaleConfiguration{
					MinCapacity: new(int32(0)), MaxCapacity: new(int32(10)),
				}
			},
		},
		{
			name: "autoscale max below min", wantCode: "InvalidRequestFormat",
			mutate: func(_ *agwFixture, gw *armnetwork.ApplicationGateway) {
				gw.Properties.SKU.Capacity = nil
				gw.Properties.AutoscaleConfiguration = &armnetwork.ApplicationGatewayAutoscaleConfiguration{
					MinCapacity: new(int32(5)), MaxCapacity: new(int32(3)),
				}
			},
		},
		{
			name: "rule priority missing", wantCode: "ApplicationGatewayRequestRoutingRulePriorityRequired",
			mutate: func(_ *agwFixture, gw *armnetwork.ApplicationGateway) {
				gw.Properties.RequestRoutingRules[0].Properties.Priority = nil
			},
		},
		{
			name: "rule priority out of range", wantCode: "InvalidRequestFormat",
			mutate: func(_ *agwFixture, gw *armnetwork.ApplicationGateway) {
				gw.Properties.RequestRoutingRules[0].Properties.Priority = new(int32(20001))
			},
		},
		{
			name: "dangling listener", wantCode: "InvalidResourceReference",
			mutate: func(_ *agwFixture, gw *armnetwork.ApplicationGateway) {
				gw.Properties.RequestRoutingRules[0].Properties.HTTPListener = agwRef(agwName, "httpListeners", "nope")
			},
		},
		{
			name: "pool of another gateway", wantCode: "InvalidResourceReference",
			mutate: func(_ *agwFixture, gw *armnetwork.ApplicationGateway) {
				gw.Properties.RequestRoutingRules[0].Properties.BackendAddressPool = agwRef(
					"other",
					"backendAddressPools",
					"pool",
				)
			},
		},
		{
			name: "two rules one listener", wantCode: "InvalidRequestFormat",
			mutate: func(_ *agwFixture, gw *armnetwork.ApplicationGateway) {
				second := *gw.Properties.RequestRoutingRules[0]
				second.Name = new("rule2")
				pr := *second.Properties
				pr.Priority = new(int32(20))
				second.Properties = &pr
				gw.Properties.RequestRoutingRules = append(gw.Properties.RequestRoutingRules, &second)
			},
		},
		{
			name: "duplicate priority", wantCode: "ApplicationGatewayRequestRoutingRulePriorityDuplicate",
			mutate: func(_ *agwFixture, gw *armnetwork.ApplicationGateway) {
				gw.Properties.HTTPListeners = append(
					gw.Properties.HTTPListeners,
					&armnetwork.ApplicationGatewayHTTPListener{
						Name: new("l2"),
						Properties: &armnetwork.ApplicationGatewayHTTPListenerPropertiesFormat{
							FrontendIPConfiguration: agwRef(agwName, "frontendIPConfigurations", "fe"),
							FrontendPort:            agwRef(agwName, "frontendPorts", "port80"),
							HostName:                new("a.example.com"),
							Protocol:                new(armnetwork.ApplicationGatewayProtocolHTTP),
						},
					},
				)
				second := *gw.Properties.RequestRoutingRules[0]
				second.Name = new("rule2")
				pr := *second.Properties
				pr.HTTPListener = agwRef(agwName, "httpListeners", "l2")
				second.Properties = &pr
				gw.Properties.RequestRoutingRules = append(gw.Properties.RequestRoutingRules, &second)
			},
		},
		{
			name: "https listener without certificate", wantCode: "InvalidRequestFormat",
			mutate: func(_ *agwFixture, gw *armnetwork.ApplicationGateway) {
				gw.Properties.HTTPListeners[0].Properties.Protocol = new(armnetwork.ApplicationGatewayProtocolHTTPS)
			},
		},
		{
			name: "bad backend address", wantCode: "InvalidRequestFormat",
			mutate: func(_ *agwFixture, gw *armnetwork.ApplicationGateway) {
				gw.Properties.BackendAddressPools[0].Properties.BackendAddresses[0].IPAddress = new("not-an-ip")
			},
		},
		{
			name: "rule without backend", wantCode: "InvalidRequestFormat",
			mutate: func(_ *agwFixture, gw *armnetwork.ApplicationGateway) {
				gw.Properties.RequestRoutingRules[0].Properties.BackendHTTPSettings = nil
			},
		},
		{
			name: "backend settings port", wantCode: "InvalidRequestFormat",
			mutate: func(_ *agwFixture, gw *armnetwork.ApplicationGateway) {
				gw.Properties.BackendHTTPSettingsCollection[0].Properties.Port = new(int32(70000))
			},
		},
		{
			name: "no gateway ip configuration", wantCode: "InvalidRequestFormat",
			mutate: func(_ *agwFixture, gw *armnetwork.ApplicationGateway) {
				gw.Properties.GatewayIPConfigurations = nil
			},
		},
		{
			name: "waf policy on non waf sku", wantCode: "InvalidResourceReference",
			mutate: func(_ *agwFixture, gw *armnetwork.ApplicationGateway) {
				gw.Properties.FirewallPolicy = &armnetwork.SubResource{ID: new(
					fwSubID + "ApplicationGatewayWebApplicationFirewallPolicies/missing")}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newAGWFixture(t)
			gw := f.gateway(agwName)

			if tc.mutate != nil {
				tc.mutate(f, &gw)
			}

			_, err := f.putGateway(t, agwName, gw)
			if tc.wantCode == "" {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			assert.Equal(t, tc.wantCode, sdkErrCode(err))
		})
	}
}

func TestNetworkSDK_AppGatewayPublicIPRules(t *testing.T) {
	t.Parallel()

	f := newAGWFixture(t)

	basic, err := f.pip(t, "basicpip", armnetwork.PublicIPAddressSKUNameBasic, "")
	require.NoError(t, err)

	gw := f.gateway(agwName)
	gw.Properties.FrontendIPConfigurations[0].Properties.PublicIPAddress = &armnetwork.SubResource{ID: basic.ID}

	_, err = f.putGateway(t, agwName, gw)
	assert.Equal(t, "ApplicationGatewayV2PublicIpNotStandardStatic", sdkErrCode(err))

	_, err = f.putGateway(t, agwName, f.gateway(agwName))
	require.NoError(t, err)

	other := f.gateway("agw2")
	other.Properties.RequestRoutingRules[0].Properties.HTTPListener = agwRef("agw2", "httpListeners", "listener")
	other.Properties.RequestRoutingRules[0].Properties.BackendAddressPool = agwRef(
		"agw2",
		"backendAddressPools",
		"pool",
	)
	other.Properties.RequestRoutingRules[0].Properties.BackendHTTPSettings = agwRef(
		"agw2",
		"backendHttpSettingsCollection",
		"settings",
	)
	other.Properties.HTTPListeners[0].Properties.FrontendIPConfiguration = agwRef(
		"agw2",
		"frontendIPConfigurations",
		"fe",
	)
	other.Properties.HTTPListeners[0].Properties.FrontendPort = agwRef("agw2", "frontendPorts", "port80")

	_, err = f.putGateway(t, "agw2", other)
	assert.Equal(t, "PublicIPAddressInUse", sdkErrCode(err))
}

func TestNetworkSDK_AppGatewayDedicatedSubnet(t *testing.T) {
	t.Parallel()

	f := newAGWFixture(t)

	// A gateway cannot move into a subnet that holds a NIC.
	_, err := f.nic(t, "nic-in-gw-subnet", f.gwSubnet, armnetwork.IPAllocationMethodDynamic, "", nil)
	require.NoError(t, err)

	_, err = f.putGateway(t, agwName, f.gateway(agwName))
	assert.Equal(t, "ApplicationGatewaySubnetNotDedicated", sdkErrCode(err))

	// A subnet used by a gateway rejects NICs.
	g2 := newAGWFixture(t)
	_, err = g2.putGateway(t, agwName, g2.gateway(agwName))
	require.NoError(t, err)

	_, err = g2.nic(t, "nic1", g2.gwSubnet, armnetwork.IPAllocationMethodDynamic, "", nil)
	assert.Equal(t, "InvalidRequestFormat", sdkErrCode(err))

	// A second gateway may share the dedicated subnet.
	pip2, err := g2.pip(t, "pip2", armnetwork.PublicIPAddressSKUNameStandard, "")
	require.NoError(t, err)

	gw2 := g2.gateway("agw2")
	gw2.Properties.FrontendIPConfigurations[0].Properties.PublicIPAddress = &armnetwork.SubResource{ID: pip2.ID}
	gw2.Properties.RequestRoutingRules[0].Properties.HTTPListener = agwRef("agw2", "httpListeners", "listener")
	gw2.Properties.RequestRoutingRules[0].Properties.BackendAddressPool = agwRef("agw2", "backendAddressPools", "pool")
	gw2.Properties.RequestRoutingRules[0].Properties.BackendHTTPSettings = agwRef(
		"agw2",
		"backendHttpSettingsCollection",
		"settings",
	)
	gw2.Properties.HTTPListeners[0].Properties.FrontendIPConfiguration = agwRef(
		"agw2",
		"frontendIPConfigurations",
		"fe",
	)
	gw2.Properties.HTTPListeners[0].Properties.FrontendPort = agwRef("agw2", "frontendPorts", "port80")

	_, err = g2.putGateway(t, "agw2", gw2)
	require.NoError(t, err)
}

func TestNetworkSDK_AppGatewayPrivateFrontend(t *testing.T) {
	t.Parallel()

	f := newAGWFixture(t)
	gw := f.gateway(agwName)
	gw.Properties.FrontendIPConfigurations = append(gw.Properties.FrontendIPConfigurations,
		&armnetwork.ApplicationGatewayFrontendIPConfiguration{
			Name: new("priv"),
			Properties: &armnetwork.ApplicationGatewayFrontendIPConfigurationPropertiesFormat{
				Subnet: &armnetwork.SubResource{ID: &f.gwSubnet}, PrivateIPAddress: new("10.0.1.50"),
				PrivateIPAllocationMethod: new(armnetwork.IPAllocationMethodStatic),
			},
		})

	got, err := f.putGateway(t, agwName, gw)
	require.NoError(t, err)
	assert.Equal(t, "10.0.1.50", *got.Properties.FrontendIPConfigurations[1].Properties.PrivateIPAddress)

	// The address is taken for other resources in the vnet.
	other := f.gateway("agw2")
	other.Properties.FrontendIPConfigurations = append(other.Properties.FrontendIPConfigurations,
		gw.Properties.FrontendIPConfigurations[1])
	pip2, err := f.pip(t, "pip2", armnetwork.PublicIPAddressSKUNameStandard, "")
	require.NoError(t, err)

	other.Properties.FrontendIPConfigurations[0].Properties.PublicIPAddress = &armnetwork.SubResource{ID: pip2.ID}
	other.Properties.RequestRoutingRules[0].Properties.HTTPListener = agwRef("agw2", "httpListeners", "listener")
	other.Properties.RequestRoutingRules[0].Properties.BackendAddressPool = agwRef(
		"agw2",
		"backendAddressPools",
		"pool",
	)
	other.Properties.RequestRoutingRules[0].Properties.BackendHTTPSettings = agwRef(
		"agw2",
		"backendHttpSettingsCollection",
		"settings",
	)
	other.Properties.HTTPListeners[0].Properties.FrontendIPConfiguration = agwRef(
		"agw2",
		"frontendIPConfigurations",
		"fe",
	)
	other.Properties.HTTPListeners[0].Properties.FrontendPort = agwRef("agw2", "frontendPorts", "port80")

	_, err = f.putGateway(t, "agw2", other)
	assert.Equal(t, "PrivateIPAddressIsAllocated", sdkErrCode(err))

	// A private frontend in another subnet is refused.
	pip3, err := f.pip(t, "pip3", armnetwork.PublicIPAddressSKUNameStandard, "")
	require.NoError(t, err)

	bad := f.gateway("agw3")
	bad.Properties.FrontendIPConfigurations[0].Properties.PublicIPAddress = &armnetwork.SubResource{ID: pip3.ID}
	bad.Properties.RequestRoutingRules[0].Properties.HTTPListener = agwRef("agw3", "httpListeners", "listener")
	bad.Properties.RequestRoutingRules[0].Properties.BackendAddressPool = agwRef("agw3", "backendAddressPools", "pool")
	bad.Properties.RequestRoutingRules[0].Properties.BackendHTTPSettings = agwRef(
		"agw3",
		"backendHttpSettingsCollection",
		"settings",
	)
	bad.Properties.HTTPListeners[0].Properties.FrontendIPConfiguration = agwRef(
		"agw3",
		"frontendIPConfigurations",
		"fe",
	)
	bad.Properties.HTTPListeners[0].Properties.FrontendPort = agwRef("agw3", "frontendPorts", "port80")
	bad.Properties.FrontendIPConfigurations = append(bad.Properties.FrontendIPConfigurations,
		&armnetwork.ApplicationGatewayFrontendIPConfiguration{
			Name: new("priv"),
			Properties: &armnetwork.ApplicationGatewayFrontendIPConfigurationPropertiesFormat{
				Subnet: &armnetwork.SubResource{ID: &f.otherSubnet},
			},
		})

	_, err = f.putGateway(t, "agw3", bad)
	assert.Equal(t, "InvalidRequestFormat", sdkErrCode(err))
}

func TestNetworkSDK_AppGatewayNICPoolMembership(t *testing.T) {
	t.Parallel()

	f := newAGWFixture(t)
	ctx := t.Context()

	_, err := f.putGateway(t, agwName, f.gateway(agwName))
	require.NoError(t, err)

	poolID := agwChild(agwName, "backendAddressPools", "pool")
	nicWithPool := func(name, pool string) (armnetwork.Interface, error) {
		p, perr := f.net.NewInterfacesClient().BeginCreateOrUpdate(ctx, netRG, name, armnetwork.Interface{
			Location: new("westus"),
			Properties: &armnetwork.InterfacePropertiesFormat{
				IPConfigurations: []*armnetwork.InterfaceIPConfiguration{{
					Name: new("ipconfig1"),
					Properties: &armnetwork.InterfaceIPConfigurationPropertiesFormat{
						Subnet: &armnetwork.Subnet{ID: &f.otherSubnet},
						ApplicationGatewayBackendAddressPools: []*armnetwork.ApplicationGatewayBackendAddressPool{
							{ID: &pool},
						},
					},
				}},
			},
		}, nil)
		if perr != nil {
			return armnetwork.Interface{}, perr
		}

		res, perr := p.PollUntilDone(ctx, pollFast())

		return res.Interface, perr
	}

	_, err = nicWithPool("nic-missing", agwChild(agwName, "backendAddressPools", "missing"))
	assert.Equal(t, "InvalidResourceReference", sdkErrCode(err))

	nic, err := nicWithPool("nic1", poolID)
	require.NoError(t, err)
	assert.Equal(t, poolID,
		*nic.Properties.IPConfigurations[0].Properties.ApplicationGatewayBackendAddressPools[0].ID)

	gw, err := f.net.NewApplicationGatewaysClient().Get(ctx, netRG, agwName, nil)
	require.NoError(t, err)

	members := gw.Properties.BackendAddressPools[0].Properties.BackendIPConfigurations
	require.Len(t, members, 1)
	assert.Contains(t, *members[0].ID, "networkInterfaces/nic1/ipConfigurations/ipconfig1")

	del, err := f.net.NewInterfacesClient().BeginDelete(ctx, netRG, "nic1", nil)
	require.NoError(t, err)
	_, err = del.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)

	gw, err = f.net.NewApplicationGatewaysClient().Get(ctx, netRG, agwName, nil)
	require.NoError(t, err)
	assert.Empty(t, gw.Properties.BackendAddressPools[0].Properties.BackendIPConfigurations)

	// Deleting the gateway drops the membership from the NIC.
	_, err = nicWithPool("nic2", poolID)
	require.NoError(t, err)

	d2, err := f.net.NewApplicationGatewaysClient().BeginDelete(ctx, netRG, agwName, nil)
	require.NoError(t, err)
	_, err = d2.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)

	got, err := f.net.NewInterfacesClient().Get(ctx, netRG, "nic2", nil)
	require.NoError(t, err)
	assert.Empty(t, got.Properties.IPConfigurations[0].Properties.ApplicationGatewayBackendAddressPools)
}

func selfSignedPFX(t *testing.T, password string) (string, string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "agw.example.com"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)

	leaf, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	pfx, err := pkcs12.Modern.Encode(key, leaf, nil, password)
	require.NoError(t, err)

	return base64.StdEncoding.EncodeToString(pfx), base64.StdEncoding.EncodeToString(der)
}

func TestNetworkSDK_AppGatewaySSLCertificate(t *testing.T) {
	t.Parallel()

	data, wantPublic := selfSignedPFX(t, "pfx-password")

	tests := []struct {
		cert     armnetwork.ApplicationGatewaySSLCertificatePropertiesFormat
		name     string
		wantCode string
	}{
		{
			name: "pfx",
			cert: armnetwork.ApplicationGatewaySSLCertificatePropertiesFormat{
				Data:     &data,
				Password: new("pfx-password"),
			},
		},
		{
			name: "wrong password", wantCode: "InvalidRequestFormat",
			cert: armnetwork.ApplicationGatewaySSLCertificatePropertiesFormat{Data: &data, Password: new("nope")},
		},
		{
			name: "not base64", wantCode: "InvalidRequestFormat",
			cert: armnetwork.ApplicationGatewaySSLCertificatePropertiesFormat{Data: new("%%%")},
		},
		{
			name: "data and key vault", wantCode: "InvalidRequestFormat",
			cert: armnetwork.ApplicationGatewaySSLCertificatePropertiesFormat{
				Data:             &data,
				KeyVaultSecretID: new("https://kv/secret"),
			},
		},
		{
			name: "key vault",
			cert: armnetwork.ApplicationGatewaySSLCertificatePropertiesFormat{
				KeyVaultSecretID: new("https://kv/secret"),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newAGWFixture(t)
			gw := f.gateway(agwName)
			gw.Properties.SSLCertificates = []*armnetwork.ApplicationGatewaySSLCertificate{
				{Name: new("cert"), Properties: &tc.cert},
			}
			gw.Properties.FrontendPorts = append(
				gw.Properties.FrontendPorts,
				&armnetwork.ApplicationGatewayFrontendPort{
					Name: new(
						"port443",
					), Properties: &armnetwork.ApplicationGatewayFrontendPortPropertiesFormat{Port: new(int32(443))},
				},
			)
			gw.Properties.HTTPListeners[0].Properties.Protocol = new(armnetwork.ApplicationGatewayProtocolHTTPS)
			gw.Properties.HTTPListeners[0].Properties.FrontendPort = agwRef(agwName, "frontendPorts", "port443")
			gw.Properties.HTTPListeners[0].Properties.SSLCertificate = agwRef(agwName, "sslCertificates", "cert")

			_, err := f.putGateway(t, agwName, gw)
			if tc.wantCode != "" {
				assert.Equal(t, tc.wantCode, sdkErrCode(err))

				return
			}

			require.NoError(t, err)

			resp := f.rawDo(t, http.MethodGet, "/subscriptions/"+aciSub+"/resourceGroups/"+netRG+
				"/providers/Microsoft.Network/applicationGateways/"+agwName+"?api-version=2025-03-01", "")
			raw, rerr := io.ReadAll(resp.Body)
			require.NoError(t, rerr)

			assert.NotContains(t, string(raw), `"data"`)
			assert.NotContains(t, string(raw), "pfx-password")
			assert.NotContains(t, string(raw), `"password"`)

			var parsed struct {
				Properties struct {
					SSLCertificates []struct {
						Properties map[string]any `json:"properties"`
					} `json:"sslCertificates"`
				} `json:"properties"`
			}
			require.NoError(t, json.Unmarshal(raw, &parsed))

			props := parsed.Properties.SSLCertificates[0].Properties
			if tc.cert.Data != nil {
				assert.Equal(t, wantPublic, props["publicCertData"])
			} else {
				assert.Equal(t, "https://kv/secret", props["keyVaultSecretId"])
			}

			// Re-sending the body without data keeps the certificate.
			again, err := f.putGateway(t, agwName, f.gateway(agwName))
			require.NoError(t, err)
			assert.NotNil(t, again.Properties)
		})
	}
}

func (f *agwFixture) wafPolicy(t *testing.T, name string, mode armnetwork.WebApplicationFirewallMode) error {
	t.Helper()

	_, err := f.net.NewWebApplicationFirewallPoliciesClient().CreateOrUpdate(t.Context(), netRG, name,
		armnetwork.WebApplicationFirewallPolicy{
			Location: new("westus"),
			Properties: &armnetwork.WebApplicationFirewallPolicyPropertiesFormat{
				PolicySettings: &armnetwork.PolicySettings{Mode: &mode},
				ManagedRules: &armnetwork.ManagedRulesDefinition{ManagedRuleSets: []*armnetwork.ManagedRuleSet{{
					RuleSetType: new("OWASP"), RuleSetVersion: new("3.2"),
				}}},
				CustomRules: []*armnetwork.WebApplicationFirewallCustomRule{{
					Name: new("block-bad"), Priority: new(int32(10)),
					RuleType: new(armnetwork.WebApplicationFirewallRuleTypeMatchRule),
					Action:   new(armnetwork.WebApplicationFirewallActionBlock),
					MatchConditions: []*armnetwork.MatchCondition{{
						MatchVariables: []*armnetwork.MatchVariable{
							{VariableName: new(armnetwork.WebApplicationFirewallMatchVariableRemoteAddr)},
						},
						Operator:    new(armnetwork.WebApplicationFirewallOperatorIPMatch),
						MatchValues: []*string{new("203.0.113.0/24")},
					}},
				}},
			},
		}, nil)

	return err
}

func TestNetworkSDK_AppGatewayWAFPolicy(t *testing.T) {
	t.Parallel()

	f := newAGWFixture(t)
	ctx := t.Context()
	pc := f.net.NewWebApplicationFirewallPoliciesClient()

	require.NoError(t, f.wafPolicy(t, "waf1", armnetwork.WebApplicationFirewallModePrevention))

	pol, err := pc.Get(ctx, netRG, "waf1", nil)
	require.NoError(t, err)
	assert.Equal(t, armnetwork.WebApplicationFirewallModePrevention, *pol.Properties.PolicySettings.Mode)
	assert.Equal(t, "3.2", *pol.Properties.ManagedRules.ManagedRuleSets[0].RuleSetVersion)

	_, err = pc.CreateOrUpdate(ctx, netRG, "bad", armnetwork.WebApplicationFirewallPolicy{
		Location: new("westus"), Properties: &armnetwork.WebApplicationFirewallPolicyPropertiesFormat{
			ManagedRules: &armnetwork.ManagedRulesDefinition{},
		},
	}, nil)
	assert.Equal(t, "InvalidRequestFormat", sdkErrCode(err))

	polID := *pol.ID

	// Standard_v2 cannot take a policy; WAF_v2 can.
	std := f.gateway(agwName)
	std.Properties.FirewallPolicy = &armnetwork.SubResource{ID: &polID}

	_, err = f.putGateway(t, agwName, std)
	assert.Equal(t, "ApplicationGatewayFirewallPolicyRequiresWafSku", sdkErrCode(err))

	waf := f.gateway(agwName)
	waf.Properties.SKU = &armnetwork.ApplicationGatewaySKU{
		Name: new(armnetwork.ApplicationGatewaySKUNameWAFV2), Tier: new(armnetwork.ApplicationGatewayTierWAFV2),
		Capacity: new(int32(2)),
	}
	waf.Properties.FirewallPolicy = &armnetwork.SubResource{ID: &polID}

	got, err := f.putGateway(t, agwName, waf)
	require.NoError(t, err)
	assert.Equal(t, polID, *got.Properties.FirewallPolicy.ID)

	pol, err = pc.Get(ctx, netRG, "waf1", nil)
	require.NoError(t, err)
	require.Len(t, pol.Properties.ApplicationGateways, 1)
	assert.Contains(t, *pol.Properties.ApplicationGateways[0].ID, "applicationGateways/agw1")

	del, err := pc.BeginDelete(ctx, netRG, "waf1", nil)
	if err == nil {
		_, err = del.PollUntilDone(ctx, pollFast())
	}

	assert.Equal(t, "ApplicationGatewayFirewallPolicyInUse", sdkErrCode(err))

	// Removing the association frees the policy.
	_, err = f.putGateway(t, agwName, f.gateway(agwName))
	require.NoError(t, err)

	del, err = pc.BeginDelete(ctx, netRG, "waf1", nil)
	require.NoError(t, err)
	_, err = del.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)
}

func TestNetworkSDK_AppGatewayStartStop(t *testing.T) {
	t.Parallel()

	f := newAGWFixture(t)
	ctx := t.Context()
	c := f.net.NewApplicationGatewaysClient()

	_, err := f.putGateway(t, agwName, f.gateway(agwName))
	require.NoError(t, err)

	state := func() armnetwork.ApplicationGatewayOperationalState {
		g, gerr := c.Get(ctx, netRG, agwName, nil)
		require.NoError(t, gerr)

		return *g.Properties.OperationalState
	}

	stop, err := c.BeginStop(ctx, netRG, agwName, nil)
	require.NoError(t, err)
	_, err = stop.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)
	assert.Equal(t, armnetwork.ApplicationGatewayOperationalStateStopped, state())

	health, err := c.BeginBackendHealth(ctx, netRG, agwName, nil)
	if err == nil {
		_, err = health.PollUntilDone(ctx, pollFast())
	}

	assert.Equal(t, "ApplicationGatewayNotRunning", sdkErrCode(err))

	// A write to a stopped gateway keeps it stopped.
	_, err = f.putGateway(t, agwName, f.gateway(agwName))
	require.NoError(t, err)
	assert.Equal(t, armnetwork.ApplicationGatewayOperationalStateStopped, state())

	start, err := c.BeginStart(ctx, netRG, agwName, nil)
	require.NoError(t, err)
	_, err = start.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)
	assert.Equal(t, armnetwork.ApplicationGatewayOperationalStateRunning, state())

	_, err = c.BeginStart(ctx, netRG, "missing", nil)
	assert.Equal(t, "ResourceNotFound", sdkErrCode(err))
}

func TestNetworkSDK_AppGatewayBackendHealth(t *testing.T) {
	t.Parallel()

	var status atomicInt

	status.set(http.StatusOK)

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" {
			w.WriteHeader(http.StatusNotFound)

			return
		}

		w.WriteHeader(status.get())
	}))
	t.Cleanup(backend.Close)

	u, err := url.Parse(backend.URL)
	require.NoError(t, err)

	port, err := strconv.Atoi(u.Port())
	require.NoError(t, err)

	f := newAGWFixture(t)
	gw := f.gateway(agwName)
	gw.Properties.BackendAddressPools[0].Properties.BackendAddresses = []*armnetwork.ApplicationGatewayBackendAddress{
		{IPAddress: new(u.Hostname())}, {IPAddress: new("10.0.2.10")},
	}
	gw.Properties.BackendHTTPSettingsCollection[0].Properties.Port = new(int32(port))
	gw.Properties.BackendHTTPSettingsCollection[0].Properties.Probe = agwRef(agwName, "probes", "probe")
	gw.Properties.Probes = []*armnetwork.ApplicationGatewayProbe{{
		Name: new("probe"),
		Properties: &armnetwork.ApplicationGatewayProbePropertiesFormat{
			Protocol: new(armnetwork.ApplicationGatewayProtocolHTTP), Path: new("/healthz"), Host: new("localhost"),
			Match: &armnetwork.ApplicationGatewayProbeHealthResponseMatch{StatusCodes: []*string{new("200-299")}},
		},
	}}

	_, err = f.putGateway(t, agwName, gw)
	require.NoError(t, err)

	tests := []struct {
		name       string
		wantHealth armnetwork.ApplicationGatewayBackendHealthServerHealth
		code       int
	}{
		{name: "up", code: http.StatusOK, wantHealth: armnetwork.ApplicationGatewayBackendHealthServerHealthUp},
		{
			name:       "down",
			code:       http.StatusInternalServerError,
			wantHealth: armnetwork.ApplicationGatewayBackendHealthServerHealthDown,
		},
	}

	for _, tc := range tests {
		status.set(tc.code)

		p, perr := f.net.NewApplicationGatewaysClient().BeginBackendHealth(t.Context(), netRG, agwName, nil)
		require.NoError(t, perr, tc.name)

		res, perr := p.PollUntilDone(t.Context(), pollFast())
		require.NoError(t, perr, tc.name)

		servers := res.BackendAddressPools[0].BackendHTTPSettingsCollection[0].Servers
		require.Len(t, servers, 2, tc.name)
		assert.Equal(t, tc.wantHealth, *servers[0].Health, tc.name)
		assert.Equal(t, armnetwork.ApplicationGatewayBackendHealthServerHealthUnknown, *servers[1].Health, tc.name)
		assert.Equal(
			t,
			agwChild(agwName, "backendAddressPools", "pool"),
			*res.BackendAddressPools[0].BackendAddressPool.ID,
		)
	}
}

func TestNetworkSDK_AppGatewayPersistence(t *testing.T) {
	t.Parallel()

	f := newAGWFixture(t)
	data, pub := selfSignedPFX(t, "pw")

	gw := f.gateway(agwName)
	gw.Properties.SSLCertificates = []*armnetwork.ApplicationGatewaySSLCertificate{{
		Name: new(
			"cert",
		), Properties: &armnetwork.ApplicationGatewaySSLCertificatePropertiesFormat{Data: &data, Password: new("pw")},
	}}

	_, err := f.putGateway(t, agwName, gw)
	require.NoError(t, err)
	require.NoError(t, f.wafPolicy(t, "waf1", armnetwork.WebApplicationFirewallModeDetection))

	stopped, err := f.net.NewApplicationGatewaysClient().BeginStop(t.Context(), netRG, agwName, nil)
	require.NoError(t, err)
	_, err = stopped.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)

	snap := f.handler.Snapshot(t.Context())
	raw, err := json.Marshal(snap)
	require.NoError(t, err)
	assert.NotContains(t, string(raw), data)

	dst := newNetStack(t)
	require.NoError(t, dst.handler.Restore(t.Context(), snap))

	got, err := dst.net.NewApplicationGatewaysClient().Get(t.Context(), netRG, agwName, nil)
	require.NoError(t, err)
	assert.Equal(t, armnetwork.ApplicationGatewayOperationalStateStopped, *got.Properties.OperationalState)
	assert.Equal(t, pub, *got.Properties.SSLCertificates[0].Properties.PublicCertData)
	assert.Equal(t, "listener", *got.Properties.HTTPListeners[0].Name)

	pol, err := dst.net.NewWebApplicationFirewallPoliciesClient().Get(t.Context(), netRG, "waf1", nil)
	require.NoError(t, err)
	assert.Equal(t, "block-bad", *pol.Properties.CustomRules[0].Name)
}

type atomicInt struct{ v atomic.Int64 }

func (a *atomicInt) set(n int) { a.v.Store(int64(n)) }
func (a *atomicInt) get() int  { return int(a.v.Load()) }
