package azurearm_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/msi/armmsi"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v8"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/privatedns/armprivatedns"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

const netRG = "rg-net"

type netStack struct {
	net     *armnetwork.ClientFactory
	dns     *armprivatedns.ClientFactory
	msi     *armmsi.ClientFactory
	handler *azurearm.Handler
	hc      *http.Client
	url     string
}

func newNetStack(t *testing.T) *netStack {
	t.Helper()

	svc, err := (&azurearm.Provider{}).Init(&service.AppContext{})
	require.NoError(t, err)

	h, ok := svc.(*azurearm.Handler)
	require.True(t, ok)

	t.Cleanup(func() { h.Shutdown(context.Background()) })

	e := echo.New()
	e.Any("/*", h.Handler())

	srv := httptest.NewTLSServer(e)
	t.Cleanup(srv.Close)

	opts := &arm.ClientOptions{
		Cloud: cloud.Configuration{Services: map[cloud.ServiceName]cloud.ServiceConfiguration{
			cloud.ResourceManager: {Endpoint: srv.URL, Audience: "https://management.core.windows.net/"},
		}},
		Transport: srv.Client(),
		Retry:     policy.RetryOptions{MaxRetries: -1},
	}

	nf, err := armnetwork.NewClientFactory(aciSub, aciCred{}, opts)
	require.NoError(t, err)

	df, err := armprivatedns.NewClientFactory(aciSub, aciCred{}, opts)
	require.NoError(t, err)

	mf, err := armmsi.NewClientFactory(aciSub, aciCred{}, opts)
	require.NoError(t, err)

	s := &netStack{net: nf, dns: df, msi: mf, handler: h, hc: srv.Client(), url: srv.URL}
	s.rawDo(t, http.MethodPut, "/subscriptions/"+aciSub+"/resourcegroups/"+netRG, `{"location":"westus"}`)

	return s
}

func (s *netStack) rawDo(t *testing.T, method, path, body string) *http.Response {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, s.url+path, strings.NewReader(body))
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/json")

	resp, err := s.hc.Do(req)
	require.NoError(t, err)

	t.Cleanup(func() { _ = resp.Body.Close() })

	return resp
}

func sdkErrCode(err error) string {
	if re, ok := errors.AsType[*azcore.ResponseError](err); ok {
		return re.ErrorCode
	}

	return ""
}

func (s *netStack) vnet(
	t *testing.T,
	name string,
	prefixes []string,
	subnets map[string]string,
) armnetwork.VirtualNetwork {
	t.Helper()

	v := armnetwork.VirtualNetwork{
		Location:   new("westus"),
		Properties: &armnetwork.VirtualNetworkPropertiesFormat{AddressSpace: &armnetwork.AddressSpace{}},
	}

	for _, p := range prefixes {
		v.Properties.AddressSpace.AddressPrefixes = append(v.Properties.AddressSpace.AddressPrefixes, new(p))
	}

	for n, p := range subnets {
		v.Properties.Subnets = append(v.Properties.Subnets, &armnetwork.Subnet{
			Name: new(n), Properties: &armnetwork.SubnetPropertiesFormat{AddressPrefix: new(p)},
		})
	}

	poller, err := s.net.NewVirtualNetworksClient().BeginCreateOrUpdate(t.Context(), netRG, name, v, nil)
	require.NoError(t, err)

	res, err := poller.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)

	return res.VirtualNetwork
}

func (s *netStack) nic(t *testing.T, name, subnetID string, method armnetwork.IPAllocationMethod, addr string,
	extra *armnetwork.InterfaceIPConfigurationPropertiesFormat,
) (armnetwork.Interface, error) {
	t.Helper()

	props := &armnetwork.InterfaceIPConfigurationPropertiesFormat{
		Subnet:                    &armnetwork.Subnet{ID: new(subnetID)},
		PrivateIPAllocationMethod: new(method),
	}

	if addr != "" {
		props.PrivateIPAddress = new(addr)
	}

	if extra != nil {
		props.PublicIPAddress = extra.PublicIPAddress
	}

	poller, err := s.net.NewInterfacesClient().BeginCreateOrUpdate(t.Context(), netRG, name, armnetwork.Interface{
		Location: new("westus"),
		Properties: &armnetwork.InterfacePropertiesFormat{
			IPConfigurations: []*armnetwork.InterfaceIPConfiguration{{Name: new("ipconfig1"), Properties: props}},
		},
	}, nil)
	if err != nil {
		return armnetwork.Interface{}, err
	}

	res, err := poller.PollUntilDone(t.Context(), pollFast())

	return res.Interface, err
}

func TestNetworkSDK_VNetSubnetValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		subnets  map[string]string
		name     string
		wantCode string
		prefixes []string
	}{
		{
			name:     "valid",
			prefixes: []string{"10.0.0.0/16"},
			subnets:  map[string]string{"a": "10.0.1.0/24", "b": "10.0.2.0/24"},
		},
		{name: "empty space", prefixes: nil, wantCode: "InvalidRequestFormat"},
		{name: "bad cidr", prefixes: []string{"10.0.0.5/16"}, wantCode: "InvalidCIDRNotation"},
		{name: "overlapping space", prefixes: []string{"10.0.0.0/16", "10.0.1.0/24"},
			wantCode: "AddressSpaceOverlap"},
		{
			name: "subnet outside space", prefixes: []string{"10.0.0.0/16"},
			subnets: map[string]string{"a": "10.1.0.0/24"}, wantCode: "NetcfgInvalidSubnet",
		},
		{
			name: "subnet too small", prefixes: []string{"10.0.0.0/16"},
			subnets: map[string]string{"a": "10.0.0.0/30"}, wantCode: "NetcfgInvalidSubnet",
		},
		{
			name: "subnets overlap", prefixes: []string{"10.0.0.0/16"},
			subnets: map[string]string{"a": "10.0.0.0/24",
				"b": "10.0.0.128/25"}, wantCode: "NetcfgSubnetRangesOverlap",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newNetStack(t)
			v := armnetwork.VirtualNetwork{
				Location:   new("westus"),
				Properties: &armnetwork.VirtualNetworkPropertiesFormat{AddressSpace: &armnetwork.AddressSpace{}},
			}

			for _, p := range tt.prefixes {
				v.Properties.AddressSpace.AddressPrefixes = append(v.Properties.AddressSpace.AddressPrefixes, new(p))
			}

			for n, p := range tt.subnets {
				v.Properties.Subnets = append(v.Properties.Subnets, &armnetwork.Subnet{
					Name: new(n), Properties: &armnetwork.SubnetPropertiesFormat{AddressPrefix: new(p)},
				})
			}

			_, err := s.net.NewVirtualNetworksClient().BeginCreateOrUpdate(t.Context(), netRG, "vnet1", v, nil)
			if tt.wantCode != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, sdkErrCode(err))

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestNetworkSDK_SubnetChild(t *testing.T) {
	t.Parallel()

	s := newNetStack(t)
	v := s.vnet(t, "vnet1", []string{"10.0.0.0/16"}, map[string]string{"a": "10.0.1.0/24"})
	require.Len(t, v.Properties.Subnets, 1)
	assert.Equal(t, "Disabled", string(*v.Properties.Subnets[0].Properties.PrivateEndpointNetworkPolicies))
	assert.Equal(t, "Enabled", string(*v.Properties.Subnets[0].Properties.PrivateLinkServiceNetworkPolicies))

	sc := s.net.NewSubnetsClient()

	tests := []struct {
		name     string
		prefix   string
		wantCode string
	}{
		{name: "b", prefix: "10.0.2.0/24"},
		{name: "outside", prefix: "192.168.0.0/24", wantCode: "NetcfgInvalidSubnet"},
		{name: "overlap", prefix: "10.0.1.128/25", wantCode: "NetcfgSubnetRangesOverlap"},
	}

	for _, tt := range tests {
		poller, err := sc.BeginCreateOrUpdate(t.Context(), netRG, "vnet1", tt.name, armnetwork.Subnet{
			Properties: &armnetwork.SubnetPropertiesFormat{
				AddressPrefix: new(tt.prefix),
				Delegations: []*armnetwork.Delegation{
					{
						Name: new("d"),
						Properties: &armnetwork.ServiceDelegationPropertiesFormat{
							ServiceName: new("Microsoft.Web/serverFarms"),
						},
					},
				},
				ServiceEndpoints: []*armnetwork.ServiceEndpointPropertiesFormat{{Service: new("Microsoft.Storage")}},
			},
		}, nil)

		if tt.wantCode != "" {
			require.Error(t, err, tt.name)
			assert.Equal(t, tt.wantCode, sdkErrCode(err), tt.name)

			continue
		}

		require.NoError(t, err)

		res, err := poller.PollUntilDone(t.Context(), pollFast())
		require.NoError(t, err)

		sp := res.Properties
		require.Len(t, sp.Delegations, 1)
		assert.Equal(
			t,
			[]*string{new("Microsoft.Network/virtualNetworks/subnets/action")},
			sp.Delegations[0].Properties.Actions,
		)
		require.Len(t, sp.ServiceEndpoints, 1)
		assert.Equal(t, "westus", *sp.ServiceEndpoints[0].Locations[0])
	}

	vn, err := s.net.NewVirtualNetworksClient().Get(t.Context(), netRG, "vnet1", nil)
	require.NoError(t, err)
	assert.Len(t, vn.Properties.Subnets, 2)

	got, err := sc.Get(t.Context(), netRG, "vnet1", "b", nil)
	require.NoError(t, err)
	assert.Equal(t, "10.0.2.0/24", *got.Properties.AddressPrefix)
}

func TestNetworkSDK_NICAddressAllocation(t *testing.T) {
	t.Parallel()

	s := newNetStack(t)
	v := s.vnet(t, "vnet1", []string{"10.0.0.0/16"}, map[string]string{"a": "10.0.1.0/24"})
	sid := *v.Properties.Subnets[0].ID

	tests := []struct {
		name     string
		method   armnetwork.IPAllocationMethod
		addr     string
		wantIP   string
		wantCode string
	}{
		{name: "dyn1", method: armnetwork.IPAllocationMethodDynamic, wantIP: "10.0.1.4"},
		{name: "dyn2", method: armnetwork.IPAllocationMethodDynamic, wantIP: "10.0.1.5"},
		{name: "static ok", method: armnetwork.IPAllocationMethodStatic, addr: "10.0.1.100", wantIP: "10.0.1.100"},
		{
			name:     "reserved low",
			method:   armnetwork.IPAllocationMethodStatic,
			addr:     "10.0.1.3",
			wantCode: "PrivateIPAddressIsReserved",
		},
		{
			name:     "reserved gw",
			method:   armnetwork.IPAllocationMethodStatic,
			addr:     "10.0.1.1",
			wantCode: "PrivateIPAddressIsReserved",
		},
		{
			name:     "reserved last",
			method:   armnetwork.IPAllocationMethodStatic,
			addr:     "10.0.1.255",
			wantCode: "PrivateIPAddressIsReserved",
		},
		{
			name:     "taken",
			method:   armnetwork.IPAllocationMethodStatic,
			addr:     "10.0.1.4",
			wantCode: "PrivateIPAddressIsAllocated",
		},
		{
			name:     "outside",
			method:   armnetwork.IPAllocationMethodStatic,
			addr:     "10.0.2.9",
			wantCode: "PrivateIPAddressNotInSubnet",
		},
		{name: "last usable", method: armnetwork.IPAllocationMethodStatic, addr: "10.0.1.254", wantIP: "10.0.1.254"},
	}

	// sequential: each case builds on the allocations of the earlier ones
	for _, tt := range tests {
		nic, err := s.nic(t, strings.ReplaceAll(tt.name, " ", "-"), sid, tt.method, tt.addr, nil)
		if tt.wantCode != "" {
			require.Error(t, err, tt.name)
			assert.Equal(t, tt.wantCode, sdkErrCode(err), tt.name)

			continue
		}

		require.NoError(t, err, tt.name)

		cfg := nic.Properties.IPConfigurations[0].Properties
		assert.Equal(t, tt.wantIP, *cfg.PrivateIPAddress, tt.name)
		assert.Regexp(t, `^00-0D-3A-`, *nic.Properties.MacAddress)
		assert.True(t, *cfg.Primary)
	}

	// update keeps the dynamic address; delete frees it for the next allocation
	again, err := s.nic(t, "dyn1", sid, armnetwork.IPAllocationMethodDynamic, "", nil)
	require.NoError(t, err)
	assert.Equal(t, "10.0.1.4", *again.Properties.IPConfigurations[0].Properties.PrivateIPAddress)

	del, err := s.net.NewInterfacesClient().BeginDelete(t.Context(), netRG, "dyn1", nil)
	require.NoError(t, err)
	_, err = del.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)

	reuse, err := s.nic(t, "dyn3", sid, armnetwork.IPAllocationMethodDynamic, "", nil)
	require.NoError(t, err)
	assert.Equal(t, "10.0.1.4", *reuse.Properties.IPConfigurations[0].Properties.PrivateIPAddress)

	avail, err := s.net.NewVirtualNetworksClient().
		CheckIPAddressAvailability(t.Context(), netRG, "vnet1", "10.0.1.5", nil)
	require.NoError(t, err)
	assert.False(t, *avail.Available)
	assert.NotEmpty(t, avail.AvailableIPAddresses)

	avail, err = s.net.NewVirtualNetworksClient().
		CheckIPAddressAvailability(t.Context(), netRG, "vnet1", "10.0.1.200", nil)
	require.NoError(t, err)
	assert.True(t, *avail.Available)

	_, err = s.net.NewVirtualNetworksClient().CheckIPAddressAvailability(t.Context(), netRG, "vnet1",
		"172.16.0.1", nil)
	assert.Equal(t, "PrivateIPAddressNotInAnySubnet", sdkErrCode(err))
}

func TestNetworkSDK_PublicIP(t *testing.T) {
	t.Parallel()

	s := newNetStack(t)
	pc := s.net.NewPublicIPAddressesClient()

	create := func(name string, sku armnetwork.PublicIPAddressSKUName,
		m armnetwork.IPAllocationMethod) (armnetwork.PublicIPAddress, error) {
		p, err := pc.BeginCreateOrUpdate(t.Context(), netRG, name, armnetwork.PublicIPAddress{
			Location: new("westus"),
			SKU:      &armnetwork.PublicIPAddressSKU{Name: new(sku)},
			Properties: &armnetwork.PublicIPAddressPropertiesFormat{
				PublicIPAllocationMethod: new(m),
				DNSSettings:              &armnetwork.PublicIPAddressDNSSettings{DomainNameLabel: new(name)},
			},
		}, nil)
		if err != nil {
			return armnetwork.PublicIPAddress{}, err
		}

		res, err := p.PollUntilDone(t.Context(), pollFast())

		return res.PublicIPAddress, err
	}

	s1, err := create("static-one", armnetwork.PublicIPAddressSKUNameStandard, armnetwork.IPAllocationMethodStatic)
	require.NoError(t, err)
	assert.Equal(t, "192.0.2.1", *s1.Properties.IPAddress)
	assert.Equal(t, "static-one.westus.cloudapp.azure.com", *s1.Properties.DNSSettings.Fqdn)
	assert.EqualValues(t, 4, *s1.Properties.IdleTimeoutInMinutes)

	s2, err := create("static-two", armnetwork.PublicIPAddressSKUNameBasic, armnetwork.IPAllocationMethodStatic)
	require.NoError(t, err)
	assert.Equal(t, "192.0.2.2", *s2.Properties.IPAddress)

	dyn, err := create("dyn-one", armnetwork.PublicIPAddressSKUNameBasic, armnetwork.IPAllocationMethodDynamic)
	require.NoError(t, err)
	assert.Nil(t, dyn.Properties.IPAddress)

	_, err = create("std-dyn", armnetwork.PublicIPAddressSKUNameStandard, armnetwork.IPAllocationMethodDynamic)
	assert.Equal(t, "InvalidRequestFormat", sdkErrCode(err))

	_, err = create("static-one", armnetwork.PublicIPAddressSKUNameStandard, armnetwork.IPAllocationMethodStatic)
	require.NoError(t, err, "update keeps its own label")

	_, err = pc.BeginCreateOrUpdate(t.Context(), netRG, "other", armnetwork.PublicIPAddress{
		Location: new("westus"),
		Properties: &armnetwork.PublicIPAddressPropertiesFormat{
			DNSSettings: &armnetwork.PublicIPAddressDNSSettings{DomainNameLabel: new("static-one")},
		},
	}, nil)
	assert.Equal(t, "DnsRecordInUse", sdkErrCode(err))

	// a dynamic address appears only once the IP is attached to a NIC, and goes away on detach
	v := s.vnet(t, "vnet1", []string{"10.0.0.0/16"}, map[string]string{"a": "10.0.1.0/24"})
	sid := *v.Properties.Subnets[0].ID

	nic, err := s.nic(t, "nic1", sid, armnetwork.IPAllocationMethodDynamic, "",
		&armnetwork.InterfaceIPConfigurationPropertiesFormat{PublicIPAddress: &armnetwork.PublicIPAddress{ID: dyn.ID}})
	require.NoError(t, err)
	assert.NotNil(t, nic.Properties.IPConfigurations[0].Properties.PublicIPAddress)

	got, err := pc.Get(t.Context(), netRG, "dyn-one", nil)
	require.NoError(t, err)
	require.NotNil(t, got.Properties.IPAddress)
	assert.Equal(t, "192.0.2.3", *got.Properties.IPAddress)
	assert.Contains(t, *got.Properties.IPConfiguration.ID, "/networkInterfaces/nic1/ipConfigurations/ipconfig1")

	del, err := pc.BeginDelete(t.Context(), netRG, "dyn-one", nil)
	if err == nil {
		_, err = del.PollUntilDone(t.Context(), pollFast())
	}

	assert.Equal(t, "PublicIPAddressCannotBeDeleted", sdkErrCode(err))

	_, err = s.nic(t, "nic1", sid, armnetwork.IPAllocationMethodDynamic, "", nil)
	require.NoError(t, err)

	got, err = pc.Get(t.Context(), netRG, "dyn-one", nil)
	require.NoError(t, err)
	assert.Nil(t, got.Properties.IPAddress)
	assert.Nil(t, got.Properties.IPConfiguration)

	del, err = pc.BeginDelete(t.Context(), netRG, "dyn-one", nil)
	require.NoError(t, err)
	_, err = del.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)
}

func TestNetworkSDK_DefaultSecurityRules(t *testing.T) {
	t.Parallel()

	s := newNetStack(t)
	nc := s.net.NewSecurityGroupsClient()

	rule := func(name string, prio int32, dir armnetwork.SecurityRuleDirection) *armnetwork.SecurityRule {
		return &armnetwork.SecurityRule{
			Name: new(name),
			Properties: &armnetwork.SecurityRulePropertiesFormat{
				Protocol: new(armnetwork.SecurityRuleProtocolTCP), Access: new(armnetwork.SecurityRuleAccessAllow),
				Direction: new(dir), Priority: new(prio),
				SourceAddressPrefix: new("*"), SourcePortRange: new("*"),
				DestinationAddressPrefix: new("*"), DestinationPortRange: new("80-443"),
			},
		}
	}

	poller, err := nc.BeginCreateOrUpdate(t.Context(), netRG, "nsg1", armnetwork.SecurityGroup{
		Location: new("westus"),
		Properties: &armnetwork.SecurityGroupPropertiesFormat{
			SecurityRules: []*armnetwork.SecurityRule{rule("web", 100, armnetwork.SecurityRuleDirectionInbound)},
		},
	}, nil)
	require.NoError(t, err)

	res, err := poller.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)

	defs := res.Properties.DefaultSecurityRules
	require.Len(t, defs, 6)

	want := map[string]int32{
		"AllowVnetInBound": 65000, "AllowAzureLoadBalancerInBound": 65001, "DenyAllInBound": 65500,
		"AllowVnetOutBound": 65000, "AllowInternetOutBound": 65001, "DenyAllOutBound": 65500,
	}

	for _, d := range defs {
		assert.Equal(t, want[*d.Name], *d.Properties.Priority, *d.Name)
		assert.Contains(t, *d.ID, "/defaultSecurityRules/"+*d.Name)
	}

	one, err := s.net.NewDefaultSecurityRulesClient().Get(t.Context(), netRG, "nsg1", "DenyAllInBound", nil)
	require.NoError(t, err)
	assert.Equal(t, armnetwork.SecurityRuleAccessDeny, *one.Properties.Access)

	pager := s.net.NewDefaultSecurityRulesClient().NewListPager(netRG, "nsg1", nil)
	page, err := pager.NextPage(t.Context())
	require.NoError(t, err)
	assert.Len(t, page.Value, 6)

	rc := s.net.NewSecurityRulesClient()

	tests := []struct {
		name     string
		rule     *armnetwork.SecurityRule
		wantCode string
	}{
		{name: "ok", rule: rule("ssh", 110, armnetwork.SecurityRuleDirectionInbound)},
		{name: "same prio other dir", rule: rule("out", 100, armnetwork.SecurityRuleDirectionOutbound)},
		{
			name:     "conflict",
			rule:     rule("dup", 100, armnetwork.SecurityRuleDirectionInbound),
			wantCode: "SecurityRuleConflict",
		},
		{
			name:     "low prio",
			rule:     rule("low", 99, armnetwork.SecurityRuleDirectionInbound),
			wantCode: "SecurityRuleInvalidPriority",
		},
		{
			name:     "high prio",
			rule:     rule("high", 4097, armnetwork.SecurityRuleDirectionInbound),
			wantCode: "SecurityRuleInvalidPriority",
		},
	}

	for _, tt := range tests {
		p, cerr := rc.BeginCreateOrUpdate(t.Context(), netRG, "nsg1", *tt.rule.Name, *tt.rule, nil)
		if tt.wantCode != "" {
			require.Error(t, cerr, tt.name)
			assert.Equal(t, tt.wantCode, sdkErrCode(cerr), tt.name)

			continue
		}

		require.NoError(t, cerr, tt.name)
		_, cerr = p.PollUntilDone(t.Context(), pollFast())
		require.NoError(t, cerr, tt.name)
	}

	bad := rule("ports", 200, armnetwork.SecurityRuleDirectionInbound)
	bad.Properties.DestinationPortRange = new("99999")
	_, err = rc.BeginCreateOrUpdate(t.Context(), netRG, "nsg1", "ports", *bad, nil)
	assert.Equal(t, "SecurityRuleInvalidPortRange", sdkErrCode(err))

	got, err := nc.Get(t.Context(), netRG, "nsg1", nil)
	require.NoError(t, err)
	assert.Len(t, got.Properties.SecurityRules, 3)
}

func TestNetworkSDK_InUseDeletion(t *testing.T) {
	t.Parallel()

	s := newNetStack(t)
	ctx := t.Context()

	nsgPoller, err := s.net.NewSecurityGroupsClient().BeginCreateOrUpdate(ctx, netRG, "nsg1",
		armnetwork.SecurityGroup{Location: new("westus")}, nil)
	require.NoError(t, err)
	nsg, err := nsgPoller.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)

	rtPoller, err := s.net.NewRouteTablesClient().BeginCreateOrUpdate(ctx, netRG, "rt1", armnetwork.RouteTable{
		Location: new("westus"),
		Properties: &armnetwork.RouteTablePropertiesFormat{Routes: []*armnetwork.Route{{
			Name: new("r1"), Properties: &armnetwork.RoutePropertiesFormat{
				AddressPrefix: new("0.0.0.0/0"), NextHopType: new(armnetwork.RouteNextHopTypeVirtualAppliance),
				NextHopIPAddress: new("10.0.9.4"),
			},
		}}},
	}, nil)
	require.NoError(t, err)
	rt, err := rtPoller.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)

	v := s.vnet(t, "vnet1", []string{"10.0.0.0/16"}, nil)
	_ = v

	sp, err := s.net.NewSubnetsClient().BeginCreateOrUpdate(ctx, netRG, "vnet1", "a", armnetwork.Subnet{
		Properties: &armnetwork.SubnetPropertiesFormat{
			AddressPrefix:        new("10.0.1.0/24"),
			NetworkSecurityGroup: &armnetwork.SecurityGroup{ID: nsg.ID},
			RouteTable:           &armnetwork.RouteTable{ID: rt.ID},
		},
	}, nil)
	require.NoError(t, err)
	sub, err := sp.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)

	gotNSG, err := s.net.NewSecurityGroupsClient().Get(ctx, netRG, "nsg1", nil)
	require.NoError(t, err)
	require.Len(t, gotNSG.Properties.Subnets, 1)

	gotRT, err := s.net.NewRouteTablesClient().Get(ctx, netRG, "rt1", nil)
	require.NoError(t, err)
	require.Len(t, gotRT.Properties.Subnets, 1)
	assert.False(t, *gotRT.Properties.DisableBgpRoutePropagation)

	// dangling references are rejected
	_, err = s.net.NewSubnetsClient().BeginCreateOrUpdate(ctx, netRG, "vnet1", "b", armnetwork.Subnet{
		Properties: &armnetwork.SubnetPropertiesFormat{
			AddressPrefix:        new("10.0.2.0/24"),
			NetworkSecurityGroup: &armnetwork.SecurityGroup{ID: new(*nsg.ID + "-missing")},
		},
	}, nil)
	assert.Equal(t, "InvalidResourceReference", sdkErrCode(err))

	delNSG := func() error {
		p, derr := s.net.NewSecurityGroupsClient().BeginDelete(ctx, netRG, "nsg1", nil)
		if derr != nil {
			return derr
		}

		_, derr = p.PollUntilDone(ctx, pollFast())

		return derr
	}
	delRT := func() error {
		p, derr := s.net.NewRouteTablesClient().BeginDelete(ctx, netRG, "rt1", nil)
		if derr != nil {
			return derr
		}

		_, derr = p.PollUntilDone(ctx, pollFast())

		return derr
	}

	assert.Equal(t, "InUseNetworkSecurityGroupCannotBeDeleted", sdkErrCode(delNSG()))
	assert.Equal(t, "InUseRouteTableCannotBeDeleted", sdkErrCode(delRT()))

	nic, err := s.nic(t, "nic1", *sub.ID, armnetwork.IPAllocationMethodDynamic, "", nil)
	require.NoError(t, err)

	delSubnet := func() error {
		p, derr := s.net.NewSubnetsClient().BeginDelete(ctx, netRG, "vnet1", "a", nil)
		if derr != nil {
			return derr
		}

		_, derr = p.PollUntilDone(ctx, pollFast())

		return derr
	}
	delVNet := func() error {
		p, derr := s.net.NewVirtualNetworksClient().BeginDelete(ctx, netRG, "vnet1", nil)
		if derr != nil {
			return derr
		}

		_, derr = p.PollUntilDone(ctx, pollFast())

		return derr
	}

	assert.Equal(t, "InUseSubnetCannotBeDeleted", sdkErrCode(delSubnet()))
	assert.Equal(t, "InUseSubnetCannotBeDeleted", sdkErrCode(delVNet()))

	gotSub, err := s.net.NewSubnetsClient().Get(ctx, netRG, "vnet1", "a", nil)
	require.NoError(t, err)
	require.Len(t, gotSub.Properties.IPConfigurations, 1)
	assert.Equal(t, *nic.Properties.IPConfigurations[0].ID, *gotSub.Properties.IPConfigurations[0].ID)

	np, err := s.net.NewInterfacesClient().BeginDelete(ctx, netRG, "nic1", nil)
	require.NoError(t, err)
	_, err = np.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)

	// still associated with the subnet
	assert.Equal(t, "InUseNetworkSecurityGroupCannotBeDeleted", sdkErrCode(delNSG()))

	_, err = s.net.NewSubnetsClient().BeginCreateOrUpdate(ctx, netRG, "vnet1", "a", armnetwork.Subnet{
		Properties: &armnetwork.SubnetPropertiesFormat{AddressPrefix: new("10.0.1.0/24")},
	}, nil)
	require.NoError(t, err)

	require.NoError(t, delNSG())
	require.NoError(t, delRT())
	require.NoError(t, delSubnet())
	require.NoError(t, delVNet())

	_, err = s.net.NewVirtualNetworksClient().Get(ctx, netRG, "vnet1", nil)
	assert.Equal(t, "ResourceNotFound", sdkErrCode(err))
}

func TestNetworkSDK_Peering(t *testing.T) {
	t.Parallel()

	s := newNetStack(t)
	ctx := t.Context()
	a := s.vnet(t, "vnet-a", []string{"10.0.0.0/16"}, nil)
	b := s.vnet(t, "vnet-b", []string{"10.1.0.0/16"}, nil)
	c := s.vnet(t, "vnet-c", []string{"10.0.5.0/24"}, nil)
	pc := s.net.NewVirtualNetworkPeeringsClient()

	peer := func(local, name string, remote *string) (armnetwork.VirtualNetworkPeering, error) {
		p, err := pc.BeginCreateOrUpdate(ctx, netRG, local, name, armnetwork.VirtualNetworkPeering{
			Properties: &armnetwork.VirtualNetworkPeeringPropertiesFormat{
				RemoteVirtualNetwork: &armnetwork.SubResource{ID: remote},
			},
		}, nil)
		if err != nil {
			return armnetwork.VirtualNetworkPeering{}, err
		}

		res, err := p.PollUntilDone(ctx, pollFast())

		return res.VirtualNetworkPeering, err
	}

	ab, err := peer("vnet-a", "a-to-b", b.ID)
	require.NoError(t, err)
	assert.Equal(t, armnetwork.VirtualNetworkPeeringStateInitiated, *ab.Properties.PeeringState)
	assert.True(t, *ab.Properties.AllowVirtualNetworkAccess)
	assert.False(t, *ab.Properties.AllowForwardedTraffic)
	assert.Equal(t, "10.1.0.0/16", *ab.Properties.RemoteAddressSpace.AddressPrefixes[0])

	_, err = peer("vnet-a", "a-to-c", c.ID)
	assert.Equal(t, "VnetAddressSpacesOverlap", sdkErrCode(err))

	_, err = peer("vnet-a", "a-to-self", a.ID)
	assert.Equal(t, "InvalidRequestFormat", sdkErrCode(err))

	_, err = peer("vnet-a", "a-to-ghost", new(*a.ID+"-ghost"))
	assert.Equal(t, "InvalidResourceReference", sdkErrCode(err))

	ba, err := peer("vnet-b", "b-to-a", a.ID)
	require.NoError(t, err)
	assert.Equal(t, armnetwork.VirtualNetworkPeeringStateConnected, *ba.Properties.PeeringState)

	again, err := pc.Get(ctx, netRG, "vnet-a", "a-to-b", nil)
	require.NoError(t, err)
	assert.Equal(t, armnetwork.VirtualNetworkPeeringStateConnected, *again.Properties.PeeringState)
	assert.Equal(t, armnetwork.VirtualNetworkPeeringLevelFullyInSync, *again.Properties.PeeringSyncLevel)

	del, err := pc.BeginDelete(ctx, netRG, "vnet-b", "b-to-a", nil)
	require.NoError(t, err)
	_, err = del.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)

	again, err = pc.Get(ctx, netRG, "vnet-a", "a-to-b", nil)
	require.NoError(t, err)
	assert.Equal(t, armnetwork.VirtualNetworkPeeringStateDisconnected, *again.Properties.PeeringState)

	pager := pc.NewListPager(netRG, "vnet-a", nil)
	page, err := pager.NextPage(ctx)
	require.NoError(t, err)
	assert.Len(t, page.Value, 1)
}

func TestNetworkSDK_RouteTableValidation(t *testing.T) {
	t.Parallel()

	s := newNetStack(t)
	ctx := t.Context()

	p, err := s.net.NewRouteTablesClient().BeginCreateOrUpdate(ctx, netRG, "rt1",
		armnetwork.RouteTable{Location: new("westus"), Tags: map[string]*string{"a": new("b")}}, nil)
	require.NoError(t, err)
	_, err = p.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)

	tests := []struct {
		name     string
		props    armnetwork.RoutePropertiesFormat
		wantCode string
	}{
		{name: "local", props: armnetwork.RoutePropertiesFormat{
			AddressPrefix: new("10.9.0.0/16"), NextHopType: new(armnetwork.RouteNextHopTypeVnetLocal),
		}},
		{name: "appliance", props: armnetwork.RoutePropertiesFormat{
			AddressPrefix: new("0.0.0.0/0"), NextHopType: new(armnetwork.RouteNextHopTypeVirtualAppliance),
			NextHopIPAddress: new("10.0.0.4"),
		}},
		{name: "no-ip", props: armnetwork.RoutePropertiesFormat{
			AddressPrefix: new("0.0.0.0/0"), NextHopType: new(armnetwork.RouteNextHopTypeVirtualAppliance),
		}, wantCode: "InvalidRequestFormat"},
		{name: "ip-on-internet", props: armnetwork.RoutePropertiesFormat{
			AddressPrefix: new(
				"0.0.0.0/0",
			),
			NextHopType:      new(armnetwork.RouteNextHopTypeInternet),
			NextHopIPAddress: new("1.2.3.4"),
		}, wantCode: "InvalidRequestFormat"},
		{name: "bad-cidr", props: armnetwork.RoutePropertiesFormat{
			AddressPrefix: new("10.9.0.1/16"), NextHopType: new(armnetwork.RouteNextHopTypeNone),
		}, wantCode: "InvalidCIDRNotation"},
	}

	for _, tt := range tests {
		rp, cerr := s.net.NewRoutesClient().
			BeginCreateOrUpdate(ctx, netRG, "rt1", tt.name, armnetwork.Route{Properties: &tt.props}, nil)
		if tt.wantCode != "" {
			require.Error(t, cerr, tt.name)
			assert.Equal(t, tt.wantCode, sdkErrCode(cerr), tt.name)

			continue
		}

		require.NoError(t, cerr, tt.name)
		_, cerr = rp.PollUntilDone(ctx, pollFast())
		require.NoError(t, cerr, tt.name)
	}

	got, err := s.net.NewRouteTablesClient().Get(ctx, netRG, "rt1", nil)
	require.NoError(t, err)
	assert.Len(t, got.Properties.Routes, 2)
	assert.NotEmpty(t, *got.Etag)

	upd, err := s.net.NewRouteTablesClient().
		UpdateTags(ctx, netRG, "rt1", armnetwork.TagsObject{Tags: map[string]*string{"x": new("y")}}, nil)
	require.NoError(t, err)
	assert.Equal(t, map[string]*string{"x": new("y")}, upd.Tags)
	assert.Len(t, upd.Properties.Routes, 2)
}

func TestNetworkSDK_ListScopesAndCascade(t *testing.T) {
	t.Parallel()

	s := newNetStack(t)
	ctx := t.Context()
	s.rawDo(t, http.MethodPut, "/subscriptions/"+aciSub+"/resourcegroups/rg-other", `{"location":"westus"}`)

	for _, n := range []string{"v1", "v2"} {
		s.vnet(t, n, []string{"10.0.0.0/16"}, nil)
	}

	p, err := s.net.NewVirtualNetworksClient().BeginCreateOrUpdate(ctx, "rg-other", "v3", armnetwork.VirtualNetwork{
		Location: new("westus"),
		Properties: &armnetwork.VirtualNetworkPropertiesFormat{
			AddressSpace: &armnetwork.AddressSpace{AddressPrefixes: []*string{new("10.2.0.0/16")}},
		},
	}, nil)
	require.NoError(t, err)
	_, err = p.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)

	count := func(pager func() ([]*armnetwork.VirtualNetwork, error)) int {
		vs, perr := pager()
		require.NoError(t, perr)

		return len(vs)
	}

	listRG := func() ([]*armnetwork.VirtualNetwork, error) {
		var out []*armnetwork.VirtualNetwork

		pg := s.net.NewVirtualNetworksClient().NewListPager(netRG, nil)
		for pg.More() {
			page, perr := pg.NextPage(ctx)
			if perr != nil {
				return nil, perr
			}

			out = append(out, page.Value...)
		}

		return out, nil
	}
	listAll := func() ([]*armnetwork.VirtualNetwork, error) {
		var out []*armnetwork.VirtualNetwork

		pg := s.net.NewVirtualNetworksClient().NewListAllPager(nil)
		for pg.More() {
			page, perr := pg.NextPage(ctx)
			if perr != nil {
				return nil, perr
			}

			out = append(out, page.Value...)
		}

		return out, nil
	}

	assert.Equal(t, 2, count(listRG))
	assert.Equal(t, 3, count(listAll))

	resp := s.rawDo(t, http.MethodDelete, "/subscriptions/"+aciSub+"/resourcegroups/"+netRG, "")
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	assert.Equal(t, 0, count(listRG))
	assert.Equal(t, 1, count(listAll))
}

func TestNetworkSDK_SnapshotRestore(t *testing.T) {
	t.Parallel()

	s := newNetStack(t)
	ctx := t.Context()

	v := s.vnet(t, "vnet1", []string{"10.0.0.0/16"}, map[string]string{"a": "10.0.1.0/24"})
	b := s.vnet(t, "vnet2", []string{"10.1.0.0/16"}, nil)
	sid := *v.Properties.Subnets[0].ID

	_, err := s.nic(t, "nic1", sid, armnetwork.IPAllocationMethodDynamic, "", nil)
	require.NoError(t, err)

	pp, err := s.net.NewPublicIPAddressesClient().BeginCreateOrUpdate(ctx, netRG, "pip1", armnetwork.PublicIPAddress{
		Location: new(
			"westus",
		),
		SKU: &armnetwork.PublicIPAddressSKU{Name: new(armnetwork.PublicIPAddressSKUNameStandard)},
		Properties: &armnetwork.PublicIPAddressPropertiesFormat{
			PublicIPAllocationMethod: new(armnetwork.IPAllocationMethodStatic),
		},
	}, nil)
	require.NoError(t, err)
	pip, err := pp.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)

	peer, err := s.net.NewVirtualNetworkPeeringsClient().
		BeginCreateOrUpdate(ctx, netRG, "vnet1", "p", armnetwork.VirtualNetworkPeering{
			Properties: &armnetwork.VirtualNetworkPeeringPropertiesFormat{
				RemoteVirtualNetwork: &armnetwork.SubResource{ID: b.ID},
			},
		}, nil)
	require.NoError(t, err)
	_, err = peer.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)

	zp, err := s.dns.NewPrivateZonesClient().
		BeginCreateOrUpdate(ctx, netRG, "contoso.internal", armprivatedns.PrivateZone{Location: new("global")}, nil)
	require.NoError(t, err)
	_, err = zp.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)

	_, err = s.msi.NewUserAssignedIdentitiesClient().
		CreateOrUpdate(ctx, netRG, "id-one", armmsi.Identity{Location: new("westus")}, nil)
	require.NoError(t, err)

	snap := s.handler.Snapshot(ctx)
	require.NotEmpty(t, snap)

	fresh := newNetStack(t)
	require.NoError(t, fresh.handler.Restore(ctx, snap))

	got, err := fresh.net.NewVirtualNetworksClient().Get(ctx, netRG, "vnet1", nil)
	require.NoError(t, err)
	assert.Equal(t, *v.Properties.ResourceGUID, *got.Properties.ResourceGUID)
	require.Len(t, got.Properties.Subnets, 1)
	require.Len(t, got.Properties.Subnets[0].Properties.IPConfigurations, 1)
	require.Len(t, got.Properties.VirtualNetworkPeerings, 1)
	assert.Equal(
		t,
		armnetwork.VirtualNetworkPeeringStateInitiated,
		*got.Properties.VirtualNetworkPeerings[0].Properties.PeeringState,
	)

	nic, err := fresh.net.NewInterfacesClient().Get(ctx, netRG, "nic1", nil)
	require.NoError(t, err)
	assert.Equal(t, "10.0.1.4", *nic.Properties.IPConfigurations[0].Properties.PrivateIPAddress)

	// the restored allocator still sees the address as taken
	next, err := fresh.nic(t, "nic2", sid, armnetwork.IPAllocationMethodDynamic, "", nil)
	require.NoError(t, err)
	assert.Equal(t, "10.0.1.5", *next.Properties.IPConfigurations[0].Properties.PrivateIPAddress)

	gp, err := fresh.net.NewPublicIPAddressesClient().Get(ctx, netRG, "pip1", nil)
	require.NoError(t, err)
	assert.Equal(t, *pip.Properties.IPAddress, *gp.Properties.IPAddress)

	z, err := fresh.dns.NewPrivateZonesClient().Get(ctx, netRG, "contoso.internal", nil)
	require.NoError(t, err)
	assert.EqualValues(t, 1, *z.Properties.NumberOfRecordSets)

	id, err := fresh.msi.NewUserAssignedIdentitiesClient().Get(ctx, netRG, "id-one", nil)
	require.NoError(t, err)
	assert.NotEmpty(t, *id.Properties.PrincipalID)
}

func TestNetworkSDK_Protocol(t *testing.T) {
	t.Parallel()

	s := newNetStack(t)
	base := "/subscriptions/" + aciSub + "/resourceGroups/" + netRG + "/providers/Microsoft.Network/routeTables/rt1"

	tests := []struct {
		name       string
		method     string
		path       string
		body       string
		wantCode   string
		wantHeader string
		wantStatus int
	}{
		{
			name:       "missing api-version",
			method:     http.MethodGet,
			path:       base,
			wantStatus: 400,
			wantCode:   "MissingApiVersionParameter",
		},
		{
			name:       "malformed api-version",
			method:     http.MethodGet,
			path:       base + "?api-version=x",
			wantStatus: 400,
			wantCode:   "InvalidApiVersionParameter",
		},
		{
			name:       "unknown api-version",
			method:     http.MethodGet,
			path:       base + "?api-version=1999-01-01",
			wantStatus: 400,
			wantCode:   "NoRegisteredProviderFound",
		},
		{
			name:       "get missing",
			method:     http.MethodGet,
			path:       base + "?api-version=2025-03-01",
			wantStatus: 404,
			wantCode:   "ResourceNotFound",
		},
		{
			name:       "no location",
			method:     http.MethodPut,
			path:       base + "?api-version=2025-03-01",
			body:       `{}`,
			wantStatus: 400,
			wantCode:   "LocationRequired",
		},
		{
			name:       "bad name",
			method:     http.MethodPut,
			path:       strings.Replace(base, "rt1", "bad%20name", 1) + "?api-version=2025-03-01",
			body:       `{"location":"westus"}`,
			wantStatus: 400,
			wantCode:   "InvalidResourceName",
		},
		{
			name: "create is 201 with async header", method: http.MethodPut, path: base + "?api-version=2025-03-01",
			body: `{"location":"West US"}`, wantStatus: 201, wantHeader: "Azure-Asyncoperation",
		},
		{
			name: "update is 200", method: http.MethodPut, path: base + "?api-version=2025-03-01",
			body: `{"location":"westus"}`, wantStatus: 200, wantHeader: "Azure-Asyncoperation",
		},
		{
			name:       "delete is 202",
			method:     http.MethodDelete,
			path:       base + "?api-version=2025-03-01",
			wantStatus: 202,
			wantHeader: "Location",
		},
		{
			name:       "delete again is 204",
			method:     http.MethodDelete,
			path:       base + "?api-version=2025-03-01",
			wantStatus: 204,
		},
	}

	// sequential: the cases walk a resource through its lifecycle
	for _, tt := range tests {
		resp := s.rawDo(t, tt.method, tt.path, tt.body)
		assert.Equal(t, tt.wantStatus, resp.StatusCode, tt.name)

		if tt.wantHeader != "" {
			assert.NotEmpty(t, resp.Header.Get(tt.wantHeader), tt.name)
		}

		if tt.wantCode != "" {
			var e struct {
				Error struct{ Code string } `json:"error"`
			}

			require.NoError(t, jsonDecode(resp, &e), tt.name)
			assert.Equal(t, tt.wantCode, e.Error.Code, tt.name)
		}
	}

	op := s.rawDo(
		t,
		http.MethodGet,
		"/subscriptions/"+aciSub+"/providers/Microsoft.Network/locations/westus/operations/abc?api-version=2025-03-01",
		"",
	)
	assert.Equal(t, 200, op.StatusCode)
}

func jsonDecode(resp *http.Response, v any) error {
	return json.NewDecoder(resp.Body).Decode(v)
}
