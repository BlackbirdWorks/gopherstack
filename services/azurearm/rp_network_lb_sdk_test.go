package azurearm_test

import (
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func netPoll[T any](t *testing.T, p *runtime.Poller[T], err error) T {
	t.Helper()

	var zero T

	if err != nil {
		require.NoError(t, err)

		return zero
	}

	res, err := p.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)

	return res
}

func (s *netStack) pip(
	t *testing.T,
	name string,
	sku armnetwork.PublicIPAddressSKUName,
	prefixID string,
) (armnetwork.PublicIPAddress, error) {
	t.Helper()

	props := &armnetwork.PublicIPAddressPropertiesFormat{
		PublicIPAllocationMethod: new(armnetwork.IPAllocationMethodStatic),
	}

	if sku == armnetwork.PublicIPAddressSKUNameBasic {
		props.PublicIPAllocationMethod = new(armnetwork.IPAllocationMethodDynamic)
	}

	if prefixID != "" {
		props.PublicIPPrefix = &armnetwork.SubResource{ID: new(prefixID)}
	}

	p, err := s.net.NewPublicIPAddressesClient().
		BeginCreateOrUpdate(t.Context(), netRG, name, armnetwork.PublicIPAddress{
			Location: new("westus"), SKU: &armnetwork.PublicIPAddressSKU{Name: new(sku)}, Properties: props,
		}, nil)
	if err != nil {
		return armnetwork.PublicIPAddress{}, err
	}

	res, err := p.PollUntilDone(t.Context(), pollFast())

	return res.PublicIPAddress, err
}

func lbSub(lbName, kind, name string) *armnetwork.SubResource {
	return &armnetwork.SubResource{ID: new("/subscriptions/" + aciSub + "/resourceGroups/" + netRG +
		"/providers/Microsoft.Network/loadBalancers/" + lbName + "/" + kind + "/" + name)}
}

// stdLB is a Standard public load balancer with one frontend, pool, probe and rule.
func stdLB(lbName string, pipID string, sku armnetwork.LoadBalancerSKUName) armnetwork.LoadBalancer {
	return armnetwork.LoadBalancer{
		Location: new("westus"),
		SKU:      &armnetwork.LoadBalancerSKU{Name: new(sku)},
		Properties: &armnetwork.LoadBalancerPropertiesFormat{
			FrontendIPConfigurations: []*armnetwork.FrontendIPConfiguration{{
				Name: new("fe"),
				Properties: &armnetwork.FrontendIPConfigurationPropertiesFormat{
					PublicIPAddress: &armnetwork.PublicIPAddress{ID: new(pipID)},
				},
			}},
			BackendAddressPools: []*armnetwork.BackendAddressPool{{Name: new("pool")}},
			Probes: []*armnetwork.Probe{{
				Name: new("probe"),
				Properties: &armnetwork.ProbePropertiesFormat{
					Protocol: new(armnetwork.ProbeProtocolTCP), Port: new(int32(80)),
				},
			}},
			LoadBalancingRules: []*armnetwork.LoadBalancingRule{{
				Name: new("rule"),
				Properties: &armnetwork.LoadBalancingRulePropertiesFormat{
					FrontendIPConfiguration: lbSub(lbName, "frontendIPConfigurations", "fe"),
					BackendAddressPool:      lbSub(lbName, "backendAddressPools", "pool"),
					Probe:                   lbSub(lbName, "probes", "probe"),
					Protocol:                new(armnetwork.TransportProtocolTCP),
					FrontendPort:            new(int32(80)), BackendPort: new(int32(8080)),
				},
			}},
		},
	}
}

func (s *netStack) putLB(t *testing.T, name string, lb armnetwork.LoadBalancer) (armnetwork.LoadBalancer, error) {
	t.Helper()

	p, err := s.net.NewLoadBalancersClient().BeginCreateOrUpdate(t.Context(), netRG, name, lb, nil)
	if err != nil {
		return armnetwork.LoadBalancer{}, err
	}

	res, err := p.PollUntilDone(t.Context(), pollFast())

	return res.LoadBalancer, err
}

func TestNetworkSDK_LoadBalancerCRUD(t *testing.T) {
	t.Parallel()

	s := newNetStack(t)
	ctx := t.Context()

	pip, err := s.pip(t, "pip1", armnetwork.PublicIPAddressSKUNameStandard, "")
	require.NoError(t, err)

	lb, err := s.putLB(t, "lb1", stdLB("lb1", *pip.ID, armnetwork.LoadBalancerSKUNameStandard))
	require.NoError(t, err)
	assert.Equal(t, "Succeeded", string(*lb.Properties.ProvisioningState))
	assert.Equal(t, "Standard", string(*lb.SKU.Name))
	assert.Equal(t, "Regional", string(*lb.SKU.Tier))
	require.Len(t, lb.Properties.FrontendIPConfigurations, 1)
	assert.Len(t, lb.Properties.FrontendIPConfigurations[0].Properties.LoadBalancingRules, 1)
	require.Len(t, lb.Properties.Probes, 1)
	assert.EqualValues(t, 15, *lb.Properties.Probes[0].Properties.IntervalInSeconds)
	require.Len(t, lb.Properties.LoadBalancingRules, 1)
	assert.EqualValues(t, 4, *lb.Properties.LoadBalancingRules[0].Properties.IdleTimeoutInMinutes)
	assert.False(t, *lb.Properties.LoadBalancingRules[0].Properties.EnableFloatingIP)

	gotPIP, err := s.net.NewPublicIPAddressesClient().Get(ctx, netRG, "pip1", nil)
	require.NoError(t, err)
	assert.Equal(t, *lb.Properties.FrontendIPConfigurations[0].ID, *gotPIP.Properties.IPConfiguration.ID)

	pool, err := s.net.NewLoadBalancerBackendAddressPoolsClient().Get(ctx, netRG, "lb1", "pool", nil)
	require.NoError(t, err)
	assert.Len(t, pool.Properties.LoadBalancingRules, 1)

	fe, err := s.net.NewLoadBalancerFrontendIPConfigurationsClient().Get(ctx, netRG, "lb1", "fe", nil)
	require.NoError(t, err)
	assert.Equal(t, *pip.ID, *fe.Properties.PublicIPAddress.ID)

	rules := s.net.NewLoadBalancerLoadBalancingRulesClient()
	rule, err := rules.Get(ctx, netRG, "lb1", "rule", nil)
	require.NoError(t, err)
	assert.EqualValues(t, 8080, *rule.Properties.BackendPort)

	probes, err := s.net.NewLoadBalancerProbesClient().Get(ctx, netRG, "lb1", "probe", nil)
	require.NoError(t, err)
	assert.Len(t, probes.Properties.LoadBalancingRules, 1)

	pager := s.net.NewLoadBalancersClient().NewListPager(netRG, nil)
	names := []string{}

	for pager.More() {
		page, perr := pager.NextPage(ctx)
		require.NoError(t, perr)

		for _, l := range page.Value {
			names = append(names, *l.Name)
		}
	}

	assert.Equal(t, []string{"lb1"}, names)

	tagged, err := s.net.NewLoadBalancersClient().UpdateTags(ctx, netRG, "lb1",
		armnetwork.TagsObject{Tags: map[string]*string{"env": new("dev")}}, nil)
	require.NoError(t, err)
	assert.Equal(t, "dev", *tagged.Tags["env"])

	// re-PUT keeps the child GUIDs and the frontend address
	again, err := s.putLB(t, "lb1", stdLB("lb1", *pip.ID, armnetwork.LoadBalancerSKUNameStandard))
	require.NoError(t, err)
	assert.Equal(t, *lb.Properties.ResourceGUID, *again.Properties.ResourceGUID)

	_, err = s.net.NewPublicIPAddressesClient().BeginDelete(ctx, netRG, "pip1", nil)
	require.Error(t, err)
	assert.Equal(t, "PublicIPAddressCannotBeDeleted", sdkErrCode(err))

	dp, err := s.net.NewLoadBalancersClient().BeginDelete(ctx, netRG, "lb1", nil)
	netPoll(t, dp, err)

	_, err = s.net.NewLoadBalancersClient().Get(ctx, netRG, "lb1", nil)
	require.Error(t, err)
	assert.Equal(t, "ResourceNotFound", sdkErrCode(err))

	dpip, err := s.net.NewPublicIPAddressesClient().BeginDelete(ctx, netRG, "pip1", nil)
	netPoll(t, dpip, err)
}

func TestNetworkSDK_LoadBalancerValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate   func(lb *armnetwork.LoadBalancer, pipID string)
		pipSKU   armnetwork.PublicIPAddressSKUName
		lbSKU    armnetwork.LoadBalancerSKUName
		name     string
		wantCode string
	}{
		{
			name:   "valid",
			pipSKU: armnetwork.PublicIPAddressSKUNameStandard,
			lbSKU:  armnetwork.LoadBalancerSKUNameStandard,
		},
		{
			name: "basic pip with standard lb", pipSKU: armnetwork.PublicIPAddressSKUNameBasic,
			lbSKU: armnetwork.LoadBalancerSKUNameStandard, wantCode: "PublicIPAndLBSkuDoNotMatch",
		},
		{
			name: "standard pip with basic lb", pipSKU: armnetwork.PublicIPAddressSKUNameStandard,
			lbSKU: armnetwork.LoadBalancerSKUNameBasic, wantCode: "PublicIPAndLBSkuDoNotMatch",
		},
		{
			name: "missing pip", pipSKU: armnetwork.PublicIPAddressSKUNameStandard,
			lbSKU: armnetwork.LoadBalancerSKUNameStandard, wantCode: "InvalidResourceReference",
			mutate: func(lb *armnetwork.LoadBalancer, _ string) {
				lb.Properties.FrontendIPConfigurations[0].Properties.PublicIPAddress.ID = new("/subscriptions/" +
					aciSub + "/resourceGroups/" + netRG + "/providers/Microsoft.Network/publicIPAddresses/nope")
			},
		},
		{
			name: "rule references unknown pool", pipSKU: armnetwork.PublicIPAddressSKUNameStandard,
			lbSKU: armnetwork.LoadBalancerSKUNameStandard, wantCode: "InvalidResourceReference",
			mutate: func(lb *armnetwork.LoadBalancer, _ string) {
				lb.Properties.LoadBalancingRules[0].Properties.BackendAddressPool = lbSub(
					"lb",
					"backendAddressPools",
					"nope",
				)
			},
		},
		{
			name: "probe http without path", pipSKU: armnetwork.PublicIPAddressSKUNameStandard,
			lbSKU: armnetwork.LoadBalancerSKUNameStandard, wantCode: "InvalidRequestFormat",
			mutate: func(lb *armnetwork.LoadBalancer, _ string) {
				lb.Properties.Probes[0].Properties.Protocol = new(armnetwork.ProbeProtocolHTTP)
			},
		},
		{
			name: "duplicate frontend port", pipSKU: armnetwork.PublicIPAddressSKUNameStandard,
			lbSKU: armnetwork.LoadBalancerSKUNameStandard, wantCode: "LoadBalancingRuleFrontendPortConflict",
			mutate: func(lb *armnetwork.LoadBalancer, _ string) {
				dup := *lb.Properties.LoadBalancingRules[0]
				dup.Name = new("rule2")
				lb.Properties.LoadBalancingRules = append(lb.Properties.LoadBalancingRules, &dup)
			},
		},
		{
			name: "outbound rule needs standard", pipSKU: armnetwork.PublicIPAddressSKUNameBasic,
			lbSKU: armnetwork.LoadBalancerSKUNameBasic, wantCode: "InvalidRequestFormat",
			mutate: func(lb *armnetwork.LoadBalancer, _ string) {
				lb.Properties.OutboundRules = []*armnetwork.OutboundRule{{
					Name: new("out"),
					Properties: &armnetwork.OutboundRulePropertiesFormat{
						Protocol:           new(armnetwork.LoadBalancerOutboundRuleProtocolAll),
						BackendAddressPool: lbSub("lb", "backendAddressPools", "pool"),
						FrontendIPConfigurations: []*armnetwork.SubResource{
							lbSub("lb", "frontendIPConfigurations", "fe"),
						},
					},
				}}
			},
		},
		{
			name: "outbound rule on standard", pipSKU: armnetwork.PublicIPAddressSKUNameStandard,
			lbSKU: armnetwork.LoadBalancerSKUNameStandard,
			mutate: func(lb *armnetwork.LoadBalancer, _ string) {
				lb.Properties.OutboundRules = []*armnetwork.OutboundRule{{
					Name: new("out"),
					Properties: &armnetwork.OutboundRulePropertiesFormat{
						Protocol:           new(armnetwork.LoadBalancerOutboundRuleProtocolAll),
						BackendAddressPool: lbSub("lb", "backendAddressPools", "pool"),
						FrontendIPConfigurations: []*armnetwork.SubResource{
							lbSub("lb", "frontendIPConfigurations", "fe"),
						},
					},
				}}
			},
		},
		{
			name: "ip based pool on basic", pipSKU: armnetwork.PublicIPAddressSKUNameBasic,
			lbSKU: armnetwork.LoadBalancerSKUNameBasic, wantCode: "InvalidRequestFormat",
			mutate: func(lb *armnetwork.LoadBalancer, _ string) {
				lb.Properties.BackendAddressPools[0].Properties = &armnetwork.BackendAddressPoolPropertiesFormat{
					LoadBalancerBackendAddresses: []*armnetwork.LoadBalancerBackendAddress{{Name: new("a")}},
				}
			},
		},
		{
			name: "ha ports on basic", pipSKU: armnetwork.PublicIPAddressSKUNameBasic,
			lbSKU: armnetwork.LoadBalancerSKUNameBasic, wantCode: "InvalidRequestFormat",
			mutate: func(lb *armnetwork.LoadBalancer, _ string) {
				r := lb.Properties.LoadBalancingRules[0].Properties
				r.Protocol, r.FrontendPort, r.BackendPort = new(
					armnetwork.TransportProtocolAll,
				), new(
					int32(0),
				), new(
					int32(0),
				)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newNetStack(t)

			pip, err := s.pip(t, "pip", tt.pipSKU, "")
			require.NoError(t, err)

			lb := stdLB("lb", *pip.ID, tt.lbSKU)
			if tt.mutate != nil {
				tt.mutate(&lb, *pip.ID)
			}

			_, err = s.putLB(t, "lb", lb)
			if tt.wantCode != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, sdkErrCode(err))

				return
			}

			require.NoError(t, err)
		})
	}
}

func (s *netStack) nicWith(
	t *testing.T,
	name, subnetID string,
	cfg *armnetwork.InterfaceIPConfigurationPropertiesFormat,
) (armnetwork.Interface, error) {
	t.Helper()

	cfg.Subnet = &armnetwork.Subnet{ID: new(subnetID)}
	if cfg.PrivateIPAllocationMethod == nil {
		cfg.PrivateIPAllocationMethod = new(armnetwork.IPAllocationMethodDynamic)
	}

	p, err := s.net.NewInterfacesClient().BeginCreateOrUpdate(t.Context(), netRG, name, armnetwork.Interface{
		Location: new("westus"),
		Properties: &armnetwork.InterfacePropertiesFormat{
			IPConfigurations: []*armnetwork.InterfaceIPConfiguration{{Name: new("ipconfig1"), Properties: cfg}},
		},
	}, nil)
	if err != nil {
		return armnetwork.Interface{}, err
	}

	res, err := p.PollUntilDone(t.Context(), pollFast())

	return res.Interface, err
}

func TestNetworkSDK_LoadBalancerNICMembership(t *testing.T) {
	t.Parallel()

	s := newNetStack(t)
	ctx := t.Context()
	v := s.vnet(t, "vnet1", []string{"10.0.0.0/16"}, map[string]string{"a": "10.0.1.0/24"})
	sid := *v.Properties.Subnets[0].ID

	pip, err := s.pip(t, "pip1", armnetwork.PublicIPAddressSKUNameStandard, "")
	require.NoError(t, err)

	lbIn := stdLB("lb1", *pip.ID, armnetwork.LoadBalancerSKUNameStandard)
	lbIn.Properties.InboundNatRules = []*armnetwork.InboundNatRule{{
		Name: new("ssh"),
		Properties: &armnetwork.InboundNatRulePropertiesFormat{
			FrontendIPConfiguration: lbSub("lb1", "frontendIPConfigurations", "fe"),
			Protocol:                new(armnetwork.TransportProtocolTCP),
			FrontendPort:            new(int32(2222)), BackendPort: new(int32(22)),
		},
	}}

	_, err = s.putLB(t, "lb1", lbIn)
	require.NoError(t, err)

	poolRef := lbSub("lb1", "backendAddressPools", "pool")
	natRef := lbSub("lb1", "inboundNatRules", "ssh")

	nic, err := s.nicWith(t, "nic1", sid, &armnetwork.InterfaceIPConfigurationPropertiesFormat{
		LoadBalancerBackendAddressPools: []*armnetwork.BackendAddressPool{{ID: poolRef.ID}},
		LoadBalancerInboundNatRules:     []*armnetwork.InboundNatRule{{ID: natRef.ID}},
	})
	require.NoError(t, err)

	cfgID := *nic.Properties.IPConfigurations[0].ID
	require.Len(t, nic.Properties.IPConfigurations[0].Properties.LoadBalancerBackendAddressPools, 1)

	pool, err := s.net.NewLoadBalancerBackendAddressPoolsClient().Get(ctx, netRG, "lb1", "pool", nil)
	require.NoError(t, err)
	require.Len(t, pool.Properties.BackendIPConfigurations, 1)
	assert.Equal(t, cfgID, *pool.Properties.BackendIPConfigurations[0].ID)

	nat, err := s.net.NewInboundNatRulesClient().Get(ctx, netRG, "lb1", "ssh", nil)
	require.NoError(t, err)
	assert.Equal(t, cfgID, *nat.Properties.BackendIPConfiguration.ID)

	// listing on both sides
	lbNICs := s.net.NewLoadBalancerNetworkInterfacesClient().NewListPager(netRG, "lb1", nil)
	require.True(t, lbNICs.More())

	page, err := lbNICs.NextPage(ctx)
	require.NoError(t, err)
	require.Len(t, page.Value, 1)
	assert.Equal(t, "nic1", *page.Value[0].Name)

	nicLBs := s.net.NewInterfaceLoadBalancersClient().NewListPager(netRG, "nic1", nil)
	require.True(t, nicLBs.More())

	lpage, err := nicLBs.NextPage(ctx)
	require.NoError(t, err)
	require.Len(t, lpage.Value, 1)
	assert.Equal(t, "lb1", *lpage.Value[0].Name)

	// a second NIC cannot take the V1 NAT rule
	_, err = s.nicWith(t, "nic2", sid, &armnetwork.InterfaceIPConfigurationPropertiesFormat{
		LoadBalancerInboundNatRules: []*armnetwork.InboundNatRule{{ID: natRef.ID}},
	})
	require.Error(t, err)
	assert.Equal(t, "InboundNatRuleInUse", sdkErrCode(err))

	// unknown pool
	_, err = s.nicWith(t, "nic3", sid, &armnetwork.InterfaceIPConfigurationPropertiesFormat{
		LoadBalancerBackendAddressPools: []*armnetwork.BackendAddressPool{
			{ID: lbSub("lb1", "backendAddressPools", "nope").ID},
		},
	})
	require.Error(t, err)
	assert.Equal(t, "InvalidResourceReference", sdkErrCode(err))

	// a pool referenced by a rule cannot be deleted; the child clients create and delete other pools
	pc := s.net.NewLoadBalancerBackendAddressPoolsClient()
	_, err = pc.BeginDelete(ctx, netRG, "lb1", "pool", nil)
	require.Error(t, err)
	assert.Equal(t, "InUseLoadBalancerChildCannotBeDeleted", sdkErrCode(err))

	cp, err := pc.BeginCreateOrUpdate(ctx, netRG, "lb1", "extra", armnetwork.BackendAddressPool{}, nil)
	extra := netPoll(t, cp, err)
	assert.Equal(t, "extra", *extra.Name)

	dp, err := pc.BeginDelete(ctx, netRG, "lb1", "extra", nil)
	netPoll(t, dp, err)

	// removing the membership from the NIC side empties the pool
	_, err = s.nicWith(t, "nic1", sid, &armnetwork.InterfaceIPConfigurationPropertiesFormat{})
	require.NoError(t, err)

	pool, err = pc.Get(ctx, netRG, "lb1", "pool", nil)
	require.NoError(t, err)
	assert.Empty(t, pool.Properties.BackendIPConfigurations)

	// NAT rule child endpoint
	nc := s.net.NewInboundNatRulesClient()
	np, err := nc.BeginCreateOrUpdate(ctx, netRG, "lb1", "rdp", armnetwork.InboundNatRule{
		Properties: &armnetwork.InboundNatRulePropertiesFormat{
			FrontendIPConfiguration: lbSub("lb1", "frontendIPConfigurations", "fe"),
			Protocol:                new(armnetwork.TransportProtocolTCP),
			FrontendPort:            new(int32(2222)), BackendPort: new(int32(3389)),
		},
	}, nil)
	require.Error(t, err)
	assert.Nil(t, np)
	assert.Equal(t, "InboundNatRuleFrontendPortConflict", sdkErrCode(err))
}

func TestNetworkSDK_LoadBalancerInternalFrontend(t *testing.T) {
	t.Parallel()

	s := newNetStack(t)
	ctx := t.Context()
	v := s.vnet(t, "vnet1", []string{"10.0.0.0/16"}, map[string]string{"a": "10.0.1.0/24"})
	sid := *v.Properties.Subnets[0].ID

	internal := func(method armnetwork.IPAllocationMethod, addr string) armnetwork.LoadBalancer {
		props := &armnetwork.FrontendIPConfigurationPropertiesFormat{
			Subnet: &armnetwork.Subnet{ID: new(sid)}, PrivateIPAllocationMethod: new(method),
		}
		if addr != "" {
			props.PrivateIPAddress = new(addr)
		}

		return armnetwork.LoadBalancer{
			Location: new(
				"westus",
			),
			SKU: &armnetwork.LoadBalancerSKU{Name: new(armnetwork.LoadBalancerSKUNameStandard)},
			Properties: &armnetwork.LoadBalancerPropertiesFormat{
				FrontendIPConfigurations: []*armnetwork.FrontendIPConfiguration{{Name: new("fe"), Properties: props}},
			},
		}
	}

	nic, err := s.nic(t, "nic1", sid, armnetwork.IPAllocationMethodStatic, "10.0.1.4", nil)
	require.NoError(t, err)
	assert.Equal(t, "10.0.1.4", *nic.Properties.IPConfigurations[0].Properties.PrivateIPAddress)

	tests := []struct {
		name     string
		method   armnetwork.IPAllocationMethod
		addr     string
		wantIP   string
		wantCode string
	}{
		{name: "dynamic skips nic address", method: armnetwork.IPAllocationMethodDynamic, wantIP: "10.0.1.5"},
		{
			name: "static conflicts with nic", method: armnetwork.IPAllocationMethodStatic, addr: "10.0.1.4",
			wantCode: "PrivateIPAddressIsAllocated",
		},
		{
			name: "static reserved", method: armnetwork.IPAllocationMethodStatic, addr: "10.0.1.2",
			wantCode: "PrivateIPAddressIsReserved",
		},
		{
			name: "static outside subnet", method: armnetwork.IPAllocationMethodStatic, addr: "10.0.9.9",
			wantCode: "PrivateIPAddressNotInSubnet",
		},
	}

	for i, tt := range tests {
		lb, lerr := s.putLB(t, "ilb"+string(rune('a'+i)), internal(tt.method, tt.addr))
		if tt.wantCode != "" {
			require.Error(t, lerr, tt.name)
			assert.Equal(t, tt.wantCode, sdkErrCode(lerr), tt.name)

			continue
		}

		require.NoError(t, lerr, tt.name)
		assert.Equal(t, tt.wantIP, *lb.Properties.FrontendIPConfigurations[0].Properties.PrivateIPAddress, tt.name)
	}

	// the frontend address is taken for NICs too, and the subnet is in use
	_, err = s.nic(t, "nic2", sid, armnetwork.IPAllocationMethodStatic, "10.0.1.5", nil)
	require.Error(t, err)
	assert.Equal(t, "PrivateIPAddressIsAllocated", sdkErrCode(err))

	_, err = s.net.NewSubnetsClient().BeginDelete(ctx, netRG, "vnet1", "a", nil)
	require.Error(t, err)
	assert.Equal(t, "InUseSubnetCannotBeDeleted", sdkErrCode(err))

	avail, err := s.net.NewVirtualNetworksClient().CheckIPAddressAvailability(ctx, netRG, "vnet1", "10.0.1.5", nil)
	require.NoError(t, err)
	assert.False(t, *avail.Available)
}
