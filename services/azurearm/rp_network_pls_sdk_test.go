package azurearm_test

import (
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type plsEnv struct {
	*netStack
	plsSubnet string
	peSubnet  string
	frontend  string
}

func newPLSEnv(t *testing.T) *plsEnv {
	t.Helper()

	s := newNetStack(t)
	v := s.vnet(t, "vnet1", []string{"10.0.0.0/16"},
		map[string]string{"pls": "10.0.1.0/24", "pe": "10.0.2.0/24", "lb": "10.0.3.0/24"})

	ids := map[string]string{}
	for _, sn := range v.Properties.Subnets {
		ids[*sn.Name] = *sn.ID
	}

	sp, err := s.net.NewSubnetsClient().BeginCreateOrUpdate(t.Context(), netRG, "vnet1", "pls", armnetwork.Subnet{
		Properties: &armnetwork.SubnetPropertiesFormat{
			AddressPrefix:                     new("10.0.1.0/24"),
			PrivateLinkServiceNetworkPolicies: new(armnetwork.VirtualNetworkPrivateLinkServiceNetworkPoliciesDisabled),
		},
	}, nil)
	netPoll(t, sp, err)

	lb, err := s.putLB(t, "ilb", armnetwork.LoadBalancer{
		Location: new("westus"),
		SKU:      &armnetwork.LoadBalancerSKU{Name: new(armnetwork.LoadBalancerSKUNameStandard)},
		Properties: &armnetwork.LoadBalancerPropertiesFormat{
			FrontendIPConfigurations: []*armnetwork.FrontendIPConfiguration{{
				Name: new("fe"),
				Properties: &armnetwork.FrontendIPConfigurationPropertiesFormat{
					Subnet: &armnetwork.Subnet{ID: new(ids["lb"])},
				},
			}},
		},
	})
	require.NoError(t, err)

	return &plsEnv{
		netStack: s, plsSubnet: ids["pls"], peSubnet: ids["pe"],
		frontend: *lb.Properties.FrontendIPConfigurations[0].ID,
	}
}

func (e *plsEnv) service(t *testing.T, name string, mod func(*armnetwork.PrivateLinkServiceProperties)) error {
	t.Helper()

	props := &armnetwork.PrivateLinkServiceProperties{
		LoadBalancerFrontendIPConfigurations: []*armnetwork.FrontendIPConfiguration{{ID: new(e.frontend)}},
		IPConfigurations: []*armnetwork.PrivateLinkServiceIPConfiguration{{
			Name: new("nat"),
			Properties: &armnetwork.PrivateLinkServiceIPConfigurationProperties{
				Subnet: &armnetwork.Subnet{ID: new(e.plsSubnet)},
			},
		}},
	}
	if mod != nil {
		mod(props)
	}

	p, err := e.net.NewPrivateLinkServicesClient().BeginCreateOrUpdate(t.Context(), netRG, name,
		armnetwork.PrivateLinkService{Location: new("westus"), Properties: props}, nil)
	if err != nil {
		return err
	}

	_, err = p.PollUntilDone(t.Context(), pollFast())

	return err
}

func (e *plsEnv) endpoint(t *testing.T, name string, manual bool, plsID string) {
	t.Helper()

	conn := &armnetwork.PrivateLinkServiceConnection{
		Name:       new("conn"),
		Properties: &armnetwork.PrivateLinkServiceConnectionProperties{PrivateLinkServiceID: new(plsID)},
	}
	props := &armnetwork.PrivateEndpointProperties{Subnet: &armnetwork.Subnet{ID: new(e.peSubnet)}}

	if manual {
		props.ManualPrivateLinkServiceConnections = []*armnetwork.PrivateLinkServiceConnection{conn}
	} else {
		props.PrivateLinkServiceConnections = []*armnetwork.PrivateLinkServiceConnection{conn}
	}

	p, err := e.net.NewPrivateEndpointsClient().BeginCreateOrUpdate(t.Context(), netRG, name,
		armnetwork.PrivateEndpoint{Location: new("westus"), Properties: props}, nil)

	netPoll(t, p, err)
}

func TestNetworkSDK_PrivateLinkServiceValidation(t *testing.T) {
	t.Parallel()

	e := newPLSEnv(t)

	tests := []struct {
		mod      func(*armnetwork.PrivateLinkServiceProperties)
		name     string
		wantCode string
	}{
		{
			name:     "policies enabled",
			wantCode: "InvalidRequestFormat",
			mod: func(p *armnetwork.PrivateLinkServiceProperties) {
				p.IPConfigurations[0].Properties.Subnet.ID = new(e.peSubnet)
			},
		},
		{name: "no frontend", wantCode: "InvalidRequestFormat", mod: func(p *armnetwork.PrivateLinkServiceProperties) {
			p.LoadBalancerFrontendIPConfigurations = nil
		}},
		{
			name:     "ghost frontend",
			wantCode: "InvalidResourceReference",
			mod: func(p *armnetwork.PrivateLinkServiceProperties) {
				p.LoadBalancerFrontendIPConfigurations[0].ID = new(e.frontend + "x")
			},
		},
		{name: "no ip config", wantCode: "InvalidRequestFormat", mod: func(p *armnetwork.PrivateLinkServiceProperties) {
			p.IPConfigurations = nil
		}},
		{
			name:     "bad subscription",
			wantCode: "InvalidRequestFormat",
			mod: func(p *armnetwork.PrivateLinkServiceProperties) {
				p.Visibility = &armnetwork.PrivateLinkServicePropertiesVisibility{Subscriptions: []*string{new("nope")}}
			},
		},
		{name: "static address", mod: func(p *armnetwork.PrivateLinkServiceProperties) {
			cp := p.IPConfigurations[0].Properties
			cp.PrivateIPAllocationMethod = new(armnetwork.IPAllocationMethodStatic)
			cp.PrivateIPAddress = new("10.0.1.9")
		}},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := e.service(t, "svc"+string(rune('a'+i)), tt.mod)
			if tt.wantCode == "" {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			assert.Equal(t, tt.wantCode, sdkErrCode(err))
		})
	}
}

func TestNetworkSDK_PrivateLinkServiceConnections(t *testing.T) {
	t.Parallel()

	e := newPLSEnv(t)
	ctx := t.Context()
	pls := e.net.NewPrivateLinkServicesClient()

	require.NoError(t, e.service(t, "svc", func(p *armnetwork.PrivateLinkServiceProperties) {
		p.AutoApproval = &armnetwork.PrivateLinkServicePropertiesAutoApproval{Subscriptions: []*string{new(aciSub)}}
	}))

	svc, err := pls.Get(ctx, netRG, "svc", nil)
	require.NoError(t, err)
	assert.Contains(t, *svc.Properties.Alias, "svc.")
	assert.Contains(t, *svc.Properties.Alias, ".westus.azure.privatelinkservice")
	require.Len(t, svc.Properties.NetworkInterfaces, 1)
	assert.Equal(t, "10.0.1.4",
		*svc.Properties.IPConfigurations[0].Properties.PrivateIPAddress)
	assert.True(t, *svc.Properties.IPConfigurations[0].Properties.Primary)

	nic, err := e.net.NewInterfacesClient().Get(ctx, netRG, nicNameOf(*svc.Properties.NetworkInterfaces[0].ID), nil)
	require.NoError(t, err)
	assert.Equal(t, *svc.ID, *nic.Properties.PrivateLinkService.ID)

	_, err = e.net.NewInterfacesClient().
		BeginDelete(ctx, netRG, nicNameOf(*svc.Properties.NetworkInterfaces[0].ID), nil)
	require.Error(t, err)

	// manual connections wait for the owner; auto connections of an auto-approved subscription do not
	e.endpoint(t, "pe-manual", true, *svc.ID)
	e.endpoint(t, "pe-auto", false, *svc.ID)

	var conns []*armnetwork.PrivateEndpointConnection

	for pager := pls.NewListPrivateEndpointConnectionsPager(netRG, "svc", nil); pager.More(); {
		page, perr := pager.NextPage(ctx)
		require.NoError(t, perr)

		conns = append(conns, page.Value...)
	}

	require.Len(t, conns, 2)

	states := map[string]string{}
	for _, c := range conns {
		states[*c.Name] = *c.Properties.PrivateLinkServiceConnectionState.Status
		assert.Equal(t, "Microsoft.Network/privateLinkServices/privateEndpointConnections", *c.Type)
	}

	var manual *armnetwork.PrivateEndpointConnection

	for _, c := range conns {
		if states[*c.Name] == "Pending" {
			manual = c
		}
	}

	require.NotNil(t, manual)
	assert.Contains(t, states, *manual.Name)

	approved, err := pls.UpdatePrivateEndpointConnection(
		ctx,
		netRG,
		"svc",
		*manual.Name,
		armnetwork.PrivateEndpointConnection{
			Properties: &armnetwork.PrivateEndpointConnectionProperties{
				PrivateLinkServiceConnectionState: &armnetwork.PrivateLinkServiceConnectionState{
					Status: new("Approved"), Description: new("ok"),
				},
			},
		},
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, "Approved", *approved.Properties.PrivateLinkServiceConnectionState.Status)

	got, err := e.net.NewPrivateEndpointsClient().Get(ctx, netRG, "pe-manual", nil)
	require.NoError(t, err)
	assert.Equal(t, "Approved",
		*got.Properties.ManualPrivateLinkServiceConnections[0].Properties.PrivateLinkServiceConnectionState.Status)
	assert.Equal(t, "10.0.2.4", e.peAddress(t, got.PrivateEndpoint))

	one, err := pls.GetPrivateEndpointConnection(ctx, netRG, "svc", *manual.Name, nil)
	require.NoError(t, err)
	assert.Equal(t, "ok", *one.Properties.PrivateLinkServiceConnectionState.Description)

	dp, err := pls.BeginDeletePrivateEndpointConnection(ctx, netRG, "svc", *manual.Name, nil)
	netPoll(t, dp, err)

	got, err = e.net.NewPrivateEndpointsClient().Get(ctx, netRG, "pe-manual", nil)
	require.NoError(t, err)
	assert.Equal(t, "Disconnected",
		*got.Properties.ManualPrivateLinkServiceConnections[0].Properties.PrivateLinkServiceConnectionState.Status)

	e.checkPLSDeletes(t, svc)
}

func nicNameOf(id string) string {
	for i := len(id) - 1; i >= 0; i-- {
		if id[i] == '/' {
			return id[i+1:]
		}
	}

	return id
}

func (e *plsEnv) peAddress(t *testing.T, pe armnetwork.PrivateEndpoint) string {
	t.Helper()

	nic, err := e.net.NewInterfacesClient().
		Get(t.Context(), netRG, nicNameOf(*pe.Properties.NetworkInterfaces[0].ID), nil)
	require.NoError(t, err)

	cfg := nic.Properties.IPConfigurations[0]
	assert.Nil(t, cfg.Properties.PrivateLinkConnectionProperties)

	return *cfg.Properties.PrivateIPAddress
}

func (e *plsEnv) checkPLSDeletes(t *testing.T, svc armnetwork.PrivateLinkServicesClientGetResponse) {
	t.Helper()

	ctx := t.Context()

	_, err := e.net.NewLoadBalancersClient().BeginDelete(ctx, netRG, "ilb", nil)
	require.Error(t, err)
	assert.Equal(t, "InUseLoadBalancerCannotBeDeleted", sdkErrCode(err))

	_, err = e.net.NewLoadBalancerFrontendIPConfigurationsClient().Get(ctx, netRG, "ilb", "fe", nil)
	require.NoError(t, err)

	dp, err := e.net.NewPrivateLinkServicesClient().BeginDelete(ctx, netRG, "svc", nil)
	netPoll(t, dp, err)

	_, err = e.net.NewInterfacesClient().Get(ctx, netRG, nicNameOf(*svc.Properties.NetworkInterfaces[0].ID), nil)
	require.Error(t, err)

	pe, err := e.net.NewPrivateEndpointsClient().Get(ctx, netRG, "pe-auto", nil)
	require.NoError(t, err)
	assert.Equal(t, "Disconnected",
		*pe.Properties.PrivateLinkServiceConnections[0].Properties.PrivateLinkServiceConnectionState.Status)

	dl, err := e.net.NewLoadBalancersClient().BeginDelete(ctx, netRG, "ilb", nil)
	netPoll(t, dl, err)
}

func TestNetworkSDK_PrivateLinkServiceVisibility(t *testing.T) {
	t.Parallel()

	e := newPLSEnv(t)
	ctx := t.Context()
	pls := e.net.NewPrivateLinkServicesClient()
	other := "11111111-1111-1111-1111-111111111111"

	require.NoError(t, e.service(t, "svc", func(p *armnetwork.PrivateLinkServiceProperties) {
		p.Visibility = &armnetwork.PrivateLinkServicePropertiesVisibility{Subscriptions: []*string{new(other)}}
		p.AutoApproval = &armnetwork.PrivateLinkServicePropertiesAutoApproval{Subscriptions: []*string{new(aciSub)}}
	}))

	svc, err := pls.Get(ctx, netRG, "svc", nil)
	require.NoError(t, err)

	check := func(loc, alias string, byGroup bool) bool {
		req := armnetwork.CheckPrivateLinkServiceVisibilityRequest{PrivateLinkServiceAlias: new(alias)}

		var res *runtime.Poller[armnetwork.PrivateLinkServicesClientCheckPrivateLinkServiceVisibilityResponse]
		if !byGroup {
			res, err = pls.BeginCheckPrivateLinkServiceVisibility(ctx, loc, req, nil)
			require.NoError(t, err)

			out, perr := res.PollUntilDone(ctx, pollFast())
			require.NoError(t, perr)

			return *out.Visible
		}

		gp, gerr := pls.BeginCheckPrivateLinkServiceVisibilityByResourceGroup(ctx, loc, netRG, req, nil)
		require.NoError(t, gerr)

		out, perr := gp.PollUntilDone(ctx, pollFast())
		require.NoError(t, perr)

		return *out.Visible
	}

	assert.True(t, check("westus", *svc.Properties.Alias, false))
	assert.True(t, check("westus", *svc.Properties.Alias, true))
	assert.False(t, check("westus", "ghost.alias", false))
	assert.False(t, check("eastus", *svc.Properties.Alias, false))

	var auto []string

	for pager := pls.NewListAutoApprovedPrivateLinkServicesPager("westus", nil); pager.More(); {
		page, perr := pager.NextPage(ctx)
		require.NoError(t, perr)

		for _, a := range page.Value {
			auto = append(auto, *a.PrivateLinkService)
		}
	}

	assert.Equal(t, []string{*svc.ID}, auto)

	snap := e.handler.Snapshot(ctx)
	fresh := newNetStack(t)
	require.NoError(t, fresh.handler.Restore(ctx, snap))

	got, err := fresh.net.NewPrivateLinkServicesClient().Get(ctx, netRG, "svc", nil)
	require.NoError(t, err)
	assert.Equal(t, *svc.Properties.Alias, *got.Properties.Alias)
}
