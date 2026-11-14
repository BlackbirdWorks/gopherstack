package azurearm_test

import (
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNetworkSDK_RouteFilters(t *testing.T) {
	t.Parallel()

	s := newNetStack(t)
	ctx := t.Context()
	filters := s.net.NewRouteFiltersClient()
	rules := s.net.NewRouteFilterRulesClient()

	rule := func(access armnetwork.Access, comms ...string) armnetwork.RouteFilterRule {
		r := armnetwork.RouteFilterRule{Properties: &armnetwork.RouteFilterRulePropertiesFormat{
			Access: new(access), RouteFilterRuleType: new(armnetwork.RouteFilterRuleTypeCommunity),
		}}
		for _, c := range comms {
			r.Properties.Communities = append(r.Properties.Communities, new(c))
		}

		return r
	}

	_, err := filters.BeginCreateOrUpdate(ctx, netRG, "noloc", armnetwork.RouteFilter{}, nil)
	require.Error(t, err)
	assert.Equal(t, "LocationRequired", sdkErrCode(err))

	rf := armnetwork.RouteFilter{
		Location: new("westus"), Tags: map[string]*string{"k": new("v")},
		Properties: &armnetwork.RouteFilterPropertiesFormat{
			Rules: []*armnetwork.RouteFilterRule{func() *armnetwork.RouteFilterRule {
				r := rule(armnetwork.AccessAllow, "12076:5010")
				r.Name = new("inline")

				return &r
			}()},
		},
	}

	p, err := filters.BeginCreateOrUpdate(ctx, netRG, "rf1", rf, nil)
	got := netPoll(t, p, err)
	require.Len(t, got.Properties.Rules, 1)
	assert.Equal(t, "westus", *got.Properties.Rules[0].Location)
	assert.Equal(t, "Succeeded", string(*got.Properties.ProvisioningState))

	tests := []struct {
		rule     armnetwork.RouteFilterRule
		name     string
		wantCode string
	}{
		{name: "ok", rule: rule(armnetwork.AccessAllow, "12076:5011")},
		{name: "no communities", rule: rule(armnetwork.AccessAllow), wantCode: "InvalidRequestFormat"},
		{name: "bad community", rule: rule(armnetwork.AccessAllow, "nope"), wantCode: "InvalidRequestFormat"},
		{name: "bad access", rule: rule("Maybe", "1:1"), wantCode: "InvalidRequestFormat"},
	}

	for _, tt := range tests {
		rp, rerr := rules.BeginCreateOrUpdate(ctx, netRG, "rf1", "r-"+string(rune('a'+len(tt.name)%20)), tt.rule, nil)
		if rerr == nil {
			_, rerr = rp.PollUntilDone(ctx, pollFast())
		}

		if tt.wantCode == "" {
			require.NoError(t, rerr, tt.name)

			continue
		}

		require.Error(t, rerr, tt.name)
		assert.Equal(t, tt.wantCode, sdkErrCode(rerr), tt.name)
	}

	one, err := rules.Get(ctx, netRG, "rf1", "inline", nil)
	require.NoError(t, err)
	assert.Equal(t, "12076:5010", *one.Properties.Communities[0])

	var names []string

	for pager := rules.NewListByRouteFilterPager(netRG, "rf1", nil); pager.More(); {
		page, perr := pager.NextPage(ctx)
		require.NoError(t, perr)

		for _, r := range page.Value {
			names = append(names, *r.Name)
		}
	}

	assert.Len(t, names, 2)

	tagged, err := filters.UpdateTags(
		ctx,
		netRG,
		"rf1",
		armnetwork.TagsObject{Tags: map[string]*string{"k": new("v2")}},
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, "v2", *tagged.Tags["k"])
	assert.Len(t, tagged.Properties.Rules, 2)

	dp, err := rules.BeginDelete(ctx, netRG, "rf1", "inline", nil)
	netPoll(t, dp, err)

	listed := 0

	for pager := filters.NewListByResourceGroupPager(netRG, nil); pager.More(); {
		page, perr := pager.NextPage(ctx)
		require.NoError(t, perr)

		listed += len(page.Value)
	}

	assert.Equal(t, 1, listed)

	fp, err := filters.BeginDelete(ctx, netRG, "rf1", nil)
	netPoll(t, fp, err)

	_, err = filters.Get(ctx, netRG, "rf1", nil)
	require.Error(t, err)
}

func TestNetworkSDK_ServiceEndpointPolicies(t *testing.T) {
	t.Parallel()

	s := newNetStack(t)
	ctx := t.Context()
	policies := s.net.NewServiceEndpointPoliciesClient()
	defs := s.net.NewServiceEndpointPolicyDefinitionsClient()

	def := func(service string, res ...string) armnetwork.ServiceEndpointPolicyDefinition {
		d := armnetwork.ServiceEndpointPolicyDefinition{
			Properties: &armnetwork.ServiceEndpointPolicyDefinitionPropertiesFormat{
				Service: new(service), Description: new("d"),
			},
		}
		for _, r := range res {
			d.Properties.ServiceResources = append(d.Properties.ServiceResources, new(r))
		}

		return d
	}

	p, err := policies.BeginCreateOrUpdate(ctx, netRG, "sep1", armnetwork.ServiceEndpointPolicy{
		Location: new("westus"),
	}, nil)
	pol := netPoll(t, p, err)
	assert.NotEmpty(t, *pol.Properties.ResourceGUID)
	assert.Empty(t, pol.Properties.ServiceEndpointPolicyDefinitions)

	tests := []struct {
		def      armnetwork.ServiceEndpointPolicyDefinition
		name     string
		wantCode string
	}{
		{name: "ok", def: def("Microsoft.Storage", "/subscriptions/"+aciSub)},
		{name: "no service", def: def("", "/subscriptions/"+aciSub), wantCode: "InvalidRequestFormat"},
		{name: "bad resource", def: def("Microsoft.Storage", "acct"), wantCode: "InvalidRequestFormat"},
	}

	for _, tt := range tests {
		dp, derr := defs.BeginCreateOrUpdate(ctx, netRG, "sep1", "def-"+string(rune('a'+len(tt.name)%20)), tt.def, nil)
		if derr == nil {
			_, derr = dp.PollUntilDone(ctx, pollFast())
		}

		if tt.wantCode == "" {
			require.NoError(t, derr, tt.name)

			continue
		}

		require.Error(t, derr, tt.name)
		assert.Equal(t, tt.wantCode, sdkErrCode(derr), tt.name)
	}

	got, err := policies.Get(ctx, netRG, "sep1", nil)
	require.NoError(t, err)
	require.Len(t, got.Properties.ServiceEndpointPolicyDefinitions, 1)

	v := s.vnet(t, "vnet1", []string{"10.0.0.0/16"}, map[string]string{"a": "10.0.1.0/24"})
	sn := *v.Properties.Subnets[0]
	sn.Properties.ServiceEndpointPolicies = []*armnetwork.ServiceEndpointPolicy{{ID: new(*pol.ID + "-ghost")}}

	sp, err := s.net.NewSubnetsClient().BeginCreateOrUpdate(ctx, netRG, "vnet1", "a", sn, nil)
	if err == nil {
		_, err = sp.PollUntilDone(ctx, pollFast())
	}

	require.Error(t, err)
	assert.Equal(t, "InvalidResourceReference", sdkErrCode(err))

	sn.Properties.ServiceEndpointPolicies[0].ID = pol.ID
	sp, err = s.net.NewSubnetsClient().BeginCreateOrUpdate(ctx, netRG, "vnet1", "a", sn, nil)
	netPoll(t, sp, err)

	got, err = policies.Get(ctx, netRG, "sep1", nil)
	require.NoError(t, err)
	require.Len(t, got.Properties.Subnets, 1)
	assert.Equal(t, *sn.ID, *got.Properties.Subnets[0].ID)

	_, err = policies.BeginDelete(ctx, netRG, "sep1", nil)
	require.Error(t, err)
	assert.Equal(t, "InUseServiceEndpointPolicyCannotBeDeleted", sdkErrCode(err))

	tagged, err := policies.UpdateTags(
		ctx,
		netRG,
		"sep1",
		armnetwork.TagsObject{Tags: map[string]*string{"a": new("b")}},
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, "b", *tagged.Tags["a"])

	sn.Properties.ServiceEndpointPolicies = nil
	sp, err = s.net.NewSubnetsClient().BeginCreateOrUpdate(ctx, netRG, "vnet1", "a", sn, nil)
	netPoll(t, sp, err)

	dd, err := defs.BeginDelete(ctx, netRG, "sep1", "def-"+string(rune('a'+len("ok")%20)), nil)
	netPoll(t, dd, err)

	dp, err := policies.BeginDelete(ctx, netRG, "sep1", nil)
	netPoll(t, dp, err)
}
