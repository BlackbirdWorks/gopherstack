package azurearm_test

import (
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (s *netStack) asg(t *testing.T, name, loc string) (armnetwork.ApplicationSecurityGroup, error) {
	t.Helper()

	p, err := s.net.NewApplicationSecurityGroupsClient().BeginCreateOrUpdate(t.Context(), netRG, name,
		armnetwork.ApplicationSecurityGroup{Location: new(loc)}, nil)
	if err != nil {
		return armnetwork.ApplicationSecurityGroup{}, err
	}

	res, err := p.PollUntilDone(t.Context(), pollFast())

	return res.ApplicationSecurityGroup, err
}

func (s *netStack) nsg(t *testing.T, name string) armnetwork.SecurityGroup {
	t.Helper()

	p, err := s.net.NewSecurityGroupsClient().BeginCreateOrUpdate(t.Context(), netRG, name,
		armnetwork.SecurityGroup{Location: new("westus")}, nil)

	return netPoll(t, p, err).SecurityGroup
}

func TestNetworkSDK_ApplicationSecurityGroups(t *testing.T) {
	t.Parallel()

	s := newNetStack(t)
	ctx := t.Context()
	v1 := s.vnet(t, "vnet1", []string{"10.0.0.0/16"}, map[string]string{"a": "10.0.1.0/24"})
	v2 := s.vnet(t, "vnet2", []string{"10.1.0.0/16"}, map[string]string{"a": "10.1.1.0/24"})

	web, err := s.asg(t, "web", "westus")
	require.NoError(t, err)
	assert.Equal(t, "Succeeded", string(*web.Properties.ProvisioningState))
	assert.NotEmpty(t, *web.Properties.ResourceGUID)

	db, err := s.asg(t, "db", "westus")
	require.NoError(t, err)

	_, err = s.net.NewApplicationSecurityGroupsClient().Get(ctx, netRG, "nope", nil)
	assert.Equal(t, "ResourceNotFound", sdkErrCode(err))

	pager := s.net.NewApplicationSecurityGroupsClient().NewListPager(netRG, nil)
	count := 0

	for pager.More() {
		page, perr := pager.NextPage(ctx)
		require.NoError(t, perr)

		count += len(page.Value)
	}

	assert.Equal(t, 2, count)

	// NIC membership, with the same-virtual-network rule
	n1, err := s.nicWith(t, "nic1", *v1.Properties.Subnets[0].ID, &armnetwork.InterfaceIPConfigurationPropertiesFormat{
		ApplicationSecurityGroups: []*armnetwork.ApplicationSecurityGroup{{ID: web.ID}},
	})
	require.NoError(t, err)
	require.Len(t, n1.Properties.IPConfigurations[0].Properties.ApplicationSecurityGroups, 1)

	_, err = s.nicWith(t, "nic2", *v2.Properties.Subnets[0].ID, &armnetwork.InterfaceIPConfigurationPropertiesFormat{
		ApplicationSecurityGroups: []*armnetwork.ApplicationSecurityGroup{{ID: web.ID}},
	})
	require.Error(t, err)
	assert.Equal(t, "ApplicationSecurityGroupVirtualNetworkMismatch", sdkErrCode(err))

	_, err = s.nicWith(t, "nic3", *v1.Properties.Subnets[0].ID, &armnetwork.InterfaceIPConfigurationPropertiesFormat{
		ApplicationSecurityGroups: []*armnetwork.ApplicationSecurityGroup{{ID: new(*web.ID + "x")}},
	})
	require.Error(t, err)
	assert.Equal(t, "InvalidResourceReference", sdkErrCode(err))

	// the db group lives in a different region than the NIC
	east, err := s.asg(t, "east", "eastus")
	require.NoError(t, err)

	_, err = s.nicWith(t, "nic4", *v1.Properties.Subnets[0].ID, &armnetwork.InterfaceIPConfigurationPropertiesFormat{
		ApplicationSecurityGroups: []*armnetwork.ApplicationSecurityGroup{{ID: east.ID}},
	})
	require.Error(t, err)
	assert.Equal(t, "InvalidResourceReference", sdkErrCode(err))

	// NSG rules
	s.nsg(t, "nsg1")

	rules := s.net.NewSecurityRulesClient()
	base := func() armnetwork.SecurityRule {
		return armnetwork.SecurityRule{Properties: &armnetwork.SecurityRulePropertiesFormat{
			Protocol: new(armnetwork.SecurityRuleProtocolTCP), Access: new(armnetwork.SecurityRuleAccessAllow),
			Direction: new(armnetwork.SecurityRuleDirectionInbound), Priority: new(int32(100)),
			SourcePortRange: new("*"), DestinationPortRange: new("1433"),
		}}
	}

	tests := []struct {
		mutate   func(r *armnetwork.SecurityRule)
		name     string
		wantCode string
	}{
		{
			name: "asg to asg",
			mutate: func(r *armnetwork.SecurityRule) {
				r.Properties.SourceApplicationSecurityGroups = []*armnetwork.ApplicationSecurityGroup{{ID: web.ID}}
				r.Properties.DestinationApplicationSecurityGroups = []*armnetwork.ApplicationSecurityGroup{{ID: db.ID}}
			},
		},
		{
			name: "asg with prefix",
			mutate: func(r *armnetwork.SecurityRule) {
				r.Properties.SourceApplicationSecurityGroups = []*armnetwork.ApplicationSecurityGroup{{ID: web.ID}}
				r.Properties.SourceAddressPrefix = new("10.0.0.0/8")
				r.Properties.DestinationAddressPrefix = new("*")
			},
			wantCode: "SecurityRuleParameterContainsConflictingValues",
		},
		{
			name: "missing asg",
			mutate: func(r *armnetwork.SecurityRule) {
				r.Properties.SourceApplicationSecurityGroups = []*armnetwork.ApplicationSecurityGroup{
					{ID: new(*web.ID + "x")},
				}
				r.Properties.DestinationAddressPrefix = new("*")
			},
			wantCode: "InvalidResourceReference",
		},
		{
			name: "asg in other region",
			mutate: func(r *armnetwork.SecurityRule) {
				r.Properties.SourceApplicationSecurityGroups = []*armnetwork.ApplicationSecurityGroup{{ID: east.ID}}
				r.Properties.DestinationAddressPrefix = new("*")
			},
			wantCode: "InvalidResourceReference",
		},
	}

	for _, tt := range tests {
		r := base()
		tt.mutate(&r)

		p, rerr := rules.BeginCreateOrUpdate(ctx, netRG, "nsg1", "r-"+string(rune('a'+len(tt.name)%26)), r, nil)
		if tt.wantCode != "" {
			require.Error(t, rerr, tt.name)
			assert.Equal(t, tt.wantCode, sdkErrCode(rerr), tt.name)

			continue
		}

		got := netPoll(t, p, rerr)
		assert.Len(t, got.Properties.SourceApplicationSecurityGroups, 1, tt.name)
		assert.Len(t, got.Properties.DestinationApplicationSecurityGroups, 1, tt.name)
	}

	// in-use deletion
	for _, name := range []string{"web", "db"} {
		dp, derr := s.net.NewApplicationSecurityGroupsClient().BeginDelete(ctx, netRG, name, nil)
		require.Error(t, derr, name)
		assert.Nil(t, dp)
		assert.Equal(t, "InUseApplicationSecurityGroupCannotBeDeleted", sdkErrCode(derr), name)
	}

	// release: drop the NIC membership and the rule, then delete
	_, err = s.nicWith(t, "nic1", *v1.Properties.Subnets[0].ID, &armnetwork.InterfaceIPConfigurationPropertiesFormat{})
	require.NoError(t, err)

	rp, err := rules.BeginDelete(ctx, netRG, "nsg1", "r-"+string(rune('a'+len("asg to asg")%26)), nil)
	netPoll(t, rp, err)

	for _, name := range []string{"web", "db"} {
		dp, derr := s.net.NewApplicationSecurityGroupsClient().BeginDelete(ctx, netRG, name, nil)
		netPoll(t, dp, derr)
	}
}
