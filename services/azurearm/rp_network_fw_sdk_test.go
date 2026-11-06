package azurearm_test

import (
	"encoding/json"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	fwName  = "fw1"
	fwSubID = "/subscriptions/" + aciSub + "/resourceGroups/" + netRG + "/providers/Microsoft.Network/"
)

type fwFixture struct {
	*netStack
	fwSubnet, mgmtSubnet, workSubnet, pip1, pip2, mgmtPIP string
}

func newFWFixture(t *testing.T) *fwFixture {
	t.Helper()

	s := newNetStack(t)
	v := s.vnet(t, "hub", []string{"10.0.0.0/16"}, map[string]string{
		"AzureFirewallSubnet": "10.0.0.0/26", "AzureFirewallManagementSubnet": "10.0.1.0/26", "work": "10.0.2.0/24",
		"tiny": "10.0.3.0/27",
	})

	f := &fwFixture{netStack: s}

	for _, sn := range v.Properties.Subnets {
		switch *sn.Name {
		case "AzureFirewallSubnet":
			f.fwSubnet = *sn.ID
		case "AzureFirewallManagementSubnet":
			f.mgmtSubnet = *sn.ID
		case "work":
			f.workSubnet = *sn.ID
		}
	}

	for name, dst := range map[string]*string{"fwpip1": &f.pip1, "fwpip2": &f.pip2, "mgmtpip": &f.mgmtPIP} {
		pip, err := s.pip(t, name, armnetwork.PublicIPAddressSKUNameStandard, "")
		require.NoError(t, err)

		*dst = *pip.ID
	}

	return f
}

func (f *fwFixture) firewall() armnetwork.AzureFirewall {
	return armnetwork.AzureFirewall{
		Location: new("westus"),
		Properties: &armnetwork.AzureFirewallPropertiesFormat{
			SKU: &armnetwork.AzureFirewallSKU{
				Name: new(armnetwork.AzureFirewallSKUNameAZFWVnet), Tier: new(armnetwork.AzureFirewallSKUTierStandard),
			},
			IPConfigurations: []*armnetwork.AzureFirewallIPConfiguration{{
				Name: new("ipc1"),
				Properties: &armnetwork.AzureFirewallIPConfigurationPropertiesFormat{
					Subnet:          &armnetwork.SubResource{ID: &f.fwSubnet},
					PublicIPAddress: &armnetwork.SubResource{ID: &f.pip1},
				},
			}},
		},
	}
}

func (f *fwFixture) putFirewall(
	t *testing.T,
	fw armnetwork.AzureFirewall,
) (armnetwork.AzureFirewall, error) {
	t.Helper()

	p, err := f.net.NewAzureFirewallsClient().BeginCreateOrUpdate(t.Context(), netRG, fwName, fw, nil)
	if err != nil {
		return armnetwork.AzureFirewall{}, err
	}

	res, err := p.PollUntilDone(t.Context(), pollFast())

	return res.AzureFirewall, err
}

func (f *fwFixture) putPolicy(
	t *testing.T,
	name, loc string,
	props armnetwork.FirewallPolicyPropertiesFormat,
) (armnetwork.FirewallPolicy, error) {
	t.Helper()

	p, err := f.net.NewFirewallPoliciesClient().BeginCreateOrUpdate(t.Context(), netRG, name,
		armnetwork.FirewallPolicy{Location: &loc, Properties: &props}, nil)
	if err != nil {
		return armnetwork.FirewallPolicy{}, err
	}

	res, err := p.PollUntilDone(t.Context(), pollFast())

	return res.FirewallPolicy, err
}

func (f *fwFixture) putIPGroup(t *testing.T, name string, addrs ...string) (armnetwork.IPGroup, error) {
	t.Helper()

	g := armnetwork.IPGroup{Location: new("westus"), Properties: &armnetwork.IPGroupPropertiesFormat{}}
	for _, a := range addrs {
		g.Properties.IPAddresses = append(g.Properties.IPAddresses, new(a))
	}

	p, err := f.net.NewIPGroupsClient().BeginCreateOrUpdate(t.Context(), netRG, name, g, nil)
	if err != nil {
		return armnetwork.IPGroup{}, err
	}

	res, err := p.PollUntilDone(t.Context(), pollFast())

	return res.IPGroup, err
}

func (f *fwFixture) putRCG(
	t *testing.T,
	policy, name string,
	prio int32,
	cols ...armnetwork.FirewallPolicyRuleCollectionClassification,
) error {
	t.Helper()

	p, err := f.net.NewFirewallPolicyRuleCollectionGroupsClient().BeginCreateOrUpdate(t.Context(), netRG, policy, name,
		armnetwork.FirewallPolicyRuleCollectionGroup{
			Properties: &armnetwork.FirewallPolicyRuleCollectionGroupProperties{Priority: &prio, RuleCollections: cols},
		}, nil)
	if err != nil {
		return err
	}

	_, err = p.PollUntilDone(t.Context(), pollFast())

	return err
}

func netRuleCollection(
	name string,
	prio int32,
	rule *armnetwork.Rule,
) armnetwork.FirewallPolicyRuleCollectionClassification {
	return &armnetwork.FirewallPolicyFilterRuleCollection{
		RuleCollectionType: new(armnetwork.FirewallPolicyRuleCollectionTypeFirewallPolicyFilterRuleCollection),
		Name:               &name, Priority: &prio,
		Action: &armnetwork.FirewallPolicyFilterRuleCollectionAction{
			Type: new(armnetwork.FirewallPolicyFilterRuleCollectionActionTypeAllow),
		},
		Rules: []armnetwork.FirewallPolicyRuleClassification{rule},
	}
}

func okNetRule() *armnetwork.Rule {
	return &armnetwork.Rule{
		RuleType: new(armnetwork.FirewallPolicyRuleTypeNetworkRule), Name: new("allow-dns"),
		IPProtocols: []*armnetwork.FirewallPolicyRuleNetworkProtocol{
			new(armnetwork.FirewallPolicyRuleNetworkProtocolUDP),
		},
		SourceAddresses:      []*string{new("10.0.2.0/24")},
		DestinationAddresses: []*string{new("168.63.129.16")},
		DestinationPorts:     []*string{new("53")},
	}
}

func TestNetworkSDK_AzureFirewallLifecycle(t *testing.T) {
	t.Parallel()

	f := newFWFixture(t)
	ctx := t.Context()

	got, err := f.putFirewall(t, f.firewall())
	require.NoError(t, err)
	assert.Equal(t, armnetwork.AzureFirewallThreatIntelModeAlert, *got.Properties.ThreatIntelMode)
	assert.Equal(t, armnetwork.ProvisioningStateSucceeded, *got.Properties.ProvisioningState)
	assert.Regexp(t, `^10\.0\.0\.\d+$`, *got.Properties.IPConfigurations[0].Properties.PrivateIPAddress)
	assert.Contains(t, *got.Properties.IPConfigurations[0].ID, "azureFirewalls/fw1/azureFirewallIpConfigurations/ipc1")

	c := f.net.NewAzureFirewallsClient()

	pager := c.NewListPager(netRG, nil)
	require.True(t, pager.More())

	page, err := pager.NextPage(ctx)
	require.NoError(t, err)
	assert.Len(t, page.Value, 1)

	_, err = f.net.NewPublicIPAddressesClient().BeginDelete(ctx, netRG, "fwpip1", nil)
	assert.Equal(t, "PublicIPAddressCannotBeDeleted", sdkErrCode(err))

	_, err = f.net.NewSubnetsClient().BeginDelete(ctx, netRG, "hub", "AzureFirewallSubnet", nil)
	assert.Equal(t, "InUseSubnetCannotBeDeleted", sdkErrCode(err))

	_, err = f.nic(t, "nic1", f.fwSubnet, armnetwork.IPAllocationMethodDynamic, "", nil)
	assert.Equal(t, "InvalidRequestFormat", sdkErrCode(err))

	del, err := c.BeginDelete(ctx, netRG, fwName, nil)
	require.NoError(t, err)
	_, err = del.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)

	_, err = c.Get(ctx, netRG, fwName, nil)
	assert.Equal(t, "ResourceNotFound", sdkErrCode(err))
}

func TestNetworkSDK_AzureFirewallValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate   func(f *fwFixture, fw *armnetwork.AzureFirewall)
		name     string
		wantCode string
	}{
		{name: "valid"},
		{
			name: "wrong subnet name", wantCode: "AzureFirewallSubnetNameInvalid",
			mutate: func(f *fwFixture, fw *armnetwork.AzureFirewall) {
				fw.Properties.IPConfigurations[0].Properties.Subnet = &armnetwork.SubResource{ID: &f.workSubnet}
			},
		},
		{
			name: "no subnet", wantCode: "InvalidRequestFormat",
			mutate: func(_ *fwFixture, fw *armnetwork.AzureFirewall) {
				fw.Properties.IPConfigurations[0].Properties.Subnet = nil
			},
		},
		{
			name: "no ip configuration", wantCode: "InvalidRequestFormat",
			mutate: func(_ *fwFixture, fw *armnetwork.AzureFirewall) { fw.Properties.IPConfigurations = nil },
		},
		{
			name: "no public ip", wantCode: "InvalidRequestFormat",
			mutate: func(_ *fwFixture, fw *armnetwork.AzureFirewall) {
				fw.Properties.IPConfigurations[0].Properties.PublicIPAddress = nil
			},
		},
		{
			name: "missing public ip", wantCode: "InvalidResourceReference",
			mutate: func(_ *fwFixture, fw *armnetwork.AzureFirewall) {
				fw.Properties.IPConfigurations[0].Properties.PublicIPAddress = &armnetwork.SubResource{
					ID: new(fwSubID + "publicIPAddresses/none"),
				}
			},
		},
		{
			name: "basic needs management", wantCode: "InvalidRequestFormat",
			mutate: func(_ *fwFixture, fw *armnetwork.AzureFirewall) {
				fw.Properties.SKU.Tier = new(armnetwork.AzureFirewallSKUTierBasic)
			},
		},
		{
			name: "basic with management",
			mutate: func(f *fwFixture, fw *armnetwork.AzureFirewall) {
				fw.Properties.SKU.Tier = new(armnetwork.AzureFirewallSKUTierBasic)
				fw.Properties.ManagementIPConfiguration = &armnetwork.AzureFirewallIPConfiguration{
					Name: new("mgmt"),
					Properties: &armnetwork.AzureFirewallIPConfigurationPropertiesFormat{
						Subnet: &armnetwork.SubResource{
							ID: &f.mgmtSubnet,
						}, PublicIPAddress: &armnetwork.SubResource{ID: &f.mgmtPIP},
					},
				}
			},
		},
		{
			name: "forced tunnelling without data plane ip",
			mutate: func(f *fwFixture, fw *armnetwork.AzureFirewall) {
				fw.Properties.IPConfigurations[0].Properties.PublicIPAddress = nil
				fw.Properties.ManagementIPConfiguration = &armnetwork.AzureFirewallIPConfiguration{
					Name: new("mgmt"),
					Properties: &armnetwork.AzureFirewallIPConfigurationPropertiesFormat{
						Subnet: &armnetwork.SubResource{
							ID: &f.mgmtSubnet,
						}, PublicIPAddress: &armnetwork.SubResource{ID: &f.mgmtPIP},
					},
				}
			},
		},
		{
			name: "management wrong subnet", wantCode: "AzureFirewallSubnetNameInvalid",
			mutate: func(f *fwFixture, fw *armnetwork.AzureFirewall) {
				fw.Properties.ManagementIPConfiguration = &armnetwork.AzureFirewallIPConfiguration{
					Name: new("mgmt"),
					Properties: &armnetwork.AzureFirewallIPConfigurationPropertiesFormat{
						Subnet: &armnetwork.SubResource{
							ID: &f.workSubnet,
						}, PublicIPAddress: &armnetwork.SubResource{ID: &f.mgmtPIP},
					},
				}
			},
		},
		{
			name: "management without public ip", wantCode: "InvalidRequestFormat",
			mutate: func(f *fwFixture, fw *armnetwork.AzureFirewall) {
				fw.Properties.ManagementIPConfiguration = &armnetwork.AzureFirewallIPConfiguration{
					Name: new("mgmt"),
					Properties: &armnetwork.AzureFirewallIPConfigurationPropertiesFormat{
						Subnet: &armnetwork.SubResource{ID: &f.mgmtSubnet},
					},
				}
			},
		},
		{
			name: "second public ip", mutate: func(f *fwFixture, fw *armnetwork.AzureFirewall) {
				fw.Properties.IPConfigurations = append(
					fw.Properties.IPConfigurations,
					&armnetwork.AzureFirewallIPConfiguration{
						Name: new("ipc2"),
						Properties: &armnetwork.AzureFirewallIPConfigurationPropertiesFormat{
							PublicIPAddress: &armnetwork.SubResource{ID: &f.pip2},
						},
					},
				)
			},
		},
		{
			name: "same public ip twice", wantCode: "PublicIPAddressInUse",
			mutate: func(f *fwFixture, fw *armnetwork.AzureFirewall) {
				fw.Properties.ManagementIPConfiguration = &armnetwork.AzureFirewallIPConfiguration{
					Name: new("mgmt"),
					Properties: &armnetwork.AzureFirewallIPConfigurationPropertiesFormat{
						Subnet: &armnetwork.SubResource{
							ID: &f.mgmtSubnet,
						}, PublicIPAddress: &armnetwork.SubResource{ID: &f.pip1},
					},
				}
			},
		},
		{
			name: "virtual hub sku", wantCode: "InvalidRequestFormat",
			mutate: func(_ *fwFixture, fw *armnetwork.AzureFirewall) {
				fw.Properties.SKU.Name = new(armnetwork.AzureFirewallSKUNameAZFWHub)
			},
		},
		{
			name: "bad threat intel mode", wantCode: "InvalidRequestFormat",
			mutate: func(_ *fwFixture, fw *armnetwork.AzureFirewall) {
				fw.Properties.ThreatIntelMode = new(armnetwork.AzureFirewallThreatIntelMode("Block"))
			},
		},
		{
			name: "missing policy", wantCode: "InvalidResourceReference",
			mutate: func(_ *fwFixture, fw *armnetwork.AzureFirewall) {
				fw.Properties.FirewallPolicy = &armnetwork.SubResource{ID: new(fwSubID + "firewallPolicies/none")}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newFWFixture(t)
			fw := f.firewall()

			if tc.mutate != nil {
				tc.mutate(f, &fw)
			}

			_, err := f.putFirewall(t, fw)
			if tc.wantCode == "" {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			assert.Equal(t, tc.wantCode, sdkErrCode(err))
		})
	}
}

func TestNetworkSDK_AzureFirewallSubnetSize(t *testing.T) {
	t.Parallel()

	f := newFWFixture(t)

	v, err := f.net.NewSubnetsClient().Get(t.Context(), netRG, "hub", "tiny", nil)
	require.NoError(t, err)

	fw := f.firewall()
	fw.Properties.IPConfigurations[0].Properties.Subnet = &armnetwork.SubResource{ID: v.ID}

	_, err = f.putFirewall(t, fw)
	assert.Equal(t, "AzureFirewallSubnetNameInvalid", sdkErrCode(err), "name is checked before size")
}

func TestNetworkSDK_FirewallPolicyHierarchy(t *testing.T) {
	t.Parallel()

	f := newFWFixture(t)
	ctx := t.Context()

	base, err := f.putPolicy(t, "base", "westus", armnetwork.FirewallPolicyPropertiesFormat{
		ThreatIntelMode: new(armnetwork.AzureFirewallThreatIntelModeAlert),
	})
	require.NoError(t, err)
	assert.Equal(t, armnetwork.FirewallPolicySKUTierStandard, *base.Properties.SKU.Tier)

	baseRef := &armnetwork.SubResource{ID: base.ID}

	tests := []struct {
		props    armnetwork.FirewallPolicyPropertiesFormat
		name     string
		loc      string
		wantCode string
	}{
		{name: "child", loc: "westus", props: armnetwork.FirewallPolicyPropertiesFormat{BasePolicy: baseRef}},
		{
			name: "stricter threat intel", loc: "westus",
			props: armnetwork.FirewallPolicyPropertiesFormat{
				BasePolicy: baseRef, ThreatIntelMode: new(armnetwork.AzureFirewallThreatIntelModeDeny),
			},
		},
		{
			name: "looser threat intel", loc: "westus", wantCode: "InvalidRequestFormat",
			props: armnetwork.FirewallPolicyPropertiesFormat{
				BasePolicy: baseRef, ThreatIntelMode: new(armnetwork.AzureFirewallThreatIntelModeOff),
			},
		},
		{
			name: "region mismatch", loc: "eastus", wantCode: "FirewallPolicyBasePolicyRegionMismatch",
			props: armnetwork.FirewallPolicyPropertiesFormat{BasePolicy: baseRef},
		},
		{
			name: "missing base", loc: "westus", wantCode: "InvalidResourceReference",
			props: armnetwork.FirewallPolicyPropertiesFormat{
				BasePolicy: &armnetwork.SubResource{ID: new(fwSubID + "firewallPolicies/none")},
			},
		},
		{
			name: "idps needs premium", loc: "westus", wantCode: "InvalidRequestFormat",
			props: armnetwork.FirewallPolicyPropertiesFormat{
				IntrusionDetection: &armnetwork.FirewallPolicyIntrusionDetection{
					Mode: new(armnetwork.FirewallPolicyIntrusionDetectionStateTypeAlert),
				},
			},
		},
		{
			name: "premium idps", loc: "westus",
			props: armnetwork.FirewallPolicyPropertiesFormat{
				SKU: &armnetwork.FirewallPolicySKU{Tier: new(armnetwork.FirewallPolicySKUTierPremium)},
				IntrusionDetection: &armnetwork.FirewallPolicyIntrusionDetection{
					Mode: new(armnetwork.FirewallPolicyIntrusionDetectionStateTypeAlert),
				},
			},
		},
		{
			name: "bad tier", loc: "westus", wantCode: "InvalidRequestFormat",
			props: armnetwork.FirewallPolicyPropertiesFormat{
				SKU: &armnetwork.FirewallPolicySKU{Tier: new(armnetwork.FirewallPolicySKUTier("Gold"))},
			},
		},
	}

	for i, tc := range tests {
		_, err = f.putPolicy(t, "p"+string(rune('a'+i)), tc.loc, tc.props)
		if tc.wantCode == "" {
			require.NoError(t, err, tc.name)

			continue
		}

		assert.Equal(t, tc.wantCode, sdkErrCode(err), tc.name)
	}

	// The base policy lists its children and cannot be deleted while they exist.
	got, err := f.net.NewFirewallPoliciesClient().Get(ctx, netRG, "base", nil)
	require.NoError(t, err)
	assert.Len(t, got.Properties.ChildPolicies, 2)

	_, err = f.net.NewFirewallPoliciesClient().BeginDelete(ctx, netRG, "base", nil)
	assert.Equal(t, "FirewallPolicyInUse", sdkErrCode(err))

	// A policy cannot be made its own ancestor.
	_, err = f.putPolicy(t, "base", "westus", armnetwork.FirewallPolicyPropertiesFormat{
		BasePolicy: &armnetwork.SubResource{ID: new(fwSubID + "firewallPolicies/pa")},
	})
	assert.Equal(t, "InvalidRequestFormat", sdkErrCode(err))

	_, err = f.putPolicy(t, "base", "westus", armnetwork.FirewallPolicyPropertiesFormat{BasePolicy: baseRef})
	assert.Equal(t, "InvalidRequestFormat", sdkErrCode(err))
}

func TestNetworkSDK_FirewallPolicyAssociation(t *testing.T) {
	t.Parallel()

	f := newFWFixture(t)
	ctx := t.Context()

	std, err := f.putPolicy(t, "std", "westus", armnetwork.FirewallPolicyPropertiesFormat{})
	require.NoError(t, err)

	prem, err := f.putPolicy(t, "prem", "westus", armnetwork.FirewallPolicyPropertiesFormat{
		SKU: &armnetwork.FirewallPolicySKU{Tier: new(armnetwork.FirewallPolicySKUTierPremium)},
	})
	require.NoError(t, err)

	fw := f.firewall()
	fw.Properties.FirewallPolicy = &armnetwork.SubResource{ID: prem.ID}

	_, err = f.putFirewall(t, fw)
	assert.Equal(t, "AzureFirewallPolicyTierMismatch", sdkErrCode(err))

	fw.Properties.FirewallPolicy = &armnetwork.SubResource{ID: std.ID}
	fw.Properties.NetworkRuleCollections = []*armnetwork.AzureFirewallNetworkRuleCollection{{
		Name: new("classic"),
		Properties: &armnetwork.AzureFirewallNetworkRuleCollectionPropertiesFormat{
			Priority: new(
				int32(100),
			), Action: &armnetwork.AzureFirewallRCAction{Type: new(armnetwork.AzureFirewallRCActionTypeAllow)},
			Rules: []*armnetwork.AzureFirewallNetworkRule{{
				Name: new(
					"r",
				), Protocols: []*armnetwork.AzureFirewallNetworkRuleProtocol{new(armnetwork.AzureFirewallNetworkRuleProtocolTCP)},
				SourceAddresses: []*string{
					new("*"),
				}, DestinationAddresses: []*string{new("*")}, DestinationPorts: []*string{new("443")},
			}},
		},
	}}

	_, err = f.putFirewall(t, fw)
	assert.Equal(t, "InvalidRequestFormat", sdkErrCode(err))

	fw.Properties.NetworkRuleCollections = nil
	_, err = f.putFirewall(t, fw)
	require.NoError(t, err)

	got, err := f.net.NewFirewallPoliciesClient().Get(ctx, netRG, "std", nil)
	require.NoError(t, err)
	require.Len(t, got.Properties.Firewalls, 1)
	assert.Contains(t, *got.Properties.Firewalls[0].ID, "azureFirewalls/fw1")

	_, err = f.net.NewFirewallPoliciesClient().BeginDelete(ctx, netRG, "std", nil)
	assert.Equal(t, "FirewallPolicyInUse", sdkErrCode(err))

	del, err := f.net.NewAzureFirewallsClient().BeginDelete(ctx, netRG, fwName, nil)
	require.NoError(t, err)
	_, err = del.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)

	pd, err := f.net.NewFirewallPoliciesClient().BeginDelete(ctx, netRG, "std", nil)
	require.NoError(t, err)
	_, err = pd.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)
}

func TestNetworkSDK_AzureFirewallClassicRules(t *testing.T) {
	t.Parallel()

	netCol := func(
		name string, prio int32, rule *armnetwork.AzureFirewallNetworkRule,
	) *armnetwork.AzureFirewallNetworkRuleCollection {
		return &armnetwork.AzureFirewallNetworkRuleCollection{
			Name: &name,
			Properties: &armnetwork.AzureFirewallNetworkRuleCollectionPropertiesFormat{
				Priority: &prio,
				Action:   &armnetwork.AzureFirewallRCAction{Type: new(armnetwork.AzureFirewallRCActionTypeDeny)},
				Rules:    []*armnetwork.AzureFirewallNetworkRule{rule},
			},
		}
	}
	rule := func(mod func(*armnetwork.AzureFirewallNetworkRule)) *armnetwork.AzureFirewallNetworkRule {
		r := &armnetwork.AzureFirewallNetworkRule{
			Name: new(
				"r",
			), Protocols: []*armnetwork.AzureFirewallNetworkRuleProtocol{new(armnetwork.AzureFirewallNetworkRuleProtocolTCP)},
			SourceAddresses: []*string{
				new("10.0.0.0/8"),
			}, DestinationAddresses: []*string{new("*")}, DestinationPorts: []*string{new("443")},
		}
		if mod != nil {
			mod(r)
		}

		return r
	}

	tests := []struct {
		name     string
		wantCode string
		cols     []*armnetwork.AzureFirewallNetworkRuleCollection
	}{
		{name: "valid", cols: []*armnetwork.AzureFirewallNetworkRuleCollection{netCol("c", 100, rule(nil))}},
		{
			name:     "priority low",
			wantCode: "InvalidRequestFormat",
			cols:     []*armnetwork.AzureFirewallNetworkRuleCollection{netCol("c", 99, rule(nil))},
		},
		{
			name:     "priority high",
			wantCode: "InvalidRequestFormat",
			cols:     []*armnetwork.AzureFirewallNetworkRuleCollection{netCol("c", 65001, rule(nil))},
		},
		{
			name: "duplicate priority", wantCode: "InvalidRequestFormat",
			cols: []*armnetwork.AzureFirewallNetworkRuleCollection{
				netCol("a", 100, rule(nil)),
				netCol("b", 100, rule(nil)),
			},
		},
		{
			name: "bad port", wantCode: "InvalidRequestFormat",
			cols: []*armnetwork.AzureFirewallNetworkRuleCollection{
				netCol("c", 100, rule(func(r *armnetwork.AzureFirewallNetworkRule) {
					r.DestinationPorts = []*string{new("99999")}
				})),
			},
		},
		{
			name: "bad source", wantCode: "InvalidRequestFormat",
			cols: []*armnetwork.AzureFirewallNetworkRuleCollection{
				netCol("c", 100, rule(func(r *armnetwork.AzureFirewallNetworkRule) {
					r.SourceAddresses = []*string{new("not an address!")}
				})),
			},
		},
		{
			name: "missing ip group", wantCode: "InvalidResourceReference",
			cols: []*armnetwork.AzureFirewallNetworkRuleCollection{
				netCol("c", 100, rule(func(r *armnetwork.AzureFirewallNetworkRule) {
					r.SourceAddresses = nil
					r.SourceIPGroups = []*string{new(fwSubID + "ipGroups/none")}
				})),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newFWFixture(t)
			fw := f.firewall()
			fw.Properties.NetworkRuleCollections = tc.cols

			got, err := f.putFirewall(t, fw)
			if tc.wantCode == "" {
				require.NoError(t, err)
				assert.Contains(t, *got.Properties.NetworkRuleCollections[0].ID, "networkRuleCollections/c")

				return
			}

			assert.Equal(t, tc.wantCode, sdkErrCode(err))
		})
	}
}

func TestNetworkSDK_RuleCollectionGroups(t *testing.T) {
	t.Parallel()

	f := newFWFixture(t)
	ctx := t.Context()

	_, err := f.putPolicy(t, "pol", "westus", armnetwork.FirewallPolicyPropertiesFormat{})
	require.NoError(t, err)

	appRule := func(mod func(*armnetwork.ApplicationRule)) *armnetwork.ApplicationRule {
		r := &armnetwork.ApplicationRule{
			RuleType: new(armnetwork.FirewallPolicyRuleTypeApplicationRule), Name: new("web"),
			Protocols: []*armnetwork.FirewallPolicyRuleApplicationProtocol{
				{ProtocolType: new(armnetwork.FirewallPolicyRuleApplicationProtocolTypeHTTPS), Port: new(int32(443))},
			},
			SourceAddresses: []*string{new("10.0.2.0/24")}, TargetFqdns: []*string{new("*.example.com")},
		}
		if mod != nil {
			mod(r)
		}

		return r
	}
	appCol := func(
		rules ...armnetwork.FirewallPolicyRuleClassification,
	) armnetwork.FirewallPolicyRuleCollectionClassification {
		return &armnetwork.FirewallPolicyFilterRuleCollection{
			RuleCollectionType: new(armnetwork.FirewallPolicyRuleCollectionTypeFirewallPolicyFilterRuleCollection),
			Name:               new("apps"), Priority: new(int32(200)),
			Action: &armnetwork.FirewallPolicyFilterRuleCollectionAction{
				Type: new(armnetwork.FirewallPolicyFilterRuleCollectionActionTypeAllow),
			},
			Rules: rules,
		}
	}
	natRule := func(mod func(*armnetwork.NatRule)) *armnetwork.NatRule {
		r := &armnetwork.NatRule{
			RuleType: new(armnetwork.FirewallPolicyRuleTypeNatRule), Name: new("ssh"),
			IPProtocols: []*armnetwork.FirewallPolicyRuleNetworkProtocol{
				new(armnetwork.FirewallPolicyRuleNetworkProtocolTCP),
			},
			SourceAddresses:      []*string{new("*")},
			DestinationAddresses: []*string{new("203.0.113.10")}, DestinationPorts: []*string{new("22")},
			TranslatedAddress: new("10.0.2.4"), TranslatedPort: new("22"),
		}
		if mod != nil {
			mod(r)
		}

		return r
	}
	natCol := func(r *armnetwork.NatRule) armnetwork.FirewallPolicyRuleCollectionClassification {
		return &armnetwork.FirewallPolicyNatRuleCollection{
			RuleCollectionType: new(armnetwork.FirewallPolicyRuleCollectionTypeFirewallPolicyNatRuleCollection),
			Name:               new("dnat"), Priority: new(int32(100)),
			Action: &armnetwork.FirewallPolicyNatRuleCollectionAction{
				Type: new(armnetwork.FirewallPolicyNatRuleCollectionActionTypeDNAT),
			},
			Rules: []armnetwork.FirewallPolicyRuleClassification{r},
		}
	}

	tests := []struct {
		name     string
		wantCode string
		cols     []armnetwork.FirewallPolicyRuleCollectionClassification
		prio     int32
	}{
		{
			name: "network",
			prio: 100,
			cols: []armnetwork.FirewallPolicyRuleCollectionClassification{netRuleCollection("net", 100, okNetRule())},
		},
		{
			name: "application",
			prio: 110,
			cols: []armnetwork.FirewallPolicyRuleCollectionClassification{appCol(appRule(nil))},
		},
		{name: "nat", prio: 120, cols: []armnetwork.FirewallPolicyRuleCollectionClassification{natCol(natRule(nil))}},
		{name: "empty group", prio: 130},
		{name: "priority low", prio: 99, wantCode: "InvalidRequestFormat"},
		{name: "priority high", prio: 65001, wantCode: "InvalidRequestFormat"},
		{
			name: "network rule bad port", prio: 140, wantCode: "InvalidRequestFormat",
			cols: []armnetwork.FirewallPolicyRuleCollectionClassification{
				netRuleCollection("net", 100, func() *armnetwork.Rule {
					r := okNetRule()
					r.DestinationPorts = []*string{new("70000")}

					return r
				}()),
			},
		},
		{
			name: "network rule bad protocol", prio: 141, wantCode: "InvalidRequestFormat",
			cols: []armnetwork.FirewallPolicyRuleCollectionClassification{
				netRuleCollection("net", 100, func() *armnetwork.Rule {
					r := okNetRule()
					r.IPProtocols = []*armnetwork.FirewallPolicyRuleNetworkProtocol{
						new(armnetwork.FirewallPolicyRuleNetworkProtocol("SCTP")),
					}

					return r
				}()),
			},
		},
		{
			name: "network rule no destination", prio: 142, wantCode: "InvalidRequestFormat",
			cols: []armnetwork.FirewallPolicyRuleCollectionClassification{
				netRuleCollection("net", 100, func() *armnetwork.Rule {
					r := okNetRule()
					r.DestinationAddresses = nil

					return r
				}()),
			},
		},
		{
			name: "network rule no source", prio: 143, wantCode: "InvalidRequestFormat",
			cols: []armnetwork.FirewallPolicyRuleCollectionClassification{
				netRuleCollection("net", 100, func() *armnetwork.Rule {
					r := okNetRule()
					r.SourceAddresses = nil

					return r
				}()),
			},
		},
		{
			name: "collection priority", prio: 144, wantCode: "InvalidRequestFormat",
			cols: []armnetwork.FirewallPolicyRuleCollectionClassification{netRuleCollection("net", 50, okNetRule())},
		},
		{
			name: "duplicate collection priority", prio: 145, wantCode: "InvalidRequestFormat",
			cols: []armnetwork.FirewallPolicyRuleCollectionClassification{
				netRuleCollection("a", 100, okNetRule()), netRuleCollection("b", 100, okNetRule()),
			},
		},
		{
			name: "mixed rule types", prio: 146, wantCode: "InvalidRequestFormat",
			cols: []armnetwork.FirewallPolicyRuleCollectionClassification{appCol(appRule(nil), okNetRule())},
		},
		{
			name: "app rule bad protocol", prio: 147, wantCode: "InvalidRequestFormat",
			cols: []armnetwork.FirewallPolicyRuleCollectionClassification{
				appCol(appRule(func(r *armnetwork.ApplicationRule) {
					r.Protocols[0].ProtocolType = new(armnetwork.FirewallPolicyRuleApplicationProtocolType("Ftp"))
				})),
			},
		},
		{
			name: "app rule no target", prio: 148, wantCode: "InvalidRequestFormat",
			cols: []armnetwork.FirewallPolicyRuleCollectionClassification{
				appCol(appRule(func(r *armnetwork.ApplicationRule) {
					r.TargetFqdns = nil
				})),
			},
		},
		{
			name: "app rule url filtering needs premium", prio: 149, wantCode: "InvalidRequestFormat",
			cols: []armnetwork.FirewallPolicyRuleCollectionClassification{
				appCol(appRule(func(r *armnetwork.ApplicationRule) {
					r.TargetFqdns = nil
					r.TargetUrls = []*string{new("www.example.com/path")}
				})),
			},
		},
		{
			name: "nat rule two destinations", prio: 150, wantCode: "InvalidRequestFormat",
			cols: []armnetwork.FirewallPolicyRuleCollectionClassification{natCol(natRule(func(r *armnetwork.NatRule) {
				r.DestinationAddresses = []*string{new("203.0.113.10"), new("203.0.113.11")}
			}))},
		},
		{
			name: "nat rule both translations", prio: 151, wantCode: "InvalidRequestFormat",
			cols: []armnetwork.FirewallPolicyRuleCollectionClassification{natCol(natRule(func(r *armnetwork.NatRule) {
				r.TranslatedFqdn = new("vm.example.com")
			}))},
		},
		{
			name: "nat rule translated port range", prio: 152, wantCode: "InvalidRequestFormat",
			cols: []armnetwork.FirewallPolicyRuleCollectionClassification{natCol(natRule(func(r *armnetwork.NatRule) {
				r.TranslatedPort = new("20-30")
			}))},
		},
		{
			name: "nat rule icmp", prio: 153, wantCode: "InvalidRequestFormat",
			cols: []armnetwork.FirewallPolicyRuleCollectionClassification{natCol(natRule(func(r *armnetwork.NatRule) {
				r.IPProtocols = []*armnetwork.FirewallPolicyRuleNetworkProtocol{
					new(armnetwork.FirewallPolicyRuleNetworkProtocolICMP),
				}
			}))},
		},
	}

	for i, tc := range tests {
		// Each case uses its own group name so a failure cannot hide behind a stale group.
		err = f.putRCG(t, "pol", "g"+string(rune('a'+i)), tc.prio, tc.cols...)
		if tc.wantCode == "" {
			require.NoError(t, err, tc.name)

			continue
		}

		assert.Equal(t, tc.wantCode, sdkErrCode(err), tc.name)
	}

	c := f.net.NewFirewallPolicyRuleCollectionGroupsClient()

	got, err := c.Get(ctx, netRG, "pol", "ga", nil)
	require.NoError(t, err)

	col, ok := got.Properties.RuleCollections[0].(*armnetwork.FirewallPolicyFilterRuleCollection)
	require.True(t, ok)
	assert.Equal(t, "allow-dns", *col.Rules[0].(*armnetwork.Rule).Name)

	natGot, err := c.Get(ctx, netRG, "pol", "gc", nil)
	require.NoError(t, err)
	assert.IsType(t, &armnetwork.FirewallPolicyNatRuleCollection{}, natGot.Properties.RuleCollections[0])

	// Group priorities are unique within a policy; the same group can be rewritten.
	err = f.putRCG(t, "pol", "other", 100)
	assert.Equal(t, "InvalidRequestFormat", sdkErrCode(err))
	require.NoError(t, f.putRCG(t, "pol", "ga", 100))

	pager := c.NewListPager(netRG, "pol", nil)
	require.True(t, pager.More())

	page, err := pager.NextPage(ctx)
	require.NoError(t, err)
	assert.Len(t, page.Value, 4)

	pol, err := f.net.NewFirewallPoliciesClient().Get(ctx, netRG, "pol", nil)
	require.NoError(t, err)
	assert.Len(t, pol.Properties.RuleCollectionGroups, 4)

	del, err := c.BeginDelete(ctx, netRG, "pol", "ga", nil)
	require.NoError(t, err)
	_, err = del.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)

	_, err = c.Get(ctx, netRG, "pol", "ga", nil)
	assert.Equal(t, "ResourceNotFound", sdkErrCode(err))

	err = f.putRCG(t, "missing", "g", 100)
	assert.Equal(t, "ResourceNotFound", sdkErrCode(err))
}

func TestNetworkSDK_PremiumRuleFeatures(t *testing.T) {
	t.Parallel()

	f := newFWFixture(t)

	_, err := f.putPolicy(t, "prem", "westus", armnetwork.FirewallPolicyPropertiesFormat{
		SKU: &armnetwork.FirewallPolicySKU{Tier: new(armnetwork.FirewallPolicySKUTierPremium)},
	})
	require.NoError(t, err)

	col := &armnetwork.FirewallPolicyFilterRuleCollection{
		RuleCollectionType: new(armnetwork.FirewallPolicyRuleCollectionTypeFirewallPolicyFilterRuleCollection),
		Name:               new("urls"), Priority: new(int32(300)),
		Action: &armnetwork.FirewallPolicyFilterRuleCollectionAction{
			Type: new(armnetwork.FirewallPolicyFilterRuleCollectionActionTypeAllow),
		},
		Rules: []armnetwork.FirewallPolicyRuleClassification{&armnetwork.ApplicationRule{
			RuleType: new(armnetwork.FirewallPolicyRuleTypeApplicationRule), Name: new("u"),
			Protocols: []*armnetwork.FirewallPolicyRuleApplicationProtocol{
				{ProtocolType: new(armnetwork.FirewallPolicyRuleApplicationProtocolTypeHTTPS), Port: new(int32(443))},
			},
			SourceAddresses: []*string{
				new("*"),
			}, TargetUrls: []*string{new("www.example.com/x")}, TerminateTLS: new(true),
		}},
	}

	require.NoError(t, f.putRCG(t, "prem", "g", 100, col))

	// Downgrading to Standard would orphan the URL-filtering rule.
	_, err = f.putPolicy(t, "prem", "westus", armnetwork.FirewallPolicyPropertiesFormat{})
	assert.Equal(t, "InvalidRequestFormat", sdkErrCode(err))
}

func TestNetworkSDK_IPGroups(t *testing.T) {
	t.Parallel()

	f := newFWFixture(t)
	ctx := t.Context()

	tests := []struct {
		name     string
		wantCode string
		addrs    []string
	}{
		{name: "mixed", addrs: []string{"10.0.0.1", "10.1.0.0/16", "192.168.0.1-192.168.0.9", "2001:db8::/32"}},
		{name: "empty"},
		{name: "bad address", addrs: []string{"10.0.0.1", "nope"}, wantCode: "InvalidIpAddressInIpGroup"},
		{name: "reversed range", addrs: []string{"10.0.0.9-10.0.0.1"}, wantCode: "InvalidIpAddressInIpGroup"},
		{name: "mixed family range", addrs: []string{"10.0.0.1-::1"}, wantCode: "InvalidIpAddressInIpGroup"},
	}

	for i, tc := range tests {
		_, err := f.putIPGroup(t, "g"+string(rune('a'+i)), tc.addrs...)
		if tc.wantCode == "" {
			require.NoError(t, err, tc.name)

			continue
		}

		assert.Equal(t, tc.wantCode, sdkErrCode(err), tc.name)
	}

	got, err := f.net.NewIPGroupsClient().Get(ctx, netRG, "ga", nil)
	require.NoError(t, err)
	assert.Len(t, got.Properties.IPAddresses, 4)

	// Rules reference groups; the group reports its users and cannot be deleted while used.
	_, err = f.putPolicy(t, "pol", "westus", armnetwork.FirewallPolicyPropertiesFormat{})
	require.NoError(t, err)

	rule := okNetRule()
	rule.SourceAddresses = nil
	rule.SourceIPGroups = []*string{got.ID}
	require.NoError(t, f.putRCG(t, "pol", "g", 100, netRuleCollection("net", 100, rule)))

	fw := f.firewall()
	fw.Properties.NetworkRuleCollections = []*armnetwork.AzureFirewallNetworkRuleCollection{{
		Name: new("classic"),
		Properties: &armnetwork.AzureFirewallNetworkRuleCollectionPropertiesFormat{
			Priority: new(
				int32(100),
			), Action: &armnetwork.AzureFirewallRCAction{Type: new(armnetwork.AzureFirewallRCActionTypeAllow)},
			Rules: []*armnetwork.AzureFirewallNetworkRule{{
				Name: new(
					"r",
				), Protocols: []*armnetwork.AzureFirewallNetworkRuleProtocol{new(armnetwork.AzureFirewallNetworkRuleProtocolTCP)},
				DestinationIPGroups: []*string{
					got.ID,
				}, SourceAddresses: []*string{new("*")}, DestinationPorts: []*string{new("443")},
			}},
		},
	}}

	_, err = f.putFirewall(t, fw)
	require.NoError(t, err)

	got, err = f.net.NewIPGroupsClient().Get(ctx, netRG, "ga", nil)
	require.NoError(t, err)
	require.Len(t, got.Properties.Firewalls, 1)
	require.Len(t, got.Properties.FirewallPolicies, 1)

	_, err = f.net.NewIPGroupsClient().BeginDelete(ctx, netRG, "ga", nil)
	assert.Equal(t, "InUseIpGroupCannotBeDeleted", sdkErrCode(err))

	fw.Properties.NetworkRuleCollections = nil
	_, err = f.putFirewall(t, fw)
	require.NoError(t, err)

	rm, err := f.net.NewFirewallPolicyRuleCollectionGroupsClient().BeginDelete(ctx, netRG, "pol", "g", nil)
	require.NoError(t, err)
	_, err = rm.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)

	del, err := f.net.NewIPGroupsClient().BeginDelete(ctx, netRG, "ga", nil)
	require.NoError(t, err)
	_, err = del.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)
}

func TestNetworkSDK_FirewallPersistence(t *testing.T) {
	t.Parallel()

	f := newFWFixture(t)
	ctx := t.Context()

	ipg, err := f.putIPGroup(t, "grp", "10.9.0.0/16")
	require.NoError(t, err)

	pol, err := f.putPolicy(t, "pol", "westus", armnetwork.FirewallPolicyPropertiesFormat{
		ThreatIntelMode: new(armnetwork.AzureFirewallThreatIntelModeDeny),
	})
	require.NoError(t, err)

	rule := okNetRule()
	rule.SourceAddresses = nil
	rule.SourceIPGroups = []*string{ipg.ID}
	require.NoError(t, f.putRCG(t, "pol", "rcg", 200, netRuleCollection("net", 100, rule)))

	fw := f.firewall()
	fw.Properties.FirewallPolicy = &armnetwork.SubResource{ID: pol.ID}
	before, err := f.putFirewall(t, fw)
	require.NoError(t, err)

	snap := f.handler.Snapshot(ctx)

	raw, err := json.Marshal(snap)
	require.NoError(t, err)
	require.NotEmpty(t, raw)

	dst := newNetStack(t)
	require.NoError(t, dst.handler.Restore(ctx, snap))

	gotFW, err := dst.net.NewAzureFirewallsClient().Get(ctx, netRG, fwName, nil)
	require.NoError(t, err)
	assert.Equal(t, *before.Properties.IPConfigurations[0].Properties.PrivateIPAddress,
		*gotFW.Properties.IPConfigurations[0].Properties.PrivateIPAddress)
	assert.Equal(t, *pol.ID, *gotFW.Properties.FirewallPolicy.ID)

	gotRCG, err := dst.net.NewFirewallPolicyRuleCollectionGroupsClient().Get(ctx, netRG, "pol", "rcg", nil)
	require.NoError(t, err)
	assert.Equal(t, int32(200), *gotRCG.Properties.Priority)

	gotGroup, err := dst.net.NewIPGroupsClient().Get(ctx, netRG, "grp", nil)
	require.NoError(t, err)
	assert.Len(t, gotGroup.Properties.FirewallPolicies, 1)
}
