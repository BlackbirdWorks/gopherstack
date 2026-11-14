package networkfirewall_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/networkfirewall"
	"github.com/aws/aws-sdk-go-v2/service/networkfirewall/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeNet struct {
	nats  map[string][2]string
	lists map[string]int
}

func (f fakeNet) NatGatewayNetwork(id string) (string, string, bool) {
	n, ok := f.nats[id]

	return n[0], n[1], ok
}

func (f fakeNet) PrefixListCIDRCount(arn string) (int, bool) {
	n, ok := f.lists[arn]

	return n, ok
}

const testPrefixList = "arn:aws:ec2:us-east-1:123456789012:prefix-list/pl-0123"

func proxyFirewallInput(policyARN, name string, nats ...string) *sdk.CreateFirewallInput {
	in := &sdk.CreateFirewallInput{
		FirewallName: aws.String(name), FirewallPolicyArn: aws.String(policyARN), NoSourcePreservation: true,
		VpcEndpoint: &types.VpcEndpoint{
			VpcId: aws.String(testVpc), SubnetMappings: []types.SubnetMapping{{SubnetId: aws.String("subnet-aaaa")}},
		},
	}
	for _, n := range nats {
		in.NatGatewayMappings = append(in.NatGatewayMappings, types.NatGatewayMapping{NatGatewayId: aws.String(n)})
	}

	return in
}

func TestProxyModeFirewall(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, f *fixture, policyARN string)
		name string
	}{
		{name: "create and describe", run: func(t *testing.T, f *fixture, policyARN string) {
			t.Helper()

			out, err := f.client.CreateFirewall(t.Context(), proxyFirewallInput(policyARN, "px", "nat-1"))
			require.NoError(t, err)
			assert.True(t, out.Firewall.NoSourcePreservation)
			assert.Equal(t, "nat-1", aws.ToString(out.Firewall.NatGatewayMappings[0].NatGatewayId))
			assert.Equal(t, testVpc, aws.ToString(out.Firewall.VpcEndpoint.VpcId))
			assert.Equal(t, "subnet-aaaa", aws.ToString(out.Firewall.VpcEndpoint.SubnetMappings[0].SubnetId))
			assert.Empty(t, aws.ToString(out.Firewall.VpcId))

			zone := out.FirewallStatus.SyncStates["us-east-1a"]
			require.Len(t, zone.NatGatewayAttachments, 1)
			assert.Equal(t, "nat-1", aws.ToString(zone.NatGatewayAttachments[0].NatGatewayId))
			assert.Equal(t, types.NatGatewayAttachmentStatusCreating, zone.NatGatewayAttachments[0].Status)

			require.Eventually(t, func() bool {
				d, derr := f.client.DescribeFirewall(
					t.Context(),
					&sdk.DescribeFirewallInput{FirewallName: aws.String("px")},
				)

				return derr == nil &&
					d.FirewallStatus.SyncStates["us-east-1a"].NatGatewayAttachments[0].Status ==
						types.NatGatewayAttachmentStatusReady
			}, 5e9, 10e6)
		}},
		{name: "validation", run: func(t *testing.T, f *fixture, policyARN string) {
			t.Helper()

			for name, in := range map[string]*sdk.CreateFirewallInput{
				"unknown nat": proxyFirewallInput(policyARN, "a", "nat-missing"),
				"other vpc":   proxyFirewallInput(policyARN, "b", "nat-2"),
				"no nat":      proxyFirewallInput(policyARN, "c"),
				"duplicate":   proxyFirewallInput(policyARN, "d", "nat-1", "nat-1"),
				"no endpoint": {
					FirewallName: aws.String("e"), FirewallPolicyArn: aws.String(policyARN), NoSourcePreservation: true,
				},
				"without flag": {
					FirewallName: aws.String("f"), FirewallPolicyArn: aws.String(policyARN),
					VpcEndpoint: proxyFirewallInput(policyARN, "f").VpcEndpoint,
				},
			} {
				_, err := f.client.CreateFirewall(t.Context(), in)
				assert.Equal(t, "InvalidRequestException", errCode(t, err), name)
			}
		}},
		{name: "proxy nat gateway", run: func(t *testing.T, f *fixture, _ string) {
			t.Helper()

			cfg, err := f.client.CreateProxyConfiguration(t.Context(), &sdk.CreateProxyConfigurationInput{
				ProxyConfigurationName: aws.String("pc"),
				DefaultRulePhaseActions: &types.ProxyConfigDefaultRulePhaseActionsRequest{
					PreDNS: types.ProxyRulePhaseActionAllow, PreREQUEST: types.ProxyRulePhaseActionAllow,
					PostRESPONSE: types.ProxyRulePhaseActionAllow,
				},
			})
			require.NoError(t, err)

			_, err = f.client.CreateProxy(t.Context(), &sdk.CreateProxyInput{
				ProxyName: aws.String("px"), NatGatewayId: aws.String("nat-missing"),
				ProxyConfigurationArn: cfg.ProxyConfiguration.ProxyConfigurationArn,
				TlsInterceptProperties: &types.TlsInterceptPropertiesRequest{
					TlsInterceptMode: types.TlsInterceptModeDisabled,
				},
			})
			assert.Equal(t, "InvalidRequestException", errCode(t, err))
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			f.handler.SetNetworkResolver(fakeNet{nats: map[string][2]string{
				"nat-1": {testVpc, "us-east-1a"}, "nat-2": {"vpc-other", "us-east-1a"},
			}})
			tc.run(t, f, aws.ToString(f.policy(t, "pol").FirewallPolicyResponse.FirewallPolicyArn))
		})
	}
}

func TestFirewallCapacityUsage(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.handler.SetNetworkResolver(fakeNet{lists: map[string]int{testPrefixList: 7}})

	empty := f.policy(t, "empty")
	fw := f.firewall(t, "fw-empty", aws.ToString(empty.FirewallPolicyResponse.FirewallPolicyArn))
	cidrs := fw.FirewallStatus.CapacityUsageSummary.CIDRs
	assert.EqualValues(t, 0, aws.ToInt32(cidrs.UtilizedCIDRCount))
	assert.EqualValues(t, 1_000_000, aws.ToInt32(cidrs.AvailableCIDRCount))

	group, err := f.client.CreateRuleGroup(t.Context(), &sdk.CreateRuleGroupInput{
		RuleGroupName: aws.String("refs"), Type: types.RuleGroupTypeStateful, Capacity: aws.Int32(10),
		RuleGroup: &types.RuleGroup{
			ReferenceSets: &types.ReferenceSets{IPSetReferences: map[string]types.IPSetReference{
				"PL": {ReferenceArn: aws.String(testPrefixList)},
			}},
			RulesSource: &types.RulesSource{RulesString: aws.String("pass tcp $PL any -> any 80 (sid:1;)\n")},
		},
	})
	require.NoError(t, err)

	pol, err := f.client.CreateFirewallPolicy(t.Context(), &sdk.CreateFirewallPolicyInput{
		FirewallPolicyName: aws.String("with-refs"),
		FirewallPolicy: &types.FirewallPolicy{
			StatelessDefaultActions:         []string{"aws:forward_to_sfe"},
			StatelessFragmentDefaultActions: []string{"aws:forward_to_sfe"},
			StatefulRuleGroupReferences: []types.StatefulRuleGroupReference{
				{ResourceArn: group.RuleGroupResponse.RuleGroupArn},
			},
		},
	})
	require.NoError(t, err)

	created := f.firewall(t, "fw-refs", aws.ToString(pol.FirewallPolicyResponse.FirewallPolicyArn))
	cidrs = created.FirewallStatus.CapacityUsageSummary.CIDRs
	assert.EqualValues(t, 7, aws.ToInt32(cidrs.UtilizedCIDRCount))
	assert.EqualValues(t, 1_000_000-7, aws.ToInt32(cidrs.AvailableCIDRCount))
	assert.EqualValues(t, 7, aws.ToInt32(cidrs.IPSetReferences[testPrefixList].ResolvedCIDRCount))
}
