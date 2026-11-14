package app

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/networkfirewall"
	nftypes "github.com/aws/aws-sdk-go-v2/service/networkfirewall/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type nfWiringCase func(t *testing.T, s subnetFixture, c *ec2.Client, nf *networkfirewall.Client, policyARN string)

func TestNetworkFirewallEC2Wiring(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  nfWiringCase
		name string
	}{
		{name: "transit gateway", run: nfWiringCase(func(
			t *testing.T, _ subnetFixture, c *ec2.Client, nf *networkfirewall.Client, policyARN string,
		) {
			t.Helper()

			tgw, err := c.CreateTransitGateway(t.Context(), &ec2.CreateTransitGatewayInput{})
			require.NoError(t, err)

			zones := []nftypes.AvailabilityZoneMapping{{AvailabilityZone: aws.String("us-east-1a")}}
			out, err := nf.CreateFirewall(t.Context(), &networkfirewall.CreateFirewallInput{
				FirewallName: aws.String("tgw-fw"), FirewallPolicyArn: aws.String(policyARN),
				TransitGatewayId: tgw.TransitGateway.TransitGatewayId, AvailabilityZoneMappings: zones,
			})
			require.NoError(t, err)
			assert.Equal(t, "000000000000", aws.ToString(out.Firewall.TransitGatewayOwnerAccountId))

			_, err = nf.CreateFirewall(t.Context(), &networkfirewall.CreateFirewallInput{
				FirewallName: aws.String("ghost-fw"), FirewallPolicyArn: aws.String(policyARN),
				TransitGatewayId: aws.String("tgw-0123456789abcdef0"), AvailabilityZoneMappings: zones,
			})
			require.ErrorContains(t, err, "InvalidRequestException")
		})},
		{name: "nat gateway", run: nfWiringCase(func(
			t *testing.T, s subnetFixture, c *ec2.Client, nf *networkfirewall.Client, policyARN string,
		) {
			t.Helper()

			eip, err := c.AllocateAddress(t.Context(), &ec2.AllocateAddressInput{Domain: ec2types.DomainTypeVpc})
			require.NoError(t, err)

			nat, err := c.CreateNatGateway(t.Context(), &ec2.CreateNatGatewayInput{
				SubnetId: aws.String(s.v4Subnet), AllocationId: eip.AllocationId,
			})
			require.NoError(t, err)

			in := func(name, natID string) *networkfirewall.CreateFirewallInput {
				return &networkfirewall.CreateFirewallInput{
					FirewallName:         aws.String(name),
					FirewallPolicyArn:    aws.String(policyARN),
					NoSourcePreservation: true,
					NatGatewayMappings:   []nftypes.NatGatewayMapping{{NatGatewayId: aws.String(natID)}},
					VpcEndpoint: &nftypes.VpcEndpoint{
						VpcId:          aws.String(s.vpcID),
						SubnetMappings: []nftypes.SubnetMapping{{SubnetId: aws.String(s.v4Subnet)}},
					},
				}
			}

			out, err := nf.CreateFirewall(t.Context(), in("px-fw", aws.ToString(nat.NatGateway.NatGatewayId)))
			require.NoError(t, err)
			require.Len(t, out.FirewallStatus.SyncStates["us-east-1a"].NatGatewayAttachments, 1)
			assert.Equal(t, aws.ToString(nat.NatGateway.NatGatewayId),
				aws.ToString(out.FirewallStatus.SyncStates["us-east-1a"].NatGatewayAttachments[0].NatGatewayId))

			_, err = nf.CreateFirewall(t.Context(), in("ghost-fw", "nat-0123456789abcdef0"))
			require.ErrorContains(t, err, "InvalidRequestException")
		})},
		{name: "prefix list", run: nfWiringCase(func(
			t *testing.T, s subnetFixture, c *ec2.Client, nf *networkfirewall.Client, _ string,
		) {
			t.Helper()

			pl, err := c.CreateManagedPrefixList(t.Context(), &ec2.CreateManagedPrefixListInput{
				PrefixListName: aws.String("fw-refs"), AddressFamily: aws.String("IPv4"), MaxEntries: aws.Int32(10),
				Entries: []ec2types.AddPrefixListEntry{
					{Cidr: aws.String("10.1.0.0/16")},
					{Cidr: aws.String("10.2.0.0/16")},
					{Cidr: aws.String("10.3.0.0/16")},
				},
			})
			require.NoError(t, err)

			plARN := aws.ToString(pl.PrefixList.PrefixListArn)

			group, err := nf.CreateRuleGroup(t.Context(), &networkfirewall.CreateRuleGroupInput{
				RuleGroupName: aws.String("refs"), Type: nftypes.RuleGroupTypeStateful, Capacity: aws.Int32(10),
				RuleGroup: &nftypes.RuleGroup{
					ReferenceSets: &nftypes.ReferenceSets{IPSetReferences: map[string]nftypes.IPSetReference{
						"PL": {ReferenceArn: aws.String(plARN)},
					}},
					RulesSource: &nftypes.RulesSource{RulesString: aws.String("pass tcp $PL any -> any 80 (sid:1;)\n")},
				},
			})
			require.NoError(t, err)

			pol, err := nf.CreateFirewallPolicy(t.Context(), &networkfirewall.CreateFirewallPolicyInput{
				FirewallPolicyName: aws.String("refs-pol"),
				FirewallPolicy: &nftypes.FirewallPolicy{
					StatelessDefaultActions:         []string{"aws:forward_to_sfe"},
					StatelessFragmentDefaultActions: []string{"aws:forward_to_sfe"},
					StatefulRuleGroupReferences: []nftypes.StatefulRuleGroupReference{
						{ResourceArn: group.RuleGroupResponse.RuleGroupArn},
					},
				},
			})
			require.NoError(t, err)

			out, err := nf.CreateFirewall(t.Context(), &networkfirewall.CreateFirewallInput{
				FirewallName:      aws.String("refs-fw"),
				FirewallPolicyArn: pol.FirewallPolicyResponse.FirewallPolicyArn,
				VpcId:             aws.String(s.vpcID),
				SubnetMappings:    []nftypes.SubnetMapping{{SubnetId: aws.String(s.v4Subnet)}},
			})
			require.NoError(t, err)

			cidrs := out.FirewallStatus.CapacityUsageSummary.CIDRs
			assert.EqualValues(t, 3, aws.ToInt32(cidrs.UtilizedCIDRCount))
			assert.EqualValues(t, 3, aws.ToInt32(cidrs.IPSetReferences[plARN].ResolvedCIDRCount))
		})},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newSubnetFixture(t)
			nf := networkfirewall.NewFromConfig(s.fx.cfg)

			pol, err := nf.CreateFirewallPolicy(t.Context(), &networkfirewall.CreateFirewallPolicyInput{
				FirewallPolicyName: aws.String("pol"),
				FirewallPolicy: &nftypes.FirewallPolicy{
					StatelessDefaultActions:         []string{"aws:forward_to_sfe"},
					StatelessFragmentDefaultActions: []string{"aws:forward_to_sfe"},
				},
			})
			require.NoError(t, err)

			tc.run(t, s, ec2.NewFromConfig(s.fx.cfg), nf, aws.ToString(pol.FirewallPolicyResponse.FirewallPolicyArn))
		})
	}
}
