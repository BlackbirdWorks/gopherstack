package networkfirewall_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/networkfirewall"
	"github.com/aws/aws-sdk-go-v2/service/networkfirewall/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	nf "github.com/blackbirdworks/gopherstack/services/networkfirewall"
)

func TestSnapshotRestoreRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		region string
	}{
		{name: "home region", region: testRegion},
		{name: "sibling region", region: "eu-west-1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			src := newFixture(t)
			client := newClient(t, src.handler, tt.region)
			ctx := t.Context()

			pol, err := client.CreateFirewallPolicy(ctx, &sdk.CreateFirewallPolicyInput{
				FirewallPolicyName: aws.String("pol"),
				FirewallPolicy: &types.FirewallPolicy{
					StatelessDefaultActions: []string{
						"aws:pass",
					},
					StatelessFragmentDefaultActions: []string{"aws:pass"},
				},
				Tags: []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}},
			})
			require.NoError(t, err)

			_, err = client.CreateFirewall(ctx, &sdk.CreateFirewallInput{
				FirewallName: aws.String("fw"), FirewallPolicyArn: pol.FirewallPolicyResponse.FirewallPolicyArn,
				VpcId: aws.String(testVpc), SubnetMappings: []types.SubnetMapping{{SubnetId: aws.String("subnet-1")}},
			})
			require.NoError(t, err)

			_, err = client.CreateRuleGroup(ctx, &sdk.CreateRuleGroupInput{
				RuleGroupName: aws.String("sf"), Type: types.RuleGroupTypeStateful, Capacity: aws.Int32(5),
				Rules: aws.String("pass tcp any any -> any 80 (sid:1;)"),
			})
			require.NoError(t, err)

			before, err := client.DescribeFirewall(ctx, &sdk.DescribeFirewallInput{FirewallName: aws.String("fw")})
			require.NoError(t, err)

			snap := src.handler.Snapshot(ctx)
			require.NotEmpty(t, snap)

			dst := newFixture(t)
			require.NoError(t, dst.handler.Restore(ctx, snap))

			dstClient := newClient(t, dst.handler, tt.region)

			after, err := dstClient.DescribeFirewall(ctx, &sdk.DescribeFirewallInput{FirewallName: aws.String("fw")})
			require.NoError(t, err)
			assert.Equal(t, aws.ToString(before.UpdateToken), aws.ToString(after.UpdateToken))
			assert.Equal(t, aws.ToString(before.Firewall.FirewallId), aws.ToString(after.Firewall.FirewallId))

			rg, err := dstClient.DescribeRuleGroup(ctx, &sdk.DescribeRuleGroupInput{
				RuleGroupName: aws.String("sf"), Type: types.RuleGroupTypeStateful,
			})
			require.NoError(t, err)
			assert.Contains(t, aws.ToString(rg.RuleGroup.RulesSource.RulesString), "sid:1")

			tags, err := dstClient.ListTagsForResource(ctx, &sdk.ListTagsForResourceInput{
				ResourceArn: pol.FirewallPolicyResponse.FirewallPolicyArn,
			})
			require.NoError(t, err)
			require.Len(t, tags.Tags, 1)

			other := "us-west-2"
			if tt.region == other {
				other = testRegion
			}

			_, err = newClient(
				t,
				dst.handler,
				other,
			).DescribeFirewall(ctx, &sdk.DescribeFirewallInput{FirewallName: aws.String("fw")})
			assert.Equal(t, "ResourceNotFoundException", errCode(t, err))
		})
	}
}

func TestResetClearsEveryRegion(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	eu := newClient(t, f.handler, "eu-west-1")

	f.policy(t, "pol")

	_, err := eu.CreateFirewallPolicy(t.Context(), &sdk.CreateFirewallPolicyInput{
		FirewallPolicyName: aws.String("pol"),
		FirewallPolicy: &types.FirewallPolicy{
			StatelessDefaultActions: []string{"aws:pass"}, StatelessFragmentDefaultActions: []string{"aws:pass"},
		},
	})
	require.NoError(t, err)

	f.handler.Reset()

	for _, c := range []*sdk.Client{f.client, eu} {
		out, lerr := c.ListFirewallPolicies(t.Context(), &sdk.ListFirewallPoliciesInput{})
		require.NoError(t, lerr)
		assert.Empty(t, out.FirewallPolicies)
	}
}

func TestRestoreRejectsInvalidSnapshot(t *testing.T) {
	t.Parallel()

	b := nf.NewInMemoryBackend(testAccount, testRegion)
	require.Error(t, b.Restore(t.Context(), []byte("{not json")))
}
