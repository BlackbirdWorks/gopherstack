package networkfirewall_test

import (
	"testing"
	"testing/synctest"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/networkfirewall"
	"github.com/aws/aws-sdk-go-v2/service/networkfirewall/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRuleGroupAndPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, f *fixture)
		name string
	}{
		{name: "stateful rules string round trip", run: func(t *testing.T, f *fixture) {
			t.Helper()

			g := f.statefulGroup(t, "sf")
			resp := g.RuleGroupResponse
			assert.Contains(t, aws.ToString(resp.RuleGroupArn), ":stateful-rulegroup/sf")
			assert.EqualValues(t, 2, aws.ToInt32(resp.ConsumedCapacity))
			assert.Equal(t, types.ResourceStatusActive, resp.RuleGroupStatus)
			assert.NotNil(t, resp.LastModifiedTime)

			d, err := f.client.DescribeRuleGroup(t.Context(), &sdk.DescribeRuleGroupInput{
				RuleGroupName: aws.String("sf"), Type: types.RuleGroupTypeStateful,
			})
			require.NoError(t, err)
			assert.Contains(t, aws.ToString(d.RuleGroup.RulesSource.RulesString), "sid:2")
			assert.Equal(t, aws.ToString(g.UpdateToken), aws.ToString(d.UpdateToken))

			md, err := f.client.DescribeRuleGroupMetadata(t.Context(), &sdk.DescribeRuleGroupMetadataInput{
				RuleGroupArn: resp.RuleGroupArn,
			})
			require.NoError(t, err)
			assert.EqualValues(t, 10, aws.ToInt32(md.Capacity))
			assert.Equal(t, types.RuleGroupTypeStateful, md.Type)
		}},
		{name: "stateless group and validation", run: func(t *testing.T, f *fixture) {
			t.Helper()

			body := &types.RuleGroup{RulesSource: &types.RulesSource{
				StatelessRulesAndCustomActions: &types.StatelessRulesAndCustomActions{
					StatelessRules: []types.StatelessRule{{
						Priority: aws.Int32(1),
						RuleDefinition: &types.RuleDefinition{
							Actions: []string{"aws:pass"},
							MatchAttributes: &types.MatchAttributes{
								Sources: []types.Address{{AddressDefinition: aws.String("10.0.0.0/8")}},
							},
						},
					}},
				},
			}}

			out, err := f.client.CreateRuleGroup(t.Context(), &sdk.CreateRuleGroupInput{
				RuleGroupName: aws.String(
					"sl",
				),
				Type:      types.RuleGroupTypeStateless,
				Capacity:  aws.Int32(5),
				RuleGroup: body,
			})
			require.NoError(t, err)
			assert.Contains(t, aws.ToString(out.RuleGroupResponse.RuleGroupArn), ":stateless-rulegroup/sl")

			_, err = f.client.CreateRuleGroup(t.Context(), &sdk.CreateRuleGroupInput{
				RuleGroupName: aws.String(
					"sl",
				),
				Type:      types.RuleGroupTypeStateless,
				Capacity:  aws.Int32(5),
				RuleGroup: body,
			})
			assert.Equal(t, "InvalidRequestException", errCode(t, err))

			_, err = f.client.CreateRuleGroup(t.Context(), &sdk.CreateRuleGroupInput{
				RuleGroupName: aws.String(
					"wrong",
				),
				Type:      types.RuleGroupTypeStateful,
				Capacity:  aws.Int32(5),
				RuleGroup: body,
			})
			assert.Equal(t, "InvalidRequestException", errCode(t, err))

			_, err = f.client.CreateRuleGroup(t.Context(), &sdk.CreateRuleGroupInput{
				RuleGroupName: aws.String("small"), Type: types.RuleGroupTypeStateful, Capacity: aws.Int32(1),
				Rules: aws.String("pass tcp any any -> any 80 (sid:1;)\npass tcp any any -> any 81 (sid:2;)"),
			})
			assert.Equal(t, "InvalidRequestException", errCode(t, err))

			f.statefulGroup(t, "sl")

			list, err := f.client.ListRuleGroups(
				t.Context(),
				&sdk.ListRuleGroupsInput{Type: types.RuleGroupTypeStateful},
			)
			require.NoError(t, err)
			require.Len(t, list.RuleGroups, 1)
			assert.Contains(t, aws.ToString(list.RuleGroups[0].Arn), "stateful-rulegroup/sl")

			all, err := f.client.ListRuleGroups(t.Context(), &sdk.ListRuleGroupsInput{})
			require.NoError(t, err)
			assert.Len(t, all.RuleGroups, 2)

			managed, err := f.client.ListRuleGroups(
				t.Context(),
				&sdk.ListRuleGroupsInput{Scope: types.ResourceManagedStatusManaged},
			)
			require.NoError(t, err)
			assert.Empty(t, managed.RuleGroups)
		}},
		{name: "update token dry run and in-use delete", run: func(t *testing.T, f *fixture) {
			t.Helper()

			g := f.statefulGroup(t, "sf")
			arn := g.RuleGroupResponse.RuleGroupArn

			_, err := f.client.UpdateRuleGroup(t.Context(), &sdk.UpdateRuleGroupInput{
				RuleGroupArn: arn, Rules: aws.String("pass tcp any any -> any 443 (sid:3;)"),
				UpdateToken: aws.String("stale"),
			})
			assert.Equal(t, "InvalidTokenException", errCode(t, err))

			dry, err := f.client.UpdateRuleGroup(t.Context(), &sdk.UpdateRuleGroupInput{
				RuleGroupArn: arn, Rules: aws.String("pass tcp any any -> any 443 (sid:3;)"),
				UpdateToken: g.UpdateToken, DryRun: true,
			})
			require.NoError(t, err)
			assert.EqualValues(t, 1, aws.ToInt32(dry.RuleGroupResponse.ConsumedCapacity))

			still, err := f.client.DescribeRuleGroup(t.Context(), &sdk.DescribeRuleGroupInput{RuleGroupArn: arn})
			require.NoError(t, err)
			assert.Equal(t, aws.ToString(g.UpdateToken), aws.ToString(still.UpdateToken))

			up, err := f.client.UpdateRuleGroup(t.Context(), &sdk.UpdateRuleGroupInput{
				RuleGroupArn: arn, Rules: aws.String("pass tcp any any -> any 443 (sid:3;)"),
				UpdateToken: g.UpdateToken, Description: aws.String("v2"),
			})
			require.NoError(t, err)
			assert.NotEqual(t, aws.ToString(g.UpdateToken), aws.ToString(up.UpdateToken))
			assert.Equal(t, "v2", aws.ToString(up.RuleGroupResponse.Description))

			pol, err := f.client.CreateFirewallPolicy(t.Context(), &sdk.CreateFirewallPolicyInput{
				FirewallPolicyName: aws.String("pol"),
				FirewallPolicy: &types.FirewallPolicy{
					StatelessDefaultActions:         []string{"aws:pass"},
					StatelessFragmentDefaultActions: []string{"aws:pass"},
					StatefulRuleGroupReferences:     []types.StatefulRuleGroupReference{{ResourceArn: arn}},
				},
			})
			require.NoError(t, err)
			assert.EqualValues(t, 10, aws.ToInt32(pol.FirewallPolicyResponse.ConsumedStatefulRuleCapacity))

			_, err = f.client.DeleteRuleGroup(t.Context(), &sdk.DeleteRuleGroupInput{RuleGroupArn: arn})
			assert.Equal(t, "InvalidOperationException", errCode(t, err))

			_, err = f.client.UpdateFirewallPolicy(t.Context(), &sdk.UpdateFirewallPolicyInput{
				FirewallPolicyArn: pol.FirewallPolicyResponse.FirewallPolicyArn,
				UpdateToken:       pol.UpdateToken,
				FirewallPolicy: &types.FirewallPolicy{
					StatelessDefaultActions:         []string{"aws:drop"},
					StatelessFragmentDefaultActions: []string{"aws:drop"},
				},
			})
			require.NoError(t, err)

			del, err := f.client.DeleteRuleGroup(t.Context(), &sdk.DeleteRuleGroupInput{RuleGroupArn: arn})
			require.NoError(t, err)
			assert.Equal(t, types.ResourceStatusDeleting, del.RuleGroupResponse.RuleGroupStatus)

			_, err = f.client.DescribeRuleGroup(t.Context(), &sdk.DescribeRuleGroupInput{RuleGroupArn: arn})
			assert.Equal(t, "ResourceNotFoundException", errCode(t, err))
		}},
		{name: "policy references and listing", run: func(t *testing.T, f *fixture) {
			t.Helper()

			_, err := f.client.CreateFirewallPolicy(t.Context(), &sdk.CreateFirewallPolicyInput{
				FirewallPolicyName: aws.String("bad"),
				FirewallPolicy: &types.FirewallPolicy{
					StatelessDefaultActions:         []string{"aws:pass"},
					StatelessFragmentDefaultActions: []string{"aws:pass"},
					StatefulRuleGroupReferences: []types.StatefulRuleGroupReference{
						{
							ResourceArn: aws.String(
								"arn:aws:network-firewall:us-east-1:123456789012:stateful-rulegroup/none",
							),
						},
					},
				},
			})
			assert.Equal(t, "ResourceNotFoundException", errCode(t, err))

			f.policy(t, "p1")
			f.policy(t, "p2")

			dup, err := f.client.CreateFirewallPolicy(t.Context(), &sdk.CreateFirewallPolicyInput{
				FirewallPolicyName: aws.String("p1"),
				FirewallPolicy: &types.FirewallPolicy{
					StatelessDefaultActions: []string{
						"aws:pass",
					},
					StatelessFragmentDefaultActions: []string{"aws:pass"},
				},
			})
			assert.Nil(t, dup)
			assert.Equal(t, "InvalidRequestException", errCode(t, err))

			list, err := f.client.ListFirewallPolicies(
				t.Context(),
				&sdk.ListFirewallPoliciesInput{MaxResults: aws.Int32(1)},
			)
			require.NoError(t, err)
			require.Len(t, list.FirewallPolicies, 1)
			assert.Equal(t, "p1", aws.ToString(list.FirewallPolicies[0].Name))
			assert.NotEmpty(t, aws.ToString(list.NextToken))

			_, err = f.client.ListFirewallPolicies(
				t.Context(),
				&sdk.ListFirewallPoliciesInput{MaxResults: aws.Int32(1000)},
			)
			assert.Equal(t, "InvalidRequestException", errCode(t, err))

			_, err = f.client.DeleteFirewallPolicy(
				t.Context(),
				&sdk.DeleteFirewallPolicyInput{FirewallPolicyName: aws.String("p1")},
			)
			require.NoError(t, err)

			_, err = f.client.DescribeFirewallPolicy(
				t.Context(),
				&sdk.DescribeFirewallPolicyInput{FirewallPolicyName: aws.String("p1")},
			)
			assert.Equal(t, "ResourceNotFoundException", errCode(t, err))
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				tt.run(t, newFixture(t))
			})
		})
	}
}
