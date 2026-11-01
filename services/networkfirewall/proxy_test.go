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

func (f *fixture) proxyRuleGroup(
	t *testing.T,
	name string,
	rules *types.ProxyRulesByRequestPhase,
) *sdk.CreateProxyRuleGroupOutput {
	t.Helper()

	out, err := f.client.CreateProxyRuleGroup(t.Context(), &sdk.CreateProxyRuleGroupInput{
		ProxyRuleGroupName: aws.String(name), Rules: rules,
	})
	require.NoError(t, err)

	return out
}

func (f *fixture) proxyConfig(t *testing.T, groups ...string) *sdk.CreateProxyConfigurationOutput {
	t.Helper()

	out, err := f.client.CreateProxyConfiguration(t.Context(), &sdk.CreateProxyConfigurationInput{
		ProxyConfigurationName: aws.String("cfg"),
		DefaultRulePhaseActions: &types.ProxyConfigDefaultRulePhaseActionsRequest{
			PreDNS: types.ProxyRulePhaseActionAllow, PreREQUEST: types.ProxyRulePhaseActionDeny,
			PostRESPONSE: types.ProxyRulePhaseActionAlert,
		},
		RuleGroupNames: groups,
	})
	require.NoError(t, err)

	return out
}

func ruleNames(rules []types.ProxyRule) []string {
	out := make([]string, len(rules))
	for i, r := range rules {
		out[i] = aws.ToString(r.ProxyRuleName)
	}

	return out
}

func domainRule(name string, action types.ProxyRulePhaseAction) types.ProxyRule {
	return types.ProxyRule{
		ProxyRuleName: aws.String(name), Action: action,
		Conditions: []types.ProxyRuleCondition{{
			ConditionKey: aws.String("request:DestinationDomain"), ConditionOperator: aws.String("StringEquals"),
			ConditionValues: []string{name + ".example.com"},
		}},
	}
}

func TestProxyRuleGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, f *fixture)
		name string
	}{
		{name: "rules lifecycle", run: func(t *testing.T, f *fixture) {
			t.Helper()

			ctx := t.Context()
			created := f.proxyRuleGroup(t, "rg", &types.ProxyRulesByRequestPhase{
				PreDNS: []types.ProxyRule{domainRule("a", types.ProxyRulePhaseActionAllow)},
			})
			arn := created.ProxyRuleGroup.ProxyRuleGroupArn
			assert.Contains(t, aws.ToString(arn), ":proxy-rule-group/rg")
			assert.NotEmpty(t, aws.ToString(created.UpdateToken))

			added, err := f.client.CreateProxyRules(ctx, &sdk.CreateProxyRulesInput{
				ProxyRuleGroupArn: arn,
				Rules: &types.CreateProxyRulesByRequestPhase{
					PreDNS: []types.CreateProxyRule{
						{
							ProxyRuleName:  aws.String("b"),
							Action:         types.ProxyRulePhaseActionDeny,
							InsertPosition: aws.Int32(0),
						},
					},
					PostRESPONSE: []types.CreateProxyRule{
						{ProxyRuleName: aws.String("c"), Action: types.ProxyRulePhaseActionAlert},
					},
				},
			})
			require.NoError(t, err)
			assert.Equal(t, []string{"b", "a"}, ruleNames(added.ProxyRuleGroup.Rules.PreDNS))
			assert.Equal(t, []string{"c"}, ruleNames(added.ProxyRuleGroup.Rules.PostRESPONSE))
			assert.NotEqual(t, aws.ToString(created.UpdateToken), aws.ToString(added.UpdateToken))

			prio, err := f.client.UpdateProxyRulePriorities(ctx, &sdk.UpdateProxyRulePrioritiesInput{
				ProxyRuleGroupArn: arn, RuleGroupRequestPhase: types.RuleGroupRequestPhasePreDns,
				UpdateToken: added.UpdateToken,
				Rules:       []types.ProxyRulePriority{{ProxyRuleName: aws.String("a"), NewPosition: aws.Int32(0)}},
			})
			require.NoError(t, err)
			require.Len(t, prio.Rules, 2)
			assert.Equal(t, "a", aws.ToString(prio.Rules[0].ProxyRuleName))
			assert.EqualValues(t, 1, aws.ToInt32(prio.Rules[1].NewPosition))

			_, err = f.client.UpdateProxyRulePriorities(ctx, &sdk.UpdateProxyRulePrioritiesInput{
				ProxyRuleGroupArn: arn, RuleGroupRequestPhase: types.RuleGroupRequestPhasePreDns,
				UpdateToken: added.UpdateToken,
				Rules:       []types.ProxyRulePriority{{ProxyRuleName: aws.String("a"), NewPosition: aws.Int32(1)}},
			})
			assert.Equal(t, "InvalidTokenException", errCode(t, err))

			upd, err := f.client.UpdateProxyRule(ctx, &sdk.UpdateProxyRuleInput{
				ProxyRuleGroupArn: arn, ProxyRuleName: aws.String("a"), UpdateToken: prio.UpdateToken,
				Action: types.ProxyRulePhaseActionDeny, Description: aws.String("blocked"),
				RemoveConditions: []types.ProxyRuleCondition{domainRule("a", "").Conditions[0]},
				AddConditions: []types.ProxyRuleCondition{{
					ConditionKey: aws.String("request:Port"), ConditionOperator: aws.String("NumericEquals"),
					ConditionValues: []string{"443"},
				}},
			})
			require.NoError(t, err)
			assert.Equal(t, types.ProxyRulePhaseActionDeny, upd.ProxyRule.Action)
			require.Len(t, upd.RemovedConditions, 1)
			require.Len(t, upd.ProxyRule.Conditions, 1)
			assert.Equal(t, "request:Port", aws.ToString(upd.ProxyRule.Conditions[0].ConditionKey))

			desc, err := f.client.DescribeProxyRule(ctx, &sdk.DescribeProxyRuleInput{
				ProxyRuleGroupName: aws.String("rg"), ProxyRuleName: aws.String("a"),
			})
			require.NoError(t, err)
			assert.Equal(t, "blocked", aws.ToString(desc.ProxyRule.Description))
			assert.Equal(t, aws.ToString(upd.UpdateToken), aws.ToString(desc.UpdateToken))

			del, err := f.client.DeleteProxyRules(ctx, &sdk.DeleteProxyRulesInput{
				ProxyRuleGroupArn: arn, Rules: []string{"b", "c"},
			})
			require.NoError(t, err)
			assert.Equal(t, []string{"a"}, ruleNames(del.ProxyRuleGroup.Rules.PreDNS))
			assert.Empty(t, del.ProxyRuleGroup.Rules.PostRESPONSE)

			_, err = f.client.DeleteProxyRules(ctx, &sdk.DeleteProxyRulesInput{
				ProxyRuleGroupArn: arn, Rules: []string{"ghost"},
			})
			assert.Equal(t, "InvalidRequestException", errCode(t, err))

			list, err := f.client.ListProxyRuleGroups(ctx, &sdk.ListProxyRuleGroupsInput{})
			require.NoError(t, err)
			require.Len(t, list.ProxyRuleGroups, 1)
			assert.Equal(t, "rg", aws.ToString(list.ProxyRuleGroups[0].Name))

			gone, err := f.client.DeleteProxyRuleGroup(ctx, &sdk.DeleteProxyRuleGroupInput{ProxyRuleGroupArn: arn})
			require.NoError(t, err)
			assert.Equal(t, "rg", aws.ToString(gone.ProxyRuleGroupName))

			_, err = f.client.DescribeProxyRuleGroup(ctx, &sdk.DescribeProxyRuleGroupInput{ProxyRuleGroupArn: arn})
			assert.Equal(t, "ResourceNotFoundException", errCode(t, err))
		}},
		{name: "validation", run: func(t *testing.T, f *fixture) {
			t.Helper()

			ctx := t.Context()
			f.proxyRuleGroup(t, "rg", nil)

			_, err := f.client.CreateProxyRuleGroup(
				ctx,
				&sdk.CreateProxyRuleGroupInput{ProxyRuleGroupName: aws.String("rg")},
			)
			assert.Equal(t, "InvalidRequestException", errCode(t, err))

			_, err = f.client.CreateProxyRuleGroup(ctx, &sdk.CreateProxyRuleGroupInput{
				ProxyRuleGroupName: aws.String("dupe"),
				Rules: &types.ProxyRulesByRequestPhase{
					PreDNS:     []types.ProxyRule{domainRule("x", types.ProxyRulePhaseActionAllow)},
					PreREQUEST: []types.ProxyRule{domainRule("x", types.ProxyRulePhaseActionAllow)},
				},
			})
			assert.Equal(t, "InvalidRequestException", errCode(t, err))

			_, err = f.client.CreateProxyRules(ctx, &sdk.CreateProxyRulesInput{
				ProxyRuleGroupName: aws.String("rg"),
				Rules: &types.CreateProxyRulesByRequestPhase{
					PreDNS: []types.CreateProxyRule{
						{ProxyRuleName: aws.String("bad name!"), Action: types.ProxyRulePhaseActionAllow},
					},
				},
			})
			assert.Equal(t, "InvalidRequestException", errCode(t, err))

			_, err = f.client.DescribeProxyRule(ctx, &sdk.DescribeProxyRuleInput{
				ProxyRuleGroupName: aws.String("rg"), ProxyRuleName: aws.String("ghost"),
			})
			assert.Equal(t, "ResourceNotFoundException", errCode(t, err))
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t, newFixture(t))
		})
	}
}

func TestProxyConfigurations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, f *fixture)
		name string
	}{
		{name: "attach detach and priorities", run: func(t *testing.T, f *fixture) {
			t.Helper()

			ctx := t.Context()
			f.proxyRuleGroup(t, "g1", nil)
			f.proxyRuleGroup(t, "g2", nil)
			f.proxyRuleGroup(t, "g3", nil)

			cfg := f.proxyConfig(t, "g1")
			assert.Equal(t, types.ProxyRulePhaseActionDeny, cfg.ProxyConfiguration.DefaultRulePhaseActions.PreREQUEST)
			require.Len(t, cfg.ProxyConfiguration.RuleGroups, 1)
			assert.EqualValues(t, 0, aws.ToInt32(cfg.ProxyConfiguration.RuleGroups[0].Priority))

			att, err := f.client.AttachRuleGroupsToProxyConfiguration(
				ctx,
				&sdk.AttachRuleGroupsToProxyConfigurationInput{
					ProxyConfigurationName: aws.String("cfg"), UpdateToken: cfg.UpdateToken,
					RuleGroups: []types.ProxyRuleGroupAttachment{
						{ProxyRuleGroupName: aws.String("g2")},
						{ProxyRuleGroupName: aws.String("g3"), InsertPosition: aws.Int32(0)},
					},
				},
			)
			require.NoError(t, err)

			names := func(c *types.ProxyConfiguration) []string {
				out := make([]string, len(c.RuleGroups))
				for i, g := range c.RuleGroups {
					out[i] = aws.ToString(g.ProxyRuleGroupName)
				}

				return out
			}
			assert.Equal(t, []string{"g3", "g1", "g2"}, names(att.ProxyConfiguration))

			_, err = f.client.AttachRuleGroupsToProxyConfiguration(ctx, &sdk.AttachRuleGroupsToProxyConfigurationInput{
				ProxyConfigurationName: aws.String("cfg"), UpdateToken: cfg.UpdateToken,
				RuleGroups: []types.ProxyRuleGroupAttachment{{ProxyRuleGroupName: aws.String("g2")}},
			})
			assert.Equal(t, "InvalidTokenException", errCode(t, err))

			moved, err := f.client.UpdateProxyRuleGroupPriorities(ctx, &sdk.UpdateProxyRuleGroupPrioritiesInput{
				ProxyConfigurationName: aws.String("cfg"),
				UpdateToken:            att.UpdateToken,
				RuleGroups: []types.ProxyRuleGroupPriority{
					{ProxyRuleGroupName: aws.String("g2"), NewPosition: aws.Int32(0)},
				},
			})
			require.NoError(t, err)
			require.Len(t, moved.ProxyRuleGroups, 3)
			assert.Equal(t, "g2", aws.ToString(moved.ProxyRuleGroups[0].ProxyRuleGroupName))
			assert.EqualValues(t, 2, aws.ToInt32(moved.ProxyRuleGroups[2].Priority))

			_, err = f.client.DeleteProxyRuleGroup(
				ctx,
				&sdk.DeleteProxyRuleGroupInput{ProxyRuleGroupName: aws.String("g2")},
			)
			assert.Equal(t, "InvalidOperationException", errCode(t, err))

			det, err := f.client.DetachRuleGroupsFromProxyConfiguration(
				ctx,
				&sdk.DetachRuleGroupsFromProxyConfigurationInput{
					ProxyConfigurationName: aws.String("cfg"), UpdateToken: moved.UpdateToken,
					RuleGroupNames: []string{"g2", "g3"},
				},
			)
			require.NoError(t, err)
			assert.Equal(t, []string{"g1"}, names(det.ProxyConfiguration))

			_, err = f.client.DetachRuleGroupsFromProxyConfiguration(
				ctx,
				&sdk.DetachRuleGroupsFromProxyConfigurationInput{
					ProxyConfigurationName: aws.String(
						"cfg",
					),
					UpdateToken:    det.UpdateToken,
					RuleGroupNames: []string{"g2"},
				},
			)
			assert.Equal(t, "InvalidRequestException", errCode(t, err))

			_, err = f.client.DeleteProxyRuleGroup(
				ctx,
				&sdk.DeleteProxyRuleGroupInput{ProxyRuleGroupName: aws.String("g2")},
			)
			require.NoError(t, err)
		}},
		{name: "update describe list delete", run: func(t *testing.T, f *fixture) {
			t.Helper()

			ctx := t.Context()
			cfg := f.proxyConfig(t)

			upd, err := f.client.UpdateProxyConfiguration(ctx, &sdk.UpdateProxyConfigurationInput{
				ProxyConfigurationArn: cfg.ProxyConfiguration.ProxyConfigurationArn, UpdateToken: cfg.UpdateToken,
				DefaultRulePhaseActions: &types.ProxyConfigDefaultRulePhaseActionsRequest{
					PreDNS: types.ProxyRulePhaseActionDeny,
				},
			})
			require.NoError(t, err)
			assert.Equal(t, types.ProxyRulePhaseActionDeny, upd.ProxyConfiguration.DefaultRulePhaseActions.PreDNS)

			d, err := f.client.DescribeProxyConfiguration(ctx, &sdk.DescribeProxyConfigurationInput{
				ProxyConfigurationName: aws.String("cfg"),
			})
			require.NoError(t, err)
			assert.Equal(t, aws.ToString(upd.UpdateToken), aws.ToString(d.UpdateToken))

			list, err := f.client.ListProxyConfigurations(
				ctx,
				&sdk.ListProxyConfigurationsInput{MaxResults: aws.Int32(1)},
			)
			require.NoError(t, err)
			require.Len(t, list.ProxyConfigurations, 1)

			_, err = f.client.CreateProxyConfiguration(ctx, &sdk.CreateProxyConfigurationInput{
				ProxyConfigurationName:  aws.String("cfg"),
				DefaultRulePhaseActions: &types.ProxyConfigDefaultRulePhaseActionsRequest{},
			})
			assert.Equal(t, "InvalidRequestException", errCode(t, err))

			_, err = f.client.CreateProxyConfiguration(ctx, &sdk.CreateProxyConfigurationInput{
				ProxyConfigurationName:  aws.String("other"),
				DefaultRulePhaseActions: &types.ProxyConfigDefaultRulePhaseActionsRequest{},
				RuleGroupNames:          []string{"ghost"},
			})
			assert.Equal(t, "ResourceNotFoundException", errCode(t, err))

			del, err := f.client.DeleteProxyConfiguration(ctx, &sdk.DeleteProxyConfigurationInput{
				ProxyConfigurationName: aws.String("cfg"),
			})
			require.NoError(t, err)
			assert.Equal(t, "cfg", aws.ToString(del.ProxyConfigurationName))

			_, err = f.client.DescribeProxyConfiguration(ctx, &sdk.DescribeProxyConfigurationInput{
				ProxyConfigurationName: aws.String("cfg"),
			})
			assert.Equal(t, "ResourceNotFoundException", errCode(t, err))
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t, newFixture(t))
		})
	}
}

func TestProxies(t *testing.T) {
	t.Parallel()

	tls := &types.TlsInterceptPropertiesRequest{TlsInterceptMode: types.TlsInterceptModeDisabled}

	tests := []struct {
		run  func(t *testing.T, f *fixture)
		name string
	}{
		{name: "lifecycle", run: func(t *testing.T, f *fixture) {
			t.Helper()

			ctx := t.Context()
			f.proxyConfig(t)

			created, err := f.client.CreateProxy(ctx, &sdk.CreateProxyInput{
				ProxyName: aws.String(
					"px",
				),
				NatGatewayId:           aws.String("nat-1"),
				ProxyConfigurationName: aws.String("cfg"),
				TlsInterceptProperties: tls,
				ListenerProperties: []types.ListenerPropertyRequest{
					{Port: aws.Int32(80), Type: types.ListenerPropertyTypeHttp},
				},
				Tags: []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}},
			})
			require.NoError(t, err)
			assert.Equal(t, types.ProxyStateAttaching, created.Proxy.ProxyState)
			assert.Contains(t, aws.ToString(created.Proxy.ProxyArn), ":proxy/px")
			assert.Equal(t, "cfg", aws.ToString(created.Proxy.ProxyConfigurationName))

			require.Eventually(t, func() bool {
				d, derr := f.client.DescribeProxy(ctx, &sdk.DescribeProxyInput{ProxyName: aws.String("px")})

				return derr == nil && d.Proxy.ProxyState == types.ProxyStateAttached
			}, 5e9, 10e6)

			d, err := f.client.DescribeProxy(ctx, &sdk.DescribeProxyInput{ProxyArn: created.Proxy.ProxyArn})
			require.NoError(t, err)
			assert.NotEmpty(t, aws.ToString(d.Proxy.PrivateDNSName))
			assert.NotEmpty(t, aws.ToString(d.Proxy.VpcEndpointServiceName))
			require.Len(t, d.Proxy.Tags, 1)

			upd, err := f.client.UpdateProxy(ctx, &sdk.UpdateProxyInput{
				ProxyName: aws.String("px"), NatGatewayId: aws.String("nat-1"), UpdateToken: d.UpdateToken,
				ListenerPropertiesToAdd: []types.ListenerPropertyRequest{
					{Port: aws.Int32(443), Type: types.ListenerPropertyTypeHttps},
				},
				ListenerPropertiesToRemove: []types.ListenerPropertyRequest{
					{Port: aws.Int32(80), Type: types.ListenerPropertyTypeHttp},
				},
			})
			require.NoError(t, err)
			require.Len(t, upd.Proxy.ListenerProperties, 1)
			assert.EqualValues(t, 443, aws.ToInt32(upd.Proxy.ListenerProperties[0].Port))
			assert.Equal(t, types.ProxyModifyStateModifying, upd.Proxy.ProxyModifyState)

			_, err = f.client.UpdateProxy(ctx, &sdk.UpdateProxyInput{
				ProxyName: aws.String("px"), NatGatewayId: aws.String("nat-1"), UpdateToken: d.UpdateToken,
			})
			assert.Equal(t, "InvalidTokenException", errCode(t, err))

			_, err = f.client.DeleteProxyConfiguration(ctx, &sdk.DeleteProxyConfigurationInput{
				ProxyConfigurationName: aws.String("cfg"),
			})
			assert.Equal(t, "InvalidOperationException", errCode(t, err))

			list, err := f.client.ListProxies(ctx, &sdk.ListProxiesInput{})
			require.NoError(t, err)
			require.Len(t, list.Proxies, 1)

			del, err := f.client.DeleteProxy(
				ctx,
				&sdk.DeleteProxyInput{ProxyName: aws.String("px"), NatGatewayId: aws.String("nat-1")},
			)
			require.NoError(t, err)
			assert.Equal(t, "nat-1", aws.ToString(del.NatGatewayId))

			_, err = f.client.DeleteProxy(
				ctx,
				&sdk.DeleteProxyInput{ProxyName: aws.String("px"), NatGatewayId: aws.String("nat-2")},
			)
			assert.Equal(t, "InvalidRequestException", errCode(t, err))

			require.Eventually(t, func() bool {
				_, derr := f.client.DescribeProxy(ctx, &sdk.DescribeProxyInput{ProxyName: aws.String("px")})

				return derr != nil
			}, 5e9, 10e6)

			_, err = f.client.DeleteProxyConfiguration(ctx, &sdk.DeleteProxyConfigurationInput{
				ProxyConfigurationName: aws.String("cfg"),
			})
			require.NoError(t, err)
		}},
		{name: "validation", run: func(t *testing.T, f *fixture) {
			t.Helper()

			ctx := t.Context()
			f.proxyConfig(t)

			_, err := f.client.CreateProxy(ctx, &sdk.CreateProxyInput{
				ProxyName: aws.String(
					"px",
				),
				NatGatewayId:           aws.String("nat-1"),
				ProxyConfigurationName: aws.String("ghost"),
				TlsInterceptProperties: tls,
			})
			assert.Equal(t, "ResourceNotFoundException", errCode(t, err))

			_, err = f.client.CreateProxy(ctx, &sdk.CreateProxyInput{
				ProxyName: aws.String(
					"px",
				),
				NatGatewayId:           aws.String("nat-1"),
				ProxyConfigurationName: aws.String("cfg"),
				TlsInterceptProperties: &types.TlsInterceptPropertiesRequest{
					TlsInterceptMode: types.TlsInterceptModeEnabled,
				},
			})
			assert.Equal(t, "InvalidRequestException", errCode(t, err))

			_, err = f.client.CreateProxy(ctx, &sdk.CreateProxyInput{
				ProxyName: aws.String(
					"px",
				),
				NatGatewayId:           aws.String("nat-1"),
				ProxyConfigurationName: aws.String("cfg"),
				TlsInterceptProperties: tls,
				ListenerProperties: []types.ListenerPropertyRequest{
					{Port: aws.Int32(80), Type: types.ListenerPropertyTypeHttp},
					{Port: aws.Int32(80), Type: types.ListenerPropertyTypeHttps},
				},
			})
			assert.Equal(t, "InvalidRequestException", errCode(t, err))

			_, err = f.client.DescribeProxy(ctx, &sdk.DescribeProxyInput{ProxyName: aws.String("ghost")})
			assert.Equal(t, "ResourceNotFoundException", errCode(t, err))

			_, err = f.client.DeleteProxy(
				ctx,
				&sdk.DeleteProxyInput{ProxyName: aws.String("ghost"), NatGatewayId: aws.String("nat-1")},
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
