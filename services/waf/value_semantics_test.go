package waf_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	wafsdk "github.com/aws/aws-sdk-go-v2/service/waf"
	"github.com/aws/aws-sdk-go-v2/service/waf/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWebACL_ActivatedRuleTypeDefaultsToRegular(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		ruleType types.WafRuleType
		want     types.WafRuleType
	}{
		{name: "omitted", want: types.WafRuleTypeRegular},
		{name: "explicit", ruleType: types.WafRuleTypeRegular, want: types.WafRuleTypeRegular},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newRealClient(t)
			ctx := t.Context()

			rule, err := c.CreateRule(ctx, &wafsdk.CreateRuleInput{
				Name: aws.String("r"), MetricName: aws.String("r"), ChangeToken: changeToken(t, c),
			})
			require.NoError(t, err)

			acl, err := c.CreateWebACL(ctx, &wafsdk.CreateWebACLInput{
				Name: aws.String("a"), MetricName: aws.String("a"), ChangeToken: changeToken(t, c),
				DefaultAction: &types.WafAction{Type: types.WafActionTypeAllow},
			})
			require.NoError(t, err)

			_, err = c.UpdateWebACL(ctx, &wafsdk.UpdateWebACLInput{
				WebACLId: acl.WebACL.WebACLId, ChangeToken: changeToken(t, c),
				Updates: []types.WebACLUpdate{{
					Action: types.ChangeActionInsert,
					ActivatedRule: &types.ActivatedRule{
						RuleId: rule.Rule.RuleId, Priority: aws.Int32(1),
						Action: &types.WafAction{Type: types.WafActionTypeBlock}, Type: tc.ruleType,
					},
				}},
			})
			require.NoError(t, err)

			got, err := c.GetWebACL(ctx, &wafsdk.GetWebACLInput{WebACLId: acl.WebACL.WebACLId})
			require.NoError(t, err)
			require.Len(t, got.WebACL.Rules, 1)
			assert.Equal(t, tc.want, got.WebACL.Rules[0].Type)
		})
	}
}
