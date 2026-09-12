package waf_test

import (
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	wafsdk "github.com/aws/aws-sdk-go-v2/service/waf"
	"github.com/aws/aws-sdk-go-v2/service/waf/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/waf"
)

// TestSlice8_WAF_RealClient covers waf's (Classic) highest-priority typed-
// client-uncovered op families (gopherstack-n3zi slice 8): WebACLs, rules,
// rule groups, every match-set condition type (IPSet/ByteMatch/
// SqlInjectionMatch/SizeConstraint/XssMatch/RegexMatch/RegexPattern/GeoMatch),
// rate-based rules, change tokens, logging configuration, permission policy,
// and tags. Every mutation drives the real GetChangeToken -> mutate ->
// GetChangeTokenStatus sequence a real client uses. Each subtest creates
// real state through the typed aws-sdk-go-v2 waf client and asserts decoded
// response values.
func TestSlice8_WAF_RealClient(t *testing.T) {
	t.Parallel()

	cases := []struct {
		fn   func(t *testing.T)
		name string
	}{
		{testSlice8IPSetRealClient, "ip_set"},
		{testSlice8ByteMatchSetRealClient, "byte_match_set"},
		{testSlice8SqlInjectionMatchSetRealClient, "sql_injection_match_set"},
		{testSlice8SizeConstraintSetRealClient, "size_constraint_set"},
		{testSlice8XssMatchSetRealClient, "xss_match_set"},
		{testSlice8GeoMatchSetRealClient, "geo_match_set"},
		{testSlice8RegexPatternSetRealClient, "regex_pattern_set"},
		{testSlice8RegexMatchSetRealClient, "regex_match_set"},
		{testSlice8RuleRealClient, "rule"},
		{testSlice8RuleGroupRealClient, "rule_group"},
		{testSlice8RateBasedRuleRealClient, "rate_based_rule"},
		{testSlice8WebACLRealClient, "web_acl"},
		{testSlice8LoggingConfigurationRealClient, "logging_configuration"},
		{testSlice8PermissionPolicyRealClient, "permission_policy"},
		{testSlice8TagsRealClient, "tags"},
		{testSlice8ChangeTokenStatusRealClient, "change_token_status"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t)
		})
	}
}

func newSlice8WAFClient(t *testing.T) *wafsdk.Client {
	t.Helper()

	h := waf.NewHandler(waf.NewInMemoryBackend("123456789012", "us-east-1"))

	return newTestWAFClient(t, h)
}

func slice8ChangeToken(t *testing.T, client *wafsdk.Client) *string {
	t.Helper()

	out, err := client.GetChangeToken(t.Context(), &wafsdk.GetChangeTokenInput{})
	require.NoError(t, err)

	return out.ChangeToken
}

func testSlice8IPSetRealClient(t *testing.T) {
	t.Helper()

	client := newSlice8WAFClient(t)
	ctx := t.Context()

	createOut, err := client.CreateIPSet(ctx, &wafsdk.CreateIPSetInput{
		Name:        aws.String("slice8-ipset"),
		ChangeToken: slice8ChangeToken(t, client),
	})
	require.NoError(t, err)
	ipSetID := aws.ToString(createOut.IPSet.IPSetId)
	require.NotEmpty(t, ipSetID)

	_, err = client.UpdateIPSet(ctx, &wafsdk.UpdateIPSetInput{
		IPSetId:     aws.String(ipSetID),
		ChangeToken: slice8ChangeToken(t, client),
		Updates: []types.IPSetUpdate{
			{
				Action: types.ChangeActionInsert,
				IPSetDescriptor: &types.IPSetDescriptor{
					Type:  types.IPSetDescriptorTypeIpv4,
					Value: aws.String("10.0.0.0/8"),
				},
			},
		},
	})
	require.NoError(t, err)

	getOut, err := client.GetIPSet(ctx, &wafsdk.GetIPSetInput{IPSetId: aws.String(ipSetID)})
	require.NoError(t, err)
	require.Len(t, getOut.IPSet.IPSetDescriptors, 1)
	assert.Equal(t, "10.0.0.0/8", aws.ToString(getOut.IPSet.IPSetDescriptors[0].Value))

	listOut, err := client.ListIPSets(ctx, &wafsdk.ListIPSetsInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, listOut.IPSets)

	_, err = client.UpdateIPSet(ctx, &wafsdk.UpdateIPSetInput{
		IPSetId:     aws.String(ipSetID),
		ChangeToken: slice8ChangeToken(t, client),
		Updates: []types.IPSetUpdate{
			{
				Action: types.ChangeActionDelete,
				IPSetDescriptor: &types.IPSetDescriptor{
					Type:  types.IPSetDescriptorTypeIpv4,
					Value: aws.String("10.0.0.0/8"),
				},
			},
		},
	})
	require.NoError(t, err)

	_, err = client.DeleteIPSet(ctx, &wafsdk.DeleteIPSetInput{
		IPSetId:     aws.String(ipSetID),
		ChangeToken: slice8ChangeToken(t, client),
	})
	require.NoError(t, err)
}

func testSlice8ByteMatchSetRealClient(t *testing.T) {
	t.Helper()

	client := newSlice8WAFClient(t)
	ctx := t.Context()

	createOut, err := client.CreateByteMatchSet(ctx, &wafsdk.CreateByteMatchSetInput{
		Name:        aws.String("slice8-bytematchset"),
		ChangeToken: slice8ChangeToken(t, client),
	})
	require.NoError(t, err)
	id := aws.ToString(createOut.ByteMatchSet.ByteMatchSetId)
	require.NotEmpty(t, id)

	_, err = client.UpdateByteMatchSet(ctx, &wafsdk.UpdateByteMatchSetInput{
		ByteMatchSetId: aws.String(id),
		ChangeToken:    slice8ChangeToken(t, client),
		Updates: []types.ByteMatchSetUpdate{
			{
				Action: types.ChangeActionInsert,
				ByteMatchTuple: &types.ByteMatchTuple{
					FieldToMatch:         &types.FieldToMatch{Type: types.MatchFieldTypeUri},
					PositionalConstraint: types.PositionalConstraintContains,
					TargetString:         []byte("/admin"),
					TextTransformation:   types.TextTransformationNone,
				},
			},
		},
	})
	require.NoError(t, err)

	getOut, err := client.GetByteMatchSet(
		ctx,
		&wafsdk.GetByteMatchSetInput{ByteMatchSetId: aws.String(id)},
	)
	require.NoError(t, err)
	require.Len(t, getOut.ByteMatchSet.ByteMatchTuples, 1)
	assert.Equal(t, "/admin", string(getOut.ByteMatchSet.ByteMatchTuples[0].TargetString))

	listOut, err := client.ListByteMatchSets(ctx, &wafsdk.ListByteMatchSetsInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, listOut.ByteMatchSets)

	_, err = client.UpdateByteMatchSet(ctx, &wafsdk.UpdateByteMatchSetInput{
		ByteMatchSetId: aws.String(id),
		ChangeToken:    slice8ChangeToken(t, client),
		Updates: []types.ByteMatchSetUpdate{
			{
				Action: types.ChangeActionDelete,
				ByteMatchTuple: &types.ByteMatchTuple{
					FieldToMatch:         &types.FieldToMatch{Type: types.MatchFieldTypeUri},
					PositionalConstraint: types.PositionalConstraintContains,
					TargetString:         []byte("/admin"),
					TextTransformation:   types.TextTransformationNone,
				},
			},
		},
	})
	require.NoError(t, err)

	_, err = client.DeleteByteMatchSet(ctx, &wafsdk.DeleteByteMatchSetInput{
		ByteMatchSetId: aws.String(id),
		ChangeToken:    slice8ChangeToken(t, client),
	})
	require.NoError(t, err)
}

func testSlice8SqlInjectionMatchSetRealClient(t *testing.T) {
	t.Helper()

	client := newSlice8WAFClient(t)
	ctx := t.Context()

	createOut, err := client.CreateSqlInjectionMatchSet(
		ctx,
		&wafsdk.CreateSqlInjectionMatchSetInput{
			Name:        aws.String("slice8-sqlimatchset"),
			ChangeToken: slice8ChangeToken(t, client),
		},
	)
	require.NoError(t, err)
	id := aws.ToString(createOut.SqlInjectionMatchSet.SqlInjectionMatchSetId)
	require.NotEmpty(t, id)

	_, err = client.UpdateSqlInjectionMatchSet(ctx, &wafsdk.UpdateSqlInjectionMatchSetInput{
		SqlInjectionMatchSetId: aws.String(id),
		ChangeToken:            slice8ChangeToken(t, client),
		Updates: []types.SqlInjectionMatchSetUpdate{
			{
				Action: types.ChangeActionInsert,
				SqlInjectionMatchTuple: &types.SqlInjectionMatchTuple{
					FieldToMatch:       &types.FieldToMatch{Type: types.MatchFieldTypeQueryString},
					TextTransformation: types.TextTransformationNone,
				},
			},
		},
	})
	require.NoError(t, err)

	getOut, err := client.GetSqlInjectionMatchSet(
		ctx, &wafsdk.GetSqlInjectionMatchSetInput{SqlInjectionMatchSetId: aws.String(id)},
	)
	require.NoError(t, err)
	require.Len(t, getOut.SqlInjectionMatchSet.SqlInjectionMatchTuples, 1)

	listOut, err := client.ListSqlInjectionMatchSets(ctx, &wafsdk.ListSqlInjectionMatchSetsInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, listOut.SqlInjectionMatchSets)

	_, err = client.UpdateSqlInjectionMatchSet(ctx, &wafsdk.UpdateSqlInjectionMatchSetInput{
		SqlInjectionMatchSetId: aws.String(id),
		ChangeToken:            slice8ChangeToken(t, client),
		Updates: []types.SqlInjectionMatchSetUpdate{
			{
				Action: types.ChangeActionDelete,
				SqlInjectionMatchTuple: &types.SqlInjectionMatchTuple{
					FieldToMatch:       &types.FieldToMatch{Type: types.MatchFieldTypeQueryString},
					TextTransformation: types.TextTransformationNone,
				},
			},
		},
	})
	require.NoError(t, err)

	_, err = client.DeleteSqlInjectionMatchSet(ctx, &wafsdk.DeleteSqlInjectionMatchSetInput{
		SqlInjectionMatchSetId: aws.String(id),
		ChangeToken:            slice8ChangeToken(t, client),
	})
	require.NoError(t, err)
}

func testSlice8SizeConstraintSetRealClient(t *testing.T) {
	t.Helper()

	client := newSlice8WAFClient(t)
	ctx := t.Context()

	createOut, err := client.CreateSizeConstraintSet(ctx, &wafsdk.CreateSizeConstraintSetInput{
		Name:        aws.String("slice8-sizeconstraintset"),
		ChangeToken: slice8ChangeToken(t, client),
	})
	require.NoError(t, err)
	id := aws.ToString(createOut.SizeConstraintSet.SizeConstraintSetId)
	require.NotEmpty(t, id)

	_, err = client.UpdateSizeConstraintSet(ctx, &wafsdk.UpdateSizeConstraintSetInput{
		SizeConstraintSetId: aws.String(id),
		ChangeToken:         slice8ChangeToken(t, client),
		Updates: []types.SizeConstraintSetUpdate{
			{
				Action: types.ChangeActionInsert,
				SizeConstraint: &types.SizeConstraint{
					FieldToMatch:       &types.FieldToMatch{Type: types.MatchFieldTypeBody},
					ComparisonOperator: types.ComparisonOperatorGt,
					TextTransformation: types.TextTransformationNone,
					Size:               8192,
				},
			},
		},
	})
	require.NoError(t, err)

	getOut, err := client.GetSizeConstraintSet(
		ctx, &wafsdk.GetSizeConstraintSetInput{SizeConstraintSetId: aws.String(id)},
	)
	require.NoError(t, err)
	require.Len(t, getOut.SizeConstraintSet.SizeConstraints, 1)
	assert.Equal(t, int64(8192), getOut.SizeConstraintSet.SizeConstraints[0].Size)

	listOut, err := client.ListSizeConstraintSets(ctx, &wafsdk.ListSizeConstraintSetsInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, listOut.SizeConstraintSets)

	_, err = client.UpdateSizeConstraintSet(ctx, &wafsdk.UpdateSizeConstraintSetInput{
		SizeConstraintSetId: aws.String(id),
		ChangeToken:         slice8ChangeToken(t, client),
		Updates: []types.SizeConstraintSetUpdate{
			{
				Action: types.ChangeActionDelete,
				SizeConstraint: &types.SizeConstraint{
					FieldToMatch:       &types.FieldToMatch{Type: types.MatchFieldTypeBody},
					ComparisonOperator: types.ComparisonOperatorGt,
					TextTransformation: types.TextTransformationNone,
					Size:               8192,
				},
			},
		},
	})
	require.NoError(t, err)

	_, err = client.DeleteSizeConstraintSet(ctx, &wafsdk.DeleteSizeConstraintSetInput{
		SizeConstraintSetId: aws.String(id),
		ChangeToken:         slice8ChangeToken(t, client),
	})
	require.NoError(t, err)
}

func testSlice8XssMatchSetRealClient(t *testing.T) {
	t.Helper()

	client := newSlice8WAFClient(t)
	ctx := t.Context()

	createOut, err := client.CreateXssMatchSet(ctx, &wafsdk.CreateXssMatchSetInput{
		Name:        aws.String("slice8-xssmatchset"),
		ChangeToken: slice8ChangeToken(t, client),
	})
	require.NoError(t, err)
	id := aws.ToString(createOut.XssMatchSet.XssMatchSetId)
	require.NotEmpty(t, id)

	_, err = client.UpdateXssMatchSet(ctx, &wafsdk.UpdateXssMatchSetInput{
		XssMatchSetId: aws.String(id),
		ChangeToken:   slice8ChangeToken(t, client),
		Updates: []types.XssMatchSetUpdate{
			{
				Action: types.ChangeActionInsert,
				XssMatchTuple: &types.XssMatchTuple{
					FieldToMatch:       &types.FieldToMatch{Type: types.MatchFieldTypeQueryString},
					TextTransformation: types.TextTransformationNone,
				},
			},
		},
	})
	require.NoError(t, err)

	getOut, err := client.GetXssMatchSet(
		ctx,
		&wafsdk.GetXssMatchSetInput{XssMatchSetId: aws.String(id)},
	)
	require.NoError(t, err)
	require.Len(t, getOut.XssMatchSet.XssMatchTuples, 1)

	listOut, err := client.ListXssMatchSets(ctx, &wafsdk.ListXssMatchSetsInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, listOut.XssMatchSets)

	_, err = client.UpdateXssMatchSet(ctx, &wafsdk.UpdateXssMatchSetInput{
		XssMatchSetId: aws.String(id),
		ChangeToken:   slice8ChangeToken(t, client),
		Updates: []types.XssMatchSetUpdate{
			{
				Action: types.ChangeActionDelete,
				XssMatchTuple: &types.XssMatchTuple{
					FieldToMatch:       &types.FieldToMatch{Type: types.MatchFieldTypeQueryString},
					TextTransformation: types.TextTransformationNone,
				},
			},
		},
	})
	require.NoError(t, err)

	_, err = client.DeleteXssMatchSet(ctx, &wafsdk.DeleteXssMatchSetInput{
		XssMatchSetId: aws.String(id),
		ChangeToken:   slice8ChangeToken(t, client),
	})
	require.NoError(t, err)
}

func testSlice8GeoMatchSetRealClient(t *testing.T) {
	t.Helper()

	client := newSlice8WAFClient(t)
	ctx := t.Context()

	createOut, err := client.CreateGeoMatchSet(ctx, &wafsdk.CreateGeoMatchSetInput{
		Name:        aws.String("slice8-geomatchset"),
		ChangeToken: slice8ChangeToken(t, client),
	})
	require.NoError(t, err)
	id := aws.ToString(createOut.GeoMatchSet.GeoMatchSetId)
	require.NotEmpty(t, id)

	_, err = client.UpdateGeoMatchSet(ctx, &wafsdk.UpdateGeoMatchSetInput{
		GeoMatchSetId: aws.String(id),
		ChangeToken:   slice8ChangeToken(t, client),
		Updates: []types.GeoMatchSetUpdate{
			{
				Action: types.ChangeActionInsert,
				GeoMatchConstraint: &types.GeoMatchConstraint{
					Type:  types.GeoMatchConstraintTypeCountry,
					Value: types.GeoMatchConstraintValueUs,
				},
			},
		},
	})
	require.NoError(t, err)

	getOut, err := client.GetGeoMatchSet(
		ctx,
		&wafsdk.GetGeoMatchSetInput{GeoMatchSetId: aws.String(id)},
	)
	require.NoError(t, err)
	require.Len(t, getOut.GeoMatchSet.GeoMatchConstraints, 1)

	listOut, err := client.ListGeoMatchSets(ctx, &wafsdk.ListGeoMatchSetsInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, listOut.GeoMatchSets)

	_, err = client.UpdateGeoMatchSet(ctx, &wafsdk.UpdateGeoMatchSetInput{
		GeoMatchSetId: aws.String(id),
		ChangeToken:   slice8ChangeToken(t, client),
		Updates: []types.GeoMatchSetUpdate{
			{
				Action: types.ChangeActionDelete,
				GeoMatchConstraint: &types.GeoMatchConstraint{
					Type:  types.GeoMatchConstraintTypeCountry,
					Value: types.GeoMatchConstraintValueUs,
				},
			},
		},
	})
	require.NoError(t, err)

	_, err = client.DeleteGeoMatchSet(ctx, &wafsdk.DeleteGeoMatchSetInput{
		GeoMatchSetId: aws.String(id),
		ChangeToken:   slice8ChangeToken(t, client),
	})
	require.NoError(t, err)
}

func testSlice8RegexPatternSetRealClient(t *testing.T) {
	t.Helper()

	client := newSlice8WAFClient(t)
	ctx := t.Context()

	createOut, err := client.CreateRegexPatternSet(ctx, &wafsdk.CreateRegexPatternSetInput{
		Name:        aws.String("slice8-regexpatternset"),
		ChangeToken: slice8ChangeToken(t, client),
	})
	require.NoError(t, err)
	id := aws.ToString(createOut.RegexPatternSet.RegexPatternSetId)
	require.NotEmpty(t, id)

	_, err = client.UpdateRegexPatternSet(ctx, &wafsdk.UpdateRegexPatternSetInput{
		RegexPatternSetId: aws.String(id),
		ChangeToken:       slice8ChangeToken(t, client),
		Updates: []types.RegexPatternSetUpdate{
			{Action: types.ChangeActionInsert, RegexPatternString: aws.String("^/api/")},
		},
	})
	require.NoError(t, err)

	getOut, err := client.GetRegexPatternSet(
		ctx, &wafsdk.GetRegexPatternSetInput{RegexPatternSetId: aws.String(id)},
	)
	require.NoError(t, err)
	require.Len(t, getOut.RegexPatternSet.RegexPatternStrings, 1)

	listOut, err := client.ListRegexPatternSets(ctx, &wafsdk.ListRegexPatternSetsInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, listOut.RegexPatternSets)

	_, err = client.UpdateRegexPatternSet(ctx, &wafsdk.UpdateRegexPatternSetInput{
		RegexPatternSetId: aws.String(id),
		ChangeToken:       slice8ChangeToken(t, client),
		Updates: []types.RegexPatternSetUpdate{
			{Action: types.ChangeActionDelete, RegexPatternString: aws.String("^/api/")},
		},
	})
	require.NoError(t, err)

	_, err = client.DeleteRegexPatternSet(ctx, &wafsdk.DeleteRegexPatternSetInput{
		RegexPatternSetId: aws.String(id),
		ChangeToken:       slice8ChangeToken(t, client),
	})
	require.NoError(t, err)
}

func testSlice8RegexMatchSetRealClient(t *testing.T) {
	t.Helper()

	client := newSlice8WAFClient(t)
	ctx := t.Context()

	patternOut, err := client.CreateRegexPatternSet(ctx, &wafsdk.CreateRegexPatternSetInput{
		Name:        aws.String("slice8-regexmatchset-pattern"),
		ChangeToken: slice8ChangeToken(t, client),
	})
	require.NoError(t, err)
	patternID := aws.ToString(patternOut.RegexPatternSet.RegexPatternSetId)

	createOut, err := client.CreateRegexMatchSet(ctx, &wafsdk.CreateRegexMatchSetInput{
		Name:        aws.String("slice8-regexmatchset"),
		ChangeToken: slice8ChangeToken(t, client),
	})
	require.NoError(t, err)
	id := aws.ToString(createOut.RegexMatchSet.RegexMatchSetId)
	require.NotEmpty(t, id)

	_, err = client.UpdateRegexMatchSet(ctx, &wafsdk.UpdateRegexMatchSetInput{
		RegexMatchSetId: aws.String(id),
		ChangeToken:     slice8ChangeToken(t, client),
		Updates: []types.RegexMatchSetUpdate{
			{
				Action: types.ChangeActionInsert,
				RegexMatchTuple: &types.RegexMatchTuple{
					FieldToMatch:       &types.FieldToMatch{Type: types.MatchFieldTypeUri},
					TextTransformation: types.TextTransformationNone,
					RegexPatternSetId:  aws.String(patternID),
				},
			},
		},
	})
	require.NoError(t, err)

	getOut, err := client.GetRegexMatchSet(
		ctx,
		&wafsdk.GetRegexMatchSetInput{RegexMatchSetId: aws.String(id)},
	)
	require.NoError(t, err)
	require.Len(t, getOut.RegexMatchSet.RegexMatchTuples, 1)
	assert.Equal(
		t,
		patternID,
		aws.ToString(getOut.RegexMatchSet.RegexMatchTuples[0].RegexPatternSetId),
	)

	listOut, err := client.ListRegexMatchSets(ctx, &wafsdk.ListRegexMatchSetsInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, listOut.RegexMatchSets)

	_, err = client.UpdateRegexMatchSet(ctx, &wafsdk.UpdateRegexMatchSetInput{
		RegexMatchSetId: aws.String(id),
		ChangeToken:     slice8ChangeToken(t, client),
		Updates: []types.RegexMatchSetUpdate{
			{
				Action: types.ChangeActionDelete,
				RegexMatchTuple: &types.RegexMatchTuple{
					FieldToMatch:       &types.FieldToMatch{Type: types.MatchFieldTypeUri},
					TextTransformation: types.TextTransformationNone,
					RegexPatternSetId:  aws.String(patternID),
				},
			},
		},
	})
	require.NoError(t, err)

	_, err = client.DeleteRegexMatchSet(ctx, &wafsdk.DeleteRegexMatchSetInput{
		RegexMatchSetId: aws.String(id),
		ChangeToken:     slice8ChangeToken(t, client),
	})
	require.NoError(t, err)
}

func testSlice8RuleRealClient(t *testing.T) {
	t.Helper()

	client := newSlice8WAFClient(t)
	ctx := t.Context()

	ipSetOut, err := client.CreateIPSet(ctx, &wafsdk.CreateIPSetInput{
		Name:        aws.String("slice8-rule-ipset"),
		ChangeToken: slice8ChangeToken(t, client),
	})
	require.NoError(t, err)
	ipSetID := aws.ToString(ipSetOut.IPSet.IPSetId)

	createOut, err := client.CreateRule(ctx, &wafsdk.CreateRuleInput{
		Name:        aws.String("slice8-rule"),
		MetricName:  aws.String("slice8rule"),
		ChangeToken: slice8ChangeToken(t, client),
	})
	require.NoError(t, err)
	ruleID := aws.ToString(createOut.Rule.RuleId)
	require.NotEmpty(t, ruleID)

	_, err = client.UpdateRule(ctx, &wafsdk.UpdateRuleInput{
		RuleId:      aws.String(ruleID),
		ChangeToken: slice8ChangeToken(t, client),
		Updates: []types.RuleUpdate{
			{
				Action: types.ChangeActionInsert,
				Predicate: &types.Predicate{
					DataId:  aws.String(ipSetID),
					Negated: aws.Bool(false),
					Type:    types.PredicateTypeIpMatch,
				},
			},
		},
	})
	require.NoError(t, err)

	getOut, err := client.GetRule(ctx, &wafsdk.GetRuleInput{RuleId: aws.String(ruleID)})
	require.NoError(t, err)
	require.Len(t, getOut.Rule.Predicates, 1)
	assert.Equal(t, ipSetID, aws.ToString(getOut.Rule.Predicates[0].DataId))

	listOut, err := client.ListRules(ctx, &wafsdk.ListRulesInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, listOut.Rules)

	_, err = client.UpdateRule(ctx, &wafsdk.UpdateRuleInput{
		RuleId:      aws.String(ruleID),
		ChangeToken: slice8ChangeToken(t, client),
		Updates: []types.RuleUpdate{
			{
				Action: types.ChangeActionDelete,
				Predicate: &types.Predicate{
					DataId:  aws.String(ipSetID),
					Negated: aws.Bool(false),
					Type:    types.PredicateTypeIpMatch,
				},
			},
		},
	})
	require.NoError(t, err)

	_, err = client.DeleteRule(ctx, &wafsdk.DeleteRuleInput{
		RuleId:      aws.String(ruleID),
		ChangeToken: slice8ChangeToken(t, client),
	})
	require.NoError(t, err)
}

func testSlice8RuleGroupRealClient(t *testing.T) {
	t.Helper()

	client := newSlice8WAFClient(t)
	ctx := t.Context()

	ruleOut, err := client.CreateRule(ctx, &wafsdk.CreateRuleInput{
		Name:        aws.String("slice8-rg-rule"),
		MetricName:  aws.String("slice8rgrule"),
		ChangeToken: slice8ChangeToken(t, client),
	})
	require.NoError(t, err)
	ruleID := aws.ToString(ruleOut.Rule.RuleId)

	createOut, err := client.CreateRuleGroup(ctx, &wafsdk.CreateRuleGroupInput{
		Name:        aws.String("slice8-rulegroup"),
		MetricName:  aws.String("slice8rulegroup"),
		ChangeToken: slice8ChangeToken(t, client),
	})
	require.NoError(t, err)
	ruleGroupID := aws.ToString(createOut.RuleGroup.RuleGroupId)
	require.NotEmpty(t, ruleGroupID)

	_, err = client.UpdateRuleGroup(ctx, &wafsdk.UpdateRuleGroupInput{
		RuleGroupId: aws.String(ruleGroupID),
		ChangeToken: slice8ChangeToken(t, client),
		Updates: []types.RuleGroupUpdate{
			{
				Action: types.ChangeActionInsert,
				ActivatedRule: &types.ActivatedRule{
					Priority: aws.Int32(1),
					RuleId:   aws.String(ruleID),
					Action:   &types.WafAction{Type: types.WafActionTypeBlock},
				},
			},
		},
	})
	require.NoError(t, err)

	getOut, err := client.GetRuleGroup(
		ctx,
		&wafsdk.GetRuleGroupInput{RuleGroupId: aws.String(ruleGroupID)},
	)
	require.NoError(t, err)
	assert.Equal(t, "slice8-rulegroup", aws.ToString(getOut.RuleGroup.Name))

	activatedOut, err := client.ListActivatedRulesInRuleGroup(
		ctx, &wafsdk.ListActivatedRulesInRuleGroupInput{RuleGroupId: aws.String(ruleGroupID)},
	)
	require.NoError(t, err)
	require.Len(t, activatedOut.ActivatedRules, 1)
	assert.Equal(t, ruleID, aws.ToString(activatedOut.ActivatedRules[0].RuleId))

	listOut, err := client.ListRuleGroups(ctx, &wafsdk.ListRuleGroupsInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, listOut.RuleGroups)

	subscribedOut, err := client.ListSubscribedRuleGroups(
		ctx,
		&wafsdk.ListSubscribedRuleGroupsInput{},
	)
	require.NoError(t, err)
	assert.NotNil(t, subscribedOut)

	_, err = client.UpdateRuleGroup(ctx, &wafsdk.UpdateRuleGroupInput{
		RuleGroupId: aws.String(ruleGroupID),
		ChangeToken: slice8ChangeToken(t, client),
		Updates: []types.RuleGroupUpdate{
			{
				Action: types.ChangeActionDelete,
				ActivatedRule: &types.ActivatedRule{
					Priority: aws.Int32(1),
					RuleId:   aws.String(ruleID),
					Action:   &types.WafAction{Type: types.WafActionTypeBlock},
				},
			},
		},
	})
	require.NoError(t, err)

	_, err = client.DeleteRuleGroup(ctx, &wafsdk.DeleteRuleGroupInput{
		RuleGroupId: aws.String(ruleGroupID),
		ChangeToken: slice8ChangeToken(t, client),
	})
	require.NoError(t, err)
}

func testSlice8RateBasedRuleRealClient(t *testing.T) {
	t.Helper()

	client := newSlice8WAFClient(t)
	ctx := t.Context()

	createOut, err := client.CreateRateBasedRule(ctx, &wafsdk.CreateRateBasedRuleInput{
		Name:        aws.String("slice8-rbr"),
		MetricName:  aws.String("slice8rbr"),
		RateKey:     types.RateKeyIp,
		RateLimit:   aws.Int64(2000),
		ChangeToken: slice8ChangeToken(t, client),
	})
	require.NoError(t, err)
	id := aws.ToString(createOut.Rule.RuleId)
	require.NotEmpty(t, id)

	_, err = client.UpdateRateBasedRule(ctx, &wafsdk.UpdateRateBasedRuleInput{
		RuleId:      aws.String(id),
		ChangeToken: slice8ChangeToken(t, client),
		RateLimit:   aws.Int64(3000),
		Updates:     []types.RuleUpdate{},
	})
	require.NoError(t, err)

	getOut, err := client.GetRateBasedRule(
		ctx,
		&wafsdk.GetRateBasedRuleInput{RuleId: aws.String(id)},
	)
	require.NoError(t, err)
	assert.Equal(t, int64(3000), aws.ToInt64(getOut.Rule.RateLimit))

	keysOut, err := client.GetRateBasedRuleManagedKeys(
		ctx, &wafsdk.GetRateBasedRuleManagedKeysInput{RuleId: aws.String(id)},
	)
	require.NoError(t, err)
	assert.NotNil(t, keysOut)

	listOut, err := client.ListRateBasedRules(ctx, &wafsdk.ListRateBasedRulesInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, listOut.Rules)

	_, err = client.DeleteRateBasedRule(ctx, &wafsdk.DeleteRateBasedRuleInput{
		RuleId:      aws.String(id),
		ChangeToken: slice8ChangeToken(t, client),
	})
	require.NoError(t, err)
}

func testSlice8WebACLRealClient(t *testing.T) {
	t.Helper()

	client := newSlice8WAFClient(t)
	ctx := t.Context()

	ruleOut, err := client.CreateRule(ctx, &wafsdk.CreateRuleInput{
		Name:        aws.String("slice8-webacl-rule"),
		MetricName:  aws.String("slice8webaclrule"),
		ChangeToken: slice8ChangeToken(t, client),
	})
	require.NoError(t, err)
	ruleID := aws.ToString(ruleOut.Rule.RuleId)

	createOut, err := client.CreateWebACL(ctx, &wafsdk.CreateWebACLInput{
		Name:          aws.String("slice8-webacl"),
		MetricName:    aws.String("slice8webacl"),
		DefaultAction: &types.WafAction{Type: types.WafActionTypeAllow},
		ChangeToken:   slice8ChangeToken(t, client),
	})
	require.NoError(t, err)
	webACLID := aws.ToString(createOut.WebACL.WebACLId)
	require.NotEmpty(t, webACLID)

	_, err = client.UpdateWebACL(ctx, &wafsdk.UpdateWebACLInput{
		WebACLId:    aws.String(webACLID),
		ChangeToken: slice8ChangeToken(t, client),
		Updates: []types.WebACLUpdate{
			{
				Action: types.ChangeActionInsert,
				ActivatedRule: &types.ActivatedRule{
					Priority: aws.Int32(1),
					RuleId:   aws.String(ruleID),
					Action:   &types.WafAction{Type: types.WafActionTypeBlock},
				},
			},
		},
	})
	require.NoError(t, err)

	getOut, err := client.GetWebACL(ctx, &wafsdk.GetWebACLInput{WebACLId: aws.String(webACLID)})
	require.NoError(t, err)
	require.Len(t, getOut.WebACL.Rules, 1)
	assert.Equal(t, ruleID, aws.ToString(getOut.WebACL.Rules[0].RuleId))

	listOut, err := client.ListWebACLs(ctx, &wafsdk.ListWebACLsInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, listOut.WebACLs)

	migrationOut, err := client.CreateWebACLMigrationStack(
		ctx,
		&wafsdk.CreateWebACLMigrationStackInput{
			WebACLId:              aws.String(webACLID),
			S3BucketName:          aws.String("slice8-migration-bucket"),
			IgnoreUnsupportedType: aws.Bool(false),
		},
	)
	if err != nil {
		t.Logf("CreateWebACLMigrationStack: %v", err)
	} else {
		assert.NotNil(t, migrationOut)
	}

	_, err = client.UpdateWebACL(ctx, &wafsdk.UpdateWebACLInput{
		WebACLId:    aws.String(webACLID),
		ChangeToken: slice8ChangeToken(t, client),
		Updates: []types.WebACLUpdate{
			{
				Action: types.ChangeActionDelete,
				ActivatedRule: &types.ActivatedRule{
					Priority: aws.Int32(1),
					RuleId:   aws.String(ruleID),
					Action:   &types.WafAction{Type: types.WafActionTypeBlock},
				},
			},
		},
	})
	require.NoError(t, err)

	_, err = client.DeleteWebACL(ctx, &wafsdk.DeleteWebACLInput{
		WebACLId:    aws.String(webACLID),
		ChangeToken: slice8ChangeToken(t, client),
	})
	require.NoError(t, err)
}

func testSlice8LoggingConfigurationRealClient(t *testing.T) {
	t.Helper()

	client := newSlice8WAFClient(t)
	ctx := t.Context()

	createOut, err := client.CreateWebACL(ctx, &wafsdk.CreateWebACLInput{
		Name:          aws.String("slice8-logging-webacl"),
		MetricName:    aws.String("slice8loggingwebacl"),
		DefaultAction: &types.WafAction{Type: types.WafActionTypeAllow},
		ChangeToken:   slice8ChangeToken(t, client),
	})
	require.NoError(t, err)
	resourceARNStr := resourceARN(t, "webacl", createOut.WebACL.WebACLId)

	putOut, err := client.PutLoggingConfiguration(ctx, &wafsdk.PutLoggingConfigurationInput{
		LoggingConfiguration: &types.LoggingConfiguration{
			ResourceArn: resourceARNStr,
			LogDestinationConfigs: []string{
				"arn:aws:firehose:us-east-1:123456789012:deliverystream/slice8",
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(
		t,
		aws.ToString(resourceARNStr),
		aws.ToString(putOut.LoggingConfiguration.ResourceArn),
	)

	getOut, err := client.GetLoggingConfiguration(
		ctx, &wafsdk.GetLoggingConfigurationInput{ResourceArn: resourceARNStr},
	)
	require.NoError(t, err)
	require.Len(t, getOut.LoggingConfiguration.LogDestinationConfigs, 1)

	listOut, err := client.ListLoggingConfigurations(ctx, &wafsdk.ListLoggingConfigurationsInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, listOut.LoggingConfigurations)

	_, err = client.DeleteLoggingConfiguration(
		ctx, &wafsdk.DeleteLoggingConfigurationInput{ResourceArn: resourceARNStr},
	)
	require.NoError(t, err)
}

func testSlice8PermissionPolicyRealClient(t *testing.T) {
	t.Helper()

	client := newSlice8WAFClient(t)
	ctx := t.Context()

	createOut, err := client.CreateRuleGroup(ctx, &wafsdk.CreateRuleGroupInput{
		Name:        aws.String("slice8-policy-rulegroup"),
		MetricName:  aws.String("slice8policyrulegroup"),
		ChangeToken: slice8ChangeToken(t, client),
	})
	require.NoError(t, err)
	resourceARNStr := resourceARN(t, "rulegroup", createOut.RuleGroup.RuleGroupId)

	policyStatement := map[string]any{
		"Version": "2012-10-17",
		"Statement": []map[string]any{
			{
				"Effect":    "Allow",
				"Principal": map[string]any{"AWS": "arn:aws:iam::111111111111:root"},
				"Action":    []string{"waf:GetRuleGroup"},
				"Resource":  aws.ToString(resourceARNStr),
			},
		},
	}

	policyBytes, err := json.Marshal(policyStatement)
	require.NoError(t, err)

	policy := string(policyBytes)

	_, err = client.PutPermissionPolicy(ctx, &wafsdk.PutPermissionPolicyInput{
		ResourceArn: resourceARNStr,
		Policy:      aws.String(policy),
	})
	require.NoError(t, err)

	getOut, err := client.GetPermissionPolicy(
		ctx,
		&wafsdk.GetPermissionPolicyInput{ResourceArn: resourceARNStr},
	)
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(getOut.Policy), "waf:GetRuleGroup")

	_, err = client.DeletePermissionPolicy(
		ctx,
		&wafsdk.DeletePermissionPolicyInput{ResourceArn: resourceARNStr},
	)
	require.NoError(t, err)
}

func testSlice8TagsRealClient(t *testing.T) {
	t.Helper()

	client := newSlice8WAFClient(t)
	ctx := t.Context()

	createOut, err := client.CreateIPSet(ctx, &wafsdk.CreateIPSetInput{
		Name:        aws.String("slice8-tags-ipset"),
		ChangeToken: slice8ChangeToken(t, client),
	})
	require.NoError(t, err)
	resourceARNStr := resourceARN(t, "ipset", createOut.IPSet.IPSetId)

	_, err = client.TagResource(ctx, &wafsdk.TagResourceInput{
		ResourceARN: resourceARNStr,
		Tags:        []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
	})
	require.NoError(t, err)

	listOut, err := client.ListTagsForResource(
		ctx,
		&wafsdk.ListTagsForResourceInput{ResourceARN: resourceARNStr},
	)
	require.NoError(t, err)
	require.Len(t, listOut.TagInfoForResource.TagList, 1)

	_, err = client.UntagResource(ctx, &wafsdk.UntagResourceInput{
		ResourceARN: resourceARNStr,
		TagKeys:     []string{"env"},
	})
	require.NoError(t, err)

	listOut2, err := client.ListTagsForResource(
		ctx,
		&wafsdk.ListTagsForResourceInput{ResourceARN: resourceARNStr},
	)
	require.NoError(t, err)
	assert.Empty(t, listOut2.TagInfoForResource.TagList)
}

func testSlice8ChangeTokenStatusRealClient(t *testing.T) {
	t.Helper()

	client := newSlice8WAFClient(t)
	ctx := t.Context()

	token := slice8ChangeToken(t, client)

	statusOut, err := client.GetChangeTokenStatus(
		ctx,
		&wafsdk.GetChangeTokenStatusInput{ChangeToken: token},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, statusOut.ChangeTokenStatus)
}
