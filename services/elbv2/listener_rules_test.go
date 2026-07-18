package elbv2_test

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateRule tests rule creation.
func TestCreateRule(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "rule-lb")
	tgArn := mustCreateTG(t, h, "rule-tg")

	listenerRec := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTP"},
		"Port":                                   {"80"},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
	})
	require.Equal(t, http.StatusOK, listenerRec.Code)

	var listenerResp struct {
		Result struct {
			Listeners struct {
				Members []struct {
					ListenerArn string `xml:"ListenerArn"`
				} `xml:"member"`
			} `xml:"Listeners"`
		} `xml:"CreateListenerResult"`
	}
	require.NoError(t, xml.Unmarshal(listenerRec.Body.Bytes(), &listenerResp))
	require.Len(t, listenerResp.Result.Listeners.Members, 1)
	listenerArn := listenerResp.Result.Listeners.Members[0].ListenerArn

	ruleRec := doELBv2(t, h, url.Values{
		"Action":                          {"CreateRule"},
		"Version":                         {"2015-12-01"},
		"ListenerArn":                     {listenerArn},
		"Priority":                        {"1"},
		"Actions.member.1.Type":           {"forward"},
		"Actions.member.1.TargetGroupArn": {tgArn},
	})
	assert.Equal(t, http.StatusOK, ruleRec.Code)

	var ruleResp struct {
		Result struct {
			Rules struct {
				Members []struct {
					RuleArn string `xml:"RuleArn"`
				} `xml:"member"`
			} `xml:"Rules"`
		} `xml:"CreateRuleResult"`
	}
	parseXMLBody(t, ruleRec, &ruleResp)
	require.Len(t, ruleResp.Result.Rules.Members, 1)
	assert.NotEmpty(t, ruleResp.Result.Rules.Members[0].RuleArn)
}

// TestDeleteRule tests rule deletion.
func TestDeleteRule(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "del-rule-lb")
	tgArn := mustCreateTG(t, h, "del-rule-tg")

	listenerRec := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTP"},
		"Port":                                   {"80"},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
	})
	require.Equal(t, http.StatusOK, listenerRec.Code)

	var listenerResp struct {
		Result struct {
			Listeners struct {
				Members []struct {
					ListenerArn string `xml:"ListenerArn"`
				} `xml:"member"`
			} `xml:"Listeners"`
		} `xml:"CreateListenerResult"`
	}
	require.NoError(t, xml.Unmarshal(listenerRec.Body.Bytes(), &listenerResp))
	listenerArn := listenerResp.Result.Listeners.Members[0].ListenerArn

	ruleRec := doELBv2(t, h, url.Values{
		"Action":                          {"CreateRule"},
		"Version":                         {"2015-12-01"},
		"ListenerArn":                     {listenerArn},
		"Priority":                        {"1"},
		"Actions.member.1.Type":           {"forward"},
		"Actions.member.1.TargetGroupArn": {tgArn},
	})
	require.Equal(t, http.StatusOK, ruleRec.Code)

	var ruleResp struct {
		Result struct {
			Rules struct {
				Members []struct {
					RuleArn string `xml:"RuleArn"`
				} `xml:"member"`
			} `xml:"Rules"`
		} `xml:"CreateRuleResult"`
	}
	require.NoError(t, xml.Unmarshal(ruleRec.Body.Bytes(), &ruleResp))
	ruleArn := ruleResp.Result.Rules.Members[0].RuleArn

	// Delete the rule
	rec := doELBv2(t, h, url.Values{
		"Action":  {"DeleteRule"},
		"Version": {"2015-12-01"},
		"RuleArn": {ruleArn},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Test missing arn
	rec2 := doELBv2(t, h, url.Values{
		"Action":  {"DeleteRule"},
		"Version": {"2015-12-01"},
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)

	// Test not found
	rec3 := doELBv2(t, h, url.Values{
		"Action":  {"DeleteRule"},
		"Version": {"2015-12-01"},
		"RuleArn": {"arn:aws:no-such-rule"},
	})
	assert.Equal(t, http.StatusBadRequest, rec3.Code)
}

// TestDescribeRules tests rule describe operations.
func TestDescribeRules(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "desc-rule-lb")
	tgArn := mustCreateTG(t, h, "desc-rule-tg")

	listenerRec := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTP"},
		"Port":                                   {"80"},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
	})
	require.Equal(t, http.StatusOK, listenerRec.Code)

	var listenerResp struct {
		Result struct {
			Listeners struct {
				Members []struct {
					ListenerArn string `xml:"ListenerArn"`
				} `xml:"member"`
			} `xml:"Listeners"`
		} `xml:"CreateListenerResult"`
	}
	require.NoError(t, xml.Unmarshal(listenerRec.Body.Bytes(), &listenerResp))
	listenerArn := listenerResp.Result.Listeners.Members[0].ListenerArn

	doELBv2(t, h, url.Values{
		"Action":                          {"CreateRule"},
		"Version":                         {"2015-12-01"},
		"ListenerArn":                     {listenerArn},
		"Priority":                        {"1"},
		"Actions.member.1.Type":           {"forward"},
		"Actions.member.1.TargetGroupArn": {tgArn},
	})

	rec := doELBv2(t, h, url.Values{
		"Action":      {"DescribeRules"},
		"Version":     {"2015-12-01"},
		"ListenerArn": {listenerArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			Rules struct {
				Members []struct {
					RuleArn   string `xml:"RuleArn"`
					IsDefault bool   `xml:"IsDefault"`
				} `xml:"member"`
			} `xml:"Rules"`
		} `xml:"DescribeRulesResult"`
	}
	parseXMLBody(t, rec, &resp)
	// 2 rules expected: 1 default (auto-created by CreateListener) + 1 explicit.
	assert.Len(t, resp.Result.Rules.Members, 2)
	defaultCount := 0
	for _, r := range resp.Result.Rules.Members {
		if r.IsDefault {
			defaultCount++
		}
	}
	assert.Equal(t, 1, defaultCount, "expected exactly one default rule")
}

// TestModifyRule tests rule modification.
func TestModifyRule(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "mod-rule-lb")
	tgArn := mustCreateTG(t, h, "mod-rule-tg")

	listenerRec := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTP"},
		"Port":                                   {"80"},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
	})
	require.Equal(t, http.StatusOK, listenerRec.Code)

	var listenerResp struct {
		Result struct {
			Listeners struct {
				Members []struct {
					ListenerArn string `xml:"ListenerArn"`
				} `xml:"member"`
			} `xml:"Listeners"`
		} `xml:"CreateListenerResult"`
	}
	require.NoError(t, xml.Unmarshal(listenerRec.Body.Bytes(), &listenerResp))
	listenerArn := listenerResp.Result.Listeners.Members[0].ListenerArn

	ruleRec := doELBv2(t, h, url.Values{
		"Action":                          {"CreateRule"},
		"Version":                         {"2015-12-01"},
		"ListenerArn":                     {listenerArn},
		"Priority":                        {"1"},
		"Actions.member.1.Type":           {"forward"},
		"Actions.member.1.TargetGroupArn": {tgArn},
	})
	require.Equal(t, http.StatusOK, ruleRec.Code)

	var ruleResp struct {
		Result struct {
			Rules struct {
				Members []struct {
					RuleArn string `xml:"RuleArn"`
				} `xml:"member"`
			} `xml:"Rules"`
		} `xml:"CreateRuleResult"`
	}
	require.NoError(t, xml.Unmarshal(ruleRec.Body.Bytes(), &ruleResp))
	ruleArn := ruleResp.Result.Rules.Members[0].RuleArn

	rec := doELBv2(t, h, url.Values{
		"Action":  {"ModifyRule"},
		"Version": {"2015-12-01"},
		"RuleArn": {ruleArn},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Not found case
	rec2 := doELBv2(t, h, url.Values{
		"Action":  {"ModifyRule"},
		"Version": {"2015-12-01"},
		"RuleArn": {"arn:aws:no-such-rule"},
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)

	// Missing arn
	rec3 := doELBv2(t, h, url.Values{
		"Action":  {"ModifyRule"},
		"Version": {"2015-12-01"},
	})
	assert.Equal(t, http.StatusBadRequest, rec3.Code)
}

// TestCreateRuleMissingListenerARN tests missing listener ARN for CreateRule.
func TestCreateRuleMissingListenerARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doELBv2(t, h, url.Values{
		"Action":  {"CreateRule"},
		"Version": {"2015-12-01"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestCreateRuleListenerNotFound tests CreateRule with nonexistent listener.
func TestCreateRuleListenerNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doELBv2(t, h, url.Values{
		"Action":                {"CreateRule"},
		"Version":               {"2015-12-01"},
		"ListenerArn":           {"arn:aws:elasticloadbalancing:us-east-1:123456789012:listener/app/no/0/no"},
		"Priority":              {"1"},
		"Actions.member.1.Type": {"fixed-response"},
		"Actions.member.1.FixedResponseConfig.StatusCode": {"200"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRuleTagOperations tests AddTags, DescribeTags, RemoveTags, and DeleteRule on a Rule resource.
func TestRuleTagOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "rule-tag-lb")
	tgArn := mustCreateTG(t, h, "rule-tag-tg")

	listenerRec := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTP"},
		"Port":                                   {"80"},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
	})
	require.Equal(t, http.StatusOK, listenerRec.Code)

	var listenerResp struct {
		Result struct {
			Listeners struct {
				Members []struct {
					ListenerArn string `xml:"ListenerArn"`
				} `xml:"member"`
			} `xml:"Listeners"`
		} `xml:"CreateListenerResult"`
	}

	require.NoError(t, xml.Unmarshal(listenerRec.Body.Bytes(), &listenerResp))
	listenerArn := listenerResp.Result.Listeners.Members[0].ListenerArn

	// Create a rule with an initial tag.
	ruleRec := doELBv2(t, h, url.Values{
		"Action":                          {"CreateRule"},
		"Version":                         {"2015-12-01"},
		"ListenerArn":                     {listenerArn},
		"Priority":                        {"10"},
		"Actions.member.1.Type":           {"forward"},
		"Actions.member.1.TargetGroupArn": {tgArn},
		"Tags.member.1.Key":               {"env"},
		"Tags.member.1.Value":             {"prod"},
	})
	require.Equal(t, http.StatusOK, ruleRec.Code)

	var ruleResp struct {
		Result struct {
			Rules struct {
				Members []struct {
					RuleArn string `xml:"RuleArn"`
				} `xml:"member"`
			} `xml:"Rules"`
		} `xml:"CreateRuleResult"`
	}

	require.NoError(t, xml.Unmarshal(ruleRec.Body.Bytes(), &ruleResp))
	ruleArn := ruleResp.Result.Rules.Members[0].RuleArn

	// DescribeTags — should return the initial "env" tag.
	descRec := doELBv2(t, h, url.Values{
		"Action":                {"DescribeTags"},
		"Version":               {"2015-12-01"},
		"ResourceArns.member.1": {ruleArn},
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp struct {
		Result struct {
			TagDescriptions struct {
				Members []struct {
					Tags struct {
						Members []struct {
							Key   string `xml:"Key"`
							Value string `xml:"Value"`
						} `xml:"member"`
					} `xml:"Tags"`
				} `xml:"member"`
			} `xml:"TagDescriptions"`
		} `xml:"DescribeTagsResult"`
	}

	require.NoError(t, xml.Unmarshal(descRec.Body.Bytes(), &descResp))
	require.Len(t, descResp.Result.TagDescriptions.Members, 1)
	require.Len(t, descResp.Result.TagDescriptions.Members[0].Tags.Members, 1)
	assert.Equal(t, "env", descResp.Result.TagDescriptions.Members[0].Tags.Members[0].Key)
	assert.Equal(t, "prod", descResp.Result.TagDescriptions.Members[0].Tags.Members[0].Value)

	// AddTags — add a second tag.
	addRec := doELBv2(t, h, url.Values{
		"Action":                {"AddTags"},
		"Version":               {"2015-12-01"},
		"ResourceArns.member.1": {ruleArn},
		"Tags.member.1.Key":     {"team"},
		"Tags.member.1.Value":   {"platform"},
	})
	assert.Equal(t, http.StatusOK, addRec.Code)

	// RemoveTags — remove the "env" tag.
	rmRec := doELBv2(t, h, url.Values{
		"Action":                {"RemoveTags"},
		"Version":               {"2015-12-01"},
		"ResourceArns.member.1": {ruleArn},
		"TagKeys.member.1":      {"env"},
	})
	assert.Equal(t, http.StatusOK, rmRec.Code)

	// DeleteRule — should close tags without panic.
	delRec := doELBv2(t, h, url.Values{
		"Action":  {"DeleteRule"},
		"Version": {"2015-12-01"},
		"RuleArn": {ruleArn},
	})
	assert.Equal(t, http.StatusOK, delRec.Code)
}

// TestDescribeRulesSortedByPriority tests that DescribeRules returns rules sorted numerically.
func TestDescribeRulesSortedByPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "sort-rules-lb")
	tgArn := mustCreateTG(t, h, "sort-rules-tg")
	listenerArn := mustCreateListener(t, h, lbArn, tgArn)

	priorities := []string{"10", "2", "100", "1"}
	for _, p := range priorities {
		rec := doELBv2(t, h, url.Values{
			"Action":                          {"CreateRule"},
			"Version":                         {"2015-12-01"},
			"ListenerArn":                     {listenerArn},
			"Priority":                        {p},
			"Actions.member.1.Type":           {"forward"},
			"Actions.member.1.TargetGroupArn": {tgArn},
			"Conditions.member.1.Field":       {"path-pattern"},
			"Conditions.member.1.PathPatternConfig.Values.member.1": {"/" + p},
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doELBv2(t, h, url.Values{
		"Action":      {"DescribeRules"},
		"Version":     {"2015-12-01"},
		"ListenerArn": {listenerArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			Rules struct {
				Members []struct {
					Priority  string `xml:"Priority"`
					IsDefault bool   `xml:"IsDefault"`
				} `xml:"member"`
			} `xml:"Rules"`
		} `xml:"DescribeRulesResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

	// Expect: 1, 2, 10, 100, default
	got := make([]string, 0, len(resp.Result.Rules.Members))
	for _, r := range resp.Result.Rules.Members {
		got = append(got, r.Priority)
	}

	expected := []string{"1", "2", "10", "100", "default"}
	assert.Equal(t, expected, got)
}

// TestCreateRuleNoActions tests that missing Actions in CreateRule returns 400.
func TestCreateRuleNoActions(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "no-rule-actions-lb")
	tgArn := mustCreateTG(t, h, "no-rule-actions-tg")
	listenerArn := mustCreateListener(t, h, lbArn, tgArn)

	rec := doELBv2(t, h, url.Values{
		"Action":      {"CreateRule"},
		"Version":     {"2015-12-01"},
		"ListenerArn": {listenerArn},
		"Priority":    {"1"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestDeleteTGReferencedByRule tests that TG referenced in a rule action cannot be deleted.
func TestDeleteTGReferencedByRule(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "rule-ref-lb")
	tgArn1 := mustCreateTG(t, h, "rule-ref-tg1")
	tgArn2 := mustCreateTG(t, h, "rule-ref-tg2")
	listenerArn := mustCreateListener(t, h, lbArn, tgArn1)

	// Create a rule referencing tgArn2.
	rec := doELBv2(t, h, url.Values{
		"Action":                          {"CreateRule"},
		"Version":                         {"2015-12-01"},
		"ListenerArn":                     {listenerArn},
		"Priority":                        {"10"},
		"Actions.member.1.Type":           {"forward"},
		"Actions.member.1.TargetGroupArn": {tgArn2},
		"Conditions.member.1.Field":       {"path-pattern"},
		"Conditions.member.1.PathPatternConfig.Values.member.1": {"/rule"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Attempting to delete tgArn2 should fail.
	delRec := doELBv2(t, h, url.Values{
		"Action":         {"DeleteTargetGroup"},
		"Version":        {"2015-12-01"},
		"TargetGroupArn": {tgArn2},
	})
	assert.Equal(t, http.StatusBadRequest, delRec.Code)
}

func TestDescribeRules_SortedByPriority(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "sort-rules-lb")
	tgArn := b1CreateTG(t, h, "sort-rules-tg")
	lArn := b1CreateListener(t, h, lbArn, tgArn)

	for _, prio := range []string{"30", "10", "20"} {
		doELBv2(t, h, url.Values{
			"Action":                    {"CreateRule"},
			"Version":                   {"2015-12-01"},
			"ListenerArn":               {lArn},
			"Priority":                  {prio},
			"Conditions.member.1.Field": {"path-pattern"},
			"Conditions.member.1.PathPatternConfig.Values.member.1": {"/p" + prio + "/*"},
			"Actions.member.1.Type":                                 {"forward"},
			"Actions.member.1.TargetGroupArn":                       {tgArn},
		})
	}

	rec := doELBv2(t, h, url.Values{
		"Action":      {"DescribeRules"},
		"Version":     {"2015-12-01"},
		"ListenerArn": {lArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			Rules struct {
				Members []struct {
					Priority string `xml:"Priority"`
				} `xml:"member"`
			} `xml:"Rules"`
		} `xml:"DescribeRulesResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	priorities := make([]string, 0)
	for _, m := range resp.Result.Rules.Members {
		if m.Priority != "default" {
			priorities = append(priorities, m.Priority)
		}
	}
	require.Len(t, priorities, 3)
	assert.Equal(t, "10", priorities[0])
	assert.Equal(t, "20", priorities[1])
	assert.Equal(t, "30", priorities[2])
}

func TestDeleteRule_Success(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "del-rule-lb")
	tgArn := b1CreateTG(t, h, "del-rule-tg")
	lArn := b1CreateListener(t, h, lbArn, tgArn)

	createRec := doELBv2(t, h, url.Values{
		"Action":                    {"CreateRule"},
		"Version":                   {"2015-12-01"},
		"ListenerArn":               {lArn},
		"Priority":                  {"100"},
		"Conditions.member.1.Field": {"path-pattern"},
		"Conditions.member.1.PathPatternConfig.Values.member.1": {"/delete/*"},
		"Actions.member.1.Type":                                 {"forward"},
		"Actions.member.1.TargetGroupArn":                       {tgArn},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var ruleResp struct {
		Result struct {
			Rules struct {
				Members []struct {
					RuleArn string `xml:"RuleArn"`
				} `xml:"member"`
			} `xml:"Rules"`
		} `xml:"CreateRuleResult"`
	}
	require.NoError(t, xml.Unmarshal(createRec.Body.Bytes(), &ruleResp))
	ruleArn := ruleResp.Result.Rules.Members[0].RuleArn

	delRec := doELBv2(t, h, url.Values{
		"Action":  {"DeleteRule"},
		"Version": {"2015-12-01"},
		"RuleArn": {ruleArn},
	})
	assert.Equal(t, http.StatusOK, delRec.Code)
}

// TestDescribeRules_UnknownArnReturnsNotFound verifies that querying a
// non-existent rule ARN (without listenerArn) returns RuleNotFound (HTTP 400, AWS query-protocol status).
func TestDescribeRules_UnknownArnReturnsNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "single_unknown_rule_arn"},
		{name: "mix_known_and_unknown_rule_arns"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newBatch2Handler()
			ghostArn := "arn:aws:elasticloadbalancing:us-east-1:000000000000:" +
				"listener-rule/app/lb/0123456789abcdef/0000000000000000/ghost"

			vals := url.Values{
				"Action":  {"DescribeRules"},
				"Version": {"2015-12-01"},
			}

			if tc.name == "mix_known_and_unknown_rule_arns" {
				lbArn := mustCreateLB(t, h, "rule-lb-mix")
				tgArn := mustCreateTG(t, h, "rule-tg-mix")
				listenerArn := mustCreateListener(t, h, lbArn, tgArn)
				realRuleArn := mustCreateRule(t, h, listenerArn, tgArn, "10")
				vals.Set("RuleArns.member.1", realRuleArn)
				vals.Set("RuleArns.member.2", ghostArn)
			} else {
				vals.Set("RuleArns.member.1", ghostArn)
			}

			rec := doELBv2(t, h, vals)
			assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
		})
	}
}

// TestDescribeRules_AllKnownArnsSucceeds verifies that DescribeRules
// without listenerArn still works when all rule ARNs exist.
func TestDescribeRules_AllKnownArnsSucceeds(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()
	lbArn := mustCreateLB(t, h, "rule-lb-known")
	tgArn := mustCreateTG(t, h, "rule-tg-known")
	listenerArn := mustCreateListener(t, h, lbArn, tgArn)
	ruleArn1 := mustCreateRule(t, h, listenerArn, tgArn, "10")
	ruleArn2 := mustCreateRule(t, h, listenerArn, tgArn, "20")

	rec := doELBv2(t, h, url.Values{
		"Action":            {"DescribeRules"},
		"Version":           {"2015-12-01"},
		"RuleArns.member.1": {ruleArn1},
		"RuleArns.member.2": {ruleArn2},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp struct {
		Result struct {
			Rules struct {
				Members []struct {
					RuleArn string `xml:"RuleArn"`
				} `xml:"member"`
			} `xml:"Rules"`
		} `xml:"DescribeRulesResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp.Result.Rules.Members, 2)
}
