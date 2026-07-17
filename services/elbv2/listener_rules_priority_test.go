package elbv2_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elbv2"
)

// TestELBv2_SetRulePriorities validates rule priority updates.
func TestELBv2_SetRulePriorities(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elbv2.Handler) url.Values
		checkResp  func(t *testing.T, rec *httptest.ResponseRecorder)
		name       string
		wantStatus int
	}{
		{
			name: "update_priorities_success",
			setup: func(t *testing.T, h *elbv2.Handler) url.Values {
				t.Helper()
				lbArn := mustCreateLB(t, h, "prio-lb")
				tgArn := mustCreateTG(t, h, "prio-tg")
				listenerArn := mustCreateListener(t, h, lbArn, tgArn)

				rec := doELBv2(t, h, url.Values{
					"Action":                              {"CreateRule"},
					"Version":                             {"2015-12-01"},
					"ListenerArn":                         {listenerArn},
					"Priority":                            {"100"},
					"Actions.member.1.Type":               {"forward"},
					"Actions.member.1.TargetGroupArn":     {tgArn},
					"Conditions.member.1.Field":           {"path-pattern"},
					"Conditions.member.1.Values.member.1": {"/api/*"},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var resp struct {
					Result struct {
						Rules struct {
							Members []struct {
								RuleArn string `xml:"RuleArn"`
							} `xml:"member"`
						} `xml:"Rules"`
					} `xml:"CreateRuleResult"`
				}
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
				require.Len(t, resp.Result.Rules.Members, 1)
				ruleArn := resp.Result.Rules.Members[0].RuleArn

				return url.Values{
					"Action":                           {"SetRulePriorities"},
					"Version":                          {"2015-12-01"},
					"RulePriorities.member.1.RuleArn":  {ruleArn},
					"RulePriorities.member.1.Priority": {"200"},
				}
			},
			wantStatus: http.StatusOK,
			checkResp: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var resp struct {
					Result struct {
						Rules struct {
							Members []struct {
								Priority string `xml:"Priority"`
							} `xml:"member"`
						} `xml:"Rules"`
					} `xml:"SetRulePrioritiesResult"`
				}
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
				require.Len(t, resp.Result.Rules.Members, 1)
				assert.Equal(t, "200", resp.Result.Rules.Members[0].Priority)
			},
		},
		{
			name: "rule_not_found",
			setup: func(t *testing.T, _ *elbv2.Handler) url.Values {
				t.Helper()

				return url.Values{
					"Action":  {"SetRulePriorities"},
					"Version": {"2015-12-01"},
					"RulePriorities.member.1.RuleArn": {
						"arn:aws:elasticloadbalancing:us-east-1:123:listener-rule/app/lb/id/id/nonexistent",
					},
					"RulePriorities.member.1.Priority": {"10"},
				}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "no_priorities_provided",
			setup: func(t *testing.T, _ *elbv2.Handler) url.Values {
				t.Helper()

				return url.Values{
					"Action":  {"SetRulePriorities"},
					"Version": {"2015-12-01"},
				}
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			vals := tt.setup(t, h)

			rec := doELBv2(t, h, vals)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.checkResp != nil {
				tt.checkResp(t, rec)
			}
		})
	}
}

// TestDefaultRuleDeletionProtected verifies default rule cannot be deleted.
func TestDefaultRuleDeletionProtected(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "default-rule-lb")
	tgArn := mustCreateTG(t, h, "default-rule-tg")
	listenerArn := mustCreateListener(t, h, lbArn, tgArn)

	// Get the default rule ARN.
	rec := doELBv2(t, h, url.Values{
		"Action":      {"DescribeRules"},
		"Version":     {"2015-12-01"},
		"ListenerArn": {listenerArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var rulesResp struct {
		Result struct {
			Rules struct {
				Members []struct {
					RuleArn   string `xml:"RuleArn"`
					IsDefault bool   `xml:"IsDefault"`
				} `xml:"member"`
			} `xml:"Rules"`
		} `xml:"DescribeRulesResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &rulesResp))

	var defaultRuleArn string
	for _, r := range rulesResp.Result.Rules.Members {
		if r.IsDefault {
			defaultRuleArn = r.RuleArn

			break
		}
	}
	require.NotEmpty(t, defaultRuleArn)

	// Attempting to delete the default rule should fail.
	delRec := doELBv2(t, h, url.Values{
		"Action":  {"DeleteRule"},
		"Version": {"2015-12-01"},
		"RuleArn": {defaultRuleArn},
	})
	assert.Equal(t, http.StatusBadRequest, delRec.Code)
	assert.Contains(t, delRec.Body.String(), "OperationNotPermitted")
}

// TestCreateRulePriorityValidation verifies priority must be 1-50000.
func TestCreateRulePriorityValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		priority   string
		wantStatus int
	}{
		{name: "valid_1", priority: "1", wantStatus: http.StatusOK},
		{name: "valid_50000", priority: "50000", wantStatus: http.StatusOK},
		{name: "zero_invalid", priority: "0", wantStatus: http.StatusBadRequest},
		{name: "negative_invalid", priority: "-1", wantStatus: http.StatusBadRequest},
		{name: "too_large", priority: "50001", wantStatus: http.StatusBadRequest},
		{name: "non_numeric", priority: "abc", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			lbArn := mustCreateLB(t, h, "prio-lb")
			tgArn := mustCreateTG(t, h, "prio-tg")
			listenerArn := mustCreateListener(t, h, lbArn, tgArn)

			rec := doELBv2(t, h, url.Values{
				"Action":                          {"CreateRule"},
				"Version":                         {"2015-12-01"},
				"ListenerArn":                     {listenerArn},
				"Priority":                        {tt.priority},
				"Actions.member.1.Type":           {"forward"},
				"Actions.member.1.TargetGroupArn": {tgArn},
				"Conditions.member.1.Field":       {"path-pattern"},
				"Conditions.member.1.PathPatternConfig.Values.member.1": {"/test"},
			})
			assert.Equal(t, tt.wantStatus, rec.Code, "body: %s", rec.Body.String())
		})
	}
}

// TestRuleCounterAfterDelete tests that rule ARNs remain unique after deletes.
func TestRuleCounterAfterDelete(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "rule-counter-lb")
	tgArn := mustCreateTG(t, h, "rule-counter-tg")
	listenerArn := mustCreateListener(t, h, lbArn, tgArn)

	// Create rule at priority 1.
	rec1 := doELBv2(t, h, url.Values{
		"Action":                          {"CreateRule"},
		"Version":                         {"2015-12-01"},
		"ListenerArn":                     {listenerArn},
		"Priority":                        {"1"},
		"Actions.member.1.Type":           {"forward"},
		"Actions.member.1.TargetGroupArn": {tgArn},
		"Conditions.member.1.Field":       {"path-pattern"},
		"Conditions.member.1.PathPatternConfig.Values.member.1": {"/a"},
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	var r1Resp struct {
		Result struct {
			Rules struct {
				Members []struct {
					RuleArn string `xml:"RuleArn"`
				} `xml:"member"`
			} `xml:"Rules"`
		} `xml:"CreateRuleResult"`
	}
	require.NoError(t, xml.Unmarshal(rec1.Body.Bytes(), &r1Resp))
	require.Len(t, r1Resp.Result.Rules.Members, 1)
	ruleArn1 := r1Resp.Result.Rules.Members[0].RuleArn

	// Delete the rule.
	delRec := doELBv2(t, h, url.Values{
		"Action":  {"DeleteRule"},
		"Version": {"2015-12-01"},
		"RuleArn": {ruleArn1},
	})
	require.Equal(t, http.StatusOK, delRec.Code)

	// Create another rule - should have a different ARN, not the same as deleted rule.
	rec2 := doELBv2(t, h, url.Values{
		"Action":                          {"CreateRule"},
		"Version":                         {"2015-12-01"},
		"ListenerArn":                     {listenerArn},
		"Priority":                        {"2"},
		"Actions.member.1.Type":           {"forward"},
		"Actions.member.1.TargetGroupArn": {tgArn},
		"Conditions.member.1.Field":       {"path-pattern"},
		"Conditions.member.1.PathPatternConfig.Values.member.1": {"/b"},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var r2Resp struct {
		Result struct {
			Rules struct {
				Members []struct {
					RuleArn string `xml:"RuleArn"`
				} `xml:"member"`
			} `xml:"Rules"`
		} `xml:"CreateRuleResult"`
	}
	require.NoError(t, xml.Unmarshal(rec2.Body.Bytes(), &r2Resp))
	require.Len(t, r2Resp.Result.Rules.Members, 1)
	ruleArn2 := r2Resp.Result.Rules.Members[0].RuleArn

	assert.NotEqual(t, ruleArn1, ruleArn2, "new rule ARN must differ from deleted rule ARN")
}

// TestDuplicateRulePriorityErrorCode tests that ErrDuplicateRulePriority returns the real
// AWS PriorityInUse error code (not the fabricated "DuplicatePriority" code the mock
// used to return; verified against aws-sdk-go-v2/service/elasticloadbalancingv2/types).
func TestDuplicateRulePriorityErrorCode(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "dup-prio-lb")
	tgArn := mustCreateTG(t, h, "dup-prio-tg")
	listenerArn := mustCreateListener(t, h, lbArn, tgArn)

	// Create rule at priority 5.
	rec := doELBv2(t, h, url.Values{
		"Action":                          {"CreateRule"},
		"Version":                         {"2015-12-01"},
		"ListenerArn":                     {listenerArn},
		"Priority":                        {"5"},
		"Actions.member.1.Type":           {"forward"},
		"Actions.member.1.TargetGroupArn": {tgArn},
		"Conditions.member.1.Field":       {"path-pattern"},
		"Conditions.member.1.PathPatternConfig.Values.member.1": {"/first"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Create another rule at the same priority.
	rec2 := doELBv2(t, h, url.Values{
		"Action":                          {"CreateRule"},
		"Version":                         {"2015-12-01"},
		"ListenerArn":                     {listenerArn},
		"Priority":                        {"5"},
		"Actions.member.1.Type":           {"forward"},
		"Actions.member.1.TargetGroupArn": {tgArn},
		"Conditions.member.1.Field":       {"path-pattern"},
		"Conditions.member.1.PathPatternConfig.Values.member.1": {"/second"},
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)

	var errResp struct {
		Error struct {
			Code string `xml:"Code"`
		} `xml:"Error"`
	}
	require.NoError(t, xml.Unmarshal(rec2.Body.Bytes(), &errResp))
	assert.Equal(t, "PriorityInUse", errResp.Error.Code)
}

// TestCreateRulePriorityRequired verifies that Priority is required for CreateRule.
func TestCreateRulePriorityRequired(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "priority-req-lb")
	tgArn := mustCreateTG(t, h, "priority-req-tg")
	listenerArn := mustCreateListener(t, h, lbArn, tgArn)

	rec := doELBv2(t, h, url.Values{
		"Action":                          {"CreateRule"},
		"Version":                         {"2015-12-01"},
		"ListenerArn":                     {listenerArn},
		"Actions.member.1.Type":           {"forward"},
		"Actions.member.1.TargetGroupArn": {tgArn},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestSetRulePrioritiesDefaultRuleRejected verifies that the default rule cannot be reordered.
func TestSetRulePrioritiesDefaultRuleRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "srp-default-lb")
	tgArn := mustCreateTG(t, h, "srp-default-tg")
	listenerArn := mustCreateListener(t, h, lbArn, tgArn)

	// Find the default rule ARN.
	rulesRec := doELBv2(t, h, url.Values{
		"Action":      {"DescribeRules"},
		"Version":     {"2015-12-01"},
		"ListenerArn": {listenerArn},
	})
	require.Equal(t, http.StatusOK, rulesRec.Code)

	var rulesResp struct {
		Result struct {
			Rules struct {
				Members []struct {
					RuleArn   string `xml:"RuleArn"`
					IsDefault bool   `xml:"IsDefault"`
				} `xml:"member"`
			} `xml:"Rules"`
		} `xml:"DescribeRulesResult"`
	}
	require.NoError(t, xml.Unmarshal(rulesRec.Body.Bytes(), &rulesResp))

	var defaultRuleArn string
	for _, r := range rulesResp.Result.Rules.Members {
		if r.IsDefault {
			defaultRuleArn = r.RuleArn
		}
	}
	require.NotEmpty(t, defaultRuleArn)

	// Attempt to set priority on default rule should fail.
	rec := doELBv2(t, h, url.Values{
		"Action":                           {"SetRulePriorities"},
		"Version":                          {"2015-12-01"},
		"RulePriorities.member.1.RuleArn":  {defaultRuleArn},
		"RulePriorities.member.1.Priority": {"5"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestSetRulePriorities(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "set-prio-lb")
	tgArn := b1CreateTG(t, h, "set-prio-tg")
	lArn := b1CreateListener(t, h, lbArn, tgArn)

	ruleArns := make([]string, 0, 2)
	for _, prio := range []string{"1", "2"} {
		r := doELBv2(t, h, url.Values{
			"Action":                    {"CreateRule"},
			"Version":                   {"2015-12-01"},
			"ListenerArn":               {lArn},
			"Priority":                  {prio},
			"Conditions.member.1.Field": {"path-pattern"},
			"Conditions.member.1.PathPatternConfig.Values.member.1": {"/p" + prio},
			"Actions.member.1.Type":                                 {"forward"},
			"Actions.member.1.TargetGroupArn":                       {tgArn},
		})
		require.Equal(t, http.StatusOK, r.Code)
		var rr struct {
			Result struct {
				Rules struct {
					Members []struct {
						RuleArn string `xml:"RuleArn"`
					} `xml:"member"`
				} `xml:"Rules"`
			} `xml:"CreateRuleResult"`
		}
		require.NoError(t, xml.Unmarshal(r.Body.Bytes(), &rr))
		ruleArns = append(ruleArns, rr.Result.Rules.Members[0].RuleArn)
	}

	// Swap priorities
	rec := doELBv2(t, h, url.Values{
		"Action":                           {"SetRulePriorities"},
		"Version":                          {"2015-12-01"},
		"RulePriorities.member.1.RuleArn":  {ruleArns[0]},
		"RulePriorities.member.1.Priority": {"2"},
		"RulePriorities.member.2.RuleArn":  {ruleArns[1]},
		"RulePriorities.member.2.Priority": {"1"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestCreateRule_InvalidPriority_Zero(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "rule-prio-zero-lb")
	tgArn := b1CreateTG(t, h, "rule-prio-zero-tg")
	lArn := b1CreateListener(t, h, lbArn, tgArn)

	rec := doELBv2(t, h, url.Values{
		"Action":                    {"CreateRule"},
		"Version":                   {"2015-12-01"},
		"ListenerArn":               {lArn},
		"Priority":                  {"0"},
		"Conditions.member.1.Field": {"path-pattern"},
		"Conditions.member.1.PathPatternConfig.Values.member.1": {"/foo"},
		"Actions.member.1.Type":                                 {"forward"},
		"Actions.member.1.TargetGroupArn":                       {tgArn},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestSetRulePriorities_CollisionWithExisting verifies that
// SetRulePriorities rejects a priority that collides with a non-batch rule.
func TestSetRulePriorities_CollisionWithExisting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		newPrio1     string // new priority for rule1 (batch)
		newPrio2     string // new priority for rule2 (batch), empty = omit rule2 from batch
		existingPrio string // priority of an additional non-batch rule
		wantCode     int
	}{
		{
			name:         "collision_with_existing_rule_rejected",
			newPrio1:     "20",
			existingPrio: "20",
			wantCode:     http.StatusBadRequest,
		},
		{
			name:         "no_collision_accepted",
			newPrio1:     "30",
			existingPrio: "20",
			wantCode:     http.StatusOK,
		},
		{
			name:     "swap_within_batch_accepted",
			newPrio1: "50",
			newPrio2: "10",
			wantCode: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newParityBHandler()
			lbArn := pbCreateLB(t, h, "srp-lb")
			tgArn := pbCreateTG(t, h, "srp-tg")
			lArn := pbCreateListener(t, h, lbArn, tgArn)

			// rule1 at 10, rule2 at 50.
			rule1Arn := pbCreateRule(t, h, lArn, tgArn, "10")
			rule2Arn := pbCreateRule(t, h, lArn, tgArn, "50")

			if tc.existingPrio != "" {
				pbCreateRule(t, h, lArn, tgArn, tc.existingPrio)
			}

			vals := url.Values{
				"Action":                           {"SetRulePriorities"},
				"Version":                          {"2015-12-01"},
				"RulePriorities.member.1.RuleArn":  {rule1Arn},
				"RulePriorities.member.1.Priority": {tc.newPrio1},
			}

			if tc.newPrio2 != "" {
				vals.Set("RulePriorities.member.2.RuleArn", rule2Arn)
				vals.Set("RulePriorities.member.2.Priority", tc.newPrio2)
			}

			rec := doELBv2(t, h, vals)
			assert.Equal(t, tc.wantCode, rec.Code, rec.Body.String())
		})
	}
}
