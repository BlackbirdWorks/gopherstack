package waf_test

// backend_test.go covers InMemoryBackend behavior that handler_audit*_test.go
// and parity_*_test.go don't exercise:
//
//   - ChangeToken enforcement: Create/Update/Delete must reject a token that
//     was never returned by GetChangeToken, or that has already been
//     consumed by an earlier mutation (WAFStaleDataException).
//   - ReferencedItem enforcement: Delete must reject removing a Rule,
//     RateBasedRule, RuleGroup, or match set that is still referenced by a
//     WebACL, RuleGroup, or Rule/RateBasedRule predicate
//     (WAFReferencedItemException), and must succeed once the reference is
//     removed.
//
// Both were previously modeled as no-ops: every backend Create/Update/Delete
// method accepted a changeToken parameter it silently discarded, and the
// unused ErrReferencedItem sentinel was wired into Handler.handleError but
// never returned by any backend method.

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/waf"
)

func errType(t *testing.T, body []byte) string {
	t.Helper()

	var resp map[string]any
	require.NoError(t, json.Unmarshal(body, &resp))

	typ, _ := resp["__type"].(string)

	return typ
}

// --- §1 ChangeToken enforcement ---

func TestChangeToken_NeverIssuedTokenRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   map[string]any
		name   string
		action string
	}{
		{
			name:   "CreateWebACL",
			action: "CreateWebACL",
			body: map[string]any{
				"ChangeToken":   "never-issued",
				"Name":          "acl",
				"MetricName":    "aclMetric",
				"DefaultAction": map[string]any{"Type": "ALLOW"},
			},
		},
		{
			name:   "CreateRule",
			action: "CreateRule",
			body: map[string]any{
				"ChangeToken": "never-issued",
				"Name":        "rule",
				"MetricName":  "ruleMetric",
			},
		},
		{
			name:   "CreateIPSet",
			action: "CreateIPSet",
			body:   map[string]any{"ChangeToken": "never-issued", "Name": "ipset"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newWAFHandler(t)
			rec := wafDo(t, h, tc.action, tc.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
			assert.Equal(t, "WAFStaleDataException", errType(t, rec.Body.Bytes()))
		})
	}
}

func TestChangeToken_AlreadyConsumedTokenRejected(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	token := wafGetToken(t, h)

	// Consume the token with a first, valid mutation.
	rec := wafDo(t, h, "CreateRule", map[string]any{
		"ChangeToken": token,
		"Name":        "first-rule",
		"MetricName":  "firstRuleMetric",
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	statusRec := wafDo(t, h, "GetChangeTokenStatus", map[string]any{"ChangeToken": token})
	require.Equal(t, http.StatusOK, statusRec.Code)

	var statusResp map[string]any
	require.NoError(t, json.Unmarshal(statusRec.Body.Bytes(), &statusResp))
	require.Equal(t, "INSYNC", statusResp["ChangeTokenStatus"], "token must be consumed before reuse attempt")

	// Reusing the same, now-INSYNC token for a second mutation must fail.
	rec = wafDo(t, h, "CreateRule", map[string]any{
		"ChangeToken": token,
		"Name":        "second-rule",
		"MetricName":  "secondRuleMetric",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	assert.Equal(t, "WAFStaleDataException", errType(t, rec.Body.Bytes()))

	rec = wafDo(t, h, "ListRules", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Len(t, listResp["Rules"], 1, "the rejected second CreateRule must not have mutated state")
}

func TestChangeToken_FreshTokenAcceptedAfterRejection(t *testing.T) {
	t.Parallel()

	// A stale-token rejection must not consume any other token: the caller
	// can immediately retry with a freshly-issued one.
	h := newWAFHandler(t)

	rec := wafDo(t, h, "CreateRule", map[string]any{
		"ChangeToken": "bogus",
		"Name":        "rule",
		"MetricName":  "ruleMetric",
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	token := wafGetToken(t, h)
	rec = wafDo(t, h, "CreateRule", map[string]any{
		"ChangeToken": token,
		"Name":        "rule",
		"MetricName":  "ruleMetric",
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
}

// --- §2 ReferencedItem enforcement ---

func TestReferencedItem_RuleBlockedByWebACL(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	aclID := wafCreateWebACL(t, h, "acl")
	ruleID := wafCreateRule(t, h, "rule")

	token := wafGetToken(t, h)
	rec := wafDo(t, h, "UpdateWebACL", map[string]any{
		"ChangeToken": token,
		"WebACLId":    aclID,
		"Updates": []map[string]any{
			{
				"Action": "INSERT",
				"ActivatedRule": map[string]any{
					"Priority": 1,
					"RuleId":   ruleID,
					"Action":   map[string]any{"Type": "BLOCK"},
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	// Still referenced by the WebACL -> delete must fail.
	token = wafGetToken(t, h)
	rec = wafDo(t, h, "DeleteRule", map[string]any{"ChangeToken": token, "RuleId": ruleID})
	assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	assert.Equal(t, "WAFReferencedItemException", errType(t, rec.Body.Bytes()))
	assert.Equal(t, 1, waf.RuleCount(h.Backend.(*waf.InMemoryBackend)))

	// Remove the reference, then delete succeeds.
	token = wafGetToken(t, h)
	rec = wafDo(t, h, "UpdateWebACL", map[string]any{
		"ChangeToken": token,
		"WebACLId":    aclID,
		"Updates": []map[string]any{
			{
				"Action": "DELETE",
				"ActivatedRule": map[string]any{
					"Priority": 1,
					"RuleId":   ruleID,
					"Action":   map[string]any{"Type": "BLOCK"},
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	token = wafGetToken(t, h)
	rec = wafDo(t, h, "DeleteRule", map[string]any{"ChangeToken": token, "RuleId": ruleID})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Equal(t, 0, waf.RuleCount(h.Backend.(*waf.InMemoryBackend)))
}

func TestReferencedItem_RuleBlockedByRuleGroup(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	ruleID := wafCreateRule(t, h, "rule")
	rgID := wafCreateRuleGroup(t, h, "group")

	token := wafGetToken(t, h)
	rec := wafDo(t, h, "UpdateRuleGroup", map[string]any{
		"ChangeToken": token,
		"RuleGroupId": rgID,
		"Updates": []map[string]any{
			{
				"Action": "INSERT",
				"ActivatedRule": map[string]any{
					"RuleId":   ruleID,
					"Priority": 1,
					"Type":     "REGULAR",
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	token = wafGetToken(t, h)
	rec = wafDo(t, h, "DeleteRule", map[string]any{"ChangeToken": token, "RuleId": ruleID})
	assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	assert.Equal(t, "WAFReferencedItemException", errType(t, rec.Body.Bytes()))
}

func TestReferencedItem_RuleGroupBlockedByWebACL(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	aclID := wafCreateWebACL(t, h, "acl")
	rgID := wafCreateRuleGroup(t, h, "group")

	token := wafGetToken(t, h)
	rec := wafDo(t, h, "UpdateWebACL", map[string]any{
		"ChangeToken": token,
		"WebACLId":    aclID,
		"Updates": []map[string]any{
			{
				"Action": "INSERT",
				"ActivatedRule": map[string]any{
					"Priority": 1,
					"RuleId":   rgID,
					"Type":     "GROUP",
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	token = wafGetToken(t, h)
	rec = wafDo(t, h, "DeleteRuleGroup", map[string]any{"ChangeToken": token, "RuleGroupId": rgID})
	assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	assert.Equal(t, "WAFReferencedItemException", errType(t, rec.Body.Bytes()))
}

func TestReferencedItem_RateBasedRuleBlockedByWebACL(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	aclID := wafCreateWebACL(t, h, "acl")
	rbrID := wafCreateRateBasedRule(t, h, "rbr")

	token := wafGetToken(t, h)
	rec := wafDo(t, h, "UpdateWebACL", map[string]any{
		"ChangeToken": token,
		"WebACLId":    aclID,
		"Updates": []map[string]any{
			{
				"Action": "INSERT",
				"ActivatedRule": map[string]any{
					"Priority": 1,
					"RuleId":   rbrID,
					"Type":     "RATE_BASED",
					"Action":   map[string]any{"Type": "BLOCK"},
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	token = wafGetToken(t, h)
	rec = wafDo(t, h, "DeleteRateBasedRule", map[string]any{"ChangeToken": token, "RuleId": rbrID})
	assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	assert.Equal(t, "WAFReferencedItemException", errType(t, rec.Body.Bytes()))
}

func TestReferencedItem_IPSetBlockedByRulePredicate(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	ruleID := wafCreateRule(t, h, "rule")
	ipSetID := wafCreateIPSet(t, h, "ipset")

	token := wafGetToken(t, h)
	rec := wafDo(t, h, "UpdateRule", map[string]any{
		"ChangeToken": token,
		"RuleId":      ruleID,
		"Updates": []map[string]any{
			{
				"Action": "INSERT",
				"Predicate": map[string]any{
					"Type":    "IPMatch",
					"DataId":  ipSetID,
					"Negated": false,
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	token = wafGetToken(t, h)
	rec = wafDo(t, h, "DeleteIPSet", map[string]any{"ChangeToken": token, "IPSetId": ipSetID})
	assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	assert.Equal(t, "WAFReferencedItemException", errType(t, rec.Body.Bytes()))
	assert.Equal(t, 1, waf.IPSetCount(h.Backend.(*waf.InMemoryBackend)))

	// Remove the predicate, then delete succeeds.
	token = wafGetToken(t, h)
	rec = wafDo(t, h, "UpdateRule", map[string]any{
		"ChangeToken": token,
		"RuleId":      ruleID,
		"Updates": []map[string]any{
			{
				"Action": "DELETE",
				"Predicate": map[string]any{
					"Type":    "IPMatch",
					"DataId":  ipSetID,
					"Negated": false,
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	token = wafGetToken(t, h)
	rec = wafDo(t, h, "DeleteIPSet", map[string]any{"ChangeToken": token, "IPSetId": ipSetID})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
}

func TestReferencedItem_RegexPatternSetBlockedByRegexMatchSet(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	patternSetID := wafCreateRegexPatternSet(t, h, "patterns")
	matchSetID := wafCreateRegexMatchSet(t, h, "matches")

	token := wafGetToken(t, h)
	rec := wafDo(t, h, "UpdateRegexMatchSet", map[string]any{
		"ChangeToken":     token,
		"RegexMatchSetId": matchSetID,
		"Updates": []map[string]any{
			{
				"Action": "INSERT",
				"RegexMatchTuple": map[string]any{
					"FieldToMatch":       map[string]any{"Type": "URI"},
					"TextTransformation": "NONE",
					"RegexPatternSetId":  patternSetID,
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	token = wafGetToken(t, h)
	rec = wafDo(t, h, "DeleteRegexPatternSet", map[string]any{
		"ChangeToken":       token,
		"RegexPatternSetId": patternSetID,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	assert.Equal(t, "WAFReferencedItemException", errType(t, rec.Body.Bytes()))
}
