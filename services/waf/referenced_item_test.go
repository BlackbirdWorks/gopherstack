package waf_test

// referenced_item_test.go covers WAFReferencedItemException enforcement:
// Delete must reject removing a Rule, RateBasedRule, RuleGroup, or match set
// that is still referenced by a WebACL, RuleGroup, or Rule/RateBasedRule
// predicate, and must succeed once the reference is removed.

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/waf"
)

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
