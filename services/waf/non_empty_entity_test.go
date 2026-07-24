package waf_test

// non_empty_entity_test.go covers WAFNonEmptyEntityException enforcement:
// real AWS WAF Classic rejects deleting a container object (WebACL, Rule,
// RuleGroup, RateBasedRule, or any match set) while it still holds child
// entities -- a WebACL that still has Rules, a Rule that still has
// Predicates, a ByteMatchSet that still has ByteMatchTuples, and so on.
// Each test creates the container, populates it with exactly one child,
// asserts the blocked delete, then removes the child and asserts the
// delete succeeds.

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/waf"
)

func TestNonEmptyEntity_WebACLBlockedByRules(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	aclID := wafCreateWebACL(t, h, "acl")
	ruleID := wafCreateRule(t, h, "rule")

	activatedRule := map[string]any{
		"Priority": 1,
		"RuleId":   ruleID,
		"Action":   map[string]any{"Type": "BLOCK"},
	}

	token := wafGetToken(t, h)
	rec := wafDo(t, h, "UpdateWebACL", map[string]any{
		"ChangeToken": token,
		"WebACLId":    aclID,
		"Updates":     []map[string]any{{"Action": "INSERT", "ActivatedRule": activatedRule}},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	token = wafGetToken(t, h)
	rec = wafDo(t, h, "DeleteWebACL", map[string]any{"ChangeToken": token, "WebACLId": aclID})
	assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	assert.Equal(t, "WAFNonEmptyEntityException", errType(t, rec.Body.Bytes()))
	assert.Equal(t, 1, waf.WebACLCount(h.Backend.(*waf.InMemoryBackend)))

	token = wafGetToken(t, h)
	rec = wafDo(t, h, "UpdateWebACL", map[string]any{
		"ChangeToken": token,
		"WebACLId":    aclID,
		"Updates":     []map[string]any{{"Action": "DELETE", "ActivatedRule": activatedRule}},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	token = wafGetToken(t, h)
	rec = wafDo(t, h, "DeleteWebACL", map[string]any{"ChangeToken": token, "WebACLId": aclID})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Equal(t, 0, waf.WebACLCount(h.Backend.(*waf.InMemoryBackend)))
}

func TestNonEmptyEntity_RuleBlockedByPredicates(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	ruleID := wafCreateRule(t, h, "rule")

	predicate := map[string]any{"DataId": "some-ip-set-id", "Type": "IPMatch", "Negated": false}

	token := wafGetToken(t, h)
	rec := wafDo(t, h, "UpdateRule", map[string]any{
		"ChangeToken": token,
		"RuleId":      ruleID,
		"Updates":     []map[string]any{{"Action": "INSERT", "Predicate": predicate}},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	token = wafGetToken(t, h)
	rec = wafDo(t, h, "DeleteRule", map[string]any{"ChangeToken": token, "RuleId": ruleID})
	assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	assert.Equal(t, "WAFNonEmptyEntityException", errType(t, rec.Body.Bytes()))
	assert.Equal(t, 1, waf.RuleCount(h.Backend.(*waf.InMemoryBackend)))

	token = wafGetToken(t, h)
	rec = wafDo(t, h, "UpdateRule", map[string]any{
		"ChangeToken": token,
		"RuleId":      ruleID,
		"Updates":     []map[string]any{{"Action": "DELETE", "Predicate": predicate}},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	token = wafGetToken(t, h)
	rec = wafDo(t, h, "DeleteRule", map[string]any{"ChangeToken": token, "RuleId": ruleID})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Equal(t, 0, waf.RuleCount(h.Backend.(*waf.InMemoryBackend)))
}

func TestNonEmptyEntity_RuleGroupBlockedByActivatedRules(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	rgID := wafCreateRuleGroup(t, h, "group")
	ruleID := wafCreateRule(t, h, "rule")

	activatedRule := map[string]any{"RuleId": ruleID, "Priority": 1, "Type": "REGULAR"}

	token := wafGetToken(t, h)
	rec := wafDo(t, h, "UpdateRuleGroup", map[string]any{
		"ChangeToken": token,
		"RuleGroupId": rgID,
		"Updates":     []map[string]any{{"Action": "INSERT", "ActivatedRule": activatedRule}},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	token = wafGetToken(t, h)
	rec = wafDo(t, h, "DeleteRuleGroup", map[string]any{"ChangeToken": token, "RuleGroupId": rgID})
	assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	assert.Equal(t, "WAFNonEmptyEntityException", errType(t, rec.Body.Bytes()))
	assert.Equal(t, 1, waf.RuleGroupCount(h.Backend.(*waf.InMemoryBackend)))

	token = wafGetToken(t, h)
	rec = wafDo(t, h, "UpdateRuleGroup", map[string]any{
		"ChangeToken": token,
		"RuleGroupId": rgID,
		"Updates":     []map[string]any{{"Action": "DELETE", "ActivatedRule": activatedRule}},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	token = wafGetToken(t, h)
	rec = wafDo(t, h, "DeleteRuleGroup", map[string]any{"ChangeToken": token, "RuleGroupId": rgID})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Equal(t, 0, waf.RuleGroupCount(h.Backend.(*waf.InMemoryBackend)))
}

func TestNonEmptyEntity_RateBasedRuleBlockedByMatchPredicates(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	rbrID := wafCreateRateBasedRule(t, h, "rbr")

	predicate := map[string]any{"DataId": "some-ip-set-id", "Type": "IPMatch", "Negated": false}

	token := wafGetToken(t, h)
	rec := wafDo(t, h, "UpdateRateBasedRule", map[string]any{
		"ChangeToken": token,
		"RuleId":      rbrID,
		"RateLimit":   int64(0),
		"Updates":     []map[string]any{{"Action": "INSERT", "Predicate": predicate}},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	token = wafGetToken(t, h)
	rec = wafDo(t, h, "DeleteRateBasedRule", map[string]any{"ChangeToken": token, "RuleId": rbrID})
	assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	assert.Equal(t, "WAFNonEmptyEntityException", errType(t, rec.Body.Bytes()))
	assert.Equal(t, 1, waf.RateBasedRuleCount(h.Backend.(*waf.InMemoryBackend)))

	token = wafGetToken(t, h)
	rec = wafDo(t, h, "UpdateRateBasedRule", map[string]any{
		"ChangeToken": token,
		"RuleId":      rbrID,
		"RateLimit":   int64(0),
		"Updates":     []map[string]any{{"Action": "DELETE", "Predicate": predicate}},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	token = wafGetToken(t, h)
	rec = wafDo(t, h, "DeleteRateBasedRule", map[string]any{"ChangeToken": token, "RuleId": rbrID})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Equal(t, 0, waf.RateBasedRuleCount(h.Backend.(*waf.InMemoryBackend)))
}

func TestNonEmptyEntity_IPSetBlockedByDescriptors(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	ipSetID := wafCreateIPSet(t, h, "ipset")

	descriptor := map[string]any{"Type": "IPV4", "Value": "10.0.0.0/8"}

	token := wafGetToken(t, h)
	rec := wafDo(t, h, "UpdateIPSet", map[string]any{
		"ChangeToken": token,
		"IPSetId":     ipSetID,
		"Updates":     []map[string]any{{"Action": "INSERT", "IPSetDescriptor": descriptor}},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	token = wafGetToken(t, h)
	rec = wafDo(t, h, "DeleteIPSet", map[string]any{"ChangeToken": token, "IPSetId": ipSetID})
	assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	assert.Equal(t, "WAFNonEmptyEntityException", errType(t, rec.Body.Bytes()))
	assert.Equal(t, 1, waf.IPSetCount(h.Backend.(*waf.InMemoryBackend)))

	token = wafGetToken(t, h)
	rec = wafDo(t, h, "UpdateIPSet", map[string]any{
		"ChangeToken": token,
		"IPSetId":     ipSetID,
		"Updates":     []map[string]any{{"Action": "DELETE", "IPSetDescriptor": descriptor}},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	token = wafGetToken(t, h)
	rec = wafDo(t, h, "DeleteIPSet", map[string]any{"ChangeToken": token, "IPSetId": ipSetID})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Equal(t, 0, waf.IPSetCount(h.Backend.(*waf.InMemoryBackend)))
}

func TestNonEmptyEntity_RegexPatternSetBlockedByPatternStrings(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	patternSetID := wafCreateRegexPatternSet(t, h, "patterns")

	token := wafGetToken(t, h)
	rec := wafDo(t, h, "UpdateRegexPatternSet", map[string]any{
		"ChangeToken":       token,
		"RegexPatternSetId": patternSetID,
		"Updates":           []map[string]any{{"Action": "INSERT", "RegexPatternString": "(?i)select"}},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	token = wafGetToken(t, h)
	rec = wafDo(t, h, "DeleteRegexPatternSet", map[string]any{
		"ChangeToken": token, "RegexPatternSetId": patternSetID,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	assert.Equal(t, "WAFNonEmptyEntityException", errType(t, rec.Body.Bytes()))
	assert.Equal(t, 1, waf.RegexPatternSetCount(h.Backend.(*waf.InMemoryBackend)))

	token = wafGetToken(t, h)
	rec = wafDo(t, h, "UpdateRegexPatternSet", map[string]any{
		"ChangeToken":       token,
		"RegexPatternSetId": patternSetID,
		"Updates":           []map[string]any{{"Action": "DELETE", "RegexPatternString": "(?i)select"}},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	token = wafGetToken(t, h)
	rec = wafDo(t, h, "DeleteRegexPatternSet", map[string]any{
		"ChangeToken": token, "RegexPatternSetId": patternSetID,
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Equal(t, 0, waf.RegexPatternSetCount(h.Backend.(*waf.InMemoryBackend)))
}

func TestNonEmptyEntity_RegexMatchSetBlockedByTuples(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	matchSetID := wafCreateRegexMatchSet(t, h, "matches")

	tuple := map[string]any{
		"FieldToMatch":       map[string]any{"Type": "URI"},
		"TextTransformation": "NONE",
		"RegexPatternSetId":  "some-pattern-set-id",
	}

	token := wafGetToken(t, h)
	rec := wafDo(t, h, "UpdateRegexMatchSet", map[string]any{
		"ChangeToken":     token,
		"RegexMatchSetId": matchSetID,
		"Updates":         []map[string]any{{"Action": "INSERT", "RegexMatchTuple": tuple}},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	token = wafGetToken(t, h)
	rec = wafDo(t, h, "DeleteRegexMatchSet", map[string]any{"ChangeToken": token, "RegexMatchSetId": matchSetID})
	assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	assert.Equal(t, "WAFNonEmptyEntityException", errType(t, rec.Body.Bytes()))
	assert.Equal(t, 1, waf.RegexMatchSetCount(h.Backend.(*waf.InMemoryBackend)))

	token = wafGetToken(t, h)
	rec = wafDo(t, h, "UpdateRegexMatchSet", map[string]any{
		"ChangeToken":     token,
		"RegexMatchSetId": matchSetID,
		"Updates":         []map[string]any{{"Action": "DELETE", "RegexMatchTuple": tuple}},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	token = wafGetToken(t, h)
	rec = wafDo(t, h, "DeleteRegexMatchSet", map[string]any{"ChangeToken": token, "RegexMatchSetId": matchSetID})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Equal(t, 0, waf.RegexMatchSetCount(h.Backend.(*waf.InMemoryBackend)))
}

func TestNonEmptyEntity_ByteMatchSetBlockedByTuples(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	setID := wafCreateNamedSet(t, h, "CreateByteMatchSet", "ByteMatchSet", "ByteMatchSetId", "bms")

	tuple := map[string]any{
		"FieldToMatch":         map[string]any{"Type": "URI"},
		"TargetString":         "/admin",
		"PositionalConstraint": "STARTS_WITH",
		"TextTransformation":   "NONE",
	}

	token := wafGetToken(t, h)
	rec := wafDo(t, h, "UpdateByteMatchSet", map[string]any{
		"ChangeToken":    token,
		"ByteMatchSetId": setID,
		"Updates":        []map[string]any{{"Action": "INSERT", "ByteMatchTuple": tuple}},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	token = wafGetToken(t, h)
	rec = wafDo(t, h, "DeleteByteMatchSet", map[string]any{"ChangeToken": token, "ByteMatchSetId": setID})
	assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	assert.Equal(t, "WAFNonEmptyEntityException", errType(t, rec.Body.Bytes()))
	assert.Equal(t, 1, waf.ByteMatchSetCount(h.Backend.(*waf.InMemoryBackend)))

	token = wafGetToken(t, h)
	rec = wafDo(t, h, "UpdateByteMatchSet", map[string]any{
		"ChangeToken":    token,
		"ByteMatchSetId": setID,
		"Updates":        []map[string]any{{"Action": "DELETE", "ByteMatchTuple": tuple}},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	token = wafGetToken(t, h)
	rec = wafDo(t, h, "DeleteByteMatchSet", map[string]any{"ChangeToken": token, "ByteMatchSetId": setID})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Equal(t, 0, waf.ByteMatchSetCount(h.Backend.(*waf.InMemoryBackend)))
}

func TestNonEmptyEntity_SizeConstraintSetBlockedByConstraints(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	setID := wafCreateNamedSet(t, h, "CreateSizeConstraintSet", "SizeConstraintSet", "SizeConstraintSetId", "scs")

	constraint := map[string]any{
		"FieldToMatch":       map[string]any{"Type": "BODY"},
		"TextTransformation": "NONE",
		"ComparisonOperator": "GT",
		"Size":               8192,
	}

	token := wafGetToken(t, h)
	rec := wafDo(t, h, "UpdateSizeConstraintSet", map[string]any{
		"ChangeToken":         token,
		"SizeConstraintSetId": setID,
		"Updates":             []map[string]any{{"Action": "INSERT", "SizeConstraint": constraint}},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	token = wafGetToken(t, h)
	rec = wafDo(t, h, "DeleteSizeConstraintSet", map[string]any{
		"ChangeToken": token, "SizeConstraintSetId": setID,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	assert.Equal(t, "WAFNonEmptyEntityException", errType(t, rec.Body.Bytes()))
	assert.Equal(t, 1, waf.SizeConstraintSetCount(h.Backend.(*waf.InMemoryBackend)))

	token = wafGetToken(t, h)
	rec = wafDo(t, h, "UpdateSizeConstraintSet", map[string]any{
		"ChangeToken":         token,
		"SizeConstraintSetId": setID,
		"Updates":             []map[string]any{{"Action": "DELETE", "SizeConstraint": constraint}},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	token = wafGetToken(t, h)
	rec = wafDo(t, h, "DeleteSizeConstraintSet", map[string]any{
		"ChangeToken": token, "SizeConstraintSetId": setID,
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Equal(t, 0, waf.SizeConstraintSetCount(h.Backend.(*waf.InMemoryBackend)))
}

func TestNonEmptyEntity_SqlInjectionMatchSetBlockedByTuples(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	setID := wafCreateNamedSet(
		t, h, "CreateSqlInjectionMatchSet", "SqlInjectionMatchSet", "SqlInjectionMatchSetId", "sqli",
	)

	tuple := map[string]any{
		"FieldToMatch":       map[string]any{"Type": "QUERY_STRING"},
		"TextTransformation": "URL_DECODE",
	}

	token := wafGetToken(t, h)
	rec := wafDo(t, h, "UpdateSqlInjectionMatchSet", map[string]any{
		"ChangeToken":            token,
		"SqlInjectionMatchSetId": setID,
		"Updates":                []map[string]any{{"Action": "INSERT", "SqlInjectionMatchTuple": tuple}},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	token = wafGetToken(t, h)
	rec = wafDo(t, h, "DeleteSqlInjectionMatchSet", map[string]any{
		"ChangeToken": token, "SqlInjectionMatchSetId": setID,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	assert.Equal(t, "WAFNonEmptyEntityException", errType(t, rec.Body.Bytes()))
	assert.Equal(t, 1, waf.SqlInjectionMatchSetCount(h.Backend.(*waf.InMemoryBackend)))

	token = wafGetToken(t, h)
	rec = wafDo(t, h, "UpdateSqlInjectionMatchSet", map[string]any{
		"ChangeToken":            token,
		"SqlInjectionMatchSetId": setID,
		"Updates":                []map[string]any{{"Action": "DELETE", "SqlInjectionMatchTuple": tuple}},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	token = wafGetToken(t, h)
	rec = wafDo(t, h, "DeleteSqlInjectionMatchSet", map[string]any{
		"ChangeToken": token, "SqlInjectionMatchSetId": setID,
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Equal(t, 0, waf.SqlInjectionMatchSetCount(h.Backend.(*waf.InMemoryBackend)))
}

func TestNonEmptyEntity_XssMatchSetBlockedByTuples(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	setID := wafCreateNamedSet(t, h, "CreateXssMatchSet", "XssMatchSet", "XssMatchSetId", "xss")

	tuple := map[string]any{
		"FieldToMatch":       map[string]any{"Type": "BODY"},
		"TextTransformation": "HTML_ENTITY_DECODE",
	}

	token := wafGetToken(t, h)
	rec := wafDo(t, h, "UpdateXssMatchSet", map[string]any{
		"ChangeToken":   token,
		"XssMatchSetId": setID,
		"Updates":       []map[string]any{{"Action": "INSERT", "XssMatchTuple": tuple}},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	token = wafGetToken(t, h)
	rec = wafDo(t, h, "DeleteXssMatchSet", map[string]any{"ChangeToken": token, "XssMatchSetId": setID})
	assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	assert.Equal(t, "WAFNonEmptyEntityException", errType(t, rec.Body.Bytes()))
	assert.Equal(t, 1, waf.XssMatchSetCount(h.Backend.(*waf.InMemoryBackend)))

	token = wafGetToken(t, h)
	rec = wafDo(t, h, "UpdateXssMatchSet", map[string]any{
		"ChangeToken":   token,
		"XssMatchSetId": setID,
		"Updates":       []map[string]any{{"Action": "DELETE", "XssMatchTuple": tuple}},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	token = wafGetToken(t, h)
	rec = wafDo(t, h, "DeleteXssMatchSet", map[string]any{"ChangeToken": token, "XssMatchSetId": setID})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Equal(t, 0, waf.XssMatchSetCount(h.Backend.(*waf.InMemoryBackend)))
}

func TestNonEmptyEntity_GeoMatchSetBlockedByConstraints(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	setID := wafCreateNamedSet(t, h, "CreateGeoMatchSet", "GeoMatchSet", "GeoMatchSetId", "geo")

	constraint := map[string]any{"Type": "Country", "Value": "CN"}

	token := wafGetToken(t, h)
	rec := wafDo(t, h, "UpdateGeoMatchSet", map[string]any{
		"ChangeToken":   token,
		"GeoMatchSetId": setID,
		"Updates":       []map[string]any{{"Action": "INSERT", "GeoMatchConstraint": constraint}},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	token = wafGetToken(t, h)
	rec = wafDo(t, h, "DeleteGeoMatchSet", map[string]any{"ChangeToken": token, "GeoMatchSetId": setID})
	assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	assert.Equal(t, "WAFNonEmptyEntityException", errType(t, rec.Body.Bytes()))
	assert.Equal(t, 1, waf.GeoMatchSetCount(h.Backend.(*waf.InMemoryBackend)))

	token = wafGetToken(t, h)
	rec = wafDo(t, h, "UpdateGeoMatchSet", map[string]any{
		"ChangeToken":   token,
		"GeoMatchSetId": setID,
		"Updates":       []map[string]any{{"Action": "DELETE", "GeoMatchConstraint": constraint}},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	token = wafGetToken(t, h)
	rec = wafDo(t, h, "DeleteGeoMatchSet", map[string]any{"ChangeToken": token, "GeoMatchSetId": setID})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Equal(t, 0, waf.GeoMatchSetCount(h.Backend.(*waf.InMemoryBackend)))
}

// wafCreateNamedSet issues a Create<createOp> request with only a Name (the
// shape shared by ByteMatchSet, SizeConstraintSet, SqlInjectionMatchSet,
// XssMatchSet, and GeoMatchSet) and extracts the new resource's ID.
func wafCreateNamedSet(
	t *testing.T, h *waf.Handler, createOp, respKey, idKey, name string,
) string {
	t.Helper()

	token := wafGetToken(t, h)
	rec := wafDo(t, h, createOp, map[string]any{"ChangeToken": token, "Name": name})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	m, ok := resp[respKey].(map[string]any)
	require.True(t, ok)
	id, ok := m[idKey].(string)
	require.True(t, ok)
	require.NotEmpty(t, id)

	return id
}
