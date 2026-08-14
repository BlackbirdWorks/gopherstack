package waf_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/waf"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real AWS WAF
// Classic operation, extracted from waf@v1.33.4 serializers.go: each op's
// awsAwsjson11_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String("AWSWAF_20150824.<Op>")
// and always request.Request.Method = "POST" against path "/" -- WAF
// Classic is JSON-RPC 1.1 (services/_PROTOCOLS.md), so unlike a
// REST-family service there is no path template to get wrong: dispatch is
// entirely by this one header. ExtractOperation and the shared
// pkgs/service.HandleTarget both derive the action the same way (split on
// "."), so the class of bug this table can catch is a dispatch-table key
// that doesn't exactly match the real op name (typo, wrong case -- WAF is
// case-sensitive JSON-RPC), not a route-template mismatch.
//
// This table covers all 77 real WAF Classic ops -- confirmed by diffing the
// actual buildOps() dispatch map against this exact list: zero mismatches
// in either direction, no dead or excluded keys. GetSupportedOperations()
// is derived directly from buildOps()'s map keys (handler.go:70-77), so it
// is correct by construction and cannot itself drift from the dispatch
// table -- the only drift risk here is buildOps() vs the SDK, which this
// table checks directly.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("AWSWAF_20150824.` and pulling the
// suffix after the last dot.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"CreateByteMatchSet", "AWSWAF_20150824.CreateByteMatchSet"},
		{"CreateGeoMatchSet", "AWSWAF_20150824.CreateGeoMatchSet"},
		{"CreateIPSet", "AWSWAF_20150824.CreateIPSet"},
		{"CreateRateBasedRule", "AWSWAF_20150824.CreateRateBasedRule"},
		{"CreateRegexMatchSet", "AWSWAF_20150824.CreateRegexMatchSet"},
		{"CreateRegexPatternSet", "AWSWAF_20150824.CreateRegexPatternSet"},
		{"CreateRule", "AWSWAF_20150824.CreateRule"},
		{"CreateRuleGroup", "AWSWAF_20150824.CreateRuleGroup"},
		{"CreateSizeConstraintSet", "AWSWAF_20150824.CreateSizeConstraintSet"},
		{"CreateSqlInjectionMatchSet", "AWSWAF_20150824.CreateSqlInjectionMatchSet"},
		{"CreateWebACL", "AWSWAF_20150824.CreateWebACL"},
		{"CreateWebACLMigrationStack", "AWSWAF_20150824.CreateWebACLMigrationStack"},
		{"CreateXssMatchSet", "AWSWAF_20150824.CreateXssMatchSet"},
		{"DeleteByteMatchSet", "AWSWAF_20150824.DeleteByteMatchSet"},
		{"DeleteGeoMatchSet", "AWSWAF_20150824.DeleteGeoMatchSet"},
		{"DeleteIPSet", "AWSWAF_20150824.DeleteIPSet"},
		{"DeleteLoggingConfiguration", "AWSWAF_20150824.DeleteLoggingConfiguration"},
		{"DeletePermissionPolicy", "AWSWAF_20150824.DeletePermissionPolicy"},
		{"DeleteRateBasedRule", "AWSWAF_20150824.DeleteRateBasedRule"},
		{"DeleteRegexMatchSet", "AWSWAF_20150824.DeleteRegexMatchSet"},
		{"DeleteRegexPatternSet", "AWSWAF_20150824.DeleteRegexPatternSet"},
		{"DeleteRule", "AWSWAF_20150824.DeleteRule"},
		{"DeleteRuleGroup", "AWSWAF_20150824.DeleteRuleGroup"},
		{"DeleteSizeConstraintSet", "AWSWAF_20150824.DeleteSizeConstraintSet"},
		{"DeleteSqlInjectionMatchSet", "AWSWAF_20150824.DeleteSqlInjectionMatchSet"},
		{"DeleteWebACL", "AWSWAF_20150824.DeleteWebACL"},
		{"DeleteXssMatchSet", "AWSWAF_20150824.DeleteXssMatchSet"},
		{"GetByteMatchSet", "AWSWAF_20150824.GetByteMatchSet"},
		{"GetChangeToken", "AWSWAF_20150824.GetChangeToken"},
		{"GetChangeTokenStatus", "AWSWAF_20150824.GetChangeTokenStatus"},
		{"GetGeoMatchSet", "AWSWAF_20150824.GetGeoMatchSet"},
		{"GetIPSet", "AWSWAF_20150824.GetIPSet"},
		{"GetLoggingConfiguration", "AWSWAF_20150824.GetLoggingConfiguration"},
		{"GetPermissionPolicy", "AWSWAF_20150824.GetPermissionPolicy"},
		{"GetRateBasedRule", "AWSWAF_20150824.GetRateBasedRule"},
		{"GetRateBasedRuleManagedKeys", "AWSWAF_20150824.GetRateBasedRuleManagedKeys"},
		{"GetRegexMatchSet", "AWSWAF_20150824.GetRegexMatchSet"},
		{"GetRegexPatternSet", "AWSWAF_20150824.GetRegexPatternSet"},
		{"GetRule", "AWSWAF_20150824.GetRule"},
		{"GetRuleGroup", "AWSWAF_20150824.GetRuleGroup"},
		{"GetSampledRequests", "AWSWAF_20150824.GetSampledRequests"},
		{"GetSizeConstraintSet", "AWSWAF_20150824.GetSizeConstraintSet"},
		{"GetSqlInjectionMatchSet", "AWSWAF_20150824.GetSqlInjectionMatchSet"},
		{"GetWebACL", "AWSWAF_20150824.GetWebACL"},
		{"GetXssMatchSet", "AWSWAF_20150824.GetXssMatchSet"},
		{"ListActivatedRulesInRuleGroup", "AWSWAF_20150824.ListActivatedRulesInRuleGroup"},
		{"ListByteMatchSets", "AWSWAF_20150824.ListByteMatchSets"},
		{"ListGeoMatchSets", "AWSWAF_20150824.ListGeoMatchSets"},
		{"ListIPSets", "AWSWAF_20150824.ListIPSets"},
		{"ListLoggingConfigurations", "AWSWAF_20150824.ListLoggingConfigurations"},
		{"ListRateBasedRules", "AWSWAF_20150824.ListRateBasedRules"},
		{"ListRegexMatchSets", "AWSWAF_20150824.ListRegexMatchSets"},
		{"ListRegexPatternSets", "AWSWAF_20150824.ListRegexPatternSets"},
		{"ListRuleGroups", "AWSWAF_20150824.ListRuleGroups"},
		{"ListRules", "AWSWAF_20150824.ListRules"},
		{"ListSizeConstraintSets", "AWSWAF_20150824.ListSizeConstraintSets"},
		{"ListSqlInjectionMatchSets", "AWSWAF_20150824.ListSqlInjectionMatchSets"},
		{"ListSubscribedRuleGroups", "AWSWAF_20150824.ListSubscribedRuleGroups"},
		{"ListTagsForResource", "AWSWAF_20150824.ListTagsForResource"},
		{"ListWebACLs", "AWSWAF_20150824.ListWebACLs"},
		{"ListXssMatchSets", "AWSWAF_20150824.ListXssMatchSets"},
		{"PutLoggingConfiguration", "AWSWAF_20150824.PutLoggingConfiguration"},
		{"PutPermissionPolicy", "AWSWAF_20150824.PutPermissionPolicy"},
		{"TagResource", "AWSWAF_20150824.TagResource"},
		{"UntagResource", "AWSWAF_20150824.UntagResource"},
		{"UpdateByteMatchSet", "AWSWAF_20150824.UpdateByteMatchSet"},
		{"UpdateGeoMatchSet", "AWSWAF_20150824.UpdateGeoMatchSet"},
		{"UpdateIPSet", "AWSWAF_20150824.UpdateIPSet"},
		{"UpdateRateBasedRule", "AWSWAF_20150824.UpdateRateBasedRule"},
		{"UpdateRegexMatchSet", "AWSWAF_20150824.UpdateRegexMatchSet"},
		{"UpdateRegexPatternSet", "AWSWAF_20150824.UpdateRegexPatternSet"},
		{"UpdateRule", "AWSWAF_20150824.UpdateRule"},
		{"UpdateRuleGroup", "AWSWAF_20150824.UpdateRuleGroup"},
		{"UpdateSizeConstraintSet", "AWSWAF_20150824.UpdateSizeConstraintSet"},
		{"UpdateSqlInjectionMatchSet", "AWSWAF_20150824.UpdateSqlInjectionMatchSet"},
		{"UpdateWebACL", "AWSWAF_20150824.UpdateWebACL"},
		{"UpdateXssMatchSet", "AWSWAF_20150824.UpdateXssMatchSet"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real WAF Classic
// operation's authoritative X-Amz-Target through ExtractOperation and
// Handler(), asserting the header resolves to the right op name and that
// Handler() does not fall through to the dispatch-miss sentinel a
// dispatch-table key mismatch would produce.
//
// WAF's dispatch-miss sentinel is the package's own ErrInvalidParameter
// (handler.go:100), wire-mapped to "WAFInvalidParameterException". Grepped:
// this exact sentinel value is never constructed anywhere else in the
// package (op handlers use awserr.ErrInvalidParameter-wrapped errors
// through other codes, or the distinct ErrNotFound/ErrStaleToken/
// ErrReferencedItem/ErrNonEmptyEntity sentinels), so unlike workmail/
// transfer, asserting on the wire type here is safe.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			backend := waf.NewInMemoryBackend("111122223333", "us-east-1")
			h := waf.NewHandler(backend)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
			req.Header.Set("X-Amz-Target", tc.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, tc.op, got)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "WAFInvalidParameterException",
				"target=%s op=%s: dispatched to the unmatched-route handler", tc.target, tc.op)
		})
	}
}
