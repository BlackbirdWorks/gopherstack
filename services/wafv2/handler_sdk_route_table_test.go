package wafv2_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/wafv2"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real WAFv2
// operation, extracted from wafv2@v1.77.3 serializers.go: each op's
// awsAwsjson11_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String("AWSWAF_20190729.<Op>")
// and always POSTs to "/" -- WAFv2 is JSON-RPC 1.1 (services/_PROTOCOLS.md),
// so unlike a REST-family service there is no path template to get wrong:
// dispatch is entirely by this one header. The target prefix is not
// guessable from the service name or directory -- read directly from
// serializers.go, as instructed. ExtractOperation and Handler() (via
// buildDispatchTable()'s merged map, dispatched through h.dispatch) both
// derive the action the same way (TrimPrefix on "AWSWAF_20190729."), so the
// class of bug this table catches is a dispatch-table key that doesn't
// exactly match the real op name (typo, wrong case -- WAFv2 is
// case-sensitive JSON-RPC), not a route-template mismatch.
//
// This table covers all 59 real WAFv2 ops (wafv2@v1.77.3) -- confirmed by
// diffing both GetSupportedOperations() and the actual buildDispatchTable()
// map (the merge of all thirteen per-family *DispatchOps() builders) against
// this exact list: zero mismatches in either direction, no dead or excluded
// keys.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("AWSWAF_20190729.` and pulling the
// suffix after the last dot.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"AssociateWebACL", "AWSWAF_20190729.AssociateWebACL"},
		{"CheckCapacity", "AWSWAF_20190729.CheckCapacity"},
		{"CreateAPIKey", "AWSWAF_20190729.CreateAPIKey"},
		{"CreateIPSet", "AWSWAF_20190729.CreateIPSet"},
		{"CreateRegexPatternSet", "AWSWAF_20190729.CreateRegexPatternSet"},
		{"CreateRuleGroup", "AWSWAF_20190729.CreateRuleGroup"},
		{"CreateWebACL", "AWSWAF_20190729.CreateWebACL"},
		{"DeleteAPIKey", "AWSWAF_20190729.DeleteAPIKey"},
		{"DeleteFirewallManagerRuleGroups", "AWSWAF_20190729.DeleteFirewallManagerRuleGroups"},
		{"DeleteIPSet", "AWSWAF_20190729.DeleteIPSet"},
		{"DeleteLoggingConfiguration", "AWSWAF_20190729.DeleteLoggingConfiguration"},
		{"DeletePermissionPolicy", "AWSWAF_20190729.DeletePermissionPolicy"},
		{"DeleteRegexPatternSet", "AWSWAF_20190729.DeleteRegexPatternSet"},
		{"DeleteRuleGroup", "AWSWAF_20190729.DeleteRuleGroup"},
		{"DeleteWebACL", "AWSWAF_20190729.DeleteWebACL"},
		{"DescribeAllManagedProducts", "AWSWAF_20190729.DescribeAllManagedProducts"},
		{"DescribeManagedProductsByVendor", "AWSWAF_20190729.DescribeManagedProductsByVendor"},
		{"DescribeManagedRuleGroup", "AWSWAF_20190729.DescribeManagedRuleGroup"},
		{"DisassociateWebACL", "AWSWAF_20190729.DisassociateWebACL"},
		{"GenerateMobileSdkReleaseUrl", "AWSWAF_20190729.GenerateMobileSdkReleaseUrl"},
		{"GetDecryptedAPIKey", "AWSWAF_20190729.GetDecryptedAPIKey"},
		{"GetIPSet", "AWSWAF_20190729.GetIPSet"},
		{"GetLoggingConfiguration", "AWSWAF_20190729.GetLoggingConfiguration"},
		{"GetManagedRuleSet", "AWSWAF_20190729.GetManagedRuleSet"},
		{"GetMobileSdkRelease", "AWSWAF_20190729.GetMobileSdkRelease"},
		{"GetPermissionPolicy", "AWSWAF_20190729.GetPermissionPolicy"},
		{"GetRateBasedStatementManagedKeys", "AWSWAF_20190729.GetRateBasedStatementManagedKeys"},
		{"GetRegexPatternSet", "AWSWAF_20190729.GetRegexPatternSet"},
		{"GetRevenueStatistics", "AWSWAF_20190729.GetRevenueStatistics"},
		{"GetRevenueStatisticsSummary", "AWSWAF_20190729.GetRevenueStatisticsSummary"},
		{"GetRevenueStatisticsTimeSeries", "AWSWAF_20190729.GetRevenueStatisticsTimeSeries"},
		{"GetRuleGroup", "AWSWAF_20190729.GetRuleGroup"},
		{"GetSampledRequests", "AWSWAF_20190729.GetSampledRequests"},
		{"GetTopPathStatisticsByTraffic", "AWSWAF_20190729.GetTopPathStatisticsByTraffic"},
		{"GetWebACL", "AWSWAF_20190729.GetWebACL"},
		{"GetWebACLForResource", "AWSWAF_20190729.GetWebACLForResource"},
		{"ListAPIKeys", "AWSWAF_20190729.ListAPIKeys"},
		{"ListAvailableManagedRuleGroups", "AWSWAF_20190729.ListAvailableManagedRuleGroups"},
		{"ListAvailableManagedRuleGroupVersions", "AWSWAF_20190729.ListAvailableManagedRuleGroupVersions"},
		{"ListIPSets", "AWSWAF_20190729.ListIPSets"},
		{"ListLoggingConfigurations", "AWSWAF_20190729.ListLoggingConfigurations"},
		{"ListManagedRuleSets", "AWSWAF_20190729.ListManagedRuleSets"},
		{"ListMobileSdkReleases", "AWSWAF_20190729.ListMobileSdkReleases"},
		{"ListRegexPatternSets", "AWSWAF_20190729.ListRegexPatternSets"},
		{"ListResourcesForWebACL", "AWSWAF_20190729.ListResourcesForWebACL"},
		{"ListRuleGroups", "AWSWAF_20190729.ListRuleGroups"},
		{"ListSettlementRecords", "AWSWAF_20190729.ListSettlementRecords"},
		{"ListTagsForResource", "AWSWAF_20190729.ListTagsForResource"},
		{"ListWebACLs", "AWSWAF_20190729.ListWebACLs"},
		{"PutLoggingConfiguration", "AWSWAF_20190729.PutLoggingConfiguration"},
		{"PutManagedRuleSetVersions", "AWSWAF_20190729.PutManagedRuleSetVersions"},
		{"PutPermissionPolicy", "AWSWAF_20190729.PutPermissionPolicy"},
		{"TagResource", "AWSWAF_20190729.TagResource"},
		{"UntagResource", "AWSWAF_20190729.UntagResource"},
		{"UpdateIPSet", "AWSWAF_20190729.UpdateIPSet"},
		{"UpdateManagedRuleSetVersionExpiryDate", "AWSWAF_20190729.UpdateManagedRuleSetVersionExpiryDate"},
		{"UpdateRegexPatternSet", "AWSWAF_20190729.UpdateRegexPatternSet"},
		{"UpdateRuleGroup", "AWSWAF_20190729.UpdateRuleGroup"},
		{"UpdateWebACL", "AWSWAF_20190729.UpdateWebACL"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real WAFv2 operation's
// authoritative X-Amz-Target through ExtractOperation and Handler(),
// asserting the header resolves to the right op name and that Handler() does
// not fall through to the "WAFInvalidOperationException" sentinel
// (errUnknownAction, handler.go's dispatch() single production call site)
// that a dispatch-table key mismatch would produce.
// "WAFInvalidOperationException" is not reused by any other entry in
// handleError's switch (grepped: each of WAFNonexistentItemException,
// WAFOptimisticLockException, WAFAssociatedItemException,
// WAFLimitsExceededException, WAFTagOperationException,
// WAFUnavailableEntityException, WAFDuplicateItemException,
// WAFConfigurationWarningException, and WAFInvalidParameterException maps
// from its own distinct sentinel), so asserting on the wire type is safe
// here, unlike workmail/transfer, where the dispatch-miss sentinel shares
// its wire type with ordinary validation errors.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			b := wafv2.NewInMemoryBackend("123456789012", "us-east-1")
			h := wafv2.NewHandler(b)

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
			req.Header.Set("X-Amz-Target", tc.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, tc.op, got)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "WAFInvalidOperationException",
				"target=%s op=%s: dispatched to the unmatched-route handler", tc.target, tc.op)
		})
	}
}
