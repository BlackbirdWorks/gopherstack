package shield_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/shield"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real Shield
// Advanced operation, extracted from shield@v1.37.4 serializers.go: each
// op's awsAwsjson11_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String("AWSShield_20160616.<Op>")
// and always POSTs to "/" -- Shield is JSON-RPC 1.1 (services/_PROTOCOLS.md),
// so unlike a REST-family service there is no path template to get wrong:
// dispatch is entirely by this one header. The target prefix is not
// guessable from the service name or directory -- read directly from
// serializers.go, as instructed. ExtractOperation and Handler() (via
// h.dispatch's four-deep helper chain -- dispatchSubscriptionAndProtectionOps,
// dispatchTagOps, dispatchDRTAndEngagementOps,
// dispatchProtectionGroupAndAttackOps, each returning an (result, ok, err)
// tuple so the caller can fall through to the next helper on ok=false, the
// same chained-switch idiom found in ses/docdb/neptune) both derive the
// action the same way (TrimPrefix on "AWSShield_20160616."), so the class of
// bug this table catches is a dispatch-table key that doesn't exactly match
// the real op name (typo, wrong case -- Shield is case-sensitive JSON-RPC),
// not a route-template mismatch.
//
// This table covers all 36 real Shield ops (shield@v1.37.4) -- confirmed by
// diffing both GetSupportedOperations() (a hand-written literal list) and
// the actual four-deep dispatch chain against this exact list: zero
// mismatches in either direction. The two diffs are genuinely independent --
// GetSupportedOperations is a separately maintained literal, not built by
// ranging over the dispatch chain.
//
// EXCLUDED: dispatchProtectionGroupAndAttackOps also switches on
// "__SimulateAttack", a gopherstack-internal test-only hook (leading double
// underscore, no corresponding X-Amz-Target in the real SDK) -- not a real
// Shield operation, so it is excluded from this table rather than tabled as
// if it were a real route.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("AWSShield_20160616.` and pulling the
// suffix after the last dot.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"AssociateDRTLogBucket", "AWSShield_20160616.AssociateDRTLogBucket"},
		{"AssociateDRTRole", "AWSShield_20160616.AssociateDRTRole"},
		{"AssociateHealthCheck", "AWSShield_20160616.AssociateHealthCheck"},
		{"AssociateProactiveEngagementDetails", "AWSShield_20160616.AssociateProactiveEngagementDetails"},
		{"CreateProtection", "AWSShield_20160616.CreateProtection"},
		{"CreateProtectionGroup", "AWSShield_20160616.CreateProtectionGroup"},
		{"CreateSubscription", "AWSShield_20160616.CreateSubscription"},
		{"DeleteProtection", "AWSShield_20160616.DeleteProtection"},
		{"DeleteProtectionGroup", "AWSShield_20160616.DeleteProtectionGroup"},
		{"DeleteSubscription", "AWSShield_20160616.DeleteSubscription"},
		{"DescribeAttack", "AWSShield_20160616.DescribeAttack"},
		{"DescribeAttackStatistics", "AWSShield_20160616.DescribeAttackStatistics"},
		{"DescribeDRTAccess", "AWSShield_20160616.DescribeDRTAccess"},
		{"DescribeEmergencyContactSettings", "AWSShield_20160616.DescribeEmergencyContactSettings"},
		{"DescribeProtection", "AWSShield_20160616.DescribeProtection"},
		{"DescribeProtectionGroup", "AWSShield_20160616.DescribeProtectionGroup"},
		{"DescribeSubscription", "AWSShield_20160616.DescribeSubscription"},
		{"DisableApplicationLayerAutomaticResponse", "AWSShield_20160616.DisableApplicationLayerAutomaticResponse"},
		{"DisableProactiveEngagement", "AWSShield_20160616.DisableProactiveEngagement"},
		{"DisassociateDRTLogBucket", "AWSShield_20160616.DisassociateDRTLogBucket"},
		{"DisassociateDRTRole", "AWSShield_20160616.DisassociateDRTRole"},
		{"DisassociateHealthCheck", "AWSShield_20160616.DisassociateHealthCheck"},
		{"EnableApplicationLayerAutomaticResponse", "AWSShield_20160616.EnableApplicationLayerAutomaticResponse"},
		{"EnableProactiveEngagement", "AWSShield_20160616.EnableProactiveEngagement"},
		{"GetSubscriptionState", "AWSShield_20160616.GetSubscriptionState"},
		{"ListAttacks", "AWSShield_20160616.ListAttacks"},
		{"ListProtectionGroups", "AWSShield_20160616.ListProtectionGroups"},
		{"ListProtections", "AWSShield_20160616.ListProtections"},
		{"ListResourcesInProtectionGroup", "AWSShield_20160616.ListResourcesInProtectionGroup"},
		{"ListTagsForResource", "AWSShield_20160616.ListTagsForResource"},
		{"TagResource", "AWSShield_20160616.TagResource"},
		{"UntagResource", "AWSShield_20160616.UntagResource"},
		{"UpdateApplicationLayerAutomaticResponse", "AWSShield_20160616.UpdateApplicationLayerAutomaticResponse"},
		{"UpdateEmergencyContactSettings", "AWSShield_20160616.UpdateEmergencyContactSettings"},
		{"UpdateProtectionGroup", "AWSShield_20160616.UpdateProtectionGroup"},
		{"UpdateSubscription", "AWSShield_20160616.UpdateSubscription"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Shield operation's
// authoritative X-Amz-Target through ExtractOperation and Handler(),
// asserting the header resolves to the right op name and that Handler()
// does not fall through to h.dispatch's single unmatched-route return
// (fmt.Errorf("%w: %s", errUnknownAction, op), handler.go's dispatch()
// single production call site).
//
// This asserts on MESSAGE TEXT ("unknown action"), not wire type. Shield's
// classifyShieldError maps errUnknownAction to the SAME wire type
// (InvalidParameterException) as two other, ordinary sentinels --
// awserr.ErrInvalidParameter and errInvalidRequest (see shieldErrorRules'
// own doc comment) -- so asserting on __type would be structurally unsafe
// here, exactly the workmail/transfer/datasync/workspaces/codebuild/personalize
// pattern named in the task, even though nothing leaks into it from a real
// op today. errUnknownAction's message ("unknown action: <op>") has exactly
// one production call site (grepped) and is not produced by any other error
// path (errInvalidRequest's own message is "invalid request", a different
// string), so asserting on message text is safe.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			h := shield.NewHandler(shield.NewInMemoryBackend("000000000000", "us-east-1"))

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
			req.Header.Set("X-Amz-Target", tc.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, tc.op, got)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "unknown action",
				"target=%s op=%s: dispatched to the unmatched-route handler", tc.target, tc.op)
		})
	}
}
