package applicationautoscaling_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/applicationautoscaling"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real
// Application Auto Scaling operation, extracted from
// applicationautoscaling@v1.45.4 serializers.go: each op's
// awsAwsjson11_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String(
// "AnyScaleFrontendService.<Op>") and always POSTs to "/" -- Application
// Auto Scaling is JSON-RPC 1.1 (services/_PROTOCOLS.md), so dispatch is
// entirely by this one header, not a path template. "AnyScaleFrontendService"
// is this service's real internal AWS codename, bearing no relation to
// "applicationautoscaling" or "ApplicationAutoscaling" -- confirmed
// directly from serializers.go, not assumed.
//
// This table covers all 14 real Application Auto Scaling ops
// (applicationautoscaling@v1.45.4) -- confirmed by diffing both
// GetSupportedOperations() and the actual buildDispatchTable() map's key
// set against this exact list: zero mismatches in either direction. Both
// are separate hand-maintained literals here (neither is built by ranging
// over the other), so the two diffs are genuinely independent checks.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("AnyScaleFrontendService.` and pulling
// the suffix after the last dot.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"DeleteScalingPolicy", "AnyScaleFrontendService.DeleteScalingPolicy"},
		{"DeleteScheduledAction", "AnyScaleFrontendService.DeleteScheduledAction"},
		{"DeregisterScalableTarget", "AnyScaleFrontendService.DeregisterScalableTarget"},
		{"DescribeScalableTargets", "AnyScaleFrontendService.DescribeScalableTargets"},
		{"DescribeScalingActivities", "AnyScaleFrontendService.DescribeScalingActivities"},
		{"DescribeScalingPolicies", "AnyScaleFrontendService.DescribeScalingPolicies"},
		{"DescribeScheduledActions", "AnyScaleFrontendService.DescribeScheduledActions"},
		{"GetPredictiveScalingForecast", "AnyScaleFrontendService.GetPredictiveScalingForecast"},
		{"ListTagsForResource", "AnyScaleFrontendService.ListTagsForResource"},
		{"PutScalingPolicy", "AnyScaleFrontendService.PutScalingPolicy"},
		{"PutScheduledAction", "AnyScaleFrontendService.PutScheduledAction"},
		{"RegisterScalableTarget", "AnyScaleFrontendService.RegisterScalableTarget"},
		{"TagResource", "AnyScaleFrontendService.TagResource"},
		{"UntagResource", "AnyScaleFrontendService.UntagResource"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Application Auto
// Scaling operation's authoritative X-Amz-Target through ExtractOperation
// and Handler(), asserting the header resolves to the right op name and
// that Handler() does not fall through to h.dispatch's unmatched-route
// branch (fmt.Errorf("%w: %s", errUnknownAction, action), handler.go's
// single production call site).
//
// This asserts on MESSAGE TEXT ("unknown action: <op>"), not wire type:
// errUnknownAction's case in handleError is grouped with errInvalidRequest
// and the JSON syntax/type-error branches, all mapping to the shared
// ValidationException -- the same type ordinary bad-input validation
// produces -- so a type assertion here would not distinguish a dispatch
// miss from a routine validation failure. errUnknownAction's message
// ("unknown action: <action>") has exactly one production call site
// (grepped) and is not produced by any other error path.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			h := applicationautoscaling.NewHandler(
				applicationautoscaling.NewInMemoryBackend("000000000000", "us-east-1"),
			)

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
			req.Header.Set("X-Amz-Target", tc.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, tc.op, got)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "unknown action: "+tc.op,
				"target=%s op=%s: dispatched to the unmatched-route handler", tc.target, tc.op)
		})
	}
}
