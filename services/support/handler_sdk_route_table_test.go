package support_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/support"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real AWS
// Support operation, extracted from support@v1.34.4 serializers.go: each
// op's awsAwsjson11_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String("AWSSupport_20130415.<Op>")
// and always POSTs to "/" -- Support is JSON-RPC 1.1 (services/_PROTOCOLS.md),
// so unlike a REST-family service there is no path template to get wrong:
// dispatch is entirely by this one header. ExtractOperation and Handler()
// (via h.dispatch's h.ops flat map, built once by buildOps()) both derive
// the action the same way (TrimPrefix on "AWSSupport_20130415."), so the
// class of bug this table catches is a dispatch-table key that doesn't
// exactly match the real op name (typo, wrong case -- Support is
// case-sensitive JSON-RPC), not a route-template mismatch.
//
// This table covers all 16 real AWS Support ops (support@v1.34.4). AWS
// Support's real API is unusually small (Business/Enterprise-support-plan
// gated), and every one of gopherstack's 16 dispatch keys corresponds to a
// real SDK operation -- confirmed by diffing this SDK-extracted list
// against both GetSupportedOperations() (a hand-written literal) and the
// actual buildOps() dispatch map (also a hand-written literal, not built by
// ranging over anything): zero mismatches in either direction, no dead or
// excluded keys. The two diffs are genuinely independent -- neither is
// derived from the other.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("AWSSupport_20130415.` and pulling the
// suffix after the last dot.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"AddAttachmentsToSet", "AWSSupport_20130415.AddAttachmentsToSet"},
		{"AddCommunicationToCase", "AWSSupport_20130415.AddCommunicationToCase"},
		{"CreateCase", "AWSSupport_20130415.CreateCase"},
		{"DescribeAttachment", "AWSSupport_20130415.DescribeAttachment"},
		{"DescribeCases", "AWSSupport_20130415.DescribeCases"},
		{"DescribeCommunications", "AWSSupport_20130415.DescribeCommunications"},
		{"DescribeCreateCaseOptions", "AWSSupport_20130415.DescribeCreateCaseOptions"},
		{"DescribeServices", "AWSSupport_20130415.DescribeServices"},
		{"DescribeSeverityLevels", "AWSSupport_20130415.DescribeSeverityLevels"},
		{"DescribeSupportedLanguages", "AWSSupport_20130415.DescribeSupportedLanguages"},
		{
			"DescribeTrustedAdvisorCheckRefreshStatuses",
			"AWSSupport_20130415.DescribeTrustedAdvisorCheckRefreshStatuses",
		},
		{"DescribeTrustedAdvisorCheckResult", "AWSSupport_20130415.DescribeTrustedAdvisorCheckResult"},
		{"DescribeTrustedAdvisorCheckSummaries", "AWSSupport_20130415.DescribeTrustedAdvisorCheckSummaries"},
		{"DescribeTrustedAdvisorChecks", "AWSSupport_20130415.DescribeTrustedAdvisorChecks"},
		{"RefreshTrustedAdvisorCheck", "AWSSupport_20130415.RefreshTrustedAdvisorCheck"},
		{"ResolveCase", "AWSSupport_20130415.ResolveCase"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Support operation's
// authoritative X-Amz-Target through ExtractOperation and Handler(),
// asserting the header resolves to the right op name and that Handler()
// does not fall through to h.dispatch's single unmatched-route return
// (fmt.Errorf("%w: %s", errUnknownAction, action), handler.go's dispatch()
// single production call site).
//
// This asserts on MESSAGE TEXT ("unknown action"), not wire type --
// handleError's resolveErrorType maps errUnknownAction to
// "ValidationException", the SAME wire type shared by ErrValidation and any
// JSON syntax/type-decode error (handler.go:280-282), so asserting on
// __type would be structurally unsafe here. errUnknownAction's message
// ("unknown action: <action>") has exactly one production call site
// (grepped) and is not produced by any other error path, so asserting on
// message text is safe.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			h := support.NewHandler(support.NewInMemoryBackend())

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
