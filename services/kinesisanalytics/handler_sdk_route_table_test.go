package kinesisanalytics_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesisanalytics"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real Kinesis
// Data Analytics v1 operation, extracted from kinesisanalytics@v1.33.4
// serializers.go: each op's awsAwsjson11_serializeOp<Op>.HandleSerialize
// sets httpBindingEncoder.SetHeader("X-Amz-Target").String(
// "KinesisAnalytics_20150814.<Op>") and always POSTs to "/" -- Kinesis Data
// Analytics v1 is JSON-RPC 1.1 (services/_PROTOCOLS.md), so dispatch is
// entirely by this one header, not a path template.
//
// This is v1's OWN target prefix, read directly from v1's pinned SDK, not
// assumed from v2's. It does NOT match kinesisanalyticsv2's prefix
// ("KinesisAnalytics_20180523", per services/kinesisanalyticsv2's own
// route table) -- v2 reuses v1's "KinesisAnalytics" product name but keeps
// its own release date, so the two literal target strings never collide.
//
// This table covers all 20 real Kinesis Data Analytics v1 ops
// (kinesisanalytics@v1.33.4) -- confirmed by diffing both
// GetSupportedOperations() and the actual buildOps() map's key set
// against this exact list: zero mismatches in either direction. Both
// GetSupportedOperations() and buildOps() are separate hand-maintained
// literals here (neither is built by ranging over the other), so the two
// diffs are genuinely independent checks.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("KinesisAnalytics_20150814.` and
// pulling the suffix after the last dot.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"AddApplicationCloudWatchLoggingOption", "KinesisAnalytics_20150814.AddApplicationCloudWatchLoggingOption"},
		{"AddApplicationInput", "KinesisAnalytics_20150814.AddApplicationInput"},
		{
			"AddApplicationInputProcessingConfiguration",
			"KinesisAnalytics_20150814.AddApplicationInputProcessingConfiguration",
		},
		{"AddApplicationOutput", "KinesisAnalytics_20150814.AddApplicationOutput"},
		{"AddApplicationReferenceDataSource", "KinesisAnalytics_20150814.AddApplicationReferenceDataSource"},
		{"CreateApplication", "KinesisAnalytics_20150814.CreateApplication"},
		{"DeleteApplication", "KinesisAnalytics_20150814.DeleteApplication"},
		{
			"DeleteApplicationCloudWatchLoggingOption",
			"KinesisAnalytics_20150814.DeleteApplicationCloudWatchLoggingOption",
		},
		{
			"DeleteApplicationInputProcessingConfiguration",
			"KinesisAnalytics_20150814.DeleteApplicationInputProcessingConfiguration",
		},
		{"DeleteApplicationOutput", "KinesisAnalytics_20150814.DeleteApplicationOutput"},
		{"DeleteApplicationReferenceDataSource", "KinesisAnalytics_20150814.DeleteApplicationReferenceDataSource"},
		{"DescribeApplication", "KinesisAnalytics_20150814.DescribeApplication"},
		{"DiscoverInputSchema", "KinesisAnalytics_20150814.DiscoverInputSchema"},
		{"ListApplications", "KinesisAnalytics_20150814.ListApplications"},
		{"ListTagsForResource", "KinesisAnalytics_20150814.ListTagsForResource"},
		{"StartApplication", "KinesisAnalytics_20150814.StartApplication"},
		{"StopApplication", "KinesisAnalytics_20150814.StopApplication"},
		{"TagResource", "KinesisAnalytics_20150814.TagResource"},
		{"UntagResource", "KinesisAnalytics_20150814.UntagResource"},
		{"UpdateApplication", "KinesisAnalytics_20150814.UpdateApplication"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Kinesis Data
// Analytics v1 operation's authoritative X-Amz-Target through
// ExtractOperation and Handler(), asserting the header resolves to the
// right op name and that Handler() does not fall through to h.dispatch's
// unmatched-route branch (fmt.Errorf("%w: %s", errUnknownAction, action),
// handler.go's single production call site).
//
// This asserts on MESSAGE TEXT ("unknown action: <op>"), not wire type:
// errUnknownAction's case in handleError is grouped with syntaxErr/typeErr
// and several other Err* sentinels, all mapping to the shared
// InvalidArgumentException -- the same type ordinary bad-argument
// validation produces -- so a type assertion here would not distinguish a
// dispatch miss from a routine validation failure. errUnknownAction's
// message ("unknown action: <action>") has exactly one production call
// site (grepped) and is not produced by any other error path.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			h := kinesisanalytics.NewHandler(kinesisanalytics.NewInMemoryBackend("us-east-1", "000000000000"))

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
