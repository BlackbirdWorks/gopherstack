package kinesisanalyticsv2_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesisanalyticsv2"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real Kinesis
// Data Analytics v2 operation, extracted from kinesisanalyticsv2@v1.41.4
// serializers.go: each op's awsAwsjson11_serializeOp<Op>.HandleSerialize
// sets httpBindingEncoder.SetHeader("X-Amz-Target").String(
// "KinesisAnalytics_20180523.<Op>") and always POSTs to "/" -- Kinesis Data
// Analytics v2 is JSON-RPC 1.1 (services/_PROTOCOLS.md), so unlike a
// REST-family service there is no path template to get wrong: dispatch is
// entirely by this one header. The target prefix
// ("KinesisAnalytics_20180523" -- note this is the v1 API's target string;
// v2 reuses it, confirmed directly rather than assumed) is read directly
// from serializers.go. ExtractOperation and Handler() (both via buildOps()'s
// map, dispatched inline through h.ops) derive the action the same way, so
// the class of bug this table catches is a dispatch-table key that doesn't
// exactly match the real op name (typo, wrong case), not a route-template
// mismatch.
//
// This table covers all 33 real Kinesis Data Analytics v2 ops
// (kinesisanalyticsv2@v1.41.4) -- confirmed by diffing both
// GetSupportedOperations() and the actual buildOps() map's key set against
// this exact list: zero mismatches in either direction, no dead or excluded
// keys. GetSupportedOperations() here is a hand-maintained literal slice,
// not built by ranging over the dispatch map, so the two diffs are
// genuinely independent checks.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("KinesisAnalytics_20180523.` and
// pulling the suffix after the last dot.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"AddApplicationCloudWatchLoggingOption", "KinesisAnalytics_20180523.AddApplicationCloudWatchLoggingOption"},
		{"AddApplicationInput", "KinesisAnalytics_20180523.AddApplicationInput"},
		{
			"AddApplicationInputProcessingConfiguration",
			"KinesisAnalytics_20180523.AddApplicationInputProcessingConfiguration",
		},
		{"AddApplicationOutput", "KinesisAnalytics_20180523.AddApplicationOutput"},
		{"AddApplicationReferenceDataSource", "KinesisAnalytics_20180523.AddApplicationReferenceDataSource"},
		{"AddApplicationVpcConfiguration", "KinesisAnalytics_20180523.AddApplicationVpcConfiguration"},
		{"CreateApplication", "KinesisAnalytics_20180523.CreateApplication"},
		{"CreateApplicationPresignedUrl", "KinesisAnalytics_20180523.CreateApplicationPresignedUrl"},
		{"CreateApplicationSnapshot", "KinesisAnalytics_20180523.CreateApplicationSnapshot"},
		{"DeleteApplication", "KinesisAnalytics_20180523.DeleteApplication"},
		{
			"DeleteApplicationCloudWatchLoggingOption",
			"KinesisAnalytics_20180523.DeleteApplicationCloudWatchLoggingOption",
		},
		{
			"DeleteApplicationInputProcessingConfiguration",
			"KinesisAnalytics_20180523.DeleteApplicationInputProcessingConfiguration",
		},
		{"DeleteApplicationOutput", "KinesisAnalytics_20180523.DeleteApplicationOutput"},
		{"DeleteApplicationReferenceDataSource", "KinesisAnalytics_20180523.DeleteApplicationReferenceDataSource"},
		{"DeleteApplicationSnapshot", "KinesisAnalytics_20180523.DeleteApplicationSnapshot"},
		{"DeleteApplicationVpcConfiguration", "KinesisAnalytics_20180523.DeleteApplicationVpcConfiguration"},
		{"DescribeApplication", "KinesisAnalytics_20180523.DescribeApplication"},
		{"DescribeApplicationOperation", "KinesisAnalytics_20180523.DescribeApplicationOperation"},
		{"DescribeApplicationSnapshot", "KinesisAnalytics_20180523.DescribeApplicationSnapshot"},
		{"DescribeApplicationVersion", "KinesisAnalytics_20180523.DescribeApplicationVersion"},
		{"DiscoverInputSchema", "KinesisAnalytics_20180523.DiscoverInputSchema"},
		{"ListApplicationOperations", "KinesisAnalytics_20180523.ListApplicationOperations"},
		{"ListApplications", "KinesisAnalytics_20180523.ListApplications"},
		{"ListApplicationSnapshots", "KinesisAnalytics_20180523.ListApplicationSnapshots"},
		{"ListApplicationVersions", "KinesisAnalytics_20180523.ListApplicationVersions"},
		{"ListTagsForResource", "KinesisAnalytics_20180523.ListTagsForResource"},
		{"RollbackApplication", "KinesisAnalytics_20180523.RollbackApplication"},
		{"StartApplication", "KinesisAnalytics_20180523.StartApplication"},
		{"StopApplication", "KinesisAnalytics_20180523.StopApplication"},
		{"TagResource", "KinesisAnalytics_20180523.TagResource"},
		{"UntagResource", "KinesisAnalytics_20180523.UntagResource"},
		{"UpdateApplication", "KinesisAnalytics_20180523.UpdateApplication"},
		{
			"UpdateApplicationMaintenanceConfiguration",
			"KinesisAnalytics_20180523.UpdateApplicationMaintenanceConfiguration",
		},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Kinesis Data
// Analytics v2 operation's authoritative X-Amz-Target through
// ExtractOperation and Handler(), asserting the header resolves to the
// right op name and that Handler() does not fall through to the
// dispatch-miss path (the `!ok` branch inline in Handler(), handler.go's
// single production call site for this exact message) that a
// dispatch-table key mismatch would produce.
//
// This service doesn't route through a shared handleError: Handler() writes
// "InvalidRequestException" directly at TWO call sites -- a missing
// X-Amz-Target header ("missing X-Amz-Target header") and an unmatched op
// ("unknown operation: <op>") -- and handleError (used only for backend
// errors post-dispatch) never produces that wire type at all, so the type
// is unique to those two miss paths but not unique BETWEEN them. Every test
// case here always sends a well-formed, prefixed X-Amz-Target, so the
// missing-header path never fires; asserting on the unmatched-op message
// text specifically ("unknown operation: <op>") distinguishes the two.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			b := kinesisanalyticsv2.NewInMemoryBackend("000000000000", "us-east-1")
			h := kinesisanalyticsv2.NewHandler(b)

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
			req.Header.Set("X-Amz-Target", tc.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, tc.op, got)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "unknown operation: "+tc.op,
				"target=%s op=%s: dispatched to the unmatched-route handler", tc.target, tc.op)
		})
	}
}
