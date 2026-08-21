package cloudwatch_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteOps is the authoritative operation list for CloudWatch, extracted
// from cloudwatch@v1.66.3's request_snapshot/*.request.snap files (this SDK
// version has no hand-written serializers.go -- it's schema-driven -- so
// the snapshots are the authoritative source instead): every real op is a
// POST to cborServicePath + "<Op>" over smithy rpc-v2-cbor, the protocol
// this SDK version speaks exclusively (see sdk_roundtrip_helper_test.go).
//
// gopherstack-jqh2 pass 4 found a real shape-3 bug here: dispatchCBOR
// (rpcv2cbor.go) is a SEPARATE op-name table from GetSupportedOperations
// and the query/form dispatch chain (dispatchFormAction et al in
// handler.go) -- all three must agree, and dispatchCBOR was missing
// StartMetricStreams/StopMetricStreams. Both ops were correctly listed in
// GetSupportedOperations and correctly wired into the form dispatch chain,
// so RouteMatcher/ExtractOperation and the legacy query-protocol path both
// looked fine; only a real client speaking rpc-v2-cbor (the only protocol
// the pinned SDK actually uses) hit the gap, landing in dispatchCBOR's
// eventual default case: "InvalidAction: unknown operation: <Op>". Fixed
// by adding cborStartMetricStreams/cborStopMetricStreams and wiring them
// into dispatchAnomalyMetricStreamCBOR. See TestSDK_StartStopMetricStreams
// for the real-SDK-client regression test.
//
// Because that dispatch-table-miss error is reachable only through
// Handler() itself, not through ExtractOperation, this test drives every op
// through the real Handler() and asserts the response is not the
// "InvalidAction" dispatch-miss sentinel -- ExtractOperation/RouteMatcher
// alone would not have caught the bug above.
//
// Regenerate by listing request_snapshot/*.request.snap in the pinned
// cloudwatch module.
func sdkRouteOps() []string {
	return []string{
		"AssociateDatasetKmsKey",
		"DeleteAlarmMuteRule",
		"DeleteAlarms",
		"DeleteAnomalyDetector",
		"DeleteDashboards",
		"DeleteInsightRules",
		"DeleteMetricStream",
		"DescribeAlarmContributors",
		"DescribeAlarmHistory",
		"DescribeAlarms",
		"DescribeAlarmsForMetric",
		"DescribeAnomalyDetectors",
		"DescribeInsightRules",
		"DisableAlarmActions",
		"DisableInsightRules",
		"DisassociateDatasetKmsKey",
		"EnableAlarmActions",
		"EnableInsightRules",
		"GetAlarmMuteRule",
		"GetDashboard",
		"GetDataset",
		"GetInsightRuleReport",
		"GetMetricData",
		"GetMetricStatistics",
		"GetMetricStream",
		"GetMetricWidgetImage",
		"GetOTelEnrichment",
		"ListAlarmMuteRules",
		"ListDashboards",
		"ListManagedInsightRules",
		"ListMetricStreams",
		"ListMetrics",
		"ListTagsForResource",
		"PutAlarmMuteRule",
		"PutAnomalyDetector",
		"PutCompositeAlarm",
		"PutDashboard",
		"PutInsightRule",
		"PutLogAlarm",
		"PutManagedInsightRules",
		"PutMetricAlarm",
		"PutMetricData",
		"PutMetricStream",
		"SetAlarmState",
		"StartMetricStreams",
		"StartOTelEnrichment",
		"StopMetricStreams",
		"StopOTelEnrichment",
		"TagResource",
		"UntagResource",
	}
}

// TestExtractOperation_SDKRouteTable drives every real CloudWatch op's
// authoritative rpc-v2-cbor request (see sdkRouteOps) through the real
// Handler() and asserts it does not fall through to the "InvalidAction"
// dispatch-table-miss sentinel (rpcv2cbor.go:259). gopherstack-jqh2 pass 4:
// re-extracted all 50 CloudWatch ops from the pinned SDK; found and fixed a
// shape-3 dispatchCBOR gap for StartMetricStreams/StopMetricStreams (see
// sdkRouteOps' doc comment).
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, op := range sdkRouteOps() {
		t.Run(strings.ToLower(op), func(t *testing.T) {
			t.Parallel()

			h := newCBORHandler()
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, cborTestServicePath+op, nil)
			req.Header.Set("Content-Type", "application/cbor")
			req.Header.Set("Smithy-Protocol", "rpc-v2-cbor")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			require.Equal(t, op, h.ExtractOperation(c))
			require.True(t, h.RouteMatcher()(c))

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "InvalidAction",
				"op=%s: dispatched to the unknown-operation handler", op)
			assert.NotContains(t, rec.Body.String(), "unknown operation",
				"op=%s: dispatched to the unknown-operation handler", op)
		})
	}
}
