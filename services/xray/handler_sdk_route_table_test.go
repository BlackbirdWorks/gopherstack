package xray_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative method+path for every real X-Ray
// operation, extracted from xray@v1.39.4 serializers.go: each entry's
// "request.Method" and the string passed to httpbinding.SplitURI in that
// op's awsRestjson1_serializeOp<Op>.HandleSerialize. Every X-Ray op posts to
// a distinct static path with no URI labels at all -- there is no
// PLACEHOLDER substitution needed and, unlike the target-header services,
// the discriminator here is the literal path rather than a header, so this
// service is NOT structurally immune to path-based unreachability the way
// JSON-RPC/query services are.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"BatchGetTraces", "POST", "/Traces"},
		{"CancelTraceRetrieval", "POST", "/CancelTraceRetrieval"},
		{"CreateGroup", "POST", "/CreateGroup"},
		{"CreateSamplingRule", "POST", "/CreateSamplingRule"},
		{"DeleteGroup", "POST", "/DeleteGroup"},
		{"DeleteResourcePolicy", "POST", "/DeleteResourcePolicy"},
		{"DeleteSamplingRule", "POST", "/DeleteSamplingRule"},
		{"GetEncryptionConfig", "POST", "/EncryptionConfig"},
		{"GetGroup", "POST", "/GetGroup"},
		{"GetGroups", "POST", "/Groups"},
		{"GetIndexingRules", "POST", "/GetIndexingRules"},
		{"GetInsight", "POST", "/Insight"},
		{"GetInsightEvents", "POST", "/InsightEvents"},
		{"GetInsightImpactGraph", "POST", "/InsightImpactGraph"},
		{"GetInsightSummaries", "POST", "/InsightSummaries"},
		{"GetRetrievedTracesGraph", "POST", "/GetRetrievedTracesGraph"},
		{"GetSamplingRules", "POST", "/GetSamplingRules"},
		{"GetSamplingStatisticSummaries", "POST", "/SamplingStatisticSummaries"},
		{"GetSamplingTargets", "POST", "/SamplingTargets"},
		{"GetServiceGraph", "POST", "/ServiceGraph"},
		{"GetTimeSeriesServiceStatistics", "POST", "/TimeSeriesServiceStatistics"},
		{"GetTraceGraph", "POST", "/TraceGraph"},
		{"GetTraceSegmentDestination", "POST", "/GetTraceSegmentDestination"},
		{"GetTraceSummaries", "POST", "/TraceSummaries"},
		{"ListResourcePolicies", "POST", "/ListResourcePolicies"},
		{"ListRetrievedTraces", "POST", "/ListRetrievedTraces"},
		{"ListTagsForResource", "POST", "/ListTagsForResource"},
		{"PutEncryptionConfig", "POST", "/PutEncryptionConfig"},
		{"PutResourcePolicy", "POST", "/PutResourcePolicy"},
		{"PutTelemetryRecords", "POST", "/TelemetryRecords"},
		{"PutTraceSegments", "POST", "/TraceSegments"},
		{"StartTraceRetrieval", "POST", "/StartTraceRetrieval"},
		{"TagResource", "POST", "/TagResource"},
		{"UntagResource", "POST", "/UntagResource"},
		{"UpdateGroup", "POST", "/UpdateGroup"},
		{"UpdateIndexingRule", "POST", "/UpdateIndexingRule"},
		{"UpdateSamplingRule", "POST", "/UpdateSamplingRule"},
		{"UpdateTraceSegmentDestination", "POST", "/UpdateTraceSegmentDestination"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real X-Ray op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts pathToOperation/xrayPaths resolve it to the right op, all 38 ops
// against xray's real op count. It then drives the same request through the
// real Handler() and asserts the response body is not exactly the literal
// "not found" plain-text sentinel Handler() (handler.go) writes via
// c.String when !xrayPaths[path] -- distinct from every domain not-found
// error in this service (e.g. "group %s not found", "sampling rule %s not
// found"), which are all written through c.JSON and so always produce a
// "{...}" body rather than the bare unquoted sentinel string. An exact-body
// check (not substring) is used deliberately: several legitimate xray error
// messages contain "not found" as a substring, so a NotContains check would
// false-positive on those, matching the same class of bug the iam route
// table's InvalidAction phrase-matching caught in the earlier pass.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			require.Equal(t, tc.op, got, "method=%s path=%s", tc.method, tc.path)

			require.NoError(t, h.Handler()(c))
			assert.NotEqual(t, "not found", rec.Body.String(),
				"method=%s path=%s op=%s: dispatched to the unmatched-route handler", tc.method, tc.path, tc.op)
		})
	}
}
