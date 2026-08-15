package networkmonitor_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative method+path for every real CloudWatch
// Network Monitor operation, extracted from networkmonitor@v1.16.4
// serializers.go: each entry's "request.Method" and the string passed to
// httpbinding.SplitURI in that op's
// awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands in for a
// {monitorName}/{probeId}/{resourceArn} URI label -- extractMonitorOp /
// extractTagOp (handler.go) never validate identifier shape, so the literal
// value doesn't matter here, only path depth and static segments. 12 real
// ops here, matching networkmonitor's real op count exactly (also matches
// GetSupportedOperations's own 12 entries one-for-one).
//
// A systematic check for a shared method+path across all 12 ops found zero
// collisions: GetMonitor/UpdateMonitor/DeleteMonitor share
// "/monitors/{monitorName}" and GetProbe/UpdateProbe/DeleteProbe share
// "/monitors/{monitorName}/probes/{probeId}", but each group is
// disambiguated by method (GET/PATCH/DELETE), which
// extractMonitorCRUDOp/extractProbeOp already switch on -- so no *required
// dynamic* (non-template) member -- the s3/glacier vacuity-trap class --
// was needed to disambiguate any route in this table.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"CreateMonitor", "POST", "/monitors"},
		{"CreateProbe", "POST", "/monitors/PLACEHOLDER/probes"},
		{"DeleteMonitor", "DELETE", "/monitors/PLACEHOLDER"},
		{"DeleteProbe", "DELETE", "/monitors/PLACEHOLDER/probes/PLACEHOLDER"},
		{"GetMonitor", "GET", "/monitors/PLACEHOLDER"},
		{"GetProbe", "GET", "/monitors/PLACEHOLDER/probes/PLACEHOLDER"},
		{"ListMonitors", "GET", "/monitors"},
		{"ListTagsForResource", "GET", "/tags/PLACEHOLDER"},
		{"TagResource", "POST", "/tags/PLACEHOLDER"},
		{"UntagResource", "DELETE", "/tags/PLACEHOLDER"},
		{"UpdateMonitor", "PATCH", "/monitors/PLACEHOLDER"},
		{"UpdateProbe", "PATCH", "/monitors/PLACEHOLDER/probes/PLACEHOLDER"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Network Monitor op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts it resolves to the right op, all 12 ops against networkmonitor's
// real op count. It then drives the same request through the real Handler()
// and asserts the response does not contain the exact literal
// "unknown action" that dispatch's terminal default case (handler.go)
// emits wrapping errUnknownAction when ExtractOperation's result matches no
// case -- this service's only dispatch-miss mode, grepped across every
// non-test .go file in this package and confirmed to appear nowhere else
// (every domain error instead carries a dynamic err.Error() message via
// handleError, never this literal).
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			e := echo.New()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			require.Equal(t, tc.op, got, "method=%s path=%s", tc.method, tc.path)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "unknown action",
				"method=%s path=%s op=%s: dispatched to the unmatched-action default", tc.method, tc.path, tc.op)
		})
	}
}
