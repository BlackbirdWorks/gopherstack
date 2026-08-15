package emrserverless_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative method+path for every real EMR
// Serverless operation, extracted from emrserverless@v1.44.4 serializers.go:
// each entry's "request.Method" and the string passed to
// httpbinding.SplitURI in that op's
// awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands in for a
// {applicationId}/{jobRunId}/{sessionId}/{resourceArn} URI label --
// parseEMRPath (handler.go) never validates identifier shape, so the
// literal value doesn't matter here, only path depth and static segments.
// 22 real ops here, matching emrserverless's real op count exactly (also
// matches GetSupportedOperations's own 22 entries one-for-one).
//
// A systematic check for a shared method+path across all 22 ops found zero
// collisions: e.g. GetApplication/UpdateApplication/DeleteApplication share
// "/applications/{applicationId}" but are disambiguated by method
// (GET/PATCH/DELETE), and GetJobRun/CancelJobRun share
// "/applications/{applicationId}/jobruns/{jobRunId}" (GET/DELETE) while
// GetSession/TerminateSession share
// "/applications/{applicationId}/sessions/{sessionId}" (also GET/DELETE) --
// both distinctions parseJobRunRoute already switches on via the "sub"
// path segment plus method -- so no *required dynamic* (non-template)
// member -- the s3/glacier vacuity-trap class -- was needed to disambiguate
// any route in this table.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"CancelJobRun", "DELETE", "/applications/PLACEHOLDER/jobruns/PLACEHOLDER"},
		{"CreateApplication", "POST", "/applications"},
		{"DeleteApplication", "DELETE", "/applications/PLACEHOLDER"},
		{"GetApplication", "GET", "/applications/PLACEHOLDER"},
		{"GetDashboardForJobRun", "GET", "/applications/PLACEHOLDER/jobruns/PLACEHOLDER/dashboard"},
		{"GetJobRun", "GET", "/applications/PLACEHOLDER/jobruns/PLACEHOLDER"},
		{"GetResourceDashboard", "GET", "/applications/PLACEHOLDER/dashboard"},
		{"GetSession", "GET", "/applications/PLACEHOLDER/sessions/PLACEHOLDER"},
		{"GetSessionEndpoint", "GET", "/applications/PLACEHOLDER/sessions/PLACEHOLDER/endpoint"},
		{"ListApplications", "GET", "/applications"},
		{"ListJobRunAttempts", "GET", "/applications/PLACEHOLDER/jobruns/PLACEHOLDER/attempts"},
		{"ListJobRuns", "GET", "/applications/PLACEHOLDER/jobruns"},
		{"ListSessions", "GET", "/applications/PLACEHOLDER/sessions"},
		{"ListTagsForResource", "GET", "/tags/PLACEHOLDER"},
		{"StartApplication", "POST", "/applications/PLACEHOLDER/start"},
		{"StartJobRun", "POST", "/applications/PLACEHOLDER/jobruns"},
		{"StartSession", "POST", "/applications/PLACEHOLDER/sessions"},
		{"StopApplication", "POST", "/applications/PLACEHOLDER/stop"},
		{"TagResource", "POST", "/tags/PLACEHOLDER"},
		{"TerminateSession", "DELETE", "/applications/PLACEHOLDER/sessions/PLACEHOLDER"},
		{"UntagResource", "DELETE", "/tags/PLACEHOLDER"},
		{"UpdateApplication", "PATCH", "/applications/PLACEHOLDER"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real EMR Serverless op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts parseEMRPath (handler.go) resolves it to the right op, all 22 ops
// against emrserverless's real op count. It then drives the same request
// through the real Handler() and asserts the response does not contain the
// exact literal "unknown operation: " that dispatch's terminal default case
// (handler.go) emits wrapping route.operation when emrDispatchTable has no
// entry for it -- this service's only dispatch-miss mode, grepped across
// every non-test .go file in this package and confirmed to appear nowhere
// else (every domain error instead carries a dynamic err.Error() message
// via handleError, never this literal).
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
			assert.NotContains(t, rec.Body.String(), "unknown operation: ",
				"method=%s path=%s op=%s: dispatched to the unmatched-route default", tc.method, tc.path, tc.op)
		})
	}
}
