package dlm_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative method+path for every real DLM
// operation, extracted from dlm@v1.39.4 serializers.go: each entry's
// "request.Method" and the string passed to httpbinding.SplitURI in that
// op's awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands in
// for the {PolicyId}/{ResourceArn} URI label -- classifyPath (handler.go)
// dispatches the /tags/ trio on HTTP method alone and the /policies/ family
// on prefix alone, never validating the label's shape, so the literal value
// doesn't matter here, only that a segment follows the base path. 8 real ops
// here, matching DLM's real op count exactly (also matches
// GetSupportedOperations's own 8 entries one-for-one).
//
// A systematic check for a shared method+path across all 8 ops found zero
// collisions -- every op has its own unique (method, path) pair, so no
// *required dynamic* (non-template) member -- the s3/glacier vacuity-trap
// class -- was needed to disambiguate any route in this table.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"CreateLifecyclePolicy", "POST", "/policies"},
		{"DeleteLifecyclePolicy", "DELETE", "/policies/PLACEHOLDER"},
		{"GetLifecyclePolicies", "GET", "/policies"},
		{"GetLifecyclePolicy", "GET", "/policies/PLACEHOLDER"},
		{"ListTagsForResource", "GET", "/tags/PLACEHOLDER"},
		{"TagResource", "POST", "/tags/PLACEHOLDER"},
		{"UntagResource", "DELETE", "/tags/PLACEHOLDER"},
		{"UpdateLifecyclePolicy", "PATCH", "/policies/PLACEHOLDER"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real DLM op's authoritative
// method+path (see sdkRouteCases) through ExtractOperation and asserts
// classifyPath (handler.go) resolves it to the right op, all 8 ops against
// DLM's real op count. It then drives the same request through the real
// Handler() and asserts the response does not contain the exact literal
// "operation not implemented" that handleREST's dispatch-miss default
// branch (handler.go:184) emits under NotImplementedException with HTTP 501
// when classifyPath returns opUnknown.
//
// "operation not implemented" was grepped across every non-test .go file in
// this package and found nowhere else: every domain error instead routes
// through mapError, whose messages are err.Error() on the package's
// awserr-based ErrPolicyNotFound/ErrInvalidRequest/ErrLimitExceeded
// sentinels, none of which contain that three-word literal (their messages
// are the bare exception-type strings "ResourceNotFoundException" /
// "InvalidRequestException" / "LimitExceededException").
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
			assert.NotContains(t, rec.Body.String(), "operation not implemented",
				"method=%s path=%s op=%s: dispatched to the unmatched-route default", tc.method, tc.path, tc.op)
		})
	}
}
