package mwaa_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative method+path for every real MWAA
// operation, extracted from mwaa@v1.43.4 serializers.go: each entry's
// "request.Method" and the string passed to httpbinding.SplitURI in that
// op's awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands in
// for any {Name}/{ResourceArn}/{EnvironmentName} URI label -- ExtractOperation
// and ServeHTTP's own routing (handler.go) do not validate ID shape, so the
// literal value doesn't matter here, only that the path matches Op. 12 real
// ops here, matching mwaa's real op count exactly.
//
// A systematic check for a shared method+path across all 12 ops found zero
// collisions, so no *required dynamic* (non-template) member -- the
// s3/glacier vacuity-trap class -- was needed to disambiguate any route in
// this table.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"CreateCliToken", "POST", "/clitoken/PLACEHOLDER"},
		{"CreateEnvironment", "PUT", "/environments/PLACEHOLDER"},
		{"CreateWebLoginToken", "POST", "/webtoken/PLACEHOLDER"},
		{"DeleteEnvironment", "DELETE", "/environments/PLACEHOLDER"},
		{"GetEnvironment", "GET", "/environments/PLACEHOLDER"},
		{"InvokeRestApi", "POST", "/restapi/PLACEHOLDER"},
		{"ListEnvironments", "GET", "/environments"},
		{"ListTagsForResource", "GET", "/tags/PLACEHOLDER"},
		{"PublishMetrics", "POST", "/metrics/environments/PLACEHOLDER"},
		{"TagResource", "POST", "/tags/PLACEHOLDER"},
		{"UntagResource", "DELETE", "/tags/PLACEHOLDER"},
		{"UpdateEnvironment", "PATCH", "/environments/PLACEHOLDER"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real MWAA op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts it resolves to the right op, all 12 ops against mwaa's real op
// count. It then drives the same request through the real Handler() (which
// wraps ServeHTTP) and asserts the response is neither of this service's
// two distinct dispatch-miss modes: ServeHTTP's own top-level default (an
// unmatched path prefix), which emits the exact literal "resource not
// found" under ResourceNotFoundException, and every dispatch* function's
// shared method-mismatch default (a recognised path prefix with no case for
// this method), which emits the exact literal "method not allowed" under
// MethodNotAllowedException. Both literals were grepped across every
// non-test .go file in this package and appear only on these miss
// branches -- every domain error instead uses a dynamic err.Error() naming
// the missing resource (writeEnvironmentResult/writeEnvironmentVoidResult)
// or a distinct literal ("failed to read request body", "invalid request
// body"), so plain substring checks on both miss literals are safe.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)

			e := echo.New()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			require.Equal(t, tc.op, got, "method=%s path=%s", tc.method, tc.path)

			require.NoError(t, h.Handler()(c))
			body := rec.Body.String()
			assert.NotContains(t, body, "resource not found",
				"method=%s path=%s op=%s: dispatched to the unmatched-path-prefix default", tc.method, tc.path, tc.op)
			assert.NotContains(t, body, "method not allowed",
				"method=%s path=%s op=%s: dispatched to the method-mismatch default", tc.method, tc.path, tc.op)
		})
	}
}
