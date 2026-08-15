//go:build !integration

package mediastoredata_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative method+path for every real MediaStore
// Data operation, extracted from mediastoredata@v1.32.4 serializers.go: each
// entry's "request.Method" and the string passed to httpbinding.SplitURI in
// that op's awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands
// in for the {Path+} URI label -- ExtractOperation/Handler() (handler.go)
// dispatch on HTTP method alone (plus, for GET only, whether the path is
// exactly "/"), never validating the object path's shape, so the literal
// value doesn't matter here. 5 real ops here, matching MediaStore Data's
// real op count exactly (also matches GetSupportedOperations's own 5
// entries one-for-one).
//
// A systematic check for a shared method+path across all 5 ops found zero
// collisions -- every op has its own unique method (ListItems and GetObject
// share GET, disambiguated by path == "/" alone, itself an unambiguous
// static discriminator, not a required *dynamic* member -- the s3/glacier
// vacuity-trap class doesn't apply here).
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"DeleteObject", "DELETE", "/PLACEHOLDER"},
		{"DescribeObject", "HEAD", "/PLACEHOLDER"},
		{"GetObject", "GET", "/PLACEHOLDER"},
		{"ListItems", "GET", "/"},
		{"PutObject", "PUT", "/PLACEHOLDER"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real MediaStore Data op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts it resolves to the right op, all 5 ops against MediaStore Data's
// real op count. It then drives the same request through the real Handler()
// and asserts the response does not contain the exact literal "method not
// allowed" that Handler()'s dispatch-miss default branch (handler.go:148)
// emits under MethodNotAllowedException with HTTP 405 when the request
// method is none of PUT/GET/DELETE/HEAD.
//
// Unlike most services in this campaign, that default branch is not merely
// collision-free -- it is structurally *unreachable* by any of this table's
// cases, or by any real SDK request at all: every one of MediaStore Data's 5
// operations uses one of exactly those 4 HTTP methods (verified above), so
// no legitimately-shaped client request can ever hit it. This is a
// clean-bound finding of the same class as rolesanywhere's message-less
// domain errors or fis's single-CamelCase-token sentinels, just arrived at
// from the dispatch side (an unreachable default) rather than the message
// side (an uncollidable string). "method not allowed" was still grepped
// across every non-test .go file in this package and found nowhere else, so
// even a hypothetical non-standard-method probe would fail safely rather
// than aliasing a real op's response.
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
			assert.NotContains(t, rec.Body.String(), "method not allowed",
				"method=%s path=%s op=%s: dispatched to the unmatched-route default", tc.method, tc.path, tc.op)
		})
	}
}
