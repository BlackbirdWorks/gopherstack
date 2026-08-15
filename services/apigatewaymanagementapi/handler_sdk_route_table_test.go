package apigatewaymanagementapi_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative method+path for every real API Gateway
// Management API operation, extracted from apigatewaymanagementapi@v1.32.4
// serializers.go: each entry's "request.Method" and the string passed to
// httpbinding.SplitURI in that op's
// awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands in for
// the {ConnectionId} URI label -- ExtractOperation/Handler() (handler.go)
// dispatch on HTTP method alone once the "/@connections/" prefix matches,
// never validating the connection ID's shape, so the literal value doesn't
// matter here, only that a non-empty segment follows the prefix. 3 real ops
// here, matching this service's real op count exactly (also matches
// GetSupportedOperations's own 3 entries one-for-one). The
// "/_gopherstack/apigwmgmt/*" admin diagnostic endpoints Handler() and
// ExtractOperation also serve (ListConnections, Broadcast, Stats, ...) are
// gopherstack-only additions with no counterpart in the real SDK and are
// deliberately excluded from this table.
//
// A systematic check for a shared method+path across all 3 ops found zero
// collisions -- all three share the identical path template, disambiguated
// solely by method, and every method (POST/GET/DELETE) is unique among them.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"DeleteConnection", "DELETE", "/@connections/PLACEHOLDER"},
		{"GetConnection", "GET", "/@connections/PLACEHOLDER"},
		{"PostToConnection", "POST", "/@connections/PLACEHOLDER"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real API Gateway
// Management API op's authoritative method+path (see sdkRouteCases) through
// ExtractOperation and asserts it resolves to the right op, all 3 ops
// against this service's real op count. It then drives the same request
// through the real Handler() and asserts the response does not contain the
// exact literal "not found" that Handler()'s dispatch-miss branch
// (handler.go:194) emits under HTTP 404 when the path matches neither the
// "/@connections/" nor "/_gopherstack/apigwmgmt/" prefix.
//
// That branch is structurally *unreachable* by any of this table's cases,
// since every real op's path already carries the "/@connections/" prefix --
// it can only fire for a request RouteMatcher would never have accepted in
// production. "not found" was still grepped across every non-test .go file
// in this package: the only other candidate hit is the doc comment on
// ErrConnectionNotFound (not a wire message -- that sentinel's actual
// GoneException body always uses the fixed text "the connection is no
// longer available", set by writeGoneException, never err.Error()), so no
// legitimate response can alias the miss sentinel.
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
			assert.NotContains(t, rec.Body.String(), "not found",
				"method=%s path=%s op=%s: dispatched to the unmatched-route default", tc.method, tc.path, tc.op)
		})
	}
}
