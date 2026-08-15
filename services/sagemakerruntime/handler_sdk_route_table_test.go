package sagemakerruntime_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative method+path for every real SageMaker
// Runtime operation, extracted from sagemakerruntime@v1.43.4 serializers.go:
// each entry's "request.Method" and the string passed to
// httpbinding.SplitURI in that op's
// awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands in for
// the {EndpointName} URI label -- pathToOperation (handler.go) matches
// purely on the fixed literal suffix after the endpoint name
// (extractEndpointName cuts at the first "/" after "/endpoints/"), so the
// literal value doesn't matter here. 3 real ops here, matching SageMaker
// Runtime's real op count and GetSupportedOperations() exactly.
//
// All three share the "/endpoints/{EndpointName}/" path root and are
// disambiguated purely by their distinct literal suffix
// (invocations / async-invocations / invocations-response-stream), so a
// systematic check for a shared method+path across all 3 ops found zero
// collisions.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"InvokeEndpoint", "POST", "/endpoints/PLACEHOLDER/invocations"},
		{"InvokeEndpointAsync", "POST", "/endpoints/PLACEHOLDER/async-invocations"},
		{"InvokeEndpointWithResponseStream", "POST", "/endpoints/PLACEHOLDER/invocations-response-stream"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real SageMaker Runtime
// op's authoritative method+path (see sdkRouteCases) through ExtractOperation
// and asserts pathToOperation (handler.go) resolves it to the right op, all
// 3 ops against SageMaker Runtime's real op count. It then drives the same
// request through the real Handler() and asserts the response's decoded
// "__type" field is never exactly "UnknownOperationException" -- the literal
// Handler() emits from its op-switch default (via errorResponse(
// "UnknownOperationException", "unknown operation: "+r.URL.Path)) when
// pathToOperation returns anything other than the three known ops. A fresh
// InMemoryBackend (newTestHandler) has no endpoint registry wired, so
// validateEndpoint is a no-op success for any EndpointName (see
// endpoint_lookup.go) and every case here reaches its real op handler.
//
// "UnknownOperationException" was grepped across every non-test .go file in
// this package and found nowhere else: the only other modeled exception
// type this handler emits directly is "ValidationError" (note the
// SageMaker-specific spelling, not "ValidationException" -- see handler.go's
// own comment on this), which shares no substring with the miss sentinel, so
// this service cannot collide.
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
			assert.NotContains(t, rec.Body.String(), "UnknownOperationException",
				"method=%s path=%s op=%s: dispatched to the unmatched-route default", tc.method, tc.path, tc.op)
		})
	}
}
