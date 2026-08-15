package pipes_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative method+path for every real EventBridge
// Pipes operation, extracted from pipes@v1.26.4 serializers.go: each
// entry's "request.Method" and the string passed to httpbinding.SplitURI in
// that op's awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER
// stands in for the {Name}/{resourceArn} URI label -- this handler's path
// extractors (extractPipeCRUDOp, extractPipeActionOp, extractTagsOp in
// handler.go) never validate identifier shape, so the literal value
// doesn't matter here, only path depth and static segments. 10 real ops
// here, matching pipes's real op count exactly (also matches
// GetSupportedOperations's own 10 entries one-for-one).
//
// A systematic check for a shared method+path across all 10 ops found zero
// collisions: CreatePipe/DescribePipe/DeletePipe/UpdatePipe share
// "/v1/pipes/{Name}" but are disambiguated by method (POST/GET/DELETE/PUT),
// which extractPipeCRUDOp already switches on -- so no *required dynamic*
// (non-template) member -- the s3/glacier vacuity-trap class -- was needed
// to disambiguate any route in this table.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"CreatePipe", "POST", "/v1/pipes/PLACEHOLDER"},
		{"DeletePipe", "DELETE", "/v1/pipes/PLACEHOLDER"},
		{"DescribePipe", "GET", "/v1/pipes/PLACEHOLDER"},
		{"ListPipes", "GET", "/v1/pipes"},
		{"ListTagsForResource", "GET", "/tags/PLACEHOLDER"},
		{"StartPipe", "POST", "/v1/pipes/PLACEHOLDER/start"},
		{"StopPipe", "POST", "/v1/pipes/PLACEHOLDER/stop"},
		{"TagResource", "POST", "/tags/PLACEHOLDER"},
		{"UntagResource", "DELETE", "/tags/PLACEHOLDER"},
		{"UpdatePipe", "PUT", "/v1/pipes/PLACEHOLDER"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Pipes op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts it resolves to the right op, all 10 ops against pipes's real op
// count. It then drives the same request through the real Handler() and
// asserts the response does not contain the exact literal "unknown action"
// that dispatch's terminal default case (handler.go) emits wrapping
// errUnknownAction when ExtractOperation's result matches no case -- this
// service's only dispatch-miss mode, grepped across every non-test .go file
// in this package and confirmed to appear nowhere else (every domain error
// instead carries NotFoundException/ConflictException/ValidationException/
// ServiceQuotaExceededException built from a dynamic err.Error(), never
// this literal).
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
