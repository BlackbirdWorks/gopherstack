package rdsdata_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative method+path for every real RDS Data
// operation, extracted from rdsdata@v1.35.4 serializers.go: each entry's
// "request.Method" and the string passed to httpbinding.SplitURI in that
// op's awsRestjson1_serializeOp<Op>.HandleSerialize. Every path here is a
// fixed literal with no {label} member -- RDS Data's whole API is six
// static, argument-free endpoints (the resourceArn/secretArn/sql all travel
// in the JSON body, never the path), so there is no PLACEHOLDER convention
// needed in this table, unlike almost every other service tabled this
// campaign. 6 real ops here, matching RDS Data's real op count exactly.
//
// A systematic check for a shared method+path across all 6 ops found zero
// collisions -- every op has its own unique (method, path) pair.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"BatchExecuteStatement", "POST", "/BatchExecute"},
		{"BeginTransaction", "POST", "/BeginTransaction"},
		{"CommitTransaction", "POST", "/CommitTransaction"},
		{"ExecuteSql", "POST", "/ExecuteSql"},
		{"ExecuteStatement", "POST", "/Execute"},
		{"RollbackTransaction", "POST", "/RollbackTransaction"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real RDS Data op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts the handler's literal path switch (handler.go) resolves it to the
// right op, all 6 ops against RDS Data's real op count. It then drives the
// same request through the real Handler() and asserts the response does not
// contain the exact literal "unknown action" that dispatch()'s default
// branch emits (via fmt.Errorf("%w: %s", errUnknownAction, op), wrapping
// errors.New("unknown action")) when ExtractOperation returns "Unknown".
//
// "unknown action" was grepped across every non-test .go file in this
// package and found nowhere else: RDSData's own domain sentinels
// (ErrTransactionNotFound -> "TransactionNotFoundException",
// ErrValidation -> "BadRequestException", errInvalidRequest -> "invalid
// request") share no substring with it, so a substring assertion is safe
// here without decoding the body.
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
				"method=%s path=%s op=%s: dispatched to the unmatched-route default", tc.method, tc.path, tc.op)
		})
	}
}
