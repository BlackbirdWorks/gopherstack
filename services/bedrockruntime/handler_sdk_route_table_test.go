package bedrockruntime_test

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative method+path for every real Bedrock
// Runtime operation, extracted from bedrockruntime@v1.57.1 serializers.go:
// each entry's "request.Method" and the string passed to
// httpbinding.SplitURI in that op's
// awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands in for
// any {modelId}/{guardrailIdentifier}/{guardrailVersion}/{invocationArn} URI
// label -- none of pathToOperation's suffix-matching or
// extractModelID/extractGuardrailIDAndVersion validate identifier shape
// (see extractModelID's own doc comment on ARN-style modelIds), so the
// literal value doesn't matter here, only the fixed literal suffix each op
// is keyed on. 11 real ops here, matching Bedrock Runtime's real op count
// and GetSupportedOperations() exactly (unlike sibling data-plane services
// in this batch, nothing here is a gopherstack-only extension).
//
// A systematic check for a shared method+path across all 11 ops found zero
// collisions -- every op has its own unique (method, path) pair; unlike
// iotdataplane's shadow/connection trio, no two Bedrock Runtime ops share an
// identical path distinguished only by method (every op here is POST or
// GET on its own distinct literal suffix).
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"ApplyGuardrail", "POST", "/guardrail/PLACEHOLDER/version/PLACEHOLDER/apply"},
		{"Converse", "POST", "/model/PLACEHOLDER/converse"},
		{"ConverseStream", "POST", "/model/PLACEHOLDER/converse-stream"},
		{"CountTokens", "POST", "/model/PLACEHOLDER/count-tokens"},
		{"GetAsyncInvoke", "GET", "/async-invoke/PLACEHOLDER"},
		{"InvokeGuardrailChecks", "POST", "/guardrail-checks/invoke"},
		{"InvokeModel", "POST", "/model/PLACEHOLDER/invoke"},
		{"InvokeModelWithBidirectionalStream", "POST", "/model/PLACEHOLDER/invoke-with-bidirectional-stream"},
		{"InvokeModelWithResponseStream", "POST", "/model/PLACEHOLDER/invoke-with-response-stream"},
		{"ListAsyncInvokes", "GET", "/async-invoke"},
		{"StartAsyncInvoke", "POST", "/async-invoke"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Bedrock Runtime op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts pathToOperation (handler.go) resolves it to the right op, all 11
// ops against Bedrock Runtime's real op count. It then drives the same
// request through the real Handler() and asserts the response's decoded
// "__type" field is never exactly "UnknownOperationException" -- the literal
// three dispatch-miss branches in handler.go (the top-level default, and the
// defaults inside handleModelPath and handleGuardrailPath) all emit via
// errorResponse("UnknownOperationException", "unknown operation: "+path).
//
// "UnknownOperationException" was grepped across every non-test .go file in
// this package and found nowhere else: the only other modeled exception
// types are ValidationException (ErrValidation) and ResourceNotFoundException
// (awserr.ErrNotFound, via handleError), neither of which contains
// "UnknownOperationException" as a substring or matches it exactly, so this
// service cannot collide -- worth recording since eight prior services in
// this campaign turned out to have a real collision on their miss sentinel.
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

			// Only the three dispatch-miss branches emit this JSON error
			// shape; a real dispatch hit either succeeds (200, and three of
			// these ops -- ConverseStream, InvokeModelWithResponseStream,
			// InvokeModelWithBidirectionalStream -- write a raw binary event
			// stream frame, not JSON) or fails with a different, non-404
			// error type via handleError. Restricting the decode to 404s
			// avoids misparsing a legitimate streaming success body.
			if rec.Code == 404 {
				var decoded struct {
					Type string `json:"__type"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &decoded))
				assert.NotEqual(t, "UnknownOperationException", decoded.Type,
					"method=%s path=%s op=%s: dispatched to the unmatched-route default", tc.method, tc.path, tc.op)
			}
		})
	}
}
