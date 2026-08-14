package polly_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative method+path for every real Polly
// operation, extracted from polly@v1.60.4 serializers.go: each entry's
// "request.Method" and the string passed to httpbinding.SplitURI in that
// op's awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands in
// for any {Name}/{TaskId} URI label -- parseRoute (handler.go) matches
// purely on the fixed literal prefix and method, never validating
// identifier shape (suffix, the resource, is url.PathUnescape'd verbatim),
// so the literal value doesn't matter here. 10 real ops here, matching
// Polly's real op count and GetSupportedOperations() exactly.
//
// Three ops (DeleteLexicon, GetLexicon, PutLexicon) share the identical
// path "/v1/lexicons/{Name}" and are disambiguated purely by method
// (DELETE/GET/PUT); two ops (StartSpeechSynthesisTask,
// ListSpeechSynthesisTasks) similarly share "/v1/synthesisTasks"
// (POST/GET). A systematic check for a shared method+path across all 10 ops
// found zero collisions once method is taken into account.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"DeleteLexicon", "DELETE", "/v1/lexicons/PLACEHOLDER"},
		{"DescribeVoices", "GET", "/v1/voices"},
		{"GetLexicon", "GET", "/v1/lexicons/PLACEHOLDER"},
		{"GetSpeechSynthesisTask", "GET", "/v1/synthesisTasks/PLACEHOLDER"},
		{"ListLexicons", "GET", "/v1/lexicons"},
		{"ListSpeechSynthesisTasks", "GET", "/v1/synthesisTasks"},
		{"PutLexicon", "PUT", "/v1/lexicons/PLACEHOLDER"},
		{"StartSpeechSynthesisStream", "POST", "/v1/synthesisStream"},
		{"StartSpeechSynthesisTask", "POST", "/v1/synthesisTasks"},
		{"SynthesizeSpeech", "POST", "/v1/speech"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Polly op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts parseRoute (handler.go) resolves it to the right op, all 10 ops
// against Polly's real op count. It then drives the same request through the
// real Handler() and asserts the response's decoded "__type" field is never
// exactly "Unknown" -- the literal opUnknown constant Handler() writes (via
// writeError(c, http.StatusNotFound, opUnknown, "unknown Polly route")) when
// parseRoute returns opUnknown.
//
// "Unknown" (as a bare, exact __type) was grepped across every non-test .go
// file in this package: writeBackendError's onceErrorTable maps every
// domain sentinel to its own distinct AWS exception name (e.g.
// "LexiconNotFoundException", "InvalidParameterValueException") or, for any
// unmapped error, "ServiceFailureException" -- none of which equal the bare
// string "Unknown", so this service cannot collide on this sentinel.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			h := newHandler()

			e := echo.New()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			require.Equal(t, tc.op, got, "method=%s path=%s", tc.method, tc.path)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), `"__type":"Unknown"`,
				"method=%s path=%s op=%s: dispatched to the unmatched-route default", tc.method, tc.path, tc.op)
		})
	}
}
