package translate_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/translate"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real Amazon
// Translate operation, extracted from translate@v1.36.4 serializers.go:
// each op's awsAwsjson11_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String(
// "AWSShineFrontendService_20170701.<Op>") and always POSTs to "/" --
// Translate is JSON-RPC 1.1 (services/_PROTOCOLS.md), so dispatch is
// entirely by this one header, not a path template.
// "AWSShineFrontendService" is this service's real internal AWS codename,
// bearing no relation to "translate" -- confirmed directly from
// serializers.go, not assumed.
//
// This table covers all 19 real Translate ops (translate@v1.36.4).
//
// SELF-REFERENTIALLY COLLAPSED, not genuinely independent: unlike most
// services in this campaign, GetSupportedOperations() here is built by
// ranging over h.ops (buildOps()'s own map) --
//
//	ops := make([]string, 0, len(h.ops))
//	for name := range h.ops { ops = append(ops, name) }
//
// -- so it is not a second, independently hand-maintained source; it IS
// the dispatch map's key set, just re-derived. Diffing this SDK list
// against GetSupportedOperations() is therefore only ONE real check (the
// dispatch map vs. the SDK), not two independent ones -- confirmed by
// dumping GetSupportedOperations() at runtime and diffing byte-for-byte
// against this exact list: zero mismatches.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("AWSShineFrontendService_20170701.`
// and pulling the suffix after the last dot.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"CreateParallelData", "AWSShineFrontendService_20170701.CreateParallelData"},
		{"DeleteParallelData", "AWSShineFrontendService_20170701.DeleteParallelData"},
		{"DeleteTerminology", "AWSShineFrontendService_20170701.DeleteTerminology"},
		{"DescribeTextTranslationJob", "AWSShineFrontendService_20170701.DescribeTextTranslationJob"},
		{"GetParallelData", "AWSShineFrontendService_20170701.GetParallelData"},
		{"GetTerminology", "AWSShineFrontendService_20170701.GetTerminology"},
		{"ImportTerminology", "AWSShineFrontendService_20170701.ImportTerminology"},
		{"ListLanguages", "AWSShineFrontendService_20170701.ListLanguages"},
		{"ListParallelData", "AWSShineFrontendService_20170701.ListParallelData"},
		{"ListTagsForResource", "AWSShineFrontendService_20170701.ListTagsForResource"},
		{"ListTerminologies", "AWSShineFrontendService_20170701.ListTerminologies"},
		{"ListTextTranslationJobs", "AWSShineFrontendService_20170701.ListTextTranslationJobs"},
		{"StartTextTranslationJob", "AWSShineFrontendService_20170701.StartTextTranslationJob"},
		{"StopTextTranslationJob", "AWSShineFrontendService_20170701.StopTextTranslationJob"},
		{"TagResource", "AWSShineFrontendService_20170701.TagResource"},
		{"TranslateDocument", "AWSShineFrontendService_20170701.TranslateDocument"},
		{"TranslateText", "AWSShineFrontendService_20170701.TranslateText"},
		{"UntagResource", "AWSShineFrontendService_20170701.UntagResource"},
		{"UpdateParallelData", "AWSShineFrontendService_20170701.UpdateParallelData"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Translate
// operation's authoritative X-Amz-Target through ExtractOperation and
// Handler(), asserting the header resolves to the right op name and that
// Handler() does not fall through to dispatch's unmatched-route branch
// (fmt.Errorf("%w: operation %q", ErrValidation, action), handler.go's
// single production call site for this exact phrasing).
//
// This asserts on MESSAGE TEXT (`operation "<op>"`, JSON-escaped to
// `operation \"<op>\"` on the wire), not wire type: ErrValidation resolves
// to the shared InvalidRequestException, the same type every other
// validation failure in this service produces (missing required field,
// bad enum value, malformed JSON), so a type assertion here would not
// distinguish a dispatch miss from a routine validation failure. The
// `operation %q` phrasing is unique to this one call site (grepped across
// the package) -- every other ErrValidation use in this service has
// different message text. A first version of this assertion checked for
// bare quotes and could never fail (json.Marshal always escapes them) --
// confirmed by deliberately mis-wiring a dispatch key and observing the
// bare-quote assertion silently pass; fixed to match the escaped form.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			h := translate.NewHandler(translate.NewInMemoryBackend("000000000000", "us-east-1"))

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
			req.Header.Set("X-Amz-Target", tc.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, tc.op, got)

			require.NoError(t, h.Handler()(c))
			// The message is JSON-encoded, so a literal `"` in the source
			// message becomes `\"` on the wire -- match the escaped form,
			// not bare quotes (a bare-quote assertion here would never
			// match any JSON body and could never fail).
			assert.NotContains(t, rec.Body.String(), `operation \"`+tc.op+`\"`,
				"target=%s op=%s: dispatched to the unmatched-route handler", tc.target, tc.op)
		})
	}
}
