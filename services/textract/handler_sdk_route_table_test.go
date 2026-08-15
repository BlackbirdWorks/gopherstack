package textract_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/textract"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real Textract
// operation, extracted from textract@v1.43.4 serializers.go: each op's
// awsAwsjson11_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String("Textract.<Op>") and
// always POSTs to "/" -- Textract is JSON-RPC 1.1 (services/_PROTOCOLS.md),
// so unlike a REST-family service there is no path template to get wrong:
// dispatch is entirely by this one header. The target prefix ("Textract",
// bare, not "AmazonTextract" or a dated variant) is read directly from
// serializers.go. ExtractOperation and Handler() (via buildOps()'s map,
// dispatched through h.dispatch) both derive the action the same way, so the
// class of bug this table catches is a dispatch-table key that doesn't
// exactly match the real op name (typo, wrong case), not a route-template
// mismatch.
//
// This table covers all 25 real Textract ops (textract@v1.43.4) --
// confirmed by diffing both GetSupportedOperations() and the actual
// buildOps() map's key set against this exact list: zero mismatches in
// either direction, no dead or excluded keys. GetSupportedOperations() here
// is a hand-maintained literal slice, not built by ranging over the dispatch
// map, so the two diffs are genuinely independent checks.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("Textract.` and pulling the suffix after
// the dot.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"AnalyzeDocument", "Textract.AnalyzeDocument"},
		{"AnalyzeExpense", "Textract.AnalyzeExpense"},
		{"AnalyzeID", "Textract.AnalyzeID"},
		{"CreateAdapter", "Textract.CreateAdapter"},
		{"CreateAdapterVersion", "Textract.CreateAdapterVersion"},
		{"DeleteAdapter", "Textract.DeleteAdapter"},
		{"DeleteAdapterVersion", "Textract.DeleteAdapterVersion"},
		{"DetectDocumentText", "Textract.DetectDocumentText"},
		{"GetAdapter", "Textract.GetAdapter"},
		{"GetAdapterVersion", "Textract.GetAdapterVersion"},
		{"GetDocumentAnalysis", "Textract.GetDocumentAnalysis"},
		{"GetDocumentTextDetection", "Textract.GetDocumentTextDetection"},
		{"GetExpenseAnalysis", "Textract.GetExpenseAnalysis"},
		{"GetLendingAnalysis", "Textract.GetLendingAnalysis"},
		{"GetLendingAnalysisSummary", "Textract.GetLendingAnalysisSummary"},
		{"ListAdapters", "Textract.ListAdapters"},
		{"ListAdapterVersions", "Textract.ListAdapterVersions"},
		{"ListTagsForResource", "Textract.ListTagsForResource"},
		{"StartDocumentAnalysis", "Textract.StartDocumentAnalysis"},
		{"StartDocumentTextDetection", "Textract.StartDocumentTextDetection"},
		{"StartExpenseAnalysis", "Textract.StartExpenseAnalysis"},
		{"StartLendingAnalysis", "Textract.StartLendingAnalysis"},
		{"TagResource", "Textract.TagResource"},
		{"UntagResource", "Textract.UntagResource"},
		{"UpdateAdapter", "Textract.UpdateAdapter"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Textract operation's
// authoritative X-Amz-Target through ExtractOperation and Handler(),
// asserting the header resolves to the right op name and that Handler()
// does not fall through to the dispatch-miss sentinel (errUnknownAction,
// handler.go's dispatch() single production call site) that a
// dispatch-table key mismatch would produce.
//
// handleError's switch groups errUnknownAction in the SAME case as
// ErrValidation, errInvalidRequest, and JSON syntax/type errors, rendering
// as either "ValidationException" or "InvalidParameterException" depending
// on opsWithoutValidationException[action] -- i.e. the dispatch-miss wire
// type is entirely shared with ordinary validation failures and even varies
// by op, exactly the workmail/transfer/codebuild pattern this campaign keeps
// finding. This test instead asserts on the dispatch-miss message text,
// which is unique regardless of which of the two types it renders as:
// dispatch's fmt.Errorf("%w: %s", errUnknownAction, action) always renders
// as `unknown action: <op>`, a substring none of this package's other error
// messages produce.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			b := textract.NewInMemoryBackend("123456789012", "us-east-1")
			h := textract.NewHandler(b)

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
			req.Header.Set("X-Amz-Target", tc.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, tc.op, got)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "unknown action: "+tc.op,
				"target=%s op=%s: dispatched to the unmatched-route handler", tc.target, tc.op)
		})
	}
}
