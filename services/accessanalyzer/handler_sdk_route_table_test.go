package accessanalyzer_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative method+path for every real Access
// Analyzer operation, extracted from accessanalyzer@v1.51.4 serializers.go:
// each entry's "request.Method" and the string passed to
// httpbinding.SplitURI in that op's
// awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands in for
// any {analyzerName}/{ruleName}/{jobId}/{id}/{accessPreviewId}/
// {resourceArn} URI label -- this handler's path parsers (parseAnalyzerPath,
// parseArchiveRule*, parsePolicyGenerationPath, etc. in handler.go and its
// per-family files) never validate identifier shape, so the literal value
// doesn't matter here, only path depth and static segments. The one
// exception is GetFindingsStatistics's real path
// "/analyzer/findings/statistics" -- "findings" there is a literal static
// segment (parseAnalyzerSubResource's pathStatistics case), not a
// PLACEHOLDER-able {analyzerName}, so it is kept literal. 39 real ops here,
// matching accessanalyzer's real op count exactly (also matches
// GetSupportedOperations's own 39 entries one-for-one).
//
// A systematic check for a shared method+path across all 39 ops found zero
// collisions -- every pair of ops sharing a path template (e.g.
// GetAccessPreview/ListAccessPreviewFindings both on
// "/access-preview/{accessPreviewId}", CancelPolicyGeneration/
// GetGeneratedPolicy both on "/policy/generation/{jobId}",
// GenerateFindingRecommendation/GetFindingRecommendation both on
// "/recommendation/{id}") is disambiguated by HTTP method alone, which this
// handler's parsers already switch on -- so no *required dynamic*
// (non-template) member -- the s3/glacier vacuity-trap class -- was needed
// to disambiguate any route in this table.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"ApplyArchiveRule", "PUT", "/archive-rule"},
		{"CancelPolicyGeneration", "PUT", "/policy/generation/PLACEHOLDER"},
		{"CheckAccessNotGranted", "POST", "/policy/check-access-not-granted"},
		{"CheckNoNewAccess", "POST", "/policy/check-no-new-access"},
		{"CheckNoPublicAccess", "POST", "/policy/check-no-public-access"},
		{"CreateAccessPreview", "PUT", "/access-preview"},
		{"CreateAnalyzer", "PUT", "/analyzer"},
		{"CreateArchiveRule", "PUT", "/analyzer/PLACEHOLDER/archive-rule"},
		{"CreateServiceLinkedAnalyzer", "PUT", "/service-linked-analyzer"},
		{"DeleteAnalyzer", "DELETE", "/analyzer/PLACEHOLDER"},
		{"DeleteArchiveRule", "DELETE", "/analyzer/PLACEHOLDER/archive-rule/PLACEHOLDER"},
		{"DeleteServiceLinkedAnalyzer", "DELETE", "/service-linked-analyzer/PLACEHOLDER"},
		{"GenerateFindingRecommendation", "POST", "/recommendation/PLACEHOLDER"},
		{"GetAccessPreview", "GET", "/access-preview/PLACEHOLDER"},
		{"GetAnalyzedResource", "GET", "/analyzed-resource"},
		{"GetAnalyzer", "GET", "/analyzer/PLACEHOLDER"},
		{"GetArchiveRule", "GET", "/analyzer/PLACEHOLDER/archive-rule/PLACEHOLDER"},
		{"GetFinding", "GET", "/finding/PLACEHOLDER"},
		{"GetFindingRecommendation", "GET", "/recommendation/PLACEHOLDER"},
		{"GetFindingsStatistics", "POST", "/analyzer/findings/statistics"},
		{"GetFindingV2", "GET", "/findingv2/PLACEHOLDER"},
		{"GetGeneratedPolicy", "GET", "/policy/generation/PLACEHOLDER"},
		{"ListAccessPreviewFindings", "POST", "/access-preview/PLACEHOLDER"},
		{"ListAccessPreviews", "GET", "/access-preview"},
		{"ListAnalyzedResources", "POST", "/analyzed-resource"},
		{"ListAnalyzers", "GET", "/analyzer"},
		{"ListArchiveRules", "GET", "/analyzer/PLACEHOLDER/archive-rule"},
		{"ListFindings", "POST", "/finding"},
		{"ListFindingsV2", "POST", "/findingv2"},
		{"ListPolicyGenerations", "GET", "/policy/generation"},
		{"ListTagsForResource", "GET", "/tags/PLACEHOLDER"},
		{"StartPolicyGeneration", "PUT", "/policy/generation"},
		{"StartResourceScan", "POST", "/resource/scan"},
		{"TagResource", "POST", "/tags/PLACEHOLDER"},
		{"UntagResource", "DELETE", "/tags/PLACEHOLDER"},
		{"UpdateAnalyzer", "PUT", "/analyzer/PLACEHOLDER"},
		{"UpdateArchiveRule", "PUT", "/analyzer/PLACEHOLDER/archive-rule/PLACEHOLDER"},
		{"UpdateFindings", "PUT", "/finding"},
		{"ValidatePolicy", "POST", "/policy/validation"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Access Analyzer op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts parseAllPaths resolves it to the right op, all 39 ops against
// accessanalyzer's real op count. It then drives the same request through
// the real Handler() and asserts the response does not contain the exact
// literal "not found" that handleREST's opUnknown branch (handler.go:202)
// emits under ResourceNotFoundException when parseAllPaths returns
// opUnknown -- this handler's dispatch-miss mode, grepped across every
// non-test .go file in this package and confirmed to appear nowhere else:
// every domain not-found error instead carries err.Error() built from
// awserr.New's msg (e.g. "ResourceNotFoundException" for
// ErrAnalyzerNotFound) or newNotFoundErr's msg (e.g.
// "PolicyGenerationNotFound", "AccessPreviewNotFound",
// "AnalyzedResourceNotFound"), none of which contain the space-separated
// literal "not found".
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
