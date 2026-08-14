package personalize_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/personalize"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real Amazon
// Personalize (control-plane) operation, extracted from
// personalize@v1.50.4 serializers.go: each op's
// awsAwsjson11_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String("AmazonPersonalize.<Op>")
// and always POSTs to "/" -- classic Personalize is JSON-RPC 1.1
// (services/_PROTOCOLS.md), so unlike a REST-family service there is no
// path template to get wrong: dispatch is entirely by this one header.
// ExtractOperation and Handler() both derive the action the same way
// (TrimPrefix on "AmazonPersonalize."), so the class of bug this table can
// catch is a dispatch-table key that doesn't exactly match the real op name
// (typo, wrong case -- Personalize is case-sensitive JSON-RPC), not a
// route-template mismatch.
//
// This table covers all 71 real classic-Personalize ops -- confirmed by
// diffing the actual buildOps() dispatch table (with the two Runtime keys
// below excluded) against this exact list: zero mismatches either
// direction, no dead key, no gap. GetSupportedOperations() is dynamically
// derived from h.ops (not a hand-maintained literal), so no separate
// GetSupportedOperations() diff is needed here.
//
// NOT covered by this table: the two Personalize *Runtime* inference ops
// gopherstack also serves from this same Handler, GetRecommendations and
// GetPersonalizedRanking (see buildOps' "Personalize Runtime" section).
// personalizeruntime@v1.36.4 is a SEPARATE, REST-JSON-1 SDK client
// (services/_PROTOCOLS.md's personalize sub-row): its serializers.go has no
// X-Amz-Target at all -- a real client POSTs directly to "/recommendations"
// / "/personalize-ranking" / "/action-recommendations" with no target
// header (confirmed by reading personalizeruntime@v1.36.4/serializers.go:
// httpbinding.SplitURI("/recommendations") etc., zero
// `SetHeader("X-Amz-Target")` call sites in the file). gopherstack ALSO
// still dispatches these two ops through the SAME X-Amz-Target mechanism as
// the control plane, under a fabricated "AmazonPersonalizeRuntime.<Op>"
// prefix no real AWS client ever sends (handler.go's
// personalizeRuntimeTargetPrefix, kept for the existing fabricated-path
// tests) -- but RouteMatcher/ExtractOperation/Handler() now ALSO route the
// real REST-JSON1 literal paths ("/recommendations",
// "/personalize-ranking") directly, so a real personalizeruntime client
// reaches the handler too (see gopherstack-92ft and
// handler_runtime_real_client_test.go's TestPersonalizeRuntime_RealSDKClient,
// which drives the real SDK client and would 404 without that routing). A
// third real personalizeruntime op, GetActionRecommendations, is still not
// implemented at all (see sdk_completeness_test.go), so there is no
// dispatch key for it to table either way.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("AmazonPersonalize.` and pulling the
// suffix after the last dot.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"CreateBatchInferenceJob", "AmazonPersonalize.CreateBatchInferenceJob"},
		{"CreateBatchSegmentJob", "AmazonPersonalize.CreateBatchSegmentJob"},
		{"CreateCampaign", "AmazonPersonalize.CreateCampaign"},
		{"CreateDataDeletionJob", "AmazonPersonalize.CreateDataDeletionJob"},
		{"CreateDataset", "AmazonPersonalize.CreateDataset"},
		{"CreateDatasetExportJob", "AmazonPersonalize.CreateDatasetExportJob"},
		{"CreateDatasetGroup", "AmazonPersonalize.CreateDatasetGroup"},
		{"CreateDatasetImportJob", "AmazonPersonalize.CreateDatasetImportJob"},
		{"CreateEventTracker", "AmazonPersonalize.CreateEventTracker"},
		{"CreateFilter", "AmazonPersonalize.CreateFilter"},
		{"CreateMetricAttribution", "AmazonPersonalize.CreateMetricAttribution"},
		{"CreateRecommender", "AmazonPersonalize.CreateRecommender"},
		{"CreateSchema", "AmazonPersonalize.CreateSchema"},
		{"CreateSolution", "AmazonPersonalize.CreateSolution"},
		{"CreateSolutionVersion", "AmazonPersonalize.CreateSolutionVersion"},
		{"DeleteCampaign", "AmazonPersonalize.DeleteCampaign"},
		{"DeleteDataset", "AmazonPersonalize.DeleteDataset"},
		{"DeleteDatasetGroup", "AmazonPersonalize.DeleteDatasetGroup"},
		{"DeleteEventTracker", "AmazonPersonalize.DeleteEventTracker"},
		{"DeleteFilter", "AmazonPersonalize.DeleteFilter"},
		{"DeleteMetricAttribution", "AmazonPersonalize.DeleteMetricAttribution"},
		{"DeleteRecommender", "AmazonPersonalize.DeleteRecommender"},
		{"DeleteSchema", "AmazonPersonalize.DeleteSchema"},
		{"DeleteSolution", "AmazonPersonalize.DeleteSolution"},
		{"DescribeAlgorithm", "AmazonPersonalize.DescribeAlgorithm"},
		{"DescribeBatchInferenceJob", "AmazonPersonalize.DescribeBatchInferenceJob"},
		{"DescribeBatchSegmentJob", "AmazonPersonalize.DescribeBatchSegmentJob"},
		{"DescribeCampaign", "AmazonPersonalize.DescribeCampaign"},
		{"DescribeDataDeletionJob", "AmazonPersonalize.DescribeDataDeletionJob"},
		{"DescribeDataset", "AmazonPersonalize.DescribeDataset"},
		{"DescribeDatasetExportJob", "AmazonPersonalize.DescribeDatasetExportJob"},
		{"DescribeDatasetGroup", "AmazonPersonalize.DescribeDatasetGroup"},
		{"DescribeDatasetImportJob", "AmazonPersonalize.DescribeDatasetImportJob"},
		{"DescribeEventTracker", "AmazonPersonalize.DescribeEventTracker"},
		{"DescribeFeatureTransformation", "AmazonPersonalize.DescribeFeatureTransformation"},
		{"DescribeFilter", "AmazonPersonalize.DescribeFilter"},
		{"DescribeMetricAttribution", "AmazonPersonalize.DescribeMetricAttribution"},
		{"DescribeRecipe", "AmazonPersonalize.DescribeRecipe"},
		{"DescribeRecommender", "AmazonPersonalize.DescribeRecommender"},
		{"DescribeSchema", "AmazonPersonalize.DescribeSchema"},
		{"DescribeSolution", "AmazonPersonalize.DescribeSolution"},
		{"DescribeSolutionVersion", "AmazonPersonalize.DescribeSolutionVersion"},
		{"GetSolutionMetrics", "AmazonPersonalize.GetSolutionMetrics"},
		{"ListBatchInferenceJobs", "AmazonPersonalize.ListBatchInferenceJobs"},
		{"ListBatchSegmentJobs", "AmazonPersonalize.ListBatchSegmentJobs"},
		{"ListCampaigns", "AmazonPersonalize.ListCampaigns"},
		{"ListDataDeletionJobs", "AmazonPersonalize.ListDataDeletionJobs"},
		{"ListDatasetExportJobs", "AmazonPersonalize.ListDatasetExportJobs"},
		{"ListDatasetGroups", "AmazonPersonalize.ListDatasetGroups"},
		{"ListDatasetImportJobs", "AmazonPersonalize.ListDatasetImportJobs"},
		{"ListDatasets", "AmazonPersonalize.ListDatasets"},
		{"ListEventTrackers", "AmazonPersonalize.ListEventTrackers"},
		{"ListFilters", "AmazonPersonalize.ListFilters"},
		{"ListMetricAttributionMetrics", "AmazonPersonalize.ListMetricAttributionMetrics"},
		{"ListMetricAttributions", "AmazonPersonalize.ListMetricAttributions"},
		{"ListRecipes", "AmazonPersonalize.ListRecipes"},
		{"ListRecommenders", "AmazonPersonalize.ListRecommenders"},
		{"ListSchemas", "AmazonPersonalize.ListSchemas"},
		{"ListSolutions", "AmazonPersonalize.ListSolutions"},
		{"ListSolutionVersions", "AmazonPersonalize.ListSolutionVersions"},
		{"ListTagsForResource", "AmazonPersonalize.ListTagsForResource"},
		{"StartRecommender", "AmazonPersonalize.StartRecommender"},
		{"StopRecommender", "AmazonPersonalize.StopRecommender"},
		{"StopSolutionVersionCreation", "AmazonPersonalize.StopSolutionVersionCreation"},
		{"TagResource", "AmazonPersonalize.TagResource"},
		{"UntagResource", "AmazonPersonalize.UntagResource"},
		{"UpdateCampaign", "AmazonPersonalize.UpdateCampaign"},
		{"UpdateDataset", "AmazonPersonalize.UpdateDataset"},
		{"UpdateMetricAttribution", "AmazonPersonalize.UpdateMetricAttribution"},
		{"UpdateRecommender", "AmazonPersonalize.UpdateRecommender"},
		{"UpdateSolution", "AmazonPersonalize.UpdateSolution"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real classic-Personalize
// operation's authoritative X-Amz-Target through ExtractOperation and
// Handler(), asserting the header resolves to the right op name and that
// Handler() does not fall through to the dispatch-miss sentinel a
// dispatch-table key mismatch would produce.
//
// Personalize's dispatch-miss sentinel wraps ErrValidation, which is
// wire-typed as "InvalidInputException" -- the same wire type every other
// validation error in this package produces (see errors.go's ErrValidation
// definition and handleError's switch in handler.go), so asserting on the
// response __type would be the workmail/transfer trap: a false positive on
// ordinary working validation. This test instead asserts on the dispatch
// miss's own message text ("not implemented"), which dispatch() produces
// only at its single `fmt.Errorf("%w: operation %q not implemented",
// ErrValidation, action)` call site and which is grepped unique in the
// package.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			backend := personalize.NewInMemoryBackend("000000000000", "us-east-1")
			h := personalize.NewHandler(backend)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
			req.Header.Set("X-Amz-Target", tc.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, tc.op, got)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "not implemented",
				"target=%s op=%s: dispatched to the unmatched-route handler", tc.target, tc.op)
		})
	}
}
