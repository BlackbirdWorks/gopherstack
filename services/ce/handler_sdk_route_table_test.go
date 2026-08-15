package ce_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ce"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real AWS Cost
// Explorer operation, extracted from costexplorer@v1.67.4 serializers.go:
// each op's awsAwsjson11_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String("AWSInsightsIndexService.<Op>")
// and always POSTs to "/" -- Cost Explorer is JSON-RPC 1.1
// (services/_PROTOCOLS.md), so unlike a REST-family service there is no
// path template to get wrong: dispatch is entirely by this one header.
// "AWSInsightsIndexService" is Cost Explorer's real internal AWS codename
// (confirmed directly from serializers.go -- not guessable from the "ce"
// directory name or the public "Cost Explorer" branding). ExtractOperation
// and Handler() (via h.dispatch's h.ops flat map, assembled by buildOps()
// merging 8 op-family fragments) both derive the action the same way
// (TrimPrefix on "AWSInsightsIndexService."), so the class of bug this
// table catches is a dispatch-table key that doesn't exactly match the
// real op name (typo, wrong case -- Cost Explorer is case-sensitive
// JSON-RPC), not a route-template mismatch.
//
// This table covers all 47 real Cost Explorer ops (costexplorer@v1.67.4) --
// confirmed by diffing this SDK-extracted list against both
// GetSupportedOperations() (a hand-written literal) and the actual dispatch
// map assembled from all 8 buildXxxOps() family functions (each also a
// hand-written literal, not built by ranging over anything): zero
// mismatches in either direction, no dead, duplicate, or excluded keys
// across the 8 families. The two diffs are genuinely independent -- neither
// GetSupportedOperations nor buildOps is derived from the other.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("AWSInsightsIndexService.` and pulling
// the suffix after the last dot.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"CreateAnomalyMonitor", "AWSInsightsIndexService.CreateAnomalyMonitor"},
		{"CreateAnomalySubscription", "AWSInsightsIndexService.CreateAnomalySubscription"},
		{"CreateCostCategoryDefinition", "AWSInsightsIndexService.CreateCostCategoryDefinition"},
		{"DeleteAnomalyMonitor", "AWSInsightsIndexService.DeleteAnomalyMonitor"},
		{"DeleteAnomalySubscription", "AWSInsightsIndexService.DeleteAnomalySubscription"},
		{"DeleteCostCategoryDefinition", "AWSInsightsIndexService.DeleteCostCategoryDefinition"},
		{"DescribeCostCategoryDefinition", "AWSInsightsIndexService.DescribeCostCategoryDefinition"},
		{"GetAnomalies", "AWSInsightsIndexService.GetAnomalies"},
		{"GetAnomalyMonitors", "AWSInsightsIndexService.GetAnomalyMonitors"},
		{"GetAnomalySubscriptions", "AWSInsightsIndexService.GetAnomalySubscriptions"},
		{"GetApproximateUsageRecords", "AWSInsightsIndexService.GetApproximateUsageRecords"},
		{"GetCommitmentPurchaseAnalysis", "AWSInsightsIndexService.GetCommitmentPurchaseAnalysis"},
		{"GetCostAndUsage", "AWSInsightsIndexService.GetCostAndUsage"},
		{"GetCostAndUsageComparisons", "AWSInsightsIndexService.GetCostAndUsageComparisons"},
		{"GetCostAndUsageWithResources", "AWSInsightsIndexService.GetCostAndUsageWithResources"},
		{"GetCostCategories", "AWSInsightsIndexService.GetCostCategories"},
		{"GetCostComparisonDrivers", "AWSInsightsIndexService.GetCostComparisonDrivers"},
		{"GetCostForecast", "AWSInsightsIndexService.GetCostForecast"},
		{"GetDimensionValues", "AWSInsightsIndexService.GetDimensionValues"},
		{"GetReservationCoverage", "AWSInsightsIndexService.GetReservationCoverage"},
		{"GetReservationPurchaseRecommendation", "AWSInsightsIndexService.GetReservationPurchaseRecommendation"},
		{"GetReservationUtilization", "AWSInsightsIndexService.GetReservationUtilization"},
		{"GetRightsizingRecommendation", "AWSInsightsIndexService.GetRightsizingRecommendation"},
		{
			"GetSavingsPlanPurchaseRecommendationDetails",
			"AWSInsightsIndexService.GetSavingsPlanPurchaseRecommendationDetails",
		},
		{"GetSavingsPlansCoverage", "AWSInsightsIndexService.GetSavingsPlansCoverage"},
		{"GetSavingsPlansPurchaseRecommendation", "AWSInsightsIndexService.GetSavingsPlansPurchaseRecommendation"},
		{"GetSavingsPlansUtilization", "AWSInsightsIndexService.GetSavingsPlansUtilization"},
		{"GetSavingsPlansUtilizationDetails", "AWSInsightsIndexService.GetSavingsPlansUtilizationDetails"},
		{"GetTags", "AWSInsightsIndexService.GetTags"},
		{"GetUsageForecast", "AWSInsightsIndexService.GetUsageForecast"},
		{"ListCommitmentPurchaseAnalyses", "AWSInsightsIndexService.ListCommitmentPurchaseAnalyses"},
		{"ListCostAllocationTagBackfillHistory", "AWSInsightsIndexService.ListCostAllocationTagBackfillHistory"},
		{"ListCostAllocationTags", "AWSInsightsIndexService.ListCostAllocationTags"},
		{"ListCostCategoryDefinitions", "AWSInsightsIndexService.ListCostCategoryDefinitions"},
		{"ListCostCategoryResourceAssociations", "AWSInsightsIndexService.ListCostCategoryResourceAssociations"},
		{
			"ListSavingsPlansPurchaseRecommendationGeneration",
			"AWSInsightsIndexService.ListSavingsPlansPurchaseRecommendationGeneration",
		},
		{"ListTagsForResource", "AWSInsightsIndexService.ListTagsForResource"},
		{"ProvideAnomalyFeedback", "AWSInsightsIndexService.ProvideAnomalyFeedback"},
		{"StartCommitmentPurchaseAnalysis", "AWSInsightsIndexService.StartCommitmentPurchaseAnalysis"},
		{"StartCostAllocationTagBackfill", "AWSInsightsIndexService.StartCostAllocationTagBackfill"},
		{
			"StartSavingsPlansPurchaseRecommendationGeneration",
			"AWSInsightsIndexService.StartSavingsPlansPurchaseRecommendationGeneration",
		},
		{"TagResource", "AWSInsightsIndexService.TagResource"},
		{"UntagResource", "AWSInsightsIndexService.UntagResource"},
		{"UpdateAnomalyMonitor", "AWSInsightsIndexService.UpdateAnomalyMonitor"},
		{"UpdateAnomalySubscription", "AWSInsightsIndexService.UpdateAnomalySubscription"},
		{"UpdateCostAllocationTagsStatus", "AWSInsightsIndexService.UpdateCostAllocationTagsStatus"},
		{"UpdateCostCategoryDefinition", "AWSInsightsIndexService.UpdateCostCategoryDefinition"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Cost Explorer
// operation's authoritative X-Amz-Target through ExtractOperation and
// Handler(), asserting the header resolves to the right op name and that
// Handler() does not fall through to h.dispatch's single unmatched-route
// return (fmt.Errorf("%w: %s", errUnknownAction, action), handler.go's
// dispatch() single production call site).
//
// This asserts on MESSAGE TEXT ("unknown action"), not wire type --
// handleError's default fallthrough case maps errUnknownAction to
// "ValidationError", the SAME wire type shared by ErrValidation and any
// JSON syntax/type-decode error (handler.go:240-257), so asserting on
// __type would be structurally unsafe here. errUnknownAction's message
// ("unknown action: <action>") has exactly one production call site
// (grepped) and is not produced by any other error path, so asserting on
// message text is safe.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
			req.Header.Set("X-Amz-Target", tc.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, tc.op, got)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "unknown action",
				"target=%s op=%s: dispatched to the unmatched-route handler", tc.target, tc.op)
		})
	}
}
