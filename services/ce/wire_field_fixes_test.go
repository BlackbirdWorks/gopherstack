package ce_test

import (
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	costexplorersdk "github.com/aws/aws-sdk-go-v2/service/costexplorer"
	cetypes "github.com/aws/aws-sdk-go-v2/service/costexplorer/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ce"
)

// TestBackfillHistory_RealClient proves ListCostAllocationTagBackfillHistory
// and StartCostAllocationTagBackfill emit BackfillRequest(s) under the real
// PascalCase wire keys. Before the fix, the response body embedded the
// backend's internal BackfillJob model directly, whose JSON tags are
// lowerCamelCase ("backfillFrom", "backfillStatus", ...) -- under this
// service's case-sensitive JSON-RPC 1.1 protocol, a real client's typed
// BackfillFrom/BackfillStatus/RequestedAt were nil/empty on every item,
// regardless of backend state. A raw-body test using the same wrong keys as
// the handler could never have caught this; only decoding through the real
// SDK type can.
func TestBackfillHistory_RealClient(t *testing.T) {
	t.Parallel()

	h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestCEClient(t, h)

	startOut, err := client.StartCostAllocationTagBackfill(
		t.Context(),
		&costexplorersdk.StartCostAllocationTagBackfillInput{
			BackfillFrom: aws.String("2024-01-01T00:00:00Z"),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, startOut.BackfillRequest)
	assert.Equal(t, "2024-01-01T00:00:00Z", aws.ToString(startOut.BackfillRequest.BackfillFrom))
	assert.Equal(t, cetypes.CostAllocationTagBackfillStatusProcessing, startOut.BackfillRequest.BackfillStatus)
	assert.NotEmpty(t, aws.ToString(startOut.BackfillRequest.RequestedAt))

	listOut, err := client.ListCostAllocationTagBackfillHistory(
		t.Context(),
		&costexplorersdk.ListCostAllocationTagBackfillHistoryInput{},
	)
	require.NoError(t, err)
	require.Len(t, listOut.BackfillRequests, 1)
	got := listOut.BackfillRequests[0]
	assert.Equal(t, "2024-01-01T00:00:00Z", aws.ToString(got.BackfillFrom))
	assert.Equal(t, cetypes.CostAllocationTagBackfillStatusProcessing, got.BackfillStatus)
	assert.NotEmpty(t, aws.ToString(got.RequestedAt))
}

// TestCommitmentPurchaseAnalysis_RealClient proves ListCommitmentPurchaseAnalyses
// emits AnalysisSummary items under the real PascalCase wire keys, and that
// StartCommitmentPurchaseAnalysis's required CommitmentPurchaseAnalysisConfiguration
// round-trips instead of being silently discarded. Before the fix: (1) the
// list response embedded the internal CommitmentAnalysis model directly
// (lowerCamelCase tags), so a real client's typed AnalysisId/AnalysisStatus
// were nil/empty on every item; (2) the handler's signature discarded the
// entire request with `_ *startCommitmentPurchaseAnalysisInput`, so the
// required Configuration was never validated, stored, or echoed back on any
// of Start/Get/List.
func TestCommitmentPurchaseAnalysis_RealClient(t *testing.T) {
	t.Parallel()

	h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestCEClient(t, h)

	cfg := &cetypes.CommitmentPurchaseAnalysisConfiguration{
		SavingsPlansPurchaseAnalysisConfiguration: &cetypes.SavingsPlansPurchaseAnalysisConfiguration{
			AnalysisType: cetypes.AnalysisTypeMaxSavings,
			LookBackTimePeriod: &cetypes.DateInterval{
				Start: aws.String("2024-01-01"),
				End:   aws.String("2024-02-01"),
			},
			SavingsPlansToAdd: []cetypes.SavingsPlans{
				{SavingsPlansType: cetypes.SupportedSavingsPlansTypeComputeSp},
			},
		},
	}

	startOut, err := client.StartCommitmentPurchaseAnalysis(
		t.Context(),
		&costexplorersdk.StartCommitmentPurchaseAnalysisInput{
			CommitmentPurchaseAnalysisConfiguration: cfg,
		},
	)
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(startOut.AnalysisId))

	getOut, err := client.GetCommitmentPurchaseAnalysis(
		t.Context(),
		&costexplorersdk.GetCommitmentPurchaseAnalysisInput{
			AnalysisId: startOut.AnalysisId,
		},
	)
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(startOut.AnalysisId), aws.ToString(getOut.AnalysisId))
	require.NotNil(t, getOut.CommitmentPurchaseAnalysisConfiguration)
	require.NotNil(t, getOut.CommitmentPurchaseAnalysisConfiguration.SavingsPlansPurchaseAnalysisConfiguration)
	assert.Equal(t,
		cetypes.AnalysisTypeMaxSavings,
		getOut.CommitmentPurchaseAnalysisConfiguration.SavingsPlansPurchaseAnalysisConfiguration.AnalysisType,
	)

	listOut, err := client.ListCommitmentPurchaseAnalyses(
		t.Context(),
		&costexplorersdk.ListCommitmentPurchaseAnalysesInput{},
	)
	require.NoError(t, err)
	require.Len(t, listOut.AnalysisSummaryList, 1)
	item := listOut.AnalysisSummaryList[0]
	assert.Equal(t, aws.ToString(startOut.AnalysisId), aws.ToString(item.AnalysisId))
	assert.Equal(t, cetypes.AnalysisStatusProcessing, item.AnalysisStatus)
	assert.NotEmpty(t, aws.ToString(item.AnalysisStartedTime))
}

// TestStartCommitmentPurchaseAnalysis_MissingConfigurationReturns400 proves
// the handler validates its required CommitmentPurchaseAnalysisConfiguration
// input rather than silently discarding it. A prior revision's handler
// signature was `_ *startCommitmentPurchaseAnalysisInput`, ignoring the
// entire request body, so a request missing this required field succeeded
// with 200 instead of the real API's ValidationException.
func TestStartCommitmentPurchaseAnalysis_MissingConfigurationReturns400(t *testing.T) {
	t.Parallel()

	h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))
	rec := doRequest(t, h, "StartCommitmentPurchaseAnalysis", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestGetCostCategories_NamesVsValues_RealClient proves GetCostCategories
// returns CostCategoryNames (not CostCategoryValues) when the request omits
// CostCategoryName, matching api_op_GetCostCategories.go's documented
// behavior. Before the fix, the handler always populated CostCategoryValues
// regardless of whether CostCategoryName was set, so a real client asking
// "what cost categories exist" (the no-name case) got an empty typed
// CostCategoryNames back every time.
func TestGetCostCategories_NamesVsValues_RealClient(t *testing.T) {
	t.Parallel()

	h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestCEClient(t, h)

	_, err := client.CreateCostCategoryDefinition(t.Context(), &costexplorersdk.CreateCostCategoryDefinitionInput{
		Name:        aws.String("Env"),
		RuleVersion: cetypes.CostCategoryRuleVersionCostCategoryExpressionV1,
		Rules: []cetypes.CostCategoryRule{
			{Value: aws.String("Production")},
		},
	})
	require.NoError(t, err)

	period := &cetypes.DateInterval{Start: aws.String("2024-01-01"), End: aws.String("2024-02-01")}

	byName, err := client.GetCostCategories(t.Context(), &costexplorersdk.GetCostCategoriesInput{
		TimePeriod:       period,
		CostCategoryName: aws.String("Env"),
	})
	require.NoError(t, err)
	assert.Empty(t, byName.CostCategoryNames)
	assert.Contains(t, byName.CostCategoryValues, "Production")

	noName, err := client.GetCostCategories(t.Context(), &costexplorersdk.GetCostCategoriesInput{
		TimePeriod: period,
	})
	require.NoError(t, err)
	assert.Empty(t, noName.CostCategoryValues)
	assert.Contains(t, noName.CostCategoryNames, "Env")
}

// TestGetRightsizingRecommendation_Configuration_RealClient proves
// GetRightsizingRecommendationOutput always echoes Configuration (with
// AWS-documented server-applied defaults when the request omits it). Before
// the fix, the field was absent from the response entirely, so a real
// client's typed .Configuration was always nil.
func TestGetRightsizingRecommendation_Configuration_RealClient(t *testing.T) {
	t.Parallel()

	h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestCEClient(t, h)

	out, err := client.GetRightsizingRecommendation(t.Context(), &costexplorersdk.GetRightsizingRecommendationInput{
		Service: aws.String("AmazonEC2"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Configuration)
	assert.True(t, out.Configuration.BenefitsConsidered)
	assert.Equal(t, cetypes.RecommendationTarget("SAME_INSTANCE_FAMILY"), out.Configuration.RecommendationTarget)

	out2, err := client.GetRightsizingRecommendation(t.Context(), &costexplorersdk.GetRightsizingRecommendationInput{
		Service: aws.String("AmazonEC2"),
		Configuration: &cetypes.RightsizingRecommendationConfiguration{
			BenefitsConsidered:   false,
			RecommendationTarget: cetypes.RecommendationTarget("CROSS_INSTANCE_FAMILY"),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, out2.Configuration)
	assert.False(t, out2.Configuration.BenefitsConsidered)
	assert.Equal(t, cetypes.RecommendationTarget("CROSS_INSTANCE_FAMILY"), out2.Configuration.RecommendationTarget)
}
