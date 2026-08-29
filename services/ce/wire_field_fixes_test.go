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

// TestGetReservationPurchaseRecommendation_NoFabricatedMetadataKeys_RealClient
// covers gopherstack-y1zn. handleGetReservationPurchaseRecommendation emitted
// a Metadata map with "RecommendationTotalCount" and "USD" (the latter was a
// stray use of handlerCurrencyCode's own value as the map key, instead of
// "CurrencyCode"); types.ReservationPurchaseRecommendationMetadata
// (costexplorer@v1.67.4 types/types.go) has neither -- only
// AdditionalMetadata/GenerationTimestamp/RecommendationId. A typed client
// silently ignores unknown keys, so the proof is the raw body.
func TestGetReservationPurchaseRecommendation_NoFabricatedMetadataKeys_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "GetReservationPurchaseRecommendation", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	body := rec.Body.String()
	assert.NotContains(t, body, `"RecommendationTotalCount"`,
		"types.ReservationPurchaseRecommendationMetadata has no RecommendationTotalCount member")
	assert.NotContains(t, body, `"USD":"USD"`,
		"the stray map key must not be the currency value itself")
	assert.NotContains(t, body, `"Metadata"`,
		"no real Metadata field is trackable, so the key should be omitted entirely")
}

// TestGetSavingsPlansPurchaseRecommendation_CurrencyCodeKey_RealClient covers
// gopherstack-y1zn. The recommendation detail and summary maps used
// handlerCurrencyCode's own value ("USD") as the map key instead of
// "CurrencyCode" (costexplorer@v1.67.4 deserializers.go); separately, the
// top-level Metadata map included a fabricated "RecommendationTotalCount" key
// (types.SavingsPlansPurchaseRecommendationMetadata has no such member).
func TestGetSavingsPlansPurchaseRecommendation_CurrencyCodeKey_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "GetSavingsPlansPurchaseRecommendation", map[string]any{
		"SavingsPlansType":     "COMPUTE_SP",
		"TermInYears":          "ONE_YEAR",
		"PaymentOption":        "NO_UPFRONT",
		"LookbackPeriodInDays": "THIRTY_DAYS",
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	body := rec.Body.String()
	assert.NotContains(t, body, `"USD":"USD"`,
		"the map key must be CurrencyCode, not the currency value itself")
	assert.Contains(t, body, `"CurrencyCode":"USD"`,
		"the real member is CurrencyCode")
	assert.NotContains(t, body, `"RecommendationTotalCount"`,
		"types.SavingsPlansPurchaseRecommendationMetadata has no RecommendationTotalCount member")
}

// TestCreateAnomalyMonitor_MonitorSpecification_RealClient covers a
// write-only-state bug found by the primary-method sweep: real
// CreateAnomalyMonitorInput.AnomalyMonitor carries a MonitorSpecification
// *types.Expression member (required for a CUSTOM monitor, or a DIMENSIONAL
// monitor whose MonitorDimension is TAG/COST_CATEGORY -- see
// costexplorer@v1.67.4 types/types.go's AnomalyMonitor doc comment, and its
// serializer/deserializer at serializers.go:2953/deserializers.go:6476).
// This field was previously entirely absent from this package's wire
// structs and internal model: a real client's MonitorSpecification was
// accepted by nothing, stored nowhere, and every GetAnomalyMonitors
// response omitted it regardless of what was sent on Create.
func TestCreateAnomalyMonitor_MonitorSpecification_RealClient(t *testing.T) {
	t.Parallel()

	h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestCEClient(t, h)

	createOut, err := client.CreateAnomalyMonitor(t.Context(), &costexplorersdk.CreateAnomalyMonitorInput{
		AnomalyMonitor: &cetypes.AnomalyMonitor{
			MonitorName: aws.String("CustomTagMonitor"),
			MonitorType: cetypes.MonitorTypeCustom,
			MonitorSpecification: &cetypes.Expression{
				Tags: &cetypes.TagValues{
					Key:    aws.String("team"),
					Values: []string{"prod"},
				},
			},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(createOut.MonitorArn))

	getOut, err := client.GetAnomalyMonitors(t.Context(), &costexplorersdk.GetAnomalyMonitorsInput{
		MonitorArnList: []string{aws.ToString(createOut.MonitorArn)},
	})
	require.NoError(t, err)
	require.Len(t, getOut.AnomalyMonitors, 1)

	got := getOut.AnomalyMonitors[0]
	require.NotNil(t, got.MonitorSpecification,
		"MonitorSpecification must round-trip through Create->Get, not be silently dropped")
	require.NotNil(t, got.MonitorSpecification.Tags)
	assert.Equal(t, "team", aws.ToString(got.MonitorSpecification.Tags.Key))
	assert.Equal(t, []string{"prod"}, got.MonitorSpecification.Tags.Values)
}

// TestAnomalySubscription_ThresholdExpression_RealClient covers the sibling
// write-only-state bug in the same family: real AnomalySubscription/
// CreateAnomalySubscriptionInput/UpdateAnomalySubscriptionInput all carry a
// ThresholdExpression *types.Expression member, the non-deprecated
// replacement for Threshold ("you can specify either Threshold or
// ThresholdExpression, but not both" -- costexplorer@v1.67.4
// types/types.go). It was entirely absent from this package's wire structs
// and internal model, so a real client using only ThresholdExpression (the
// documented modern path) had it silently dropped on Create, missing on
// every Get, and any Update value discarded too.
func TestAnomalySubscription_ThresholdExpression_RealClient(t *testing.T) {
	t.Parallel()

	h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestCEClient(t, h)

	monOut, err := client.CreateAnomalyMonitor(t.Context(), &costexplorersdk.CreateAnomalyMonitorInput{
		AnomalyMonitor: &cetypes.AnomalyMonitor{
			MonitorName:      aws.String("Mon"),
			MonitorType:      cetypes.MonitorTypeDimensional,
			MonitorDimension: cetypes.MonitorDimensionService,
		},
	})
	require.NoError(t, err)

	thresholdExpr := &cetypes.Expression{
		Dimensions: &cetypes.DimensionValues{
			Key:    cetypes.DimensionAnomalyTotalImpactAbsolute,
			Values: []string{"100"},
		},
	}

	createOut, err := client.CreateAnomalySubscription(t.Context(), &costexplorersdk.CreateAnomalySubscriptionInput{
		AnomalySubscription: &cetypes.AnomalySubscription{
			SubscriptionName: aws.String("Sub"),
			Frequency:        cetypes.AnomalySubscriptionFrequencyDaily,
			MonitorArnList:   []string{aws.ToString(monOut.MonitorArn)},
			Subscribers: []cetypes.Subscriber{
				{Address: aws.String("a@example.com"), Type: cetypes.SubscriberTypeEmail},
			},
			ThresholdExpression: thresholdExpr,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(createOut.SubscriptionArn))

	getOut, err := client.GetAnomalySubscriptions(t.Context(), &costexplorersdk.GetAnomalySubscriptionsInput{
		SubscriptionArnList: []string{aws.ToString(createOut.SubscriptionArn)},
	})
	require.NoError(t, err)
	require.Len(t, getOut.AnomalySubscriptions, 1)

	got := getOut.AnomalySubscriptions[0]
	require.NotNil(t, got.ThresholdExpression,
		"ThresholdExpression must round-trip through Create->Get, not be silently dropped")
	require.NotNil(t, got.ThresholdExpression.Dimensions)
	assert.Equal(t, cetypes.DimensionAnomalyTotalImpactAbsolute, got.ThresholdExpression.Dimensions.Key)
	assert.Equal(t, []string{"100"}, got.ThresholdExpression.Dimensions.Values)

	// Update with a new ThresholdExpression must also round-trip, not be discarded.
	newExpr := &cetypes.Expression{
		Dimensions: &cetypes.DimensionValues{
			Key:    cetypes.DimensionAnomalyTotalImpactPercentage,
			Values: []string{"50"},
		},
	}
	_, err = client.UpdateAnomalySubscription(t.Context(), &costexplorersdk.UpdateAnomalySubscriptionInput{
		SubscriptionArn:     createOut.SubscriptionArn,
		ThresholdExpression: newExpr,
	})
	require.NoError(t, err)

	getOut2, err := client.GetAnomalySubscriptions(t.Context(), &costexplorersdk.GetAnomalySubscriptionsInput{
		SubscriptionArnList: []string{aws.ToString(createOut.SubscriptionArn)},
	})
	require.NoError(t, err)
	require.Len(t, getOut2.AnomalySubscriptions, 1)
	got2 := getOut2.AnomalySubscriptions[0]
	require.NotNil(t, got2.ThresholdExpression)
	assert.Equal(t, cetypes.DimensionAnomalyTotalImpactPercentage, got2.ThresholdExpression.Dimensions.Key)
	assert.Equal(t, []string{"50"}, got2.ThresholdExpression.Dimensions.Values)
}

// TestGetAnomalyMonitors_DimensionalValueCount_RealClient covers a
// write-only-state-style sibling bug found by sweeping AnomalyMonitor's
// other real members alongside the MonitorSpecification fix above: real
// types.AnomalyMonitor.DimensionalValueCount ("the value for evaluated
// dimensions" -- costexplorer@v1.67.4 types/types.go) was entirely absent
// from this package's wire struct and never computed, so a real client's
// typed DimensionalValueCount was always the zero value regardless of
// backend state. For a DIMENSIONAL monitor on the SERVICE or LINKED_ACCOUNT
// dimension this emulator has a real, non-fabricated source to derive it
// from: the count of that dimension's distinct values in the synthetic cost
// ledger (the same data GetDimensionValues already reads,
// syntheticServiceCatalog seeding 12 distinct SERVICE values).
func TestGetAnomalyMonitors_DimensionalValueCount_RealClient(t *testing.T) {
	t.Parallel()

	h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestCEClient(t, h)

	createOut, err := client.CreateAnomalyMonitor(t.Context(), &costexplorersdk.CreateAnomalyMonitorInput{
		AnomalyMonitor: &cetypes.AnomalyMonitor{
			MonitorName:      aws.String("ServiceMonitor"),
			MonitorType:      cetypes.MonitorTypeDimensional,
			MonitorDimension: cetypes.MonitorDimensionService,
		},
	})
	require.NoError(t, err)

	getOut, err := client.GetAnomalyMonitors(t.Context(), &costexplorersdk.GetAnomalyMonitorsInput{
		MonitorArnList: []string{aws.ToString(createOut.MonitorArn)},
	})
	require.NoError(t, err)
	require.Len(t, getOut.AnomalyMonitors, 1)
	assert.EqualValues(
		t,
		12,
		getOut.AnomalyMonitors[0].DimensionalValueCount,
		"DimensionalValueCount must reflect the real distinct-SERVICE-value count, not be silently dropped",
	)
}
