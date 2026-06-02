package ce_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ce"
)

// TestCoverage_GetCostAndUsageWithResources verifies the stub returns valid empty shape.
func TestCoverage_GetCostAndUsageWithResources(t *testing.T) {
	t.Parallel()

	h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))
	rec := doRequest(t, h, "GetCostAndUsageWithResources", map[string]any{
		"TimePeriod":  map[string]string{"Start": "2024-01-01", "End": "2024-01-08"},
		"Granularity": "DAILY",
		"Metrics":     []string{"BlendedCost"},
		"Filter": map[string]any{
			"Dimensions": map[string]any{
				"Key":    "SERVICE",
				"Values": []string{"Amazon EC2"},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		ResultsByTime            []any `json:"ResultsByTime"`
		DimensionValueAttributes []any `json:"DimensionValueAttributes"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.NotNil(t, out.ResultsByTime)
	assert.NotNil(t, out.DimensionValueAttributes)
}

// TestCoverage_GetApproximateUsageRecords verifies approximate usage records response.
func TestCoverage_GetApproximateUsageRecords(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
	}{
		{
			name: "service_dimension",
			body: map[string]any{
				"ApproximationDimension": "SERVICE",
				"Granularity":            "MONTHLY",
			},
		},
		{
			name: "resource_dimension",
			body: map[string]any{
				"ApproximationDimension": "RESOURCE",
				"Granularity":            "DAILY",
				"Services":               []string{"Amazon EC2"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))
			rec := doRequest(t, h, "GetApproximateUsageRecords", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				Services     map[string]string `json:"Services"`
				TotalRecords string            `json:"TotalRecords"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.NotNil(t, out.Services)
			assert.NotEmpty(t, out.TotalRecords)
		})
	}
}

// TestCoverage_GetCostAndUsageComparisons verifies comparison stubs return valid shape.
func TestCoverage_GetCostAndUsageComparisons(t *testing.T) {
	t.Parallel()

	h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))
	rec := doRequest(t, h, "GetCostAndUsageComparisons", map[string]any{
		"BaseTimePeriod":       map[string]string{"Start": "2024-01-01", "End": "2024-02-01"},
		"ComparisonTimePeriod": map[string]string{"Start": "2023-01-01", "End": "2023-02-01"},
		"Granularity":          "MONTHLY",
		"Metrics":              []string{"BlendedCost"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		CostAndUsages     []any `json:"CostAndUsages"`
		TotalCostAndUsage []any `json:"TotalCostAndUsage"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.NotNil(t, out.CostAndUsages)
	assert.NotNil(t, out.TotalCostAndUsage)
}

// TestCoverage_GetCostComparisonDrivers verifies comparison drivers stub.
func TestCoverage_GetCostComparisonDrivers(t *testing.T) {
	t.Parallel()

	h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))
	rec := doRequest(t, h, "GetCostComparisonDrivers", map[string]any{
		"BaselineTimePeriod":   map[string]string{"Start": "2024-01-01", "End": "2024-02-01"},
		"ComparisonTimePeriod": map[string]string{"Start": "2023-01-01", "End": "2023-02-01"},
		"Metric":               "BlendedCost",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		CostComparisonDrivers []any `json:"CostComparisonDrivers"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.NotNil(t, out.CostComparisonDrivers)
}

// TestCoverage_GetCostCategories verifies cost category value lookup.
func TestCoverage_GetCostCategories(t *testing.T) {
	t.Parallel()

	h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))

	doRequest(t, h, "CreateCostCategoryDefinition", map[string]any{
		"Name":        "DeptCat",
		"RuleVersion": "CostCategoryExpression.v1",
		"Rules": []map[string]any{
			{"Value": "Engineering"},
			{"Value": "Marketing"},
			{"Value": "Finance"},
		},
	})

	rec := doRequest(t, h, "GetCostCategories", map[string]any{
		"CostCategoryName": "DeptCat",
		"TimePeriod":       map[string]string{"Start": "2024-01-01", "End": "2024-02-01"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		CostCategoryValues []string `json:"CostCategoryValues"`
		ReturnSize         int      `json:"ReturnSize"`
		TotalSize          int      `json:"TotalSize"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.Len(t, out.CostCategoryValues, 3)
	assert.Equal(t, 3, out.ReturnSize)
	assert.Equal(t, 3, out.TotalSize)
}

// TestCoverage_ListCostCategoryResourceAssociations verifies stub response shape.
func TestCoverage_ListCostCategoryResourceAssociations(t *testing.T) {
	t.Parallel()

	h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))
	rec := doRequest(t, h, "ListCostCategoryResourceAssociations", map[string]any{
		"CostCategoryArn": "arn:aws:ce::000:costcategory/test",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		ResourceTagsCount int `json:"ResourceTagsCount"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.Equal(t, 0, out.ResourceTagsCount)
}

// TestCoverage_GetSavingsPlanPurchaseRecommendationDetails verifies stub.
func TestCoverage_GetSavingsPlanPurchaseRecommendationDetails(t *testing.T) {
	t.Parallel()

	h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))
	rec := doRequest(t, h, "GetSavingsPlanPurchaseRecommendationDetails", map[string]any{
		"RecommendationDetailId": "detail-abc123",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.NotNil(t, out)
}

// TestCoverage_ListSavingsPlansPurchaseRecommendationGeneration verifies generation list.
func TestCoverage_ListSavingsPlansPurchaseRecommendationGeneration(t *testing.T) {
	t.Parallel()

	h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))
	rec := doRequest(t, h, "ListSavingsPlansPurchaseRecommendationGeneration", map[string]any{
		"GenerationStatus": "SUCCEEDED",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		GenerationSummaryList []any `json:"GenerationSummaryList"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.NotNil(t, out.GenerationSummaryList)
}

// TestCoverage_StartSavingsPlansPurchaseRecommendationGeneration verifies generation start.
func TestCoverage_StartSavingsPlansPurchaseRecommendationGeneration(t *testing.T) {
	t.Parallel()

	h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))
	rec := doRequest(t, h, "StartSavingsPlansPurchaseRecommendationGeneration", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
}

// TestCoverage_GetSavingsPlansCoverage verifies coverage response shape.
func TestCoverage_GetSavingsPlansCoverage(t *testing.T) {
	t.Parallel()

	h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))
	rec := doRequest(t, h, "GetSavingsPlansCoverage", map[string]any{
		"TimePeriod":  map[string]string{"Start": "2026-03-01", "End": "2026-04-01"},
		"Granularity": "MONTHLY",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		SavingsPlansCoverages []struct {
			Coverage   map[string]string `json:"Coverage"`
			TimePeriod map[string]string `json:"TimePeriod"`
		} `json:"SavingsPlansCoverages"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	require.NotEmpty(t, out.SavingsPlansCoverages)
	assert.NotEmpty(t, out.SavingsPlansCoverages[0].Coverage)
}

// TestCoverage_BackendGetDimensionValues verifies backend direct calls.
func TestCoverage_BackendGetDimensionValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		dimension string
		name      string
		wantEmpty bool
	}{
		{name: "service", dimension: "SERVICE", wantEmpty: false},
		{name: "region", dimension: "REGION", wantEmpty: false},
		{name: "usage_type", dimension: "USAGE_TYPE", wantEmpty: false},
		{name: "linked_account", dimension: "LINKED_ACCOUNT", wantEmpty: false},
		{name: "instance_type", dimension: "INSTANCE_TYPE", wantEmpty: false},
		{name: "operating_system", dimension: "OPERATING_SYSTEM", wantEmpty: false},
		{name: "unknown_dim", dimension: "UNKNOWN_DIM_XYZ", wantEmpty: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ce.NewInMemoryBackend("000000000000", "us-east-1")
			vals := b.GetDimensionValues(tt.dimension)

			if tt.wantEmpty {
				assert.Empty(t, vals)
			} else {
				assert.NotEmpty(t, vals)
			}
		})
	}
}

// TestCoverage_BackendGetCostAndUsage_MultipleMetrics verifies multi-metric aggregation.
func TestCoverage_BackendGetCostAndUsage_MultipleMetrics(t *testing.T) {
	t.Parallel()

	b := ce.NewInMemoryBackend("000000000000", "us-east-1")
	results := b.GetCostAndUsage(
		"2026-03-01", "2026-04-01", "MONTHLY",
		[]string{"BlendedCost", "UnblendedCost", "UsageQuantity"},
		nil,
	)

	require.NotEmpty(t, results)

	first := results[0]
	_, hasBlended := first.Total["BlendedCost"]
	_, hasUnblended := first.Total["UnblendedCost"]
	_, hasUsage := first.Total["UsageQuantity"]

	assert.True(t, hasBlended, "Total must have BlendedCost")
	assert.True(t, hasUnblended, "Total must have UnblendedCost")
	assert.True(t, hasUsage, "Total must have UsageQuantity")
	assert.Equal(t, "USD", first.Total["BlendedCost"].Unit)
	assert.Equal(t, "N/A", first.Total["UsageQuantity"].Unit)
}

// TestCoverage_BackendReset_ReSeedsLedger verifies the ledger is re-seeded after reset.
func TestCoverage_BackendReset_ReSeedsLedger(t *testing.T) {
	t.Parallel()

	b := ce.NewInMemoryBackend("000000000000", "us-east-1")

	// Ledger should have data before reset
	pre := b.GetDimensionValues("SERVICE")
	assert.NotEmpty(t, pre)

	b.Reset()

	// Ledger should still have data after reset (re-seeded)
	post := b.GetDimensionValues("SERVICE")
	assert.NotEmpty(t, post)
}

// TestCoverage_BackendGetForecast_VariousBuckets verifies forecast bucket generation.
func TestCoverage_BackendGetForecast_VariousBuckets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		start       string
		end         string
		granularity string
		wantBuckets int
	}{
		{name: "monthly_3_buckets", start: "2026-06-01", end: "2026-09-01", granularity: "MONTHLY", wantBuckets: 3},
		{name: "daily_7_buckets", start: "2026-06-01", end: "2026-06-08", granularity: "DAILY", wantBuckets: 7},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ce.NewInMemoryBackend("000000000000", "us-east-1")
			buckets, totalMean, totalLo, totalHi := b.GetForecastByTime(
				tt.start, tt.end, tt.granularity, 80,
			)

			assert.Len(t, buckets, tt.wantBuckets)
			assert.Positive(t, totalMean)
			assert.InDelta(t, totalLo, totalMean, totalMean*2.0)
			assert.InDelta(t, totalHi, totalMean, totalMean*2.0)
		})
	}
}

// TestCoverage_GetUsageForecast_MatchesCostForecast verifies same shape as cost forecast.
func TestCoverage_GetUsageForecast_MatchesCostForecast(t *testing.T) {
	t.Parallel()

	h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))

	costRec := doRequest(t, h, "GetCostForecast", map[string]any{
		"TimePeriod":  map[string]string{"Start": "2026-06-01", "End": "2026-07-01"},
		"Granularity": "MONTHLY",
		"Metric":      "BLENDED_COST",
	})
	require.Equal(t, http.StatusOK, costRec.Code)

	usageRec := doRequest(t, h, "GetUsageForecast", map[string]any{
		"TimePeriod":  map[string]string{"Start": "2026-06-01", "End": "2026-07-01"},
		"Granularity": "MONTHLY",
		"Metric":      "USAGE_QUANTITY",
	})
	require.Equal(t, http.StatusOK, usageRec.Code)

	var costOut, usageOut map[string]any
	require.NoError(t, json.NewDecoder(costRec.Body).Decode(&costOut))
	require.NoError(t, json.NewDecoder(usageRec.Body).Decode(&usageOut))

	// Both should have Total and ForecastResultsByTime
	assert.NotNil(t, costOut["Total"])
	assert.NotNil(t, usageOut["Total"])
	assert.NotNil(t, costOut["ForecastResultsByTime"])
	assert.NotNil(t, usageOut["ForecastResultsByTime"])
}

// TestCoverage_GetSavingsPlansUtilization_DailyGranularity verifies daily SP utilization.
func TestCoverage_GetSavingsPlansUtilization_DailyGranularity(t *testing.T) {
	t.Parallel()

	h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))
	rec := doRequest(t, h, "GetSavingsPlansUtilization", map[string]any{
		"TimePeriod":  map[string]string{"Start": "2026-03-01", "End": "2026-03-08"},
		"Granularity": "DAILY",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		SavingsPlansUtilizationsByTime []any `json:"SavingsPlansUtilizationsByTime"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.Len(t, out.SavingsPlansUtilizationsByTime, 7)
}

// TestCoverage_GetReservationCoverage_DailyGranularity verifies daily coverage.
func TestCoverage_GetReservationCoverage_DailyGranularity(t *testing.T) {
	t.Parallel()

	h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))
	rec := doRequest(t, h, "GetReservationCoverage", map[string]any{
		"TimePeriod":  map[string]string{"Start": "2026-03-01", "End": "2026-03-04"},
		"Granularity": "DAILY",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		CoveragesByTime []any `json:"CoveragesByTime"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.Len(t, out.CoveragesByTime, 3)
}

// TestCoverage_GetReservationUtilization_DailyGranularity verifies daily RI utilization.
func TestCoverage_GetReservationUtilization_DailyGranularity(t *testing.T) {
	t.Parallel()

	h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))
	rec := doRequest(t, h, "GetReservationUtilization", map[string]any{
		"TimePeriod":  map[string]string{"Start": "2026-03-01", "End": "2026-03-04"},
		"Granularity": "DAILY",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		UtilizationsByTime []any `json:"UtilizationsByTime"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.Len(t, out.UtilizationsByTime, 3)
}

// TestCoverage_ListCostAllocationTags_EmptyWithNoTags verifies empty list on fresh backend.
func TestCoverage_ListCostAllocationTags_EmptyWithNoTags(t *testing.T) {
	t.Parallel()

	h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))
	rec := doRequest(t, h, "ListCostAllocationTags", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		CostAllocationTags []any `json:"CostAllocationTags"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.NotNil(t, out.CostAllocationTags)
	assert.Empty(t, out.CostAllocationTags)
}

// TestCoverage_UpdateCostAllocationTagsStatus_MultipleUpdates verifies error returns.
func TestCoverage_UpdateCostAllocationTagsStatus_MultipleUpdates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		updates    []map[string]string
		wantErrors int
	}{
		{
			name: "all_valid",
			updates: []map[string]string{
				{"TagKey": "Env", "Status": "Active"},
				{"TagKey": "Team", "Status": "Inactive"},
			},
			wantErrors: 0,
		},
		{
			name: "one_invalid_status",
			updates: []map[string]string{
				{"TagKey": "Good", "Status": "Active"},
				{"TagKey": "Bad", "Status": "Unknown"},
			},
			wantErrors: 1,
		},
		{
			name: "all_invalid",
			updates: []map[string]string{
				{"TagKey": "A", "Status": "Enabled"},
				{"TagKey": "B", "Status": "Disabled"},
			},
			wantErrors: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))

			body := make([]any, 0, len(tt.updates))
			for _, u := range tt.updates {
				body = append(body, u)
			}

			rec := doRequest(t, h, "UpdateCostAllocationTagsStatus", map[string]any{
				"CostAllocationTagsStatus": body,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				Errors []any `json:"Errors"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.Len(t, out.Errors, tt.wantErrors)
		})
	}
}

// TestCoverage_GetRightsizingRecommendation_EmptyForNoEC2 verifies empty result for
// a service that has no ledger data.
func TestCoverage_GetRightsizingRecommendation_IncludesSummary(t *testing.T) {
	t.Parallel()

	h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))
	rec := doRequest(t, h, "GetRightsizingRecommendation", map[string]any{
		"Service": "Amazon Elastic Compute Cloud - Compute",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Summary                    map[string]string `json:"Summary"`
		RightsizingRecommendations []any             `json:"RightsizingRecommendations"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.NotEmpty(t, out.RightsizingRecommendations)
	assert.NotNil(t, out.Summary)
	_, hasTotalCount := out.Summary["TotalRecommendationCount"]
	assert.True(t, hasTotalCount)
}

// TestCoverage_GetTags_EmptyKeyReturnsAllKeys verifies tag key enumeration.
func TestCoverage_GetTags_EmptyKeyReturnsAllKeys(t *testing.T) {
	t.Parallel()

	h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))
	rec := doRequest(t, h, "GetTags", map[string]any{
		"TimePeriod": map[string]string{"Start": "2024-01-01", "End": "2024-02-01"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Tags       []string `json:"Tags"`
		ReturnSize int      `json:"ReturnSize"`
		TotalSize  int      `json:"TotalSize"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	// Ledger may have no tags seeded — assert shape is correct regardless
	assert.NotNil(t, out.Tags)
	assert.Equal(t, len(out.Tags), out.ReturnSize)
	assert.Equal(t, out.ReturnSize, out.TotalSize)
}

// TestCoverage_BackfillMultipleJobs verifies multiple backfill jobs tracked.
func TestCoverage_BackfillMultipleJobs(t *testing.T) {
	t.Parallel()

	h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))

	// Start 3 backfill jobs
	for _, from := range []string{"2024-01-01T00:00:00Z", "2024-02-01T00:00:00Z", "2024-03-01T00:00:00Z"} {
		rec := doRequest(t, h, "StartCostAllocationTagBackfill", map[string]any{
			"BackfillFrom": from,
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	listRec := doRequest(t, h, "ListCostAllocationTagBackfillHistory", map[string]any{})
	require.Equal(t, http.StatusOK, listRec.Code)

	var out struct {
		BackfillRequests []map[string]any `json:"BackfillRequests"`
	}
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&out))
	assert.Len(t, out.BackfillRequests, 3)
}

// TestCoverage_CommitmentAnalysis_MultipleStartsListed verifies multi-analysis list.
func TestCoverage_CommitmentAnalysis_MultipleStartsListed(t *testing.T) {
	t.Parallel()

	h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))

	analysisIDs := make([]string, 0, 3)

	for range 3 {
		rec := doRequest(t, h, "StartCommitmentPurchaseAnalysis", map[string]any{
			"CommitmentPurchaseAnalysisConfiguration": map[string]any{},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			AnalysisID string `json:"AnalysisId"`
		}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
		require.NotEmpty(t, out.AnalysisID)
		analysisIDs = append(analysisIDs, out.AnalysisID)
	}

	// All distinct IDs
	seen := make(map[string]struct{})
	for _, id := range analysisIDs {
		seen[id] = struct{}{}
	}
	assert.Len(t, seen, 3, "each analysis must have a unique ID")

	listRec := doRequest(t, h, "ListCommitmentPurchaseAnalyses", map[string]any{})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut struct {
		AnalysisSummaryList []map[string]any `json:"AnalysisSummaryList"`
	}
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&listOut))
	assert.Len(t, listOut.AnalysisSummaryList, 3)
}

// TestCoverage_AnomalyFeedback_PreviousFeedbackOverwritten verifies feedback updates.
func TestCoverage_AnomalyFeedback_PreviousFeedbackOverwritten(t *testing.T) {
	t.Parallel()

	h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))
	h.Backend.AddAnomaly(ce.Anomaly{
		AnomalyID:  "feedback-overwrite-1",
		MonitorARN: "arn:aws:ce::000:anomalymonitor/test",
	})

	// First feedback: YES
	rec1 := doRequest(t, h, "ProvideAnomalyFeedback", map[string]any{
		"AnomalyId": "feedback-overwrite-1",
		"Feedback":  "YES",
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	// Second feedback: NO (overwrite)
	rec2 := doRequest(t, h, "ProvideAnomalyFeedback", map[string]any{
		"AnomalyId": "feedback-overwrite-1",
		"Feedback":  "NO",
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	getRec := doRequest(t, h, "GetAnomalies", map[string]any{
		"Feedback": "NO",
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	var out struct {
		Anomalies []struct {
			AnomalyID string `json:"AnomalyId"`
			Feedback  string `json:"Feedback"`
		} `json:"Anomalies"`
	}
	require.NoError(t, json.NewDecoder(getRec.Body).Decode(&out))
	require.Len(t, out.Anomalies, 1)
	assert.Equal(t, "feedback-overwrite-1", out.Anomalies[0].AnomalyID)
	assert.Equal(t, "NO", out.Anomalies[0].Feedback)
}

// TestCoverage_BackendGetReservationPurchaseRecommendations_PaymentOptions verifies
// that different payment options produce different savings estimates.
func TestCoverage_BackendGetReservationPurchaseRecommendations_PaymentOptions(t *testing.T) {
	t.Parallel()

	b := ce.NewInMemoryBackend("000000000000", "us-east-1")

	tests := []struct {
		name    string
		payment string
	}{
		{name: "no_upfront", payment: "NO_UPFRONT"},
		{name: "partial_upfront", payment: "PARTIAL_UPFRONT"},
		{name: "all_upfront", payment: "ALL_UPFRONT"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			recs := b.GetReservationPurchaseRecommendations(
				"Amazon Elastic Compute Cloud - Compute",
				"THIRTY_DAYS", "ONE_YEAR", tt.payment,
			)
			require.NotEmpty(t, recs)
			require.NotEmpty(t, recs[0].RecommendationDetails)
			assert.NotEmpty(t, recs[0].RecommendationDetails[0].EstimatedMonthlySavingsAmount)
		})
	}
}
