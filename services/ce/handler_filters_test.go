package ce_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/ce"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetCostCategoriesFilterAndSortNarrow proves Filter.CostCategories and
// SortBy genuinely narrow/reorder a multi-item CostCategoryValues result,
// not merely parse without effect.
func TestGetCostCategoriesFilterAndSortNarrow(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, "CreateCostCategoryDefinition", map[string]any{
		"Name":        "FilterCat",
		"RuleVersion": "CostCategoryExpression.v1",
		"Rules": []map[string]any{
			{"Value": "Alpha"},
			{"Value": "Bravo"},
			{"Value": "Charlie"},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	baseBody := map[string]any{
		"CostCategoryName": "FilterCat",
		"TimePeriod":       map[string]string{"Start": "2024-01-01", "End": "2024-02-01"},
	}

	unfilteredRec := doRequest(t, h, "GetCostCategories", baseBody)
	require.Equal(t, http.StatusOK, unfilteredRec.Code)

	var unfiltered struct {
		CostCategoryValues []string `json:"CostCategoryValues"`
	}
	require.NoError(t, json.NewDecoder(unfilteredRec.Body).Decode(&unfiltered))
	require.Equal(t, []string{"Alpha", "Bravo", "Charlie"}, unfiltered.CostCategoryValues)

	filteredBody := map[string]any{
		"CostCategoryName": "FilterCat",
		"TimePeriod":       map[string]string{"Start": "2024-01-01", "End": "2024-02-01"},
		"Filter": map[string]any{
			"CostCategories": map[string]any{
				"Key":    "FilterCat",
				"Values": []string{"Alpha", "Charlie"},
			},
		},
	}
	filteredRec := doRequest(t, h, "GetCostCategories", filteredBody)
	require.Equal(t, http.StatusOK, filteredRec.Code)

	var filtered struct {
		CostCategoryValues []string `json:"CostCategoryValues"`
		ReturnSize         int      `json:"ReturnSize"`
		TotalSize          int      `json:"TotalSize"`
	}
	require.NoError(t, json.NewDecoder(filteredRec.Body).Decode(&filtered))
	assert.Equal(t, []string{"Alpha", "Charlie"}, filtered.CostCategoryValues)
	assert.Equal(t, 2, filtered.ReturnSize)
	assert.Equal(t, 2, filtered.TotalSize)

	sortedBody := map[string]any{
		"CostCategoryName": "FilterCat",
		"TimePeriod":       map[string]string{"Start": "2024-01-01", "End": "2024-02-01"},
		"SortBy":           []map[string]any{{"Key": "UsageQuantity", "SortOrder": "DESCENDING"}},
	}
	sortedRec := doRequest(t, h, "GetCostCategories", sortedBody)
	require.Equal(t, http.StatusOK, sortedRec.Code)

	var sorted struct {
		CostCategoryValues []string `json:"CostCategoryValues"`
	}
	require.NoError(t, json.NewDecoder(sortedRec.Body).Decode(&sorted))
	assert.Equal(t, []string{"Charlie", "Bravo", "Alpha"}, sorted.CostCategoryValues)
}

// TestGetDimensionValuesFilterAndSortNarrow proves Filter.Dimensions and
// SortBy genuinely narrow/reorder the SERVICE dimension's multi-item value
// set, using the backend's default 90-day synthetic ledger (12 services).
func TestGetDimensionValuesFilterAndSortNarrow(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	unfilteredRec := doRequest(t, h, "GetDimensionValues", map[string]any{
		"Dimension": "SERVICE",
	})
	require.Equal(t, http.StatusOK, unfilteredRec.Code)

	var unfiltered struct {
		DimensionValues []struct {
			Value string `json:"Value"`
		} `json:"DimensionValues"`
	}
	require.NoError(t, json.NewDecoder(unfilteredRec.Body).Decode(&unfiltered))
	require.Len(t, unfiltered.DimensionValues, 12, "synthetic catalog has 12 services")

	// AWS Lambda is the only service seeded with usage type Lambda-GB-Second,
	// so constraining SERVICE by that USAGE_TYPE narrows 12 values to 1.
	filteredRec := doRequest(t, h, "GetDimensionValues", map[string]any{
		"Dimension": "SERVICE",
		"Filter": map[string]any{
			"Dimensions": map[string]any{
				"Key":    "USAGE_TYPE",
				"Values": []string{"Lambda-GB-Second"},
			},
		},
	})
	require.Equal(t, http.StatusOK, filteredRec.Code)

	var filtered struct {
		DimensionValues []struct {
			Value string `json:"Value"`
		} `json:"DimensionValues"`
		ReturnSize int `json:"ReturnSize"`
	}
	require.NoError(t, json.NewDecoder(filteredRec.Body).Decode(&filtered))
	require.Len(t, filtered.DimensionValues, 1)
	assert.Equal(t, "AWS Lambda", filtered.DimensionValues[0].Value)
	assert.Equal(t, 1, filtered.ReturnSize)

	// EC2 has the largest weight (0.40) in the synthetic catalog, so it must
	// have the highest total BlendedCost and sort first under DESCENDING.
	sortedRec := doRequest(t, h, "GetDimensionValues", map[string]any{
		"Dimension": "SERVICE",
		"SortBy":    []map[string]any{{"Key": "BlendedCost", "SortOrder": "DESCENDING"}},
	})
	require.Equal(t, http.StatusOK, sortedRec.Code)

	var sorted struct {
		DimensionValues []struct {
			Value string `json:"Value"`
		} `json:"DimensionValues"`
	}
	require.NoError(t, json.NewDecoder(sortedRec.Body).Decode(&sorted))
	require.NotEmpty(t, sorted.DimensionValues)
	assert.Equal(t, "Amazon Elastic Compute Cloud - Compute", sorted.DimensionValues[0].Value)
}

// TestGetTagsFilterAndSortAccepted verifies Filter/SortBy on GetTags parse
// and apply without error. Unlike GetDimensionValues/GetCostCategories, this
// emulator's synthetic cost ledger never populates CostEntry.Tags (see
// seedCostLedger in cost_usage.go) -- no CE operation writes per-transaction
// tags -- so there is no tagged state anywhere for a Tags filter to narrow.
// This is documented here rather than fabricated: the filtering code is real
// (GetTagKeysFiltered/GetTagValuesFiltered) and will behave correctly the
// moment tagged ledger data exists, but that data does not exist today.
func TestGetTagsFilterAndSortAccepted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "GetTags", map[string]any{
		"TimePeriod": map[string]string{"Start": "2024-01-01", "End": "2024-02-01"},
		"Filter": map[string]any{
			"Tags": map[string]any{"Key": "Environment", "Values": []string{"prod"}},
		},
		"SortBy": []map[string]any{{"Key": "BlendedCost", "SortOrder": "DESCENDING"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Tags []string `json:"Tags"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.Empty(t, out.Tags, "no ledger entry is ever tagged in this emulator")
}

// TestGetSavingsPlansCoverageRegionFilterNarrows proves the REGION Dimensions
// filter genuinely narrows the single synthetic coverage entry to zero items
// when it doesn't match the request's region, rather than being accepted and
// ignored.
func TestGetSavingsPlansCoverageRegionFilterNarrows(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	matchRec := doRequest(t, h, "GetSavingsPlansCoverage", map[string]any{
		"TimePeriod": map[string]string{"Start": "2024-01-01", "End": "2024-02-01"},
		"Filter": map[string]any{
			"Dimensions": map[string]any{"Key": "REGION", "Values": []string{"us-east-1"}},
		},
	})
	require.Equal(t, http.StatusOK, matchRec.Code)

	var matched struct {
		SavingsPlansCoverages []any `json:"SavingsPlansCoverages"`
	}
	require.NoError(t, json.NewDecoder(matchRec.Body).Decode(&matched))
	assert.Len(t, matched.SavingsPlansCoverages, 1)

	missRec := doRequest(t, h, "GetSavingsPlansCoverage", map[string]any{
		"TimePeriod": map[string]string{"Start": "2024-01-01", "End": "2024-02-01"},
		"Filter": map[string]any{
			"Dimensions": map[string]any{"Key": "REGION", "Values": []string{"us-west-2"}},
		},
	})
	require.Equal(t, http.StatusOK, missRec.Code)

	var missed struct {
		SavingsPlansCoverages []any `json:"SavingsPlansCoverages"`
	}
	require.NoError(t, json.NewDecoder(missRec.Body).Decode(&missed))
	assert.Empty(t, missed.SavingsPlansCoverages)
}

// TestGetSavingsPlansPurchaseRecommendationAccountFilterNarrows proves the
// LINKED_ACCOUNT Dimensions filter genuinely suppresses the single synthetic
// recommendation when it names a different account.
func TestGetSavingsPlansPurchaseRecommendationAccountFilterNarrows(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	matchRec := doRequest(t, h, "GetSavingsPlansPurchaseRecommendation", map[string]any{
		"Filter": map[string]any{
			"Dimensions": map[string]any{"Key": "LINKED_ACCOUNT", "Values": []string{"000000000000"}},
		},
	})
	require.Equal(t, http.StatusOK, matchRec.Code)

	var matched map[string]any
	require.NoError(t, json.NewDecoder(matchRec.Body).Decode(&matched))
	assert.NotNil(t, matched["SavingsPlansPurchaseRecommendation"])

	missRec := doRequest(t, h, "GetSavingsPlansPurchaseRecommendation", map[string]any{
		"Filter": map[string]any{
			"Dimensions": map[string]any{"Key": "LINKED_ACCOUNT", "Values": []string{"999999999999"}},
		},
	})
	require.Equal(t, http.StatusOK, missRec.Code)

	var missed map[string]any
	require.NoError(t, json.NewDecoder(missRec.Body).Decode(&missed))
	assert.Nil(t, missed["SavingsPlansPurchaseRecommendation"])
}

// TestGetReservationPurchaseRecommendationAccountFilterNarrows proves the
// LINKED_ACCOUNT Dimensions filter genuinely suppresses the single synthetic
// recommendation when it names a different account.
func TestGetReservationPurchaseRecommendationAccountFilterNarrows(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	matchRec := doRequest(t, h, "GetReservationPurchaseRecommendation", map[string]any{
		"Service": "Amazon Elastic Compute Cloud - Compute",
		"Filter": map[string]any{
			"Dimensions": map[string]any{"Key": "LINKED_ACCOUNT", "Values": []string{"000000000000"}},
		},
	})
	require.Equal(t, http.StatusOK, matchRec.Code)

	var matched struct {
		Recommendations []any `json:"Recommendations"`
	}
	require.NoError(t, json.NewDecoder(matchRec.Body).Decode(&matched))
	assert.NotEmpty(t, matched.Recommendations)

	missRec := doRequest(t, h, "GetReservationPurchaseRecommendation", map[string]any{
		"Service": "Amazon Elastic Compute Cloud - Compute",
		"Filter": map[string]any{
			"Dimensions": map[string]any{"Key": "LINKED_ACCOUNT", "Values": []string{"999999999999"}},
		},
	})
	require.Equal(t, http.StatusOK, missRec.Code)

	var missed struct {
		Recommendations []any `json:"Recommendations"`
	}
	require.NoError(t, json.NewDecoder(missRec.Body).Decode(&missed))
	assert.Empty(t, missed.Recommendations)
}

// TestGetReservationCoverageSortByTimeReorders proves SortBy genuinely
// reorders a multi-item CoveragesByTime result (one entry per day).
func TestGetReservationCoverageSortByTimeReorders(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{
		"TimePeriod":  map[string]string{"Start": "2024-01-01", "End": "2024-01-04"},
		"Granularity": "DAILY",
	}

	ascRec := doRequest(t, h, "GetReservationCoverage", body)
	require.Equal(t, http.StatusOK, ascRec.Code)

	var asc struct {
		CoveragesByTime []struct {
			TimePeriod map[string]string `json:"TimePeriod"`
		} `json:"CoveragesByTime"`
	}
	require.NoError(t, json.NewDecoder(ascRec.Body).Decode(&asc))
	require.Len(t, asc.CoveragesByTime, 3)
	assert.Equal(t, "2024-01-01", asc.CoveragesByTime[0].TimePeriod["Start"])
	assert.Equal(t, "2024-01-03", asc.CoveragesByTime[2].TimePeriod["Start"])

	descBody := map[string]any{
		"TimePeriod":  map[string]string{"Start": "2024-01-01", "End": "2024-01-04"},
		"Granularity": "DAILY",
		"SortBy":      map[string]any{"Key": "Time", "SortOrder": "DESCENDING"},
	}
	descRec := doRequest(t, h, "GetReservationCoverage", descBody)
	require.Equal(t, http.StatusOK, descRec.Code)

	var desc struct {
		CoveragesByTime []struct {
			TimePeriod map[string]string `json:"TimePeriod"`
		} `json:"CoveragesByTime"`
	}
	require.NoError(t, json.NewDecoder(descRec.Body).Decode(&desc))
	require.Len(t, desc.CoveragesByTime, 3)
	assert.Equal(t, "2024-01-03", desc.CoveragesByTime[0].TimePeriod["Start"])
	assert.Equal(t, "2024-01-01", desc.CoveragesByTime[2].TimePeriod["Start"])
}

// TestGetReservationCoverageServiceFilterZeroesCost proves Filter.Dimensions
// (SERVICE) genuinely changes the computed coverage cost, using a date range
// that overlaps the backend's live 90-day synthetic ledger.
func TestGetReservationCoverageServiceFilterZeroesCost(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	now := time.Now().UTC()
	start := now.AddDate(0, 0, -2).Format("2006-01-02")
	end := now.AddDate(0, 0, 1).Format("2006-01-02")

	unfilteredRec := doRequest(t, h, "GetReservationCoverage", map[string]any{
		"TimePeriod":  map[string]string{"Start": start, "End": end},
		"Granularity": "DAILY",
	})
	require.Equal(t, http.StatusOK, unfilteredRec.Code)

	var unfiltered struct {
		Total struct {
			CoverageCost struct {
				OnDemandCost string `json:"OnDemandCost"`
			} `json:"CoverageCost"`
		} `json:"Total"`
	}
	require.NoError(t, json.NewDecoder(unfilteredRec.Body).Decode(&unfiltered))
	require.NotEqual(t, "0.0000", unfiltered.Total.CoverageCost.OnDemandCost)

	filteredRec := doRequest(t, h, "GetReservationCoverage", map[string]any{
		"TimePeriod":  map[string]string{"Start": start, "End": end},
		"Granularity": "DAILY",
		"Filter": map[string]any{
			"Dimensions": map[string]any{"Key": "SERVICE", "Values": []string{"NonexistentService"}},
		},
	})
	require.Equal(t, http.StatusOK, filteredRec.Code)

	var filtered struct {
		Total struct {
			CoverageCost struct {
				OnDemandCost string `json:"OnDemandCost"`
			} `json:"CoverageCost"`
		} `json:"Total"`
	}
	require.NoError(t, json.NewDecoder(filteredRec.Body).Decode(&filtered))
	assert.Equal(t, "0.0000", filtered.Total.CoverageCost.OnDemandCost)
}

// TestGetReservationUtilizationSortByTimeReorders proves SortBy genuinely
// reorders a multi-item UtilizationsByTime result.
func TestGetReservationUtilizationSortByTimeReorders(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	descBody := map[string]any{
		"TimePeriod":  map[string]string{"Start": "2024-01-01", "End": "2024-01-04"},
		"Granularity": "DAILY",
		"SortBy":      map[string]any{"Key": "Time", "SortOrder": "DESCENDING"},
	}
	descRec := doRequest(t, h, "GetReservationUtilization", descBody)
	require.Equal(t, http.StatusOK, descRec.Code)

	var desc struct {
		UtilizationsByTime []struct {
			TimePeriod map[string]string `json:"TimePeriod"`
		} `json:"UtilizationsByTime"`
	}
	require.NoError(t, json.NewDecoder(descRec.Body).Decode(&desc))
	require.Len(t, desc.UtilizationsByTime, 3)
	assert.Equal(t, "2024-01-03", desc.UtilizationsByTime[0].TimePeriod["Start"])
	assert.Equal(t, "2024-01-01", desc.UtilizationsByTime[2].TimePeriod["Start"])
}

// TestGetCostComparisonDriversFilterAccepted documents that Filter is
// accepted on the wire but has no effect: this emulator never computes
// comparison drivers, so CostComparisonDrivers is always empty regardless of
// any filter.
func TestGetCostComparisonDriversFilterAccepted(t *testing.T) {
	t.Parallel()

	h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))

	rec := doRequest(t, h, "GetCostComparisonDrivers", map[string]any{
		"BaselineTimePeriod":   map[string]string{"Start": "2024-01-01", "End": "2024-02-01"},
		"ComparisonTimePeriod": map[string]string{"Start": "2024-02-01", "End": "2024-03-01"},
		"MetricForComparison":  "UnblendedCost",
		"Filter": map[string]any{
			"Dimensions": map[string]any{"Key": "SERVICE", "Values": []string{"AWS Lambda"}},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		CostComparisonDrivers []any `json:"CostComparisonDrivers"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.Empty(t, out.CostComparisonDrivers)
}
