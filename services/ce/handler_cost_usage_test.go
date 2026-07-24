package ce_test

import (
	"encoding/json"
	"maps"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ce"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetCostAndUsageWithResources_ResourceIDs verifies the stub returns valid empty shape.
func TestGetCostAndUsageWithResources_ResourceIDs(t *testing.T) {
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

// TestGetApproximateUsageRecords_Shape verifies approximate usage records response.
func TestGetApproximateUsageRecords_Shape(t *testing.T) {
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
				"Services":               []string{"Amazon Elastic Compute Cloud - Compute"},
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
				Services     map[string]int64 `json:"Services"`
				TotalRecords int64            `json:"TotalRecords"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.NotNil(t, out.Services)
			assert.Positive(t, out.TotalRecords)
		})
	}
}

// TestGetCostAndUsageComparisons_Shape verifies the real AWS wire shape
// (CostAndUsageComparisons/BaselineTimePeriod/MetricForComparison, not the previously
// invented CostAndUsages/BaseTimePeriod/Metrics fields) and that the comparison amounts
// are derived from the synthetic cost ledger rather than always-empty.
func TestGetCostAndUsageComparisons_Shape(t *testing.T) {
	t.Parallel()

	h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))
	rec := doRequest(t, h, "GetCostAndUsageComparisons", map[string]any{
		"BaselineTimePeriod":   map[string]string{"Start": "2024-01-01", "End": "2024-02-01"},
		"ComparisonTimePeriod": map[string]string{"Start": "2023-01-01", "End": "2023-02-01"},
		"MetricForComparison":  "BlendedCost",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		TotalCostAndUsage       map[string]any `json:"TotalCostAndUsage"`
		CostAndUsageComparisons []struct {
			Metrics map[string]struct {
				BaselineTimePeriodAmount   string `json:"BaselineTimePeriodAmount"`
				ComparisonTimePeriodAmount string `json:"ComparisonTimePeriodAmount"`
				Difference                 string `json:"Difference"`
			} `json:"Metrics"`
		} `json:"CostAndUsageComparisons"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	require.NotEmpty(t, out.CostAndUsageComparisons)
	assert.NotEmpty(t, out.TotalCostAndUsage["BlendedCost"])
	assert.NotEmpty(t, out.CostAndUsageComparisons[0].Metrics["BlendedCost"].ComparisonTimePeriodAmount)
}

// TestGetCostAndUsageComparisons_RequiredFields verifies BaselineTimePeriod,
// ComparisonTimePeriod, and MetricForComparison are enforced as required, matching real
// AWS CE's validateOpGetCostAndUsageComparisonsInput.
func TestGetCostAndUsageComparisons_RequiredFields(t *testing.T) {
	t.Parallel()

	full := map[string]any{
		"BaselineTimePeriod":   map[string]string{"Start": "2024-01-01", "End": "2024-02-01"},
		"ComparisonTimePeriod": map[string]string{"Start": "2023-01-01", "End": "2023-02-01"},
		"MetricForComparison":  "BlendedCost",
	}

	tests := []struct {
		mutate         func(body map[string]any)
		name           string
		wantStatusCode int
	}{
		{
			name:           "missing_baseline_time_period",
			mutate:         func(b map[string]any) { delete(b, "BaselineTimePeriod") },
			wantStatusCode: http.StatusBadRequest,
		},
		{
			name:           "missing_comparison_time_period",
			mutate:         func(b map[string]any) { delete(b, "ComparisonTimePeriod") },
			wantStatusCode: http.StatusBadRequest,
		},
		{
			name:           "missing_metric_for_comparison",
			mutate:         func(b map[string]any) { delete(b, "MetricForComparison") },
			wantStatusCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			body := make(map[string]any, len(full))
			maps.Copy(body, full)
			tt.mutate(body)

			h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))
			rec := doRequest(t, h, "GetCostAndUsageComparisons", body)
			assert.Equal(t, tt.wantStatusCode, rec.Code)
		})
	}
}

// TestGetCostComparisonDrivers_Shape verifies comparison drivers stub.
func TestGetCostComparisonDrivers_Shape(t *testing.T) {
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

// TestGetUsageForecast_MatchesCostForecast verifies same shape as cost forecast.
func TestGetUsageForecast_MatchesCostForecast(t *testing.T) {
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

// TestGetTags_EmptyKeyReturnsAllKeys verifies tag key enumeration.
func TestGetTags_EmptyKeyReturnsAllKeys(t *testing.T) {
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

// TestGetTags_WithTagKey exercises GetTagValues backend path.
func TestGetTags_WithTagKey(t *testing.T) {
	t.Parallel()

	h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))
	rec := doRequest(t, h, "GetTags", map[string]any{
		"TimePeriod": map[string]string{"Start": "2024-01-01", "End": "2024-02-01"},
		"TagKey":     "Environment",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Tags []string `json:"Tags"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.NotNil(t, out.Tags)
}

// TestGetCostAndUsage_GroupByDimensions exercises extractGroupKeys branches.
func TestGetCostAndUsage_GroupByDimensions(t *testing.T) {
	t.Parallel()

	dimensions := []string{"REGION", "USAGE_TYPE", "LINKED_ACCOUNT", "TAG$Env"}

	for _, dim := range dimensions {
		t.Run(dim, func(t *testing.T) {
			t.Parallel()

			h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))
			rec := doRequest(t, h, "GetCostAndUsage", map[string]any{
				"TimePeriod":  map[string]string{"Start": "2026-03-01", "End": "2026-04-01"},
				"Granularity": "MONTHLY",
				"Metrics":     []string{"BlendedCost"},
				"GroupBy": []map[string]string{
					{"Type": "DIMENSION", "Key": dim},
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				ResultsByTime []map[string]any `json:"ResultsByTime"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.NotNil(t, out.ResultsByTime)
		})
	}
}

// TestGetCostAndUsage_AlternateMetrics exercises getMetricValue branches.
func TestGetCostAndUsage_AlternateMetrics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		metric string
	}{
		{name: "unblended_cost", metric: "UnblendedCost"},
		{name: "amortized_cost", metric: "AmortizedCost"},
		{name: "net_amortized_cost", metric: "NetAmortizedCost"},
		{name: "net_unblended_cost", metric: "NetUnblendedCost"},
		{name: "usage_quantity", metric: "UsageQuantity"},
		{name: "normalized_usage", metric: "NormalizedUsageAmount"},
		{name: "unknown_metric", metric: "UnknownMetric"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))
			rec := doRequest(t, h, "GetCostAndUsage", map[string]any{
				"TimePeriod":  map[string]string{"Start": "2026-03-01", "End": "2026-04-01"},
				"Granularity": "MONTHLY",
				"Metrics":     []string{tt.metric},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				ResultsByTime []map[string]any `json:"ResultsByTime"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.NotEmpty(t, out.ResultsByTime)
		})
	}
}

func TestGetCostAndUsage_ReturnsResultsByTime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body         map[string]any
		name         string
		wantStatCode int
		wantNonEmpty bool
	}{
		{
			name: "daily_granularity",
			body: map[string]any{
				"TimePeriod":  map[string]string{"Start": "2024-01-01", "End": "2024-01-08"},
				"Granularity": "DAILY",
				"Metrics":     []string{"BlendedCost"},
			},
			wantStatCode: http.StatusOK,
			wantNonEmpty: true,
		},
		{
			name: "monthly_granularity",
			body: map[string]any{
				"TimePeriod":  map[string]string{"Start": "2024-01-01", "End": "2024-03-01"},
				"Granularity": "MONTHLY",
				"Metrics":     []string{"BlendedCost", "UnblendedCost"},
			},
			wantStatCode: http.StatusOK,
			wantNonEmpty: true,
		},
		{
			name: "group_by_service",
			body: map[string]any{
				"TimePeriod":  map[string]string{"Start": "2024-01-01", "End": "2024-02-01"},
				"Granularity": "MONTHLY",
				"Metrics":     []string{"BlendedCost"},
				"GroupBy":     []map[string]string{{"Type": "DIMENSION", "Key": "SERVICE"}},
			},
			wantStatCode: http.StatusOK,
			wantNonEmpty: true,
		},
		{
			name: "group_by_region",
			body: map[string]any{
				"TimePeriod":  map[string]string{"Start": "2024-01-01", "End": "2024-02-01"},
				"Granularity": "MONTHLY",
				"Metrics":     []string{"BlendedCost"},
				"GroupBy":     []map[string]string{{"Type": "DIMENSION", "Key": "REGION"}},
			},
			wantStatCode: http.StatusOK,
			wantNonEmpty: true,
		},
		{
			name: "no_time_period_uses_defaults",
			body: map[string]any{
				"Granularity": "MONTHLY",
				"Metrics":     []string{"BlendedCost"},
			},
			wantStatCode: http.StatusOK,
			wantNonEmpty: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "GetCostAndUsage", tt.body)
			assert.Equal(t, tt.wantStatCode, rec.Code)

			var out struct {
				ResultsByTime []any `json:"ResultsByTime"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

			if tt.wantNonEmpty {
				assert.NotEmpty(t, out.ResultsByTime)
			}
		})
	}
}

func TestGetCostAndUsage_GroupByProducesGroups(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "GetCostAndUsage", map[string]any{
		"TimePeriod":  map[string]string{"Start": "2026-03-01", "End": "2026-04-01"},
		"Granularity": "MONTHLY",
		"Metrics":     []string{"BlendedCost"},
		"GroupBy":     []map[string]string{{"Type": "DIMENSION", "Key": "SERVICE"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		ResultsByTime []struct {
			TimePeriod map[string]string `json:"TimePeriod"`
			Groups     []struct {
				Metrics map[string]map[string]string `json:"Metrics"`
				Keys    []string                     `json:"Keys"`
			} `json:"Groups"`
		} `json:"ResultsByTime"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	require.NotEmpty(t, out.ResultsByTime)

	first := out.ResultsByTime[0]
	assert.NotEmpty(t, first.Groups, "grouped result must have Groups")
	for _, g := range first.Groups {
		assert.NotEmpty(t, g.Keys, "each group must have Keys")
		assert.NotEmpty(t, g.Metrics, "each group must have Metrics")
	}
}

func TestGetCostAndUsage_TotalPresentWhenNoGroupBy(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "GetCostAndUsage", map[string]any{
		"TimePeriod":  map[string]string{"Start": "2024-01-01", "End": "2024-02-01"},
		"Granularity": "MONTHLY",
		"Metrics":     []string{"BlendedCost"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		ResultsByTime []struct {
			Total map[string]struct {
				Amount string `json:"Amount"`
				Unit   string `json:"Unit"`
			} `json:"Total"`
			Estimated bool `json:"Estimated"`
		} `json:"ResultsByTime"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	require.NotEmpty(t, out.ResultsByTime)

	first := out.ResultsByTime[0]
	bc, ok := first.Total["BlendedCost"]
	require.True(t, ok, "Total must contain BlendedCost metric")
	assert.NotEmpty(t, bc.Amount)
	assert.Equal(t, "USD", bc.Unit)
}

func TestGetDimensionValues_ServiceDimension(t *testing.T) {
	t.Parallel()

	tests := []struct {
		dimension string
		name      string
	}{
		{name: "service_dimension", dimension: "SERVICE"},
		{name: "region_dimension", dimension: "REGION"},
		{name: "usage_type_dimension", dimension: "USAGE_TYPE"},
		{name: "linked_account", dimension: "LINKED_ACCOUNT"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "GetDimensionValues", map[string]any{
				"Dimension":  tt.dimension,
				"TimePeriod": map[string]string{"Start": "2024-01-01", "End": "2024-02-01"},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				DimensionValues []struct {
					Value string `json:"Value"`
				} `json:"DimensionValues"`
				ReturnSize int `json:"ReturnSize"`
				TotalSize  int `json:"TotalSize"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.NotEmpty(t, out.DimensionValues)
			assert.Positive(t, out.ReturnSize)
			assert.Equal(t, out.ReturnSize, out.TotalSize)
		})
	}
}

func TestGetDimensionValues_SearchStringFilters(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Get all service dimension values
	recAll := doRequest(t, h, "GetDimensionValues", map[string]any{
		"Dimension":  "SERVICE",
		"TimePeriod": map[string]string{"Start": "2024-01-01", "End": "2024-02-01"},
	})
	require.Equal(t, http.StatusOK, recAll.Code)

	var allOut struct {
		DimensionValues []struct {
			Value string `json:"Value"`
		} `json:"DimensionValues"`
	}
	require.NoError(t, json.NewDecoder(recAll.Body).Decode(&allOut))
	totalCount := len(allOut.DimensionValues)

	// Filter by "Lambda"
	recFiltered := doRequest(t, h, "GetDimensionValues", map[string]any{
		"Dimension":    "SERVICE",
		"SearchString": "Lambda",
		"TimePeriod":   map[string]string{"Start": "2024-01-01", "End": "2024-02-01"},
	})
	require.Equal(t, http.StatusOK, recFiltered.Code)

	var filteredOut struct {
		DimensionValues []struct {
			Value string `json:"Value"`
		} `json:"DimensionValues"`
	}
	require.NoError(t, json.NewDecoder(recFiltered.Body).Decode(&filteredOut))
	assert.Less(t, len(filteredOut.DimensionValues), totalCount)
	for _, dv := range filteredOut.DimensionValues {
		assert.Contains(t, dv.Value, "Lambda")
	}
}

func TestGetCostForecast_ReturnsTimeSeries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
	}{
		{
			name: "monthly_forecast",
			body: map[string]any{
				"TimePeriod":  map[string]string{"Start": "2024-02-01", "End": "2024-05-01"},
				"Granularity": "MONTHLY",
				"Metric":      "BLENDED_COST",
			},
		},
		{
			name: "daily_forecast",
			body: map[string]any{
				"TimePeriod":  map[string]string{"Start": "2024-02-01", "End": "2024-02-15"},
				"Granularity": "DAILY",
				"Metric":      "BLENDED_COST",
			},
		},
		{
			name: "with_prediction_interval_level_95",
			body: map[string]any{
				"TimePeriod":              map[string]string{"Start": "2024-02-01", "End": "2024-03-01"},
				"Granularity":             "MONTHLY",
				"Metric":                  "BLENDED_COST",
				"PredictionIntervalLevel": 95,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "GetCostForecast", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				Total struct {
					MeanValue                    string `json:"MeanValue"`
					PredictionIntervalLowerBound string `json:"PredictionIntervalLowerBound"`
					PredictionIntervalUpperBound string `json:"PredictionIntervalUpperBound"`
				} `json:"Total"`
				ForecastResultsByTime []struct {
					TimePeriod map[string]string `json:"TimePeriod"`
					MeanValue  string            `json:"MeanValue"`
				} `json:"ForecastResultsByTime"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.NotEmpty(t, out.Total.MeanValue)
			assert.NotEmpty(t, out.Total.PredictionIntervalLowerBound)
			assert.NotEmpty(t, out.Total.PredictionIntervalUpperBound)
			assert.NotEmpty(t, out.ForecastResultsByTime)

			for _, fr := range out.ForecastResultsByTime {
				assert.NotEmpty(t, fr.TimePeriod["Start"])
				assert.NotEmpty(t, fr.TimePeriod["End"])
				assert.NotEmpty(t, fr.MeanValue)
			}
		})
	}
}

// TestGetCostAndUsage_GranularityRequired verifies real AWS requires Granularity.
func TestGetCostAndUsage_GranularityRequired(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "missing_granularity_returns_400",
			body: map[string]any{
				"TimePeriod": map[string]string{"Start": "2024-01-01", "End": "2024-02-01"},
				"Metrics":    []string{"BlendedCost"},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "with_granularity_returns_200",
			body: map[string]any{
				"TimePeriod":  map[string]string{"Start": "2024-01-01", "End": "2024-02-01"},
				"Granularity": "MONTHLY",
				"Metrics":     []string{"BlendedCost"},
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "GetCostAndUsage", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestGetDimensionValues_DimensionRequired verifies real AWS requires Dimension.
func TestGetDimensionValues_DimensionRequired(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "missing_dimension_returns_400",
			body: map[string]any{
				"TimePeriod": map[string]string{"Start": "2024-01-01", "End": "2024-02-01"},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "with_dimension_returns_200",
			body: map[string]any{
				"TimePeriod": map[string]string{"Start": "2024-01-01", "End": "2024-02-01"},
				"Dimension":  "SERVICE",
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "GetDimensionValues", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_CostQuery_ReturnsOK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   any
		name   string
		action string
	}{
		{
			name:   "get_cost_and_usage",
			action: "GetCostAndUsage",
			body: map[string]any{
				"TimePeriod":  map[string]string{"Start": "2024-01-01", "End": "2024-02-01"},
				"Granularity": "MONTHLY",
				"Metrics":     []string{"BlendedCost"},
			},
		},
		{
			name:   "get_dimension_values",
			action: "GetDimensionValues",
			body: map[string]any{
				"TimePeriod": map[string]string{"Start": "2024-01-01", "End": "2024-02-01"},
				"Dimension":  "SERVICE",
			},
		},
		{
			name:   "get_tags",
			action: "GetTags",
			body: map[string]any{
				"TimePeriod": map[string]string{"Start": "2024-01-01", "End": "2024-02-01"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.action, tt.body)
			assert.Equal(t, http.StatusOK, rec.Code)
		})
	}
}

func TestHandler_Forecast_ReturnsOK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   any
		name   string
		action string
	}{
		{
			name:   "get_cost_forecast",
			action: "GetCostForecast",
			body: map[string]any{
				"TimePeriod":  map[string]string{"Start": "2024-02-01", "End": "2024-03-01"},
				"Granularity": "MONTHLY",
				"Metric":      "BLENDED_COST",
			},
		},
		{
			name:   "get_usage_forecast",
			action: "GetUsageForecast",
			body: map[string]any{
				"TimePeriod":  map[string]string{"Start": "2024-02-01", "End": "2024-03-01"},
				"Granularity": "MONTHLY",
				"Metric":      "USAGE_QUANTITY",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.action, tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.NotNil(t, out["Total"])
			forecastResults, ok := out["ForecastResultsByTime"].([]any)
			require.True(t, ok)
			assert.NotEmpty(t, forecastResults)
		})
	}
}

func TestHandler_GetApproximateUsageRecords(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body           map[string]any
		name           string
		wantStatusCode int
	}{
		{
			name: "derives_from_cost_ledger",
			body: map[string]any{
				"ApproximationDimension": "SERVICE",
				"Granularity":            "MONTHLY",
			},
			wantStatusCode: http.StatusOK,
		},
		{
			name: "missing_approximation_dimension_returns_400",
			body: map[string]any{
				"Granularity": "MONTHLY",
			},
			wantStatusCode: http.StatusBadRequest,
		},
		{
			name: "missing_granularity_returns_400",
			body: map[string]any{
				"ApproximationDimension": "SERVICE",
			},
			wantStatusCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "GetApproximateUsageRecords", tt.body)
			assert.Equal(t, tt.wantStatusCode, rec.Code)

			if tt.wantStatusCode != http.StatusOK {
				return
			}

			var out struct {
				Services       map[string]int64  `json:"Services"`
				LookbackPeriod map[string]string `json:"LookbackPeriod"`
				TotalRecords   int64             `json:"TotalRecords"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.Positive(t, out.TotalRecords)
			assert.NotEmpty(t, out.LookbackPeriod["Start"])
			assert.NotEmpty(t, out.LookbackPeriod["End"])
		})
	}
}

func TestHandler_GetCostAndUsageComparisons(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body           map[string]any
		name           string
		wantStatusCode int
	}{
		{
			name: "returns_derived_comparisons",
			body: map[string]any{
				"BaselineTimePeriod":   map[string]string{"Start": "2024-01-01", "End": "2024-02-01"},
				"ComparisonTimePeriod": map[string]string{"Start": "2023-01-01", "End": "2023-02-01"},
				"MetricForComparison":  "BlendedCost",
			},
			wantStatusCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "GetCostAndUsageComparisons", tt.body)
			assert.Equal(t, tt.wantStatusCode, rec.Code)

			var out struct {
				CostAndUsageComparisons []any `json:"CostAndUsageComparisons"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.NotEmpty(t, out.CostAndUsageComparisons)
		})
	}
}

func TestHandler_GetCostAndUsageWithResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body           map[string]any
		name           string
		wantStatusCode int
	}{
		{
			name: "returns_empty_results",
			body: map[string]any{
				"TimePeriod":  map[string]string{"Start": "2024-01-01", "End": "2024-02-01"},
				"Granularity": "MONTHLY",
				"Metrics":     []string{"BlendedCost"},
				"Filter": map[string]any{
					"Dimensions": map[string]any{
						"Key":    "SERVICE",
						"Values": []string{"Amazon Elastic Compute Cloud - Compute"},
					},
				},
			},
			wantStatusCode: http.StatusOK,
		},
		{
			name: "missing_filter_returns_400",
			body: map[string]any{
				"TimePeriod":  map[string]string{"Start": "2024-01-01", "End": "2024-02-01"},
				"Granularity": "MONTHLY",
				"Metrics":     []string{"BlendedCost"},
			},
			wantStatusCode: http.StatusBadRequest,
		},
		{
			name: "missing_granularity_returns_400",
			body: map[string]any{
				"TimePeriod": map[string]string{"Start": "2024-01-01", "End": "2024-02-01"},
				"Metrics":    []string{"BlendedCost"},
				"Filter": map[string]any{
					"Dimensions": map[string]any{
						"Key":    "SERVICE",
						"Values": []string{"Amazon Elastic Compute Cloud - Compute"},
					},
				},
			},
			wantStatusCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "GetCostAndUsageWithResources", tt.body)
			assert.Equal(t, tt.wantStatusCode, rec.Code)

			if tt.wantStatusCode != http.StatusOK {
				return
			}

			var out struct {
				ResultsByTime []any `json:"ResultsByTime"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.Empty(t, out.ResultsByTime)
		})
	}
}

func TestHandler_GetCostComparisonDrivers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body           map[string]any
		name           string
		wantStatusCode int
	}{
		{
			name: "returns_empty_drivers",
			body: map[string]any{
				"BaselineTimePeriod":   map[string]string{"Start": "2023-01-01", "End": "2024-01-01"},
				"ComparisonTimePeriod": map[string]string{"Start": "2024-01-01", "End": "2025-01-01"},
				"Metric":               "BlendedCost",
			},
			wantStatusCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "GetCostComparisonDrivers", tt.body)
			assert.Equal(t, tt.wantStatusCode, rec.Code)

			var out struct {
				CostComparisonDrivers []any `json:"CostComparisonDrivers"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.Empty(t, out.CostComparisonDrivers)
		})
	}
}
