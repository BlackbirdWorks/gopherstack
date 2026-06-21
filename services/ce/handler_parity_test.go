package ce_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ce"
)

// --- GetCostAndUsage parity ---

func TestParity_GetCostAndUsage_ReturnsResultsByTime(t *testing.T) {
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

func TestParity_GetCostAndUsage_GroupByProducesGroups(t *testing.T) {
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

func TestParity_GetCostAndUsage_TotalPresentWhenNoGroupBy(t *testing.T) {
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

// --- GetDimensionValues parity ---

func TestParity_GetDimensionValues_ServiceDimension(t *testing.T) {
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

func TestParity_GetDimensionValues_SearchStringFilters(t *testing.T) {
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

// --- GetCostForecast parity ---

func TestParity_GetCostForecast_ReturnsTimeSeries(t *testing.T) {
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

// --- GetSavingsPlansUtilization parity ---

func TestParity_GetSavingsPlansUtilization_ReturnsTotalAndByTime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
	}{
		{
			name: "monthly_aggregated",
			body: map[string]any{
				"TimePeriod":  map[string]string{"Start": "2024-01-01", "End": "2024-03-01"},
				"Granularity": "MONTHLY",
			},
		},
		{
			name: "daily_aggregated",
			body: map[string]any{
				"TimePeriod":  map[string]string{"Start": "2024-01-01", "End": "2024-01-08"},
				"Granularity": "DAILY",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "GetSavingsPlansUtilization", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				Total struct {
					Utilization struct {
						TotalCommitment       string `json:"TotalCommitment"`
						UsedCommitment        string `json:"UsedCommitment"`
						UnusedCommitment      string `json:"UnusedCommitment"`
						UtilizationPercentage string `json:"UtilizationPercentage"`
					} `json:"Utilization"`
					Savings struct {
						NetSavings             string `json:"NetSavings"`
						OnDemandCostEquivalent string `json:"OnDemandCostEquivalent"`
					} `json:"Savings"`
				} `json:"Total"`
				SavingsPlansUtilizationsByTime []any `json:"SavingsPlansUtilizationsByTime"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

			assert.NotEmpty(t, out.Total.Utilization.TotalCommitment)
			assert.NotEmpty(t, out.Total.Utilization.UsedCommitment)
			assert.NotEmpty(t, out.Total.Savings.NetSavings)
			assert.NotEmpty(t, out.SavingsPlansUtilizationsByTime)
		})
	}
}

// --- GetSavingsPlansUtilizationDetails parity ---

func TestParity_GetSavingsPlansUtilizationDetails_ReturnsDetails(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "GetSavingsPlansUtilizationDetails", map[string]any{
		"TimePeriod": map[string]string{"Start": "2026-03-01", "End": "2026-04-01"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Total                          map[string]any    `json:"Total"`
		TimePeriod                     map[string]string `json:"TimePeriod"`
		SavingsPlansUtilizationDetails []struct {
			SavingsPlanArn string `json:"SavingsPlanArn"`
			Utilization    struct {
				UtilizationPercentage string `json:"UtilizationPercentage"`
			} `json:"Utilization"`
		} `json:"SavingsPlansUtilizationDetails"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.NotEmpty(t, out.SavingsPlansUtilizationDetails)
	assert.NotNil(t, out.Total)
	assert.NotEmpty(t, out.TimePeriod)

	for _, d := range out.SavingsPlansUtilizationDetails {
		assert.NotEmpty(t, d.SavingsPlanArn)
		assert.NotEmpty(t, d.Utilization.UtilizationPercentage)
	}
}

// --- GetReservationCoverage parity ---

func TestParity_GetReservationCoverage_Structure(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "GetReservationCoverage", map[string]any{
		"TimePeriod":  map[string]string{"Start": "2024-01-01", "End": "2024-02-01"},
		"Granularity": "MONTHLY",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Total           map[string]any `json:"Total"`
		CoveragesByTime []struct {
			TimePeriod map[string]string `json:"TimePeriod"`
			Total      struct {
				CoverageHours struct {
					CoverageHoursPercentage string `json:"CoverageHoursPercentage"`
					ReservedHours           string `json:"ReservedHours"`
					TotalRunningHours       string `json:"TotalRunningHours"`
				} `json:"CoverageHours"`
			} `json:"Total"`
		} `json:"CoveragesByTime"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	require.NotEmpty(t, out.CoveragesByTime)
	assert.NotNil(t, out.Total)

	first := out.CoveragesByTime[0]
	assert.NotEmpty(t, first.TimePeriod["Start"])
	assert.Equal(t, "65.0000", first.Total.CoverageHours.CoverageHoursPercentage)
}

// --- GetReservationUtilization parity ---

func TestParity_GetReservationUtilization_Structure(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "GetReservationUtilization", map[string]any{
		"TimePeriod":  map[string]string{"Start": "2024-01-01", "End": "2024-02-01"},
		"Granularity": "MONTHLY",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Total              map[string]any `json:"Total"`
		UtilizationsByTime []struct {
			TimePeriod map[string]string `json:"TimePeriod"`
			Total      struct {
				UtilizationPercentage string `json:"UtilizationPercentage"`
				NetRISavings          string `json:"NetRISavings"`
			} `json:"Total"`
		} `json:"UtilizationsByTime"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	require.NotEmpty(t, out.UtilizationsByTime)
	assert.NotNil(t, out.Total)

	first := out.UtilizationsByTime[0]
	assert.Equal(t, "88.0000", first.Total.UtilizationPercentage)
	assert.NotEmpty(t, first.Total.NetRISavings)
}

// --- GetReservationPurchaseRecommendation parity ---

func TestParity_GetReservationPurchaseRecommendation_FieldShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
	}{
		{
			name: "ec2_one_year_no_upfront",
			body: map[string]any{
				"Service":              "Amazon Elastic Compute Cloud - Compute",
				"LookbackPeriodInDays": "THIRTY_DAYS",
				"TermInYears":          "ONE_YEAR",
				"PaymentOption":        "NO_UPFRONT",
			},
		},
		{
			name: "ec2_three_year_all_upfront",
			body: map[string]any{
				"Service":              "Amazon Elastic Compute Cloud - Compute",
				"LookbackPeriodInDays": "SIXTY_DAYS",
				"TermInYears":          "THREE_YEARS",
				"PaymentOption":        "ALL_UPFRONT",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "GetReservationPurchaseRecommendation", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				Metadata        map[string]string `json:"Metadata"`
				Recommendations []struct {
					RecommendationDetails []struct {
						EstimatedMonthlySavingsAmount          string `json:"EstimatedMonthlySavingsAmount"`
						EstimatedMonthlySavingsPercentage      string `json:"EstimatedMonthlySavingsPercentage"`
						RecommendedNumberOfInstancesToPurchase string `json:"RecommendedNumberOfInstancesToPurchase"`
					} `json:"RecommendationDetails"`
				} `json:"Recommendations"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			require.NotEmpty(t, out.Recommendations)
			require.NotEmpty(t, out.Recommendations[0].RecommendationDetails)

			detail := out.Recommendations[0].RecommendationDetails[0]
			assert.NotEmpty(t, detail.EstimatedMonthlySavingsAmount)
			assert.NotEmpty(t, detail.RecommendedNumberOfInstancesToPurchase)
		})
	}
}

// --- GetRightsizingRecommendation parity ---

func TestParity_GetRightsizingRecommendation_FieldShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "GetRightsizingRecommendation", map[string]any{
		"Service": "AmazonEC2",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Summary                    map[string]string `json:"Summary"`
		RightsizingRecommendations []struct {
			AccountID       string `json:"AccountId"`
			RightsizingType string `json:"RightsizingType"`
			CurrentInstance struct {
				ResourceID  string `json:"ResourceId"`
				MonthlyCost string `json:"MonthlyCost"`
			} `json:"CurrentInstance"`
			ModifyRecommendationDetail struct {
				TargetInstances []struct {
					EstimatedMonthlySavings string `json:"EstimatedMonthlySavings"`
					DefaultTargetInstance   bool   `json:"DefaultTargetInstance"`
				} `json:"TargetInstances"`
			} `json:"ModifyRecommendationDetail"`
		} `json:"RightsizingRecommendations"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	require.NotEmpty(t, out.RightsizingRecommendations)
	assert.NotNil(t, out.Summary)

	rec0 := out.RightsizingRecommendations[0]
	assert.NotEmpty(t, rec0.AccountID)
	assert.Equal(t, "MODIFY", rec0.RightsizingType)
	assert.NotEmpty(t, rec0.CurrentInstance.ResourceID)
	require.NotEmpty(t, rec0.ModifyRecommendationDetail.TargetInstances)
	assert.True(t, rec0.ModifyRecommendationDetail.TargetInstances[0].DefaultTargetInstance)
}

// --- CostAllocationTags parity ---

func TestParity_CostAllocationTags_UpdateAndList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		updateBody     map[string]any
		filterStatus   string
		wantTagCount   int
		wantStatusCode int
	}{
		{
			name: "activate_two_tags_list_active",
			updateBody: map[string]any{
				"CostAllocationTagsStatus": []map[string]string{
					{"TagKey": "Env", "Status": "Active"},
					{"TagKey": "Team", "Status": "Active"},
				},
			},
			filterStatus:   "Active",
			wantTagCount:   2,
			wantStatusCode: http.StatusOK,
		},
		{
			name: "deactivate_one_tag",
			updateBody: map[string]any{
				"CostAllocationTagsStatus": []map[string]string{
					{"TagKey": "Project", "Status": "Inactive"},
				},
			},
			filterStatus:   "Inactive",
			wantTagCount:   1,
			wantStatusCode: http.StatusOK,
		},
		{
			name: "invalid_status_returns_errors",
			updateBody: map[string]any{
				"CostAllocationTagsStatus": []map[string]string{
					{"TagKey": "BadTag", "Status": "Enabled"},
				},
			},
			wantStatusCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "UpdateCostAllocationTagsStatus", tt.updateBody)
			assert.Equal(t, tt.wantStatusCode, rec.Code)

			if tt.filterStatus == "" {
				return
			}

			listRec := doRequest(t, h, "ListCostAllocationTags", map[string]any{
				"Status": tt.filterStatus,
			})
			require.Equal(t, http.StatusOK, listRec.Code)

			var listOut struct {
				CostAllocationTags []struct {
					TagKey string `json:"TagKey"`
					Status string `json:"Status"`
					Type   string `json:"Type"`
				} `json:"CostAllocationTags"`
			}
			require.NoError(t, json.NewDecoder(listRec.Body).Decode(&listOut))
			assert.Len(t, listOut.CostAllocationTags, tt.wantTagCount)

			for _, tag := range listOut.CostAllocationTags {
				assert.Equal(t, tt.filterStatus, tag.Status)
				assert.NotEmpty(t, tag.TagKey)
			}
		})
	}
}

func TestParity_CostAllocationTags_ListFiltersWork(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create several tags
	doRequest(t, h, "UpdateCostAllocationTagsStatus", map[string]any{
		"CostAllocationTagsStatus": []map[string]string{
			{"TagKey": "Alpha", "Status": "Active"},
			{"TagKey": "Beta", "Status": "Active"},
			{"TagKey": "Gamma", "Status": "Inactive"},
		},
	})

	t.Run("filter_by_tag_keys", func(t *testing.T) {
		t.Parallel()

		rec := doRequest(t, h, "ListCostAllocationTags", map[string]any{
			"TagKeys": []string{"Alpha", "Beta"},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			CostAllocationTags []struct {
				TagKey string `json:"TagKey"`
			} `json:"CostAllocationTags"`
		}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
		assert.Len(t, out.CostAllocationTags, 2)
	})

	t.Run("filter_by_status_active", func(t *testing.T) {
		t.Parallel()

		rec := doRequest(t, h, "ListCostAllocationTags", map[string]any{
			"Status": "Active",
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			CostAllocationTags []struct {
				TagKey string `json:"TagKey"`
				Status string `json:"Status"`
			} `json:"CostAllocationTags"`
		}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
		assert.Len(t, out.CostAllocationTags, 2)

		for _, tag := range out.CostAllocationTags {
			assert.Equal(t, "Active", tag.Status)
		}
	})
}

// --- Backfill parity ---

func TestParity_CostAllocationTagBackfill_StateTracking(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Start backfill
	rec := doRequest(t, h, "StartCostAllocationTagBackfill", map[string]any{
		"BackfillFrom": "2024-01-01T00:00:00Z",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var startOut struct {
		BackfillRequest struct {
			BackfillFrom   string `json:"backfillFrom"`
			BackfillStatus string `json:"backfillStatus"`
			RequestedAt    string `json:"requestedAt"`
		} `json:"BackfillRequest"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&startOut))
	assert.Equal(t, "2024-01-01T00:00:00Z", startOut.BackfillRequest.BackfillFrom)
	assert.Equal(t, "PROCESSING", startOut.BackfillRequest.BackfillStatus)

	// List history
	listRec := doRequest(t, h, "ListCostAllocationTagBackfillHistory", map[string]any{})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut struct {
		BackfillRequests []struct {
			BackfillStatus string `json:"backfillStatus"`
		} `json:"BackfillRequests"`
	}
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&listOut))
	require.Len(t, listOut.BackfillRequests, 1)
	assert.Equal(t, "PROCESSING", listOut.BackfillRequests[0].BackfillStatus)
}

func TestParity_CostAllocationTagBackfill_MissingFromReturns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "StartCostAllocationTagBackfill", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- CommitmentPurchaseAnalysis parity ---

func TestParity_CommitmentPurchaseAnalysis_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Start analysis
	rec := doRequest(t, h, "StartCommitmentPurchaseAnalysis", map[string]any{
		"CommitmentPurchaseAnalysisConfiguration": map[string]any{
			"SavingsPlansPurchaseAnalysisConfiguration": map[string]string{
				"SavingsPlansType": "COMPUTE_SP",
				"TermInYears":      "ONE_YEAR",
				"PaymentOption":    "NO_UPFRONT",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var startOut struct {
		AnalysisID              string `json:"AnalysisId"`
		AnalysisStartedTime     string `json:"AnalysisStartedTime"`
		EstimatedCompletionTime string `json:"EstimatedCompletionTime"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&startOut))
	require.NotEmpty(t, startOut.AnalysisID)
	assert.NotEmpty(t, startOut.AnalysisStartedTime)
	assert.NotEmpty(t, startOut.EstimatedCompletionTime)

	// Get analysis
	getRec := doRequest(t, h, "GetCommitmentPurchaseAnalysis", map[string]any{
		"AnalysisId": startOut.AnalysisID,
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut struct {
		AnalysisID     string `json:"AnalysisId"`
		AnalysisStatus string `json:"AnalysisStatus"`
	}
	require.NoError(t, json.NewDecoder(getRec.Body).Decode(&getOut))
	assert.Equal(t, startOut.AnalysisID, getOut.AnalysisID)
	assert.Equal(t, "PROCESSING", getOut.AnalysisStatus)

	// List analyses
	listRec := doRequest(t, h, "ListCommitmentPurchaseAnalyses", map[string]any{})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut struct {
		AnalysisSummaryList []struct {
			AnalysisID string `json:"analysisId"`
		} `json:"AnalysisSummaryList"`
	}
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&listOut))
	require.Len(t, listOut.AnalysisSummaryList, 1)
}

// --- ProvideAnomalyFeedback parity ---

func TestParity_ProvideAnomalyFeedback_PersistsAndValidates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		feedback       string
		wantFeedback   string
		wantStatusCode int
	}{
		{
			name:           "yes_feedback",
			feedback:       "YES",
			wantStatusCode: http.StatusOK,
			wantFeedback:   "YES",
		},
		{
			name:           "no_feedback",
			feedback:       "NO",
			wantStatusCode: http.StatusOK,
			wantFeedback:   "NO",
		},
		{
			name:           "planned_activity_feedback",
			feedback:       "PLANNED_ACTIVITY",
			wantStatusCode: http.StatusOK,
			wantFeedback:   "PLANNED_ACTIVITY",
		},
		{
			name:           "invalid_feedback_returns_400",
			feedback:       "MAYBE",
			wantStatusCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			h.Backend.AddAnomaly(ce.Anomaly{
				AnomalyID:  "feedback-test-1",
				MonitorARN: "arn:aws:ce::000:anomalymonitor/test",
			})

			rec := doRequest(t, h, "ProvideAnomalyFeedback", map[string]any{
				"AnomalyId": "feedback-test-1",
				"Feedback":  tt.feedback,
			})
			assert.Equal(t, tt.wantStatusCode, rec.Code)

			if tt.wantFeedback == "" {
				return
			}

			// Verify persisted
			getRec := doRequest(t, h, "GetAnomalies", map[string]any{
				"Feedback": tt.wantFeedback,
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
			assert.Equal(t, "feedback-test-1", out.Anomalies[0].AnomalyID)
			assert.Equal(t, tt.wantFeedback, out.Anomalies[0].Feedback)
		})
	}
}

func TestParity_ProvideAnomalyFeedback_NotFoundReturns404(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ProvideAnomalyFeedback", map[string]any{
		"AnomalyId": "nonexistent-anomaly",
		"Feedback":  "YES",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestParity_ProvideAnomalyFeedback_MissingIDReturns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ProvideAnomalyFeedback", map[string]any{
		"Feedback": "YES",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- AnomalyScore and Impact struct parity ---

func TestParity_GetAnomalies_ScoreAndImpactAreObjects(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	h.Backend.AddAnomaly(ce.Anomaly{
		AnomalyID:   "struct-test-1",
		MonitorARN:  "arn:aws:ce::000:anomalymonitor/test",
		TotalImpact: 500.0,
		AnomalyScore: ce.AnomalyScore{
			MaxScore:     0.95,
			CurrentScore: 0.87,
		},
	})

	rec := doRequest(t, h, "GetAnomalies", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Anomalies []struct {
			AnomalyScore struct {
				MaxScore     float64 `json:"MaxScore"`
				CurrentScore float64 `json:"CurrentScore"`
			} `json:"AnomalyScore"`
			Impact struct {
				MaxImpact        float64 `json:"MaxImpact"`
				TotalImpact      float64 `json:"TotalImpact"`
				TotalActualSpend float64 `json:"TotalActualSpend"`
			} `json:"Impact"`
		} `json:"Anomalies"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	require.Len(t, out.Anomalies, 1)

	a := out.Anomalies[0]
	assert.InDelta(t, 0.95, a.AnomalyScore.MaxScore, 0.001)
	assert.InDelta(t, 0.87, a.AnomalyScore.CurrentScore, 0.001)
	assert.Positive(t, a.Impact.MaxImpact)
	assert.Positive(t, a.Impact.TotalActualSpend)
}

// --- GetSavingsPlansPurchaseRecommendation parity ---

func TestParity_GetSavingsPlansPurchaseRecommendation_ReturnsRecommendation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
	}{
		{
			name: "compute_sp_one_year",
			body: map[string]any{
				"SavingsPlansType":     "COMPUTE_SP",
				"TermInYears":          "ONE_YEAR",
				"PaymentOption":        "NO_UPFRONT",
				"LookbackPeriodInDays": "THIRTY_DAYS",
			},
		},
		{
			name: "ec2_sp_three_year",
			body: map[string]any{
				"SavingsPlansType":     "EC2_INSTANCE_SP",
				"TermInYears":          "THREE_YEARS",
				"PaymentOption":        "PARTIAL_UPFRONT",
				"LookbackPeriodInDays": "SIXTY_DAYS",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "GetSavingsPlansPurchaseRecommendation", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				Metadata                           map[string]string `json:"Metadata"`
				SavingsPlansPurchaseRecommendation struct {
					SavingsPlansType                          string           `json:"SavingsPlansType"`
					SavingsPlansPurchaseRecommendationDetails []map[string]any `json:"SavingsPlansPurchaseRecommendationDetails"`
				} `json:"SavingsPlansPurchaseRecommendation"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.NotEmpty(t, out.SavingsPlansPurchaseRecommendation.SavingsPlansType)
			assert.NotEmpty(t, out.SavingsPlansPurchaseRecommendation.SavingsPlansPurchaseRecommendationDetails)
			assert.NotNil(t, out.Metadata)
		})
	}
}

// --- Snapshot/Restore parity for new state ---

func TestParity_SnapshotRestore_IncludesCommitmentAnalyses(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create an analysis
	rec := doRequest(t, h, "StartCommitmentPurchaseAnalysis", map[string]any{
		"CommitmentPurchaseAnalysisConfiguration": map[string]any{},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var startOut struct {
		AnalysisID string `json:"AnalysisId"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&startOut))

	// Snapshot + restore
	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))
	require.NoError(t, fresh.Restore(t.Context(), snap))

	// The restored handler should have the analysis
	getRec := doRequest(t, fresh, "GetCommitmentPurchaseAnalysis", map[string]any{
		"AnalysisId": startOut.AnalysisID,
	})
	assert.Equal(t, http.StatusOK, getRec.Code)
}
