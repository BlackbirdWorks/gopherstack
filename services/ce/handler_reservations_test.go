package ce_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ce"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetReservationCoverage_DailyGranularity verifies daily coverage.
func TestGetReservationCoverage_DailyGranularity(t *testing.T) {
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

// TestGetReservationUtilization_DailyGranularity verifies daily RI utilization.
func TestGetReservationUtilization_DailyGranularity(t *testing.T) {
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

// TestCoverage_GetRightsizingRecommendation_EmptyForNoEC2 verifies empty result for
// a service that has no ledger data.
func TestGetRightsizingRecommendation_IncludesSummary(t *testing.T) {
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

func TestGetReservationCoverage_Structure(t *testing.T) {
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

func TestGetReservationUtilization_Structure(t *testing.T) {
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

func TestGetReservationPurchaseRecommendation_FieldShape(t *testing.T) {
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

func TestGetRightsizingRecommendation_FieldShape(t *testing.T) {
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

func TestHandler_GetReservationCoverage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body           map[string]any
		name           string
		wantStatusCode int
	}{
		{
			name: "returns_empty_coverage",
			body: map[string]any{
				"TimePeriod":  map[string]string{"Start": "2024-01-01", "End": "2024-02-01"},
				"Granularity": "MONTHLY",
			},
			wantStatusCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "GetReservationCoverage", tt.body)
			assert.Equal(t, tt.wantStatusCode, rec.Code)

			var out struct {
				CoveragesByTime []any `json:"CoveragesByTime"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.NotEmpty(t, out.CoveragesByTime)
		})
	}
}

func TestHandler_GetReservationPurchaseRecommendation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body           map[string]any
		name           string
		wantStatusCode int
	}{
		{
			name: "returns_recommendations_with_synthetic_data",
			body: map[string]any{
				"Service":              "Amazon Elastic Compute Cloud - Compute",
				"LookbackPeriodInDays": "SIXTY_DAYS",
				"TermInYears":          "ONE_YEAR",
				"PaymentOption":        "NO_UPFRONT",
			},
			wantStatusCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "GetReservationPurchaseRecommendation", tt.body)
			assert.Equal(t, tt.wantStatusCode, rec.Code)

			var out struct {
				Recommendations []any `json:"Recommendations"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.NotEmpty(t, out.Recommendations)
		})
	}
}

func TestHandler_GetReservationUtilization(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body           map[string]any
		name           string
		wantStatusCode int
	}{
		{
			name: "returns_utilization_with_synthetic_data",
			body: map[string]any{
				"TimePeriod":  map[string]string{"Start": "2024-01-01", "End": "2024-02-01"},
				"Granularity": "MONTHLY",
			},
			wantStatusCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "GetReservationUtilization", tt.body)
			assert.Equal(t, tt.wantStatusCode, rec.Code)

			var out struct {
				UtilizationsByTime []any `json:"UtilizationsByTime"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.NotEmpty(t, out.UtilizationsByTime)
		})
	}
}
