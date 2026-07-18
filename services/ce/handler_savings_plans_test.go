package ce_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/services/ce"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetSavingsPlanPurchaseRecommendationDetails verifies stub.
func TestGetSavingsPlanPurchaseRecommendationDetails(t *testing.T) {
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

// TestListSavingsPlansPurchaseRecommendationGeneration verifies generation list.
func TestListSavingsPlansPurchaseRecommendationGeneration(t *testing.T) {
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

// TestStartSavingsPlansPurchaseRecommendationGeneration verifies generation start.
func TestStartSavingsPlansPurchaseRecommendationGeneration(t *testing.T) {
	t.Parallel()

	h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))
	rec := doRequest(t, h, "StartSavingsPlansPurchaseRecommendationGeneration", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
}

// TestGetSavingsPlansCoverage verifies coverage response shape.
func TestGetSavingsPlansCoverage(t *testing.T) {
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

// TestGetSavingsPlansUtilization_DailyGranularity verifies daily SP utilization.
func TestGetSavingsPlansUtilization_DailyGranularity(t *testing.T) {
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

func TestCeRegion_SavingsPlansCoverageUsesCtxRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		region     string
		wantRegion string
	}{
		{name: "default region", region: "us-east-1", wantRegion: "us-east-1"},
		{name: "eu-west-1 cross-region", region: "eu-west-1", wantRegion: "eu-west-1"},
		{
			name:       "ap-southeast-2 cross-region",
			region:     "ap-southeast-2",
			wantRegion: "ap-southeast-2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", tt.region))
			meta := &awsmeta.Metadata{Account: "111122223333", Region: tt.region, Partition: "aws"}
			rec := doRequestWithMeta(t, h, meta, "GetSavingsPlansCoverage", map[string]any{
				"TimePeriod":  map[string]string{"Start": "2026-01-01", "End": "2026-02-01"},
				"Granularity": "MONTHLY",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				SavingsPlansCoverages []struct {
					Attributes map[string]string `json:"Attributes"`
				} `json:"SavingsPlansCoverages"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			require.NotEmpty(t, out.SavingsPlansCoverages)
			assert.Equal(t, tt.wantRegion, out.SavingsPlansCoverages[0].Attributes["Region"])
		})
	}
}

func TestCeRegion_SavingsPlansPurchaseRecommendationUsesCtxRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		region     string
		wantRegion string
	}{
		{name: "eu-west-2 cross-region", region: "eu-west-2", wantRegion: "eu-west-2"},
		{name: "us-west-2 cross-region", region: "us-west-2", wantRegion: "us-west-2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", tt.region))
			meta := &awsmeta.Metadata{Account: "222233334444", Region: tt.region, Partition: "aws"}
			rec := doRequestWithMeta(
				t,
				h,
				meta,
				"GetSavingsPlansPurchaseRecommendation",
				map[string]any{
					"SavingsPlansType":     "COMPUTE_SP",
					"TermInYears":          "ONE_YEAR",
					"PaymentOption":        "NO_UPFRONT",
					"LookbackPeriodInDays": "THIRTY_DAYS",
				},
			)
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				SavingsPlansPurchaseRecommendation struct {
					RecommendationDetails []map[string]any `json:"SavingsPlansPurchaseRecommendationDetails"`
				} `json:"SavingsPlansPurchaseRecommendation"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			details := out.SavingsPlansPurchaseRecommendation.RecommendationDetails
			require.NotEmpty(t, details)
			spDetails, ok := details[0]["SavingsPlansDetails"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.wantRegion, spDetails["Region"])
		})
	}
}

func TestGetSavingsPlansUtilization_ReturnsTotalAndByTime(t *testing.T) {
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

func TestGetSavingsPlansUtilizationDetails_ReturnsDetails(t *testing.T) {
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

func TestGetSavingsPlansPurchaseRecommendation_ReturnsRecommendation(t *testing.T) {
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
