package ce_test

import (
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	costexplorersdk "github.com/aws/aws-sdk-go-v2/service/costexplorer"
	cetypes "github.com/aws/aws-sdk-go-v2/service/costexplorer/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListCommitmentPurchaseAnalyses_AnalysisIDs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		pick    []int
		unknown bool
		want    int
	}{
		{name: "one_id", pick: []int{1}, want: 1},
		{name: "two_ids", pick: []int{0, 2}, want: 2},
		{name: "unknown_id", unknown: true, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestCEClient(t, newTestHandler(t))
			cfg := &cetypes.CommitmentPurchaseAnalysisConfiguration{
				SavingsPlansPurchaseAnalysisConfiguration: &cetypes.SavingsPlansPurchaseAnalysisConfiguration{
					AnalysisType: cetypes.AnalysisTypeMaxSavings,
					LookBackTimePeriod: &cetypes.DateInterval{
						Start: aws.String("2024-01-01"), End: aws.String("2024-02-01"),
					},
					SavingsPlansToAdd: []cetypes.SavingsPlans{
						{SavingsPlansType: cetypes.SupportedSavingsPlansTypeComputeSp},
					},
				},
			}

			ids := make([]string, 0, 3)

			for range 3 {
				out, err := client.StartCommitmentPurchaseAnalysis(t.Context(),
					&costexplorersdk.StartCommitmentPurchaseAnalysisInput{CommitmentPurchaseAnalysisConfiguration: cfg})
				require.NoError(t, err)

				ids = append(ids, aws.ToString(out.AnalysisId))
			}

			want := []string{"no-such-analysis"}
			if !tt.unknown {
				want = nil
				for _, i := range tt.pick {
					want = append(want, ids[i])
				}
			}

			out, err := client.ListCommitmentPurchaseAnalyses(t.Context(),
				&costexplorersdk.ListCommitmentPurchaseAnalysesInput{AnalysisIds: want})
			require.NoError(t, err)
			require.Len(t, out.AnalysisSummaryList, tt.want)

			if !tt.unknown {
				for _, s := range out.AnalysisSummaryList {
					assert.Contains(t, want, aws.ToString(s.AnalysisId))
				}
			}
		})
	}
}

func TestGetReservationPurchaseRecommendation_AccountAndSpecification(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		accountID string
		class     cetypes.OfferingClass
		wantClass string
		wantRecs  bool
	}{
		{name: "own_account", accountID: "000000000000", wantRecs: true, wantClass: "STANDARD"},
		{name: "other_account", accountID: "999999999999"},
		{name: "convertible", class: cetypes.OfferingClassConvertible, wantRecs: true, wantClass: "CONVERTIBLE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestCEClient(t, newTestHandler(t))
			in := &costexplorersdk.GetReservationPurchaseRecommendationInput{
				Service: aws.String("Amazon Elastic Compute Cloud - Compute"),
			}

			if tt.accountID != "" {
				in.AccountId = aws.String(tt.accountID)
			}

			if tt.class != "" {
				in.ServiceSpecification = &cetypes.ServiceSpecification{
					EC2Specification: &cetypes.EC2Specification{OfferingClass: tt.class},
				}
			}

			out, err := client.GetReservationPurchaseRecommendation(t.Context(), in)
			require.NoError(t, err)

			if !tt.wantRecs {
				assert.Empty(t, out.Recommendations)

				return
			}

			require.NotEmpty(t, out.Recommendations)
			assert.Equal(t, tt.wantClass,
				string(out.Recommendations[0].ServiceSpecification.EC2Specification.OfferingClass))
		})
	}
}

func TestGetForecast_RequiredMembers(t *testing.T) {
	t.Parallel()

	full := map[string]any{
		"TimePeriod":  map[string]string{"Start": "2024-02-01", "End": "2024-03-01"},
		"Metric":      "BLENDED_COST",
		"Granularity": "MONTHLY",
	}

	tests := []struct {
		name     string
		drop     string
		wantCode int
	}{
		{name: "complete", wantCode: http.StatusOK},
		{name: "no_time_period", drop: "TimePeriod", wantCode: http.StatusBadRequest},
		{name: "no_metric", drop: "Metric", wantCode: http.StatusBadRequest},
		{name: "no_granularity", drop: "Granularity", wantCode: http.StatusBadRequest},
	}

	for _, action := range []string{"GetCostForecast", "GetUsageForecast"} {
		for _, tt := range tests {
			t.Run(action+"_"+tt.name, func(t *testing.T) {
				t.Parallel()

				body := map[string]any{}
				for k, v := range full {
					if k != tt.drop {
						body[k] = v
					}
				}

				rec := doRequest(t, newTestHandler(t), action, body)
				assert.Equal(t, tt.wantCode, rec.Code)
			})
		}
	}
}
