package ce_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	costexplorersdk "github.com/aws/aws-sdk-go-v2/service/costexplorer"
	cetypes "github.com/aws/aws-sdk-go-v2/service/costexplorer/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ce"
)

// TestGetSavingsPlansCoverage_Pagination_RealClient proves
// GetSavingsPlansCoverage now buckets by Granularity (a prior revision always
// returned exactly one entry regardless of the requested time range) and that
// MaxResults/NextToken pagination over those buckets is real.
func TestGetSavingsPlansCoverage_Pagination_RealClient(t *testing.T) {
	t.Parallel()

	h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestCEClient(t, h)

	end := time.Now().UTC().Truncate(24 * time.Hour)
	start := end.AddDate(0, 0, -10)
	period := &cetypes.DateInterval{
		Start: aws.String(start.Format("2006-01-02")),
		End:   aws.String(end.Format("2006-01-02")),
	}

	out, err := client.GetSavingsPlansCoverage(t.Context(), &costexplorersdk.GetSavingsPlansCoverageInput{
		TimePeriod:  period,
		Granularity: cetypes.GranularityDaily,
		MaxResults:  aws.Int32(3),
	})
	require.NoError(t, err)
	assert.Len(t, out.SavingsPlansCoverages, 3, "MaxResults=3 must cap the page at 3 of the 10 daily buckets")
	require.NotEmpty(t, aws.ToString(out.NextToken), "a 10-bucket range capped to 3 per page must have a next page")

	out2, err := client.GetSavingsPlansCoverage(t.Context(), &costexplorersdk.GetSavingsPlansCoverageInput{
		TimePeriod:  period,
		Granularity: cetypes.GranularityDaily,
		MaxResults:  aws.Int32(3),
		NextToken:   out.NextToken,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, out2.SavingsPlansCoverages)
	assert.NotEqual(t,
		aws.ToString(out.SavingsPlansCoverages[0].TimePeriod.Start),
		aws.ToString(out2.SavingsPlansCoverages[0].TimePeriod.Start),
		"the second page must start after the first, not repeat it",
	)
}

// TestGetSavingsPlansUtilizationDetails_DataType_RealClient proves DataType
// (previously wire-declared as the fabricated field name "Fields", which
// matches no real GetSavingsPlansUtilizationDetailsInput member) genuinely
// selects which sections of each detail item are populated.
func TestGetSavingsPlansUtilizationDetails_DataType_RealClient(t *testing.T) {
	t.Parallel()

	h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestCEClient(t, h)

	period := &cetypes.DateInterval{Start: aws.String("2024-01-01"), End: aws.String("2024-02-01")}

	full, err := client.GetSavingsPlansUtilizationDetails(
		t.Context(),
		&costexplorersdk.GetSavingsPlansUtilizationDetailsInput{
			TimePeriod: period,
		},
	)
	require.NoError(t, err)
	require.Len(t, full.SavingsPlansUtilizationDetails, 1)
	assert.NotNil(t, full.SavingsPlansUtilizationDetails[0].Utilization)
	assert.NotNil(t, full.SavingsPlansUtilizationDetails[0].Savings)
	assert.NotNil(t, full.SavingsPlansUtilizationDetails[0].AmortizedCommitment)

	attrsOnly, err := client.GetSavingsPlansUtilizationDetails(
		t.Context(),
		&costexplorersdk.GetSavingsPlansUtilizationDetailsInput{
			TimePeriod: period,
			DataType:   []cetypes.SavingsPlansDataType{cetypes.SavingsPlansDataTypeAttributes},
		},
	)
	require.NoError(t, err)
	require.Len(t, attrsOnly.SavingsPlansUtilizationDetails, 1)
	d := attrsOnly.SavingsPlansUtilizationDetails[0]
	assert.NotEmpty(t, d.Attributes, "requested ATTRIBUTES must still be populated")
	assert.Nil(t, d.Utilization, "un-requested Utilization must be omitted")
	assert.Nil(t, d.Savings, "un-requested Savings must be omitted")
	assert.Nil(t, d.AmortizedCommitment, "un-requested AmortizedCommitment must be omitted")
}

// TestGetSavingsPlansUtilization_SortBy_RealClient proves SortBy genuinely
// reorders the per-bucket SavingsPlansUtilizationsByTime list by a numeric
// metric that varies per bucket (NetSavings, derived from that bucket's
// ledger total), rather than being silently dropped.
func TestGetSavingsPlansUtilization_SortBy_RealClient(t *testing.T) {
	t.Parallel()

	h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestCEClient(t, h)

	end := time.Now().UTC().Truncate(24 * time.Hour)
	start := end.AddDate(0, 0, -14)
	period := &cetypes.DateInterval{
		Start: aws.String(start.Format("2006-01-02")),
		End:   aws.String(end.Format("2006-01-02")),
	}

	out, err := client.GetSavingsPlansUtilization(t.Context(), &costexplorersdk.GetSavingsPlansUtilizationInput{
		TimePeriod:  period,
		Granularity: cetypes.GranularityDaily,
		SortBy:      &cetypes.SortDefinition{Key: aws.String("NetSavings"), SortOrder: cetypes.SortOrderDescending},
	})
	require.NoError(t, err)
	require.Greater(t, len(out.SavingsPlansUtilizationsByTime), 2, "need multiple buckets to prove a real reorder")

	for i := 1; i < len(out.SavingsPlansUtilizationsByTime); i++ {
		prev := aws.ToString(out.SavingsPlansUtilizationsByTime[i-1].Savings.NetSavings)
		cur := aws.ToString(out.SavingsPlansUtilizationsByTime[i].Savings.NetSavings)
		assert.GreaterOrEqual(t, prev, cur, "DESCENDING NetSavings sort must be honored across all buckets")
	}
}

// TestListSavingsPlansPurchaseRecommendationGeneration_RecommendationIDs_RealClient
// proves RecommendationIds narrows the list to the requested generation jobs
// instead of being parsed off the wire and discarded.
func TestListSavingsPlansPurchaseRecommendationGeneration_RecommendationIDs_RealClient(t *testing.T) {
	t.Parallel()

	h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestCEClient(t, h)

	const seeded = 3

	ids := make([]string, 0, seeded)

	for range seeded {
		out, err := client.StartSavingsPlansPurchaseRecommendationGeneration(
			t.Context(), &costexplorersdk.StartSavingsPlansPurchaseRecommendationGenerationInput{},
		)
		require.NoError(t, err)
		ids = append(ids, aws.ToString(out.RecommendationId))
	}

	listOut, err := client.ListSavingsPlansPurchaseRecommendationGeneration(
		t.Context(),
		&costexplorersdk.ListSavingsPlansPurchaseRecommendationGenerationInput{
			RecommendationIds: []string{ids[1]},
		},
	)
	require.NoError(t, err)
	require.Len(t, listOut.GenerationSummaryList, 1)
	assert.Equal(t, ids[1], aws.ToString(listOut.GenerationSummaryList[0].RecommendationId))
}
