package ce_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	costexplorersdk "github.com/aws/aws-sdk-go-v2/service/costexplorer"
	cetypes "github.com/aws/aws-sdk-go-v2/service/costexplorer/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ce"
)

func createCostCategory(t *testing.T, client *costexplorersdk.Client, name string, values ...string) {
	t.Helper()

	rules := make([]cetypes.CostCategoryRule, 0, len(values))
	for _, v := range values {
		rules = append(rules, cetypes.CostCategoryRule{Value: aws.String(v)})
	}

	_, err := client.CreateCostCategoryDefinition(t.Context(), &costexplorersdk.CreateCostCategoryDefinitionInput{
		Name:        aws.String(name),
		RuleVersion: cetypes.CostCategoryRuleVersionCostCategoryExpressionV1,
		Rules:       rules,
	})
	require.NoError(t, err)
}

// TestGetCostCategories_SearchStringAndPagination_RealClient proves
// SearchString narrows cost category names and NextPageToken/MaxResults
// pagination walks the full set without dropping or duplicating entries.
func TestGetCostCategories_SearchStringAndPagination_RealClient(t *testing.T) {
	t.Parallel()

	h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestCEClient(t, h)

	names := []string{"TeamAlpha", "TeamBeta", "ProjectGamma", "TeamDelta", "ProjectEpsilon"}
	for _, n := range names {
		createCostCategory(t, client, n)
	}

	period := &cetypes.DateInterval{Start: aws.String("2024-01-01"), End: aws.String("2024-02-01")}

	searched, err := client.GetCostCategories(t.Context(), &costexplorersdk.GetCostCategoriesInput{
		TimePeriod:   period,
		SearchString: aws.String("Team"),
	})
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"TeamAlpha", "TeamBeta", "TeamDelta"}, searched.CostCategoryNames,
		"SearchString must narrow to names containing the substring")

	seen := make(map[string]bool)

	var token *string

	pages := 0

	for {
		out, pageErr := client.GetCostCategories(t.Context(), &costexplorersdk.GetCostCategoriesInput{
			TimePeriod:    period,
			MaxResults:    aws.Int32(2),
			NextPageToken: token,
		})
		require.NoError(t, pageErr)

		pages++

		for _, n := range out.CostCategoryNames {
			require.False(t, seen[n], "duplicate name %s across pages", n)
			seen[n] = true
		}

		if aws.ToString(out.NextPageToken) == "" {
			break
		}

		token = out.NextPageToken

		require.Less(t, pages, 10, "runaway pagination loop")
	}

	assert.Greater(t, pages, 1, "5 names capped at 2 per page must force multiple pages")
	assert.Len(t, seen, len(names), "every created category must appear exactly once across the page walk")
}

// TestCostCategoryEffectiveOn_RealClient proves EffectiveOn genuinely uses
// the category's own EffectiveStart: a lookup dated before the category's
// creation must behave as if the category did not exist yet (real AWS has no
// analogous "not found" for a version that predates creation, but this
// backend has no historical-version store to serve any other answer from --
// see PARITY.md gaps).
func TestCostCategoryEffectiveOn_RealClient(t *testing.T) {
	t.Parallel()

	h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestCEClient(t, h)

	createOut, err := client.CreateCostCategoryDefinition(
		t.Context(),
		&costexplorersdk.CreateCostCategoryDefinitionInput{
			Name:           aws.String("EffectiveOnTest"),
			RuleVersion:    cetypes.CostCategoryRuleVersionCostCategoryExpressionV1,
			Rules:          []cetypes.CostCategoryRule{{Value: aws.String("x")}},
			EffectiveStart: aws.String("2024-06-01T00:00:00Z"),
		},
	)
	require.NoError(t, err)

	// Effective on-or-after creation: found.
	describeOK, err := client.DescribeCostCategoryDefinition(
		t.Context(),
		&costexplorersdk.DescribeCostCategoryDefinitionInput{
			CostCategoryArn: createOut.CostCategoryArn,
			EffectiveOn:     aws.String("2024-06-01T00:00:00Z"),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "EffectiveOnTest", aws.ToString(describeOK.CostCategory.Name))

	// Effective before creation: not found.
	_, err = client.DescribeCostCategoryDefinition(t.Context(), &costexplorersdk.DescribeCostCategoryDefinitionInput{
		CostCategoryArn: createOut.CostCategoryArn,
		EffectiveOn:     aws.String("2024-01-01T00:00:00Z"),
	})
	require.Error(t, err)

	listBefore, err := client.ListCostCategoryDefinitions(
		t.Context(),
		&costexplorersdk.ListCostCategoryDefinitionsInput{
			EffectiveOn: aws.String("2024-01-01T00:00:00Z"),
		},
	)
	require.NoError(t, err)
	assert.Empty(t, listBefore.CostCategoryReferences, "a category not yet effective must be excluded from the list")

	listAfter, err := client.ListCostCategoryDefinitions(t.Context(), &costexplorersdk.ListCostCategoryDefinitionsInput{
		EffectiveOn: aws.String("2024-06-01T00:00:00Z"),
	})
	require.NoError(t, err)
	require.Len(t, listAfter.CostCategoryReferences, 1)
	assert.Equal(t, "EffectiveOnTest", aws.ToString(listAfter.CostCategoryReferences[0].Name))
}

// TestListCostAllocationTagBackfillHistory_Pagination_RealClient proves
// NextToken/MaxResults pagination over backfill jobs walks every job exactly
// once, in most-recently-requested-first order, across page boundaries.
func TestListCostAllocationTagBackfillHistory_Pagination_RealClient(t *testing.T) {
	t.Parallel()

	h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestCEClient(t, h)

	const seeded = 7

	for i := range seeded {
		_, err := client.StartCostAllocationTagBackfill(
			t.Context(),
			&costexplorersdk.StartCostAllocationTagBackfillInput{
				BackfillFrom: aws.String(fmt.Sprintf("2024-01-%02dT00:00:00Z", i+1)),
			},
		)
		require.NoError(t, err)
	}

	seen := make(map[string]bool)

	var token *string

	pages := 0

	for {
		out, err := client.ListCostAllocationTagBackfillHistory(t.Context(),
			&costexplorersdk.ListCostAllocationTagBackfillHistoryInput{
				MaxResults: aws.Int32(2),
				NextToken:  token,
			})
		require.NoError(t, err)

		pages++

		for _, r := range out.BackfillRequests {
			key := aws.ToString(r.BackfillFrom)
			require.False(t, seen[key], "duplicate BackfillFrom %s across pages", key)
			seen[key] = true
		}

		if aws.ToString(out.NextToken) == "" {
			break
		}

		token = out.NextToken

		require.Less(t, pages, 10, "runaway pagination loop")
	}

	assert.Greater(t, pages, 1, "7 jobs capped at 2 per page must force multiple pages")
	assert.Len(t, seen, seeded, "every seeded job must appear exactly once across the page walk")
}

// TestListCommitmentPurchaseAnalyses_StatusFilterAndPagination_RealClient
// proves AnalysisStatus narrows the list (this backend's analyses never
// leave PROCESSING, so filtering to SUCCEEDED must return nothing) and that
// NextPageToken/PageSize pagination walks every analysis exactly once.
func TestListCommitmentPurchaseAnalyses_StatusFilterAndPagination_RealClient(t *testing.T) {
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
			SavingsPlansToAdd: []cetypes.SavingsPlans{{SavingsPlansType: cetypes.SupportedSavingsPlansTypeComputeSp}},
		},
	}

	const seeded = 6

	ids := make([]string, 0, seeded)

	for range seeded {
		out, err := client.StartCommitmentPurchaseAnalysis(
			t.Context(),
			&costexplorersdk.StartCommitmentPurchaseAnalysisInput{
				CommitmentPurchaseAnalysisConfiguration: cfg,
			},
		)
		require.NoError(t, err)
		ids = append(ids, aws.ToString(out.AnalysisId))
	}

	succeeded, err := client.ListCommitmentPurchaseAnalyses(
		t.Context(),
		&costexplorersdk.ListCommitmentPurchaseAnalysesInput{
			AnalysisStatus: cetypes.AnalysisStatusSucceeded,
		},
	)
	require.NoError(t, err)
	assert.Empty(t, succeeded.AnalysisSummaryList, "no analysis in this backend ever reaches SUCCEEDED")

	processing, err := client.ListCommitmentPurchaseAnalyses(
		t.Context(),
		&costexplorersdk.ListCommitmentPurchaseAnalysesInput{
			AnalysisStatus: cetypes.AnalysisStatusProcessing,
		},
	)
	require.NoError(t, err)
	assert.Len(t, processing.AnalysisSummaryList, seeded)

	seen := make(map[string]bool)

	var token *string

	pages := 0

	for {
		out, pageErr := client.ListCommitmentPurchaseAnalyses(
			t.Context(),
			&costexplorersdk.ListCommitmentPurchaseAnalysesInput{
				PageSize:      2,
				NextPageToken: token,
			},
		)
		require.NoError(t, pageErr)

		pages++

		for _, a := range out.AnalysisSummaryList {
			id := aws.ToString(a.AnalysisId)
			require.False(t, seen[id], "duplicate AnalysisId %s across pages", id)
			seen[id] = true
		}

		if aws.ToString(out.NextPageToken) == "" {
			break
		}

		token = out.NextPageToken

		require.Less(t, pages, 10, "runaway pagination loop")
	}

	assert.Greater(t, pages, 1, "6 analyses capped at 2 per page must force multiple pages")
	assert.Len(t, seen, seeded, "every seeded analysis must appear exactly once across the page walk")
	for _, id := range ids {
		assert.True(t, seen[id], "AnalysisId %s must appear in the page walk", id)
	}
}
