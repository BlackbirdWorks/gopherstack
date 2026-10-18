package budgets_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	bsdk "github.com/aws/aws-sdk-go-v2/service/budgets"
	"github.com/aws/aws-sdk-go-v2/service/budgets/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func costBudget(name, amount string) *types.Budget {
	return &types.Budget{
		BudgetName:  aws.String(name),
		BudgetType:  types.BudgetTypeCost,
		TimeUnit:    types.TimeUnitMonthly,
		BudgetLimit: &types.Spend{Amount: aws.String(amount), Unit: aws.String("USD")},
	}
}

func createBudget(t *testing.T, f *fixture, name, amount string) {
	t.Helper()

	_, err := f.client.CreateBudget(t.Context(), &bsdk.CreateBudgetInput{
		AccountId: aws.String(testAccount), Budget: costBudget(name, amount),
	})
	require.NoError(t, err)
}

func TestBudgetLifecycle(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()
	acct := aws.String(testAccount)

	createBudget(t, f, "monthly", "100")

	got, err := f.client.DescribeBudget(
		ctx,
		&bsdk.DescribeBudgetInput{AccountId: acct, BudgetName: aws.String("monthly")},
	)
	require.NoError(t, err)

	b := got.Budget
	assert.Equal(t, "100", aws.ToString(b.BudgetLimit.Amount))
	assert.Equal(t, time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC), aws.ToTime(b.TimePeriod.Start))
	assert.Equal(t, 2087, aws.ToTime(b.TimePeriod.End).Year())
	assert.Equal(t, "0", aws.ToString(b.CalculatedSpend.ActualSpend.Amount))

	f.spend.set(40)

	got, err = f.client.DescribeBudget(
		ctx,
		&bsdk.DescribeBudgetInput{AccountId: acct, BudgetName: aws.String("monthly")},
	)
	require.NoError(t, err)
	assert.Equal(t, "40", aws.ToString(got.Budget.CalculatedSpend.ActualSpend.Amount))
	require.NotNil(t, got.Budget.CalculatedSpend.ForecastedSpend)

	updated := costBudget("monthly", "250")
	_, err = f.client.UpdateBudget(ctx, &bsdk.UpdateBudgetInput{AccountId: acct, NewBudget: updated})
	require.NoError(t, err)

	list, err := f.client.DescribeBudgets(ctx, &bsdk.DescribeBudgetsInput{AccountId: acct})
	require.NoError(t, err)
	require.Len(t, list.Budgets, 1)
	assert.Equal(t, "250", aws.ToString(list.Budgets[0].BudgetLimit.Amount))

	_, err = f.client.DeleteBudget(ctx, &bsdk.DeleteBudgetInput{AccountId: acct, BudgetName: aws.String("monthly")})
	require.NoError(t, err)

	_, err = f.client.DescribeBudget(ctx, &bsdk.DescribeBudgetInput{AccountId: acct, BudgetName: aws.String("monthly")})
	assert.Equal(t, "NotFoundException", apiCode(err))
}

func TestBudgetErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(b *types.Budget)
		name   string
		want   string
	}{
		{
			name:   "bad name",
			mutate: func(b *types.Budget) { b.BudgetName = aws.String("a:b") },
			want:   "InvalidParameterException",
		},
		{
			name:   "action substring",
			mutate: func(b *types.Budget) { b.BudgetName = aws.String("x/action/y") },
			want:   "InvalidParameterException",
		},
		{name: "no limit", mutate: func(b *types.Budget) { b.BudgetLimit = nil }, want: "InvalidParameterException"},
		{
			name:   "bad amount",
			mutate: func(b *types.Budget) { b.BudgetLimit.Amount = aws.String("-3") },
			want:   "InvalidParameterException",
		},
		{
			name: "two clauses",
			mutate: func(b *types.Budget) {
				b.FilterExpression = &types.Expression{
					Dimensions: &types.ExpressionDimensionValues{Key: types.DimensionService, Values: []string{"x"}},
					Tags:       &types.TagValues{Key: aws.String("k")},
				}
			},
			want: "InvalidParameterException",
		},
		{name: "planned on daily", mutate: func(b *types.Budget) {
			b.TimeUnit = types.TimeUnitDaily
			b.PlannedBudgetLimits = map[string]types.Spend{"0": {Amount: aws.String("1"), Unit: aws.String("USD")}}
		}, want: "InvalidParameterException"},
		{name: "ok", mutate: func(*types.Budget) {}, want: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			b := costBudget("b1", "10")
			tc.mutate(b)

			_, err := f.client.CreateBudget(
				t.Context(),
				&bsdk.CreateBudgetInput{AccountId: aws.String(testAccount), Budget: b},
			)
			assert.Equal(t, tc.want, apiCode(err))
		})
	}
}

func TestBudgetDuplicateAndAccount(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	createBudget(t, f, "dup", "10")

	_, err := f.client.CreateBudget(t.Context(), &bsdk.CreateBudgetInput{
		AccountId: aws.String(testAccount), Budget: costBudget("dup", "10"),
	})
	assert.Equal(t, "DuplicateRecordException", apiCode(err))

	_, err = f.client.CreateBudget(t.Context(), &bsdk.CreateBudgetInput{
		AccountId: aws.String("12"), Budget: costBudget("x", "10"),
	})
	assert.Equal(t, "InvalidParameterException", apiCode(err))
}

func TestBudgetPagination(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	for _, n := range []string{"a", "b", "c"} {
		createBudget(t, f, n, "10")
	}

	var names []string

	p := bsdk.NewDescribeBudgetsPaginator(f.client, &bsdk.DescribeBudgetsInput{
		AccountId: aws.String(testAccount), MaxResults: aws.Int32(2),
	})
	pages := 0

	for p.HasMorePages() {
		out, err := p.NextPage(t.Context())
		require.NoError(t, err)

		pages++

		for _, b := range out.Budgets {
			names = append(names, aws.ToString(b.BudgetName))
		}
	}

	assert.Equal(t, []string{"a", "b", "c"}, names)
	assert.Equal(t, 2, pages)

	_, err := f.client.DescribeBudgets(t.Context(), &bsdk.DescribeBudgetsInput{
		AccountId: aws.String(testAccount), NextToken: aws.String("%%%"),
	})
	assert.Equal(t, "InvalidNextTokenException", apiCode(err))
}

func TestPlannedAndAutoAdjust(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.spend.set(30)
	ctx := t.Context()
	acct := aws.String(testAccount)

	planned := costBudget("planned", "100")
	planned.PlannedBudgetLimits = map[string]types.Spend{"0": {Amount: aws.String("100"), Unit: aws.String("USD")}}
	_, err := f.client.CreateBudget(ctx, &bsdk.CreateBudgetInput{AccountId: acct, Budget: planned})
	require.NoError(t, err)

	auto := &types.Budget{
		BudgetName: aws.String("auto"), BudgetType: types.BudgetTypeCost, TimeUnit: types.TimeUnitMonthly,
		AutoAdjustData: &types.AutoAdjustData{
			AutoAdjustType:    types.AutoAdjustTypeHistorical,
			HistoricalOptions: &types.HistoricalOptions{BudgetAdjustmentPeriod: aws.Int32(2)},
		},
	}
	_, err = f.client.CreateBudget(ctx, &bsdk.CreateBudgetInput{AccountId: acct, Budget: auto})
	require.NoError(t, err)

	got, err := f.client.DescribeBudget(ctx, &bsdk.DescribeBudgetInput{AccountId: acct, BudgetName: aws.String("auto")})
	require.NoError(t, err)
	assert.Equal(t, "30", aws.ToString(got.Budget.BudgetLimit.Amount))
	assert.EqualValues(t, 2, aws.ToInt32(got.Budget.AutoAdjustData.HistoricalOptions.LookBackAvailablePeriods))
	assert.NotNil(t, got.Budget.AutoAdjustData.LastAutoAdjustTime)

	bad := *auto
	bad.BudgetName = aws.String("auto2")
	bad.AutoAdjustData = &types.AutoAdjustData{AutoAdjustType: types.AutoAdjustTypeHistorical}
	_, err = f.client.CreateBudget(ctx, &bsdk.CreateBudgetInput{AccountId: acct, Budget: &bad})
	assert.Equal(t, "InvalidParameterException", apiCode(err))
}

func TestPerformanceHistory(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.spend.set(12)
	createBudget(t, f, "perf", "50")

	out, err := f.client.DescribeBudgetPerformanceHistory(t.Context(), &bsdk.DescribeBudgetPerformanceHistoryInput{
		AccountId: aws.String(testAccount), BudgetName: aws.String("perf"),
	})
	require.NoError(t, err)

	list := out.BudgetPerformanceHistory.BudgetedAndActualAmountsList
	require.Len(t, list, 1)
	assert.Equal(t, "50", aws.ToString(list[0].BudgetedAmount.Amount))
	assert.Equal(t, "12", aws.ToString(list[0].ActualAmount.Amount))

	_, err = f.client.DescribeBudgetPerformanceHistory(t.Context(), &bsdk.DescribeBudgetPerformanceHistoryInput{
		AccountId: aws.String(testAccount), BudgetName: aws.String("missing"),
	})
	assert.Equal(t, "NotFoundException", apiCode(err))
}

func TestTags(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()

	_, err := f.client.CreateBudget(ctx, &bsdk.CreateBudgetInput{
		AccountId: aws.String(testAccount), Budget: costBudget("tagged", "5"),
		ResourceTags: []types.ResourceTag{{Key: aws.String("env"), Value: aws.String("dev")}},
	})
	require.NoError(t, err)

	arn := aws.String("arn:aws:budgets::" + testAccount + ":budget/tagged")

	_, err = f.client.TagResource(ctx, &bsdk.TagResourceInput{
		ResourceARN: arn, ResourceTags: []types.ResourceTag{{Key: aws.String("team"), Value: aws.String("x")}},
	})
	require.NoError(t, err)

	got, err := f.client.ListTagsForResource(ctx, &bsdk.ListTagsForResourceInput{ResourceARN: arn})
	require.NoError(t, err)
	assert.Len(t, got.ResourceTags, 2)

	_, err = f.client.UntagResource(ctx, &bsdk.UntagResourceInput{ResourceARN: arn, ResourceTagKeys: []string{"env"}})
	require.NoError(t, err)

	got, err = f.client.ListTagsForResource(ctx, &bsdk.ListTagsForResourceInput{ResourceARN: arn})
	require.NoError(t, err)
	require.Len(t, got.ResourceTags, 1)
	assert.Equal(t, "team", aws.ToString(got.ResourceTags[0].Key))

	_, err = f.client.ListTagsForResource(ctx, &bsdk.ListTagsForResourceInput{
		ResourceARN: aws.String("arn:aws:budgets::" + testAccount + ":budget/none"),
	})
	assert.Equal(t, "NotFoundException", apiCode(err))
}
