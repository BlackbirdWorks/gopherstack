package budgets_test

import (
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	bsdk "github.com/aws/aws-sdk-go-v2/service/budgets"
	"github.com/aws/aws-sdk-go-v2/service/budgets/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/budgets"
)

type windowSpend struct {
	byMonth map[int]float64
}

func (w windowSpend) Spend(start, _ time.Time, _ map[string][]string, _ string) (float64, bool) {
	return w.byMonth[int(start.Month())], true
}

type recordingSpend struct {
	last map[string][]string
	mu   sync.Mutex
}

func (r *recordingSpend) Spend(_, _ time.Time, filters map[string][]string, _ string) (float64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.last = filters

	return 0, true
}

func TestAutoAdjustRecomputesAtRollover(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.backend.SetSpendSource(windowSpend{byMonth: map[int]float64{
		int(time.February): 100, int(time.March): 300, int(time.April): 500,
	}})

	ctx := t.Context()
	acct := aws.String(testAccount)
	bg := costBudget("auto", "1")
	bg.BudgetLimit = nil
	bg.AutoAdjustData = &types.AutoAdjustData{
		AutoAdjustType:    types.AutoAdjustTypeHistorical,
		HistoricalOptions: &types.HistoricalOptions{BudgetAdjustmentPeriod: aws.Int32(1)},
	}

	_, err := f.client.CreateBudget(ctx, &bsdk.CreateBudgetInput{AccountId: acct, Budget: bg})
	require.NoError(t, err)

	limit := func() string {
		got, derr := f.client.DescribeBudget(
			ctx,
			&bsdk.DescribeBudgetInput{AccountId: acct, BudgetName: aws.String("auto")},
		)
		require.NoError(t, derr)

		return aws.ToString(got.Budget.BudgetLimit.Amount)
	}

	assert.Equal(t, "100", limit())

	f.backend.SetClock(func() time.Time { return time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC) })
	f.backend.EvaluateAll(ctx)
	assert.Equal(t, "100", limit())

	f.backend.SetClock(func() time.Time { return time.Date(2026, 4, 2, 0, 0, 0, 0, time.UTC) })
	f.backend.EvaluateAll(ctx)
	assert.Equal(t, "300", limit())

	f.backend.SetClock(func() time.Time { return time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC) })
	f.backend.EvaluateAll(ctx)
	assert.Equal(t, "500", limit())
}

func TestCostTypesExcludeChargeTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		types *budgets.CostTypes
		want  []string
	}{
		{name: "defaults", types: nil},
		{name: "tax", types: &budgets.CostTypes{IncludeTax: new(false)}, want: []string{"Tax"}},
		{
			name: "several",
			types: &budgets.CostTypes{
				IncludeCredit: new(false), IncludeSupport: new(false), IncludeUpfront: new(false),
			},
			want: []string{"Credit", "Support", "Upfront", "SavingsPlanUpfrontFee"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			rec := &recordingSpend{}
			f.backend.SetSpendSource(rec)

			bg := budgets.Budget{
				BudgetName: "ct", BudgetType: "COST", TimeUnit: "MONTHLY",
				BudgetLimit: &budgets.Spend{Amount: "100", Unit: "USD"}, CostTypes: tc.types,
			}
			require.NoError(t, f.backend.CreateBudget(t.Context(), testAccount, bg, nil, nil))
			f.backend.EvaluateAll(t.Context())

			rec.mu.Lock()
			defer rec.mu.Unlock()

			assert.ElementsMatch(t, tc.want, rec.last[budgets.ExcludeChargeTypeFilter])
		})
	}
}
