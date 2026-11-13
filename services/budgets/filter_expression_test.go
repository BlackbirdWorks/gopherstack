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

type exprSpend struct {
	got   *budgets.Expression
	mu    sync.Mutex
	plain int
}

func (e *exprSpend) Spend(_, _ time.Time, _ map[string][]string, _ string) (float64, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.plain++

	return 1, true
}

func (e *exprSpend) SpendExpression(
	_, _ time.Time, expr *budgets.Expression, _ map[string][]string, _ string,
) (float64, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.got = expr

	return 42, true
}

func TestBudgetFilterExpressionReachesSpendSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		expr      *types.Expression
		name      string
		wantSpend string
	}{
		{
			name: "or",
			expr: &types.Expression{Or: []types.Expression{
				{Dimensions: &types.ExpressionDimensionValues{Key: types.DimensionService, Values: []string{"a"}}},
				{Not: &types.Expression{Tags: &types.TagValues{Key: aws.String("k"), Values: []string{"v"}}}},
			}},
			wantSpend: "42",
		},
		{name: "no expression", wantSpend: "1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			src := &exprSpend{}
			f.backend.SetSpendSource(src)

			bg := costBudget("b1", "100")
			bg.FilterExpression = tt.expr

			_, err := f.client.CreateBudget(t.Context(), &bsdk.CreateBudgetInput{
				AccountId: aws.String(testAccount), Budget: bg,
			})
			require.NoError(t, err)

			got, err := f.client.DescribeBudget(t.Context(), &bsdk.DescribeBudgetInput{
				AccountId: aws.String(testAccount), BudgetName: aws.String("b1"),
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantSpend, aws.ToString(got.Budget.CalculatedSpend.ActualSpend.Amount))

			if tt.expr != nil {
				require.NotNil(t, src.got)
				assert.Len(t, src.got.Or, 2)
			}
		})
	}
}
