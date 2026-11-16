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

type fakeCommitments struct {
	gotStart, gotEnd time.Time
	gotType          string
	pct              float64
	ok               bool
}

func (f *fakeCommitments) Commitment(start, end time.Time, budgetType string, _ map[string][]string) (float64, bool) {
	f.gotStart, f.gotEnd, f.gotType = start, end, budgetType

	return f.pct, f.ok
}

func TestReservationBudgetsReportCommitmentPercentage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		budgetType types.BudgetType
		source     *fakeCommitments
		want       string
	}{
		{
			name:       "utilization",
			budgetType: types.BudgetTypeRIUtilization,
			source:     &fakeCommitments{pct: 62.5, ok: true},
			want:       "62.5",
		},
		{
			name:       "coverage",
			budgetType: types.BudgetTypeRICoverage,
			source:     &fakeCommitments{pct: 80, ok: true},
			want:       "80",
		},
		{name: "no reservations", budgetType: types.BudgetTypeRIUtilization, source: &fakeCommitments{}, want: "0"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			f.backend.SetCommitmentSource(tc.source)

			_, err := f.client.CreateBudget(t.Context(), &bsdk.CreateBudgetInput{
				AccountId: aws.String(testAccount),
				Budget: &types.Budget{
					BudgetName:  aws.String("ri"),
					BudgetType:  tc.budgetType,
					TimeUnit:    types.TimeUnitMonthly,
					BudgetLimit: &types.Spend{Amount: aws.String("90"), Unit: aws.String("PERCENTAGE")},
				},
			})
			require.NoError(t, err)

			got, err := f.client.DescribeBudget(t.Context(), &bsdk.DescribeBudgetInput{
				AccountId: aws.String(testAccount), BudgetName: aws.String("ri"),
			})
			require.NoError(t, err)

			actual := got.Budget.CalculatedSpend.ActualSpend
			assert.Equal(t, tc.want, aws.ToString(actual.Amount))
			assert.Equal(t, "PERCENTAGE", aws.ToString(actual.Unit))
			assert.Equal(t, string(tc.budgetType), tc.source.gotType)
			assert.Equal(t, time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC), tc.source.gotStart)
			assert.Equal(t, time.Date(2026, 3, 15, 12, 0, 0, 0, time.UTC), tc.source.gotEnd, "clamped to now")
		})
	}
}
