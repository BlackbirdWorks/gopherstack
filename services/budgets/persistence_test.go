package budgets_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	bsdk "github.com/aws/aws-sdk-go-v2/service/budgets"
	"github.com/aws/aws-sdk-go-v2/service/budgets/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		corrupt bool
		want    int
	}{
		{name: "roundtrip", want: 1},
		{name: "bad version", corrupt: true, want: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			createBudget(t, f, "persist", "10")

			_, err := f.client.CreateBudgetAction(t.Context(), actionInput("persist", types.ApprovalModelManual, 80))
			require.NoError(t, err)

			snap := f.backend.Snapshot(t.Context())
			require.NotEmpty(t, snap)

			if tc.corrupt {
				snap = []byte(`{"version":99}`)
			}

			g := newFixture(t)
			require.NoError(t, g.backend.Restore(t.Context(), snap))

			list, err := g.client.DescribeBudgets(
				t.Context(),
				&bsdk.DescribeBudgetsInput{AccountId: aws.String(testAccount)},
			)
			require.NoError(t, err)
			assert.Len(t, list.Budgets, tc.want)

			if tc.want > 0 {
				acts, actsErr := g.client.DescribeBudgetActionsForAccount(
					t.Context(),
					&bsdk.DescribeBudgetActionsForAccountInput{
						AccountId: aws.String(testAccount),
					},
				)
				require.NoError(t, actsErr)
				assert.Len(t, acts.Actions, 1)
			}
		})
	}
}
