package budgets_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	bsdk "github.com/aws/aws-sdk-go-v2/service/budgets"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNextTokenExpiry(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 3, 15, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		list func(ctx context.Context, f *fixture, token *string) (*string, error)
		name string
	}{
		{
			name: "describe budgets",
			list: func(ctx context.Context, f *fixture, token *string) (*string, error) {
				out, err := f.client.DescribeBudgets(ctx, &bsdk.DescribeBudgetsInput{
					AccountId: aws.String(testAccount), MaxResults: aws.Int32(1), NextToken: token,
				})
				if err != nil {
					return nil, err
				}

				return out.NextToken, nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			ctx := t.Context()

			for _, n := range []string{"a", "b", "c"} {
				createBudget(t, f, n, "10")
			}

			token, err := tt.list(ctx, f, nil)
			require.NoError(t, err)
			require.NotNil(t, token)

			f.backend.SetClock(func() time.Time { return start.Add(23 * time.Hour) })

			next, err := tt.list(ctx, f, token)
			require.NoError(t, err)
			assert.NotNil(t, next)

			f.backend.SetClock(func() time.Time { return start.Add(25 * time.Hour) })

			_, err = tt.list(ctx, f, token)
			assert.Equal(t, "ExpiredNextTokenException", apiCode(err))

			_, err = tt.list(ctx, f, aws.String("%%%"))
			assert.Equal(t, "InvalidNextTokenException", apiCode(err))
		})
	}
}
