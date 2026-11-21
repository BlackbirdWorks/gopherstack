package budgets_test

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	bsdk "github.com/aws/aws-sdk-go-v2/service/budgets"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/services/budgets"
)

func TestBillingViewResolutionIsLogged(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		arn     string
		wantLog bool
		wantErr bool
	}{
		{name: "valid", arn: "arn:aws:billing::000000000000:billingview/custom-abc", wantLog: true},
		{name: "none"},
		{name: "malformed", arn: "billingview/x", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)

			var buf bytes.Buffer

			ctx := logger.Save(t.Context(), slog.New(slog.NewTextHandler(&buf, nil)))
			bg := budgets.Budget{
				BudgetName: "bv", BudgetType: "COST", TimeUnit: "MONTHLY", BillingViewArn: tt.arn,
				BudgetLimit: &budgets.Spend{Amount: "100", Unit: "USD"},
			}

			err := f.backend.CreateBudget(ctx, testAccount, bg, nil, nil)
			if tt.wantErr {
				require.Error(t, err)
				assert.NotContains(t, buf.String(), "would resolve billing view")

				return
			}

			require.NoError(t, err)

			d, derr := f.client.DescribeBudget(t.Context(),
				&bsdk.DescribeBudgetInput{AccountId: aws.String(testAccount), BudgetName: aws.String("bv")})
			require.NoError(t, derr)

			if tt.wantLog {
				assert.Contains(t, buf.String(), "would resolve billing view")
				assert.Contains(t, buf.String(), tt.arn)
				assert.Equal(t, "HEALTHY", string(d.Budget.HealthStatus.Status))
			} else {
				assert.NotContains(t, buf.String(), "would resolve billing view")
				assert.Nil(t, d.Budget.HealthStatus)
			}
		})
	}
}
