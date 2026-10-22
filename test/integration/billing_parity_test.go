package integration_test

import (
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/bcmdataexports"
	bcmtypes "github.com/aws/aws-sdk-go-v2/service/bcmdataexports/types"
	"github.com/aws/aws-sdk-go-v2/service/budgets"
	budgetstypes "github.com/aws/aws-sdk-go-v2/service/budgets/types"
	"github.com/aws/aws-sdk-go-v2/service/pricing"
	pricingtypes "github.com/aws/aws-sdk-go-v2/service/pricing/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func billingConfig(t *testing.T) aws.Config {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	return cfg
}

func TestIntegration_Pricing_ReservedTermsAndSavingsPlans(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	c := pricing.NewFromConfig(billingConfig(t), func(o *pricing.Options) { o.BaseEndpoint = aws.String(endpoint) })

	out, err := c.GetProducts(t.Context(), &pricing.GetProductsInput{
		ServiceCode: aws.String("AmazonSQS"),
		Filters: []pricingtypes.Filter{{
			Type: pricingtypes.FilterTypeTermMatch, Field: aws.String("regionCode"), Value: aws.String("eu-west-2"),
		}},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, out.PriceList)

	lists, err := c.ListPriceLists(t.Context(), &pricing.ListPriceListsInput{
		ServiceCode: aws.String("ComputeSavingsPlans"), CurrencyCode: aws.String("USD"),
		EffectiveDate: aws.Time(time.Now()), RegionCode: aws.String("us-east-1"),
	})
	require.NoError(t, err)
	require.Len(t, lists.PriceLists, 1)

	got, err := c.GetPriceListFileUrl(t.Context(), &pricing.GetPriceListFileUrlInput{
		PriceListArn: lists.PriceLists[0].PriceListArn, FileFormat: aws.String("json"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(got.Url))
}

func TestIntegration_Budgets_AutoAdjustRoundTrip(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	c := budgets.NewFromConfig(billingConfig(t), func(o *budgets.Options) { o.BaseEndpoint = aws.String(endpoint) })
	name := "auto-" + uuid.NewString()[:8]

	_, err := c.CreateBudget(t.Context(), &budgets.CreateBudgetInput{
		AccountId: aws.String("000000000000"),
		Budget: &budgetstypes.Budget{
			BudgetName: aws.String(
				name,
			),
			BudgetType: budgetstypes.BudgetTypeCost,
			TimeUnit:   budgetstypes.TimeUnitMonthly,
			AutoAdjustData: &budgetstypes.AutoAdjustData{
				AutoAdjustType:    budgetstypes.AutoAdjustTypeHistorical,
				HistoricalOptions: &budgetstypes.HistoricalOptions{BudgetAdjustmentPeriod: aws.Int32(1)},
			},
		},
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = c.DeleteBudget(t.Context(), &budgets.DeleteBudgetInput{
			AccountId: aws.String("000000000000"), BudgetName: aws.String(name),
		})
	})

	got, err := c.DescribeBudget(t.Context(), &budgets.DescribeBudgetInput{
		AccountId: aws.String("000000000000"), BudgetName: aws.String(name),
	})
	require.NoError(t, err)
	require.NotNil(t, got.Budget.AutoAdjustData.LastAutoAdjustTime)
	require.NotNil(t, got.Budget.BudgetLimit)
}

func TestIntegration_BCMDataExports_NotFoundFields(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	c := bcmdataexports.NewFromConfig(billingConfig(t), func(o *bcmdataexports.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	_, err := c.GetTable(t.Context(), &bcmdataexports.GetTableInput{TableName: aws.String("NO_SUCH_TABLE")})
	nf, ok := errors.AsType[*bcmtypes.ResourceNotFoundException](err)
	require.True(t, ok, "got %v", err)
	assert.Equal(t, "Table", aws.ToString(nf.ResourceType))
	assert.Equal(t, "NO_SUCH_TABLE", aws.ToString(nf.ResourceId))
}
