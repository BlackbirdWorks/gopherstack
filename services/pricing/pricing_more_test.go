package pricing_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	psdk "github.com/aws/aws-sdk-go-v2/service/pricing"
	"github.com/aws/aws-sdk-go-v2/service/pricing/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/pricing"
)

func TestNewServicesAndRegions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		code   string
		region string
		want   int
	}{
		{"AmazonSQS", "sa-east-1", 2},
		{"AmazonSNS", "ap-southeast-2", 3},
		{"AmazonCloudFront", "ca-central-1", 2},
		{"AmazonElastiCache", "us-east-2", 8},
		{"AmazonECS", "eu-west-2", 2},
		{"AmazonEC2", "ap-northeast-2", 19},
	}

	for _, tc := range tests {
		t.Run(tc.code+"/"+tc.region, func(t *testing.T) {
			t.Parallel()

			c := newClient(t)
			out, err := c.GetProducts(t.Context(), &psdk.GetProductsInput{
				ServiceCode: aws.String(tc.code), MaxResults: aws.Int32(100),
				Filters: []types.Filter{filter(types.FilterTypeTermMatch, "regionCode", tc.region)},
			})
			require.NoError(t, err)
			assert.GreaterOrEqual(t, len(out.PriceList), tc.want)
		})
	}
}

func TestReservedTermMatrix(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	out, err := c.GetProducts(t.Context(), &psdk.GetProductsInput{
		ServiceCode: aws.String("AmazonEC2"),
		Filters: []types.Filter{
			filter(types.FilterTypeTermMatch, "instanceType", "m5.large"),
			filter(types.FilterTypeTermMatch, "regionCode", "us-east-1"),
			filter(types.FilterTypeTermMatch, "operatingSystem", "Linux"),
		},
	})
	require.NoError(t, err)
	require.Len(t, out.PriceList, 1)

	var item struct {
		Terms struct {
			Reserved map[string]struct {
				PriceDimensions map[string]struct {
					PricePerUnit map[string]string `json:"pricePerUnit"`
					Unit         string            `json:"unit"`
				} `json:"priceDimensions"`
				TermAttributes map[string]string `json:"termAttributes"`
			} `json:"Reserved"`
		} `json:"terms"`
	}

	require.NoError(t, json.Unmarshal([]byte(out.PriceList[0]), &item))
	require.Len(t, item.Terms.Reserved, 12)

	seen := map[string]bool{}

	for _, term := range item.Terms.Reserved {
		ta := term.TermAttributes
		seen[ta["LeaseContractLength"]+"/"+ta["OfferingClass"]+"/"+ta["PurchaseOption"]] = true

		var hourly, upfront float64

		hasUpfront := false

		for _, d := range term.PriceDimensions {
			v, perr := strconv.ParseFloat(d.PricePerUnit["USD"], 64)
			require.NoError(t, perr)

			if d.Unit == "Quantity" {
				upfront, hasUpfront = v, true
			} else {
				hourly = v
			}
		}

		switch ta["PurchaseOption"] {
		case "No Upfront":
			assert.False(t, hasUpfront)
			assert.Positive(t, hourly)
		case "Partial Upfront":
			assert.True(t, hasUpfront)
			assert.Positive(t, hourly)
			assert.Positive(t, upfront)
		case "All Upfront":
			assert.True(t, hasUpfront)
			assert.InDelta(t, 0, hourly, 1e-12)
		}
	}

	assert.Len(t, seen, 12)
}

func TestSavingsPlansPriceLists(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	effective := time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)

	lists, err := c.ListPriceLists(t.Context(), &psdk.ListPriceListsInput{
		ServiceCode: aws.String("ComputeSavingsPlans"), CurrencyCode: aws.String("USD"),
		EffectiveDate: aws.Time(effective), RegionCode: aws.String("eu-west-1"),
	})
	require.NoError(t, err)
	require.Len(t, lists.PriceLists, 1)
	assert.Equal(t, []string{"json"}, lists.PriceLists[0].FileFormats)

	_, err = c.GetPriceListFileUrl(t.Context(), &psdk.GetPriceListFileUrlInput{
		PriceListArn: lists.PriceLists[0].PriceListArn, FileFormat: aws.String("csv"),
	})
	assert.Equal(t, "InvalidParameterException", apiCode(err))

	got, err := c.GetPriceListFileUrl(t.Context(), &psdk.GetPriceListFileUrlInput{
		PriceListArn: lists.PriceLists[0].PriceListArn, FileFormat: aws.String("json"),
	})
	require.NoError(t, err)

	u, err := url.Parse(aws.ToString(got.Url))
	require.NoError(t, err)

	e := echo.New()
	e.GET(pricing.FilePathPattern, pricing.NewHandler().ServePriceListFile)

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, u.Path, http.NoBody))
	require.Equal(t, http.StatusOK, rec.Code)

	var file struct {
		Terms struct {
			SavingsPlan []struct {
				Rates []struct {
					DiscountedRate struct {
						Price string `json:"price"`
					} `json:"discountedRate"`
					DiscountedServiceCode string `json:"discountedServiceCode"`
				} `json:"rates"`
			} `json:"savingsPlan"`
		} `json:"terms"`
		Products []struct {
			ProductFamily string `json:"productFamily"`
		} `json:"products"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &file))
	require.Len(t, file.Terms.SavingsPlan, 6)
	assert.Len(t, file.Products, 6)
	require.NotEmpty(t, file.Terms.SavingsPlan[0].Rates)
	assert.Equal(t, "AmazonEC2", file.Terms.SavingsPlan[0].Rates[0].DiscountedServiceCode)
	assert.Equal(t, "ComputeSavingsPlans", file.Products[0].ProductFamily)
}
