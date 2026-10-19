package pricing_test

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	psdk "github.com/aws/aws-sdk-go-v2/service/pricing"
	"github.com/aws/aws-sdk-go-v2/service/pricing/types"
	"github.com/aws/smithy-go"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/sdktest"
	"github.com/blackbirdworks/gopherstack/services/pricing"
)

func newClient(t *testing.T) *psdk.Client {
	t.Helper()

	srv := sdktest.Serve(t, pricing.NewHandler())

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	return psdk.NewFromConfig(cfg, func(o *psdk.Options) { o.BaseEndpoint = aws.String(srv) })
}

func apiCode(err error) string {
	if ae, ok := errors.AsType[smithy.APIError](err); ok {
		return ae.ErrorCode()
	}

	return ""
}

func filter(typ types.FilterType, field, value string) types.Filter {
	return types.Filter{Type: typ, Field: aws.String(field), Value: aws.String(value)}
}

func TestDescribeServices(t *testing.T) {
	t.Parallel()

	c := newClient(t)

	all, err := c.DescribeServices(t.Context(), &psdk.DescribeServicesInput{})
	require.NoError(t, err)
	assert.Equal(t, "aws_v1", aws.ToString(all.FormatVersion))

	codes := make([]string, 0, len(all.Services))
	for _, s := range all.Services {
		codes = append(codes, aws.ToString(s.ServiceCode))
	}

	assert.ElementsMatch(t, []string{"AmazonEC2", "AmazonS3", "AWSLambda", "AmazonDynamoDB", "AmazonRDS"}, codes)

	one, err := c.DescribeServices(t.Context(), &psdk.DescribeServicesInput{ServiceCode: aws.String("AmazonEC2")})
	require.NoError(t, err)
	require.Len(t, one.Services, 1)
	assert.Contains(t, one.Services[0].AttributeNames, "instanceType")

	_, err = c.DescribeServices(t.Context(), &psdk.DescribeServicesInput{ServiceCode: aws.String("Nope")})
	assert.Equal(t, "NotFoundException", apiCode(err))

	_, err = c.DescribeServices(t.Context(), &psdk.DescribeServicesInput{FormatVersion: aws.String("v9")})
	assert.Equal(t, "InvalidParameterException", apiCode(err))
}

func TestGetAttributeValues(t *testing.T) {
	t.Parallel()

	c := newClient(t)

	out, err := c.GetAttributeValues(t.Context(), &psdk.GetAttributeValuesInput{
		ServiceCode: aws.String("AmazonEC2"), AttributeName: aws.String("operatingSystem"),
	})
	require.NoError(t, err)

	vals := make([]string, 0, len(out.AttributeValues))
	for _, v := range out.AttributeValues {
		vals = append(vals, aws.ToString(v.Value))
	}

	assert.Equal(t, []string{"Linux", "Windows"}, vals)

	_, err = c.GetAttributeValues(t.Context(), &psdk.GetAttributeValuesInput{
		ServiceCode: aws.String("AmazonEC2"), AttributeName: aws.String("bogus"),
	})
	assert.Equal(t, "NotFoundException", apiCode(err))

	_, err = c.GetAttributeValues(t.Context(), &psdk.GetAttributeValuesInput{
		ServiceCode: aws.String("Nope"), AttributeName: aws.String("x"),
	})
	assert.Equal(t, "NotFoundException", apiCode(err))
}

func TestGetProductsFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		code    string
		wantErr string
		filters []types.Filter
		want    int
	}{
		{name: "term match", code: "AmazonEC2", want: 1, filters: []types.Filter{
			filter(types.FilterTypeTermMatch, "instanceType", "t3.micro"),
			filter(types.FilterTypeTermMatch, "regionCode", "us-east-1"),
			filter(types.FilterTypeTermMatch, "operatingSystem", "Linux"),
		}},
		{name: "equals case sensitive", code: "AmazonEC2", want: 0, filters: []types.Filter{
			filter(types.FilterTypeEquals, "instanceType", "T3.MICRO"),
		}},
		{name: "contains", code: "AmazonEC2", want: 6, filters: []types.Filter{
			filter(types.FilterTypeContains, "instanceType", "t3."),
			filter(types.FilterTypeEquals, "operatingSystem", "Linux"),
			filter(types.FilterTypeAnyOf, "regionCode", "us-east-1,us-west-2"),
			filter(types.FilterTypeEquals, "capacitystatus", "Used"),
		}},
		{name: "none of", code: "AmazonRDS", want: 1, filters: []types.Filter{
			filter(types.FilterTypeEquals, "regionCode", "us-east-1"),
			filter(types.FilterTypeEquals, "instanceType", "db.t3.micro"),
			filter(types.FilterTypeEquals, "databaseEngine", "MySQL"),
			filter(types.FilterTypeNoneOf, "deploymentOption", "Multi-AZ"),
		}},
		{name: "service code field", code: "AWSLambda", want: 3, filters: []types.Filter{
			filter(types.FilterTypeTermMatch, "ServiceCode", "AWSLambda"),
			filter(types.FilterTypeTermMatch, "regionCode", "eu-west-1"),
		}},
		{name: "unknown service", code: "Nope", wantErr: "NotFoundException"},
		{name: "bad type", code: "AmazonS3", wantErr: "InvalidParameterException", filters: []types.Filter{
			filter(types.FilterType("LIKE"), "x", "y"),
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newClient(t)
			out, err := c.GetProducts(t.Context(), &psdk.GetProductsInput{
				ServiceCode: aws.String(tc.code), Filters: tc.filters, MaxResults: aws.Int32(100),
			})

			if tc.wantErr != "" {
				assert.Equal(t, tc.wantErr, apiCode(err))

				return
			}

			require.NoError(t, err)
			assert.Len(t, out.PriceList, tc.want)
		})
	}
}

func TestProductShape(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	out, err := c.GetProducts(t.Context(), &psdk.GetProductsInput{
		ServiceCode: aws.String("AmazonEC2"),
		Filters: []types.Filter{
			filter(types.FilterTypeTermMatch, "instanceType", "t3.micro"),
			filter(types.FilterTypeTermMatch, "regionCode", "us-east-1"),
			filter(types.FilterTypeTermMatch, "operatingSystem", "Linux"),
		},
	})
	require.NoError(t, err)
	require.Len(t, out.PriceList, 1)

	var item struct {
		Product struct {
			Attributes    map[string]string `json:"attributes"`
			SKU           string            `json:"sku"`
			ProductFamily string            `json:"productFamily"`
		} `json:"product"`
		Terms struct {
			OnDemand map[string]struct {
				PriceDimensions map[string]struct {
					PricePerUnit map[string]string `json:"pricePerUnit"`
					Unit         string            `json:"unit"`
				} `json:"priceDimensions"`
			} `json:"OnDemand"`
			Reserved map[string]struct {
				TermAttributes map[string]string `json:"termAttributes"`
			} `json:"Reserved"`
		} `json:"terms"`
		ServiceCode string `json:"serviceCode"`
	}

	require.NoError(t, json.Unmarshal([]byte(out.PriceList[0]), &item))
	assert.Equal(t, "AmazonEC2", item.ServiceCode)
	assert.Equal(t, "Compute Instance", item.Product.ProductFamily)
	assert.Equal(t, "US East (N. Virginia)", item.Product.Attributes["location"])
	assert.Len(t, item.Terms.Reserved, 2)

	od := item.Terms.OnDemand[item.Product.SKU+".JRTCKXETXF"]
	require.Len(t, od.PriceDimensions, 1)

	for _, d := range od.PriceDimensions {
		assert.Equal(t, "Hrs", d.Unit)
		assert.Equal(t, "0.0104000000", d.PricePerUnit["USD"])
	}
}

func TestGetProductsPagination(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	p := psdk.NewGetProductsPaginator(c, &psdk.GetProductsInput{
		ServiceCode: aws.String("AWSLambda"), MaxResults: aws.Int32(7),
	})

	total, pages := 0, 0

	for p.HasMorePages() {
		out, err := p.NextPage(t.Context())
		require.NoError(t, err)

		total += len(out.PriceList)
		pages++
	}

	assert.Equal(t, 18, total)
	assert.Equal(t, 3, pages)

	_, err := c.GetProducts(t.Context(), &psdk.GetProductsInput{
		ServiceCode: aws.String("AWSLambda"), NextToken: aws.String("%%%"),
	})
	assert.Equal(t, "InvalidNextTokenException", apiCode(err))
}

func TestPriceListsAndFile(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	effective := time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)

	lists, err := c.ListPriceLists(t.Context(), &psdk.ListPriceListsInput{
		ServiceCode: aws.String("AmazonS3"), CurrencyCode: aws.String("USD"), EffectiveDate: aws.Time(effective),
		RegionCode: aws.String("eu-central-1"),
	})
	require.NoError(t, err)
	require.Len(t, lists.PriceLists, 1)

	pl := lists.PriceLists[0]
	assert.Equal(t, []string{"csv", "json"}, pl.FileFormats)
	assert.True(t, strings.HasPrefix(aws.ToString(pl.PriceListArn), "arn:aws:pricing:::price-list/aws/AmazonS3/USD/"))

	early, err := c.ListPriceLists(t.Context(), &psdk.ListPriceListsInput{
		ServiceCode: aws.String("AmazonS3"), CurrencyCode: aws.String("USD"),
		EffectiveDate: aws.Time(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)),
	})
	require.NoError(t, err)
	assert.Empty(t, early.PriceLists)

	_, err = c.ListPriceLists(t.Context(), &psdk.ListPriceListsInput{
		ServiceCode: aws.String("Nope"), CurrencyCode: aws.String("USD"), EffectiveDate: aws.Time(effective),
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = c.ListPriceLists(t.Context(), &psdk.ListPriceListsInput{
		ServiceCode: aws.String("AmazonS3"), CurrencyCode: aws.String("EUR"), EffectiveDate: aws.Time(effective),
	})
	assert.Equal(t, "InvalidParameterException", apiCode(err))

	e := echo.New()
	e.GET(pricing.FilePathPattern, pricing.NewHandler().ServePriceListFile)

	for _, format := range []string{"json", "csv"} {
		got, urlErr := c.GetPriceListFileUrl(t.Context(), &psdk.GetPriceListFileUrlInput{
			PriceListArn: pl.PriceListArn, FileFormat: aws.String(format),
		})
		require.NoError(t, urlErr)

		u, parseErr := url.Parse(aws.ToString(got.Url))
		require.NoError(t, parseErr)
		assert.True(t, strings.HasSuffix(u.Path, "/index."+format))

		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, u.Path, http.NoBody))
		require.Equal(t, http.StatusOK, rec.Code)

		if format == "json" {
			var file struct {
				Products  map[string]map[string]any `json:"products"`
				OfferCode string                    `json:"offerCode"`
			}

			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &file))
			assert.Equal(t, "AmazonS3", file.OfferCode)
			assert.NotEmpty(t, file.Products)
		} else {
			assert.Contains(t, rec.Body.String(), "\"OfferCode\"")
		}
	}

	_, err = c.GetPriceListFileUrl(t.Context(), &psdk.GetPriceListFileUrlInput{
		PriceListArn: pl.PriceListArn, FileFormat: aws.String("xml"),
	})
	assert.Equal(t, "InvalidParameterException", apiCode(err))

	_, err = c.GetPriceListFileUrl(t.Context(), &psdk.GetPriceListFileUrlInput{
		PriceListArn: aws.String(
			"arn:aws:pricing:::price-list/aws/AmazonS3/USD/1/us-east-1",
		),
		FileFormat: aws.String("json"),
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet,
		"/_gopherstack/pricing/offers/v1.0/aws/AmazonS3/1/us-east-1/index.json", http.NoBody))
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
