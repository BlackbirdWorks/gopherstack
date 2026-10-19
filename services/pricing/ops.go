package pricing

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/labstack/echo/v5"
)

const (
	serviceName = "pricing"
	homeRegion  = "us-east-1"

	// FilePathPattern is the route that serves price list files.
	FilePathPattern = "/_gopherstack/pricing/offers/v1.0/aws/:service/:version/:region/:file"
	filePathPrefix  = "/_gopherstack/pricing/offers/v1.0/aws/"
)

type baseURLKey struct{}

func baseURL(c *echo.Context) string {
	scheme := "http"
	if c.Request().TLS != nil {
		scheme = "https"
	}

	return scheme + "://" + c.Request().Host
}

type (
	describeServicesReq struct {
		MaxResults    *int32 `json:"MaxResults"`
		FormatVersion string `json:"FormatVersion"`
		NextToken     string `json:"NextToken"`
		ServiceCode   string `json:"ServiceCode"`
	}

	describeServicesResp struct {
		FormatVersion string        `json:"FormatVersion"`
		NextToken     string        `json:"NextToken,omitempty"`
		Services      []ServiceInfo `json:"Services"`
	}

	attributeValuesReq struct {
		MaxResults    *int32 `json:"MaxResults"`
		AttributeName string `json:"AttributeName"`
		NextToken     string `json:"NextToken"`
		ServiceCode   string `json:"ServiceCode"`
	}

	attributeValue struct {
		Value string `json:"Value"`
	}

	attributeValuesResp struct {
		NextToken       string           `json:"NextToken,omitempty"`
		AttributeValues []attributeValue `json:"AttributeValues"`
	}

	productsReq struct {
		MaxResults    *int32   `json:"MaxResults"`
		FormatVersion string   `json:"FormatVersion"`
		NextToken     string   `json:"NextToken"`
		ServiceCode   string   `json:"ServiceCode"`
		Filters       []Filter `json:"Filters"`
	}

	productsResp struct {
		FormatVersion string   `json:"FormatVersion"`
		NextToken     string   `json:"NextToken,omitempty"`
		PriceList     []string `json:"PriceList"`
	}

	priceListsReq struct {
		MaxResults    *int32  `json:"MaxResults"`
		CurrencyCode  string  `json:"CurrencyCode"`
		NextToken     string  `json:"NextToken"`
		RegionCode    string  `json:"RegionCode"`
		ServiceCode   string  `json:"ServiceCode"`
		EffectiveDate float64 `json:"EffectiveDate"`
	}

	priceListsResp struct {
		NextToken  string          `json:"NextToken,omitempty"`
		PriceLists []PriceListInfo `json:"PriceLists"`
	}

	fileURLReq struct {
		FileFormat   string `json:"FileFormat"`
		PriceListArn string `json:"PriceListArn"`
	}

	fileURLResp struct {
		URL string `json:"Url"`
	}
)

func registerOps(ops map[string]opFunc) {
	ops["DescribeServices"] = bind((*Handler).describeServices)
	ops["GetAttributeValues"] = bind((*Handler).getAttributeValues)
	ops["GetProducts"] = bind((*Handler).getProducts)
	ops["ListPriceLists"] = bind((*Handler).listPriceLists)
	ops["GetPriceListFileUrl"] = bind((*Handler).getPriceListFileURL)
}

func (h *Handler) describeServices(_ context.Context, in *describeServicesReq) (*describeServicesResp, error) {
	svcs, next, err := DescribeServices(in.ServiceCode, in.FormatVersion, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &describeServicesResp{FormatVersion: formatVersion, Services: svcs, NextToken: next}, nil
}

func (h *Handler) getAttributeValues(_ context.Context, in *attributeValuesReq) (*attributeValuesResp, error) {
	vals, next, err := AttributeValues(in.ServiceCode, in.AttributeName, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	out := &attributeValuesResp{NextToken: next, AttributeValues: make([]attributeValue, len(vals))}
	for i, v := range vals {
		out.AttributeValues[i] = attributeValue{Value: v}
	}

	return out, nil
}

func (h *Handler) getProducts(_ context.Context, in *productsReq) (*productsResp, error) {
	list, next, err := Products(in.ServiceCode, in.FormatVersion, in.NextToken, in.MaxResults, in.Filters)
	if err != nil {
		return nil, err
	}

	return &productsResp{FormatVersion: formatVersion, PriceList: list, NextToken: next}, nil
}

func (h *Handler) listPriceLists(_ context.Context, in *priceListsReq) (*priceListsResp, error) {
	sec := int64(in.EffectiveDate)
	eff := time.Unix(sec, int64((in.EffectiveDate-float64(sec))*float64(time.Second))).UTC()

	lists, next, err := PriceLists(in.ServiceCode, in.CurrencyCode, in.RegionCode, eff, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &priceListsResp{PriceLists: lists, NextToken: next}, nil
}

func (h *Handler) getPriceListFileURL(ctx context.Context, in *fileURLReq) (*fileURLResp, error) {
	code, ver, region, kind, err := FileKind(in.PriceListArn, in.FileFormat)
	if err != nil {
		return nil, err
	}

	base, _ := ctx.Value(baseURLKey{}).(string)

	return &fileURLResp{URL: fmt.Sprintf("%s%s%s/%s/%s/index.%s", base, filePathPrefix, code, ver, region, kind)}, nil
}

// ServePriceListFile serves a price list file at FilePathPattern.
func (h *Handler) ServePriceListFile(c *echo.Context) error {
	file := c.Param("file")

	var kind string

	switch file {
	case "index.json":
		kind = formatJSON
	case "index.csv":
		kind = formatCSV
	default:
		return echo.NewHTTPError(http.StatusNotFound, "unknown price list file")
	}

	body, contentType, ok := RenderFile(c.Param("service"), c.Param("version"), c.Param("region"), kind)
	if !ok {
		return echo.NewHTTPError(http.StatusNotFound, "price list not found")
	}

	return c.Blob(http.StatusOK, contentType, body)
}
