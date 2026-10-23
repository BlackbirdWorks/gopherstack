package pricing

import (
	"crypto/sha256"
	"encoding/base32"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

const (
	svcComputeSP = "ComputeSavingsPlans"
	spUsageOS    = "Linux"
)

// spPlan is one Compute Savings Plan term; discount is the fraction of the On Demand rate that is charged.
type spPlan struct {
	option   string
	years    int
	discount float64
}

// spPlans are illustrative discounts, not published Savings Plans rates.
var spPlans = []spPlan{ //nolint:gochecknoglobals // immutable catalog input
	{optNoUpfront, 1, 0.84}, {optPartial, 1, 0.80}, {optAllUpfront, 1, 0.78},
	{optNoUpfront, 3, 0.72}, {optPartial, 3, 0.66}, {optAllUpfront, 3, 0.64},
}

type spRate struct {
	DiscountedRate        spPrice `json:"discountedRate"`
	RateCode              string  `json:"rateCode"`
	UnitPrice             string  `json:"unitPrice"`
	DiscountedSKU         string  `json:"discountedSku"`
	DiscountedUsageType   string  `json:"discountedUsageType"`
	DiscountedOperation   string  `json:"discountedOperation"`
	DiscountedServiceCode string  `json:"discountedServiceCode"`
}

type spPrice struct {
	Price    string `json:"price"`
	Currency string `json:"currency"`
}

type spLease struct {
	Unit     string `json:"unit"`
	Duration int    `json:"duration"`
}

type spTerm struct {
	SKU           string   `json:"sku"`
	Description   string   `json:"description"`
	EffectiveDate string   `json:"effectiveDate"`
	Rates         []spRate `json:"rates"`
	Lease         spLease  `json:"leaseContractLength"`
}

type spProduct struct {
	Attributes    map[string]string `json:"attributes"`
	SKU           string            `json:"sku"`
	ProductFamily string            `json:"productFamily"`
	ServiceCode   string            `json:"serviceCode"`
}

type spFile struct {
	Terms       map[string][]spTerm `json:"terms"`
	Version     string              `json:"version"`
	PublishDate string              `json:"publishDate"`
	Products    []spProduct         `json:"products"`
}

func spSKU(region string, p spPlan) string {
	sum := sha256.Sum256(fmt.Appendf(nil, "%s|%s|%d|%s", svcComputeSP, p.option, p.years, region))

	return base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(sum[:])[:skuLen]
}

func regionLocation(code string) string {
	for _, r := range regions {
		if r.code == code {
			return r.location
		}
	}

	return ""
}

func onDemandHourly(e *entry) (float64, bool) {
	for _, d := range e.item.Terms.OnDemand[e.item.Product.SKU+"."+onDemandTerm].PriceDimensions {
		v, err := strconv.ParseFloat(d.PricePerUnit[currency], 64)

		return v, err == nil
	}

	return 0, false
}

func spRates(sku string, p spPlan, entries []*entry) []spRate {
	var out []spRate

	for _, e := range entries {
		attrs := e.item.Product.Attributes
		if attrs["operatingSystem"] != spUsageOS || e.item.Product.ProductFamily != "Compute Instance" {
			continue
		}

		price, ok := onDemandHourly(e)
		if !ok {
			continue
		}

		sum := sha256.Sum256([]byte(sku + "|" + e.item.Product.SKU))
		suffix := base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(sum[:])[:rateSuffixLen]
		rate := strconv.FormatFloat(price*p.discount, 'f', 10, 64)
		out = append(out, spRate{
			DiscountedRate:        spPrice{Price: rate, Currency: currency},
			RateCode:              sku + "." + suffix,
			UnitPrice:             rate,
			DiscountedSKU:         e.item.Product.SKU,
			DiscountedUsageType:   attrs[attrUsage],
			DiscountedOperation:   attrs[attrOperation],
			DiscountedServiceCode: svcEC2,
		})
	}

	slices.SortFunc(out, func(a, b spRate) int { return strings.Compare(a.DiscountedSKU, b.DiscountedSKU) })

	return out
}

// renderSavingsPlans returns the Compute Savings Plans price list file of a region.
func renderSavingsPlans(region string) []byte {
	entries := regionEntries(svcEC2, region)
	f := spFile{Version: catalogVersion, PublishDate: publicationDate, Terms: map[string][]spTerm{"savingsPlan": {}}}

	for _, p := range spPlans {
		sku := spSKU(region, p)
		f.Products = append(f.Products, spProduct{
			SKU: sku, ProductFamily: svcComputeSP, ServiceCode: svcComputeSP,
			Attributes: map[string]string{
				"purchaseOption": p.option, "purchaseTerm": fmt.Sprintf("%dyr", p.years),
				"location": regionLocation(region), "locationType": "AWS Region", "regionCode": region,
			},
		})
		f.Terms["savingsPlan"] = append(f.Terms["savingsPlan"], spTerm{
			SKU:           sku,
			Description:   fmt.Sprintf("Compute Savings Plan %dyr %s", p.years, p.option),
			EffectiveDate: publicationDate,
			Lease:         spLease{Duration: p.years, Unit: "year"},
			Rates:         spRates(sku, p, entries),
		})
	}

	raw, _ := json.Marshal(f)

	return raw
}
