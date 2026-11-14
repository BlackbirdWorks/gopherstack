package bcmdataexports

import (
	"encoding/hex"
	"encoding/json/v2"
	"fmt"
	"hash/fnv"
	"maps"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/billingexport"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	isoSecond      = "2006-01-02T15:04:05Z"
	hoursPerDay    = 24
	dayLen         = 24 * time.Hour
	providerAWS    = "AWS"
	legalEntityAWS = "Amazon Web Services, Inc."
	currencyUSD    = "USD"
	pairLen        = 2
	unitQuantity   = "Quantity"
)

type rowCtx struct {
	start     time.Time
	end       time.Time
	billStart time.Time
	billEnd   time.Time
	payer     string
	line      billingexport.CostLine
	frac      float64
}

func jsonMap(kv ...string) string {
	m := make(map[string]string, len(kv)/pairLen)
	for i := 0; i+1 < len(kv); i += 2 {
		m[kv[i]] = kv[i+1]
	}

	b, _ := json.Marshal(m, jsonx.V1())

	return string(b)
}

func ratio(cost, usage float64) any {
	if usage == 0 {
		return nil
	}

	return cost / usage
}

func lineID(r *rowCtx) string {
	h := fnv.New128a()
	_, _ = fmt.Fprintf(h, "%s|%s", r.line.Key(), r.start)

	return hex.EncodeToString(h.Sum(nil))
}

func rateText(cost, usage float64) any {
	v := ratio(cost, usage)
	if v == nil {
		return ""
	}

	return fmt.Sprintf("%.10f", v)
}

func (r *rowCtx) cost() float64 { return r.line.Unblended * r.frac }

func curValues(r *rowCtx) map[string]any {
	code := billingexport.ProductCode(r.line.Service)
	l := &r.line

	return map[string]any{
		"identity_line_item_id":              lineID(r),
		"identity_time_interval":             r.start.Format(isoSecond) + "/" + r.end.Format(isoSecond),
		"bill_invoice_id":                    "",
		"bill_invoicing_entity":              legalEntityAWS,
		"bill_billing_entity":                providerAWS,
		"bill_bill_type":                     "Anniversary",
		"bill_payer_account_id":              r.payer,
		"bill_billing_period_start_date":     r.billStart.Format(isoSecond),
		"bill_billing_period_end_date":       r.billEnd.Format(isoSecond),
		"line_item_usage_account_id":         l.Account,
		"line_item_line_item_type":           l.LineItemType(),
		"line_item_usage_start_date":         r.start.Format(isoSecond),
		"line_item_usage_end_date":           r.end.Format(isoSecond),
		"line_item_product_code":             code,
		"line_item_usage_type":               l.UsageType,
		"line_item_operation":                l.Operation,
		"line_item_availability_zone":        l.AvailabilityZone,
		"line_item_resource_id":              l.ResourceID,
		"line_item_usage_amount":             l.Usage * r.frac,
		"line_item_unblended_rate":           rateText(l.Unblended, l.Usage),
		"line_item_unblended_cost":           r.cost(),
		"line_item_blended_rate":             rateText(l.Blended, l.Usage),
		"line_item_blended_cost":             l.Blended * r.frac,
		"line_item_currency_code":            currencyUSD,
		"line_item_line_item_description":    l.Service + " " + l.UsageType + " usage",
		"line_item_legal_entity":             legalEntityAWS,
		"product_region_code":                l.Region,
		"product_servicecode":                code,
		"product_usagetype":                  l.UsageType,
		"product_operation":                  l.Operation,
		"product_instance_type":              l.InstanceType,
		"product_product_family":             l.ProductFamily,
		"pricing_unit":                       unitQuantity,
		"pricing_term":                       "OnDemand",
		"pricing_currency":                   currencyUSD,
		"pricing_public_on_demand_cost":      r.cost(),
		"pricing_public_on_demand_rate":      rateText(l.Unblended, l.Usage),
		"product":                            productMap(l, code),
		"resource_tags":                      tagMap(l.Tags, "user:"),
		"tags":                               tagMap(l.Tags, "user_"),
		"cost_category":                      "{}",
		"discount":                           "{}",
		"split_line_item_parent_resource_id": "",
		"split_line_item_split_usage":        l.Usage * r.frac,
		"split_line_item_split_usage_ratio":  1.0,
		"split_line_item_split_cost":         r.cost(),
		"split_line_item_unused_cost":        0.0,
	}
}

func productMap(l *billingexport.CostLine, code string) string {
	kv := []string{"product_name", l.Service, "region_code", l.Region, "servicecode", code, "usagetype", l.UsageType}

	for _, p := range [][2]string{
		{"operation", l.Operation}, {"instance_type", l.InstanceType}, {"product_family", l.ProductFamily},
	} {
		if p[1] != "" {
			kv = append(kv, p[0], p[1])
		}
	}

	return jsonMap(kv...)
}

func tagMap(t map[string]string, prefix string) string {
	kv := make([]string, 0, len(t)*pairLen)

	for _, k := range slices.Sorted(maps.Keys(t)) {
		kv = append(kv, prefix+k, t[k])
	}

	return jsonMap(kv...)
}

func focusValues(r *rowCtx) map[string]any {
	l := &r.line
	code := billingexport.ProductCode(l.Service)

	return map[string]any{
		"AvailabilityZone":    l.AvailabilityZone,
		"BilledCost":          l.Blended * r.frac,
		"BillingAccountId":    r.payer,
		"BillingCurrency":     currencyUSD,
		"BillingPeriodEnd":    r.billEnd.Format(isoSecond),
		"BillingPeriodStart":  r.billStart.Format(isoSecond),
		"ChargeCategory":      l.ChargeCategory(),
		"ChargeDescription":   l.Service + " " + l.UsageType + " usage",
		"ChargeFrequency":     "Usage-Based",
		"ChargePeriodEnd":     r.end.Format(isoSecond),
		"ChargePeriodStart":   r.start.Format(isoSecond),
		"ConsumedQuantity":    l.Usage * r.frac,
		"ConsumedUnit":        unitQuantity,
		"ContractedCost":      r.cost(),
		"ContractedUnitPrice": ratio(l.Unblended, l.Usage),
		"EffectiveCost":       r.cost(),
		"InvoiceIssuerName":   providerAWS,
		"ListCost":            r.cost(),
		"ListUnitPrice":       ratio(l.Unblended, l.Usage),
		"PricingCategory":     "Standard",
		"PricingQuantity":     l.Usage * r.frac,
		"PricingUnit":         unitQuantity,
		"ProviderName":        providerAWS,
		"PublisherName":       providerAWS,
		"RegionId":            l.Region,
		"RegionName":          l.Region,
		"ResourceId":          l.ResourceID,
		"ServiceCategory":     "Other",
		"ServiceName":         l.Service,
		"SubAccountId":        l.Account,
		"Tags":                tagMap(l.Tags, ""),
		"x_CostCategories":    "{}",
		"x_Discounts":         "{}",
		"x_Operation":         l.Operation,
		"x_ServiceCode":       code,
		"x_UsageType":         l.UsageType,
	}
}

func focus12Values(r *rowCtx) map[string]any {
	v := focusValues(r)
	delete(v, "x_CostCategories")
	delete(v, "x_UsageType")
	v["PricingCurrency"] = currencyUSD
	v["PricingCurrencyEffectiveCost"] = r.cost()
	v["PricingCurrencyListUnitPrice"] = ratio(r.line.Unblended, r.line.Usage)
	v["PricingCurrencyContractedUnitPrice"] = ratio(r.line.Unblended, r.line.Usage)
	v["InvoiceId"] = ""

	return v
}

func dashboardValues(r *rowCtx) map[string]any {
	l := &r.line
	code := billingexport.ProductCode(l.Service)

	return map[string]any{
		"amortized_cost":    r.cost(),
		"availability_zone": l.AvailabilityZone,
		"billing_entity":    providerAWS,
		"billing_period":    r.billStart.Format(isoSecond),
		"charge_category":   dashboardChargeCategory(l),
		"charge_type":       l.LineItemType(),
		"instance_type":     l.InstanceType,
		"invoice_id":        "",
		"item_description":  l.Service + " " + l.UsageType + " usage",
		"legal_entity":      legalEntityAWS,
		"linked_account_id": l.Account,
		"operation":         l.Operation,
		"payer_account_id":  r.payer,
		"pricing_unit":      unitQuantity,
		"product_code":      code,
		"product_family":    l.ProductFamily,
		"product_name":      l.Service,
		"public_cost":       r.cost(),
		"purchase_option":   "OnDemand",
		"region":            l.Region,
		"service":           code,
		"unblended_cost":    r.cost(),
		"usage_date":        r.start.Format(isoSecond),
		"usage_quantity":    l.Usage * r.frac,
		"usage_type":        l.UsageType,
	}
}

func dashboardChargeCategory(l *billingexport.CostLine) string {
	if l.LineItemType() == "Usage" {
		return "Running_usage"
	}

	return "Non_Usage"
}

func intervalsOf(gran string, ln *billingexport.CostLine, billEnd time.Time) []rowCtx {
	day := ln.Day.UTC().Truncate(dayLen)

	switch gran {
	case granHourly:
		out := make([]rowCtx, hoursPerDay)
		for h := range out {
			s := day.Add(time.Duration(h) * time.Hour)
			out[h] = rowCtx{line: *ln, start: s, end: s.Add(time.Hour), frac: 1.0 / hoursPerDay}
		}

		return out
	case granMonthly:
		return []rowCtx{{line: *ln, start: ln.Day, end: billEnd, frac: 1}}
	default:
		return []rowCtx{{line: *ln, start: day, end: day.AddDate(0, 0, 1), frac: 1}}
	}
}

func aggregateMonthly(lines []billingexport.CostLine, billStart time.Time) []billingexport.CostLine {
	idx := map[string]int{}

	var out []billingexport.CostLine

	for _, ln := range lines {
		k := ln.Key()
		i, ok := idx[k]

		if !ok {
			idx[k] = len(out)
			ln.Day = billStart
			out = append(out, ln)

			continue
		}

		out[i].Blended += ln.Blended
		out[i].Unblended += ln.Unblended
		out[i].Usage += ln.Usage
	}

	return out
}

// tableRows materialises the rows of a table for one billing period.
func tableRows(
	t tableDef, cols []columnDef, props map[string]string, lines []billingexport.CostLine,
	billStart, billEnd time.Time, payer string,
) [][]any {
	var values func(*rowCtx) map[string]any

	switch t.Name {
	case tableCUR:
		values = curValues
	case tableFocus:
		values = focusValues
	case tableFocus12:
		values = focus12Values
	case tableDashboard:
		values = dashboardValues
	default:
		return nil
	}

	gran := props[propGran]
	if gran == granMonthly {
		lines = aggregateMonthly(lines, billStart)
	}

	var rows [][]any

	for i := range lines {
		for _, rc := range intervalsOf(gran, &lines[i], billEnd) {
			rc.billStart, rc.billEnd, rc.payer = billStart, billEnd, payer
			vals := values(&rc)
			row := make([]any, len(cols))

			for j, c := range cols {
				row[j] = vals[c.Name]
			}

			rows = append(rows, row)
		}
	}

	return rows
}

func columnTypeParquet(t string) string {
	if t == typeDec {
		return billingexport.TypeDouble
	}

	return billingexport.TypeString
}

func periodLabel(t time.Time) string { return "BILLING_PERIOD=" + t.Format("2006-01") }
