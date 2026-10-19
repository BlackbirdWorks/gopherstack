package bcmdataexports

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/billingexport"
)

const (
	isoSecond    = "2006-01-02T15:04:05Z"
	hoursPerDay  = 24
	dayLen       = 24 * time.Hour
	providerAWS  = "AWS"
	pairLen      = 2
	unitQuantity = "Quantity"
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

	b, _ := json.Marshal(m)

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
	_, _ = fmt.Fprintf(h, "%s|%s|%s|%s|%s", r.line.Account, r.line.Service, r.line.UsageType, r.line.Region, r.start)

	return hex.EncodeToString(h.Sum(nil))
}

func curValues(r *rowCtx) map[string]any {
	code := billingexport.ProductCode(r.line.Service)

	return map[string]any{
		"identity_line_item_id":           lineID(r),
		"identity_time_interval":          r.start.Format(isoSecond) + "/" + r.end.Format(isoSecond),
		"bill_invoice_id":                 "",
		"bill_invoicing_entity":           "Amazon Web Services, Inc.",
		"bill_billing_entity":             providerAWS,
		"bill_bill_type":                  "Anniversary",
		"bill_payer_account_id":           r.payer,
		"bill_billing_period_start_date":  r.billStart.Format(isoSecond),
		"bill_billing_period_end_date":    r.billEnd.Format(isoSecond),
		"line_item_usage_account_id":      r.line.Account,
		"line_item_line_item_type":        "Usage",
		"line_item_usage_start_date":      r.start.Format(isoSecond),
		"line_item_usage_end_date":        r.end.Format(isoSecond),
		"line_item_product_code":          code,
		"line_item_usage_type":            r.line.UsageType,
		"line_item_operation":             "",
		"line_item_availability_zone":     "",
		"line_item_resource_id":           "",
		"line_item_usage_amount":          r.line.Usage * r.frac,
		"line_item_unblended_rate":        ratio(r.line.Unblended, r.line.Usage),
		"line_item_unblended_cost":        r.line.Unblended * r.frac,
		"line_item_blended_rate":          ratio(r.line.Blended, r.line.Usage),
		"line_item_blended_cost":          r.line.Blended * r.frac,
		"line_item_currency_code":         "USD",
		"line_item_line_item_description": r.line.Service + " " + r.line.UsageType + " usage",
		"line_item_legal_entity":          "Amazon Web Services, Inc.",
		"product_product_name":            r.line.Service,
		"product_region_code":             r.line.Region,
		"product_servicecode":             code,
		"pricing_unit":                    unitQuantity,
		"pricing_term":                    "OnDemand",
		"pricing_public_on_demand_cost":   r.line.Unblended * r.frac,
		"pricing_public_on_demand_rate":   ratio(r.line.Unblended, r.line.Usage),
		"product": jsonMap("product_name", r.line.Service, "region_code", r.line.Region,
			"servicecode", code),
		"resource_tags":                      "{}",
		"cost_category":                      "{}",
		"discount":                           "{}",
		"split_line_item_parent_resource_id": "",
		"split_line_item_split_usage":        r.line.Usage * r.frac,
		"split_line_item_split_usage_ratio":  1.0,
		"split_line_item_split_cost":         r.line.Unblended * r.frac,
		"split_line_item_unused_cost":        0.0,
	}
}

func focusValues(r *rowCtx) map[string]any {
	code := billingexport.ProductCode(r.line.Service)

	return map[string]any{
		"BilledCost":          r.line.Blended * r.frac,
		"BillingAccountId":    r.payer,
		"BillingCurrency":     "USD",
		"BillingPeriodEnd":    r.billEnd.Format(isoSecond),
		"BillingPeriodStart":  r.billStart.Format(isoSecond),
		"ChargeCategory":      "Usage",
		"ChargeDescription":   r.line.Service + " " + r.line.UsageType + " usage",
		"ChargeFrequency":     "Usage-Based",
		"ChargePeriodEnd":     r.end.Format(isoSecond),
		"ChargePeriodStart":   r.start.Format(isoSecond),
		"ConsumedQuantity":    r.line.Usage * r.frac,
		"ConsumedUnit":        unitQuantity,
		"ContractedCost":      r.line.Unblended * r.frac,
		"ContractedUnitPrice": ratio(r.line.Unblended, r.line.Usage),
		"EffectiveCost":       r.line.Unblended * r.frac,
		"InvoiceIssuerName":   providerAWS,
		"ListCost":            r.line.Unblended * r.frac,
		"ListUnitPrice":       ratio(r.line.Unblended, r.line.Usage),
		"PricingCategory":     "Standard",
		"PricingQuantity":     r.line.Usage * r.frac,
		"PricingUnit":         unitQuantity,
		"ProviderName":        providerAWS,
		"PublisherName":       providerAWS,
		"RegionId":            r.line.Region,
		"RegionName":          r.line.Region,
		"ServiceCategory":     "Other",
		"ServiceName":         r.line.Service,
		"SubAccountId":        r.line.Account,
		"Tags":                "{}",
		"x_CostCategories":    "{}",
		"x_Discounts":         "{}",
		"x_ServiceCode":       code,
		"x_UsageType":         r.line.UsageType,
	}
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
	type key struct{ svc, region, usage, account string }

	idx := map[key]int{}

	var out []billingexport.CostLine

	for _, ln := range lines {
		k := key{ln.Service, ln.Region, ln.UsageType, ln.Account}
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
