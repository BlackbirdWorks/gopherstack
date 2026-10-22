package cur

import (
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/billingexport"
)

const (
	isoSecond    = "2006-01-02T15:04:05Z"
	manifestTime = "20060102T150405.000Z"
	typeString   = billingexport.TypeString
	typeDouble   = billingexport.TypeDouble
	hoursPerDay  = 24
	catLineItem  = "lineItem"
	catBill      = "bill"
	catProduct   = "product"
	catSplit     = "splitLineItem"
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

type colDef struct {
	val      func(r *rowCtx) any
	category string
	name     string
	typ      string
}

func col(category, name, typ string, val func(r *rowCtx) any) colDef {
	return colDef{category: category, name: name, typ: typ, val: val}
}

func (c colDef) legacy() string { return c.category + "/" + c.name }

func rate(cost, usage float64) any {
	if usage == 0 {
		return nil
	}

	return cost / usage
}

func lineItemID(r *rowCtx) string {
	h := fnv.New128a()
	_, _ = fmt.Fprintf(h, "%s|%s|%s|%s|%s", r.line.Account, r.line.Service, r.line.UsageType, r.line.Region, r.start)

	return hex.EncodeToString(h.Sum(nil))
}

func baseColumns() []colDef {
	return slices.Concat([]colDef{
		col("identity", "LineItemId", typeString, func(r *rowCtx) any { return lineItemID(r) }),
		col("identity", "TimeInterval", typeString, func(r *rowCtx) any {
			return r.start.Format(isoSecond) + "/" + r.end.Format(isoSecond)
		}),
		col(catBill, "InvoiceId", typeString, func(*rowCtx) any { return "" }),
		col(catBill, "BillingEntity", typeString, func(*rowCtx) any { return "AWS" }),
		col(catBill, "InvoicingEntity", typeString, func(*rowCtx) any { return "Amazon Web Services, Inc." }),
		col(catBill, "BillType", typeString, func(*rowCtx) any { return "Anniversary" }),
		col(catBill, "PayerAccountId", typeString, func(r *rowCtx) any { return r.payer }),
		col(
			catBill,
			"BillingPeriodStartDate",
			typeString,
			func(r *rowCtx) any { return r.billStart.Format(isoSecond) },
		),
		col(catBill, "BillingPeriodEndDate", typeString, func(r *rowCtx) any { return r.billEnd.Format(isoSecond) }),
		col(catLineItem, "UsageAccountId", typeString, func(r *rowCtx) any { return r.line.Account }),
		col(catLineItem, "LineItemType", typeString, func(*rowCtx) any { return "Usage" }),
		col(catLineItem, "LegalEntity", typeString, func(*rowCtx) any { return "Amazon Web Services, Inc." }),
		col(catLineItem, "UsageStartDate", typeString, func(r *rowCtx) any { return r.start.Format(isoSecond) }),
		col(catLineItem, "UsageEndDate", typeString, func(r *rowCtx) any { return r.end.Format(isoSecond) }),
		col(
			catLineItem,
			"ProductCode",
			typeString,
			func(r *rowCtx) any { return billingexport.ProductCode(r.line.Service) },
		),
		col(catLineItem, "UsageType", typeString, func(r *rowCtx) any { return r.line.UsageType }),
		blank(catLineItem, "Operation", typeString),
		blank(catLineItem, "AvailabilityZone", typeString),
		col(catLineItem, "UsageAmount", typeDouble, func(r *rowCtx) any { return r.line.Usage * r.frac }),
		blank(catLineItem, "NormalizationFactor", typeDouble),
		blank(catLineItem, "NormalizedUsageAmount", typeDouble),
		col(catLineItem, "CurrencyCode", typeString, func(*rowCtx) any { return "USD" }),
		col(
			catLineItem,
			"UnblendedRate",
			typeString,
			func(r *rowCtx) any { return rateString(r.line.Unblended, r.line.Usage) },
		),
		col(catLineItem, "UnblendedCost", typeDouble, func(r *rowCtx) any { return r.line.Unblended * r.frac }),
		col(
			catLineItem,
			"BlendedRate",
			typeString,
			func(r *rowCtx) any { return rateString(r.line.Blended, r.line.Usage) },
		),
		col(catLineItem, "BlendedCost", typeDouble, func(r *rowCtx) any { return r.line.Blended * r.frac }),
		col(catLineItem, "LineItemDescription", typeString, func(r *rowCtx) any {
			return fmt.Sprintf("%s %s usage", r.line.Service, r.line.UsageType)
		}),
		blank(catLineItem, "TaxType", typeString),
		col(catProduct, "ProductName", typeString, func(r *rowCtx) any { return r.line.Service }),
		blank(catProduct, "instanceType", typeString),
		blank(catProduct, "location", typeString),
		blank(catProduct, "locationType", typeString),
		blank(catProduct, "productFamily", typeString),
		col(catProduct, "servicename", typeString, func(r *rowCtx) any { return r.line.Service }),
		blank(catProduct, "sku", typeString),
		col(catProduct, "usagetype", typeString, func(r *rowCtx) any { return r.line.UsageType }),
		col(catProduct, "region", typeString, func(r *rowCtx) any { return r.line.Region }),
		col(
			catProduct,
			"servicecode",
			typeString,
			func(r *rowCtx) any { return billingexport.ProductCode(r.line.Service) },
		),
		col("pricing", "unit", typeString, func(*rowCtx) any { return "Quantity" }),
	}, pricingColumns(), blankGroup("reservation", reservationColumns), blankGroup("savingsPlan", savingsPlanColumns),
		blankGroup("discount", discountColumns))
}

func rateString(cost, usage float64) any {
	v := rate(cost, usage)
	if v == nil {
		return ""
	}

	return fmt.Sprintf("%.10f", v)
}

func columnsFor(elements []string) []colDef {
	cols := baseColumns()

	for _, e := range elements {
		switch e {
		case schemaResources:
			cols = append(cols, col(catLineItem, "ResourceId", typeString, func(*rowCtx) any { return "" }))
		case schemaSplit:
			cols = append(cols,
				col(catSplit, "ParentResourceId", typeString, func(*rowCtx) any { return "" }),
				col(catSplit, "SplitUsage", typeDouble, func(r *rowCtx) any { return r.line.Usage * r.frac }),
				col(catSplit, "SplitUsageRatio", typeDouble, func(*rowCtx) any { return 1.0 }),
				col(catSplit, "SplitCost", typeDouble, func(r *rowCtx) any { return r.line.Unblended * r.frac }),
				col(catSplit, "UnusedCost", typeDouble, func(*rowCtx) any { return 0.0 }),
			)
		}
	}

	return cols
}

// buildTable renders the report rows for one billing period.
func buildTable(
	r *Report,
	lines []billingexport.CostLine,
	billStart, billEnd time.Time,
	payer string,
) *billingexport.Table {
	defs := columnsFor(r.AdditionalSchemaElements)
	t := &billingexport.Table{Columns: make([]billingexport.Column, len(defs))}

	for i, d := range defs {
		name := d.legacy()
		if r.Format == formatParquet {
			name = billingexport.SnakeCase(name)
		}

		t.Columns[i] = billingexport.Column{Name: name, Type: d.typ}
	}

	if r.TimeUnit == timeMonthly {
		lines = aggregateMonthly(lines, billStart)
	}

	for _, ln := range lines {
		for _, rc := range intervals(r.TimeUnit, &ln, billEnd) {
			rc.billStart, rc.billEnd, rc.payer = billStart, billEnd, payer
			row := make([]any, len(defs))

			for i, d := range defs {
				row[i] = d.val(&rc)
			}

			t.Rows = append(t.Rows, row)
		}
	}

	return t
}

func intervals(unit string, ln *billingexport.CostLine, billEnd time.Time) []rowCtx {
	day := ln.Day.UTC().Truncate(dayLen)

	switch unit {
	case timeHourly:
		out := make([]rowCtx, hoursPerDay)
		for h := range out {
			s := day.Add(time.Duration(h) * time.Hour)
			out[h] = rowCtx{line: *ln, start: s, end: s.Add(time.Hour), frac: 1.0 / hoursPerDay}
		}

		return out
	case timeMonthly:
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
