package bcmdataexports

import (
	"slices"
	"strings"
)

const (
	tableCUR     = "COST_AND_USAGE_REPORT"
	tableFocus   = "FOCUS_1_0_AWS"
	tableCarbon  = "CARBON_EMISSIONS"
	tableRecs    = "COST_OPTIMIZATION_RECOMMENDATIONS"
	propGran     = "TIME_GRANULARITY"
	propResource = "INCLUDE_RESOURCES"
	propSplit    = "INCLUDE_SPLIT_COST_ALLOCATION_DATA"
	propDiscount = "INCLUDE_MANUAL_DISCOUNT_COMPATIBILITY"
	propAllRecs  = "INCLUDE_ALL_RECOMMENDATIONS"
	propFilter   = "FILTER"

	granHourly  = "HOURLY"
	granMonthly = "MONTHLY"

	typeStr = "String"
	typeDec = "Decimal"
	typeMap = "Map"
)

type propertyDef struct {
	Name        string
	Description string
	Default     string
	Valid       []string
}

type columnDef struct {
	Name        string
	Type        string
	Description string
	// cond gates the column on a table property being TRUE.
	cond string
}

type tableDef struct {
	Name        string
	Description string
	Properties  []propertyDef
	Columns     []columnDef
}

func boolProp(name, desc string) propertyDef {
	return propertyDef{Name: name, Description: desc, Default: "FALSE", Valid: []string{"TRUE", "FALSE"}}
}

func granProp() propertyDef {
	return propertyDef{
		Name: propGran, Description: "Time granularity of the table rows.", Default: granHourly,
		Valid: []string{granHourly, "DAILY", granMonthly},
	}
}

func cols(typ string, names ...string) []columnDef {
	out := make([]columnDef, len(names))
	for i, n := range names {
		out[i] = columnDef{Name: n, Type: typ, Description: strings.ReplaceAll(n, "_", " ")}
	}

	return out
}

func joinCols(parts ...[]columnDef) []columnDef {
	var out []columnDef
	for _, p := range parts {
		out = append(out, p...)
	}

	return out
}

func tableDefs() []tableDef {
	return []tableDef{curTable(), focusTable(), carbonTable(), recsTable()}
}

func curTable() tableDef {
	return tableDef{
		Name:        tableCUR,
		Description: "Cost and Usage Report 2.0: line item level cost and usage for the billing account.",
		Properties: []propertyDef{
			granProp(),
			boolProp(propResource, "Include the resource ID column."),
			boolProp(propSplit, "Include split cost allocation data columns."),
			boolProp(propDiscount, "Include manual discount compatibility."),
		},
		Columns: joinCols(
			cols(
				typeStr,
				"identity_line_item_id",
				"identity_time_interval",
				"bill_invoice_id",
				"bill_invoicing_entity",
				"bill_billing_entity",
				"bill_bill_type",
				"bill_payer_account_id",
				"bill_billing_period_start_date",
				"bill_billing_period_end_date",
				"line_item_usage_account_id",
				"line_item_line_item_type",
				"line_item_usage_start_date",
				"line_item_usage_end_date",
				"line_item_product_code",
				"line_item_usage_type",
				"line_item_operation",
				"line_item_availability_zone",
			),
			[]columnDef{
				{Name: "line_item_resource_id", Type: typeStr, Description: "resource id", cond: propResource},
			},
			cols(typeDec, "line_item_usage_amount", "line_item_unblended_rate", "line_item_unblended_cost",
				"line_item_blended_rate", "line_item_blended_cost"),
			cols(
				typeStr,
				"line_item_currency_code",
				"line_item_line_item_description",
				"line_item_legal_entity",
				"product_product_name",
				"product_region_code",
				"product_servicecode",
				"pricing_unit",
				"pricing_term",
			),
			cols(typeDec, "pricing_public_on_demand_cost", "pricing_public_on_demand_rate"),
			cols(typeMap, "product", "resource_tags", "cost_category", "discount"),
			[]columnDef{
				{
					Name:        "split_line_item_parent_resource_id",
					Type:        typeStr,
					Description: "parent resource id",
					cond:        propSplit,
				},
				{Name: "split_line_item_split_usage", Type: typeDec, Description: "split usage", cond: propSplit},
				{
					Name:        "split_line_item_split_usage_ratio",
					Type:        typeDec,
					Description: "split usage ratio",
					cond:        propSplit,
				},
				{Name: "split_line_item_split_cost", Type: typeDec, Description: "split cost", cond: propSplit},
				{Name: "split_line_item_unused_cost", Type: typeDec, Description: "unused cost", cond: propSplit},
			},
		),
	}
}

func focusTable() tableDef {
	return tableDef{
		Name:        tableFocus,
		Description: "FinOps Open Cost and Usage Specification (FOCUS) 1.0 with AWS extensions.",
		Properties:  []propertyDef{granProp()},
		Columns: joinCols(
			cols(typeDec, "BilledCost"),
			cols(typeStr, "BillingAccountId", "BillingAccountName", "BillingCurrency", "BillingPeriodEnd",
				"BillingPeriodStart", "ChargeCategory", "ChargeClass", "ChargeDescription", "ChargeFrequency",
				"ChargePeriodEnd", "ChargePeriodStart", "CommitmentDiscountCategory", "CommitmentDiscountId",
				"CommitmentDiscountName", "CommitmentDiscountType"),
			cols(typeDec, "ConsumedQuantity"),
			cols(typeStr, "ConsumedUnit"),
			cols(typeDec, "ContractedCost", "ContractedUnitPrice", "EffectiveCost"),
			cols(typeStr, "InvoiceIssuerName"),
			cols(typeDec, "ListCost", "ListUnitPrice"),
			cols(typeStr, "PricingCategory"),
			cols(typeDec, "PricingQuantity"),
			cols(typeStr, "PricingUnit", "ProviderName", "PublisherName", "RegionId", "RegionName", "ResourceId",
				"ResourceName", "ResourceType", "ServiceCategory", "ServiceName", "SkuId", "SkuPriceId",
				"SubAccountId", "SubAccountName"),
			cols(typeMap, "Tags"),
			cols(typeStr, "x_CostCategories", "x_Discounts", "x_Operation", "x_ServiceCode", "x_UsageType"),
		),
	}
}

func carbonTable() tableDef {
	return tableDef{
		Name:        tableCarbon,
		Description: "Estimated carbon emissions of AWS usage.",
		Columns: joinCols(
			cols(typeStr, "last_refresh_timestamp", "location", "model_version", "payer_account_id", "product_code",
				"region_code", "usage_account_id", "usage_period_start", "usage_period_end"),
			cols(typeDec, "total_lbm_emissions_value", "total_mbm_emissions_value", "total_scope_1_emissions_value",
				"total_scope_2_lbm_emissions_value", "total_scope_2_mbm_emissions_value",
				"total_scope_3_lbm_emissions_value", "total_scope_3_mbm_emissions_value"),
			cols(typeStr, "total_lbm_emissions_unit", "total_mbm_emissions_unit"),
		),
	}
}

func recsTable() tableDef {
	return tableDef{
		Name:        tableRecs,
		Description: "Cost optimization recommendations across AWS accounts.",
		Properties: []propertyDef{
			boolProp(
				propAllRecs,
				"Include all recommendations rather than only the highest-savings one per resource.",
			),
			{Name: propFilter, Description: "JSON filter applied to the recommendations.", Default: "{}"},
		},
		Columns: joinCols(
			cols(
				typeStr,
				"account_id",
				"action_type",
				"currency_code",
				"current_resource_details",
				"current_resource_summary",
				"current_resource_type",
				"implementation_effort",
				"last_refresh_timestamp",
				"recommendation_id",
				"recommended_resource_details",
				"recommended_resource_summary",
				"recommended_resource_type",
				"region",
				"resource_arn",
				"restart_needed",
				"rollback_possible",
			),
			cols(typeDec, "estimated_monthly_cost_after_discount", "estimated_monthly_cost_before_discount",
				"estimated_monthly_savings_after_discount", "estimated_monthly_savings_before_discount",
				"estimated_savings_percentage_after_discount", "estimated_savings_percentage_before_discount",
				"recommendation_lookback_period_in_days"),
			cols(typeMap, "tags"),
		),
	}
}

func findTable(name string) (tableDef, bool) {
	for _, t := range tableDefs() {
		if t.Name == name {
			return t, true
		}
	}

	return tableDef{}, false
}

// resolveProps validates props against the table and fills defaults.
func (t tableDef) resolveProps(props map[string]string) (map[string]string, error) {
	out := make(map[string]string, len(t.Properties))
	for _, p := range t.Properties {
		out[p.Name] = p.Default
	}

	for k, v := range props {
		i := slices.IndexFunc(t.Properties, func(p propertyDef) bool { return p.Name == k })
		if i < 0 {
			return nil, fail(ErrValidation, "table %s has no property %q", t.Name, k)
		}

		if valid := t.Properties[i].Valid; valid != nil && !slices.Contains(valid, v) {
			return nil, fail(ErrValidation, "value %q is invalid for property %s", v, k)
		}

		out[k] = v
	}

	return out, nil
}

// columnsFor returns the columns present under the resolved props.
func (t tableDef) columnsFor(props map[string]string) []columnDef {
	var out []columnDef

	for _, c := range t.Columns {
		if c.cond == "" || props[c.cond] == "TRUE" {
			out = append(out, c)
		}
	}

	return out
}
