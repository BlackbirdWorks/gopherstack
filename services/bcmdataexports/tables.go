package bcmdataexports

import (
	"slices"
	"strings"
)

const (
	tableCUR        = "COST_AND_USAGE_REPORT"
	tableFocus      = "FOCUS_1_0_AWS"
	tableFocus12    = "FOCUS_1_2_AWS"
	tableDashboard  = "COST_AND_USAGE_DASHBOARD"
	propCapacity    = "INCLUDE_CAPACITY_RESERVATION_DATA"
	propIAM         = "INCLUDE_IAM_PRINCIPAL_DATA"
	propBillingView = "BILLING_VIEW_ARN"
	tableCarbon     = "CARBON_EMISSIONS"
	tableRecs       = "COST_OPTIMIZATION_RECOMMENDATIONS"
	propGran        = "TIME_GRANULARITY"
	propResource    = "INCLUDE_RESOURCES"
	propSplit       = "INCLUDE_SPLIT_COST_ALLOCATION_DATA"
	propDiscount    = "INCLUDE_MANUAL_DISCOUNT_COMPATIBILITY"
	propAllRecs     = "INCLUDE_ALL_RECOMMENDATIONS"
	propFilter      = "FILTER"

	granHourly  = "HOURLY"
	granMonthly = "MONTHLY"

	valTrue  = "TRUE"
	valFalse = "FALSE"

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
	// cond gates the column on a table property being TRUE; notCond removes it when that property is TRUE.
	cond    string
	notCond string
}

type tableDef struct {
	Name        string
	Description string
	Properties  []propertyDef
	Columns     []columnDef
}

func boolProp(name, desc string) propertyDef {
	return propertyDef{Name: name, Description: desc, Default: valFalse, Valid: []string{valTrue, valFalse}}
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
	return []tableDef{curTable(), focusTable(), focus12Table(), dashboardTable(), carbonTable(), recsTable()}
}

func condCols(cond, typ string, names ...string) []columnDef {
	out := cols(typ, names...)
	for i := range out {
		out[i].cond = cond
	}

	return out
}

func curTable() tableDef {
	return tableDef{
		Name:        tableCUR,
		Description: "Cost and Usage Report 2.0: line item level cost and usage for the billing account.",
		Properties: []propertyDef{
			granProp(),
			boolProp(propResource, "Include the resource ID column."),
			boolProp(propSplit, "Include split cost allocation data columns."),
			boolProp(propDiscount, "Remove the discount columns and bill discounts as separate line items."),
			{
				Name:        propCapacity,
				Description: "Specifies whether to include capacity reservation data in the data export.",
				Valid:       []string{"TRUE", "FALSE"},
			},
			{
				Name:        propBillingView,
				Description: "The Amazon Resource Name (ARN) of the billing group for this data export.",
			},
			{
				Name:        propIAM,
				Description: "Specifies whether to include Identity and Access Management (IAM) principal columns.",
				Valid:       []string{"TRUE", "FALSE"},
			},
		},
		Columns: joinCols(
			cols(typeStr, "bill_bill_type", "bill_billing_entity", "bill_billing_period_end_date",
				"bill_billing_period_start_date", "bill_invoice_id", "bill_invoicing_entity",
				"bill_payer_account_id", "bill_payer_account_name"),
			cols(typeMap, "cost_category"),
			condCols(propCapacity, typeStr, "capacity_reservation_capacity_reservation_arn",
				"capacity_reservation_capacity_reservation_status", "capacity_reservation_capacity_reservation_type"),
			withoutDiscount(cols(typeMap, "discount")),
			withoutDiscount(cols(typeDec, "discount_total_discount")),
			cols(typeDec, "discount_bundled_discount"),
			cols(typeStr, "identity_line_item_id", "identity_time_interval"),
			cols(typeStr, "line_item_usage_account_name", "line_item_availability_zone"),
			cols(typeDec, "line_item_blended_cost"),
			cols(typeStr, "line_item_blended_rate", "line_item_currency_code"),
			condCols(propIAM, typeStr, "line_item_iam_principal"),
			cols(typeStr, "line_item_legal_entity", "line_item_line_item_description", "line_item_line_item_type"),
			cols(typeDec, "line_item_net_unblended_cost"),
			cols(typeStr, "line_item_net_unblended_rate"),
			cols(typeDec, "line_item_normalization_factor", "line_item_normalized_usage_amount"),
			cols(typeStr, "line_item_operation", "line_item_product_code"),
			condCols(propResource, typeStr, "line_item_resource_id"),
			cols(typeStr, "line_item_tax_type"),
			cols(typeDec, "line_item_unblended_cost"),
			cols(typeStr, "line_item_unblended_rate", "line_item_usage_account_id"),
			cols(typeDec, "line_item_usage_amount"),
			cols(typeStr, "line_item_usage_end_date", "line_item_usage_start_date", "line_item_usage_type",
				"line_item_user_identifier"),
			cols(typeStr, "pricing_currency", "pricing_lease_contract_length", "pricing_offering_class"),
			cols(typeDec, "pricing_public_on_demand_cost"),
			cols(typeStr, "pricing_public_on_demand_rate", "pricing_purchase_option", "pricing_rate_code",
				"pricing_rate_id", "pricing_term", "pricing_unit"),
			cols(typeMap, "product"),
			cols(typeStr, "product_comment", "product_fee_code", "product_fee_description", "product_from_location",
				"product_from_location_type", "product_from_region_code", "product_instanceSKU",
				"product_instance_family", "product_instance_type", "product_location", "product_location_type",
				"product_operation", "product_pricing_unit", "product_product_family", "product_region_code",
				"product_sku", "product_servicecode", "product_to_location_type", "product_to_location",
				"product_to_region_code", "product_usagetype"),
			reservationAndSavingsColumns(),
			condCols(propSplit, typeDec, "split_line_item_actual_usage", "split_line_item_net_split_cost",
				"split_line_item_net_unused_cost"),
			condCols(propSplit, typeStr, "split_line_item_parent_resource_id"),
			condCols(propSplit, typeDec, "split_line_item_public_on_demand_split_cost",
				"split_line_item_public_on_demand_unused_cost", "split_line_item_reserved_usage",
				"split_line_item_split_cost", "split_line_item_split_usage", "split_line_item_split_usage_ratio",
				"split_line_item_unused_cost"),
			cols(typeMap, "tags"),
		),
	}
}

func reservationAndSavingsColumns() []columnDef {
	return joinCols(
		cols(typeDec, "reservation_amortized_upfront_cost_for_usage",
			"reservation_amortized_upfront_fee_for_billing_period"),
		cols(typeStr, "reservation_availability_zone"),
		cols(typeDec, "reservation_effective_cost"),
		cols(typeStr, "reservation_end_time", "reservation_modification_status"),
		cols(typeDec, "reservation_net_amortized_upfront_cost_for_usage",
			"reservation_net_amortized_upfront_fee_for_billing_period", "reservation_net_effective_cost",
			"reservation_net_recurring_fee_for_usage",
			"reservation_net_unused_amortized_upfront_fee_for_billing_period",
			"reservation_net_unused_recurring_fee", "reservation_net_upfront_value"),
		cols(typeStr, "reservation_normalized_units_per_reservation", "reservation_number_of_reservations"),
		cols(typeDec, "reservation_recurring_fee_for_usage"),
		cols(typeStr, "reservation_reservation_a_r_n", "reservation_start_time", "reservation_subscription_id",
			"reservation_total_reserved_normalized_units", "reservation_total_reserved_units",
			"reservation_units_per_reservation"),
		cols(typeDec, "reservation_unused_amortized_upfront_fee_for_billing_period",
			"reservation_unused_normalized_unit_quantity", "reservation_unused_quantity",
			"reservation_unused_recurring_fee", "reservation_upfront_value"),
		cols(typeMap, "resource_tags"),
		cols(typeDec, "savings_plan_amortized_upfront_commitment_for_billing_period"),
		cols(typeStr, "savings_plan_end_time", "savings_plan_instance_type_family"),
		cols(typeDec, "savings_plan_net_amortized_upfront_commitment_for_billing_period",
			"savings_plan_net_recurring_commitment_for_billing_period",
			"savings_plan_net_savings_plan_effective_cost"),
		cols(typeStr, "savings_plan_offering_type", "savings_plan_payment_option", "savings_plan_purchase_term"),
		cols(typeDec, "savings_plan_recurring_commitment_for_billing_period"),
		cols(typeStr, "savings_plan_region", "savings_plan_savings_plan_a_r_n"),
		cols(typeDec, "savings_plan_savings_plan_effective_cost", "savings_plan_savings_plan_rate"),
		cols(typeStr, "savings_plan_start_time"),
		cols(typeDec, "savings_plan_total_commitment_to_date", "savings_plan_used_commitment"),
	)
}

// withoutDiscount gates the columns that INCLUDE_MANUAL_DISCOUNT_COMPATIBILITY removes.
func withoutDiscount(c []columnDef) []columnDef {
	for i := range c {
		c[i].notCond = propDiscount
	}

	return c
}

var focusDoubleColumns = map[string]bool{ //nolint:gochecknoglobals // immutable lookup
	"BilledCost": true, "CommitmentDiscountQuantity": true, "ConsumedQuantity": true, "ContractedCost": true,
	"ContractedUnitPrice": true, "EffectiveCost": true, "ListCost": true, "ListUnitPrice": true,
	"PricingCurrencyContractedUnitPrice": true, "PricingCurrencyEffectiveCost": true,
	"PricingCurrencyListUnitPrice": true, "PricingQuantity": true,
}

var focusMapColumns = map[string]bool{ //nolint:gochecknoglobals // immutable lookup
	"Tags": true, "x_CostCategories": true, "x_Discounts": true, "SkuPriceDetails": true,
}

func focusCols(names ...string) []columnDef {
	out := make([]columnDef, len(names))

	for i, n := range names {
		typ := typeStr

		switch {
		case focusDoubleColumns[n]:
			typ = typeDec
		case focusMapColumns[n]:
			typ = typeMap
		}

		out[i] = columnDef{Name: n, Type: typ, Description: n}
	}

	return out
}

func focusTable() tableDef {
	return tableDef{
		Name:        tableFocus,
		Description: "FinOps Open Cost and Usage Specification (FOCUS) 1.0 with AWS columns.",
		Properties:  []propertyDef{granProp()},
		Columns: focusCols(
			"AvailabilityZone", "BilledCost", "BillingAccountId", "BillingAccountName", "BillingCurrency",
			"BillingPeriodEnd", "BillingPeriodStart", "ChargeCategory", "ChargeClass", "ChargeDescription",
			"ChargeFrequency", "ChargePeriodEnd", "ChargePeriodStart", "CommitmentDiscountCategory",
			"CommitmentDiscountId", "CommitmentDiscountName", "CommitmentDiscountStatus", "CommitmentDiscountType",
			"ConsumedQuantity", "ConsumedUnit", "ContractedCost", "ContractedUnitPrice", "EffectiveCost",
			"InvoiceIssuerName", "ListCost", "ListUnitPrice", "PricingCategory", "PricingQuantity", "PricingUnit",
			"ProviderName", "PublisherName", "RegionId", "RegionName", "ResourceId", "ResourceName", "ResourceType",
			"ServiceCategory", "ServiceName", "SkuId", "SkuPriceId", "SubAccountId", "SubAccountName", "Tags",
			"x_CostCategories", "x_Discounts", "x_Operation", "x_ServiceCode", "x_UsageType",
		),
	}
}

func focus12Table() tableDef {
	return tableDef{
		Name:        tableFocus12,
		Description: "FinOps Open Cost and Usage Specification (FOCUS) 1.2 with AWS columns.",
		Properties:  []propertyDef{granProp()},
		Columns: focusCols(
			"AvailabilityZone", "BilledCost", "BillingAccountId", "BillingAccountName", "BillingAccountType",
			"BillingCurrency", "BillingPeriodEnd", "BillingPeriodStart", "CapacityReservationId",
			"CapacityReservationStatus", "ChargeCategory", "ChargeClass", "ChargeDescription", "ChargeFrequency",
			"ChargePeriodEnd", "ChargePeriodStart", "CommitmentDiscountCategory", "CommitmentDiscountId",
			"CommitmentDiscountName", "CommitmentDiscountQuantity", "CommitmentDiscountStatus",
			"CommitmentDiscountType", "CommitmentDiscountUnit", "ConsumedQuantity", "ConsumedUnit", "ContractedCost",
			"ContractedUnitPrice", "EffectiveCost", "InvoiceId", "InvoiceIssuerName", "ListCost", "ListUnitPrice",
			"PricingCategory", "PricingCurrency", "PricingCurrencyContractedUnitPrice",
			"PricingCurrencyEffectiveCost", "PricingCurrencyListUnitPrice", "PricingQuantity", "PricingUnit",
			"ProviderName", "PublisherName", "RegionId", "RegionName", "ResourceId", "ResourceName", "ResourceType",
			"ServiceCategory", "ServiceName", "ServiceSubcategory", "SkuId", "SkuMeter", "SkuPriceId",
			"SkuPriceDetails", "SubAccountId", "SubAccountName", "SubAccountType", "Tags", "x_Discounts",
			"x_Operation", "x_ServiceCode",
		),
	}
}

func dashboardTable() tableDef {
	return tableDef{
		Name:        tableDashboard,
		Description: "A view over CUR for CostAndUsageDashboard",
		Columns: joinCols(
			cols(typeDec, "amortized_cost"),
			cols(typeStr, "availability_zone", "billing_entity", "billing_period", "charge_category", "charge_type",
				"current_generation", "database_engine", "instance_type", "instance_type_family", "invoice_id",
				"item_description", "legal_entity", "linked_account_id", "linked_account_name", "operation",
				"payer_account_id", "payer_account_name", "platform", "pricing_unit", "processor",
				"processor_features", "product_code", "product_family", "product_from_location", "product_group",
				"product_name", "product_to_location"),
			cols(typeDec, "public_cost"),
			cols(typeStr, "purchase_option", "region", "ri_sp_arn"),
			cols(typeDec, "ri_sp_trueup", "ri_sp_upfront_fee"),
			cols(typeStr, "service", "tenancy"),
			cols(typeDec, "unblended_cost"),
			cols(typeStr, "usage_date"),
			cols(typeDec, "usage_quantity"),
			cols(typeStr, "usage_type"),
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
		if (c.cond == "" || props[c.cond] == valTrue) && props[c.notCond] != valTrue {
			out = append(out, c)
		}
	}

	return out
}
