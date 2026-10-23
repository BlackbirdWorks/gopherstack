package cur

import "github.com/blackbirdworks/gopherstack/pkgs/billingexport"

// blank is a column the cost ledger has no data for; its cells are empty, as in a CUR row it does not apply to.
func blank(category, name, typ string) colDef {
	return col(category, name, typ, func(*rowCtx) any { return nil })
}

type colSpec struct{ name, typ string }

func blankGroup(category string, specs []colSpec) []colDef {
	out := make([]colDef, len(specs))
	for i, s := range specs {
		out[i] = blank(category, s.name, s.typ)
	}

	return out
}

func pricingColumns() []colDef {
	return []colDef{
		blank("pricing", "RateCode", typeString),
		blank("pricing", "RateId", typeString),
		col("pricing", "currency", typeString, func(*rowCtx) any { return "USD" }),
		col("pricing", "publicOnDemandCost", typeDouble, func(r *rowCtx) any { return r.line.Unblended * r.frac }),
		col("pricing", "publicOnDemandRate", typeString, func(r *rowCtx) any {
			return rateString(r.line.Unblended, r.line.Usage)
		}),
		col("pricing", "term", typeString, func(*rowCtx) any { return "OnDemand" }),
		blank("pricing", "LeaseContractLength", typeString),
		blank("pricing", "OfferingClass", typeString),
		blank("pricing", "PurchaseOption", typeString),
	}
}

var reservationColumns = []colSpec{ //nolint:gochecknoglobals // immutable column dictionary
	{"AmortizedUpfrontCostForUsage", typeDouble}, {"AmortizedUpfrontFeeForBillingPeriod", typeDouble},
	{"AvailabilityZone", typeString}, {"EffectiveCost", typeDouble}, {"EndTime", typeString},
	{"ModificationStatus", typeString}, {"NormalizedUnitsPerReservation", typeString},
	{"NumberOfReservations", typeString}, {"RecurringFeeForUsage", typeDouble}, {"ReservationARN", typeString},
	{"StartTime", typeString}, {"SubscriptionId", typeString}, {"TotalReservedNormalizedUnits", typeString},
	{"TotalReservedUnits", typeString}, {"UnitsPerReservation", typeString},
	{"UnusedAmortizedUpfrontFeeForBillingPeriod", typeDouble}, {"UnusedNormalizedUnitQuantity", typeDouble},
	{"UnusedQuantity", typeDouble}, {"UnusedRecurringFee", typeDouble}, {"UpfrontValue", typeDouble},
}

var savingsPlanColumns = []colSpec{ //nolint:gochecknoglobals // immutable column dictionary
	{"TotalCommitmentToDate", typeDouble}, {"SavingsPlanARN", typeString}, {"SavingsPlanRate", typeDouble},
	{"UsedCommitment", typeDouble}, {"SavingsPlanEffectiveCost", typeDouble},
	{"AmortizedUpfrontCommitmentForBillingPeriod", typeDouble},
	{"RecurringCommitmentForBillingPeriod", typeDouble}, {"StartTime", typeString}, {"EndTime", typeString},
	{"OfferingType", typeString}, {"PaymentOption", typeString}, {"PurchaseTerm", typeString},
	{"Region", typeString},
}

var discountColumns = []colSpec{ //nolint:gochecknoglobals // immutable column dictionary
	{"EdpDiscount", typeDouble}, {"TotalDiscount", typeDouble}, {"BundledDiscount", typeDouble},
}

// dataStatusTable is the one-row table Athena-integrated reports publish next to the data.
func dataStatusTable() *billingexport.Table {
	return &billingexport.Table{
		Columns: []billingexport.Column{{Name: "status", Type: typeString}},
		Rows:    [][]any{{"READY"}},
	}
}
