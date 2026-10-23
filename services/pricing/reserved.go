package pricing

const (
	lease1yr       = "1yr"
	lease3yr       = "3yr"
	hoursPerYear   = 8760
	partialShare   = 0.5
	partialRatio   = 0.95
	allUpfrontRate = 0.9
	convertibleMul = 1.12
	upfrontUnit    = "Quantity"
	optNoUpfront   = "No Upfront"
	optPartial     = "Partial Upfront"
	optAllUpfront  = "All Upfront"
	classStandard  = "standard"
	classConvert   = "convertible"
)

// reservedOffer is one Reserved term; ratio is the all-in fraction of the On Demand hourly rate.
type reservedOffer struct {
	code, lease, class, option string
	ratio                      float64
	years                      int
}

// reservedOffers are the twelve EC2/RDS-style Reserved terms; the ratios are illustrative.
var reservedOffers = []reservedOffer{ //nolint:gochecknoglobals // immutable catalog input
	{reserved1yTerm, lease1yr, classStandard, optNoUpfront, reserved1yRatio, 1},
	{"HU7G6KETJZ", lease1yr, classStandard, optPartial, reserved1yRatio * partialRatio, 1},
	{"6QCMYABX3D", lease1yr, classStandard, optAllUpfront, reserved1yRatio * allUpfrontRate, 1},
	{reserved3yTerm, lease3yr, classStandard, optNoUpfront, reserved3yRatio, 3},
	{"R5XV2EPZQZ", lease3yr, classStandard, optPartial, reserved3yRatio * partialRatio, 3},
	{"BPH4J8HBKS", lease3yr, classStandard, optAllUpfront, reserved3yRatio * allUpfrontRate, 3},
	{"7NE97W5U4E", lease1yr, classConvert, optNoUpfront, reserved1yRatio * convertibleMul, 1},
	{"Z2E3P23VKM", lease1yr, classConvert, optPartial, reserved1yRatio * convertibleMul * partialRatio, 1},
	{"CUZHX8X6JH", lease1yr, classConvert, optAllUpfront, reserved1yRatio * convertibleMul * allUpfrontRate, 1},
	{"Q7UJUT2CE6", lease3yr, classConvert, optNoUpfront, reserved3yRatio * convertibleMul, 3},
	{"VJWZNREJX2", lease3yr, classConvert, optPartial, reserved3yRatio * convertibleMul * partialRatio, 3},
	{"MZU6U2429S", lease3yr, classConvert, optAllUpfront, reserved3yRatio * convertibleMul * allUpfrontRate, 3},
}

func reservedAttrs(o reservedOffer) map[string]string {
	return map[string]string{"LeaseContractLength": o.lease, "OfferingClass": o.class, "PurchaseOption": o.option}
}

// reservedDims splits the offer's all-in hourly cost between the recurring rate and the upfront fee.
func reservedDims(o reservedOffer, hrs []dim) []dim {
	if len(hrs) == 0 {
		return nil
	}

	total := hrs[0].usd * o.ratio

	upfrontShare := map[string]float64{optNoUpfront: 0, optPartial: partialShare, optAllUpfront: 1}[o.option]
	fee := total * upfrontShare * float64(hoursPerYear*o.years)

	out := make([]dim, 0, len(hrs)+1)

	for _, d := range hrs {
		out = append(out, dim{unit: d.unit, desc: d.desc, begin: d.begin, end: d.end, usd: total * (1 - upfrontShare)})
	}

	if o.option != optNoUpfront {
		out = append(out, dim{unit: upfrontUnit, desc: "Upfront Fee", begin: "0", end: infinity, usd: fee})
	}

	return out
}

func reservedTerms(sku string, dims []dim) map[string]termJSON {
	hrs := hourly(dims)
	out := make(map[string]termJSON, len(reservedOffers))

	for _, o := range reservedOffers {
		out[sku+"."+o.code] = buildTerm(sku, o.code, reservedAttrs(o), reservedDims(o, hrs), 1)
	}

	return out
}
