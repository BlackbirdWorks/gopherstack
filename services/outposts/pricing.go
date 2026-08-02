package outposts

// pricing.go implements a deterministic, clearly-synthetic placeholder
// pricing model used by CreateQuote/CreateRenewal/GetRenewalPricing. These
// numbers are NOT real AWS Outposts pricing: there is no public,
// machine-readable source for actual Outposts subscription pricing in this
// SDK module or anywhere else in this repo (unlike, say, an EC2 instance
// type's vCPU count). The formula exists purely so the wire-accurate,
// correctly-typed Currency/MonthlyRecurringPrice/UpfrontPrice fields carry
// internally-consistent, deterministic values instead of zeros -- see
// PARITY.md's "Quote/renewal pricing model" note.

const (
	basePriceOneYear    float32 = 12000
	basePriceThreeYears float32 = 30000
	basePriceFiveYears  float32 = 45000

	monthsPerYear = 12

	upfrontFractionAll     float32 = 1
	upfrontFractionPartial float32 = 0.5

	termYearsThree = 3
	termYearsFive  = 5
)

func basePriceForTerm(term string) float32 {
	switch term {
	case PaymentTermThreeYears:
		return basePriceThreeYears
	case PaymentTermFiveYears:
		return basePriceFiveYears
	default: // ONE_YEAR and unknown/unspecified
		return basePriceOneYear
	}
}

func upfrontFraction(option string) float32 {
	switch option {
	case PaymentOptionAllUpfront:
		return upfrontFractionAll
	case PaymentOptionPartialUpfront:
		return upfrontFractionPartial
	default: // NO_UPFRONT and unknown/unspecified
		return 0
	}
}

func termYears(term string) int {
	switch term {
	case PaymentTermThreeYears:
		return termYearsThree
	case PaymentTermFiveYears:
		return termYearsFive
	default: // ONE_YEAR and unknown/unspecified
		return 1
	}
}

// computePricing returns (monthlyRecurringPrice, upfrontPrice) for one
// PaymentOption/PaymentTerm combination under this synthetic model.
func computePricing(option, term string) (float32, float32) {
	base := basePriceForTerm(term)
	upfrontAmt := base * upfrontFraction(option)
	remaining := base - upfrontAmt
	months := float32(termYears(term) * monthsPerYear)

	var monthlyAmt float32
	if months > 0 {
		monthlyAmt = remaining / months
	}

	return monthlyAmt, upfrontAmt
}

// defaultPaymentOptions/defaultPaymentTerms are the values CreateQuote uses
// when the caller omits RequestedPaymentOptions/RequestedPaymentTerms --
// matching the SDK doc comment: "If not specified, all available payment
// options [terms] are returned."
//
//nolint:gochecknoglobals // static reference data
var defaultPaymentOptions = []string{PaymentOptionAllUpfront, PaymentOptionPartialUpfront, PaymentOptionNoUpfront}

//nolint:gochecknoglobals // static reference data
var defaultPaymentTerms = []string{PaymentTermOneYear, PaymentTermThreeYears, PaymentTermFiveYears}

// buildPricingOptions computes one PricingOption per (option, term)
// combination in the cross product of options x terms.
func buildPricingOptions(options, terms []string) []PricingOption {
	if len(options) == 0 {
		options = defaultPaymentOptions
	}

	if len(terms) == 0 {
		terms = defaultPaymentTerms
	}

	out := make([]PricingOption, 0, len(options)*len(terms))

	for _, opt := range options {
		for _, term := range terms {
			monthly, upfront := computePricing(opt, term)
			out = append(out, PricingOption{
				Currency:              currencyUSD,
				PaymentOption:         opt,
				PaymentTerm:           term,
				MonthlyRecurringPrice: monthly,
				UpfrontPrice:          upfront,
			})
		}
	}

	return out
}
