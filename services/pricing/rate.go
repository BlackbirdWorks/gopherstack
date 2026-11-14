package pricing

import "strconv"

// OnDemandRate returns the first-tier On Demand USD price per unit of the catalog product in regionCode
// whose attributes contain every entry of match. ok is false when no product or dimension matches.
func OnDemandRate(serviceCode, regionCode string, match map[string]string, unit string) (float64, bool) {
	for _, e := range theCatalog().byService[serviceCode] {
		if e.region != regionCode || !attrsContain(e.item.Product.Attributes, match) {
			continue
		}

		for _, term := range e.item.Terms.OnDemand {
			if v, ok := firstTierRate(term, unit); ok {
				return v, true
			}
		}
	}

	return 0, false
}

func attrsContain(have, want map[string]string) bool {
	for k, v := range want {
		if have[k] != v {
			return false
		}
	}

	return true
}

func firstTierRate(term termJSON, unit string) (float64, bool) {
	for _, d := range term.PriceDimensions {
		if d.Unit != unit || d.BeginRange != "0" {
			continue
		}

		v, err := strconv.ParseFloat(d.PricePerUnit[currency], 64)
		if err == nil {
			return v, true
		}
	}

	return 0, false
}
