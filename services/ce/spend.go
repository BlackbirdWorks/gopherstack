package ce

import "strings"

// budgetFilterDims maps Budgets cost-filter keys onto the ledger dimensions this backend models.
var budgetFilterDims = map[string]string{ //nolint:gochecknoglobals // immutable lookup
	"Service":       dimKeyService,
	mapKeyRegion:    dimKeyRegion,
	"UsageType":     dimKeyUsageType,
	"LinkedAccount": dimKeyLinkedAccount,
}

// LedgerSpend sums metric over ledger entries with start <= date < end (YYYY-MM-DD).
// filters uses Budgets CostFilters keys; keys the ledger does not model do not narrow.
func (b *InMemoryBackend) LedgerSpend(start, end string, filters map[string][]string, metric string) float64 {
	b.mu.RLock("LedgerSpend")
	defer b.mu.RUnlock()

	total := 0.0

	for _, e := range b.costLedgerInBucket(start, end) {
		if ledgerMatches(e, filters) {
			total += getMetricValue(e, metric)
		}
	}

	return total
}

func ledgerMatches(e CostEntry, filters map[string][]string) bool {
	for key, vals := range filters {
		if len(vals) == 0 {
			continue
		}

		if key == "TagKeyValue" {
			if !tagKeyValueMatches(e, vals) {
				return false
			}

			continue
		}

		dim, ok := budgetFilterDims[key]
		if !ok {
			continue
		}

		v, ok := dimensionFieldValue(e, dim)
		if ok && !stringSliceContainsFold(vals, v) {
			return false
		}
	}

	return true
}

func tagKeyValueMatches(e CostEntry, vals []string) bool {
	for _, kv := range vals {
		k, v, _ := strings.Cut(strings.TrimPrefix(kv, "user:"), "$")
		if e.Tags[k] == v {
			return true
		}
	}

	return false
}
