package ce

import "strings"

// ExcludeChargeTypeFilter is the LedgerSpend filter key listing charge types to leave out.
const ExcludeChargeTypeFilter = "ExcludeChargeType"

const (
	chargeTypeUsage    = "Usage"
	purchaseOnDemand   = "On Demand"
	billingEntityAWS   = "AWS"
	invoicingEntityAWS = "Amazon Web Services, Inc."
)

func entryPurchaseType(e CostEntry) string {
	if e.PurchaseType == "" {
		return purchaseOnDemand
	}

	return e.PurchaseType
}

func entryChargeType(e CostEntry) string {
	if e.ChargeType == "" {
		return chargeTypeUsage
	}

	return e.ChargeType
}

// budgetFilterDims maps Budgets cost-filter keys onto the ledger dimensions this backend models.
var budgetFilterDims = map[string]string{ //nolint:gochecknoglobals // immutable lookup
	"Service":          dimKeyService,
	mapKeyRegion:       dimKeyRegion,
	"UsageType":        dimKeyUsageType,
	"LinkedAccount":    dimKeyLinkedAccount,
	"AZ":               dimKeyAZ,
	mapKeyInstanceType: dimKeyInstanceType,
	"Operation":        dimKeyOperation,
	"PurchaseType":     dimKeyPurchaseType,
	"UsageTypeGroup":   dimKeyUsageTypeGroup,
	"BillingEntity":    dimKeyBillingEntity,
	"InvoicingEntity":  dimKeyInvoicingEntity,
	"LegalEntityName":  dimKeyLegalEntity,
	"SubscriptionId":   dimKeySubscriptionID,
}

// costCategoryFilterKey is the Budgets CostFilters key whose values are "<category name>$<category value>".
const costCategoryFilterKey = "CostCategory"

// categoryResolver returns the value a cost category assigns to an entry; empty when none applies.
type categoryResolver func(name string, e *CostEntry) string

func (b *InMemoryBackend) categoryResolverLocked() categoryResolver {
	cats := make(map[string]*CostCategory, b.costCategories.Len())
	for _, c := range b.costCategories.All() {
		cats[c.Name] = c
	}

	return func(name string, e *CostEntry) string { return assignCategory(cats, name, e, 0) }
}

func costCategoryFilterMatches(e CostEntry, vals []string, cat categoryResolver) bool {
	for _, kv := range vals {
		name, want, ok := strings.Cut(kv, "$")
		if ok && name != "" && want != "" && strings.EqualFold(cat(name, &e), want) {
			return true
		}
	}

	return false
}

// LedgerEntries returns a copy of the ledger entries with start <= date < end (YYYY-MM-DD).
func (b *InMemoryBackend) LedgerEntries(start, end string) []CostEntry {
	b.Accrue()

	b.mu.RLock("LedgerEntries")
	defer b.mu.RUnlock()

	return b.costLedgerInBucket(start, end)
}

// LedgerSpend sums metric over ledger entries with start <= date < end (YYYY-MM-DD).
// filters uses Budgets CostFilters keys; keys the ledger does not model do not narrow.
func (b *InMemoryBackend) LedgerSpend(start, end string, filters map[string][]string, metric string) float64 {
	b.Accrue()

	b.mu.RLock("LedgerSpend")
	defer b.mu.RUnlock()

	total := 0.0
	cat := b.categoryResolverLocked()

	for _, e := range b.costLedgerInBucket(start, end) {
		if ledgerMatches(e, filters, cat) {
			total += getMetricValue(e, metric)
		}
	}

	return total
}

func ledgerMatches(e CostEntry, filters map[string][]string, cat categoryResolver) bool {
	for key, vals := range filters {
		if len(vals) > 0 && !ledgerKeyMatches(e, key, vals, cat) {
			return false
		}
	}

	return true
}

func ledgerKeyMatches(e CostEntry, key string, vals []string, cat categoryResolver) bool {
	switch key {
	case ExcludeChargeTypeFilter:
		return !stringSliceContainsFold(vals, entryChargeType(e))
	case costCategoryFilterKey:
		return costCategoryFilterMatches(e, vals, cat)
	case "TagKeyValue":
		return tagKeyValueMatches(e, vals)
	}

	dim, ok := budgetFilterDims[key]
	if !ok {
		return true
	}

	v, ok := dimensionFieldValue(e, dim)

	return !ok || stringSliceContainsFold(vals, v)
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

// RecordCharge appends an entry (for example a Tax or Support charge) to the cost ledger.
func (b *InMemoryBackend) RecordCharge(e CostEntry) {
	b.mu.Lock("RecordCharge")
	defer b.mu.Unlock()

	b.costLedger = append(b.costLedger, e)
}
