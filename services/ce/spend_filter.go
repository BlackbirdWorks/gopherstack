package ce

import "strings"

// SpendValues is one Dimensions or Tags clause of a SpendFilter.
type SpendValues struct {
	Key          string
	Values       []string
	MatchOptions []string
}

// SpendFilter is a Budgets filter expression tree evaluated against ledger entries.
type SpendFilter struct {
	Not        *SpendFilter
	Dimensions *SpendValues
	Tags       *SpendValues
	And        []SpendFilter
	Or         []SpendFilter
}

// LedgerSpendFiltered is LedgerSpend narrowed further by an expression tree.
func (b *InMemoryBackend) LedgerSpendFiltered(
	start, end string, f *SpendFilter, filters map[string][]string, metric string,
) float64 {
	b.mu.RLock("LedgerSpendFiltered")
	defer b.mu.RUnlock()

	total := 0.0

	for _, e := range b.costLedgerInBucket(start, end) {
		if ledgerMatches(e, filters) && f.matches(e) {
			total += getMetricValue(e, metric)
		}
	}

	return total
}

func (f *SpendFilter) matches(e CostEntry) bool {
	switch {
	case f == nil:
		return true
	case len(f.And) > 0:
		for i := range f.And {
			if !f.And[i].matches(e) {
				return false
			}
		}

		return true
	case len(f.Or) > 0:
		for i := range f.Or {
			if f.Or[i].matches(e) {
				return true
			}
		}

		return false
	case f.Not != nil:
		return !f.Not.matches(e)
	case f.Dimensions != nil:
		v, ok := dimensionFieldValue(e, f.Dimensions.Key)

		return !ok || f.Dimensions.matchValue(v, true)
	case f.Tags != nil:
		v, present := e.Tags[strings.TrimPrefix(f.Tags.Key, "user:")]

		return f.Tags.matchValue(v, present)
	default:
		return true
	}
}

// matchValue applies the single MatchOption (default EQUALS, case-insensitive) to v.
func (s *SpendValues) matchValue(v string, present bool) bool {
	opt, caseSensitive := "EQUALS", false

	for _, o := range s.MatchOptions {
		switch o {
		case "CASE_SENSITIVE":
			caseSensitive = true
		case "CASE_INSENSITIVE":
			caseSensitive = false
		default:
			opt = o
		}
	}

	if opt == "ABSENT" {
		return !present || v == ""
	}

	for _, want := range s.Values {
		got, w := v, want

		if !caseSensitive {
			got, w = strings.ToLower(got), strings.ToLower(w)
		}

		if matchesOption(opt, got, w) {
			return true
		}
	}

	return false
}

func matchesOption(opt, got, want string) bool {
	switch opt {
	case "STARTS_WITH":
		return strings.HasPrefix(got, want)
	case "ENDS_WITH":
		return strings.HasSuffix(got, want)
	case "CONTAINS":
		return strings.Contains(got, want)
	default:
		return got == want
	}
}
