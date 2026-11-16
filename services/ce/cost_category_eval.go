package ce

import (
	"strings"
)

const (
	costCategoryMaxDepth  = 8
	ruleTypeInherited     = "INHERITED_VALUE"
	inheritedDimensionTag = "TAG"
	matchAbsent           = "ABSENT"
	matchStartsWith       = "STARTS_WITH"
	matchEndsWith         = "ENDS_WITH"
	matchContains         = "CONTAINS"
	matchCaseSensitive    = "CASE_SENSITIVE"
)

func modelRule(r costCategoryRule) CostCategoryRule {
	out := CostCategoryRule{Rule: r.Rule, Value: r.Value, Type: r.Type}
	if r.InheritedValue != nil {
		iv := inheritedRule(*r.InheritedValue)
		out.InheritedValue = &iv
	}

	return out
}

func wireRule(r CostCategoryRule) costCategoryRule {
	out := costCategoryRule{Rule: r.Rule, Value: r.Value, Type: r.Type}
	if r.InheritedValue != nil {
		iv := wireInheritedRule(*r.InheritedValue)
		out.InheritedValue = &iv
	}

	return out
}

// CostCategoryValue returns the value the named cost category assigns to e: the first matching rule's value,
// else the category's default value; empty when the category does not exist or nothing applies.
func (b *InMemoryBackend) CostCategoryValue(name string, e *CostEntry) string {
	b.mu.RLock("CostCategoryValue")
	cats := make(map[string]*CostCategory, b.costCategories.Len())

	for _, c := range b.costCategories.All() {
		cp := *c
		cp.Rules = append([]CostCategoryRule(nil), c.Rules...)
		cats[c.Name] = &cp
	}
	b.mu.RUnlock()

	return assignCategory(cats, name, e, 0)
}

func assignCategory(cats map[string]*CostCategory, name string, e *CostEntry, depth int) string {
	cat, ok := cats[name]
	if !ok || depth > costCategoryMaxDepth {
		return ""
	}

	for i := range cat.Rules {
		r := &cat.Rules[i]

		if r.Type == ruleTypeInherited {
			if v := inheritedValue(r.InheritedValue, e); v != "" {
				return v
			}

			continue
		}

		if r.Rule != nil && categoryMatch(cats, e, r.Rule, depth) {
			return r.Value
		}
	}

	return cat.DefaultValue
}

func inheritedValue(iv *inheritedRule, e *CostEntry) string {
	if iv == nil {
		return ""
	}

	if strings.EqualFold(iv.DimensionName, inheritedDimensionTag) {
		return e.Tags[iv.DimensionKey]
	}

	v, _ := dimensionFieldValue(*e, iv.DimensionName)

	return v
}

// categoryMatch evaluates a rule expression strictly: a dimension the ledger does not model matches nothing.
func categoryMatch(cats map[string]*CostCategory, e *CostEntry, expr *ceExpression, depth int) bool {
	switch {
	case len(expr.And) > 0:
		for i := range expr.And {
			if !categoryMatch(cats, e, &expr.And[i], depth) {
				return false
			}
		}

		return true
	case len(expr.Or) > 0:
		for i := range expr.Or {
			if categoryMatch(cats, e, &expr.Or[i], depth) {
				return true
			}
		}

		return false
	case expr.Not != nil:
		return !categoryMatch(cats, e, expr.Not, depth)
	case expr.Dimensions != nil:
		got, ok := dimensionFieldValue(*e, expr.Dimensions.Key)

		return ok && matchValues(expr.Dimensions.MatchOptions, expr.Dimensions.Values, got)
	case expr.Tags != nil:
		return matchValues(expr.Tags.MatchOptions, expr.Tags.Values, e.Tags[expr.Tags.Key])
	case expr.CostCategories != nil:
		got := assignCategory(cats, expr.CostCategories.Key, e, depth+1)

		return matchValues(expr.CostCategories.MatchOptions, expr.CostCategories.Values, got)
	default:
		return false
	}
}

func matchValues(opts, values []string, got string) bool {
	mode, fold := "", true

	for _, o := range opts {
		switch strings.ToUpper(o) {
		case matchCaseSensitive:
			fold = false
		case matchAbsent, matchStartsWith, matchEndsWith, matchContains:
			mode = strings.ToUpper(o)
		}
	}

	if mode == matchAbsent {
		return got == ""
	}

	for _, v := range values {
		a, c := got, v
		if fold {
			a, c = strings.ToLower(a), strings.ToLower(c)
		}

		if compareMatch(mode, a, c) {
			return true
		}
	}

	return false
}

func compareMatch(mode, a, c string) bool {
	switch mode {
	case matchStartsWith:
		return strings.HasPrefix(a, c)
	case matchEndsWith:
		return strings.HasSuffix(a, c)
	case matchContains:
		return strings.Contains(a, c)
	default:
		return a == c
	}
}
