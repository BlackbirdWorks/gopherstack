package glue

// This file implements SearchAssets' filter-clause evaluator. Unlike every
// other request/response shape in this package, SearchFilterClause is a
// Smithy tagged union (exactly one of AndAllFilters/OrAnyFilters/
// AttributeFilter/MapFilter is present per clause) -- confirmed against
// aws-sdk-go-v2/service/glue/serializers.go's
// awsAwsjson11_serializeDocumentSearchFilterClause, which discriminates on
// which single JSON key is present. Since this backend only ever needs to
// *decode* an already-serialized request (never produce one), a plain struct
// with one field per possible member -- rather than reproducing the SDK's
// exact Go-side interface+concrete-type union representation -- is
// sufficient and is parsed by encoding/json without a custom
// UnmarshalJSON.

// searchFilterClause mirrors types.SearchFilterClause's wire shape.
type searchFilterClause struct {
	AttributeFilter *searchAttributeFilter `json:"AttributeFilter,omitempty"`
	MapFilter       *searchMapFilter       `json:"MapFilter,omitempty"`
	AndAllFilters   []searchFilterClause   `json:"AndAllFilters,omitempty"`
	OrAnyFilters    []searchFilterClause   `json:"OrAnyFilters,omitempty"`
}

// searchFilterValue mirrors types.SearchFilterValue's wire shape (exactly one
// of StringValue/LongValue is present).
type searchFilterValue struct {
	StringValue *string `json:"StringValue,omitempty"`
	LongValue   *int64  `json:"LongValue,omitempty"`
}

// searchAttributeFilter mirrors types.SearchAttributeFilter.
type searchAttributeFilter struct {
	Value     searchFilterValue `json:"Value"`
	Attribute string            `json:"Attribute"`
	Operator  string            `json:"Operator"`
}

// searchMapFilterValue mirrors types.SearchMapFilterValue's wire shape
// (string comparison only -- the only member the real SDK defines).
type searchMapFilterValue struct {
	StringValue *string `json:"StringValue,omitempty"`
}

// searchMapFilter mirrors types.SearchMapFilter. Unlike searchAttributeFilter
// it carries no Operator -- the real shape only supports equality on a map
// key's value.
type searchMapFilter struct {
	Value     searchMapFilterValue `json:"Value"`
	Attribute string               `json:"Attribute"`
	Key       string               `json:"Key"`
}

// searchSort mirrors types.SearchSort.
type searchSort struct {
	Attribute string `json:"Attribute"`
	Order     string `json:"Order,omitempty"`
}

// matchesFilter reports whether asset a satisfies clause. A nil clause (no
// FilterClause given in the request) matches every asset.
func matchesFilter(a *Asset, clause *searchFilterClause) bool {
	if clause == nil {
		return true
	}

	switch {
	case len(clause.AndAllFilters) > 0:
		return matchesAllFilters(a, clause.AndAllFilters)
	case len(clause.OrAnyFilters) > 0:
		return matchesAnyFilter(a, clause.OrAnyFilters)
	case clause.AttributeFilter != nil:
		return matchesAttributeFilter(a, clause.AttributeFilter)
	case clause.MapFilter != nil:
		return matchesMapFilter(a, clause.MapFilter)
	default:
		return true
	}
}

func matchesAllFilters(a *Asset, clauses []searchFilterClause) bool {
	for i := range clauses {
		if !matchesFilter(a, &clauses[i]) {
			return false
		}
	}

	return true
}

func matchesAnyFilter(a *Asset, clauses []searchFilterClause) bool {
	for i := range clauses {
		if matchesFilter(a, &clauses[i]) {
			return true
		}
	}

	return false
}

// matchesAttributeFilter evaluates a single-attribute comparison against one
// of Asset's own scalar fields.
func matchesAttributeFilter(a *Asset, f *searchAttributeFilter) bool {
	switch f.Attribute {
	case "Name":
		return compareStringOp(f.Operator, a.Name, f.Value)
	case "Description":
		return compareStringOp(f.Operator, a.Description, f.Value)
	case "Id":
		return compareStringOp(f.Operator, a.ID, f.Value)
	case "AssetTypeId":
		return compareStringOp(f.Operator, a.AssetTypeID, f.Value)
	case "CreatedAt":
		return compareNumOp(f.Operator, a.CreatedAt, f.Value)
	case "UpdatedAt":
		return compareNumOp(f.Operator, a.UpdatedAt, f.Value)
	default:
		return false
	}
}

// matchesMapFilter evaluates equality against an entry in one of Asset's map
// attributes. "Forms" (keyed by form name, compared against the form's
// Content) is the only map attribute this backend exposes for search.
func matchesMapFilter(a *Asset, f *searchMapFilter) bool {
	if f.Attribute != "Forms" {
		return false
	}

	entry, ok := a.Forms[f.Key]
	if !ok {
		return false
	}

	want := ""
	if f.Value.StringValue != nil {
		want = *f.Value.StringValue
	}

	return entry.Content == want
}

// compareStringOp evaluates one of SearchFilterOperator's six operators
// against a string attribute.
func compareStringOp(op, actual string, val searchFilterValue) bool {
	if op == "notExists" {
		return actual == ""
	}

	want := ""
	if val.StringValue != nil {
		want = *val.StringValue
	}

	switch op {
	case "equals":
		return actual == want
	case "greaterThan":
		return actual > want
	case "greaterThanOrEquals":
		return actual >= want
	case "lessThan":
		return actual < want
	case "lessThanOrEquals":
		return actual <= want
	default:
		return false
	}
}

// compareNumOp evaluates one of SearchFilterOperator's six operators against
// a numeric (epoch-seconds timestamp) attribute.
func compareNumOp(op string, actual float64, val searchFilterValue) bool {
	if op == "notExists" {
		return actual == 0
	}

	var want float64
	if val.LongValue != nil {
		want = float64(*val.LongValue)
	}

	switch op {
	case "equals":
		return actual == want
	case "greaterThan":
		return actual > want
	case "greaterThanOrEquals":
		return actual >= want
	case "lessThan":
		return actual < want
	case "lessThanOrEquals":
		return actual <= want
	default:
		return false
	}
}
