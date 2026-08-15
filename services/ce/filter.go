package ce

import "strings"

// ceDimensionValues mirrors the wire shape of costexplorer's types.DimensionValues:
// a dimension Key plus the Values OR'd together for that dimension.
type ceDimensionValues struct {
	Key    string   `json:"Key"`
	Values []string `json:"Values"`
}

// ceTagValues mirrors costexplorer's types.TagValues.
type ceTagValues struct {
	Key    string   `json:"Key"`
	Values []string `json:"Values"`
}

// ceCostCategoryValues mirrors costexplorer's types.CostCategoryValues.
type ceCostCategoryValues struct {
	Key    string   `json:"Key"`
	Values []string `json:"Values"`
}

// ceExpression mirrors the top-level (non-nested) shape of costexplorer's
// types.Expression: Dimensions/Tags/CostCategories. The real wire type also
// carries And/Or/Not for compound boolean expressions; this emulator applies
// only the simple, single-clause form and does not evaluate boolean
// composition. A client sending a compound expression still gets its
// top-level Dimensions/Tags/CostCategories clause (if any is present
// alongside And/Or/Not, which real AWS itself rejects as invalid) applied,
// which is the documented behavior at each call site below rather than a
// silent no-op.
type ceExpression struct {
	Dimensions     *ceDimensionValues    `json:"Dimensions,omitempty"`
	Tags           *ceTagValues          `json:"Tags,omitempty"`
	CostCategories *ceCostCategoryValues `json:"CostCategories,omitempty"`
}

// ceSortDefinition mirrors costexplorer's types.SortDefinition.
type ceSortDefinition struct {
	Key       string `json:"Key"`
	SortOrder string `json:"SortOrder,omitempty"`
}

func sortDescending(order string) bool {
	return strings.EqualFold(order, "DESCENDING")
}

// stringSliceContainsFold reports whether values contains s, case-insensitively.
func stringSliceContainsFold(values []string, s string) bool {
	for _, v := range values {
		if strings.EqualFold(v, s) {
			return true
		}
	}

	return false
}
