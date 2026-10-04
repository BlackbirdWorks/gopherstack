package ssm

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// QueryFilter is the shared wire shape of types.InventoryFilter and types.ComplianceStringFilter.
type QueryFilter struct {
	Key    string   `json:"Key,omitempty"`
	Type   string   `json:"Type,omitempty"`
	Values []string `json:"Values,omitempty"`
}

// Normalized operators; the wire spellings are Equal/EQUAL, NotEqual/NOT_EQUAL and so on.
const (
	queryOpEqual       = "equal"
	queryOpNotEqual    = "notequal"
	queryOpBeginWith   = "beginwith"
	queryOpLessThan    = "lessthan"
	queryOpGreaterThan = "greaterthan"
	queryOpExists      = "exists"
)

func normalizeQueryOp(op string, allowExists bool) (string, error) {
	if op == "" {
		return queryOpEqual, nil
	}

	n := strings.ToLower(strings.ReplaceAll(op, "_", ""))
	switch n {
	case queryOpEqual, queryOpNotEqual, queryOpBeginWith, queryOpLessThan, queryOpGreaterThan:
		return n, nil
	case queryOpExists:
		if allowExists {
			return n, nil
		}
	}

	return "", fmt.Errorf("%w: unsupported filter Type %q", ErrValidationException, op)
}

func validateQueryFilters(filters []QueryFilter, inventory bool) error {
	for _, f := range filters {
		if inventory && (f.Key == "" || len(f.Values) == 0) {
			return fmt.Errorf("%w: filter Key and Values are required", ErrValidationException)
		}

		if _, err := normalizeQueryOp(f.Type, inventory); err != nil {
			return err
		}
	}

	return nil
}

func queryLess(a, b string) bool {
	fa, errA := strconv.ParseFloat(a, 64)
	fb, errB := strconv.ParseFloat(b, 64)

	if errA == nil && errB == nil {
		return fa < fb
	}

	return a < b
}

func queryOpMatches(op, actual, v string) bool {
	switch op {
	case queryOpEqual:
		return actual == v
	case queryOpBeginWith:
		return strings.HasPrefix(actual, v)
	case queryOpLessThan:
		return queryLess(actual, v)
	case queryOpGreaterThan:
		return queryLess(v, actual)
	}

	return false
}

// queryValueMatches reports whether actual satisfies f; an absent attribute matches nothing.
func queryValueMatches(f QueryFilter, actual string, present bool) bool {
	op, err := normalizeQueryOp(f.Type, true)
	if err != nil || !present {
		return false
	}

	switch op {
	case queryOpExists:
		return true
	case queryOpNotEqual:
		return !slices.Contains(f.Values, actual)
	}

	return slices.ContainsFunc(f.Values, func(v string) bool { return queryOpMatches(op, actual, v) })
}

// complianceItemAttr resolves a ComplianceStringFilter key against the attributes a stored item tracks.
func complianceItemAttr(item ComplianceItem, key string) (string, bool) {
	switch key {
	case "ComplianceType":
		return item.ComplianceType, true
	case "ResourceType":
		return item.ResourceType, true
	case "ResourceId":
		return item.ResourceID, true
	case "Status":
		return item.Status, true
	case "Severity":
		return item.Severity, item.Severity != ""
	case "Title":
		return item.Title, item.Title != ""
	case "Id":
		return item.ID, item.ID != ""
	case "ExecutionId":
		if item.ExecutionSummary != nil {
			return item.ExecutionSummary.ExecutionID, item.ExecutionSummary.ExecutionID != ""
		}
	case "ExecutionType":
		if item.ExecutionSummary != nil {
			return item.ExecutionSummary.ExecutionType, item.ExecutionSummary.ExecutionType != ""
		}
	}

	return "", false
}

func complianceItemMatches(item ComplianceItem, filters []QueryFilter) bool {
	for _, f := range filters {
		actual, ok := complianceItemAttr(item, f.Key)
		if !queryValueMatches(f, actual, ok) {
			return false
		}
	}

	return true
}

// inventoryEntryMatches checks filters keyed "<TypeName>.<Attribute>" against one content entry of typeName.
func inventoryEntryMatches(typeName string, entry map[string]string, filters []QueryFilter) bool {
	for _, f := range filters {
		attr, ok := strings.CutPrefix(f.Key, typeName+".")
		if !ok {
			return false
		}

		actual, present := entry[attr]
		if !queryValueMatches(f, actual, present) {
			return false
		}
	}

	return true
}

// inventoryItemsMatch reports whether every filter is satisfied by some content entry of its own type.
func inventoryItemsMatch(items []InventoryItem, filters []QueryFilter) bool {
	for _, f := range filters {
		typeName, _, _ := strings.Cut(f.Key, ".")
		matched := false

		for _, item := range items {
			if item.TypeName != typeName {
				continue
			}

			for _, entry := range item.Content {
				if inventoryEntryMatches(typeName, entry, []QueryFilter{f}) {
					matched = true
				}
			}
		}

		if !matched {
			return false
		}
	}

	return true
}
