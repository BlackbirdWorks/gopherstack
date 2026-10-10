package glue

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
)

type sortCriterion struct {
	FieldName string `json:"FieldName"`
	Sort      string `json:"Sort,omitempty"`
}

const (
	sortAscending  = "ASC"
	sortDescending = "DESC"
)

// sortTables orders tables by the criteria in sequence; Sort defaults to ASC.
func sortTables(tables []*Table, criteria []sortCriterion) error {
	for _, c := range criteria {
		if tableSortKey(&Table{}, c.FieldName) == nil {
			return fmt.Errorf("%w: unsupported SortCriteria FieldName %q", ErrValidation, c.FieldName)
		}

		if c.Sort != "" && c.Sort != sortAscending && c.Sort != sortDescending {
			return fmt.Errorf("%w: SortCriteria Sort must be ASC or DESC", ErrValidation)
		}
	}

	slices.SortStableFunc(tables, func(a, b *Table) int {
		for _, c := range criteria {
			res := cmpSortKeys(tableSortKey(a, c.FieldName), tableSortKey(b, c.FieldName))
			if c.Sort == sortDescending {
				res = -res
			}

			if res != 0 {
				return res
			}
		}

		return 0
	})

	return nil
}

func cmpSortKeys(a, b any) int {
	if af, ok := a.(float64); ok {
		bf, _ := b.(float64)

		return cmp.Compare(af, bf)
	}

	as, _ := a.(string)
	bs, _ := b.(string)

	return strings.Compare(strings.ToLower(as), strings.ToLower(bs))
}

func tableSortKey(t *Table, field string) any {
	switch field {
	case predKeyName:
		return t.Name
	case "DatabaseName":
		return t.DatabaseName
	case predKeyDescription:
		return t.Description
	case "TableType":
		return t.TableType
	case "Owner":
		return t.Owner
	case "CreateTime":
		return t.CreateTime
	case "UpdateTime":
		return t.UpdateTime
	default:
		return nil
	}
}
