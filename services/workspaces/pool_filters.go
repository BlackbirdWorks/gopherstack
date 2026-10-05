package workspaces

import (
	"fmt"
	"slices"
	"strings"
)

const (
	poolFilterNameBackend = "PoolName"
	opEquals              = "EQUALS"
	opNotEquals           = "NOTEQUALS"
	opContains            = "CONTAINS"
	opNotContains         = "NOTCONTAINS"
)

// PoolFilter is one DescribeWorkspacesPools filter condition.
type PoolFilter struct {
	Name     string
	Operator string
	Values   []string
}

func validatePoolFilters(filters []PoolFilter) error {
	for _, f := range filters {
		if f.Name != poolFilterNameBackend {
			return fmt.Errorf("%w: unsupported filter name %q", ErrInvalidParameter, f.Name)
		}

		switch f.Operator {
		case opEquals, opNotEquals, opContains, opNotContains:
		default:
			return fmt.Errorf("%w: unsupported filter operator %q", ErrInvalidParameter, f.Operator)
		}

		if len(f.Values) == 0 {
			return fmt.Errorf("%w: filter %s needs at least one value", ErrInvalidParameter, f.Name)
		}
	}

	return nil
}

// poolMatchesFilters ANDs the filters; EQUALS/CONTAINS match any value, the NOT forms match none.
func poolMatchesFilters(p *storedPool, filters []PoolFilter) bool {
	for _, f := range filters {
		var hit bool

		switch f.Operator {
		case opEquals, opNotEquals:
			hit = slices.Contains(f.Values, p.PoolName)
		default:
			hit = slices.ContainsFunc(f.Values, func(v string) bool { return strings.Contains(p.PoolName, v) })
		}

		if hit != (f.Operator == opEquals || f.Operator == opContains) {
			return false
		}
	}

	return true
}
