package detective

import (
	"cmp"
	"fmt"
	"slices"
	"time"
)

// InvestigationFilter mirrors types.FilterCriteria (api_op_ListInvestigations.go):
// non-nil members are ANDed; each StringFilter is an exact match.
type InvestigationFilter struct {
	CreatedStart *time.Time
	CreatedEnd   *time.Time
	EntityARN    *string
	Severity     *string
	State        *string
	Status       *string
}

// InvestigationSort mirrors types.SortCriteria.
type InvestigationSort struct {
	Field string
	Order string
}

const (
	sortFieldSeverity    = "SEVERITY"
	sortFieldStatus      = "STATUS"
	sortFieldCreatedTime = "CREATED_TIME"
	sortOrderAsc         = "ASC"
	sortOrderDesc        = "DESC"
)

func severityRank(s string) int {
	return slices.Index([]string{"INFORMATIONAL", "LOW", "MEDIUM", "HIGH", "CRITICAL"}, s)
}

func (f InvestigationFilter) matches(inv *storedInvestigation) bool {
	switch {
	case f.EntityARN != nil && inv.EntityARN != *f.EntityARN,
		f.Severity != nil && inv.Severity != *f.Severity,
		f.State != nil && inv.State != *f.State,
		f.Status != nil && inv.Status != *f.Status,
		f.CreatedStart != nil && inv.CreatedTime.Before(*f.CreatedStart),
		f.CreatedEnd != nil && inv.CreatedTime.After(*f.CreatedEnd):
		return false
	}

	return true
}

func validateInvestigationSort(s InvestigationSort) error {
	switch s.Field {
	case "", sortFieldSeverity, sortFieldStatus, sortFieldCreatedTime:
	default:
		return fmt.Errorf("%w: invalid SortCriteria.Field %q", ErrValidation, s.Field)
	}

	switch s.Order {
	case "", sortOrderAsc, sortOrderDesc:
	default:
		return fmt.Errorf("%w: invalid SortCriteria.SortOrder %q", ErrValidation, s.Order)
	}

	return nil
}

func sortInvestigations(items []*storedInvestigation, s InvestigationSort) {
	cmpFn := func(a, b *storedInvestigation) int {
		switch s.Field {
		case sortFieldSeverity:
			return cmp.Compare(severityRank(a.Severity), severityRank(b.Severity))
		case sortFieldStatus:
			return cmp.Compare(a.Status, b.Status)
		case sortFieldCreatedTime:
			return a.CreatedTime.Compare(b.CreatedTime)
		}

		return 0
	}

	slices.SortStableFunc(items, func(a, b *storedInvestigation) int {
		c := cmpFn(a, b)
		if s.Order == sortOrderDesc {
			c = -c
		}

		return cmp.Or(c, cmp.Compare(a.InvestigationID, b.InvestigationID))
	})
}

type stringFilterWire struct {
	Value string `json:"Value"`
}

func (s *stringFilterWire) value() *string {
	if s == nil {
		return nil
	}

	return &s.Value
}
