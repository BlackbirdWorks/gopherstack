package dmsengine

import (
	"fmt"
	"strconv"
	"strings"
)

// Selection-rule filter operators (DMS User Guide, "Using source filters").
const (
	opLTE        = "lte"
	opSTE        = "ste"
	opGTE        = "gte"
	opEQ         = "eq"
	opNotEQ      = "noteq"
	opBetween    = "between"
	opNotBetween = "notbetween"
	opNull       = "null"
	opNotNull    = "notnull"
)

type filterCondition struct {
	Op    string     `json:"filter-operator"`
	Value flexString `json:"value"`
	Start flexString `json:"start-value"`
	End   flexString `json:"end-value"`
}

// RuleFilter is one entry of a selection rule's filters array.
type RuleFilter struct {
	Type       string            `json:"filter-type"`
	Column     string            `json:"column-name"`
	Conditions []filterCondition `json:"filter-conditions"`
}

func validateFilter(f RuleFilter) error {
	if f.Type != "source" {
		return fmt.Errorf("%w: unsupported filter-type %q", ErrMapping, f.Type)
	}

	if f.Column == "" || len(f.Conditions) == 0 {
		return fmt.Errorf("%w: a filter needs column-name and filter-conditions", ErrMapping)
	}

	for _, c := range f.Conditions {
		switch c.Op {
		case opLTE, opSTE, opGTE, opEQ, opNotEQ, opBetween, opNotBetween, opNull, opNotNull:
		default:
			return fmt.Errorf("%w: unsupported filter-operator %q", ErrMapping, c.Op)
		}
	}

	return nil
}

func (c filterCondition) negative() bool { return c.Op == opNotEQ || c.Op == opNotBetween }

func (c filterCondition) inclusive() bool {
	switch c.Op {
	case opLTE, opSTE, opGTE, opEQ, opBetween:
		return true
	}

	return false
}

// effective applies the documented NULL notes: with a negative and a null condition in the same filter, NULL rows are
// never replicated, and without an inclusive condition nothing is.
func (f RuleFilter) effective() ([]filterCondition, bool) {
	var neg, null, incl bool

	for _, c := range f.Conditions {
		neg, null, incl = neg || c.negative(), null || c.Op == opNull, incl || c.inclusive()
	}

	if !neg || !null {
		return f.Conditions, true
	}

	if !incl {
		return nil, false
	}

	out := make([]filterCondition, 0, len(f.Conditions))

	for _, c := range f.Conditions {
		if c.Op != opNull {
			out = append(out, c)
		}
	}

	return out, true
}

// TableFilter is the compiled filters of one selection rule bound to a source table.
type TableFilter struct {
	table   Table
	filters []boundFilter
}

type boundFilter struct {
	conds []filterCondition
	col   Column
	idx   int
	none  bool
}

func newTableFilter(t Table, fs []RuleFilter) (*TableFilter, error) {
	if len(fs) == 0 {
		return nil, nil //nolint:nilnil // no filters
	}

	tf := &TableFilter{table: t}

	for _, f := range fs {
		idx := -1

		for i, c := range t.Columns {
			if c.Name == f.Column {
				idx = i
			}
		}

		if idx < 0 {
			return nil, fmt.Errorf("%w: filter column %q does not exist in %s", ErrMapping, f.Column, t.Key())
		}

		conds, ok := f.effective()
		tf.filters = append(tf.filters, boundFilter{conds: conds, col: t.Columns[idx], idx: idx, none: !ok})
	}

	return tf, nil
}

// Match reports whether a source row passes every filter.
func (f *TableFilter) Match(r Row) bool {
	if f == nil {
		return true
	}

	for _, bf := range f.filters {
		if bf.none || bf.idx >= len(r) || !bf.match(r[bf.idx]) {
			return false
		}
	}

	return true
}

func (b boundFilter) match(v any) bool {
	for _, c := range b.conds {
		if b.matchCond(c, v) {
			return true
		}
	}

	return false
}

func (b boundFilter) matchCond(c filterCondition, v any) bool {
	switch c.Op {
	case opNull:
		return v == nil
	case opNotNull:
		return v != nil
	}

	if v == nil {
		return false
	}

	cmp := func(s flexString) int { return compareFilterValue(b.col.Kind, v, string(s)) }

	switch c.Op {
	case opLTE, opSTE:
		return cmp(c.Value) <= 0
	case opGTE:
		return cmp(c.Value) >= 0
	case opEQ:
		return cmp(c.Value) == 0
	case opNotEQ:
		return cmp(c.Value) != 0
	case opBetween:
		return cmp(c.Start) >= 0 && cmp(c.End) <= 0
	}

	return cmp(c.Start) < 0 || cmp(c.End) > 0
}

// compareFilterValue compares a column value with a rule literal: numerically for numeric columns, otherwise bytewise
// (SQLite BINARY), which orders the ISO date and time forms DMS filters use.
func compareFilterValue(k Kind, v any, lit string) int {
	var s string

	switch x := v.(type) {
	case string:
		s = x
	case []byte:
		s = string(x)
	}

	if k == KindInt || k == KindDecimal || k == KindFloat {
		a, aerr := strconv.ParseFloat(strings.TrimSpace(s), 64)
		b, berr := strconv.ParseFloat(strings.TrimSpace(lit), 64)

		if aerr == nil && berr == nil {
			return cmpOrdered(a, b)
		}
	}

	return strings.Compare(s, lit)
}

// SQL renders the filters as a WHERE predicate with positional arguments for the dialect.
func (f *TableFilter) SQL(fam Family, firstArg int) (string, []any) {
	if f == nil {
		return "", nil
	}

	var (
		parts []string
		args  []any
	)

	ph := func() string {
		args = append(args, nil)

		if fam == FamilyPostgres {
			return "$" + strconv.Itoa(firstArg+len(args)-1)
		}

		return "?"
	}
	bind := func(s flexString) string {
		p := ph()
		args[len(args)-1] = string(s)

		return p
	}

	for _, bf := range f.filters {
		if bf.none {
			return "1 = 0", nil
		}

		col := QuoteIdent(fam, bf.col.Name)
		alts := make([]string, 0, len(bf.conds))

		for _, c := range bf.conds {
			alts = append(alts, condSQL(c, col, bind))
		}

		parts = append(parts, "("+strings.Join(alts, " OR ")+")")
	}

	return strings.Join(parts, " AND "), args
}

func condSQL(c filterCondition, col string, bind func(flexString) string) string {
	switch c.Op {
	case opNull:
		return col + " IS NULL"
	case opNotNull:
		return col + " IS NOT NULL"
	case opLTE, opSTE:
		return col + " <= " + bind(c.Value)
	case opGTE:
		return col + " >= " + bind(c.Value)
	case opEQ:
		return col + " = " + bind(c.Value)
	case opNotEQ:
		return col + " <> " + bind(c.Value)
	case opBetween:
		return col + " BETWEEN " + bind(c.Start) + " AND " + bind(c.End)
	}

	return col + " NOT BETWEEN " + bind(c.Start) + " AND " + bind(c.End)
}
