package s3

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// evalQuery applies a parsed sqlQuery to a set of rows.
// Each row is a map of column name → string value.
func evalQuery(q *sqlQuery, rows []map[string]string) ([]map[string]string, error) {
	return evalQueryGeneric(
		q, rows,
		func(r map[string]string) sqlRow { return &stringRow{data: r} },
		evalAggQuery, projectStringRow, sortStringRows,
	)
}

func projectStringRow(
	q *sqlQuery,
	row sqlRow,
	rawRow map[string]string,
) (map[string]string, error) {
	if q.selectAll {
		return rawRow, nil
	}

	projected := make(map[string]string, len(q.columns))

	for i, col := range q.columns {
		val, err := col.expr.eval(row)
		if err != nil {
			return nil, err
		}

		name := columnName(col, i)
		projected[name] = fmt.Sprintf("%v", val)
	}

	return projected, nil
}

// evalQueryJSON applies a parsed sqlQuery to JSON rows (map[string]any).
func evalQueryJSON(q *sqlQuery, rows []map[string]any) ([]map[string]any, error) {
	return evalQueryGeneric(
		q, rows,
		func(r map[string]any) sqlRow { return &jsonRow{data: r} },
		evalAggQueryJSON, projectJSONRow, sortJSONRows,
	)
}

// evalQueryGeneric is the shared filter-sort-project implementation for both string and JSON rows.
func evalQueryGeneric[R any](
	q *sqlQuery,
	rows []R,
	wrapRow func(R) sqlRow,
	evalAgg func(*sqlQuery, []R) ([]R, error),
	projectRow func(*sqlQuery, sqlRow, R) (R, error),
	sortRows func([]R, []sqlOrderByClause),
) ([]R, error) {
	if q.hasAggregates() {
		return evalAgg(q, rows)
	}

	// Filter on original rows so ORDER BY can reference any column.
	filtered := make([]R, 0, len(rows))

	for _, rawRow := range rows {
		if q.condition != nil {
			val, err := q.condition.eval(wrapRow(rawRow))
			if err != nil {
				return nil, err
			}

			if !isTruthy(val) {
				continue
			}
		}

		filtered = append(filtered, rawRow)

		if q.limit > 0 && len(q.orderBy) == 0 && len(filtered) >= q.limit {
			break
		}
	}

	// Sort on original data before projection so ORDER BY can access any column.
	if len(q.orderBy) > 0 {
		sortRows(filtered, q.orderBy)

		if q.limit > 0 && len(filtered) > q.limit {
			filtered = filtered[:q.limit]
		}
	}

	result := make([]R, 0, len(filtered))

	for _, rawRow := range filtered {
		projected, err := projectRow(q, wrapRow(rawRow), rawRow)
		if err != nil {
			return nil, err
		}

		result = append(result, projected)
	}

	return result, nil
}

func projectJSONRow(q *sqlQuery, row sqlRow, rawRow map[string]any) (map[string]any, error) {
	if q.selectAll {
		return rawRow, nil
	}

	projected := make(map[string]any, len(q.columns))

	for i, col := range q.columns {
		val, err := col.expr.eval(row)
		if err != nil {
			return nil, err
		}

		name := columnName(col, i)
		projected[name] = val
	}

	return projected, nil
}

func columnName(col selectColumn, idx int) string {
	if col.alias != "" {
		return col.alias
	}

	if ref, ok := col.expr.(*sqlColumnRef); ok {
		return ref.name
	}

	return fmt.Sprintf("_col%d", idx+1)
}

// stringRow implements sqlRow for CSV rows (string values).
type stringRow struct {
	data map[string]string
}

func (r *stringRow) field(name string) (string, bool) {
	for k, v := range r.data {
		if strings.EqualFold(k, name) {
			return v, true
		}
	}

	return "", false
}

// jsonRow implements sqlRow for JSON rows (any values).
type jsonRow struct {
	data map[string]any
}

func (r *jsonRow) field(name string) (string, bool) {
	for k, v := range r.data {
		if strings.EqualFold(k, name) {
			return fmt.Sprintf("%v", v), true
		}
	}

	return "", false
}

// sortedKeys returns sorted keys of a map for deterministic positional access.
// Keys of the form "_N" (positional CSV columns) are sorted numerically so that
// _1, _2, ..., _9, _10 comes before _2 in the positional ordering rather than
// the lexicographic _1, _10, _2 ordering.
func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	slices.SortFunc(keys, func(a, b string) int {
		ai, aok := positionalIndex(a)
		bi, bok := positionalIndex(b)

		if aok && bok {
			return ai - bi
		}

		if aok {
			return -1
		}

		if bok {
			return 1
		}

		return strings.Compare(a, b)
	})

	return keys
}

// positionalIndex returns the numeric index for positional CSV column keys like
// "_1", "_2", "_10". Returns (0, false) for non-positional keys.
func positionalIndex(s string) (int, bool) {
	if len(s) < 2 || s[0] != '_' {
		return 0, false
	}

	n, err := strconv.Atoi(s[1:])
	if err != nil || n < 1 {
		return 0, false
	}

	return n, true
}

// aggAccumulator accumulates running state for one aggregate expression.
type aggAccumulator struct {
	fn       sqlAggFuncName
	sum      float64
	count    int64
	minVal   float64
	maxVal   float64
	hasValue bool
}

func (a *aggAccumulator) accumulate(val any, countStar bool) {
	switch a.fn {
	case aggFuncCount:
		a.accumulateCount(val, countStar)
	case aggFuncSum, aggFuncAvg:
		a.accumulateSumAvg(val)
	case aggFuncMin:
		a.accumulateMinMax(val, true)
	case aggFuncMax:
		a.accumulateMinMax(val, false)
	}
}

func (a *aggAccumulator) accumulateCount(val any, countStar bool) {
	if countStar || (val != nil && val != "" && val != sqlNullValue) {
		a.count++
	}
}

func (a *aggAccumulator) accumulateSumAvg(val any) {
	if n, ok := toFloat64(val); ok {
		a.sum += n
		a.count++
	}
}

func (a *aggAccumulator) accumulateMinMax(val any, isMin bool) {
	n, ok := toFloat64(val)
	if !ok {
		return
	}

	if isMin {
		if !a.hasValue || n < a.minVal {
			a.minVal = n
			a.hasValue = true
		}
	} else {
		if !a.hasValue || n > a.maxVal {
			a.maxVal = n
			a.hasValue = true
		}
	}
}

func (a *aggAccumulator) result() string {
	switch a.fn {
	case aggFuncCount:
		return strconv.FormatInt(a.count, 10)

	case aggFuncSum:
		return strconv.FormatFloat(a.sum, 'f', -1, 64)

	case aggFuncAvg:
		if a.count == 0 {
			return ""
		}

		return strconv.FormatFloat(a.sum/float64(a.count), 'f', -1, 64)

	case aggFuncMin:
		if !a.hasValue {
			return ""
		}

		return strconv.FormatFloat(a.minVal, 'f', -1, 64)

	case aggFuncMax:
		if !a.hasValue {
			return ""
		}

		return strconv.FormatFloat(a.maxVal, 'f', -1, 64)

	default:
		return ""
	}
}

// evalAggQuery evaluates a query with aggregate functions over string rows.
// Returns a single result row with the computed aggregate values.
func evalAggQuery(q *sqlQuery, rows []map[string]string) ([]map[string]string, error) {
	accs := initAggAccumulators(q)

	for _, rawRow := range rows {
		if err := runAggAccumulation(q, accs, &stringRow{data: rawRow}); err != nil {
			return nil, err
		}
	}

	result := make(map[string]string, len(q.columns))

	for i, col := range q.columns {
		if accs[i] != nil {
			result[columnName(col, i)] = accs[i].result()
		}
	}

	return []map[string]string{result}, nil
}

// evalAggQueryJSON evaluates a query with aggregate functions over JSON rows.
func evalAggQueryJSON(q *sqlQuery, rows []map[string]any) ([]map[string]any, error) {
	accs := initAggAccumulators(q)

	for _, rawRow := range rows {
		if err := runAggAccumulation(q, accs, &jsonRow{data: rawRow}); err != nil {
			return nil, err
		}
	}

	result := make(map[string]any, len(q.columns))

	for i, col := range q.columns {
		if accs[i] != nil {
			result[columnName(col, i)] = accs[i].result()
		}
	}

	return []map[string]any{result}, nil
}

func initAggAccumulators(q *sqlQuery) []*aggAccumulator {
	accs := make([]*aggAccumulator, len(q.columns))

	for i, col := range q.columns {
		if agg, ok := col.expr.(*sqlAggExpr); ok {
			accs[i] = &aggAccumulator{fn: agg.fn}
		}
	}

	return accs
}

func runAggAccumulation(q *sqlQuery, accs []*aggAccumulator, row sqlRow) error {
	if q.condition != nil {
		val, err := q.condition.eval(row)
		if err != nil {
			return err
		}

		if !isTruthy(val) {
			return nil
		}
	}

	for i, col := range q.columns {
		if accs[i] == nil {
			continue
		}

		agg, ok := col.expr.(*sqlAggExpr)
		if !ok {
			continue
		}

		countStar := agg.arg == nil

		var val any

		if !countStar {
			var err error

			val, err = agg.arg.eval(row)
			if err != nil {
				return err
			}
		}

		accs[i].accumulate(val, countStar)
	}

	return nil
}

// toFloat64 converts any value to float64 for numeric comparisons and aggregates.
func toFloat64(v any) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case int64:
		return float64(val), true
	case int:
		return float64(val), true
	case string:
		n, err := strconv.ParseFloat(val, 64)

		return n, err == nil
	default:
		return 0, false
	}
}

// compareAnyForSort compares two values for ORDER BY sorting.
// Numbers are compared numerically; all other values are compared as strings.
func compareAnyForSort(a, b any) int {
	as := fmt.Sprintf("%v", a)
	bs := fmt.Sprintf("%v", b)

	an, aerr := strconv.ParseFloat(as, 64)
	bn, berr := strconv.ParseFloat(bs, 64)

	if aerr == nil && berr == nil {
		switch {
		case an < bn:
			return -1
		case an > bn:
			return 1
		default:
			return 0
		}
	}

	return strings.Compare(as, bs)
}

// sortStringRows sorts rows in-place by the ORDER BY clauses.
func sortStringRows(rows []map[string]string, orderBy []sqlOrderByClause) {
	slices.SortStableFunc(rows, func(a, b map[string]string) int {
		for _, clause := range orderBy {
			av, _ := clause.expr.eval(&stringRow{data: a})
			bv, _ := clause.expr.eval(&stringRow{data: b})
			cmp := compareAnyForSort(av, bv)

			if cmp != 0 {
				if clause.desc {
					return -cmp
				}

				return cmp
			}
		}

		return 0
	})
}

// sortJSONRows sorts JSON rows in-place by the ORDER BY clauses.
func sortJSONRows(rows []map[string]any, orderBy []sqlOrderByClause) {
	slices.SortStableFunc(rows, func(a, b map[string]any) int {
		for _, clause := range orderBy {
			av, _ := clause.expr.eval(&jsonRow{data: a})
			bv, _ := clause.expr.eval(&jsonRow{data: b})
			cmp := compareAnyForSort(av, bv)

			if cmp != 0 {
				if clause.desc {
					return -cmp
				}

				return cmp
			}
		}

		return 0
	})
}
