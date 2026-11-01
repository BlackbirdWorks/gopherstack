package ocinosql

import (
	"encoding/json"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

type entry struct {
	row  map[string]any
	size int
}

type plan struct {
	table *Table
	sv    *schemaView
	stmt  *selectStmt
	vars  map[string]any
	desc  bool
}

func (pl *plan) normPath(steps []step) ([]step, *APIError) {
	if len(steps) > 1 && !steps[0].isIdx {
		_, isCol := pl.sv.lookup(steps[0].key)
		if !isCol && (strs.Equal(steps[0].key, pl.stmt.alias) || strs.Equal(steps[0].key, pl.stmt.table)) {
			steps = steps[1:]
		}
	}

	if steps[0].isIdx {
		return nil, errParam("syntax error: a path must start with a column name")
	}

	i, ok := pl.sv.lookup(steps[0].key)
	if !ok {
		return nil, errParam("column '" + steps[0].key + "' does not exist in table " + pl.table.Name)
	}

	out := append([]step{}, steps...)
	out[0].key = pl.sv.cols[i].Name

	return out, nil
}

// bind resolves paths in an expression tree and checks variable references.
func (pl *plan) bind(e expr) *APIError {
	switch x := e.(type) {
	case *binExpr:
		return firstErr(pl.bind(x.l), pl.bind(x.r))
	case *notExpr:
		return pl.bind(x.e)
	case *nullExpr:
		return pl.bind(x.e)
	case *inExpr:
		if aerr := pl.bind(x.e); aerr != nil {
			return aerr
		}

		for _, v := range x.list {
			if aerr := pl.bind(v); aerr != nil {
				return aerr
			}
		}
	case *pathExpr:
		steps, aerr := pl.normPath(x.steps)
		if aerr != nil {
			return aerr
		}

		x.steps = steps
	case *varExpr:
		if _, ok := pl.stmt.vars[x.name]; !ok {
			return errParam("variable " + x.name + " is used but not declared")
		}
	}

	return nil
}

func newPlan(t *Table, st *selectStmt, vars map[string]any) (*plan, *APIError) {
	pl := &plan{table: t, sv: newSchemaView(t.Columns), stmt: st}

	bound, aerr := bindVars(st, vars)
	if aerr != nil {
		return nil, aerr
	}

	pl.vars = bound

	if st.where != nil {
		if aerr = pl.bind(st.where); aerr != nil {
			return nil, aerr
		}
	}

	for i := range st.items {
		if st.items[i].path, aerr = pl.normPath(st.items[i].path); aerr != nil {
			return nil, aerr
		}
	}

	for i := range st.order {
		if st.order[i].path, aerr = pl.normPath(st.order[i].path); aerr != nil {
			return nil, aerr
		}
	}

	return pl, pl.checkOrder()
}

func bindVars(st *selectStmt, vars map[string]any) (map[string]any, *APIError) {
	out := map[string]any{}

	for name, ty := range st.vars {
		v, ok := vars[name]
		if !ok {
			v, ok = vars[strings.TrimPrefix(name, "$")]
		}

		if !ok {
			return nil, errParam("variable " + name + " is declared but not bound")
		}

		n, msg := coerce(ty, v)
		if msg != "" {
			return nil, errParam("variable " + name + " " + msg)
		}

		out[name] = n
	}

	return out, nil
}

func pathKey(steps []step) (string, bool) {
	var sb strings.Builder

	for i, s := range steps[1:] {
		if s.isIdx {
			return "", false
		}

		if i > 0 {
			sb.WriteByte('.')
		}

		sb.WriteString(s.key)
	}

	return sb.String(), true
}

func (pl *plan) checkOrder() *APIError {
	ord := pl.stmt.order
	if len(ord) == 0 {
		return nil
	}

	for _, o := range ord[1:] {
		if o.desc != ord[0].desc {
			return errParam("ORDER BY with mixed ASC and DESC is not supported")
		}
	}

	pl.desc = ord[0].desc

	if matchesKey(ord, pl.table.PrimaryKey) {
		return nil
	}

	for _, ix := range pl.table.Indexes {
		if ix.State != stateActive || len(ord) > len(ix.Keys) {
			continue
		}

		ok := true

		for i, o := range ord {
			jp, plain := pathKey(o.path)
			if !plain || !strs.Equal(o.path[0].key, ix.Keys[i].Column) || jp != ix.Keys[i].JSONPath {
				ok = false

				break
			}
		}

		if ok {
			return nil
		}
	}

	return errParam("ORDER BY must name a leading prefix of the primary key or of an ACTIVE index")
}

func matchesKey(ord []orderItem, pk []string) bool {
	if len(ord) > len(pk) {
		return false
	}

	for i, o := range ord {
		if len(o.path) != 1 || !strs.Equal(o.path[0].key, pk[i]) {
			return false
		}
	}

	return true
}

func lookupStep(v any, s step) (any, bool) {
	if s.isIdx {
		arr, ok := v.([]any)
		if !ok || s.idx >= len(arr) {
			return nil, false
		}

		return arr[s.idx], true
	}

	m, ok := v.(map[string]any)
	if !ok {
		return nil, false
	}

	if x, found := m[s.key]; found {
		return x, true
	}

	for k, x := range m {
		if strs.Equal(k, s.key) {
			return x, true
		}
	}

	return nil, false
}

func evalPath(row map[string]any, steps []step) any {
	var cur any = row

	for _, s := range steps {
		next, ok := lookupStep(cur, s)
		if !ok {
			return nil
		}

		cur = next
	}

	return cur
}

func (pl *plan) value(e expr, row map[string]any) any {
	switch x := e.(type) {
	case *litExpr:
		return x.v
	case *varExpr:
		return pl.vars[x.name]
	case *pathExpr:
		return evalPath(row, x.steps)
	default:
		return pl.truth(e, row)
	}
}

func (pl *plan) truth(e expr, row map[string]any) bool {
	switch x := e.(type) {
	case *binExpr:
		return pl.evalBin(x, row)
	case *notExpr:
		return !pl.truth(x.e, row)
	case *nullExpr:
		return (pl.value(x.e, row) == nil) != x.not
	case *inExpr:
		v := pl.value(x.e, row)
		found := false

		for _, c := range x.list {
			if r, ok := compareVals(v, pl.value(c, row)); ok && r == 0 {
				found = true
			}
		}

		return found != x.not && v != nil
	}

	b, _ := pl.value(e, row).(bool)

	return b
}

func (pl *plan) evalBin(x *binExpr, row map[string]any) bool {
	switch x.op {
	case kwAnd:
		return pl.truth(x.l, row) && pl.truth(x.r, row)
	case "OR":
		return pl.truth(x.l, row) || pl.truth(x.r, row)
	}

	l, r := pl.value(x.l, row), pl.value(x.r, row)
	if l == nil || r == nil {
		return false
	}

	c, ok := compareVals(l, r)
	if !ok {
		return x.op == "!="
	}

	switch x.op {
	case "=":
		return c == 0
	case "!=":
		return c != 0
	case "<":
		return c < 0
	case "<=":
		return c <= 0
	case ">":
		return c > 0
	}

	return c >= 0
}

func cmpNilFirst(a, b any) int {
	switch {
	case a == nil && b == nil:
		return 0
	case a == nil:
		return -1
	case b == nil:
		return 1
	}

	c, _ := compareVals(a, b)

	return c
}

func (pl *plan) sortEntries(es []entry) {
	order := pl.stmt.order
	pk := pl.table.PrimaryKey

	slices.SortStableFunc(es, func(a, b entry) int {
		for _, o := range order {
			if c := cmpNilFirst(evalPath(a.row, o.path), evalPath(b.row, o.path)); c != 0 {
				if pl.desc {
					return -c
				}

				return c
			}
		}

		for _, k := range pk {
			if c := cmpNilFirst(a.row[k], b.row[k]); c != 0 {
				if pl.desc {
					return -c
				}

				return c
			}
		}

		return 0
	})
}

func (pl *plan) project(es []entry) []map[string]any {
	st := pl.stmt
	if st.count {
		return []map[string]any{{"Column_1": json.Number(strconv.Itoa(len(es)))}}
	}

	out := make([]map[string]any, 0, len(es))

	for _, e := range es {
		if st.star {
			out = append(out, e.row)

			continue
		}

		m := make(map[string]any, len(st.items))

		for i, it := range st.items {
			name := it.alias
			if name == "" {
				last := it.path[len(it.path)-1]
				name = last.key

				if last.isIdx || m[name] != nil {
					name = "Column_" + strconv.Itoa(i+1)
				}
			}

			m[name] = evalPath(e.row, it.path)
		}

		out = append(out, m)
	}

	return out
}

// run scans the table, filters, orders and slices the rows; scanned is the number of bytes read.
func (pl *plan) run(now func() time.Time) ([]map[string]any, int) {
	scanned := 0

	var es []entry

	for k, r := range pl.table.Rows {
		if r.Expire != nil && !r.Expire.After(now()) {
			delete(pl.table.Rows, k)

			continue
		}

		scanned += len(r.Value)
		row := decodeRow(r.Value)

		if pl.stmt.where == nil || pl.truth(pl.stmt.where, row) {
			es = append(es, entry{row: row, size: len(r.Value)})
		}
	}

	pl.sortEntries(es)

	if o := pl.stmt.offset; o != nil {
		es = es[min(int(*o), len(es)):]
	}

	if l := pl.stmt.limit; l != nil && int(*l) < len(es) {
		es = es[:*l]
	}

	return pl.project(es), scanned
}
