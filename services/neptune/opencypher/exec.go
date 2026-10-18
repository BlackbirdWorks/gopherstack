package opencypher

import (
	"fmt"
	"maps"
	"sort"
	"strings"
)

func (x *exec) run(q *query) (*Result, error) {
	rows := []row{{}}
	res := &Result{}

	for _, c := range q.clauses {
		if err := x.ctx.Err(); err != nil {
			return nil, fmt.Errorf("query cancelled: %w", err)
		}

		var err error

		switch t := c.(type) {
		case matchClause:
			rows, err = x.execMatch(t, rows)
		case createClause:
			rows, err = x.execCreate(t, rows)
		case mergeClause:
			rows, err = x.execMerge(t, rows)
		case setClause:
			err = x.applyItems(t.items, rows)
		case removeClause:
			err = x.execRemove(t, rows)
		case deleteClause:
			err = x.execDelete(t, rows)
		case unwindClause:
			rows, err = x.execUnwind(t, rows)
		case withClause:
			rows, err = x.execWith(t, rows)
		case returnClause:
			res, err = x.execReturn(t, rows)
		}

		if err != nil {
			return nil, err
		}
	}

	return res, nil
}

func (x *exec) execUnwind(c unwindClause, rows []row) ([]row, error) {
	var out []row

	for _, r := range rows {
		v, err := x.eval(c.e, r, nil)
		if err != nil {
			return nil, err
		}

		switch t := v.(type) {
		case nil:
		case []any:
			for _, e := range t {
				out = append(out, r.with(c.v, e))
			}
		default:
			out = append(out, r.with(c.v, v))
		}
	}

	return out, nil
}

func (x *exec) execWith(c withClause, rows []row) ([]row, error) {
	outs, _, err := x.project(c.proj, rows)
	if err != nil {
		return nil, err
	}

	rs := make([]row, 0, len(outs))

	for _, o := range outs {
		if c.where != nil {
			v, werr := x.eval(c.where, o.vals, o.grp)
			if werr != nil {
				return nil, werr
			}

			if b, _ := v.(bool); !b {
				continue
			}
		}

		rs = append(rs, o.vals)
	}

	return rs, nil
}

func (x *exec) execReturn(c returnClause, rows []row) (*Result, error) {
	outs, cols, err := x.project(c.proj, rows)
	if err != nil {
		return nil, err
	}

	res := &Result{Columns: cols, Rows: make([][]any, 0, len(outs))}

	for _, o := range outs {
		vals := make([]any, len(cols))
		for i, col := range cols {
			vals[i] = o.vals[col]
		}

		res.Rows = append(res.Rows, vals)
	}

	return res, nil
}

type outRow struct {
	vals row
	src  row
	grp  []row
}

func (x *exec) project(p projection, rows []row) ([]outRow, []string, error) {
	cols := x.columns(p, rows)

	var (
		outs []outRow
		err  error
	)

	if anyProjAgg(p) {
		outs, err = x.projectGrouped(p, rows, cols)
	} else {
		outs, err = x.projectPlain(p, rows, cols)
	}

	if err != nil {
		return nil, nil, err
	}

	if p.distinct {
		outs = distinctRows(outs, cols)
	}

	if len(p.order) > 0 {
		if err = x.sortRows(p.order, outs); err != nil {
			return nil, nil, err
		}
	}

	if outs, err = x.skipLimit(p, outs); err != nil {
		return nil, nil, err
	}

	return outs, cols, nil
}

func anyProjAgg(p projection) bool {
	for _, it := range p.items {
		if containsAgg(it.e) {
			return true
		}
	}

	return false
}

func (x *exec) columns(p projection, rows []row) []string {
	var cols []string

	if p.star && len(rows) > 0 {
		for k := range rows[0] {
			cols = append(cols, k)
		}

		sort.Strings(cols)
	}

	for _, it := range p.items {
		cols = append(cols, it.name)
	}

	return cols
}

func (x *exec) projectPlain(p projection, rows []row, cols []string) ([]outRow, error) {
	outs := make([]outRow, 0, len(rows))

	for _, r := range rows {
		vals := make(row, len(cols))

		if p.star {
			maps.Copy(vals, r)
		}

		for _, it := range p.items {
			v, err := x.eval(it.e, r, nil)
			if err != nil {
				return nil, err
			}

			vals[it.name] = v
		}

		outs = append(outs, outRow{vals: vals, src: r})
	}

	return outs, nil
}

func (x *exec) projectGrouped(p projection, rows []row, cols []string) ([]outRow, error) {
	type group struct {
		rep  row
		rows []row
	}

	var (
		groups []*group
		index  = map[string]*group{}
	)

	for _, r := range rows {
		key := ""

		var keySb227 strings.Builder
		for _, it := range p.items {
			if containsAgg(it.e) {
				continue
			}

			v, err := x.eval(it.e, r, nil)
			if err != nil {
				return nil, err
			}

			keySb227.WriteString(valueKey(v) + "|")
		}
		key += keySb227.String()

		g, ok := index[key]
		if !ok {
			g = &group{rep: r}
			index[key] = g
			groups = append(groups, g)
		}

		g.rows = append(g.rows, r)
	}

	if len(groups) == 0 && !hasGroupKey(p) {
		groups = append(groups, &group{rep: row{}, rows: []row{}})
	}

	outs := make([]outRow, 0, len(groups))

	for _, g := range groups {
		vals := make(row, len(cols))

		for _, it := range p.items {
			v, err := x.eval(it.e, g.rep, g.rows)
			if err != nil {
				return nil, err
			}

			vals[it.name] = v
		}

		outs = append(outs, outRow{vals: vals, src: g.rep, grp: g.rows})
	}

	return outs, nil
}

func hasGroupKey(p projection) bool {
	for _, it := range p.items {
		if !containsAgg(it.e) {
			return true
		}
	}

	return p.star
}

func distinctRows(outs []outRow, cols []string) []outRow {
	seen := map[string]bool{}
	res := outs[:0:0]

	for _, o := range outs {
		key := ""
		var keySb290 strings.Builder
		for _, c := range cols {
			keySb290.WriteString(valueKey(o.vals[c]) + "|")
		}
		key += keySb290.String()

		if !seen[key] {
			seen[key] = true
			res = append(res, o)
		}
	}

	return res
}

func (x *exec) sortRows(items []orderItem, outs []outRow) error {
	keys := make([][]any, len(outs))

	for i, o := range outs {
		env := make(row, len(o.src)+len(o.vals))
		maps.Copy(env, o.src)

		maps.Copy(env, o.vals)

		keys[i] = make([]any, len(items))

		for j, it := range items {
			v, err := x.eval(it.e, env, o.grp)
			if err != nil {
				return err
			}

			keys[i][j] = v
		}
	}

	idx := make([]int, len(outs))
	for i := range idx {
		idx[i] = i
	}

	sort.SliceStable(idx, func(a, b int) bool {
		for j, it := range items {
			c := order(keys[idx[a]][j], keys[idx[b]][j])
			if c == 0 {
				continue
			}

			if it.desc {
				return c > 0
			}

			return c < 0
		}

		return false
	})

	sorted := make([]outRow, len(outs))
	for i, k := range idx {
		sorted[i] = outs[k]
	}

	copy(outs, sorted)

	return nil
}

func (x *exec) skipLimit(p projection, outs []outRow) ([]outRow, error) {
	if p.skip != nil {
		n, err := x.nonNegative(p.skip, "SKIP")
		if err != nil {
			return nil, err
		}

		outs = outs[min(n, len(outs)):]
	}

	if p.limit != nil {
		n, err := x.nonNegative(p.limit, "LIMIT")
		if err != nil {
			return nil, err
		}

		outs = outs[:min(n, len(outs))]
	}

	return outs, nil
}

func (x *exec) nonNegative(e expr, what string) (int, error) {
	v, err := x.eval(e, row{}, nil)
	if err != nil {
		return 0, err
	}

	n, ok := v.(int64)
	if !ok || n < 0 {
		return 0, malformed("%s must be a non-negative integer", what)
	}

	return int(n), nil
}
