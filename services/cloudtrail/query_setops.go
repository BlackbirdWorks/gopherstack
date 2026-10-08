package cloudtrail

import (
	"maps"
	"math"
	"slices"
	"sort"
	"strings"
)

// inSubNode, cmpSubNode and existsNode are WHERE nodes holding an uncorrelated subquery; resolveWhere
// replaces them with their evaluated form before any row is tested.
type inSubNode struct {
	sub    *queryNode
	column string
	negate bool
}

func (inSubNode) eval(map[string]string) bool { return false }

type cmpSubNode struct {
	sub    *queryNode
	column string
	negate bool
}

func (cmpSubNode) eval(map[string]string) bool { return false }

type existsNode struct{ sub *queryNode }

func (existsNode) eval(map[string]string) bool { return false }

type constNode bool

func (n constNode) eval(map[string]string) bool { return bool(n) }

// relation is a positional result set: output column names and their rows.
type relation struct {
	names []string
	rows  [][]string
}

func relationFromCells(cells [][]map[string]string, items []selectItem) relation {
	rel := relation{rows: make([][]string, 0, len(cells))}

	for _, it := range items {
		rel.names = append(rel.names, it.outName)
	}

	for _, row := range cells {
		vals := make([]string, 0, len(row))

		for _, cell := range row {
			for k, v := range cell {
				if len(items) == 0 && len(rel.rows) == 0 {
					rel.names = append(rel.names, k)
				}

				vals = append(vals, v)
			}
		}

		rel.rows = append(rel.rows, vals)
	}

	return rel
}

func (r relation) cells() [][]map[string]string {
	out := make([][]map[string]string, 0, len(r.rows))

	for _, row := range r.rows {
		cells := make([]map[string]string, 0, len(row))

		for i, v := range row {
			cells = append(cells, map[string]string{r.names[i]: v})
		}

		out = append(out, cells)
	}

	return out
}

// queryExec carries the event log and scan counters across one statement's evaluation.
type queryExec struct {
	events  []Event
	matched int64
}

// run evaluates a query expression. top marks the statement's root, the only level the row cap applies to.
func (x *queryExec) run(n *queryNode, top bool) (relation, string) {
	if n.sel != nil {
		return x.runSelect(n.sel, top)
	}

	left, errMsg := x.run(n.left, false)
	if errMsg != "" {
		return relation{}, errMsg
	}

	right, errMsg := x.run(n.right, false)
	if errMsg != "" {
		return relation{}, errMsg
	}

	if len(left.rows) > 0 && len(right.rows) > 0 && len(left.rows[0]) != len(right.rows[0]) {
		return relation{}, n.op + " operands must have the same number of columns"
	}

	out := relation{names: left.names, rows: combineRows(n, left.rows, right.rows)}
	if len(out.names) == 0 {
		out.names = right.names
	}

	if sortErr := sortRelation(out, n.orderBy); sortErr != "" {
		return relation{}, sortErr
	}

	limit := math.MaxInt
	if top {
		limit = effectiveQueryLimit(n.limit)
	} else if n.limit > 0 {
		limit = n.limit
	}

	if len(out.rows) > limit {
		out.rows = out.rows[:limit]
	}

	return out, ""
}

func rowKey(row []string) string { return strings.Join(row, groupKeyFieldSep) }

func combineRows(n *queryNode, left, right [][]string) [][]string {
	switch n.op {
	case setOpUnion:
		all := slices.Concat(left, right)
		if n.all {
			return all
		}

		return dedupRows(all)
	case setOpIntersect:
		return dedupRows(filterRows(left, memberOf(keySet(right), true)))
	default:
		return dedupRows(filterRows(left, memberOf(keySet(right), false)))
	}
}

func memberOf(set map[string]struct{}, want bool) func([]string) bool {
	return func(r []string) bool {
		_, ok := set[rowKey(r)]

		return ok == want
	}
}

func keySet(rows [][]string) map[string]struct{} {
	out := make(map[string]struct{}, len(rows))
	for _, r := range rows {
		out[rowKey(r)] = struct{}{}
	}

	return out
}

func filterRows(rows [][]string, keep func([]string) bool) [][]string {
	out := make([][]string, 0, len(rows))

	for _, r := range rows {
		if keep(r) {
			out = append(out, r)
		}
	}

	return out
}

func dedupRows(rows [][]string) [][]string {
	seen := make(map[string]struct{}, len(rows))
	out := make([][]string, 0, len(rows))

	for _, r := range rows {
		k := rowKey(r)
		if _, dup := seen[k]; dup {
			continue
		}

		seen[k] = struct{}{}
		out = append(out, r)
	}

	return out
}

func sortRelation(rel relation, orderBy []orderTerm) string {
	idx := make([]int, len(orderBy))

	for i, term := range orderBy {
		idx[i] = slices.IndexFunc(rel.names, func(n string) bool { return strings.EqualFold(n, term.name) })
		if idx[i] < 0 {
			return "ORDER BY " + term.name + " must name an output column of the combined query"
		}
	}

	sort.SliceStable(rel.rows, func(i, j int) bool {
		for k, term := range orderBy {
			c := compareOrderValues(rel.rows[i][idx[k]], rel.rows[j][idx[k]])
			if c == 0 {
				continue
			}

			if term.desc {
				return c > 0
			}

			return c < 0
		}

		return false
	})

	return ""
}

func (x *queryExec) runSelect(pq *parsedLakeQuery, top bool) (relation, string) {
	rows, errMsg := x.sourceRows(pq)
	if errMsg != "" {
		return relation{}, errMsg
	}

	filtered := make([]map[string]string, 0, len(rows))

	where, errMsg := x.resolveWhere(pq.where)
	if errMsg != "" {
		return relation{}, errMsg
	}

	for _, row := range rows {
		if where == nil || where.eval(row) {
			filtered = append(filtered, row)
		}
	}

	x.matched += int64(len(filtered))

	limit := math.MaxInt
	if top {
		limit = effectiveQueryLimit(pq.limit)
	} else if pq.limit > 0 {
		limit = pq.limit
	}

	hide := sourcePrefixes(pq)

	cells, errMsg := projectRows(filtered, *pq, limit, hide)
	if errMsg != "" {
		return relation{}, errMsg
	}

	return relationFromCells(cells, pq.items), ""
}

func sourcePrefixes(pq *parsedLakeQuery) []string {
	out := make([]string, 0, 1+len(pq.joins))
	out = append(out, pq.from.prefix())

	for _, j := range pq.joins {
		out = append(out, j.src.prefix())
	}

	return out
}

// resolveWhere replaces subquery nodes with their evaluated equivalents.
func (x *queryExec) resolveWhere(e whereExpr) (whereExpr, string) {
	switch n := e.(type) {
	case andNode:
		l, r, errMsg := x.resolvePair(n.left, n.right)

		return andNode{left: l, right: r}, errMsg
	case orNode:
		l, r, errMsg := x.resolvePair(n.left, n.right)

		return orNode{left: l, right: r}, errMsg
	case notNode:
		inner, errMsg := x.resolveWhere(n.inner)

		return notNode{inner: inner}, errMsg
	case inSubNode:
		rel, errMsg := x.run(n.sub, false)
		if errMsg != "" {
			return nil, errMsg
		}

		values := make(map[string]struct{}, len(rel.rows))
		for _, r := range rel.rows {
			if len(r) > 0 {
				values[r[0]] = struct{}{}
			}
		}

		return inNode{column: n.column, values: values, negate: n.negate}, ""
	case cmpSubNode:
		rel, errMsg := x.run(n.sub, false)
		if errMsg != "" {
			return nil, errMsg
		}

		if len(rel.rows) != 1 || len(rel.rows[0]) != 1 {
			return nil, "scalar subquery must return exactly one row and one column"
		}

		return cmpNode{column: n.column, value: rel.rows[0][0], negate: n.negate}, ""
	case existsNode:
		rel, errMsg := x.run(n.sub, false)

		return constNode(len(rel.rows) > 0), errMsg
	default:
		return e, ""
	}
}

func (x *queryExec) resolvePair(l, r whereExpr) (whereExpr, whereExpr, string) {
	left, errMsg := x.resolveWhere(l)
	if errMsg != "" {
		return nil, nil, errMsg
	}

	right, errMsg := x.resolveWhere(r)

	return left, right, errMsg
}

// sourceRows builds the FROM relation, folding in each JOIN.
func (x *queryExec) sourceRows(pq *parsedLakeQuery) ([]map[string]string, string) {
	qualify := len(pq.joins) > 0 || pq.from.alias != ""

	rows, errMsg := x.loadSource(pq.from, qualify)
	if errMsg != "" {
		return nil, errMsg
	}

	for _, j := range pq.joins {
		right, rightErr := x.loadSource(j.src, true)
		if rightErr != "" {
			return nil, rightErr
		}

		rows = joinRows(rows, right, j)
	}

	return rows, ""
}

func (x *queryExec) loadSource(src fromSource, qualify bool) ([]map[string]string, string) {
	var rows []map[string]string

	if src.sub != nil {
		rel, errMsg := x.run(src.sub, false)
		if errMsg != "" {
			return nil, errMsg
		}

		rows = make([]map[string]string, 0, len(rel.rows))

		for _, r := range rel.rows {
			row := make(map[string]string, len(r))
			for i, v := range r {
				row[strings.ToLower(rel.names[i])] = v
			}

			rows = append(rows, row)
		}
	} else {
		rows = make([]map[string]string, 0, len(x.events))
		for _, ev := range x.events {
			rows = append(rows, eventToRow(ev))
		}
	}

	prefix := src.prefix()
	if !qualify || prefix == "" {
		return rows, ""
	}

	for _, row := range rows {
		for k, v := range maps.Clone(row) {
			row[prefix+"."+k] = v
		}
	}

	return rows, ""
}

type joinPair struct{ left, right string }

// joinRows combines left and right per the join's equality conditions. NULL (absent) keys never match.
func joinRows(left, right []map[string]string, j joinClause) []map[string]string {
	rp := j.src.prefix()
	conds := orientConds(j.on, rp)
	index := make(map[string][]int, len(right))

	for i, row := range right {
		if k, ok := joinKey(row, conds, false); ok {
			index[k] = append(index[k], i)
		}
	}

	matchedRight := make([]bool, len(right))
	out := make([]map[string]string, 0, len(left))

	for _, lrow := range left {
		k, ok := joinKey(lrow, conds, true)
		hits := index[k]

		if !ok || len(hits) == 0 {
			if j.kind == joinLeft {
				out = append(out, lrow)
			}

			continue
		}

		for _, ri := range hits {
			matchedRight[ri] = true
			out = append(out, mergeRows(lrow, right[ri], rp))
		}
	}

	if j.kind == joinRight {
		for i, row := range right {
			if !matchedRight[i] {
				out = append(out, row)
			}
		}
	}

	return out
}

// orientConds makes each pair's left column the one from the already-joined side.
func orientConds(on []joinCond, rightPrefix string) []joinPair {
	conds := make([]joinPair, len(on))

	for i, c := range on {
		l, r := c.left, c.right
		if rightPrefix != "" && strings.HasPrefix(l, rightPrefix+".") && !strings.HasPrefix(r, rightPrefix+".") {
			l, r = r, l
		}

		conds[i] = joinPair{left: l, right: r}
	}

	return conds
}

func joinKey(row map[string]string, conds []joinPair, leftSide bool) (string, bool) {
	parts := make([]string, len(conds))

	for i, c := range conds {
		col := c.right
		if leftSide {
			col = c.left
		}

		if row[col] == "" {
			return "", false
		}

		parts[i] = row[col]
	}

	return strings.Join(parts, groupKeyFieldSep), true
}

// mergeRows overlays right onto a copy of left; unqualified right columns never shadow left ones.
func mergeRows(left, right map[string]string, rightPrefix string) map[string]string {
	out := maps.Clone(left)

	for k, v := range right {
		if _, exists := out[k]; !exists || (rightPrefix != "" && strings.HasPrefix(k, rightPrefix+".")) {
			out[k] = v
		}
	}

	return out
}
