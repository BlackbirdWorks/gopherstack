package bqsql

import (
	"strconv"
	"strings"
)

const (
	recursionCol        = "__iter"
	maxRecursionDepth   = 500
	recursionLimitError = "Recursive CTE iteration limit of 500 exceeded"
)

func refersTo(q *Query, name string) bool {
	if q.body == nil || q.body.op != litUnionAll || q.body.right == nil || q.body.right.sel == nil {
		return false
	}

	return fromRefers(q.body.right.sel.from, name)
}

func fromRefers(f *fromItem, name string) bool {
	if f == nil {
		return false
	}

	switch f.kind {
	case fromTable:
		return len(f.path) == 1 && strings.EqualFold(f.path[0], name)
	case fromJoin:
		return fromRefers(f.left, name) || fromRefers(f.right, name)
	case fromSubquery, fromUnnest, fromPivot:
		return false
	}

	return false
}

// compileRecursive compiles "anchor UNION ALL recursive" with an iteration counter so runaway recursion fails.
func (c *compiler) compileRecursive(w cte, defs map[string]*cteDef) (*compiledQuery, error) {
	body := w.query.body

	if len(w.query.with) > 0 || len(w.query.order) > 0 || w.query.limit != nil {
		return nil, unsupported("a recursive CTE body with its own WITH, ORDER BY or LIMIT")
	}

	left, _, err := c.compileSetExpr(body.left, nil)
	if err != nil {
		return nil, err
	}

	names := make([]string, len(left.cols))
	for i, col := range left.cols {
		names[i] = col.Name
	}

	if len(w.cols) > 0 {
		if len(w.cols) != len(left.cols) {
			return nil, invalid("WITH %s has %d columns but %d names", w.name, len(left.cols), len(w.cols))
		}

		names = w.cols
	}

	sqlNames := uniqueNames(names)

	def := &cteDef{}
	for i, col := range left.cols {
		def.cols = append(def.cols, &colInfo{name: names[i], typ: col.Type, sql: quoteIdent(sqlNames[i])})
	}

	def.cols = append(def.cols, &colInfo{name: recursionCol, sql: quoteIdent(recursionCol), hidden: true})
	defs[w.name] = def

	rsel := *body.right.sel
	rsel.items = append(
		append([]selectItem{}, rsel.items...),
		selectItem{expr: &node{kind: nRecurIter}, alias: recursionCol},
	)

	so, err := c.compileSelect(&rsel, nil)
	if err != nil {
		return nil, err
	}

	if len(so.cols) != len(left.cols)+1 {
		return nil, invalid(
			"Queries in UNION ALL have mismatched column count; query 1 has %d columns, query 2 has %d columns",
			len(left.cols), len(so.cols)-1,
		)
	}

	return &compiledQuery{
		sql:       "SELECT *, 0 FROM (" + left.sql + ") UNION ALL " + so.sql,
		cols:      left.cols,
		aliases:   left.aliases,
		recursive: true,
	}, nil
}

func recurIter(sc *scope) (*gx, error) {
	for s := sc; s != nil; s = s.parent {
		for _, rel := range s.rels {
			for _, col := range rel.cols {
				if col.name == recursionCol {
					it := col.sql

					return &gx{
						sql: "CASE WHEN " + it + " >= " + strconv.Itoa(maxRecursionDepth) +
							" THEN bq_error(" + quoteStr(recursionLimitError) + ") ELSE " + it + " + 1 END",
						typ: scalar(KindInt),
					}, nil
				}
			}
		}
	}

	return nil, invalid("recursive reference not found")
}
