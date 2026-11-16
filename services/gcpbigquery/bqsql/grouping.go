package bqsql

import (
	"strconv"
	"strings"
)

const maxGroupingSets = 4096

// parseGroupBy parses the GROUP BY list; ROLLUP, CUBE and GROUPING SETS expand to grouping sets.
func (p *parser) parseGroupBy(s *selectStmt) error {
	var (
		sets    [][]*node
		special bool
		plain   []*node
	)

	sets = [][]*node{nil}

	for {
		elemSets, isSpecial, err := p.groupElement()
		if err != nil {
			return err
		}

		special = special || isSpecial
		if !isSpecial {
			plain = append(plain, elemSets[0]...)
		}

		if sets, err = crossSets(sets, elemSets); err != nil {
			return err
		}

		if !p.acceptPunct(",") {
			break
		}
	}

	if special {
		s.groupSets = sets
	} else {
		s.groupBy = plain
	}

	return nil
}

func crossSets(a, b [][]*node) ([][]*node, error) {
	if len(a)*len(b) > maxGroupingSets {
		return nil, invalid("GROUP BY expands to more than %d grouping sets", maxGroupingSets)
	}

	out := make([][]*node, 0, len(a)*len(b))

	for _, x := range a {
		for _, y := range b {
			set := make([]*node, 0, len(x)+len(y))
			set = append(set, x...)
			set = append(set, y...)
			out = append(out, set)
		}
	}

	return out, nil
}

func (p *parser) groupElement() ([][]*node, bool, error) {
	if err := p.enter(); err != nil {
		return nil, false, err
	}
	defer p.leave()

	next := p.peekNext()

	switch {
	case p.kw("ROLLUP") && next.isPunct("("):
		p.i++

		tuples, err := p.tupleList()

		return rollupSets(tuples), true, err
	case p.kw("CUBE") && next.isPunct("("):
		p.i++

		tuples, err := p.tupleList()
		if err == nil && len(tuples) > maxCubeDims {
			err = invalid("CUBE has too many elements")
		}

		return cubeSets(tuples), true, err
	case p.kw("GROUPING") && next.isKw("SETS"):
		p.i += 2

		sets, err := p.groupingSetList()

		return sets, true, err
	}

	e, err := p.expr()
	if err != nil {
		return nil, false, err
	}

	return [][]*node{{e}}, false, nil
}

const maxCubeDims = 12

// tupleList parses "(e | (e, e), ...)" returning each element as a tuple of expressions.
func (p *parser) tupleList() ([][]*node, error) {
	if err := p.expectPunct("("); err != nil {
		return nil, err
	}

	var out [][]*node

	for {
		t, err := p.tuple()
		if err != nil {
			return nil, err
		}

		out = append(out, t)

		if !p.acceptPunct(",") {
			break
		}
	}

	return out, p.expectPunct(")")
}

// tuple parses one expression or a parenthesised expression list; "()" is the empty tuple.
func (p *parser) tuple() ([]*node, error) {
	if p.peek().isPunct("(") {
		save := p.i
		p.i++

		if p.acceptPunct(")") {
			return nil, nil
		}

		list, err := p.exprList()
		if err == nil && p.acceptPunct(")") && (p.peek().isPunct(",") || p.peek().isPunct(")")) {
			return list, nil
		}

		p.i = save
	}

	e, err := p.expr()
	if err != nil {
		return nil, err
	}

	return []*node{e}, nil
}

func (p *parser) groupingSetList() ([][]*node, error) {
	if err := p.expectPunct("("); err != nil {
		return nil, err
	}

	var out [][]*node

	for {
		if p.peekNext().isPunct("(") && (p.kw("ROLLUP") || p.kw("CUBE")) ||
			p.kw("GROUPING") && p.peekNext().isKw("SETS") {
			sets, _, err := p.groupElement()
			if err != nil {
				return nil, err
			}

			out = append(out, sets...)
		} else {
			t, err := p.tuple()
			if err != nil {
				return nil, err
			}

			out = append(out, t)
		}

		if !p.acceptPunct(",") {
			break
		}
	}

	return out, p.expectPunct(")")
}

func rollupSets(tuples [][]*node) [][]*node {
	out := make([][]*node, 0, len(tuples)+1)

	for n := len(tuples); n >= 0; n-- {
		var set []*node
		for _, t := range tuples[:n] {
			set = append(set, t...)
		}

		out = append(out, set)
	}

	return out
}

func cubeSets(tuples [][]*node) [][]*node {
	total := 1 << len(tuples)
	out := make([][]*node, 0, total)

	for mask := total - 1; mask >= 0; mask-- {
		var set []*node

		for i, t := range tuples {
			if mask&(1<<(len(tuples)-1-i)) != 0 {
				set = append(set, t...)
			}
		}

		out = append(out, set)
	}

	return out
}

func nodeKey(n *node) string {
	if n == nil {
		return "-"
	}

	var sb strings.Builder

	sb.WriteString(strconv.Itoa(int(n.kind)) + "|" + strings.ToLower(n.name) + "|" + n.str + "|")

	if len(n.parts) > 0 {
		sb.WriteString(strings.ToLower(n.parts[len(n.parts)-1]))
	}

	if n.not {
		sb.WriteString("!")
	}

	if n.distinct {
		sb.WriteString("D")
	}

	if n.safe {
		sb.WriteString("S")
	}

	if n.castTo != nil {
		sb.WriteString("T" + strconv.Itoa(int(n.castTo.Kind)))
	}

	if n.query != nil {
		sb.WriteString("Q" + strconv.Itoa(n.pos))
	}

	sb.WriteString("(")

	for _, a := range n.args {
		sb.WriteString(nodeKey(a) + ",")
	}

	sb.WriteString(")")

	return sb.String()
}

type groupRewrite struct {
	dropped map[string]bool
}

// rewrite replaces expressions of rolled-up columns outside aggregates with typed NULLs and resolves GROUPING().
func (g *groupRewrite) rewrite(n *node) *node {
	if n == nil {
		return nil
	}

	if n.kind == nCall && n.over == nil {
		switch {
		case strings.EqualFold(n.name, "GROUPING") && len(n.args) == 1:
			if g.dropped[nodeKey(n.args[0])] {
				return &node{kind: nInt, str: "1", pos: n.pos}
			}

			return &node{kind: nInt, str: "0", pos: n.pos}
		case isAggregateName(n.name):
			return n
		}
	}

	if g.dropped[nodeKey(n)] && n.kind != nInt && n.kind != nString {
		return &node{kind: nNullOf, args: []*node{n}, pos: n.pos}
	}

	if n.query != nil || n.over != nil || len(n.args) == 0 {
		return n
	}

	cp := *n
	cp.args = make([]*node, len(n.args))

	for i, a := range n.args {
		cp.args[i] = g.rewrite(a)
	}

	return &cp
}

func aggregateNames() map[string]bool {
	m := map[string]bool{}

	for _, n := range []string{
		"COUNT", "COUNTIF", "SUM", "AVG", "MIN", "MAX", "ANY_VALUE", "LOGICAL_AND", "LOGICAL_OR",
		"APPROX_COUNT_DISTINCT", "STRING_AGG", "ARRAY_AGG", "APPROX_QUANTILES", "APPROX_TOP_COUNT", "STDDEV",
		"STDDEV_SAMP", "STDDEV_POP", "VARIANCE", "VAR_SAMP", "VAR_POP", "HLL_COUNT.INIT", "HLL_COUNT.MERGE",
		"HLL_COUNT.MERGE_PARTIAL",
	} {
		m[n] = true
	}

	return m
}

func isAggregateName(name string) bool { return aggregateNames()[strings.ToUpper(name)] }

// resolveGroupSets turns ordinals and select-list aliases in the grouping sets into the expressions they name.
func resolveGroupSets(sets [][]*node, items []selectItem) ([][]*node, error) {
	out := make([][]*node, len(sets))

	for i, set := range sets {
		out[i] = make([]*node, len(set))

		for j, n := range set {
			r, err := resolveGroupExpr(n, items)
			if err != nil {
				return nil, err
			}

			out[i][j] = r
		}
	}

	return out, nil
}

func resolveGroupExpr(n *node, items []selectItem) (*node, error) {
	if n.kind == nInt {
		i, _ := strconv.Atoi(n.str)
		if i < 1 || i > len(items) || items[i-1].expr.kind == nStar {
			return nil, invalid("GROUP BY position %d is out of range", i)
		}

		return items[i-1].expr, nil
	}

	if n.kind == nPath && len(n.parts) == 1 {
		for _, it := range items {
			if it.alias != "" && strings.EqualFold(it.alias, n.parts[0]) && it.expr.kind != nStar {
				return it.expr, nil
			}
		}
	}

	return n, nil
}

func (c *compiler) compileGroupingSets(s *selectStmt, outer *scope) (*compiledQuery, error) {
	sets, err := resolveGroupSets(s.groupSets, s.items)
	if err != nil {
		return nil, err
	}

	for _, it := range s.items {
		if it.expr.kind == nStar {
			return nil, invalid("SELECT * is not allowed with GROUP BY ROLLUP, CUBE or GROUPING SETS")
		}
	}

	universe := map[string]bool{}

	for _, set := range sets {
		for _, n := range set {
			universe[nodeKey(n)] = true
		}
	}

	var out *compiledQuery

	parts := make([]string, 0, len(sets))

	for _, set := range sets {
		cq, cerr := c.compileOneGroupingSet(s, set, universe, outer)
		if cerr != nil {
			return nil, cerr
		}

		parts = append(parts, "SELECT * FROM ("+cq.sql+")")

		if out == nil {
			out = cq

			continue
		}

		for i := range out.cols {
			out.cols[i].Type = commonType(out.cols[i].Type, cq.cols[i].Type)
		}
	}

	out.sql = strings.Join(parts, " UNION ALL ")

	return out, nil
}

func (c *compiler) compileOneGroupingSet(
	s *selectStmt, set []*node, universe map[string]bool, outer *scope,
) (*compiledQuery, error) {
	in := map[string]bool{}
	for _, n := range set {
		in[nodeKey(n)] = true
	}

	rw := &groupRewrite{dropped: map[string]bool{}}

	for k := range universe {
		if !in[k] {
			rw.dropped[k] = true
		}
	}

	cp := *s
	cp.groupSets = nil
	cp.groupBy = set
	cp.items = make([]selectItem, len(s.items))
	cp.global = len(set) == 0

	for i, it := range s.items {
		alias := it.alias
		if alias == "" {
			alias = itemName(it, i)
		}

		cp.items[i] = selectItem{expr: rw.rewrite(it.expr), alias: alias}
	}

	cp.having = rw.rewrite(s.having)
	cp.qualify = rw.rewrite(s.qualify)

	so, err := c.compileSelect(&cp, outer)
	if err != nil {
		return nil, err
	}

	return &compiledQuery{sql: so.sql, cols: so.cols, aliases: so.aliases}, nil
}
