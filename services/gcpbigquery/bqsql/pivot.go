package bqsql

import (
	"strings"
)

type pivotAgg struct {
	call  *node
	alias string
}

type pivotValue struct {
	expr  *node
	alias string
}

type unpivotSet struct {
	alias *node
	cols  []string
}

type pivotSpec struct {
	input     *node
	nameCol   string
	aggs      []pivotAgg
	values    []pivotValue
	valueCols []string
	sets      []unpivotSet
	unpivot   bool
	withNulls bool
}

func (p *parser) atPivot() bool {
	t, n := p.peek(), p.peekNext()

	switch {
	case t.isKw("PIVOT"):
		return n.isPunct("(")
	case t.isKw("UNPIVOT"):
		return n.isPunct("(") || n.isKw("INCLUDE") || n.isKw("EXCLUDE")
	}

	return false
}

// pivotSuffix wraps f in the PIVOT or UNPIVOT operators that follow it.
func (p *parser) pivotSuffix(f *fromItem) (*fromItem, error) {
	for p.atPivot() {
		var (
			spec *pivotSpec
			err  error
		)

		if p.kw("PIVOT") {
			p.i++
			spec, err = p.pivotClause()
		} else {
			p.i++
			spec, err = p.unpivotClause()
		}

		if err != nil {
			return nil, err
		}

		f = &fromItem{kind: fromPivot, left: f, pivot: spec}

		if f.alias, err = p.optAlias(); err != nil {
			return nil, err
		}
	}

	return f, nil
}

func (p *parser) pivotClause() (*pivotSpec, error) {
	spec := &pivotSpec{}

	if err := p.expectPunct("("); err != nil {
		return nil, err
	}

	for {
		call, err := p.expr()
		if err != nil {
			return nil, err
		}

		alias, aerr := p.optAlias()
		if aerr != nil {
			return nil, aerr
		}

		spec.aggs = append(spec.aggs, pivotAgg{call: call, alias: alias})

		if !p.acceptPunct(",") {
			break
		}
	}

	if err := p.expectKw("FOR"); err != nil {
		return nil, err
	}

	path, err := p.tablePath()
	if err != nil {
		return nil, err
	}

	spec.input = &node{kind: nPath, parts: path, pos: p.peek().pos}

	if err = p.expectKw("IN"); err != nil {
		return nil, err
	}

	if err = p.expectPunct("("); err != nil {
		return nil, err
	}

	for {
		v, verr := p.expr()
		if verr != nil {
			return nil, verr
		}

		alias, aerr := p.optAlias()
		if aerr != nil {
			return nil, aerr
		}

		spec.values = append(spec.values, pivotValue{expr: v, alias: alias})

		if !p.acceptPunct(",") {
			break
		}
	}

	if err = p.expectPunct(")"); err != nil {
		return nil, err
	}

	return spec, p.expectPunct(")")
}

func (p *parser) unpivotClause() (*pivotSpec, error) {
	spec := &pivotSpec{unpivot: true}

	switch {
	case p.acceptKw("INCLUDE"):
		spec.withNulls = true

		if err := p.expectKw("NULLS"); err != nil {
			return nil, err
		}
	case p.acceptKw("EXCLUDE"):
		if err := p.expectKw("NULLS"); err != nil {
			return nil, err
		}
	}

	var err error

	if err = p.expectPunct("("); err != nil {
		return nil, err
	}

	if spec.valueCols, err = p.identTuple(); err != nil {
		return nil, err
	}

	if err = p.expectKw("FOR"); err != nil {
		return nil, err
	}

	if spec.nameCol, err = p.identName(); err != nil {
		return nil, err
	}

	if err = p.expectKw("IN"); err != nil {
		return nil, err
	}

	if spec.sets, err = p.unpivotSets(); err != nil {
		return nil, err
	}

	return spec, p.expectPunct(")")
}

func (p *parser) unpivotSets() ([]unpivotSet, error) {
	if err := p.expectPunct("("); err != nil {
		return nil, err
	}

	var out []unpivotSet

	for {
		cols, err := p.identTuple()
		if err != nil {
			return nil, err
		}

		set := unpivotSet{cols: cols}

		if p.acceptKw("AS") {
			if set.alias, err = p.primary(); err != nil {
				return nil, err
			}
		}

		out = append(out, set)

		if !p.acceptPunct(",") {
			break
		}
	}

	return out, p.expectPunct(")")
}

// identTuple parses "name" or "(name, name, ...)".
func (p *parser) identTuple() ([]string, error) {
	if p.acceptPunct("(") {
		names, err := p.identList()
		if err != nil {
			return nil, err
		}

		return names, nil
	}

	n, err := p.identName()

	return []string{n}, err
}

func (c *compiler) fromPivot(f *fromItem, sc *scope) (string, error) {
	inner := &scope{parent: sc.parent, aliasItems: map[string]*gx{}}

	base, err := c.compileFrom(f.left, inner)
	if err != nil {
		return "", err
	}

	var cols []*colInfo

	for _, rel := range inner.rels {
		cols = append(cols, c.starColumns(rel)...)
	}

	var (
		out    []selectedCol
		sqlOut string
	)

	if f.pivot.unpivot {
		out, sqlOut, err = c.unpivotSQL(f.pivot, cols, base)
	} else {
		out, sqlOut, err = c.pivotSQL(f.pivot, inner, cols, base)
	}

	if err != nil {
		return "", err
	}

	alias := f.alias
	if alias == "" {
		alias = c.uniq("_pv")
	}

	rel := &relation{alias: alias}
	for _, col := range out {
		rel.cols = append(
			rel.cols,
			&colInfo{name: col.name, typ: col.typ, sql: quoteIdent(alias) + "." + quoteIdent(col.name)},
		)
	}

	sc.rels = append(sc.rels, rel)

	return "(" + sqlOut + ") AS " + quoteIdent(alias), nil
}

type selectedCol struct {
	name string
	typ  Type
}

func pathRoots(n *node, into map[string]bool) {
	if n == nil {
		return
	}

	if n.kind == nPath {
		into[strings.ToLower(n.parts[0])] = true

		if len(n.parts) > 1 {
			into[strings.ToLower(n.parts[1])] = true
		}
	}

	for _, a := range n.args {
		pathRoots(a, into)
	}
}

func checkPivotAggs(spec *pivotSpec) (map[string]bool, error) {
	used := map[string]bool{}
	pathRoots(spec.input, used)

	for _, a := range spec.aggs {
		if len(spec.aggs) > 1 && a.alias == "" {
			return nil, invalid("Each aggregate in PIVOT must have an alias when there are several")
		}

		if a.call.kind != nCall || !isAggregateName(a.call.name) || len(a.call.args) != 1 || a.call.over != nil {
			return nil, invalid("PIVOT expects aggregate function calls with a single argument")
		}

		pathRoots(a.call, used)
	}

	return used, nil
}

func (c *compiler) pivotSQL(
	spec *pivotSpec,
	inner *scope,
	cols []*colInfo,
	base string,
) ([]selectedCol, string, error) {
	used, err := checkPivotAggs(spec)
	if err != nil {
		return nil, "", err
	}

	in, err := c.expr(spec.input, inner)
	if err != nil {
		return nil, "", err
	}

	var (
		sel, group []string
		out        []selectedCol
	)

	for _, col := range cols {
		if !used[strings.ToLower(col.name)] {
			sel = append(sel, col.sql+" AS "+quoteIdent(col.name))
			group = append(group, col.sql)
			out = append(out, selectedCol{col.name, col.typ})
		}
	}

	valSel, valOut, err := c.pivotValues(spec, in, inner)
	if err != nil {
		return nil, "", err
	}

	sql := "SELECT " + strings.Join(append(sel, valSel...), ", ") + " FROM " + base
	if len(group) > 0 {
		sql += " GROUP BY " + strings.Join(group, ", ")
	}

	return append(out, valOut...), sql, nil
}

// pivotValues builds one aggregate column per IN value and aggregate.
func (c *compiler) pivotValues(spec *pivotSpec, in *gx, inner *scope) ([]string, []selectedCol, error) {
	var (
		sel  []string
		out  []selectedCol
		seen = map[string]bool{}
	)

	for _, v := range spec.values {
		vg, err := c.expr(v.expr, nil)
		if err != nil {
			return nil, nil, err
		}

		cond := "(" + in.sql + ") IS (" + coerceLiteral(vg, in.typ).sql + ")"

		for _, a := range spec.aggs {
			name, nerr := pivotColumnName(a.alias, v)
			if nerr != nil {
				return nil, nil, nerr
			}

			if seen[strings.ToLower(name)] {
				return nil, nil, invalid("Duplicate column name %s in PIVOT output", name)
			}

			seen[strings.ToLower(name)] = true

			g, gerr := c.pivotAggregate(a.call, cond, inner)
			if gerr != nil {
				return nil, nil, gerr
			}

			sel = append(sel, g.sql+" AS "+quoteIdent(name))
			out = append(out, selectedCol{name, g.typ})
		}
	}

	return sel, out, nil
}

func (c *compiler) pivotAggregate(call *node, cond string, inner *scope) (*gx, error) {
	arg := call.args[0]
	argSQL, argTyp := "1", scalar(KindInt)

	if arg.kind != nStar {
		g, err := c.expr(arg, inner)
		if err != nil {
			return nil, err
		}

		argSQL, argTyp = g.sql, g.typ
	}

	cp := *call
	cp.args = []*node{{kind: nRawSQL, str: "CASE WHEN " + cond + " THEN " + argSQL + " END", castTo: &argTyp}}

	return c.expr(&cp, inner)
}

func pivotColumnName(prefix string, v pivotValue) (string, error) {
	name := v.alias

	if name == "" {
		var err error

		if name, err = valueText(v.expr); err != nil {
			return "", err
		}
	}

	if prefix != "" {
		return prefix + "_" + name, nil
	}

	return name, nil
}

// valueText renders a PIVOT value the way BigQuery names the output column.
func valueText(n *node) (string, error) {
	switch n.kind {
	case nNull:
		return "NULL", nil
	case nString:
		return n.str, nil
	case nBool:
		return strings.ToUpper(n.name), nil
	case nInt, nFloat:
		return numberName(n.str), nil
	case nUnary:
		if n.name == "-" && len(n.args) == 1 && (n.args[0].kind == nInt || n.args[0].kind == nFloat) {
			return "minus_" + strings.ReplaceAll(n.args[0].str, ".", "_point_"), nil
		}
	case nTyped:
		if strings.EqualFold(n.name, litDate) {
			return "_" + strings.ReplaceAll(n.str, "-", "_"), nil
		}
	case nParam, nPath, nStar, nBinary, nCall, nCase, nCast, nExtract, nBetween, nIn, nLike, nIs, nExists, nSubquery,
		nArrayQuery, nArray, nStruct, nIndex, nField, nInterval, nBytes, nNullOf, nRecurIter, nRawSQL, nTableArg:
	}

	return "", invalid("A PIVOT value of this type needs an alias")
}

func numberName(s string) string { return "_" + strings.ReplaceAll(s, ".", "_point_") }

func (c *compiler) unpivotSQL(spec *pivotSpec, cols []*colInfo, base string) ([]selectedCol, string, error) {
	byName := map[string]*colInfo{}
	for _, col := range cols {
		byName[strings.ToLower(col.name)] = col
	}

	unpivoted := map[string]bool{}

	for _, set := range spec.sets {
		if len(set.cols) != len(spec.valueCols) {
			return nil, "", invalid("UNPIVOT column sets must have %d columns", len(spec.valueCols))
		}

		for _, name := range set.cols {
			if byName[strings.ToLower(name)] == nil {
				return nil, "", invalid("Unrecognized name: %s", name)
			}

			unpivoted[strings.ToLower(name)] = true
		}
	}

	for _, name := range append([]string{spec.nameCol}, spec.valueCols...) {
		if unpivoted[strings.ToLower(name)] {
			return nil, "", invalid("UNPIVOT output column %s conflicts with an unpivoted column", name)
		}
	}

	var (
		other []*colInfo
		out   []selectedCol
	)

	for _, col := range cols {
		if !unpivoted[strings.ToLower(col.name)] {
			other = append(other, col)
			out = append(out, selectedCol{col.name, col.typ})
		}
	}

	nameTyp, err := unpivotNameType(spec)
	if err != nil {
		return nil, "", err
	}

	valTypes := make([]Type, len(spec.valueCols))

	for i, vc := range spec.valueCols {
		valTypes[i] = unpivotValueType(spec, byName, i)
		out = append(out, selectedCol{vc, valTypes[i]})
	}

	out = append(out, selectedCol{spec.nameCol, nameTyp})
	parts := make([]string, len(spec.sets))

	for i, set := range spec.sets {
		parts[i] = c.unpivotMember(spec, set, byName, other, base, nameTyp, valTypes)
	}

	return out, "SELECT * FROM (" + strings.Join(parts, " UNION ALL ") + ")", nil
}

// unpivotValueType is the type of value column i: the first column set's type, widened when decimals are mixed in.
func unpivotValueType(spec *pivotSpec, byName map[string]*colInfo, i int) Type {
	first := byName[strings.ToLower(spec.sets[0].cols[i])].typ
	gs := make([]*gx, len(spec.sets))

	for j, set := range spec.sets {
		gs[j] = &gx{typ: byName[strings.ToLower(set.cols[i])].typ}
	}

	if !hasDec(gs) {
		return first
	}

	t, _ := unifyAll(gs)

	return t
}

func unpivotNameType(spec *pivotSpec) (Type, error) {
	typ := scalar(KindString)

	for i, set := range spec.sets {
		if set.alias == nil {
			continue
		}

		var k Kind

		switch set.alias.kind {
		case nString:
			k = KindString
		case nInt:
			k = KindInt
		default:
			return typ, invalid("UNPIVOT row value aliases must be STRING or INT64 literals")
		}

		if i == 0 {
			typ = scalar(k)
		} else if typ.Kind != k {
			return typ, invalid("UNPIVOT row value aliases must all have the same type")
		}
	}

	if typ.Kind == KindInt {
		for _, set := range spec.sets {
			if set.alias == nil {
				return typ, invalid("INT64 UNPIVOT aliases must be given for every column set")
			}
		}
	}

	return typ, nil
}

func (c *compiler) unpivotMember(
	spec *pivotSpec, set unpivotSet, byName map[string]*colInfo, other []*colInfo, base string, nameTyp Type,
	valTypes []Type,
) string {
	sel := make([]string, 0, len(other)+len(set.cols)+1)

	for _, col := range other {
		sel = append(sel, col.sql+" AS "+quoteIdent(col.name))
	}

	conds := make([]string, 0, len(set.cols))

	for i, name := range set.cols {
		col := byName[strings.ToLower(name)]
		sel = append(sel, toType(&gx{sql: col.sql, typ: col.typ}, valTypes[i]).sql+" AS "+quoteIdent(spec.valueCols[i]))
		conds = append(conds, col.sql+" IS NOT NULL")
	}

	label := quoteStr(strings.Join(set.cols, "_"))

	if set.alias != nil {
		label = quoteStr(set.alias.str)
		if nameTyp.Kind == KindInt {
			label = set.alias.str
		}
	}

	sel = append(sel, label+" AS "+quoteIdent(spec.nameCol))

	sql := "SELECT " + strings.Join(sel, ", ") + " FROM " + base
	if !spec.withNulls {
		sql += " WHERE " + strings.Join(conds, " OR ")
	}

	return sql
}
