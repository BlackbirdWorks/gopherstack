package bqsql

import (
	"encoding/hex"
	"math"
	"strconv"
	"strings"
)

func commonType(a, b Type) Type {
	switch {
	case a.Kind == KindUnknown:
		return b
	case b.Kind == KindUnknown || a.Kind == b.Kind && a.Kind != KindArray && a.Kind != KindStruct:
		return a
	case a.IsNumeric() && b.IsNumeric():
		return numericCommon(a, b)
	}

	return a
}

func numericCommon(a, b Type) Type {
	switch {
	case a.Kind == KindFloat || b.Kind == KindFloat:
		return scalar(KindFloat)
	case a.Kind == KindBigNumeric || b.Kind == KindBigNumeric:
		return scalar(KindBigNumeric)
	case a.Kind == KindNumeric || b.Kind == KindNumeric:
		return scalar(KindNumeric)
	}

	return scalar(KindInt)
}

// fieldSQL extracts struct member f from the JSON text produced by base.
func fieldSQL(base string, f Column) string {
	path := `'$."` + strings.ReplaceAll(strings.ReplaceAll(f.Name, `\`, `\\`), `"`, `\"`) + `"'`
	s := "json_extract(" + base + ", " + path + ")"

	if f.Type.Kind == KindBytes {
		return "bq_b64dec(" + s + ")"
	}

	return s
}

// jsonVal wraps g so it embeds into json_object/json_array with its proper JSON type.
func jsonVal(g *gx) string {
	switch g.typ.Kind {
	case KindBool:
		return "(CASE WHEN " + g.sql + " IS NULL THEN NULL WHEN " + g.sql + " THEN json('true') ELSE json('false') END)"
	case KindStruct, KindArray:
		return "json(" + g.sql + ")"
	case KindBytes:
		return "bq_b64enc(" + g.sql + ")"
	case KindNumeric, KindBigNumeric:
		return "CAST(" + g.sql + " AS TEXT)"
	case KindUnknown, KindInt, KindFloat, KindString, KindTimestamp, KindDate, KindTime, KindDatetime,
		KindJSON, KindGeography:
		return g.sql
	}

	return g.sql
}

func (c *compiler) expr(n *node, sc *scope) (*gx, error) {
	if err := c.guard(); err != nil {
		return nil, err
	}

	switch n.kind {
	case nNull:
		return &gx{sql: litNull}, nil
	case nInt, nFloat, nString, nBytes, nBool, nTyped:
		return c.literal(n)
	case nParam:
		return c.param(n)
	case nPath:
		return c.path(n, sc)
	case nCall:
		return c.call(n, sc)
	case nStar:
		return nil, invalid("Argument * is only allowed in COUNT(*) and the select list")
	case nInterval:
		return nil, invalid("INTERVAL is only supported as an argument of date and time arithmetic")
	case nUnary, nBinary, nCase, nCast, nExtract, nBetween, nIn, nLike, nIs:
		return c.operatorExpr(n, sc)
	case nExists, nSubquery, nArrayQuery, nArray, nStruct, nIndex, nField:
		return c.structuredExpr(n, sc)
	}

	return nil, invalid("unsupported expression")
}

func (c *compiler) operatorExpr(n *node, sc *scope) (*gx, error) {
	switch n.kind { //nolint:exhaustive // other kinds are routed by expr
	case nUnary:
		return c.unary(n, sc)
	case nBinary:
		return c.binary(n, sc)
	case nCase:
		return c.caseExpr(n, sc)
	case nCast:
		return c.cast(n, sc)
	case nExtract:
		return c.extract(n, sc)
	}

	return c.predicate(n, sc)
}

func (c *compiler) structuredExpr(n *node, sc *scope) (*gx, error) {
	switch n.kind { //nolint:exhaustive // other kinds are routed by expr
	case nArray:
		return c.arrayLit(n, sc)
	case nStruct:
		return c.structLit(n, sc)
	case nIndex:
		return c.index(n, sc)
	case nField:
		return c.fieldAccess(n, sc)
	}

	return c.subquery(n, sc)
}

const maxCompileSteps = 1_000_000

func (c *compiler) guard() error {
	c.seq++

	if c.seq > maxCompileSteps {
		return invalid("Query is too complex")
	}

	return nil
}

func (c *compiler) literal(n *node) (*gx, error) {
	switch n.kind {
	case nInt:
		if _, err := strconv.ParseInt(n.str, decimalBase, bitSize); err != nil {
			return nil, invalid("Integer literal %s is out of range for INT64", n.str)
		}

		return &gx{sql: n.str, typ: scalar(KindInt), lit: n}, nil
	case nFloat:
		f, err := strconv.ParseFloat(n.str, bitSize)
		if err != nil || math.IsInf(f, 0) || math.IsNaN(f) {
			return nil, invalid("Floating point literal %s is out of range", n.str)
		}

		txt := strconv.FormatFloat(f, 'g', -1, bitSize)
		if !strings.ContainsAny(txt, ".eE") {
			txt += ".0"
		}

		return &gx{sql: txt, typ: scalar(KindFloat), lit: n}, nil
	case nString:
		return &gx{sql: quoteStr(n.str), typ: scalar(KindString), lit: n}, nil
	case nBytes:
		return &gx{sql: "X'" + hex.EncodeToString([]byte(n.str)) + "'", typ: scalar(KindBytes), lit: n}, nil
	case nBool:
		sql := "0"
		if n.name == litTrueX {
			sql = "1"
		}

		return &gx{sql: sql, typ: scalar(KindBool), lit: n}, nil
	case nNull, nParam, nPath, nStar, nUnary, nBinary, nCall, nCase, nCast, nExtract, nBetween, nIn, nLike, nIs,
		nExists, nSubquery, nArrayQuery, nArray, nStruct, nIndex, nField, nInterval, nTyped:
	}

	return c.typedLiteral(n)
}

func (c *compiler) typedLiteral(n *node) (*gx, error) {
	k, _ := KindOf(n.name)

	if isTemporal(scalar(k)) {
		t, err := ParseTemporal(k, n.str)
		if err != nil {
			return nil, invalid("Invalid %s literal: %q", n.name, n.str)
		}

		return &gx{sql: quoteStr(FormatTemporal(k, t)), typ: scalar(k)}, nil
	}

	if k == KindNumeric || k == KindBigNumeric {
		s, err := storedNumeric(n.str)
		if err != nil {
			return nil, invalid("Invalid %s literal: %q", n.name, n.str)
		}

		return &gx{sql: s, typ: scalar(k)}, nil
	}

	return &gx{sql: quoteStr(n.str), typ: scalar(KindJSON)}, nil
}

func (c *compiler) param(n *node) (*gx, error) {
	var p *Param

	pos := 0

	for i := range c.in.Params {
		q := &c.in.Params[i]

		if q.Name == "" {
			pos++
		}

		if n.name != "" && strings.EqualFold(q.Name, n.name) || n.name == "" && q.Name == "" && pos == n.index {
			p = q

			break
		}
	}

	if p == nil {
		if n.name != "" {
			return nil, invalid("Query parameter '%s' not found at [1:%d]", n.name, n.pos+1)
		}

		return nil, invalid("Query parameter at position %d was not supplied", n.index)
	}

	typ := p.Field.SQLType()
	sql := c.bind(p)

	if typ.Kind == KindStruct || typ.Kind == KindArray {
		sql = "json(" + sql + ")"
	}

	return &gx{sql: sql, typ: typ}, nil
}

func (c *compiler) path(n *node, sc *scope) (*gx, error) {
	if len(n.parts) == 1 {
		switch strings.ToUpper(n.parts[0]) {
		case "CURRENT_DATE", "CURRENT_TIMESTAMP", "CURRENT_DATETIME", "CURRENT_TIME":
			return c.call(&node{kind: nCall, name: strings.ToUpper(n.parts[0]), pos: n.pos}, sc)
		}
	}

	for s := sc; s != nil; s = s.parent {
		g, found, err := c.resolveIn(s, n.parts)
		if err != nil {
			return nil, err
		}

		if found {
			return g, nil
		}
	}

	return nil, invalid("Unrecognized name: %s at [1:%d]", n.parts[0], n.pos+1)
}

func (c *compiler) resolveIn(s *scope, parts []string) (*gx, bool, error) {
	if s.preferAlias && len(parts) == 1 {
		if g, ok := s.aliasItems[strings.ToLower(parts[0])]; ok {
			return g, true, nil
		}
	}

	if len(parts) >= minQualifiedParts {
		if g, found, err := resolveQualified(s, parts); found || err != nil {
			return g, found, err
		}
	}

	hit, err := uniqueColumn(s, parts[0])
	if err != nil {
		return nil, false, err
	}

	if hit != nil {
		g, ferr := applyFields(&gx{sql: hit.sql, typ: hit.typ}, parts[1:])

		return g, true, ferr
	}

	if g, ok := s.aliasItems[strings.ToLower(parts[0])]; ok && len(parts) == 1 && !s.preferAlias {
		return g, true, nil
	}

	return nil, false, nil
}

const minQualifiedParts = 2

// resolveQualified resolves alias.column[.field...] and alias.field for value-table relations.
func resolveQualified(s *scope, parts []string) (*gx, bool, error) {
	for _, rel := range s.rels {
		if !strings.EqualFold(rel.alias, parts[0]) {
			continue
		}

		if col := findCol([]*relation{rel}, parts[1]); col != nil {
			g, err := applyFields(&gx{sql: col.sql, typ: col.typ}, parts[2:])

			return g, true, err
		}

		if rel.value && len(rel.cols) > 0 {
			col := rel.cols[0]
			g, err := applyFields(&gx{sql: col.sql, typ: col.typ}, parts[1:])

			return g, true, err
		}
	}

	return nil, false, nil
}

// uniqueColumn finds the single visible column called name across the scope's relations.
func uniqueColumn(s *scope, name string) (*colInfo, error) {
	var hit *colInfo

	for _, rel := range s.rels {
		for _, col := range rel.cols {
			if col.hidden || !strings.EqualFold(col.name, name) {
				continue
			}

			if hit != nil && hit.sql != col.sql {
				return nil, invalid("Column name %s is ambiguous", name)
			}

			hit = col
		}
	}

	return hit, nil
}

func applyFields(g *gx, fields []string) (*gx, error) {
	for _, name := range fields {
		var err error

		if g, err = fieldOf(g, name); err != nil {
			return nil, err
		}
	}

	return g, nil
}

func fieldOf(g *gx, name string) (*gx, error) {
	if g.typ.Kind != KindStruct {
		return nil, invalid("Cannot access field %s on a value with type %s", name, g.typ.Kind.SQLName())
	}

	for _, f := range g.typ.Fields {
		if strings.EqualFold(f.Name, name) {
			return &gx{sql: fieldSQL(g.sql, f), typ: f.Type}, nil
		}
	}

	return nil, invalid("Field name %s does not exist in STRUCT", name)
}

func (c *compiler) fieldAccess(n *node, sc *scope) (*gx, error) {
	base, err := c.expr(n.args[0], sc)
	if err != nil {
		return nil, err
	}

	return fieldOf(base, n.name)
}

func (c *compiler) unary(n *node, sc *scope) (*gx, error) {
	a, err := c.expr(n.args[0], sc)
	if err != nil {
		return nil, err
	}

	switch n.name {
	case "-":
		return &gx{sql: "(-" + a.sql + ")", typ: a.typ}, nil
	case "~":
		return &gx{sql: "(~" + a.sql + ")", typ: a.typ}, nil
	case litNot:
		return &gx{sql: "(NOT " + a.sql + ")", typ: scalar(KindBool)}, nil
	}

	return a, nil
}

func isTemporal(t Type) bool {
	return t.Kind == KindTimestamp || t.Kind == KindDate || t.Kind == KindDatetime || t.Kind == KindTime
}

// coerceLiteral normalises a string literal compared with a temporal operand into that operand's stored form.
func coerceLiteral(lit *gx, to Type) *gx {
	if lit.lit == nil || lit.lit.kind != nString || !isTemporal(to) {
		return lit
	}

	t, err := ParseTemporal(to.Kind, lit.lit.str)
	if err != nil {
		return lit
	}

	return &gx{sql: quoteStr(FormatTemporal(to.Kind, t)), typ: to}
}

func (c *compiler) binary(n *node, sc *scope) (*gx, error) {
	op := n.name

	if (op == "+" || op == "-") && n.args[1].kind == nInterval {
		return c.intervalArith(n, sc)
	}

	l, err := c.expr(n.args[0], sc)
	if err != nil {
		return nil, err
	}

	r, err := c.expr(n.args[1], sc)
	if err != nil {
		return nil, err
	}

	switch op {
	case litAnd, "OR":
		return &gx{sql: "(" + l.sql + " " + op + " " + r.sql + ")", typ: scalar(KindBool)}, nil
	case "=", "!=", "<", ">", "<=", ">=":
		l, r = coerceLiteral(l, r.typ), coerceLiteral(r, l.typ)

		return &gx{sql: "(" + l.sql + " " + op + " " + r.sql + ")", typ: scalar(KindBool)}, nil
	}

	return c.arithmetic(op, l, r)
}

func (c *compiler) arithmetic(op string, l, r *gx) (*gx, error) {
	switch op {
	case "+", "-", "*":
		return &gx{sql: "(" + l.sql + " " + op + " " + r.sql + ")", typ: arithType(l.typ, r.typ)}, nil
	case "/":
		t := arithType(l.typ, r.typ)
		if t.Kind == KindInt {
			t = scalar(KindFloat)
		}

		return &gx{sql: "bq_div(" + l.sql + ", " + r.sql + ")", typ: t}, nil
	case "||":
		if l.typ.Kind == KindArray {
			return &gx{sql: "bq_array_concat(" + l.sql + ", " + r.sql + ")", typ: l.typ}, nil
		}

		t := scalar(KindString)
		if l.typ.Kind == KindBytes {
			t = scalar(KindBytes)
		}

		return &gx{sql: "(" + l.sql + " || " + r.sql + ")", typ: t}, nil
	case "&", "|", "<<", ">>":
		return &gx{sql: "(" + l.sql + " " + op + " " + r.sql + ")", typ: scalar(KindInt)}, nil
	case "^":
		return &gx{
			sql: "((" + l.sql + " | " + r.sql + ") - (" + l.sql + " & " + r.sql + "))",
			typ: scalar(KindInt),
		}, nil
	}

	return nil, invalid("Unsupported operator %s", op)
}

func arithType(a, b Type) Type {
	if a.IsNumeric() || b.IsNumeric() {
		return numericCommon(coalesceNumeric(a), coalesceNumeric(b))
	}

	return scalar(KindInt)
}

func coalesceNumeric(t Type) Type {
	if t.IsNumeric() {
		return t
	}

	return scalar(KindInt)
}

func (c *compiler) intervalArith(n *node, sc *scope) (*gx, error) {
	l, err := c.expr(n.args[0], sc)
	if err != nil {
		return nil, err
	}

	iv := n.args[1]

	v, err := c.expr(iv.args[0], sc)
	if err != nil {
		return nil, err
	}

	amount := v.sql
	if n.name == "-" {
		amount = "(-" + amount + ")"
	}

	return &gx{sql: "bq_date_add(" + l.sql + ", " + amount + ", " + quoteStr(iv.name) + ")", typ: l.typ}, nil
}

func (c *compiler) caseExpr(n *node, sc *scope) (*gx, error) {
	args := n.args

	var sb strings.Builder

	sb.WriteString("(CASE")

	if n.operand {
		op, err := c.expr(args[0], sc)
		if err != nil {
			return nil, err
		}

		sb.WriteString(" " + op.sql)

		args = args[1:]
	}

	var typ Type

	nWhen := len(args)
	if n.hasElse {
		nWhen--
	}

	for i := 0; i+1 < nWhen+1 && i < nWhen; i += 2 {
		w, err := c.expr(args[i], sc)
		if err != nil {
			return nil, err
		}

		t, err := c.expr(args[i+1], sc)
		if err != nil {
			return nil, err
		}

		typ = commonType(typ, t.typ)
		sb.WriteString(" WHEN " + w.sql + " THEN " + t.sql)
	}

	if n.hasElse {
		e, err := c.expr(args[len(args)-1], sc)
		if err != nil {
			return nil, err
		}

		typ = commonType(typ, e.typ)
		sb.WriteString(" ELSE " + e.sql)
	}

	sb.WriteString(" END)")

	return &gx{sql: sb.String(), typ: typ}, nil
}

func (c *compiler) cast(n *node, sc *scope) (*gx, error) {
	a, err := c.expr(n.args[0], sc)
	if err != nil {
		return nil, err
	}

	to := *n.castTo
	fn := "bq_cast"

	if n.safe {
		fn = "bq_safe_cast"
	}

	if to.Kind == KindArray || to.Kind == KindStruct {
		if a.typ.Kind != to.Kind && a.typ.Kind != KindUnknown {
			return nil, invalid("Invalid cast from %s to %s", a.typ.Kind.SQLName(), to.Kind.SQLName())
		}

		return &gx{sql: a.sql, typ: to}, nil
	}

	return &gx{
		sql: fn + "(" + a.sql + ", " + quoteStr(to.Kind.SQLName()) + ", " + quoteStr(a.typ.Kind.SQLName()) + ")",
		typ: to,
	}, nil
}

func (c *compiler) extract(n *node, sc *scope) (*gx, error) {
	a, err := c.expr(n.args[0], sc)
	if err != nil {
		return nil, err
	}

	typ := scalar(KindInt)

	switch n.name {
	case litDate:
		typ = scalar(KindDate)
	case litTime:
		typ = scalar(KindTime)
	case litDatetime:
		typ = scalar(KindDatetime)
	}

	return &gx{sql: "bq_extract(" + a.sql + ", " + quoteStr(n.name) + ")", typ: typ}, nil
}

func (c *compiler) predicate(n *node, sc *scope) (*gx, error) {
	args := make([]*gx, len(n.args))

	for i, a := range n.args {
		g, err := c.expr(a, sc)
		if err != nil {
			return nil, err
		}

		args[i] = g
	}

	boolT := scalar(KindBool)
	not := ""

	if n.not {
		not = "NOT "
	}

	switch n.kind {
	case nLike:
		return &gx{sql: "(" + not + "bq_like(" + args[0].sql + ", " + args[1].sql + "))", typ: boolT}, nil
	case nBetween:
		lo, hi := coerceLiteral(args[1], args[0].typ), coerceLiteral(args[2], args[0].typ)

		return &gx{sql: "(" + args[0].sql + " " + not + "BETWEEN " + lo.sql + " AND " + hi.sql + ")", typ: boolT}, nil
	case nIs:
		return isPredicate(n, args), nil
	case nIn:
		return c.inPredicate(n, args, sc)
	case nNull, nInt, nFloat, nString, nBytes, nBool, nTyped, nParam, nPath, nStar, nUnary, nBinary, nCall, nCase,
		nCast, nExtract, nExists, nSubquery, nArrayQuery, nArray, nStruct, nIndex, nField, nInterval:
	}

	return nil, invalid("unsupported predicate")
}

func isPredicate(n *node, args []*gx) *gx {
	not := ""
	if n.not {
		not = "NOT "
	}

	boolT := scalar(KindBool)

	if n.name == litDistinct {
		op := "IS NOT"
		if n.not {
			op = "IS"
		}

		return &gx{sql: "(" + args[0].sql + " " + op + " " + args[1].sql + ")", typ: boolT}
	}

	return &gx{sql: "(" + args[0].sql + " IS " + not + n.name + ")", typ: boolT}
}

func (c *compiler) inPredicate(n *node, args []*gx, sc *scope) (*gx, error) {
	op := "IN"
	if n.not {
		op = "NOT IN"
	}

	boolT := scalar(KindBool)

	switch {
	case n.name == litUnnest:
		return &gx{
			sql: "(" + args[0].sql + " " + op + " (SELECT \"value\" FROM json_each(" + args[1].sql + ")))",
			typ: boolT,
		}, nil
	case n.query != nil:
		cq, err := c.compileQuery(n.query, sc)
		if err != nil {
			return nil, err
		}

		if len(cq.cols) != 1 {
			return nil, invalid("Subquery of type IN must have only one output column")
		}

		return &gx{sql: "(" + args[0].sql + " " + op + " (" + cq.sql + "))", typ: boolT}, nil
	}

	list := make([]string, len(args)-1)
	for i, a := range args[1:] {
		list[i] = coerceLiteral(a, args[0].typ).sql
	}

	return &gx{sql: "(" + args[0].sql + " " + op + " (" + strings.Join(list, ", ") + "))", typ: boolT}, nil
}

func (c *compiler) subquery(n *node, sc *scope) (*gx, error) {
	cq, err := c.compileQuery(n.query, sc)
	if err != nil {
		return nil, err
	}

	switch n.kind {
	case nExists:
		return &gx{sql: "EXISTS (" + cq.sql + ")", typ: scalar(KindBool)}, nil
	case nArrayQuery:
		if len(cq.cols) != 1 {
			return nil, invalid("ARRAY subquery must return exactly one column")
		}

		g := &gx{sql: quoteIdent("v"), typ: cq.cols[0].Type}
		sql := "(SELECT json_group_array(" + jsonVal(
			g,
		) + ") FROM (SELECT " + quoteIdent(
			cq.aliases[0],
		) + " AS \"v\" FROM (" + cq.sql + ")))"

		return &gx{sql: sql, typ: arrayOf(cq.cols[0].Type)}, nil
	case nNull, nInt, nFloat, nString, nBytes, nBool, nTyped, nParam, nPath, nStar, nUnary, nBinary, nCall, nCase,
		nCast, nExtract, nBetween, nIn, nLike, nIs, nSubquery, nArray, nStruct, nIndex, nField, nInterval:
	}

	if len(cq.cols) != 1 {
		return nil, invalid("Scalar subquery must return exactly one column")
	}

	return &gx{sql: "(" + cq.sql + ")", typ: cq.cols[0].Type}, nil
}

func (c *compiler) arrayLit(n *node, sc *scope) (*gx, error) {
	var elem Type

	parts := make([]string, len(n.args))

	for i, a := range n.args {
		g, err := c.expr(a, sc)
		if err != nil {
			return nil, err
		}

		elem = commonType(elem, g.typ)
		parts[i] = jsonVal(g)
	}

	if n.elemType != nil {
		elem = *n.elemType
	}

	if elem.Kind == KindUnknown {
		elem = scalar(KindInt)
	}

	return &gx{sql: "json_array(" + strings.Join(parts, ", ") + ")", typ: arrayOf(elem)}, nil
}

func (c *compiler) structLit(n *node, sc *scope) (*gx, error) {
	cols := make([]Column, len(n.args))
	parts := make([]string, 0, len(n.args)*2) //nolint:mnd // name and value per member

	for i, a := range n.args {
		g, err := c.expr(a, sc)
		if err != nil {
			return nil, err
		}

		name := n.names[i]
		if name == "" && n.castTo != nil && i < len(n.castTo.Fields) {
			name = n.castTo.Fields[i].Name
		}

		if name == "" && a.kind == nPath {
			name = a.parts[len(a.parts)-1]
		}

		if name == "" {
			name = "_field_" + strconv.Itoa(i+1)
		}

		typ := g.typ
		if n.castTo != nil && i < len(n.castTo.Fields) {
			typ = n.castTo.Fields[i].Type
		}

		cols[i] = Column{Name: name, Type: typ}
		parts = append(parts, quoteStr(name), jsonVal(g))
	}

	return &gx{sql: "json_object(" + strings.Join(parts, ", ") + ")", typ: Type{Kind: KindStruct, Fields: cols}}, nil
}

func (c *compiler) index(n *node, sc *scope) (*gx, error) {
	base, err := c.expr(n.args[0], sc)
	if err != nil {
		return nil, err
	}

	if base.typ.Kind != KindArray || base.typ.Elem == nil {
		return nil, invalid("Subscript access is only supported on arrays")
	}

	idx, err := c.expr(n.args[1], sc)
	if err != nil {
		return nil, err
	}

	pos := idx.sql
	if n.name == "ORDINAL" || n.name == "SAFE_ORDINAL" {
		pos = "(" + pos + " - 1)"
	}

	sql := "json_extract(" + base.sql + ", '$[' || " + pos + " || ']')"

	if base.typ.Elem.Kind == KindBytes {
		sql = "bq_b64dec(" + sql + ")"
	}

	return &gx{sql: sql, typ: *base.typ.Elem}, nil
}

// coerceFor adapts g to the column type it is stored into: temporal string literals are normalised and struct
// members are matched by position, as BigQuery does for literals with anonymous or differently named members.
func coerceFor(g *gx, target Type) *gx {
	return coerceTo(coerceLiteral(g, target), target)
}

func coerceTo(g *gx, target Type) *gx {
	switch {
	case g.typ.Kind == KindStruct && target.Kind == KindStruct && len(g.typ.Fields) == len(target.Fields):
		parts := make([]string, 0, len(target.Fields)*2) //nolint:mnd // name and value per member

		for i, tf := range target.Fields {
			src := g.typ.Fields[i]
			sub := coerceTo(&gx{sql: fieldSQL(g.sql, src), typ: src.Type}, tf.Type)
			parts = append(parts, quoteStr(tf.Name), jsonVal(sub))
		}

		return &gx{sql: "json_object(" + strings.Join(parts, ", ") + ")", typ: target}
	case g.typ.Kind == KindArray && target.Kind == KindArray && g.typ.Elem != nil && target.Elem != nil &&
		target.Elem.Kind == KindStruct:
		elem := coerceTo(&gx{sql: `"_ce"."value"`, typ: *g.typ.Elem}, *target.Elem)

		return &gx{
			sql: "(SELECT json_group_array(" + jsonVal(elem) + ") FROM json_each(" + g.sql + ") AS \"_ce\")",
			typ: target,
		}
	}

	return g
}
