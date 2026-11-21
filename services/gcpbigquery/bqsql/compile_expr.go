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
	case a.Kind == KindArray && b.Kind == KindArray && a.Elem != nil && b.Elem != nil &&
		(isDec(*a.Elem) || isDec(*b.Elem)):
		return arrayOf(commonType(*a.Elem, *b.Elem))
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
		KindJSON, KindGeography, KindInterval:
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
	case nNullOf, nRecurIter, nTableArg, nRawSQL:
		return c.internalExpr(n, sc)
	case nParam:
		return c.param(n)
	case nPath:
		return c.path(n, sc)
	case nCall:
		return c.call(n, sc)
	case nStar:
		return nil, invalid("Argument * is only allowed in COUNT(*) and the select list")
	case nInterval:
		return c.intervalValue(n, sc)
	case nUnary, nBinary, nCase, nCast, nExtract, nBetween, nIn, nLike, nIs:
		return c.operatorExpr(n, sc)
	case nExists, nSubquery, nArrayQuery, nArray, nStruct, nIndex, nField:
		return c.structuredExpr(n, sc)
	}

	return nil, invalid("unsupported expression")
}

// internalExpr compiles the synthetic nodes the compiler builds itself.
func (c *compiler) internalExpr(n *node, sc *scope) (*gx, error) {
	switch n.kind { //nolint:exhaustive // only the synthetic kinds are routed here
	case nNullOf:
		g, err := c.expr(n.args[0], sc)
		if err != nil {
			return nil, err
		}

		return &gx{sql: litNull, typ: g.typ}, nil
	case nRecurIter:
		return recurIter(sc)
	case nRawSQL:
		return &gx{sql: n.str, typ: *n.castTo}, nil
	}

	return nil, invalid("TABLE arguments are only allowed for table function parameters")
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

const (
	maxCompileSteps = 1_000_000
	castZoneArg     = 2
)

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
		nExists, nSubquery, nArrayQuery, nArray, nStruct, nIndex, nField, nInterval, nTyped, nNullOf, nRecurIter,
		nRawSQL, nTableArg:
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
		d, err := ParseDecimal(k, n.str)
		if err != nil {
			return nil, invalid("Invalid %s literal: %q", n.name, n.str)
		}

		return &gx{sql: quoteStr(d.SQLText()), typ: scalar(k)}, nil
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

	if g, ok, err := c.resolveVar(n.parts); ok || err != nil {
		return g, err
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
		if a.typ.Kind == KindInterval {
			return &gx{sql: "bq_interval_neg(" + a.sql + ")", typ: a.typ}, nil
		}

		if isDec(a.typ) {
			return &gx{sql: decCall("neg", a.sql, decFlag(a.typ)), typ: a.typ}, nil
		}

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

	l, err := c.expr(n.args[0], sc)
	if err != nil {
		return nil, err
	}

	r, err := c.expr(n.args[1], sc)
	if err != nil {
		return nil, err
	}

	if g, ok, ierr := intervalBinary(op, l, r); ok || ierr != nil {
		return g, ierr
	}

	if isDec(l.typ) || isDec(r.typ) {
		if g, ok, derr := decOperator(op, l, r); ok || derr != nil {
			return g, derr
		}

		if op == "+" || op == "-" || op == "*" || op == "/" {
			l, r, _ = unifyPair(l, r)
		}
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
		t := arithType(l.typ, r.typ)
		if t.Kind == KindInt {
			fn := map[string]string{"+": "bq_cadd", "-": "bq_csub", "*": "bq_cmul"}[op]

			return &gx{sql: fn + "(" + l.sql + ", " + r.sql + ")", typ: t}, nil
		}

		return &gx{sql: "(" + l.sql + " " + op + " " + r.sql + ")", typ: t}, nil
	case "/":
		t := arithType(l.typ, r.typ)
		if t.Kind == KindInt {
			t = scalar(KindFloat)
		}

		return &gx{sql: "bq_div(" + l.sql + ", " + r.sql + ")", typ: t}, nil
	case "||":
		if isDec(l.typ) || isDec(r.typ) {
			return nil, noSignature("operator ||", l, r)
		}

		if l.typ.Kind == KindArray {
			t, us := unifyAll([]*gx{l, r})

			return &gx{sql: "bq_array_concat(" + us[0].sql + ", " + us[1].sql + ")", typ: t}, nil
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

func (c *compiler) caseExpr(n *node, sc *scope) (*gx, error) {
	args := n.args

	var op *gx

	if n.operand {
		g, err := c.expr(args[0], sc)
		if err != nil {
			return nil, err
		}

		op, args = g, args[1:]
	}

	nWhen := len(args)
	if n.hasElse {
		nWhen--
	}

	whens := make([]*gx, 0, nWhen/2+1)
	results := make([]*gx, 0, nWhen/2+1)

	for i := 0; i+1 < nWhen+1 && i < nWhen; i += 2 {
		w, err := c.expr(args[i], sc)
		if err != nil {
			return nil, err
		}

		t, err := c.expr(args[i+1], sc)
		if err != nil {
			return nil, err
		}

		whens, results = append(whens, w), append(results, t)
	}

	if n.hasElse {
		e, err := c.expr(args[len(args)-1], sc)
		if err != nil {
			return nil, err
		}

		results = append(results, e)
	}

	if err := decMix(results...); err != nil {
		return nil, err
	}

	typ, results := unifyAll(results)

	var sb strings.Builder

	sb.WriteString("(CASE")

	if op != nil {
		ws := append([]*gx{op}, whens...)
		if err := decMix(ws...); err != nil {
			return nil, err
		}

		_, ws = unifyAll(ws)
		op, whens = ws[0], ws[1:]

		sb.WriteString(" " + op.sql)
	}

	for i, w := range whens {
		sb.WriteString(" WHEN " + w.sql + " THEN " + results[i].sql)
	}

	if n.hasElse {
		sb.WriteString(" ELSE " + results[len(results)-1].sql)
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

	if containsDec(a.typ) && !isDec(a.typ) && to.Kind == KindString {
		return nil, invalid("Invalid cast from %s to %s", a.typ.Kind.SQLName(), to.Kind.SQLName())
	}

	if isDec(to) || isDec(a.typ) {
		if len(n.args) > 1 {
			return nil, unsupported("CAST ... FORMAT with NUMERIC or BIGNUMERIC")
		}

		return decCast(a, to, n.safe)
	}

	if len(n.args) > 1 {
		return c.castFormat(n, sc, a, to)
	}

	if n.safe {
		fn = "bq_safe_cast"
	}

	if to.Kind == KindArray || to.Kind == KindStruct {
		if a.typ.Kind != to.Kind && a.typ.Kind != KindUnknown {
			return nil, invalid("Invalid cast from %s to %s", a.typ.Kind.SQLName(), to.Kind.SQLName())
		}

		out := *coerceTo(a, to)
		out.typ = to

		return &out, nil
	}

	return &gx{
		sql: fn + "(" + a.sql + ", " + quoteStr(to.Kind.SQLName()) + ", " + quoteStr(a.typ.Kind.SQLName()) + ")",
		typ: to,
	}, nil
}

func (c *compiler) castFormat(n *node, sc *scope, a *gx, to Type) (*gx, error) {
	f, err := c.expr(n.args[1], sc)
	if err != nil {
		return nil, err
	}

	tz := "NULL"

	if len(n.args) > castZoneArg {
		z, zerr := c.expr(n.args[2], sc)
		if zerr != nil {
			return nil, zerr
		}

		tz = z.sql
	}

	fn := "bq_cast_format"
	if n.safe {
		fn = "bq_safe_cast_format"
	}

	return &gx{
		sql: fn + "(" + a.sql + ", " + quoteStr(to.Kind.SQLName()) + ", " + quoteStr(a.typ.Kind.SQLName()) + ", " +
			f.sql + ", " + tz + ")",
		typ: to,
	}, nil
}

func (c *compiler) extract(n *node, sc *scope) (*gx, error) {
	a, err := c.expr(n.args[0], sc)
	if err != nil {
		return nil, err
	}

	typ := scalar(KindInt)

	if a.typ.Kind == KindInterval {
		return &gx{sql: "bq_interval_extract(" + a.sql + ", " + quoteStr(n.name) + ")", typ: typ}, nil
	}

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

	if n.kind == nBetween && hasDec(args) {
		if g := firstNonNumeric(args); g != nil {
			return nil, noSignature("BETWEEN", args[0], g)
		}

		_, args = unifyAll(args)
	}

	switch n.kind {
	case nLike:
		if hasDec(args) {
			return nil, noSignature("operator LIKE", args[0], args[1])
		}

		return &gx{sql: "(" + not + "bq_like(" + args[0].sql + ", " + args[1].sql + "))", typ: boolT}, nil
	case nBetween:
		lo, hi := coerceLiteral(args[1], args[0].typ), coerceLiteral(args[2], args[0].typ)

		return &gx{sql: "(" + args[0].sql + " " + not + "BETWEEN " + lo.sql + " AND " + hi.sql + ")", typ: boolT}, nil
	case nIs:
		return isPredicate(n, args)
	case nIn:
		return c.inPredicate(n, args, sc)
	case nNull, nInt, nFloat, nString, nBytes, nBool, nTyped, nParam, nPath, nStar, nUnary, nBinary, nCall, nCase,
		nCast, nExtract, nExists, nSubquery, nArrayQuery, nArray, nStruct, nIndex, nField, nInterval, nNullOf, nRecurIter,
		nRawSQL, nTableArg:
	}

	return nil, invalid("unsupported predicate")
}

func firstNonNumeric(gs []*gx) *gx {
	for _, g := range gs {
		if !decOperand(g.typ) {
			return g
		}
	}

	return nil
}

func isPredicate(n *node, args []*gx) (*gx, error) {
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

		if hasDec(args) {
			if g := firstNonNumeric(args); g != nil {
				return nil, noSignature("IS DISTINCT FROM", args[0], g)
			}

			_, args = unifyAll(args)
		}

		return &gx{sql: "(" + args[0].sql + " " + op + " " + args[1].sql + ")", typ: boolT}, nil
	}

	return &gx{sql: "(" + args[0].sql + " IS " + not + n.name + ")", typ: boolT}, nil
}

func (c *compiler) inPredicate(n *node, args []*gx, sc *scope) (*gx, error) {
	op := "IN"
	if n.not {
		op = "NOT IN"
	}

	boolT := scalar(KindBool)

	switch {
	case n.name == litUnnest:
		val := &gx{sql: jsonEachValue}
		if el := args[1].typ.Elem; el != nil {
			val.typ = *el
		}

		if isDec(val.typ) || isDec(args[0].typ) {
			t, us := unifyAll([]*gx{args[0], val})
			args[0], val = us[0], toType(val, t)
		}

		return &gx{
			sql: "(" + args[0].sql + " " + op + " (SELECT " + val.sql + " FROM json_each(" + args[1].sql + ")))",
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

		if isDec(cq.cols[0].Type) || isDec(args[0].typ) {
			col := &gx{sql: quoteIdent(cq.aliases[0]), typ: cq.cols[0].Type}
			t, us := unifyAll([]*gx{args[0], col})
			args[0], col = us[0], toType(col, t)

			return &gx{
				sql: "(" + args[0].sql + " " + op + " (SELECT " + col.sql + " FROM (" + cq.sql + ")))",
				typ: boolT,
			}, nil
		}

		return &gx{sql: "(" + args[0].sql + " " + op + " (" + cq.sql + "))", typ: boolT}, nil
	}

	if hasDec(args) {
		if g := firstNonNumeric(args); g != nil {
			return nil, noSignature("IN", args[0], g)
		}

		_, args = unifyAll(args)
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
		nCast, nExtract, nBetween, nIn, nLike, nIs, nSubquery, nArray, nStruct, nIndex, nField, nInterval, nNullOf,
		nRecurIter, nRawSQL, nTableArg:
	}

	if len(cq.cols) != 1 {
		return nil, invalid("Scalar subquery must return exactly one column")
	}

	return &gx{sql: "(" + cq.sql + ")", typ: cq.cols[0].Type}, nil
}

func (c *compiler) arrayLit(n *node, sc *scope) (*gx, error) {
	items := make([]*gx, len(n.args))

	for i, a := range n.args {
		g, err := c.expr(a, sc)
		if err != nil {
			return nil, err
		}

		items[i] = g
	}

	if err := decMix(items...); err != nil {
		return nil, err
	}

	elem, items := unifyAll(items)
	if n.elemType != nil {
		elem = *n.elemType

		for i, g := range items {
			items[i] = toType(g, elem)
		}
	}

	parts := make([]string, len(items))
	for i, g := range items {
		parts[i] = jsonVal(g)
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
			g = toType(g, typ)
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

	chk := "bq_array_chk"
	if strings.HasPrefix(n.name, "SAFE_") {
		chk = "bq_safe_array_chk"
	}

	sql := "json_extract(" + base.sql + ", '$[' || " + chk + "(" + base.sql + ", " + pos + ") || ']')"

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
	case isDec(target) || target.Kind == KindFloat && isDec(g.typ):
		return constrainDec(toType(g, target), target)
	case isDec(g.typ) && (target.Kind == KindString || target.Kind == KindInt):
		if out, err := decCast(g, target, false); err == nil {
			return out
		}
	case arrayNeedsCoerce(g.typ, target):
		elem := coerceTo(&gx{sql: `"_ce"."value"`, typ: *g.typ.Elem}, *target.Elem)

		return &gx{
			sql: "(SELECT json_group_array(" + jsonVal(elem) + ") FROM json_each(" + g.sql + ") AS \"_ce\")",
			typ: target,
		}
	case g.typ.Kind == KindStruct && target.Kind == KindStruct && len(g.typ.Fields) == len(target.Fields):
		parts := make([]string, 0, len(target.Fields)*2) //nolint:mnd // name and value per member

		for i, tf := range target.Fields {
			src := g.typ.Fields[i]
			sub := coerceTo(&gx{sql: fieldSQL(g.sql, src), typ: src.Type}, tf.Type)
			parts = append(parts, quoteStr(tf.Name), jsonVal(sub))
		}

		return &gx{sql: "json_object(" + strings.Join(parts, ", ") + ")", typ: target}
	}

	return g
}

// arrayNeedsCoerce reports an array whose elements must be rewritten to reach the target element type.
func arrayNeedsCoerce(from, to Type) bool {
	if from.Kind != KindArray || to.Kind != KindArray || from.Elem == nil || to.Elem == nil {
		return false
	}

	return to.Elem.Kind == KindStruct || isDec(*to.Elem) && (from.Elem.Kind != to.Elem.Kind || to.Elem.Prec > 0) ||
		isDec(*from.Elem) && !isDec(*to.Elem)
}
