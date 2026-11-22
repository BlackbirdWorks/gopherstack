package bqsql

import (
	"strconv"
	"strings"
)

const jsonEachValue = `"value"`

func isDec(t Type) bool { return t.Kind == KindNumeric || t.Kind == KindBigNumeric }

func decFlag(t Type) string {
	if t.Kind == KindBigNumeric {
		return "1"
	}

	return "0"
}

func (g *gx) floatLiteral() bool { return g.lit != nil && g.lit.kind == nFloat }

func decCall(name string, args ...string) string {
	return "bq_dec_" + name + "(" + strings.Join(args, ", ") + ")"
}

// toType adapts g to type t the way implicit coercion does: INT64 and literal FLOAT64 become decimals when t is
// NUMERIC or BIGNUMERIC, decimals become FLOAT64 when t is FLOAT64.
func toType(g *gx, t Type) *gx {
	switch {
	case isDec(t) && g.typ.Kind == KindInt:
		return intToDec(g, t)
	case isDec(t) && g.typ.Kind == KindFloat:
		return floatToDec(g, t)
	case isDec(t) && isDec(g.typ):
		return &gx{sql: g.sql, typ: t, lit: g.lit}
	case t.Kind == KindFloat && isDec(g.typ):
		return &gx{sql: decCall("to_float", g.sql), typ: t}
	case t.Kind == KindArray && g.typ.Kind == KindArray && t.Elem != nil && g.typ.Elem != nil &&
		(isDec(*t.Elem) || isDec(*g.typ.Elem)):
		return coerceDecArray(g, t)
	}

	return g
}

func intToDec(g *gx, t Type) *gx {
	if g.lit != nil && g.lit.kind == nInt {
		if d, err := ParseDecimal(t.Kind, g.lit.str); err == nil {
			return &gx{sql: quoteStr(d.SQLText()), typ: t, lit: g.lit}
		}
	}

	return &gx{sql: decCall("from_int", g.sql, decFlag(t)), typ: t}
}

func floatToDec(g *gx, t Type) *gx {
	if g.floatLiteral() {
		if d, err := ParseDecimal(t.Kind, g.lit.str); err == nil {
			return &gx{sql: quoteStr(d.SQLText()), typ: t}
		}
	}

	return &gx{sql: decCall("from_float", g.sql, decFlag(t)), typ: t}
}

// coerceDecArray converts the elements of array g to the element type of t when either side is a decimal.
func coerceDecArray(g *gx, t Type) *gx {
	from := *g.typ.Elem
	if isDec(from) && isDec(*t.Elem) {
		return &gx{sql: g.sql, typ: t}
	}

	el := toType(&gx{sql: jsonEachValue, typ: from}, *t.Elem)
	if el.sql == jsonEachValue {
		return g
	}

	return &gx{
		sql: "(CASE WHEN " + g.sql + " IS NULL THEN NULL ELSE (SELECT json_group_array(" + el.sql +
			") FROM json_each(" + g.sql + ")) END)",
		typ: t,
	}
}

func hasDecDeep(gs []*gx) bool {
	for _, g := range gs {
		if containsDec(g.typ) {
			return true
		}
	}

	return false
}

func hasDecArray(gs []*gx) bool {
	for _, g := range gs {
		if g.typ.Kind == KindArray && g.typ.Elem != nil && isDec(*g.typ.Elem) {
			return true
		}
	}

	return false
}

func hasDec(gs []*gx) bool {
	for _, g := range gs {
		if isDec(g.typ) {
			return true
		}
	}

	return false
}

// unifyAll computes the common type of gs and coerces every member to it. Without a decimal member it is the
// identity on the SQL.
func unifyAll(gs []*gx) (Type, []*gx) {
	var t Type
	for _, g := range gs {
		t = commonType(t, g.typ)
	}

	if !hasDec(gs) && !hasDecArray(gs) {
		return t, gs
	}

	var strict Type

	for _, g := range gs {
		if !g.floatLiteral() {
			strict = commonType(strict, g.typ)
		}
	}

	if isDec(strict) {
		t = strict
	}

	out := make([]*gx, len(gs))
	for i, g := range gs {
		out[i] = toType(g, t)
	}

	return t, out
}

func unifyPair(l, r *gx) (*gx, *gx, Type) {
	t, out := unifyAll([]*gx{l, r})

	return out[0], out[1], t
}

func decOperand(t Type) bool {
	return t.Kind == KindUnknown || t.Kind == KindInt || t.Kind == KindFloat || isDec(t)
}

// decMix rejects a decimal combined with a type it does not coerce to.
func decMix(gs ...*gx) error {
	if !hasDec(gs) {
		return nil
	}

	for _, g := range gs {
		if !decOperand(g.typ) {
			return invalid("Incompatible types %s and %s", firstDecType(gs).Kind.SQLName(), g.typ.Kind.SQLName())
		}
	}

	return nil
}

func firstDecType(gs []*gx) Type {
	for _, g := range gs {
		if isDec(g.typ) {
			return g.typ
		}
	}

	return Type{}
}

func noSignature(what string, l, r *gx) error {
	return invalid(
		"No matching signature for %s for argument types: %s, %s", what, l.typ.Kind.SQLName(), r.typ.Kind.SQLName(),
	)
}

// decOperator compiles an operator with at least one decimal operand; ok is false for operators that are not numeric.
func decOperator(op string, l, r *gx) (*gx, bool, error) {
	switch op {
	case "=", "!=", "<", ">", "<=", ">=", "+", "-", "*", "/":
	default:
		return nil, false, nil
	}

	if !decOperand(l.typ) || !decOperand(r.typ) {
		return nil, false, noSignature("operator "+op, l, r)
	}

	l2, r2, t := unifyPair(l, r)

	if op != "+" && op != "-" && op != "*" && op != "/" {
		return &gx{sql: "(" + l2.sql + " " + op + " " + r2.sql + ")", typ: scalar(KindBool)}, true, nil
	}

	if !isDec(t) {
		return nil, false, nil
	}

	fn := map[string]string{"+": decAdd, "-": decSub, "*": decMul, "/": decDiv}[op]

	return &gx{sql: decCall(fn, l2.sql, r2.sql, decFlag(t)), typ: t}, true, nil
}

// constrainDec enforces NUMERIC(P, S) on g when the target type is parameterised.
func constrainDec(g *gx, t Type) *gx {
	if !isDec(t) || t.Prec == 0 || !isDec(g.typ) {
		return g
	}

	return &gx{
		sql: decCall("constrain", g.sql, strconv.Itoa(t.Prec), strconv.Itoa(t.Scale), decFlag(t)),
		typ: t,
	}
}

func decFunc(safe bool, name string, args ...string) string {
	if safe {
		return "bq_safe_" + decCall(name, args...)[3:]
	}

	return decCall(name, args...)
}

// decCast compiles CAST/SAFE_CAST when the source or the target is NUMERIC or BIGNUMERIC.
func decCast(a *gx, to Type, safe bool) (*gx, error) {
	if a.typ.Kind == KindUnknown {
		return &gx{sql: a.sql, typ: to}, nil
	}

	switch {
	case isDec(to):
		sql, ok := decCastTo(a, to, safe)
		if !ok {
			return nil, invalid("Invalid cast from %s to %s", a.typ.Kind.SQLName(), to.Kind.SQLName())
		}

		g := &gx{sql: sql, typ: to}
		if to.Prec > 0 {
			g.sql = decFunc(safe, "constrain", sql, strconv.Itoa(to.Prec), strconv.Itoa(to.Scale), decFlag(to))
		}

		return g, nil
	case to.Kind == KindInt:
		return &gx{sql: decFunc(safe, "to_int", a.sql), typ: to}, nil
	case to.Kind == KindFloat:
		return &gx{sql: decCall("to_float", a.sql), typ: to}, nil
	case to.Kind == KindString:
		return &gx{sql: decCall("to_str", a.sql), typ: to}, nil
	}

	return nil, invalid("Invalid cast from %s to %s", a.typ.Kind.SQLName(), to.Kind.SQLName())
}

func decCastTo(a *gx, to Type, safe bool) (string, bool) {
	switch a.typ.Kind { //nolint:exhaustive // other sources are not castable to decimals
	case KindInt:
		return decFunc(safe, "from_int", a.sql, decFlag(to)), true
	case KindFloat:
		return decFunc(safe, "from_float", a.sql, decFlag(to)), true
	case KindString:
		return decFunc(safe, "from_str", a.sql, decFlag(to)), true
	case KindNumeric, KindBigNumeric:
		if a.typ.Kind == to.Kind {
			return a.sql, true
		}

		return decFunc(safe, "convert", a.sql, decFlag(to)), true
	}

	return "", false
}

func floatArgs(args []*gx) []*gx {
	out := make([]*gx, len(args))
	for i, g := range args {
		out[i] = toType(g, scalar(KindFloat))
	}

	return out
}

func roundMode(g *gx) (string, error) {
	if g.lit == nil || g.lit.kind != nString {
		return "", invalid("ROUND rounding mode must be a string literal")
	}

	switch strings.ToUpper(g.lit.str) {
	case "ROUND_HALF_AWAY_FROM_ZERO":
		return "0", nil
	case "ROUND_HALF_EVEN":
		return "1", nil
	}

	return "", invalid("Invalid rounding mode %q", g.lit.str)
}

const (
	decAdd     = "add"
	decSub     = "sub"
	decMul     = "mul"
	decDiv     = "div"
	decNegName = "neg"
	udfSneg    = "sneg"
)

// decFunction compiles a scalar function whose arguments include a decimal. handled is false when the generic
// path should run with the decimals decoded to FLOAT64.
func decFunction(udf string, safe bool, args []*gx) (*gx, bool, error) {
	switch udf {
	case "abs", "sign", "ceil", "floor", "sqrt", "exp", "ln", "log10", udfSneg:
		return decUnaryFn(udf, safe, args)
	case "round", "trunc":
		return decRoundFn(udf, safe, args)
	case "log":
		if len(args) == 1 && isDec(args[0].typ) {
			return &gx{sql: decFunc(safe, "ln", args[0].sql, decFlag(args[0].typ)), typ: args[0].typ}, true, nil
		}

		return decPair("log", safe, args)
	case "pow", "mod", "intdiv":
		return decPair(udf, safe, args)
	case "sdiv", "sadd", "ssub", "smul":
		name := map[string]string{"sdiv": decDiv, "sadd": decAdd, "ssub": decSub, "smul": decMul}[udf]

		return decPair(name, true, args)
	case "greatest", "least":
		return decExtreme(udf, args)
	case "generate_array":
		return decGenerateArraySQL(args)
	}

	return nil, false, nil
}

func decUnaryFn(udf string, safe bool, args []*gx) (*gx, bool, error) {
	t := args[0].typ
	if len(args) != 1 || !isDec(t) {
		return nil, false, nil
	}

	name := udf
	if udf == udfSneg {
		name, safe = decNegName, true
	}

	return &gx{sql: decFunc(safe, name, args[0].sql, decFlag(t)), typ: t}, true, nil
}

func decExtreme(udf string, args []*gx) (*gx, bool, error) {
	if err := decMix(args...); err != nil {
		return nil, false, err
	}

	ct, us := unifyAll(args)
	if !isDec(ct) {
		return nil, false, nil
	}

	if len(us) == 1 {
		return us[0], true, nil
	}

	fn := map[string]string{"greatest": "max", "least": "min"}[udf]
	parts := make([]string, len(us))

	for i, u := range us {
		parts[i] = u.sql
	}

	return &gx{sql: fn + "(" + strings.Join(parts, ", ") + ")", typ: ct}, true, nil
}

func decPair(name string, safe bool, args []*gx) (*gx, bool, error) {
	if len(args) != 2 { //nolint:mnd // binary functions
		return nil, false, nil
	}

	if err := decMix(args...); err != nil {
		return nil, false, err
	}

	l, r, t := unifyPair(args[0], args[1])
	if !isDec(t) {
		return nil, false, nil
	}

	return &gx{sql: decFunc(safe, name, l.sql, r.sql, decFlag(t)), typ: t}, true, nil
}

func decRoundFn(udf string, safe bool, args []*gx) (*gx, bool, error) {
	t := args[0].typ
	if !isDec(t) {
		return nil, false, nil
	}

	digits := "0"

	if len(args) > 1 {
		d := toType(args[1], scalar(KindInt))
		digits = d.sql
	}

	if udf == "trunc" {
		return &gx{sql: decFunc(safe, "trunc", args[0].sql, digits, decFlag(t)), typ: t}, true, nil
	}

	mode := "0"

	if len(args) > 2 { //nolint:mnd // optional rounding mode
		m, err := roundMode(args[2])
		if err != nil {
			return nil, false, err
		}

		mode = m
	}

	return &gx{sql: decFunc(safe, "round", args[0].sql, digits, mode, decFlag(t)), typ: t}, true, nil
}

func decGenerateArraySQL(args []*gx) (*gx, bool, error) {
	if !hasDec(args) {
		return nil, false, nil
	}

	step := &gx{sql: "1", typ: scalar(KindInt)}
	if len(args) > 2 { //nolint:mnd // optional step
		step = args[2]
	}

	t, us := unifyAll([]*gx{args[0], args[1], step})
	if !isDec(t) {
		return nil, false, nil
	}

	return &gx{
		sql: decCall("generate_array", us[0].sql, us[1].sql, toType(step, t).sql, decFlag(t)),
		typ: arrayOf(t),
	}, true, nil
}

// decCallArgs routes a scalar call with decimal arguments: it returns the finished expression, or the arguments
// the generic function should receive (decimals decoded to FLOAT64, or FORMAT's tagged strings).
func decCallArgs(udf string, safe bool, args []*gx) ([]*gx, *gx, error) {
	if !hasDecDeep(args) {
		if udf == "round" && len(args) > arityTwo {
			return nil, nil, invalid("ROUND rounding mode requires a NUMERIC or BIGNUMERIC argument")
		}

		return args, nil, nil
	}

	switch udf {
	case "format":
		return nil, formatDecimalCall(args), nil
	case "array_to_string":
		return nil, nil, invalid("No matching signature for function ARRAY_TO_STRING for argument types: %s",
			args[0].typ.Kind.SQLName())
	case "array_concat":
		_, us := unifyAll(args)

		return us, nil, nil
	}

	if !hasDec(args) {
		return args, nil, nil
	}

	g, ok, err := decFunction(udf, safe, args)
	if err != nil || ok {
		return nil, g, err
	}

	return floatArgs(args), nil, nil
}

// formatDecimalCall compiles FORMAT with decimal arguments: a mask tells the runtime which arguments are encoded.
func formatDecimalCall(args []*gx) *gx {
	sqls := make([]string, len(args))
	mask := make([]byte, 0, len(args))

	for i, g := range args {
		sqls[i] = g.sql

		if i == 0 {
			continue
		}

		switch g.typ.Kind {
		case KindNumeric:
			mask = append(mask, 'N')
		case KindBigNumeric:
			mask = append(mask, 'B')
		default:
			mask = append(mask, '-')

			if containsDec(g.typ) {
				sqls[i] = decimalJSON(g, false)
			}
		}
	}

	return &gx{
		sql: "bq_format_dec(" + quoteStr(string(mask)) + ", " + strings.Join(sqls, ", ") + ")",
		typ: scalar(KindString),
	}
}

// decimalJSON renders a value that is or contains decimals as JSON text, decimals as numbers or, when quote, strings.
func decimalJSON(g *gx, quote bool) string {
	switch {
	case isDec(g.typ):
		if quote {
			return "json_quote(" + decCall("to_str", g.sql) + ")"
		}

		return "json(" + decCall("to_str", g.sql) + ")"
	case g.typ.Kind == KindArray && g.typ.Elem != nil && containsDec(g.typ):
		el := decimalJSONMember(&gx{sql: jsonEachValue, typ: *g.typ.Elem}, jsonEachValue, quote)

		return "(CASE WHEN " + g.sql + " IS NULL THEN NULL ELSE (SELECT json_group_array(" + el +
			") FROM json_each(" + g.sql + ")) END)"
	case g.typ.Kind == KindStruct && containsDec(g.typ):
		parts := make([]string, 0, argc2*len(g.typ.Fields))

		for _, f := range g.typ.Fields {
			path := `'$."` + strings.ReplaceAll(strings.ReplaceAll(f.Name, `\`, `\\`), `"`, `\"`) + `"'`
			m := decimalJSONMember(
				&gx{sql: "json_extract(" + g.sql + ", " + path + ")", typ: f.Type},
				g.sql+" -> "+path,
				quote,
			)
			parts = append(parts, quoteStr(f.Name), m)
		}

		return "(CASE WHEN " + g.sql + " IS NULL THEN NULL ELSE json_object(" + strings.Join(parts, ", ") + ") END)"
	}

	return g.sql
}

func decimalJSONMember(g *gx, raw string, quote bool) string {
	if !containsDec(g.typ) {
		return "(" + raw + ")"
	}

	return decimalJSON(g, quote)
}

func containsDec(t Type) bool {
	switch {
	case isDec(t):
		return true
	case t.Kind == KindArray && t.Elem != nil:
		return containsDec(*t.Elem)
	case t.Kind == KindStruct:
		for _, f := range t.Fields {
			if containsDec(f.Type) {
				return true
			}
		}
	}

	return false
}

func addDecimalFuncs(t map[string]*fnDef) {
	t["PARSE_NUMERIC"] = custom(1, 1, genParseDecimal(KindNumeric))
	t["PARSE_BIGNUMERIC"] = custom(1, 1, genParseDecimal(KindBigNumeric))
	t["SUM"] = custom(1, 1, genSumAvg("sum", sumType))
	t["AVG"] = custom(1, 1, genSumAvg("avg", avgType))

	for name, udf := range map[string]string{
		"STDDEV": "stddev_samp", "STDDEV_SAMP": "stddev_samp", "STDDEV_POP": "stddev_pop", "VARIANCE": "var_samp",
		"VAR_SAMP": "var_samp", "VAR_POP": "var_pop",
	} {
		t[name] = custom(1, 1, genMoment(udf))
	}
}

func genParseDecimal(k Kind) func(c *compiler, n *node, sc *scope) (*gx, error) {
	return func(c *compiler, n *node, sc *scope) (*gx, error) {
		a, err := c.args(n, sc)
		if err != nil {
			return nil, err
		}

		return &gx{sql: decFunc(n.safe, "parse", a[0].sql, decFlag(scalar(k))), typ: scalar(k)}, nil
	}
}

func genSumAvg(name string, ret func([]*gx) Type) func(c *compiler, n *node, sc *scope) (*gx, error) {
	plain := aggGen(name, ret, false)

	return func(c *compiler, n *node, sc *scope) (*gx, error) {
		a, err := c.args(n, sc)
		if err != nil {
			return nil, err
		}

		if !isDec(a[0].typ) {
			return plain(c, n, sc)
		}

		suffix := "_n"
		if a[0].typ.Kind == KindBigNumeric {
			suffix = "_b"
		}

		sql, err := c.aggSQL(n, sc, "bq_dec_"+name+suffix, []string{a[0].sql}, "")
		if err != nil {
			return nil, err
		}

		return &gx{sql: sql, typ: ret(a)}, nil
	}
}

// unifySetColumns coerces the columns of one set-operation side to the unified column types.
func unifySetColumns(cq *compiledQuery, target []Column) string {
	parts := make([]string, len(target))
	changed := false

	for i, col := range target {
		src := &gx{sql: quoteIdent(cq.aliases[i]), typ: cq.cols[i].Type}
		g := toType(src, col.Type)

		if g.sql != src.sql {
			changed = true
		}

		parts[i] = g.sql + " AS " + quoteIdent(cq.aliases[i])
	}

	if !changed {
		return cq.sql
	}

	return "SELECT " + strings.Join(parts, ", ") + " FROM (" + cq.sql + ")"
}

func genMoment(udf string) func(c *compiler, n *node, sc *scope) (*gx, error) {
	plain := aggGen("bq_"+udf, fixed(KindFloat), false)

	return func(c *compiler, n *node, sc *scope) (*gx, error) {
		a, err := c.args(n, sc)
		if err != nil {
			return nil, err
		}

		if !isDec(a[0].typ) {
			return plain(c, n, sc)
		}

		sql, err := c.aggSQL(n, sc, "bq_dec_"+udf, []string{a[0].sql}, "")
		if err != nil {
			return nil, err
		}

		return &gx{sql: sql, typ: scalar(KindFloat)}, nil
	}
}

// rangeOffsetFrame reports a RANGE frame with a numeric offset such as RANGE BETWEEN 1 PRECEDING AND CURRENT ROW.
func rangeOffsetFrame(frame string) bool {
	up := strings.ToUpper(strings.TrimSpace(frame))
	if !strings.HasPrefix(up, "RANGE") {
		return false
	}

	for _, dir := range []string{" PRECEDING", " FOLLOWING"} {
		for rest := up; ; {
			i := strings.Index(rest, dir)
			if i < 0 {
				break
			}

			if i > 0 && rest[i-1] >= '0' && rest[i-1] <= '9' {
				return true
			}

			rest = rest[i+len(dir):]
		}
	}

	return false
}
