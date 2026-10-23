package bqsql

import (
	"strings"
)

const (
	arityTwo   = 2
	arityThree = 3
	arityFour  = 4
	aritySix   = 6
)

type fnDef struct {
	ret     func(args []*gx) Type
	gen     func(c *compiler, n *node, sc *scope) (*gx, error)
	udf     string
	min     int
	max     int
	partArg int
}

func fixed(k Kind) func([]*gx) Type { return func([]*gx) Type { return scalar(k) } }

func fixedType(t Type) func([]*gx) Type { return func([]*gx) Type { return t } }

func firstArgType(a []*gx) Type {
	if len(a) > 0 {
		return a[0].typ
	}

	return Type{}
}

func commonOf(a []*gx) Type {
	var t Type
	for _, g := range a {
		t = commonType(t, g.typ)
	}

	return t
}

func floatish(a []*gx) Type {
	if len(a) > 0 && (a[0].typ.Kind == KindNumeric || a[0].typ.Kind == KindBigNumeric) {
		return a[0].typ
	}

	return scalar(KindFloat)
}

func elemOf(i int) func([]*gx) Type {
	return func(a []*gx) Type {
		if i < len(a) && a[i].typ.Elem != nil {
			return *a[i].typ.Elem
		}

		return Type{}
	}
}

func def(udf string, minArgs, maxArgs int, ret func([]*gx) Type) *fnDef {
	return &fnDef{udf: udf, min: minArgs, max: maxArgs, ret: ret, partArg: -1}
}

func custom(minArgs, maxArgs int, gen func(c *compiler, n *node, sc *scope) (*gx, error)) *fnDef {
	return &fnDef{gen: gen, min: minArgs, max: maxArgs, partArg: -1}
}

func withPart(d *fnDef, idx int) *fnDef {
	d.partArg = idx

	return d
}

func functionTable() map[string]*fnDef {
	t := map[string]*fnDef{}

	addMathFuncs(t)
	addStringFuncs(t)
	addTimeFuncs(t)
	addArrayFuncs(t)
	addAggFuncs(t)
	addMiscFuncs(t)

	return t
}

func addMathFuncs(t map[string]*fnDef) {
	floatFuncs := []string{
		"SQRT", "EXP", "LN", "LOG10", "SIN", "COS", "TAN", "ASIN", "ACOS", "ATAN", "SINH", "COSH", "TANH", "CBRT",
	}

	for _, n := range floatFuncs {
		t[n] = def(strings.ToLower(n), 1, 1, fixed(KindFloat))
	}

	t["ABS"] = def("abs", 1, 1, firstArgType)
	t["SIGN"] = def("sign", 1, 1, firstArgType)
	t["CEIL"], t["CEILING"] = def("ceil", 1, 1, floatish), def("ceil", 1, 1, floatish)
	t["FLOOR"] = def("floor", 1, 1, floatish)
	t["ROUND"] = def("round", 1, arityTwo, floatish)
	t["TRUNC"] = def("trunc", 1, arityTwo, floatish)
	t["POW"], t["POWER"] = def(
		"pow",
		arityTwo,
		arityTwo,
		fixed(KindFloat),
	), def(
		"pow",
		arityTwo,
		arityTwo,
		fixed(KindFloat),
	)
	t["LOG"] = def("log", 1, arityTwo, fixed(KindFloat))
	t["ATAN2"] = def("atan2", arityTwo, arityTwo, fixed(KindFloat))
	t["MOD"] = def("mod", arityTwo, arityTwo, commonOf)
	t["DIV"] = def("intdiv", arityTwo, arityTwo, fixed(KindInt))
	t["SAFE_DIVIDE"] = def("sdiv", arityTwo, arityTwo, fixed(KindFloat))
	t["SAFE_ADD"] = def("sadd", arityTwo, arityTwo, commonOf)
	t["SAFE_SUBTRACT"] = def("ssub", arityTwo, arityTwo, commonOf)
	t["SAFE_MULTIPLY"] = def("smul", arityTwo, arityTwo, commonOf)
	t["SAFE_NEGATE"] = def("sneg", 1, 1, firstArgType)
	t["IEEE_DIVIDE"] = def("ieee_div", arityTwo, arityTwo, fixed(KindFloat))
	t["GREATEST"] = def("greatest", 1, -1, commonOf)
	t["LEAST"] = def("least", 1, -1, commonOf)
	t["IS_NAN"] = def("is_nan", 1, 1, fixed(KindBool))
	t["IS_INF"] = def("is_inf", 1, 1, fixed(KindBool))
	t["RAND"] = def("rand", 0, 0, fixed(KindFloat))
}

func addStringFuncs(t map[string]*fnDef) {
	str := fixed(KindString)
	for name, spec := range map[string][3]int{
		"LOWER": {1, 1, int(KindString)}, "UPPER": {1, 1, int(KindString)}, "INITCAP": {1, 2, int(KindString)},
		"REVERSE": {1, 1, int(KindString)}, "REPEAT": {2, 2, int(KindString)}, "REPLACE": {3, 3, int(KindString)},
		"TRIM": {1, 2, int(KindString)}, "LTRIM": {1, 2, int(KindString)}, "RTRIM": {1, 2, int(KindString)},
		"LPAD": {2, 3, int(KindString)}, "RPAD": {2, 3, int(KindString)}, litLeft: {2, 2, int(KindString)},
		litRight: {2, 2, int(KindString)}, "CHR": {1, 1, int(KindString)}, "TO_BASE64": {1, 1, int(KindString)},
		"TO_HEX": {1, 1, int(KindString)}, "REGEXP_REPLACE": {3, 3, int(KindString)},
	} {
		t[name] = def(strings.ToLower(name), spec[0], spec[1], str)
	}

	t["SUBSTR"], t["SUBSTRING"] = def(
		"substr",
		arityTwo,
		arityThree,
		firstArgType,
	), def(
		"substr",
		arityTwo,
		arityThree,
		firstArgType,
	)
	t["CONCAT"] = def("concat", 1, -1, firstArgType)
	t["LENGTH"], t["CHAR_LENGTH"], t["CHARACTER_LENGTH"] = def("length", 1, 1, fixed(KindInt)),
		def("char_length", 1, 1, fixed(KindInt)), def("char_length", 1, 1, fixed(KindInt))
	t["BYTE_LENGTH"] = def("byte_length", 1, 1, fixed(KindInt))
	t["ASCII"] = def("ascii", 1, 1, fixed(KindInt))
	t["STRPOS"], t["INSTR"] = def(
		"strpos",
		arityTwo,
		arityTwo,
		fixed(KindInt),
	), def(
		"instr",
		arityTwo,
		arityFour,
		fixed(KindInt),
	)
	t["STARTS_WITH"] = def("starts_with", arityTwo, arityTwo, fixed(KindBool))
	t["ENDS_WITH"] = def("ends_with", arityTwo, arityTwo, fixed(KindBool))
	t["CONTAINS_SUBSTR"] = def("contains_substr", arityTwo, arityTwo, fixed(KindBool))
	t["REGEXP_CONTAINS"] = def("regexp_contains", arityTwo, arityTwo, fixed(KindBool))
	t["REGEXP_EXTRACT"] = def("regexp_extract", arityTwo, arityFour, firstArgType)
	t["REGEXP_EXTRACT_ALL"] = def("regexp_extract_all", arityTwo, arityTwo, fixedType(arrayOf(scalar(KindString))))
	t["SPLIT"] = def("split", 1, arityTwo, fixedType(arrayOf(scalar(KindString))))
	t["FROM_BASE64"] = def("from_base64", 1, 1, fixed(KindBytes))
	t["FROM_HEX"] = def("from_hex", 1, 1, fixed(KindBytes))
	t["MD5"] = def("md5", 1, 1, fixed(KindBytes))
	t["SHA1"] = def("sha1", 1, 1, fixed(KindBytes))
	t["SHA256"] = def("sha256", 1, 1, fixed(KindBytes))
	t["SHA512"] = def("sha512", 1, 1, fixed(KindBytes))
	t["FORMAT"] = def("format", 1, -1, fixed(KindString))
	t[litString] = def("string", 1, arityTwo, fixed(KindString))
	t["TO_JSON_STRING"] = custom(1, arityTwo, genToJSONString)
}

func addTimeFuncs(t map[string]*fnDef) {
	for _, n := range []string{"CURRENT_DATE", "CURRENT_TIMESTAMP", "CURRENT_DATETIME", "CURRENT_TIME"} {
		k, _ := KindOf(strings.TrimPrefix(n, "CURRENT_"))
		t[n] = def(strings.ToLower(n), 0, 1, fixed(k))
	}

	t[litDate] = def("date", 1, arityThree, fixed(KindDate))
	t[litTimestamp] = def("timestamp", 1, arityTwo, fixed(KindTimestamp))
	t[litDatetime] = def("datetime", 1, aritySix, fixed(KindDatetime))
	t[litTime] = def("time", 1, arityThree, fixed(KindTime))
	t["DATE_FROM_UNIX_DATE"] = def("date_from_unix_date", 1, 1, fixed(KindDate))
	t["UNIX_DATE"] = def("unix_date", 1, 1, fixed(KindInt))
	t["TIMESTAMP_SECONDS"] = def("timestamp_seconds", 1, 1, fixed(KindTimestamp))
	t["TIMESTAMP_MILLIS"] = def("timestamp_millis", 1, 1, fixed(KindTimestamp))
	t["TIMESTAMP_MICROS"] = def("timestamp_micros", 1, 1, fixed(KindTimestamp))
	t["UNIX_SECONDS"] = def("unix_seconds", 1, 1, fixed(KindInt))
	t["UNIX_MILLIS"] = def("unix_millis", 1, 1, fixed(KindInt))
	t["UNIX_MICROS"] = def("unix_micros", 1, 1, fixed(KindInt))
	t["LAST_DAY"] = withPart(def("last_day", 1, arityTwo, fixed(KindDate)), 1)

	for _, p := range []struct {
		name string
		kind Kind
	}{{litDate, KindDate}, {litTimestamp, KindTimestamp}, {litDatetime, KindDatetime}, {litTime, KindTime}} {
		t[p.name+"_ADD"] = custom(arityTwo, arityTwo, genDateAdd(false))
		t[p.name+"_SUB"] = custom(arityTwo, arityTwo, genDateAdd(true))
		t[p.name+"_DIFF"] = withPart(def("date_diff", arityThree, arityThree, fixed(KindInt)), arityTwo)
		t[p.name+"_TRUNC"] = withPart(def("date_trunc", arityTwo, arityThree, fixedType(scalar(p.kind))), 1)
		t["FORMAT_"+p.name] = def("format_time", arityTwo, arityThree, fixed(KindString))
		t["PARSE_"+p.name] = def("parse_"+strings.ToLower(p.name), arityTwo, arityThree, fixed(p.kind))
	}

	t["GENERATE_DATE_ARRAY"] = custom(arityTwo, arityThree, genDateArray(KindDate))
	t["GENERATE_TIMESTAMP_ARRAY"] = custom(arityThree, arityThree, genDateArray(KindTimestamp))
}

func addArrayFuncs(t map[string]*fnDef) {
	t["ARRAY_LENGTH"] = custom(1, 1, func(c *compiler, n *node, sc *scope) (*gx, error) {
		a, err := c.args(n, sc)
		if err != nil {
			return nil, err
		}

		return &gx{sql: "json_array_length(" + a[0].sql + ")", typ: scalar(KindInt)}, nil
	})
	t["ARRAY_CONCAT"] = def("array_concat", 1, -1, firstArgType)
	t["ARRAY_REVERSE"] = def("array_reverse", 1, 1, firstArgType)
	t["ARRAY_TO_STRING"] = def("array_to_string", arityTwo, arityThree, fixed(KindString))
	t["ARRAY_FIRST"] = def("array_first", 1, 1, elemOf(0))
	t["ARRAY_LAST"] = def("array_last", 1, 1, elemOf(0))
	t["GENERATE_ARRAY"] = def(
		"generate_array",
		arityTwo,
		arityThree,
		func(a []*gx) Type { return arrayOf(commonOf(a)) },
	)
}

func addMiscFuncs(t map[string]*fnDef) {
	t["IF"] = custom(arityThree, arityThree, genIf)
	t["IFNULL"] = custom(arityTwo, arityTwo, genCoalesce)
	t["COALESCE"] = custom(1, -1, genCoalesce)
	t["NULLIF"] = custom(arityTwo, arityTwo, func(c *compiler, n *node, sc *scope) (*gx, error) {
		a, err := c.args(n, sc)
		if err != nil {
			return nil, err
		}

		return &gx{sql: "NULLIF(" + a[0].sql + ", " + coerceLiteral(a[1], a[0].typ).sql + ")", typ: a[0].typ}, nil
	})
	t["GENERATE_UUID"] = def("generate_uuid", 0, 0, fixed(KindString))
	t["ERROR"] = def("error", 1, 1, fixedType(Type{}))
	t["JSON_EXTRACT_SCALAR"], t["JSON_VALUE"] = def(
		"json_value",
		1,
		arityTwo,
		fixed(KindString),
	), def(
		"json_value",
		1,
		arityTwo,
		fixed(KindString),
	)
	t["JSON_EXTRACT"], t["JSON_QUERY"] = def(
		"json_query",
		1,
		arityTwo,
		fixed(KindJSON),
	), def(
		"json_query",
		1,
		arityTwo,
		fixed(KindJSON),
	)
	t["PARSE_JSON"] = def("parse_json", 1, arityTwo, fixed(KindJSON))
	t["SESSION_USER"] = def("session_user", 0, 0, fixed(KindString))
}

func (c *compiler) args(n *node, sc *scope) ([]*gx, error) {
	out := make([]*gx, len(n.args))

	for i, a := range n.args {
		g, err := c.expr(a, sc)
		if err != nil {
			return nil, err
		}

		out[i] = g
	}

	return out, nil
}

func arityOK(d *fnDef, n int) bool { return n >= d.min && (d.max < 0 || n <= d.max) }

func (c *compiler) call(n *node, sc *scope) (*gx, error) {
	d, ok := c.fns[n.name]
	if !ok {
		return nil, invalid("Function not found: %s at [1:%d]", n.name, n.pos+1)
	}

	if !arityOK(d, len(n.args)) {
		return nil, invalid("Number of arguments does not match for function %s", n.name)
	}

	if d.gen != nil {
		return d.gen(c, n, sc)
	}

	if n.distinct || n.over != nil {
		return nil, invalid("Function %s does not support DISTINCT or OVER", n.name)
	}

	args := make([]*gx, len(n.args))

	for i, a := range n.args {
		g, err := c.callArg(d, i, a, sc)
		if err != nil {
			return nil, err
		}

		args[i] = g
	}

	name := "bq_" + d.udf
	if n.safe {
		name = "bq_safe_" + d.udf
	}

	sqls := make([]string, len(args))
	for i, a := range args {
		sqls[i] = a.sql
	}

	return &gx{sql: name + "(" + strings.Join(sqls, ", ") + ")", typ: d.ret(args)}, nil
}

// callArg compiles one argument, turning date-part identifiers into string literals.
func (c *compiler) callArg(d *fnDef, i int, a *node, sc *scope) (*gx, error) {
	if i == d.partArg {
		if part, ok := datePart(a); ok {
			return &gx{sql: quoteStr(part), typ: scalar(KindString)}, nil
		}
	}

	return c.expr(a, sc)
}

func datePart(a *node) (string, bool) {
	switch {
	case a.kind == nPath && len(a.parts) == 1:
		return strings.ToUpper(a.parts[0]), true
	case a.kind == nCall && a.name == litWeek && len(a.args) == 1 && a.args[0].kind == nPath:
		return "WEEK(" + strings.ToUpper(a.args[0].parts[0]) + ")", true
	}

	return "", false
}

func genIf(c *compiler, n *node, sc *scope) (*gx, error) {
	a, err := c.args(n, sc)
	if err != nil {
		return nil, err
	}

	return &gx{
		sql: "(CASE WHEN " + a[0].sql + " THEN " + a[1].sql + " ELSE " + a[2].sql + " END)",
		typ: commonType(a[1].typ, a[2].typ),
	}, nil
}

func genCoalesce(c *compiler, n *node, sc *scope) (*gx, error) {
	a, err := c.args(n, sc)
	if err != nil {
		return nil, err
	}

	parts := make([]string, len(a))
	for i, g := range a {
		parts[i] = g.sql
	}

	if len(parts) == 1 {
		return a[0], nil
	}

	return &gx{sql: "COALESCE(" + strings.Join(parts, ", ") + ")", typ: commonOf(a)}, nil
}

func genToJSONString(c *compiler, n *node, sc *scope) (*gx, error) {
	a, err := c.args(n, sc)
	if err != nil {
		return nil, err
	}

	x := a[0]

	switch x.typ.Kind {
	case KindStruct, KindArray:
		return &gx{sql: "CAST(json(" + x.sql + ") AS TEXT)", typ: scalar(KindString)}, nil
	case KindBool:
		return &gx{
			sql: "(CASE WHEN " + x.sql + " IS NULL THEN 'null' WHEN " + x.sql + " THEN 'true' ELSE 'false' END)",
			typ: scalar(KindString),
		}, nil
	case KindInt, KindFloat, KindNumeric, KindBigNumeric:
		return &gx{sql: "COALESCE(CAST(" + x.sql + " AS TEXT), 'null')", typ: scalar(KindString)}, nil
	case KindJSON:
		return &gx{sql: x.sql, typ: scalar(KindString)}, nil
	case KindUnknown, KindString, KindBytes, KindTimestamp, KindDate, KindTime, KindDatetime, KindGeography:
	}

	return &gx{sql: "COALESCE(json_quote(" + jsonVal(x) + "), 'null')", typ: scalar(KindString)}, nil
}

// intervalArgs compiles (value, INTERVAL n PART) into the value, amount and part SQL.
func (c *compiler) intervalArgs(n *node, sc *scope) (*gx, string, string, error) {
	v, err := c.expr(n.args[0], sc)
	if err != nil {
		return nil, "", "", err
	}

	iv := n.args[1]
	if iv.kind != nInterval {
		return nil, "", "", invalid("%s requires an INTERVAL argument", n.name)
	}

	amount, err := c.expr(iv.args[0], sc)
	if err != nil {
		return nil, "", "", err
	}

	return v, amount.sql, quoteStr(iv.name), nil
}

func genDateAdd(sub bool) func(c *compiler, n *node, sc *scope) (*gx, error) {
	return func(c *compiler, n *node, sc *scope) (*gx, error) {
		v, amount, part, err := c.intervalArgs(n, sc)
		if err != nil {
			return nil, err
		}

		if sub {
			amount = "(-" + amount + ")"
		}

		kind := strings.TrimSuffix(strings.TrimSuffix(n.name, "_ADD"), "_SUB")
		k, _ := KindOf(kind)

		return &gx{sql: "bq_date_add(" + v.sql + ", " + amount + ", " + part + ")", typ: scalar(k)}, nil
	}
}

func genDateArray(k Kind) func(c *compiler, n *node, sc *scope) (*gx, error) {
	return func(c *compiler, n *node, sc *scope) (*gx, error) {
		start, err := c.expr(n.args[0], sc)
		if err != nil {
			return nil, err
		}

		end, err := c.expr(n.args[1], sc)
		if err != nil {
			return nil, err
		}

		amount, part := "1", quoteStr(litDay)

		if len(n.args) == 3 { //nolint:mnd // optional interval
			iv := n.args[2]
			if iv.kind != nInterval {
				return nil, invalid("%s requires an INTERVAL step", n.name)
			}

			a, stepErr := c.expr(iv.args[0], sc)
			if stepErr != nil {
				return nil, stepErr
			}

			amount, part = a.sql, quoteStr(iv.name)
		}

		sql := "bq_generate_dates(" + start.sql + ", " + end.sql + ", " + amount + ", " + part + ")"

		return &gx{sql: sql, typ: arrayOf(scalar(k))}, nil
	}
}

func addAggFuncs(t map[string]*fnDef) {
	t["COUNT"] = custom(1, 1, aggGen("count", fixed(KindInt), false))
	t["COUNTIF"] = custom(1, 1, genCountIf)
	t["SUM"] = custom(1, 1, aggGen("sum", sumType, false))
	t["AVG"] = custom(1, 1, aggGen("avg", avgType, false))
	t["MIN"] = custom(1, 1, aggGen("min", firstArgType, false))
	t["MAX"] = custom(1, 1, aggGen("max", firstArgType, false))
	t["ANY_VALUE"] = custom(1, 1, aggGen("min", firstArgType, false))
	t["LOGICAL_AND"] = custom(1, 1, aggGen("min", fixed(KindBool), false))
	t["LOGICAL_OR"] = custom(1, 1, aggGen("max", fixed(KindBool), false))
	t["APPROX_COUNT_DISTINCT"] = custom(1, 1, aggGen("count", fixed(KindInt), true))
	t["STRING_AGG"] = custom(1, arityTwo, genStringAgg)
	t["ARRAY_AGG"] = custom(1, 1, genArrayAgg)

	for name, udf := range map[string]string{
		"STDDEV": "stddev_samp", "STDDEV_SAMP": "stddev_samp", "STDDEV_POP": "stddev_pop", "VARIANCE": "var_samp",
		"VAR_SAMP": "var_samp", "VAR_POP": "var_pop",
	} {
		t[name] = custom(1, 1, aggGen("bq_"+udf, fixed(KindFloat), false))
	}

	for name, ret := range map[string]func([]*gx) Type{
		"ROW_NUMBER": fixed(KindInt), "RANK": fixed(KindInt), "DENSE_RANK": fixed(KindInt), "NTILE": fixed(KindInt),
		"PERCENT_RANK": fixed(KindFloat), "CUME_DIST": fixed(KindFloat), "LAG": firstArgType, "LEAD": firstArgType,
		"FIRST_VALUE": firstArgType, "LAST_VALUE": firstArgType, "NTH_VALUE": firstArgType,
	} {
		t[name] = custom(0, arityThree, aggGen(strings.ToLower(name), ret, false))
	}
}

func sumType(a []*gx) Type {
	if len(a) > 0 && a[0].typ.IsNumeric() {
		return a[0].typ
	}

	return scalar(KindInt)
}

func avgType(a []*gx) Type {
	if len(a) > 0 && (a[0].typ.Kind == KindNumeric || a[0].typ.Kind == KindBigNumeric) {
		return a[0].typ
	}

	return scalar(KindFloat)
}

// aggSQL assembles name(args) with DISTINCT, ORDER BY, FILTER and OVER as the call node requests.
func (c *compiler) aggSQL(n *node, sc *scope, fname string, args []string, filter string) (string, error) {
	var sb strings.Builder

	sb.WriteString(fname + "(")

	if n.distinct {
		sb.WriteString("DISTINCT ")
	}

	sb.WriteString(strings.Join(args, ", "))

	if len(n.order) > 0 {
		parts := make([]string, len(n.order))

		for i, o := range n.order {
			g, err := c.expr(o.expr, sc)
			if err != nil {
				return "", err
			}

			parts[i] = g.sql
			if o.desc {
				parts[i] += litDesc
			}
		}

		sb.WriteString(" ORDER BY " + strings.Join(parts, ", "))
	}

	sb.WriteString(")")

	if filter != "" {
		sb.WriteString(" FILTER (WHERE " + filter + ")")
	}

	if n.over != nil {
		over, err := c.overSQL(n.over, sc)
		if err != nil {
			return "", err
		}

		sb.WriteString(" OVER (" + over + ")")
	}

	return sb.String(), nil
}

func (c *compiler) overSQL(w *windowSpec, sc *scope) (string, error) {
	var parts []string

	if len(w.partition) > 0 {
		list := make([]string, len(w.partition))

		for i, p := range w.partition {
			g, err := c.expr(p, sc)
			if err != nil {
				return "", err
			}

			list[i] = g.sql
		}

		parts = append(parts, "PARTITION BY "+strings.Join(list, ", "))
	}

	if len(w.order) > 0 {
		list := make([]string, len(w.order))

		for i, o := range w.order {
			g, err := c.expr(o.expr, sc)
			if err != nil {
				return "", err
			}

			list[i] = g.sql
			if o.desc {
				list[i] += litDesc
			}
		}

		parts = append(parts, "ORDER BY "+strings.Join(list, ", "))
	}

	if w.frame != "" {
		parts = append(parts, w.frame)
	}

	return strings.Join(parts, " "), nil
}

func aggGen(fname string, ret func([]*gx) Type, distinct bool) func(c *compiler, n *node, sc *scope) (*gx, error) {
	return func(c *compiler, n *node, sc *scope) (*gx, error) {
		if distinct {
			n.distinct = true
		}

		var args []string

		var gs []*gx

		for _, a := range n.args {
			if a.kind == nStar {
				args = append(args, "*")

				continue
			}

			g, err := c.expr(a, sc)
			if err != nil {
				return nil, err
			}

			gs = append(gs, g)
			args = append(args, g.sql)
		}

		sql, err := c.aggSQL(n, sc, fname, args, "")
		if err != nil {
			return nil, err
		}

		return &gx{sql: sql, typ: ret(gs)}, nil
	}
}

func genCountIf(c *compiler, n *node, sc *scope) (*gx, error) {
	a, err := c.args(n, sc)
	if err != nil {
		return nil, err
	}

	sql, err := c.aggSQL(n, sc, "count", []string{"CASE WHEN " + a[0].sql + " THEN 1 END"}, "")
	if err != nil {
		return nil, err
	}

	return &gx{sql: sql, typ: scalar(KindInt)}, nil
}

func genStringAgg(c *compiler, n *node, sc *scope) (*gx, error) {
	a, err := c.args(n, sc)
	if err != nil {
		return nil, err
	}

	args := []string{a[0].sql}
	if len(a) == 2 { //nolint:mnd // optional delimiter
		args = append(args, a[1].sql)
	} else if !n.distinct {
		args = append(args, "','")
	}

	sql, err := c.aggSQL(n, sc, "group_concat", args, "")
	if err != nil {
		return nil, err
	}

	return &gx{sql: sql, typ: a[0].typ}, nil
}

func genArrayAgg(c *compiler, n *node, sc *scope) (*gx, error) {
	a, err := c.args(n, sc)
	if err != nil {
		return nil, err
	}

	if n.limit != nil {
		return nil, unsupported("ARRAY_AGG ... LIMIT")
	}

	filter := ""
	if n.nulls {
		filter = a[0].sql + " IS NOT NULL"
	}

	sql, err := c.aggSQL(n, sc, "json_group_array", []string{jsonVal(a[0])}, filter)
	if err != nil {
		return nil, err
	}

	return &gx{sql: sql, typ: arrayOf(defaultType(a[0].typ))}, nil
}
