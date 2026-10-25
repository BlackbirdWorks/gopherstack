package kql

import (
	"math"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode"
)

// Resolver supplies workspace tables to the engine.
type Resolver interface {
	// Table returns the named table of the workspace ("" is the query's own workspace).
	Table(workspace, name string) (*Rel, bool)
}

type letDef struct {
	def node
}

type env struct {
	now   time.Time
	res   Resolver
	lets  map[string]*letDef
	depth int
}

type scalar struct {
	fn  func(row []any) (any, error)
	typ Type
}

type scope struct {
	env  *env
	agg  *aggCollector
	cols []Col
}

func (sc *scope) lookup(name string) (int, Type, bool) {
	for i, c := range sc.cols {
		if c.Name == name {
			return i, c.Type, true
		}
	}

	return 0, "", false
}

func constScalar(v any, t Type) scalar {
	return scalar{typ: t, fn: func([]any) (any, error) { return v, nil }}
}

const maxLetDepth = 32

func (sc *scope) compile(n node) (scalar, error) {
	switch x := n.(type) {
	case *litNode:
		return constScalar(x.v, x.t), nil
	case *identNode:
		return sc.compileIdent(x)
	case *unNode:
		return sc.compileUnary(x)
	case *binNode:
		return sc.compileBinary(x)
	case *inNode:
		return sc.compileIn(x)
	case *betweenNode:
		return sc.compileBetween(x)
	case *memberNode:
		return sc.compileMember(x)
	case *indexNode:
		return sc.compileIndex(x)
	case *callNode:
		return sc.compileCall(x)
	}

	return scalar{}, unsupported("a tabular expression in a scalar context")
}

func (sc *scope) compileIdent(x *identNode) (scalar, error) {
	if i, t, ok := sc.lookup(x.name); ok {
		return scalar{typ: t, fn: func(r []any) (any, error) { return r[i], nil }}, nil
	}

	if def, ok := sc.env.lets[x.name]; ok && !isTabularNode(def.def) {
		if sc.env.depth > maxLetDepth {
			return scalar{}, semErr("let statement '%s' is recursive", x.name)
		}

		sc.env.depth++
		defer func() { sc.env.depth-- }()

		inner := &scope{env: sc.env}

		s, err := inner.compile(def.def)
		if err != nil {
			return scalar{}, err
		}

		return s, nil
	}

	return scalar{}, semErr("Failed to resolve scalar expression named '%s'", x.name)
}

func isTabularNode(n node) bool {
	switch n.(type) {
	case *pipeNode, *unionNode, *datatableNode, *rangeNode, *printNode, *tableRef:
		return true
	}

	return false
}

func (sc *scope) compileUnary(x *unNode) (scalar, error) {
	in, err := sc.compile(x.x)
	if err != nil {
		return scalar{}, err
	}

	if x.op == "+" {
		return in, nil
	}

	return scalar{typ: in.typ, fn: func(r []any) (any, error) {
		v, evalErr := in.fn(r)
		if evalErr != nil || v == nil {
			return nil, evalErr
		}

		switch n := v.(type) {
		case int64:
			return -n, nil
		case float64:
			return -n, nil
		case time.Duration:
			return -n, nil
		}

		return nil, semErr("cannot negate a value of type %s", typeOfValue(v))
	}}, nil
}

func typeOfValue(v any) Type {
	switch v.(type) {
	case bool:
		return TBool
	case int64:
		return TLong
	case float64:
		return TReal
	case string:
		return TString
	case time.Time:
		return TDatetime
	case time.Duration:
		return TTimespan
	}

	return TDynamic
}

func (sc *scope) compilePair(l, r node) (scalar, scalar, error) {
	a, err := sc.compile(l)
	if err != nil {
		return scalar{}, scalar{}, err
	}

	b, err := sc.compile(r)

	return a, b, err
}

func (sc *scope) compileBinary(x *binNode) (scalar, error) {
	a, b, err := sc.compilePair(x.l, x.r)
	if err != nil {
		return scalar{}, err
	}

	switch x.op {
	case opAnd, opOr:
		return logicOp(x.op, a, b)
	case "+", "-", "*", "/", "%":
		return arithOp(x.op, a, b)
	case "==", "!=", "<", "<=", ">", ">=":
		return cmpOp(x.op, a, b)
	case "=~", "!~":
		return ciEqOp(x.op == "!~", a, b), nil
	}

	return stringOp(x.op, a, b)
}

func boolOperand(v any) (bool, bool, error) {
	if v == nil {
		return false, true, nil
	}

	if b, ok := v.(bool); ok {
		return b, false, nil
	}

	return false, false, semErr("a boolean operand was expected but found %s", typeOfValue(v))
}

func logicOp(op string, a, b scalar) (scalar, error) {
	isAnd := op == opAnd

	return scalar{typ: TBool, fn: func(r []any) (any, error) {
		av, err := a.fn(r)
		if err != nil {
			return nil, err
		}

		ab, anull, err := boolOperand(av)
		if err != nil {
			return nil, err
		}

		if !anull && ab != isAnd {
			return ab, nil
		}

		bv, err := b.fn(r)
		if err != nil {
			return nil, err
		}

		bb, bnull, err := boolOperand(bv)
		if err != nil {
			return nil, err
		}

		switch {
		case !bnull && bb != isAnd:
			return bb, nil
		case anull || bnull:
			return nil, nil //nolint:nilnil // SQL-style three-valued logic
		}

		return isAnd, nil
	}}, nil
}

type arithRule struct {
	ops      string
	l, r, to Type
}

var arithRules = []arithRule{ //nolint:gochecknoglobals // static lookup
	{ops: "-", l: TDatetime, r: TDatetime, to: TTimespan},
	{ops: "+-", l: TDatetime, r: TTimespan, to: TDatetime},
	{ops: "+", l: TTimespan, r: TDatetime, to: TDatetime},
	{ops: "+-", l: TTimespan, r: TTimespan, to: TTimespan},
	{ops: "/", l: TTimespan, r: TTimespan, to: TReal},
}

func numericResult(a, b Type) Type {
	switch {
	case a == TReal || b == TReal:
		return TReal
	case a == TInt && b == TInt:
		return TInt
	}

	return TLong
}

func timespanScaling(op string, a, b Type) bool {
	if a == TTimespan && isNumType(b) {
		return op == "*" || op == "/"
	}

	return isNumType(a) && b == TTimespan && op == "*"
}

func arithType(op string, a, b Type) (Type, error) {
	switch {
	case a == TDynamic || b == TDynamic || a == "" || b == "":
		return TDynamic, nil
	case isNumType(a) && isNumType(b):
		return numericResult(a, b), nil
	case timespanScaling(op, a, b):
		return TTimespan, nil
	}

	for _, r := range arithRules {
		if r.l == a && r.r == b && strings.Contains(r.ops, op) {
			return r.to, nil
		}
	}

	return "", semErr("operator '%s' cannot be applied to %s and %s", op, a, b)
}

// evalPair evaluates two operands against a row.
func evalPair(a, b scalar, r []any) (any, any, error) {
	av, err := a.fn(r)
	if err != nil {
		return nil, nil, err
	}

	bv, err := b.fn(r)
	if err != nil {
		return nil, nil, err
	}

	return av, bv, nil
}

func arithOp(op string, a, b scalar) (scalar, error) {
	t, err := arithType(op, a.typ, b.typ)
	if err != nil {
		return scalar{}, err
	}

	return scalar{typ: t, fn: func(r []any) (any, error) {
		av, bv, evalErr := evalPair(a, b, r)
		if evalErr != nil {
			return nil, evalErr
		}

		if av == nil || bv == nil {
			return nullValue()
		}

		return arith(op, av, bv)
	}}, nil
}

func arith(op string, a, b any) (any, error) {
	var (
		out any
		ok  bool
	)

	switch x := a.(type) {
	case int64:
		out, ok = arithInt(op, x, b)
	case float64:
		out, ok = arithFloat(op, x, b)
	case time.Time:
		out, ok = arithTime(op, x, b)
	case time.Duration:
		return durArith(op, x, b)
	}

	if !ok {
		return nil, semErr("operator '%s' cannot be applied to %s and %s", op, typeOfValue(a), typeOfValue(b))
	}

	return out, nil
}

func arithInt(op string, x int64, b any) (any, bool) {
	switch y := b.(type) {
	case int64:
		return intArith(op, x, y), true
	case float64:
		return floatArith(op, float64(x), y), true
	case time.Duration:
		if op == "*" {
			return time.Duration(x) * y, true
		}
	}

	return nil, false
}

func arithFloat(op string, x float64, b any) (any, bool) {
	if y, ok := toFloat(b); ok && isNumeric(b) {
		return floatArith(op, x, y), true
	}

	if y, ok := b.(time.Duration); ok && op == "*" {
		return time.Duration(x * float64(y)), true
	}

	return nil, false
}

func arithTime(op string, x time.Time, b any) (any, bool) {
	switch y := b.(type) {
	case time.Time:
		if op == "-" {
			return x.Sub(y), true
		}
	case time.Duration:
		switch op {
		case "+":
			return x.Add(y), true
		case "-":
			return x.Add(-y), true
		}
	}

	return nil, false
}

func durArith(op string, x time.Duration, b any) (any, error) {
	switch y := b.(type) {
	case time.Duration:
		switch op {
		case "+":
			return x + y, nil
		case "-":
			return x - y, nil
		case "/":
			if y == 0 {
				return nil, nil //nolint:nilnil // division by zero is null
			}

			return float64(x) / float64(y), nil
		}
	case time.Time:
		if op == "+" {
			return y.Add(x), nil
		}
	case int64, float64:
		f, _ := toFloat(y)

		switch op {
		case "*":
			return time.Duration(float64(x) * f), nil
		case "/":
			if f == 0 {
				return nil, nil //nolint:nilnil // division by zero is null
			}

			return time.Duration(float64(x) / f), nil
		}
	}

	return nil, semErr("operator '%s' cannot be applied to timespan and %s", op, typeOfValue(b))
}

func intArith(op string, x, y int64) any {
	switch op {
	case "+":
		return x + y
	case "-":
		return x - y
	case "*":
		return x * y
	case "/":
		if y == 0 {
			return nil
		}

		return x / y
	}

	if y == 0 {
		return nil
	}

	return x % y
}

func floatArith(op string, x, y float64) any {
	switch op {
	case "+":
		return x + y
	case "-":
		return x - y
	case "*":
		return x * y
	case "/":
		return x / y
	}

	return math.Mod(x, y)
}

func typesComparable(a, b Type) bool {
	switch {
	case a == TDynamic || b == TDynamic || a == "" || b == "" || a == b:
		return true
	case isNumType(a) && isNumType(b):
		return true
	case (a == TString || a == TGUID) && (b == TString || b == TGUID):
		return true
	}

	return false
}

func cmpOp(op string, a, b scalar) (scalar, error) {
	if !typesComparable(a.typ, b.typ) {
		return scalar{}, semErr(
			"cannot compare values of types %s and %s; try adding explicit casts", a.typ, b.typ)
	}

	return scalar{typ: TBool, fn: func(r []any) (any, error) {
		av, bv, evalErr := evalPair(a, b, r)
		if evalErr != nil {
			return nil, evalErr
		}

		if av == nil || bv == nil {
			return nullValue()
		}

		c, ok := compareValues(av, bv)
		if !ok {
			switch op {
			case "==":
				return false, nil
			case "!=":
				return true, nil
			}

			return nullValue()
		}

		return cmpResult(op, c), nil
	}}, nil
}

func cmpResult(op string, c int) bool {
	switch op {
	case "==":
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

// binaryScalar builds a scalar that evaluates two operands and yields null when either is null.
func binaryScalar(a, b scalar, typ Type, f func(av, bv any) (any, error)) scalar {
	return scalar{typ: typ, fn: func(r []any) (any, error) {
		av, bv, evalErr := evalPair(a, b, r)
		if evalErr != nil {
			return nil, evalErr
		}

		if av == nil || bv == nil {
			return nullValue()
		}

		return f(av, bv)
	}}
}

func ciEqOp(neg bool, a, b scalar) scalar {
	return binaryScalar(a, b, TBool, func(av, bv any) (any, error) {
		return strings.EqualFold(ToString(av), ToString(bv)) != neg, nil
	})
}

func stringOp(op string, a, b scalar) (scalar, error) {
	base := strings.TrimPrefix(op, "!")
	neg := strings.HasPrefix(op, "!")

	if base != opMatchesRegex {
		return binaryScalar(a, b, TBool, func(av, bv any) (any, error) {
			return applyStringOp(base, ToString(av), ToString(bv)) != neg, nil
		}), nil
	}

	var re *regexp.Regexp

	if lit, ok := constOf(b); ok {
		var err error
		if re, err = compileRegex(ToString(lit)); err != nil {
			return scalar{}, err
		}
	}

	return binaryScalar(a, b, TBool, func(av, bv any) (any, error) {
		rx := re
		if rx == nil {
			var err error
			if rx, err = compileRegex(ToString(bv)); err != nil {
				return nil, err
			}
		}

		return rx.MatchString(ToString(av)) != neg, nil
	}), nil
}

func constOf(s scalar) (any, bool) {
	v, err := s.fn(nil)
	if err != nil || v == nil {
		return nil, false
	}

	return v, true
}

func compileRegex(s string) (*regexp.Regexp, error) {
	re, err := regexp.Compile(s)
	if err != nil {
		return nil, semErr("invalid regular expression %q", s)
	}

	return re, nil
}

func applyStringOp(op, hay, needle string) bool {
	cs := strings.HasSuffix(op, "_cs")
	op = strings.TrimSuffix(op, "_cs")

	if !cs {
		hay, needle = strings.ToLower(hay), strings.ToLower(needle)
	}

	switch op {
	case "contains":
		return strings.Contains(hay, needle)
	case "startswith":
		return strings.HasPrefix(hay, needle)
	case "endswith":
		return strings.HasSuffix(hay, needle)
	case "has":
		return hasTerm(hay, needle, true, true)
	case "hasprefix":
		return hasTerm(hay, needle, true, false)
	case "hassuffix":
		return hasTerm(hay, needle, false, true)
	}

	return false
}

func isTermRune(r rune) bool {
	return r < unicode.MaxASCII && (unicode.IsLetter(r) || unicode.IsDigit(r))
}

// hasTerm reports whether needle occurs in hay on term boundaries.
func hasTerm(hay, needle string, startBound, endBound bool) bool {
	if needle == "" {
		return true
	}

	first := rune(needle[0])
	last := rune(needle[len(needle)-1])
	from := 0

	for {
		i := strings.Index(hay[from:], needle)
		if i < 0 {
			return false
		}

		i += from
		end := i + len(needle)
		okStart := !startBound || !isTermRune(first) || i == 0 || !isTermRune(rune(hay[i-1]))
		okEnd := !endBound || !isTermRune(last) || end == len(hay) || !isTermRune(rune(hay[end]))

		if okStart && okEnd {
			return true
		}

		from = i + 1
	}
}

func (sc *scope) compileIn(x *inNode) (scalar, error) {
	val, err := sc.compile(x.x)
	if err != nil {
		return scalar{}, err
	}

	items := make([]scalar, len(x.list))
	for i, e := range x.list {
		if items[i], err = sc.compile(e); err != nil {
			return scalar{}, err
		}

		if !typesComparable(val.typ, items[i].typ) && items[i].typ != TDynamic {
			return scalar{}, semErr("cannot compare values of types %s and %s", val.typ, items[i].typ)
		}
	}

	return scalar{typ: TBool, fn: func(r []any) (any, error) {
		v, evalErr := val.fn(r)
		if evalErr != nil || v == nil {
			return nil, evalErr
		}

		found, evalErr := anyItemEquals(items, v, x.ci, r)
		if evalErr != nil {
			return nil, evalErr
		}

		return found != x.neg, nil
	}}, nil
}

func anyItemEquals(items []scalar, v any, ci bool, r []any) (bool, error) {
	for _, it := range items {
		iv, err := it.fn(r)
		if err != nil {
			return false, err
		}

		cands := []any{iv}
		if arr, ok := iv.([]any); ok {
			cands = arr
		}

		if slices.ContainsFunc(cands, func(c any) bool { return inEqual(v, c, ci) }) {
			return true, nil
		}
	}

	return false, nil
}

func inEqual(a, b any, ci bool) bool {
	if ci {
		as, aok := a.(string)
		bs, bok := b.(string)

		if aok && bok {
			return strings.EqualFold(as, bs)
		}
	}

	c, ok := compareValues(a, b)

	return ok && c == 0
}

func (sc *scope) compileBetween(x *betweenNode) (scalar, error) {
	v, err := sc.compile(x.x)
	if err != nil {
		return scalar{}, err
	}

	lo, hi, err := sc.compilePair(x.lo, x.hi)
	if err != nil {
		return scalar{}, err
	}

	if !typesComparable(v.typ, lo.typ) || !typesComparable(v.typ, hi.typ) {
		return scalar{}, semErr("between bounds must have the same type as the value")
	}

	return scalar{typ: TBool, fn: func(r []any) (any, error) {
		a, l, evalErr := evalPair(v, lo, r)
		if evalErr != nil || a == nil || l == nil {
			return nil, evalErr
		}

		h, evalErr := hi.fn(r)
		if evalErr != nil || h == nil {
			return nil, evalErr
		}

		c1, ok1 := compareValues(a, l)
		c2, ok2 := compareValues(a, h)

		if !ok1 || !ok2 {
			return nullValue()
		}

		return (c1 >= 0 && c2 <= 0) != x.neg, nil
	}}, nil
}

func (sc *scope) compileMember(x *memberNode) (scalar, error) {
	base, err := sc.compile(x.x)
	if err != nil {
		return scalar{}, err
	}

	return scalar{typ: TDynamic, fn: func(r []any) (any, error) {
		v, evalErr := base.fn(r)
		if evalErr != nil {
			return nil, evalErr
		}

		return dynamicIndex(v, x.name), nil
	}}, nil
}

func (sc *scope) compileIndex(x *indexNode) (scalar, error) {
	base, idx, err := sc.compilePair(x.x, x.idx)
	if err != nil {
		return scalar{}, err
	}

	return scalar{typ: TDynamic, fn: func(r []any) (any, error) {
		v, i, evalErr := evalPair(base, idx, r)
		if evalErr != nil {
			return nil, evalErr
		}

		return dynamicIndex(v, i), nil
	}}, nil
}

func dynamicIndex(v, key any) any {
	if s, ok := v.(string); ok {
		parsed, err := parseJSONValue(s)
		if err != nil {
			return nil
		}

		v = parsed
	}

	switch c := v.(type) {
	case map[string]any:
		if k, ok := key.(string); ok {
			return c[k]
		}
	case []any:
		if i, ok := key.(int64); ok {
			if i < 0 {
				i += int64(len(c))
			}

			if i >= 0 && i < int64(len(c)) {
				return c[i]
			}
		}
	}

	return nil
}

func errArgs(name string, want, got int) error {
	return semErr("function '%s' expects %d argument(s) but got %d", name, want, got)
}

func (sc *scope) compileArgs(args []node) ([]scalar, error) {
	out := make([]scalar, len(args))

	for i, a := range args {
		s, err := sc.compile(a)
		if err != nil {
			return nil, err
		}

		out[i] = s
	}

	return out, nil
}

func (sc *scope) compileCall(x *callNode) (scalar, error) {
	name := strings.ToLower(x.name)

	if isAggregate(name) {
		if sc.agg == nil {
			return scalar{}, semErr("aggregation function '%s' is only valid in summarize", x.name)
		}

		return sc.agg.register(name, x, sc)
	}

	def, ok := scalarFuncs[name]
	if !ok {
		return scalar{}, semErr("Failed to resolve function named '%s'", x.name)
	}

	if len(x.args) < def.min || (def.max >= 0 && len(x.args) > def.max) {
		return scalar{}, errArgs(x.name, def.min, len(x.args))
	}

	args, err := sc.compileArgs(x.args)
	if err != nil {
		return scalar{}, err
	}

	types := make([]Type, len(args))
	for i, a := range args {
		types[i] = a.typ
	}

	ret, err := def.ret(types)
	if err != nil {
		return scalar{}, err
	}

	now := sc.env.now

	return scalar{typ: ret, fn: func(r []any) (any, error) {
		vs, evalErr := evalArgs(args, def.nullable, r)
		if evalErr != nil || vs == nil {
			return nil, evalErr
		}

		return def.call(&callCtx{now: now}, vs)
	}}, nil
}

// evalArgs evaluates call arguments; it returns nil values (no error) when a non-nullable argument is null.
func evalArgs(args []scalar, nullable bool, r []any) ([]any, error) {
	vs := make([]any, len(args))

	for i, a := range args {
		v, err := a.fn(r)
		if err != nil {
			return nil, err
		}

		if v == nil && !nullable {
			return nil, nil
		}

		vs[i] = v
	}

	return vs, nil
}
