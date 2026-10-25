package kql

import (
	"math"
	"regexp"
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
		v, err := in.fn(r)
		if err != nil || v == nil {
			return nil, err
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
	case "and", "or":
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
	isAnd := op == "and"

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

func arithType(op string, a, b Type) (Type, error) {
	switch {
	case a == TDynamic || b == TDynamic || a == "" || b == "":
		return TDynamic, nil
	case isNumType(a) && isNumType(b):
		if a == TReal || b == TReal {
			return TReal, nil
		}

		if a == TInt && b == TInt {
			return TInt, nil
		}

		return TLong, nil
	case a == TDatetime && b == TDatetime && op == "-":
		return TTimespan, nil
	case a == TDatetime && b == TTimespan && (op == "+" || op == "-"):
		return TDatetime, nil
	case a == TTimespan && b == TDatetime && op == "+":
		return TDatetime, nil
	case a == TTimespan && b == TTimespan && (op == "+" || op == "-"):
		return TTimespan, nil
	case a == TTimespan && b == TTimespan && op == "/":
		return TReal, nil
	case (a == TTimespan && isNumType(b) && (op == "*" || op == "/")) ||
		(isNumType(a) && b == TTimespan && op == "*"):
		return TTimespan, nil
	}

	return "", semErr("operator '%s' cannot be applied to %s and %s", op, a, b)
}

func arithOp(op string, a, b scalar) (scalar, error) {
	t, err := arithType(op, a.typ, b.typ)
	if err != nil {
		return scalar{}, err
	}

	return scalar{typ: t, fn: func(r []any) (any, error) {
		av, err := a.fn(r)
		if err != nil {
			return nil, err
		}

		bv, err := b.fn(r)
		if err != nil {
			return nil, err
		}

		if av == nil || bv == nil {
			return nil, nil //nolint:nilnil // null propagates
		}

		return arith(op, av, bv)
	}}, nil
}

func arith(op string, a, b any) (any, error) {
	switch x := a.(type) {
	case int64:
		switch y := b.(type) {
		case int64:
			return intArith(op, x, y), nil
		case float64:
			return floatArith(op, float64(x), y), nil
		case time.Duration:
			if op == "*" {
				return time.Duration(x) * y, nil
			}
		}
	case float64:
		if y, ok := toFloat(b); ok && isNumeric(b) {
			return floatArith(op, x, y), nil
		}

		if y, ok := b.(time.Duration); ok && op == "*" {
			return time.Duration(x * float64(y)), nil
		}
	case time.Time:
		switch y := b.(type) {
		case time.Time:
			if op == "-" {
				return x.Sub(y), nil
			}
		case time.Duration:
			if op == "+" {
				return x.Add(y), nil
			}

			if op == "-" {
				return x.Add(-y), nil
			}
		}
	case time.Duration:
		return durArith(op, x, b)
	}

	return nil, semErr("operator '%s' cannot be applied to %s and %s", op, typeOfValue(a), typeOfValue(b))
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

func comparable(a, b Type) bool {
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
	if !comparable(a.typ, b.typ) {
		return scalar{}, semErr(
			"cannot compare values of types %s and %s; try adding explicit casts", a.typ, b.typ)
	}

	return scalar{typ: TBool, fn: func(r []any) (any, error) {
		av, err := a.fn(r)
		if err != nil {
			return nil, err
		}

		bv, err := b.fn(r)
		if err != nil {
			return nil, err
		}

		if av == nil || bv == nil {
			return nil, nil //nolint:nilnil // comparison with null is null
		}

		c, ok := compareValues(av, bv)
		if !ok {
			switch op {
			case "==":
				return false, nil
			case "!=":
				return true, nil
			}

			return nil, nil //nolint:nilnil // incomparable dynamic values
		}

		switch op {
		case "==":
			return c == 0, nil
		case "!=":
			return c != 0, nil
		case "<":
			return c < 0, nil
		case "<=":
			return c <= 0, nil
		case ">":
			return c > 0, nil
		}

		return c >= 0, nil
	}}, nil
}

func ciEqOp(neg bool, a, b scalar) scalar {
	return scalar{typ: TBool, fn: func(r []any) (any, error) {
		av, err := a.fn(r)
		if err != nil {
			return nil, err
		}

		bv, err := b.fn(r)
		if err != nil {
			return nil, err
		}

		if av == nil || bv == nil {
			return nil, nil //nolint:nilnil // null propagates
		}

		eq := strings.EqualFold(ToString(av), ToString(bv))

		return eq != neg, nil
	}}
}

func stringOp(op string, a, b scalar) (scalar, error) {
	base := strings.TrimPrefix(op, "!")
	neg := strings.HasPrefix(op, "!")

	var re *regexp.Regexp

	if base == "matches regex" {
		if lit, ok := constOf(b); ok {
			var err error
			if re, err = compileRegex(ToString(lit)); err != nil {
				return scalar{}, err
			}
		}
	}

	return scalar{typ: TBool, fn: func(r []any) (any, error) {
		av, err := a.fn(r)
		if err != nil {
			return nil, err
		}

		bv, err := b.fn(r)
		if err != nil {
			return nil, err
		}

		if av == nil || bv == nil {
			return nil, nil //nolint:nilnil // null propagates
		}

		hay, needle := ToString(av), ToString(bv)

		if base == "matches regex" {
			rx := re
			if rx == nil {
				if rx, err = compileRegex(needle); err != nil {
					return nil, err
				}
			}

			return rx.MatchString(hay) != neg, nil
		}

		return applyStringOp(base, hay, needle) != neg, nil
	}}, nil
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

		if !comparable(val.typ, items[i].typ) && items[i].typ != TDynamic {
			return scalar{}, semErr("cannot compare values of types %s and %s", val.typ, items[i].typ)
		}
	}

	return scalar{typ: TBool, fn: func(r []any) (any, error) {
		v, err := val.fn(r)
		if err != nil || v == nil {
			return nil, err
		}

		for _, it := range items {
			iv, err := it.fn(r)
			if err != nil {
				return nil, err
			}

			cands := []any{iv}
			if arr, ok := iv.([]any); ok {
				cands = arr
			}

			for _, c := range cands {
				if inEqual(v, c, x.ci) {
					return !x.neg, nil
				}
			}
		}

		return x.neg, nil
	}}, nil
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

	if !comparable(v.typ, lo.typ) || !comparable(v.typ, hi.typ) {
		return scalar{}, semErr("between bounds must have the same type as the value")
	}

	return scalar{typ: TBool, fn: func(r []any) (any, error) {
		a, err := v.fn(r)
		if err != nil || a == nil {
			return nil, err
		}

		l, err := lo.fn(r)
		if err != nil || l == nil {
			return nil, err
		}

		h, err := hi.fn(r)
		if err != nil || h == nil {
			return nil, err
		}

		c1, ok1 := compareValues(a, l)
		c2, ok2 := compareValues(a, h)

		if !ok1 || !ok2 {
			return nil, nil //nolint:nilnil // incomparable
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
		v, err := base.fn(r)
		if err != nil {
			return nil, err
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
		v, err := base.fn(r)
		if err != nil {
			return nil, err
		}

		i, err := idx.fn(r)
		if err != nil {
			return nil, err
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
		vs := make([]any, len(args))

		for i, a := range args {
			v, err := a.fn(r)
			if err != nil {
				return nil, err
			}

			if v == nil && !def.nullable {
				return nil, nil //nolint:nilnil // null in, null out
			}

			vs[i] = v
		}

		return def.call(&callCtx{now: now}, vs)
	}}, nil
}

func fnError(name, msg string) error { return semErr("%s: %s", name, msg) }
