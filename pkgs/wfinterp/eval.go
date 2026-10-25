package wfinterp

import (
	"math"
	"strings"
)

type evaluator struct {
	eng  *Engine
	vars func(name string) (any, bool)
	subs map[string]*routine
	cc   *callCtx
}

func (ev *evaluator) eval(n node) (any, error) {
	switch x := n.(type) {
	case *litNode:
		return x.v, nil
	case *nameNode:
		return ev.evalName(x)
	case *memberNode:
		return ev.evalMember(x)
	case *indexNode:
		return ev.evalIndex(x)
	case *callNode:
		return ev.evalCall(x)
	case *unaryNode:
		return ev.evalUnary(x)
	case *binNode:
		return ev.evalBinary(x)
	case *listNode:
		return ev.evalList(x)
	case *mapNode:
		return ev.evalMap(x)
	default:
		return nil, typeErr("unsupported expression")
	}
}

func (ev *evaluator) evalName(x *nameNode) (any, error) {
	if v, ok := ev.vars(x.name); ok {
		return v, nil
	}

	if _, ok := ev.subs[x.name]; ok {
		return &FuncRef{Name: x.name}, nil
	}

	if v, ok := ev.eng.lookupConst(x.name); ok {
		return v, nil
	}

	return nil, rtErr(TagKeyError, "variable %q is not defined", x.name)
}

func (ev *evaluator) rootIsVar(n node) bool {
	d, ok := dotted(n)
	if !ok {
		return true
	}

	root, _, _ := strings.Cut(d, ".")
	_, isVar := ev.vars(root)

	return isVar
}

func (ev *evaluator) evalMember(x *memberNode) (any, error) {
	if d, ok := dotted(x); ok && !ev.rootIsVar(x) {
		if v, found := ev.eng.lookupConst(d); found {
			return v, nil
		}
	}

	base, err := ev.eval(x.x)
	if err != nil {
		return nil, err
	}

	return getField(base, x.name)
}

func (ev *evaluator) evalIndex(x *indexNode) (any, error) {
	base, err := ev.eval(x.x)
	if err != nil {
		return nil, err
	}

	idx, err := ev.eval(x.i)
	if err != nil {
		return nil, err
	}

	return getField(base, idx)
}

func getField(base, key any) (any, error) {
	switch b := base.(type) {
	case map[string]any:
		k := formatValue(key)
		if _, isStr := key.(string); !isStr && !isNumber(key) {
			return nil, typeErr("map keys must be strings, not %s", typeName(key))
		}

		v, ok := b[k]
		if !ok {
			return nil, rtErr(TagKeyError, "key not found: %s", k)
		}

		return v, nil
	case []any:
		i, ok := key.(int64)
		if !ok {
			return nil, typeErr("list index must be an integer, not %s", typeName(key))
		}

		if i < 0 || i >= int64(len(b)) {
			return nil, rtErr(TagIndexError, "list index out of range: %d", i)
		}

		return b[i], nil
	default:
		return nil, typeErr("cannot read %q from a %s", formatValue(key), typeName(base))
	}
}

func (ev *evaluator) evalList(x *listNode) (any, error) {
	out := make([]any, len(x.items))

	for i, it := range x.items {
		v, err := ev.eval(it)
		if err != nil {
			return nil, err
		}

		out[i] = v
	}

	return out, nil
}

func (ev *evaluator) evalMap(x *mapNode) (any, error) {
	out := make(map[string]any, len(x.keys))

	for i := range x.keys {
		k, err := ev.eval(x.keys[i])
		if err != nil {
			return nil, err
		}

		ks, ok := k.(string)
		if !ok {
			return nil, typeErr("map keys must be strings, not %s", typeName(k))
		}

		v, err := ev.eval(x.vals[i])
		if err != nil {
			return nil, err
		}

		out[ks] = v
	}

	return out, nil
}

func (ev *evaluator) evalCall(x *callNode) (any, error) {
	name, ok := dotted(x.fn)
	if !ok || ev.rootIsVar(x.fn) {
		return nil, typeErr("expression is not callable")
	}

	switch name {
	case fnIf:
		return ev.evalIf(x)
	case fnDefault:
		return ev.evalDefault(x)
	}

	spec, found := ev.eng.funcs[name]
	if !found {
		return nil, rtErr(TagKeyError, "function not found: %s", name)
	}

	if spec.blocking {
		return nil, typeErr("%s cannot be used in an expression; use a call step", name)
	}

	if len(x.args) < spec.min || len(x.args) > len(spec.params) {
		return nil, typeErr("%s: expected %d to %d arguments, got %d", name, spec.min, len(spec.params), len(x.args))
	}

	args := make([]any, len(spec.params))

	for i, a := range x.args {
		v, err := ev.eval(a)
		if err != nil {
			return nil, err
		}

		args[i] = v
	}

	return spec.fn(ev.cc, args)
}

func (ev *evaluator) evalIf(x *callNode) (any, error) {
	if len(x.args) != 3 { //nolint:mnd // if(cond, a, b)
		return nil, typeErr("if: expected 3 arguments, got %d", len(x.args))
	}

	c, err := ev.eval(x.args[0])
	if err != nil {
		return nil, err
	}

	b, ok := c.(bool)
	if !ok {
		return nil, typeErr("if: condition must be a boolean, not %s", typeName(c))
	}

	if b {
		return ev.eval(x.args[1])
	}

	return ev.eval(x.args[2])
}

func (ev *evaluator) evalDefault(x *callNode) (any, error) {
	if len(x.args) != 2 { //nolint:mnd // default(val, fallback)
		return nil, typeErr("default: expected 2 arguments, got %d", len(x.args))
	}

	v, err := ev.eval(x.args[0])
	if err != nil {
		return nil, err
	}

	if v != nil {
		return v, nil
	}

	return ev.eval(x.args[1])
}

func (ev *evaluator) evalUnary(x *unaryNode) (any, error) {
	v, err := ev.eval(x.x)
	if err != nil {
		return nil, err
	}

	if x.op == "not" {
		b, ok := v.(bool)
		if !ok {
			return nil, typeErr("not: operand must be a boolean, not %s", typeName(v))
		}

		return !b, nil
	}

	switch n := v.(type) {
	case int64:
		return -n, nil
	case float64:
		return -n, nil
	default:
		return nil, typeErr("unary -: operand must be a number, not %s", typeName(v))
	}
}

func (ev *evaluator) evalBinary(x *binNode) (any, error) {
	l, err := ev.eval(x.l)
	if err != nil {
		return nil, err
	}

	if x.op == opAnd || x.op == opOr {
		return ev.evalLogic(x, l)
	}

	r, err := ev.eval(x.r)
	if err != nil {
		return nil, err
	}

	switch x.op {
	case "==":
		return equal(l, r), nil
	case "!=":
		return !equal(l, r), nil
	case "<", ">", "<=", ">=":
		return compare(x.op, l, r)
	case "in":
		return membership(l, r)
	default:
		return arith(x.op, l, r)
	}
}

func (ev *evaluator) evalLogic(x *binNode, l any) (any, error) {
	lb, ok := l.(bool)
	if !ok {
		return nil, typeErr("%s: operands must be booleans, not %s", x.op, typeName(l))
	}

	if (x.op == opAnd && !lb) || (x.op == opOr && lb) {
		return lb, nil
	}

	r, err := ev.eval(x.r)
	if err != nil {
		return nil, err
	}

	rb, ok := r.(bool)
	if !ok {
		return nil, typeErr("%s: operands must be booleans, not %s", x.op, typeName(r))
	}

	return rb, nil
}

func compare(op string, l, r any) (any, error) {
	var c int

	if fl, ok := toFloat(l); ok {
		fr, isNum := toFloat(r)
		if !isNum {
			return nil, typeErr("%s: cannot compare %s with %s", op, typeName(l), typeName(r))
		}

		c = cmpFloat(fl, fr)
	} else if sl, isStr := l.(string); isStr {
		sr, isStr2 := r.(string)
		if !isStr2 {
			return nil, typeErr("%s: cannot compare %s with %s", op, typeName(l), typeName(r))
		}

		c = strings.Compare(sl, sr)
	} else {
		return nil, typeErr("%s: cannot compare %s with %s", op, typeName(l), typeName(r))
	}

	switch op {
	case "<":
		return c < 0, nil
	case ">":
		return c > 0, nil
	case "<=":
		return c <= 0, nil
	default:
		return c >= 0, nil
	}
}

func cmpFloat(a, b float64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func membership(l, r any) (any, error) {
	switch c := r.(type) {
	case []any:
		for _, e := range c {
			if equal(e, l) {
				return true, nil
			}
		}

		return false, nil
	case map[string]any:
		_, ok := c[formatValue(l)]

		return ok, nil
	case string:
		s, ok := l.(string)
		if !ok {
			return nil, typeErr("in: a string can only contain strings, not %s", typeName(l))
		}

		return strings.Contains(c, s), nil
	default:
		return nil, typeErr("in: right operand must be a list, map or string, not %s", typeName(r))
	}
}

func arith(op string, l, r any) (any, error) {
	if op == "+" {
		if ls, ok := l.(string); ok {
			rs, isStr := r.(string)
			if !isStr {
				return nil, typeErr("+: cannot add %s to a string", typeName(r))
			}

			return ls + rs, nil
		}
	}

	li, lInt := l.(int64)
	ri, rInt := r.(int64)

	if lInt && rInt && op != "/" {
		return intArith(op, li, ri)
	}

	lf, lok := toFloat(l)
	rf, rok := toFloat(r)

	if !lok || !rok {
		return nil, typeErr("%s: unsupported operand types %s and %s", op, typeName(l), typeName(r))
	}

	return floatArith(op, lf, rf)
}

func intArith(op string, a, b int64) (any, error) {
	switch op {
	case "+":
		return a + b, nil
	case "-":
		return a - b, nil
	case "*":
		return a * b, nil
	}

	if b == 0 {
		return nil, rtErr(TagZeroDivisionError, "division by zero")
	}

	if op == "%" {
		return a % b, nil
	}

	q := a / b
	if (a%b != 0) && ((a < 0) != (b < 0)) {
		q--
	}

	return q, nil
}

func floatArith(op string, a, b float64) (any, error) {
	switch op {
	case "+":
		return a + b, nil
	case "-":
		return a - b, nil
	case "*":
		return a * b, nil
	}

	if b == 0 {
		return nil, rtErr(TagZeroDivisionError, "division by zero")
	}

	switch op {
	case "/":
		return a / b, nil
	case "%":
		return math.Mod(a, b), nil
	default:
		return math.Floor(a / b), nil
	}
}
