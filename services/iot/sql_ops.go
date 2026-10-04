package iot

import (
	"math"
	"strings"
)

const (
	opAnd = "AND"
	opOr  = "OR"
	opNot = "NOT"
)

type unaryNode struct {
	x  sqlNode
	op string
}

func (n unaryNode) eval(c *sqlCtx) any {
	v := n.x.eval(c)

	if n.op == opNot {
		b, ok := toBoolConv(v)
		if !ok {
			return sqlUndefined{}
		}

		return !b
	}

	switch t := v.(type) {
	case int64:
		return -t
	case float64:
		return c.num(-t)
	}

	if f, ok := toDecimal(v); ok {
		return c.num(-f)
	}

	return sqlUndefined{}
}

type binaryNode struct {
	l, r sqlNode
	op   string
}

func (n binaryNode) eval(c *sqlCtx) any {
	l, r := n.l.eval(c), n.r.eval(c)

	switch n.op {
	case opAnd, opOr:
		return logicalOp(n.op, l, r)
	case "=", "<>", "!=":
		return equalityOp(n.op, l, r)
	case "<", "<=", ">", ">=":
		return orderOp(n.op, l, r)
	case "+":
		return plusOp(c, l, r)
	}

	return arithOp(c, n.op, l, r)
}

func logicalOp(op string, l, r any) any {
	lb, lok := toBoolConv(l)
	rb, rok := toBoolConv(r)

	if !lok || !rok {
		return sqlUndefined{}
	}

	if op == opAnd {
		return lb && rb
	}

	return lb || rb
}

func equalityOp(op string, l, r any) any {
	eq, ok := sqlEquals(l, r)
	if !ok {
		return sqlUndefined{}
	}

	if op == "=" {
		return eq
	}

	return !eq
}

// sqlEquals compares values; ok is false when either side is Undefined.
func sqlEquals(l, r any) (bool, bool) {
	if isUndef(l) || isUndef(r) {
		return false, false
	}

	return valuesEqual(l, r), true
}

func valuesEqual(l, r any) bool {
	switch lt := l.(type) {
	case nil:
		return r == nil
	case int64, float64:
		lf, _ := toDecimal(lt)
		rf, ok := r.(float64)

		if ri, isInt := r.(int64); isInt {
			rf, ok = float64(ri), true
		}

		return ok && lf == rf
	case []any:
		return arraysEqual(lt, r)
	case *sqlObject:
		return objectsEqual(lt, r)
	}

	return l == r
}

func arraysEqual(l []any, r any) bool {
	ra, ok := r.([]any)
	if !ok || len(ra) != len(l) {
		return false
	}

	for i := range l {
		if !valuesEqual(l[i], ra[i]) {
			return false
		}
	}

	return true
}

func objectsEqual(l *sqlObject, r any) bool {
	ro, ok := r.(*sqlObject)
	if !ok || len(ro.keys) != len(l.keys) {
		return false
	}

	for k, lv := range l.vals {
		rv, found := ro.vals[k]
		if !found || !valuesEqual(lv, rv) {
			return false
		}
	}

	return true
}

// orderOp compares as Decimals; non-numeric operands are Undefined.
func orderOp(op string, l, r any) any {
	lf, lok := toDecimal(l)
	rf, rok := toDecimal(r)

	if !lok || !rok {
		return sqlUndefined{}
	}

	switch op {
	case "<":
		return lf < rf
	case "<=":
		return lf <= rf
	case ">":
		return lf > rf
	}

	return lf >= rf
}

func plusOp(c *sqlCtx, l, r any) any {
	_, ls := l.(string)
	_, rs := r.(string)

	if ls || rs {
		lstr, lok := toStringConv(l)
		rstr, rok := toStringConv(r)

		if !lok || !rok {
			return sqlUndefined{}
		}

		return lstr + rstr
	}

	return numericOp(c, "+", l, r, false)
}

func arithOp(c *sqlCtx, op string, l, r any) any {
	return numericOp(c, op, l, r, true)
}

func numericOp(c *sqlCtx, op string, l, r any, coerce bool) any {
	li, lInt := l.(int64)
	ri, rInt := r.(int64)

	if lInt && rInt {
		if v, ok := intArith(op, li, ri); ok {
			return v
		}
	}

	lf, lok := numberOperand(l, coerce)
	rf, rok := numberOperand(r, coerce)

	if !lok || !rok {
		return sqlUndefined{}
	}

	return floatArith(c, op, lf, rf)
}

func numberOperand(v any, coerce bool) (float64, bool) {
	if coerce {
		return toDecimal(v)
	}

	switch t := v.(type) {
	case int64:
		return float64(t), true
	case float64:
		return t, true
	}

	return 0, false
}

func intArith(op string, l, r int64) (any, bool) {
	switch op {
	case "+":
		return l + r, true
	case "-":
		return l - r, true
	case "*":
		return l * r, true
	case "/":
		if r != 0 && l%r == 0 {
			return l / r, true
		}
	case "%":
		if r != 0 {
			return l % r, true
		}
	}

	return nil, false
}

func floatArith(c *sqlCtx, op string, l, r float64) any {
	switch op {
	case "+":
		return c.num(l + r)
	case "-":
		return c.num(l - r)
	case "*":
		return c.num(l * r)
	case "/":
		if r == 0 {
			return sqlUndefined{}
		}

		return c.num(l / r)
	case "%":
		if r == 0 {
			return sqlUndefined{}
		}

		return c.num(math.Mod(l, r))
	}

	return sqlUndefined{}
}

type inNode struct{ l, r sqlNode }

func (n inNode) eval(c *sqlCtx) any {
	l, r := n.l.eval(c), n.r.eval(c)

	arr, ok := r.([]any)
	if !ok || isUndef(l) {
		return sqlUndefined{}
	}

	for _, e := range arr {
		if valuesEqual(l, e) {
			return true
		}
	}

	return false
}

func kwEqual(s, kw string) bool { return strings.EqualFold(s, kw) }
