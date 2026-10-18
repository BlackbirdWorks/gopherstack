package opencypher

import (
	"context"
	"maps"
	"math"
	"slices"
	"strings"
)

type row map[string]any

type exec struct {
	ctx    context.Context
	g      *Graph
	params map[string]any
}

func (r row) with(name string, v any) row {
	c := make(row, len(r)+1)
	maps.Copy(c, r)

	if name != "" {
		c[name] = v
	}

	return c
}

func (x *exec) eval(e expr, r row, grp []row) (any, error) {
	switch t := e.(type) {
	case litExpr:
		return t.v, nil
	case paramExpr:
		v, ok := x.params[t.name]
		if !ok {
			return nil, &Error{Code: CodeInvalidParameter, Detail: "Expected parameter(s): " + t.name}
		}

		return v, nil
	case identExpr:
		v, ok := r[t.name]
		if !ok {
			return nil, malformed("Variable `%s` not defined", t.name)
		}

		return v, nil
	case propExpr:
		return x.evalProp(t, r, grp)
	case callExpr:
		return x.evalCall(t, r, grp)
	case unaryExpr:
		return x.evalUnary(t, r, grp)
	case binaryExpr:
		return x.evalBinary(t, r, grp)
	default:
		return x.evalCompound(e, r, grp)
	}
}

func (x *exec) evalCompound(e expr, r row, grp []row) (any, error) {
	switch t := e.(type) {
	case indexExpr:
		return x.evalIndex(t, r, grp)
	case sliceExpr:
		return x.evalSlice(t, r, grp)
	case isNullExpr:
		v, err := x.eval(t.e, r, grp)

		return (v == nil) != t.not, err
	case listExpr:
		return x.evalList(t, r, grp)
	case mapExpr:
		return x.evalMap(&t, r, grp)
	case labelsExpr:
		return x.evalHasLabels(t, r, grp)
	case caseExpr:
		return x.evalCase(t, r, grp)
	default:
		return nil, unsupported("unsupported expression")
	}
}

func (x *exec) evalProp(t propExpr, r row, grp []row) (any, error) {
	base, err := x.eval(t.e, r, grp)
	if err != nil {
		return nil, err
	}

	switch b := base.(type) {
	case nil:
		return nil, nil //nolint:nilnil // property of null is null
	case *Node:
		return b.Props[t.key], nil
	case *Rel:
		return b.Props[t.key], nil
	case map[string]any:
		return b[t.key], nil
	default:
		return nil, malformed("Type mismatch: expected Map, Node or Relationship but was %s", typeName(base))
	}
}

func (x *exec) evalIndex(t indexExpr, r row, grp []row) (any, error) {
	base, err := x.eval(t.e, r, grp)
	if err != nil {
		return nil, err
	}

	idx, err := x.eval(t.i, r, grp)
	if err != nil || base == nil || idx == nil {
		return nil, err
	}

	switch b := base.(type) {
	case []any:
		i, ok := idx.(int64)
		if !ok {
			return nil, malformed("Type mismatch: list index must be Integer but was %s", typeName(idx))
		}

		if i < 0 {
			i += int64(len(b))
		}

		if i < 0 || i >= int64(len(b)) {
			return nil, nil //nolint:nilnil // out of range is null
		}

		return b[i], nil
	case map[string]any:
		k, ok := idx.(string)
		if !ok {
			return nil, malformed("Type mismatch: map key must be String but was %s", typeName(idx))
		}

		return b[k], nil
	default:
		return nil, malformed("Type mismatch: expected List or Map but was %s", typeName(base))
	}
}

func (x *exec) evalSlice(t sliceExpr, r row, grp []row) (any, error) {
	base, err := x.eval(t.e, r, grp)
	if err != nil {
		return nil, err
	}

	list, ok := base.([]any)
	if !ok {
		return nil, err
	}

	from, to := 0, len(list)

	if t.from != nil {
		if from, err = x.intBound(t.from, r, grp, len(list)); err != nil {
			return nil, err
		}
	}

	if t.to != nil {
		if to, err = x.intBound(t.to, r, grp, len(list)); err != nil {
			return nil, err
		}
	}

	if from > to {
		return []any{}, nil
	}

	return append([]any{}, list[from:to]...), nil
}

func (x *exec) intBound(e expr, r row, grp []row, n int) (int, error) {
	v, err := x.eval(e, r, grp)
	if err != nil {
		return 0, err
	}

	i, ok := v.(int64)
	if !ok {
		return 0, malformed("Type mismatch: slice bound must be Integer but was %s", typeName(v))
	}

	if i < 0 {
		i += int64(n)
	}

	return int(min(max(i, 0), int64(n))), nil
}

func (x *exec) evalList(t listExpr, r row, grp []row) (any, error) {
	out := make([]any, 0, len(t.items))

	for _, it := range t.items {
		v, err := x.eval(it, r, grp)
		if err != nil {
			return nil, err
		}

		out = append(out, v)
	}

	return out, nil
}

func (x *exec) evalMap(t *mapExpr, r row, grp []row) (any, error) {
	out := make(map[string]any, len(t.keys))

	for i, k := range t.keys {
		v, err := x.eval(t.vals[i], r, grp)
		if err != nil {
			return nil, err
		}

		out[k] = v
	}

	return out, nil
}

func (x *exec) evalHasLabels(t labelsExpr, r row, grp []row) (any, error) {
	v, err := x.eval(t.e, r, grp)
	if err != nil || v == nil {
		return nil, err
	}

	n, ok := v.(*Node)
	if !ok {
		return nil, malformed("Type mismatch: expected Node but was %s", typeName(v))
	}

	return hasLabels(n, t.labels), nil
}

func hasLabels(n *Node, labels []string) bool {
	for _, want := range labels {
		found := slices.Contains(n.Labels, want)

		if !found {
			return false
		}
	}

	return true
}

func (x *exec) evalCase(t caseExpr, r row, grp []row) (any, error) {
	var subject any

	var err error

	if t.subject != nil {
		if subject, err = x.eval(t.subject, r, grp); err != nil {
			return nil, err
		}
	}

	for i, w := range t.whens {
		wv, werr := x.eval(w, r, grp)
		if werr != nil {
			return nil, werr
		}

		hit := false

		if t.subject != nil {
			hit, _ = equals(subject, wv)
		} else if b, ok := wv.(bool); ok {
			hit = b
		}

		if hit {
			return x.eval(t.thens[i], r, grp)
		}
	}

	if t.els != nil {
		return x.eval(t.els, r, grp)
	}

	return nil, nil //nolint:nilnil // no branch matched
}

func (x *exec) evalUnary(t unaryExpr, r row, grp []row) (any, error) {
	v, err := x.eval(t.e, r, grp)
	if err != nil {
		return nil, err
	}

	switch t.op {
	case "NOT":
		b, null, berr := asBool(v)
		if berr != nil || null {
			return nil, berr
		}

		return !b, nil
	case "-":
		switch n := v.(type) {
		case nil:
			return nil, nil //nolint:nilnil // null propagates
		case int64:
			return -n, nil
		case float64:
			return -n, nil
		}

		return nil, malformed("Type mismatch: expected Number but was %s", typeName(v))
	default:
		return v, nil
	}
}

func asBool(v any) (bool, bool, error) {
	switch t := v.(type) {
	case nil:
		return false, true, nil
	case bool:
		return t, false, nil
	default:
		return false, false, malformed("Type mismatch: expected Boolean but was %s", typeName(v))
	}
}

func (x *exec) evalBinary(t binaryExpr, r row, grp []row) (any, error) {
	switch t.op {
	case opAnd, opOr, "XOR":
		return x.evalLogic(t, r, grp)
	}

	l, err := x.eval(t.l, r, grp)
	if err != nil {
		return nil, err
	}

	rv, err := x.eval(t.r, r, grp)
	if err != nil {
		return nil, err
	}

	switch t.op {
	case "=", "<>", "<", ">", "<=", ">=":
		return compareOp(t.op, l, rv), nil
	case "IN":
		return inList(l, rv)
	case "STARTS WITH", "ENDS WITH", "CONTAINS":
		return stringOp(t.op, l, rv)
	default:
		return arith(t.op, l, rv)
	}
}

const (
	opAnd = "AND"
	opOr  = "OR"
)

func (x *exec) evalLogic(t binaryExpr, r row, grp []row) (any, error) {
	lb, lnull, err := x.evalBool(t.l, r, grp)
	if err != nil {
		return nil, err
	}

	if !lnull && ((t.op == opAnd && !lb) || (t.op == opOr && lb)) {
		return lb, nil
	}

	rb, rnull, err := x.evalBool(t.r, r, grp)
	if err != nil {
		return nil, err
	}

	return combine(t.op, lb, lnull, rb, rnull), nil
}

func (x *exec) evalBool(e expr, r row, grp []row) (bool, bool, error) {
	v, err := x.eval(e, r, grp)
	if err != nil {
		return false, false, err
	}

	return asBool(v)
}

func combine(op string, lb, lnull, rb, rnull bool) any {
	switch {
	case op == opAnd && !rnull && !rb:
		return false
	case op == opOr && !rnull && rb:
		return true
	case lnull || rnull:
		return nil
	case op == opAnd:
		return true
	case op == opOr:
		return false
	default:
		return lb != rb
	}
}

func compareOp(op string, l, r any) any {
	switch op {
	case "=", "<>":
		eq, known := equals(l, r)
		if !known {
			return nil
		}

		return eq == (op == "=")
	}

	c, ok := compare(l, r)
	if !ok {
		return nil
	}

	switch op {
	case "<":
		return c < 0
	case ">":
		return c > 0
	case "<=":
		return c <= 0
	default:
		return c >= 0
	}
}

func inList(l, r any) (any, error) {
	if r == nil {
		return nil, nil //nolint:nilnil // null list
	}

	list, ok := r.([]any)
	if !ok {
		return nil, malformed("Type mismatch: expected List but was %s", typeName(r))
	}

	unknown := false

	for _, e := range list {
		eq, known := equals(l, e)
		if known && eq {
			return true, nil
		}

		unknown = unknown || !known
	}

	if unknown {
		return nil, nil //nolint:nilnil // three-valued logic
	}

	return false, nil
}

func stringOp(op string, l, r any) (any, error) {
	if l == nil || r == nil {
		return nil, nil //nolint:nilnil // null propagates
	}

	a, aok := l.(string)
	b, bok := r.(string)

	if !aok || !bok {
		return nil, nil //nolint:nilnil // non-strings compare as null
	}

	switch op {
	case "STARTS WITH":
		return strings.HasPrefix(a, b), nil
	case "ENDS WITH":
		return strings.HasSuffix(a, b), nil
	default:
		return strings.Contains(a, b), nil
	}
}

func arith(op string, l, r any) (any, error) {
	if l == nil || r == nil {
		return nil, nil //nolint:nilnil // null propagates
	}

	if op == "+" {
		if v, ok := concat(l, r); ok {
			return v, nil
		}
	}

	li, lint := l.(int64)
	ri, rint := r.(int64)

	if lint && rint && op != "^" {
		return intArith(op, li, ri)
	}

	if !isNumber(l) || !isNumber(r) {
		return nil, malformed("Type mismatch: cannot apply %s to %s and %s", op, typeName(l), typeName(r))
	}

	return floatArith(op, toFloat(l), toFloat(r)), nil
}

func floatArith(op string, a, b float64) float64 {
	switch op {
	case "+":
		return a + b
	case "-":
		return a - b
	case "*":
		return a * b
	case "/":
		return a / b
	case "%":
		return math.Mod(a, b)
	default:
		return math.Pow(a, b)
	}
}

func concat(l, r any) (any, bool) {
	ls, lstr := l.(string)
	rs, rstr := r.(string)

	switch {
	case lstr && rstr:
		return ls + rs, true
	case lstr && isNumber(r):
		return ls + stringify(r), true
	case rstr && isNumber(l):
		return stringify(l) + rs, true
	}

	ll, llist := l.([]any)
	rl, rlist := r.([]any)

	switch {
	case llist && rlist:
		return append(append([]any{}, ll...), rl...), true
	case llist:
		return append(append([]any{}, ll...), r), true
	case rlist:
		return append([]any{l}, rl...), true
	}

	return nil, false
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
		return nil, &Error{Code: CodeMalformedQuery, Detail: "/ by zero"}
	}

	if op == "/" {
		return a / b, nil
	}

	return a % b, nil
}
