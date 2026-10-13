package apigateway

import (
	"strconv"
	"strings"
)

const maxVTLMacroDepth = 20

type vtlState struct {
	err    error
	vars   map[string]any
	macros map[string]*vtlMacroDef
	out    strings.Builder
	depth  int
	halted bool
	broke  bool
}

// RenderTemplateStrict renders a Velocity template with API Gateway's mapping
// objects ($input, $context, $stageVariables, $util), returning an error for a
// template that does not parse.
func RenderTemplateStrict(tmpl string, ctx VTLContext) (string, error) {
	nodes, macros, err := parseVTL(tmpl)
	if err != nil {
		return "", err
	}

	st := &vtlState{vars: vtlGlobals(ctx), macros: macros}

	if err = st.render(nodes); err != nil {
		return "", err
	}

	if st.err != nil {
		return "", st.err
	}

	return st.out.String(), nil
}

func (st *vtlState) render(nodes []any) error {
	for _, n := range nodes {
		if st.halted || st.broke {
			return nil
		}

		if err := st.renderNode(n); err != nil {
			return err
		}
	}

	return nil
}

func (st *vtlState) renderNode(n any) error {
	switch t := n.(type) {
	case vtlText:
		st.out.WriteString(string(t))
	case *vtlRefNode:
		st.out.WriteString(st.renderRef(t.ref))
	case *vtlSetNode:
		return st.execSet(t)
	case *vtlIfNode:
		return st.execIf(t)
	case *vtlForeachNode:
		return st.execForeach(t)
	case *vtlMacroCall:
		return st.execMacro(t)
	case vtlBreakNode:
		st.broke = true
	case vtlStopNode:
		st.halted = true
	}

	return nil
}

func (st *vtlState) renderRef(ref *vtlRef) string {
	v := st.evalRef(ref)
	if v == nil {
		if ref.quiet {
			return ""
		}

		if m, ok := st.vars[ref.name].(*vtlMap); ok && m.emptyForMissing && len(ref.accessors) > 0 {
			return ""
		}

		if isAWSObject(st.vars[ref.name]) && len(ref.accessors) > 0 && ref.accessors[len(ref.accessors)-1].isCall {
			return ""
		}

		return ref.raw
	}

	return javaString(v)
}

func isAWSObject(v any) bool {
	_, ok := v.(vtlObject)

	return ok
}

func (st *vtlState) execSet(n *vtlSetNode) error {
	val := st.eval(n.expr)
	if st.err != nil {
		return st.err
	}

	if len(n.target.accessors) == 0 {
		if val == nil {
			// Velocity leaves the variable untouched when assigned null.
			return nil
		}

		st.vars[n.target.name] = val

		return nil
	}

	holder := st.evalAccessors(st.vars[n.target.name], n.target.accessors[:len(n.target.accessors)-1])

	last := n.target.accessors[len(n.target.accessors)-1]
	if m, ok := holder.(*vtlMap); ok && !last.isCall {
		m.put(last.name, val)
	}

	return nil
}

func (st *vtlState) execIf(n *vtlIfNode) error {
	for i, cond := range n.conds {
		v := st.eval(cond)
		if st.err != nil {
			return st.err
		}

		if vtlTruthy(v) {
			return st.render(n.bodies[i])
		}
	}

	return st.render(n.elseBody)
}

func (st *vtlState) execForeach(n *vtlForeachNode) error {
	listVal := st.eval(n.list)
	if st.err != nil {
		return st.err
	}

	items := iterableItems(listVal)
	prev, hadPrev := st.vars[n.varName]

	defer func() {
		if hadPrev {
			st.vars[n.varName] = prev
		} else {
			delete(st.vars, n.varName)
		}
	}()

	for i, item := range items {
		st.vars[n.varName] = item
		info := newVTLMap()
		info.put("index", int64(i))
		info.put("count", int64(i+1))
		info.put("hasNext", i < len(items)-1)
		info.put("first", i == 0)
		info.put("last", i == len(items)-1)
		st.vars["foreach"] = info
		st.vars["velocityCount"] = int64(i + 1)

		if err := st.render(n.body); err != nil {
			return err
		}

		if st.halted {
			return nil
		}

		if st.broke {
			st.broke = false

			break
		}
	}

	delete(st.vars, "foreach")
	delete(st.vars, "velocityCount")

	return nil
}

func iterableItems(v any) []any {
	switch t := v.(type) {
	case *vtlList:
		return append([]any(nil), t.items...)
	case *vtlMap:
		out := make([]any, 0, len(t.keys))
		for _, k := range t.keys {
			out = append(out, t.items[k])
		}

		return out
	}

	return nil
}

func (st *vtlState) execMacro(call *vtlMacroCall) error {
	def := st.macros[call.name]
	if def == nil || st.depth >= maxVTLMacroDepth {
		return nil
	}

	saved := make(map[string]any, len(def.params))
	present := make(map[string]bool, len(def.params))

	for i, param := range def.params {
		old, ok := st.vars[param]
		saved[param], present[param] = old, ok

		if i < len(call.args) {
			st.vars[param] = st.eval(call.args[i])
		}
	}

	st.depth++
	err := st.render(def.body)
	st.depth--

	for param := range saved {
		if present[param] {
			st.vars[param] = saved[param]
		} else {
			delete(st.vars, param)
		}
	}

	return err
}

func (st *vtlState) eval(e any) any {
	switch t := e.(type) {
	case *vtlLiteral:
		return t.value
	case *vtlRef:
		return st.evalRef(t)
	case *vtlInterp:
		sub := &vtlState{vars: st.vars, macros: st.macros, depth: st.depth}
		if err := sub.render(t.nodes); err != nil && st.err == nil {
			st.err = err
		}

		return sub.out.String()
	case *vtlListLit:
		return st.evalList(t)
	case *vtlMapLit:
		return st.evalMapLiteral(t)
	case *vtlRange:
		return st.evalRange(t)
	case *vtlUnary:
		return st.evalUnary(t)
	case *vtlBinary:
		return st.evalBinary(t)
	}

	return nil
}

func (st *vtlState) evalList(t *vtlListLit) any {
	l := &vtlList{items: make([]any, 0, len(t.items))}

	for _, item := range t.items {
		l.items = append(l.items, st.eval(item))
	}

	return l
}

func (st *vtlState) evalMapLiteral(t *vtlMapLit) any {
	m := newVTLMap()

	for i := range t.keys {
		m.put(javaString(st.eval(t.keys[i])), st.eval(t.values[i]))
	}

	return m
}

func (st *vtlState) evalRange(t *vtlRange) any {
	a, aok := toInt64(st.eval(t.from))
	b, bok := toInt64(st.eval(t.to))

	if !aok || !bok {
		return nil
	}

	l := &vtlList{}
	if a <= b {
		for i := a; i <= b; i++ {
			l.items = append(l.items, i)
		}
	} else {
		for i := a; i >= b; i-- {
			l.items = append(l.items, i)
		}
	}

	return l
}

func toInt64(v any) (int64, bool) {
	switch t := v.(type) {
	case int64:
		return t, true
	case int:
		return int64(t), true
	case float64:
		return int64(t), true
	case string:
		n, err := strconv.ParseInt(strings.TrimSpace(t), 10, 64)

		return n, err == nil
	}

	return 0, false
}

func (st *vtlState) evalRef(ref *vtlRef) any {
	base, ok := st.vars[ref.name]
	if !ok {
		return nil
	}

	return st.evalAccessors(base, ref.accessors)
}

func (st *vtlState) evalAccessors(base any, accs []vtlAccessor) any {
	cur := base

	for _, acc := range accs {
		if cur == nil {
			return nil
		}

		cur = st.step(cur, acc)
	}

	return cur
}

func (st *vtlState) step(cur any, acc vtlAccessor) any {
	if acc.index != nil {
		return indexValue(cur, st.eval(acc.index))
	}

	if !acc.isCall {
		return propertyValue(cur, acc.name)
	}

	args := make([]any, len(acc.args))

	for i, a := range acc.args {
		args[i] = st.eval(a)
	}

	v, _ := callVTLMethod(cur, acc.name, args)

	return v
}

func indexValue(cur, idx any) any {
	switch t := cur.(type) {
	case *vtlList:
		if i, ok := toInt64(idx); ok && i >= 0 && i < int64(len(t.items)) {
			return t.items[i]
		}
	case *vtlMap:
		v, _ := t.get(javaString(idx))

		return v
	}

	return nil
}

func propertyValue(cur any, name string) any {
	switch t := cur.(type) {
	case *vtlMap:
		v, _ := t.get(name)

		return v
	case vtlObject:
		v, _ := t.property(name)

		return v
	}

	return nil
}

func (st *vtlState) evalUnary(t *vtlUnary) any {
	v := st.eval(t.operand)

	if t.op == "!" {
		return !vtlTruthy(v)
	}

	switch n := v.(type) {
	case int64:
		return -n
	case float64:
		return -n
	}

	return nil
}

func (st *vtlState) evalBinary(t *vtlBinary) any {
	left := st.eval(t.left)

	switch t.op {
	case "&&":
		return vtlTruthy(left) && vtlTruthy(st.eval(t.right))
	case "||":
		return vtlTruthy(left) || vtlTruthy(st.eval(t.right))
	}

	return applyBinary(t.op, left, st.eval(t.right))
}

func applyBinary(op string, left, right any) any {
	switch op {
	case "==":
		return vtlEquals(left, right)
	case "!=":
		return !vtlEquals(left, right)
	case "<", ">", "<=", ">=":
		return vtlOrdered(op, left, right)
	}

	return vtlArithmetic(op, left, right)
}

func vtlEquals(a, b any) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}

	an, aok := vtlToNumber(a)
	bn, bok := vtlToNumber(b)

	if aok && bok {
		return numberValue(an) == numberValue(bn)
	}

	if ab, ok := a.(bool); ok {
		bb, ok2 := b.(bool)

		return ok2 && ab == bb
	}

	return javaString(a) == javaString(b)
}

func numberValue(n any) float64 {
	switch t := n.(type) {
	case int64:
		return float64(t)
	case float64:
		return t
	}

	return 0
}

func vtlOrdered(op string, a, b any) bool {
	cmp, ok := vtlCompare(a, b)
	if !ok {
		return false
	}

	switch op {
	case "<":
		return cmp < 0
	case ">":
		return cmp > 0
	case "<=":
		return cmp <= 0
	}

	return cmp >= 0
}

func vtlCompare(a, b any) (int, bool) {
	an, aok := vtlToNumber(a)
	bn, bok := vtlToNumber(b)

	if aok && bok {
		x, y := numberValue(an), numberValue(bn)

		switch {
		case x < y:
			return -1, true
		case x > y:
			return 1, true
		}

		return 0, true
	}

	as, aIsStr := a.(string)
	bs, bIsStr := b.(string)

	if !aIsStr || !bIsStr {
		return 0, false
	}

	return strings.Compare(as, bs), true
}

func vtlArithmetic(op string, a, b any) any {
	an, aok := vtlToNumber(a)
	bn, bok := vtlToNumber(b)

	if !aok || !bok {
		if op == "+" && (a != nil || b != nil) {
			return javaString(a) + javaString(b)
		}

		return nil
	}

	ai, aInt := an.(int64)
	bi, bInt := bn.(int64)

	if aInt && bInt {
		return intArithmetic(op, ai, bi)
	}

	return floatArithmetic(op, numberValue(an), numberValue(bn))
}

func floatArithmetic(op string, x, y float64) any {
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

	return nil
}

func intArithmetic(op string, a, b int64) any {
	switch op {
	case "+":
		return a + b
	case "-":
		return a - b
	case "*":
		return a * b
	case "/":
		if b == 0 {
			return nil
		}

		return a / b
	case "%":
		if b == 0 {
			return nil
		}

		return a % b
	}

	return nil
}
