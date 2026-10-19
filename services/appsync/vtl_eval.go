package appsync

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

const (
	vtlMaxSteps = 2_000_000
	utilRoot    = "util"
)

// vlist is a mutable VTL list (java.util.List semantics).
type vlist struct{ items []any }

// MarshalJSON lets stash values holding VTL lists encode as JSON arrays.
func (l *vlist) MarshalJSON() ([]byte, error) {
	if l.items == nil {
		return []byte("[]"), nil
	}

	return json.Marshal(l.items)
}

// vns is a utility namespace reference ($util, $util.time, ...).
type vns string

type vtlOutcome struct {
	Value    any
	Output   string
	Returned bool
}

type ctlKind int

const (
	ctlNone ctlKind = iota
	ctlBreak
	ctlStop
	ctlReturn
)

type vtlRun struct {
	vars     map[string]any
	util     *utilEnv
	retVal   any
	out      strings.Builder
	steps    int
	returned bool
}

// runVTL renders tmpl with ctx as $ctx/$context. The stash inside ctx is
// shared and mutated in place.
func runVTL(tmpl string, ctx map[string]any, ue *utilEnv) (vtlOutcome, error) {
	nodes, err := parseVTL(tmpl)
	if err != nil {
		return vtlOutcome{}, err
	}

	if ue == nil {
		ue = &utilEnv{}
	}

	c, _ := vnorm(ctx).(map[string]any)
	if st, ok := ctx["stash"].(map[string]any); ok {
		for k, v := range st {
			st[k] = vnorm(v)
		}

		c["stash"] = st
	}

	r := &vtlRun{vars: map[string]any{"ctx": c, "context": c}, util: ue}

	if _, err = r.execNodes(nodes); err != nil {
		return vtlOutcome{}, err
	}

	return vtlOutcome{Output: r.out.String(), Returned: r.returned, Value: r.retVal}, nil
}

func (r *vtlRun) tick() error {
	r.steps++
	if r.steps > vtlMaxSteps {
		return fmt.Errorf("%w: template exceeded the execution limit", ErrValidation)
	}

	return nil
}

func (r *vtlRun) execNodes(nodes []vnode) (ctlKind, error) {
	for _, n := range nodes {
		if err := r.tick(); err != nil {
			return ctlNone, err
		}

		ctl, err := r.execNode(n)
		if err != nil || ctl != ctlNone {
			return ctl, err
		}
	}

	return ctlNone, nil
}

func (r *vtlRun) execNode(n vnode) (ctlKind, error) {
	switch n := n.(type) {
	case vtext:
		r.out.WriteString(string(n))
	case *vrefNode:
		return ctlNone, r.execRef(n)
	case *vsetNode:
		return ctlNone, r.execSet(n)
	case *vifNode:
		return r.execIf(n)
	case *vforeachNode:
		return r.execForeach(n)
	case *vreturnNode:
		return r.execReturn(n)
	case *vbreakNode:
		return ctlBreak, nil
	case *vstopNode:
		return ctlStop, nil
	}

	return ctlNone, nil
}

func (r *vtlRun) execRef(n *vrefNode) error {
	v, err := r.evalRef(n.ref)
	if err != nil {
		return err
	}

	if v == nil {
		if !n.quiet {
			r.out.WriteString(n.raw)
		}

		return nil
	}

	r.out.WriteString(vrender(v))

	return nil
}

func (r *vtlRun) execReturn(n *vreturnNode) (ctlKind, error) {
	r.returned = true

	if n.val != nil {
		v, err := r.eval(n.val)
		if err != nil {
			return ctlNone, err
		}

		r.retVal = vexport(v)
	}

	return ctlReturn, nil
}

func (r *vtlRun) execIf(n *vifNode) (ctlKind, error) {
	for i, c := range n.conds {
		v, err := r.eval(c)
		if err != nil {
			return ctlNone, err
		}

		if vtruthy(v) {
			return r.execNodes(n.bodies[i])
		}
	}

	return r.execNodes(n.els)
}

func (r *vtlRun) execForeach(n *vforeachNode) (ctlKind, error) {
	it, err := r.eval(n.iter)
	if err != nil {
		return ctlNone, err
	}

	items := iterItems(it)
	prev := r.vars["foreach"]

	defer func() { r.vars["foreach"] = prev }()

	for i, item := range items {
		r.vars[n.name] = item
		r.vars["foreach"] = map[string]any{
			"index": int64(i), "count": int64(i + 1), "hasNext": i < len(items)-1,
			"first": i == 0, "last": i == len(items)-1,
		}

		ctl, bodyErr := r.execNodes(n.body)
		if bodyErr != nil {
			return ctlNone, bodyErr
		}

		switch ctl {
		case ctlBreak:
			return ctlNone, nil
		case ctlStop, ctlReturn:
			return ctl, nil
		case ctlNone:
		}
	}

	return ctlNone, nil
}

func iterItems(v any) []any {
	switch t := v.(type) {
	case *vlist:
		return append([]any(nil), t.items...)
	case map[string]any:
		out := make([]any, 0, len(t))
		for _, k := range sortedKeys(t) {
			out = append(out, t[k])
		}

		return out
	case nil:
		return nil
	}

	return nil
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys
}

func (r *vtlRun) execSet(n *vsetNode) error {
	v, err := r.eval(n.val)
	if err != nil {
		return err
	}

	if len(n.target.chain) == 0 {
		r.vars[n.target.name] = v

		return nil
	}

	base, err := r.evalChain(n.target, len(n.target.chain)-1)
	if err != nil {
		return err
	}

	last := n.target.chain[len(n.target.chain)-1]

	switch last.kind {
	case accProp:
		return assignKey(base, last.name, v)
	case accIndex:
		k, kerr := r.eval(last.idx)
		if kerr != nil {
			return kerr
		}

		return assignKey(base, k, v)
	case accMethod:
	}

	return fmt.Errorf("%w: cannot assign to a method call", ErrValidation)
}

func assignKey(base, key, v any) error {
	switch b := base.(type) {
	case map[string]any:
		b[vrender(key)] = v

		return nil
	case *vlist:
		i, ok := toInt(key)
		if !ok || i < 0 || i >= len(b.items) {
			return fmt.Errorf("%w: list index out of range", ErrValidation)
		}

		b.items[i] = v

		return nil
	}

	return nil
}

func (r *vtlRun) evalRef(ref *vref) (any, error) { return r.evalChain(ref, len(ref.chain)) }

func (r *vtlRun) evalChain(ref *vref, n int) (any, error) {
	cur, ok := r.vars[ref.name]
	if !ok {
		cur = r.root(ref.name)
	}

	for _, acc := range ref.chain[:n] {
		if cur == nil {
			return noValue(), nil
		}

		var err error

		if cur, err = r.access(cur, acc); err != nil {
			return nil, err
		}
	}

	return cur, nil
}

func (r *vtlRun) root(name string) any {
	switch name {
	case utilRoot:
		return vns(utilRoot)
	case "extensions":
		return vns("extensions")
	}

	return nil
}

func (r *vtlRun) access(cur any, acc vacc) (any, error) {
	switch acc.kind {
	case accProp:
		return r.getProp(cur, acc.name), nil
	case accIndex:
		k, err := r.eval(acc.idx)
		if err != nil {
			return nil, err
		}

		return getIndex(cur, k), nil
	default:
		args, err := r.evalArgs(acc.args)
		if err != nil {
			return nil, err
		}

		return r.callMethod(cur, acc.name, args)
	}
}

func (r *vtlRun) evalArgs(exprs []vexpr) ([]any, error) {
	args := make([]any, 0, len(exprs))

	for _, e := range exprs {
		v, err := r.eval(e)
		if err != nil {
			return nil, err
		}

		args = append(args, v)
	}

	return args, nil
}

func (r *vtlRun) getProp(cur any, name string) any {
	switch t := cur.(type) {
	case map[string]any:
		return t[name]
	case vns:
		if t == utilRoot && isUtilNamespace(name) {
			return vns(name)
		}
	}

	return nil
}

func getIndex(cur, key any) any {
	switch t := cur.(type) {
	case map[string]any:
		return t[vrender(key)]
	case *vlist:
		if i, ok := toInt(key); ok && i >= 0 && i < len(t.items) {
			return t.items[i]
		}
	}

	return nil
}

func (r *vtlRun) callMethod(cur any, name string, args []any) (any, error) {
	if ns, ok := cur.(vns); ok {
		key := name
		if ns != utilRoot {
			key = string(ns) + "." + name
		}

		out, err := r.util.call(key, vexportArgs(args))
		if err != nil {
			return nil, err
		}

		return vnorm(out), nil
	}

	return javaMethod(cur, name, args)
}

func vexportArgs(args []any) []any {
	out := make([]any, len(args))
	for i, a := range args {
		out[i] = vexport(a)
	}

	return out
}

func (r *vtlRun) eval(e vexpr) (any, error) {
	if err := r.tick(); err != nil {
		return nil, err
	}

	switch e := e.(type) {
	case *vlit:
		return e.v, nil
	case *vref:
		return r.evalRef(e)
	case *vinterp:
		return r.evalInterp(e)
	case *vlistEx:
		return r.evalListLit(e)
	case *vrange:
		return r.evalRange(e)
	case *vmapEx:
		return r.evalMapLit(e)
	case *vunary:
		return r.evalUnary(e)
	case *vbin:
		return r.evalBinary(e)
	}

	return nil, fmt.Errorf("%w: unsupported expression", ErrValidation)
}

func (r *vtlRun) evalInterp(e *vinterp) (any, error) {
	sub := &vtlRun{vars: r.vars, util: r.util, steps: r.steps}

	if _, err := sub.execNodes(e.parts); err != nil {
		return nil, err
	}

	r.steps = sub.steps

	return sub.out.String(), nil
}

func (r *vtlRun) evalListLit(e *vlistEx) (any, error) {
	items, err := r.evalArgs(e.items)
	if err != nil {
		return nil, err
	}

	return &vlist{items: items}, nil
}

func (r *vtlRun) evalRange(e *vrange) (any, error) {
	lo, err := r.eval(e.lo)
	if err != nil {
		return nil, err
	}

	hi, err := r.eval(e.hi)
	if err != nil {
		return nil, err
	}

	a, ok1 := toInt(lo)
	b, ok2 := toInt(hi)

	if !ok1 || !ok2 {
		return noValue(), nil
	}

	const maxRange = 100_000
	if b-a > maxRange || a-b > maxRange {
		return nil, fmt.Errorf("%w: range too large", ErrValidation)
	}

	out := &vlist{}

	if a <= b {
		for i := a; i <= b; i++ {
			out.items = append(out.items, int64(i))
		}
	} else {
		for i := a; i >= b; i-- {
			out.items = append(out.items, int64(i))
		}
	}

	return out, nil
}

func (r *vtlRun) evalMapLit(e *vmapEx) (any, error) {
	m := make(map[string]any, len(e.keys))

	for i := range e.keys {
		k, err := r.eval(e.keys[i])
		if err != nil {
			return nil, err
		}

		v, err := r.eval(e.vals[i])
		if err != nil {
			return nil, err
		}

		m[vrender(k)] = v
	}

	return m, nil
}

func (r *vtlRun) evalUnary(e *vunary) (any, error) {
	x, err := r.eval(e.x)
	if err != nil {
		return nil, err
	}

	if e.op == "!" {
		return !vtruthy(x), nil
	}

	f, isInt, ok := toNum(x)
	if !ok {
		return noValue(), nil
	}

	if isInt {
		return int64(-f), nil
	}

	return -f, nil
}

func (r *vtlRun) evalBinary(e *vbin) (any, error) {
	l, err := r.eval(e.l)
	if err != nil {
		return nil, err
	}

	switch e.op {
	case "&&":
		if !vtruthy(l) {
			return false, nil
		}

		return r.evalTruthy(e.r)
	case "||":
		if vtruthy(l) {
			return true, nil
		}

		return r.evalTruthy(e.r)
	}

	rv, err := r.eval(e.r)
	if err != nil {
		return nil, err
	}

	return binaryOp(e.op, l, rv), nil
}

func (r *vtlRun) evalTruthy(e vexpr) (any, error) {
	v, err := r.eval(e)

	return vtruthy(v), err
}

func binaryOp(op string, l, r any) any {
	switch op {
	case "==":
		return vequal(l, r)
	case "!=":
		return !vequal(l, r)
	case "<", ">", "<=", ">=":
		return vcompare(op, l, r)
	}

	return varith(op, l, r)
}

func vcompare(op string, l, r any) bool {
	a, _, ok1 := toNum(l)
	b, _, ok2 := toNum(r)

	var c int

	switch {
	case ok1 && ok2:
		c = cmpFloat(a, b)
	default:
		ls, lok := l.(string)
		rs, rok := r.(string)

		if !lok || !rok {
			return false
		}

		c = strings.Compare(ls, rs)
	}

	switch op {
	case "<":
		return c < 0
	case ">":
		return c > 0
	case "<=":
		return c <= 0
	}

	return c >= 0
}

func cmpFloat(a, b float64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}

	return 0
}

func varith(op string, l, r any) any {
	if op == "+" {
		if v, ok := plusNonNumeric(l, r); ok {
			return v
		}
	}

	a, aInt, ok1 := toNum(l)
	b, bInt, ok2 := toNum(r)

	if !ok1 || !ok2 {
		return noValue()
	}

	if aInt && bInt {
		return intArith(op, int64(a), int64(b))
	}

	return floatArith(op, a, b)
}

func noValue() any { return nil }

func plusNonNumeric(l, r any) (any, bool) {
	_, ls := l.(string)
	_, rs := r.(string)

	if ls || rs {
		return vrender(l) + vrender(r), true
	}

	if ll, ok := l.(*vlist); ok {
		if rl, isList := r.(*vlist); isList {
			return &vlist{items: append(append([]any(nil), ll.items...), rl.items...)}, true
		}
	}

	return noValue(), false
}

func floatArith(op string, a, b float64) any {
	switch op {
	case "+":
		return a + b
	case "-":
		return a - b
	case "*":
		return a * b
	case "/":
		if b != 0 {
			return a / b
		}
	case "%":
		if b != 0 {
			return math.Mod(a, b)
		}
	}

	return noValue()
}

func intArith(op string, a, b int64) any {
	switch op {
	case "+":
		return a + b
	case "-":
		return a - b
	case "*":
		return a * b
	case "/":
		if b != 0 {
			return a / b
		}
	case "%":
		if b != 0 {
			return a % b
		}
	}

	return noValue()
}

func vtruthy(v any) bool {
	switch t := v.(type) {
	case nil:
		return false
	case bool:
		return t
	}

	return true
}

func vequal(a, b any) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}

	x, _, ok1 := toNum(a)
	y, _, ok2 := toNum(b)

	if ok1 && ok2 {
		return x == y
	}

	if ab, ok := a.(bool); ok {
		bb, isBool := b.(bool)

		return isBool && ab == bb
	}

	return vrender(a) == vrender(b)
}

// toNum reports v as a float64, with isInt true when it is integral.
func toNum(v any) (float64, bool, bool) {
	switch t := v.(type) {
	case int64:
		return float64(t), true, true
	case int:
		return float64(t), true, true
	case float64:
		return t, false, true
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return float64(i), true, true
		}

		g, err := t.Float64()

		return g, false, err == nil
	}

	return 0, false, false
}

func toInt(v any) (int, bool) {
	f, _, ok := toNum(v)
	if !ok {
		if s, isStr := v.(string); isStr {
			if i, err := strconv.Atoi(s); err == nil {
				return i, true
			}
		}

		return 0, false
	}

	return int(f), true
}

func vrender(v any) string {
	switch t := v.(type) {
	case nil:
		return "null"
	case string:
		return t
	case bool:
		return strconv.FormatBool(t)
	case int64:
		return strconv.FormatInt(t, 10)
	case int:
		return strconv.Itoa(t)
	case float64:
		return renderFloat(t)
	case json.Number:
		return t.String()
	case *vlist:
		parts := make([]string, len(t.items))
		for i, it := range t.items {
			parts[i] = vrender(it)
		}

		return "[" + strings.Join(parts, ", ") + "]"
	case map[string]any:
		parts := make([]string, 0, len(t))
		for _, k := range sortedKeys(t) {
			parts = append(parts, k+"="+vrender(t[k]))
		}

		return "{" + strings.Join(parts, ", ") + "}"
	case vns:
		return string(t)
	}

	return fmt.Sprint(v)
}

func renderFloat(f float64) string {
	if f == math.Trunc(f) && math.Abs(f) < 1e15 {
		return strconv.FormatFloat(f, 'f', 1, 64)
	}

	return strconv.FormatFloat(f, 'g', -1, 64)
}

// vnorm deep-copies v into VTL values: lists become *vlist and integral
// numbers become int64.
func vnorm(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, x := range t {
			out[k] = vnorm(x)
		}

		return out
	case []any:
		items := make([]any, len(t))
		for i, x := range t {
			items[i] = vnorm(x)
		}

		return &vlist{items: items}
	case []string:
		items := make([]any, len(t))
		for i, x := range t {
			items[i] = x
		}

		return &vlist{items: items}
	case float64:
		if t == math.Trunc(t) && math.Abs(t) < 1e15 {
			return int64(t)
		}

		return t
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return i
		}

		f, _ := t.Float64()

		return f
	case int:
		return int64(t)
	}

	return v
}

// vexport deep-copies v back into plain JSON values.
func vexport(v any) any {
	switch t := v.(type) {
	case *vlist:
		items := make([]any, len(t.items))
		for i, x := range t.items {
			items[i] = vexport(x)
		}

		return items
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, x := range t {
			out[k] = vexport(x)
		}

		return out
	}

	return v
}
