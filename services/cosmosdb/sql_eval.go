package cosmosdb

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// undef is the Cosmos SQL "undefined" value (a missing property, an unbound parameter, a failed operation).
type undef struct{}

func isUndef(v any) bool {
	_, ok := v.(undef)

	return ok
}

// evalCtx carries per-query evaluation state. group is non-nil only while evaluating a GROUP BY output row.
type evalCtx struct {
	now      time.Time
	udf      *udfRuntime
	coll     *queryColl
	params   map[string]any
	regex    map[string]*regexp.Regexp
	stats    *retrievalStats
	group    [][]any
	geometry bool
	grouped  bool
}

func newEvalCtx(params []QueryParameter) *evalCtx {
	pv := make(map[string]any, len(params))
	for _, p := range params {
		pv[p.Name] = normalizeParam(p.Value)
	}

	return &evalCtx{
		params: pv, now: time.Now().UTC(), regex: make(map[string]*regexp.Regexp), udf: &udfRuntime{},
	}
}

// normalizeParam converts Go numeric kinds in a bound parameter to json.Number so comparisons stay exact.
func normalizeParam(v any) any {
	switch t := v.(type) {
	case float64, float32, int, int32, int64:
		n, _ := toNumber(t)

		return n
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = normalizeParam(e)
		}

		return out
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, e := range t {
			out[k] = normalizeParam(e)
		}

		return out
	default:
		return v
	}
}

// eval evaluates x and materializes a whole-row reference into a document map.
func (ec *evalCtx) eval(x sqlExpr, env []any) any {
	v := ec.evalRaw(x, env)
	if r, ok := v.(*sqlRow); ok {
		return r.full()
	}

	return v
}

func (ec *evalCtx) evalRaw(x sqlExpr, env []any) any {
	switch n := x.(type) {
	case *exprVar:
		if n.idx < len(env) {
			return env[n.idx]
		}

		return undef{}
	case *exprProp:
		return propOf(ec.evalRaw(n.base, env), n.key)
	case *exprIndex:
		return indexOf(ec.evalRaw(n.base, env), ec.eval(n.idx, env))
	default:
		return ec.evalNode(x, env)
	}
}

func propOf(base any, key string) any {
	switch t := base.(type) {
	case *sqlRow:
		if v, ok := t.lookup(key); ok {
			return v
		}
	case map[string]any:
		if v, ok := t[key]; ok {
			return v
		}
	}

	return undef{}
}

func indexOf(base, idx any) any {
	switch t := base.(type) {
	case []any:
		n, ok := toNumber(idx)
		if !ok {
			return undef{}
		}

		i, err := n.Int64()
		if err != nil || i < 0 || i >= int64(len(t)) {
			return undef{}
		}

		return t[i]
	case map[string]any, *sqlRow:
		if s, ok := idx.(string); ok {
			return propOf(t, s)
		}
	}

	return undef{}
}

func (ec *evalCtx) evalNode(x sqlExpr, env []any) any {
	switch n := x.(type) {
	case *exprLit:
		return n.v
	case *exprParam:
		if v, ok := ec.params[n.name]; ok {
			return v
		}

		return undef{}
	case *exprUnary:
		return ec.evalUnary(n, env)
	case *exprBinary:
		return ec.evalBinary(n, env)
	case *exprCall:
		return ec.evalCall(n, env)
	case *exprSubquery:
		return ec.evalSubquery(n, env)
	default:
		return ec.evalPredicate(x, env)
	}
}

func (ec *evalCtx) evalPredicate(x sqlExpr, env []any) any {
	switch n := x.(type) {
	case *exprIsNull:
		v := ec.eval(n.x, env)
		if isUndef(v) {
			return false
		}

		return (v == nil) != n.negate
	case *exprIn:
		return ec.evalIn(n, env)
	case *exprBetween:
		return ec.evalBetween(n, env)
	case *exprLike:
		return ec.evalLike(n, env)
	default:
		return ec.evalConstructor(x, env)
	}
}

func (ec *evalCtx) evalConstructor(x sqlExpr, env []any) any {
	switch n := x.(type) {
	case *exprTernary:
		switch valueTri(ec.eval(n.cond, env)) {
		case sqlTrue:
			return ec.eval(n.a, env)
		case sqlFalse:
			return ec.eval(n.b, env)
		default:
			return undef{}
		}
	case *exprArrayLit:
		out := make([]any, 0, len(n.items))

		for _, it := range n.items {
			if v := ec.eval(it, env); !isUndef(v) {
				out = append(out, v)
			}
		}

		return out
	case *exprObjectLit:
		out := make(map[string]any, len(n.keys))

		for i, k := range n.keys {
			if v := ec.eval(n.vals[i], env); !isUndef(v) {
				out[k] = v
			}
		}

		return out
	default:
		return undef{}
	}
}

func valueTri(v any) sqlTriState {
	if b, ok := v.(bool); ok {
		return boolToTri(b)
	}

	return sqlUndefined
}

func triValue(t sqlTriState) any {
	switch t {
	case sqlTrue:
		return true
	case sqlFalse:
		return false
	default:
		return undef{}
	}
}

func (ec *evalCtx) truth(x sqlExpr, env []any) sqlTriState { return valueTri(ec.eval(x, env)) }

func (ec *evalCtx) evalUnary(n *exprUnary, env []any) any {
	v := ec.eval(n.x, env)

	switch n.op {
	case sqlTokNot:
		return triValue(triNot(valueTri(v)))
	case sqlTokMinus:
		num, ok := toNumber(v)
		if !ok {
			return undef{}
		}

		return numArith(sqlTokMinus, json.Number("0"), num)
	default:
		if num, ok := toNumber(v); ok {
			return num
		}

		return undef{}
	}
}

func (ec *evalCtx) evalBinary(n *exprBinary, env []any) any {
	if v, ok := ec.evalLogical(n, env); ok {
		return v
	}

	l, r := ec.eval(n.l, env), ec.eval(n.r, env)
	if isUndef(l) || isUndef(r) {
		return undef{}
	}

	if isSQLCompareOp(n.op) {
		return compareValues(l, r, n.op)
	}

	if n.op == sqlTokConcat {
		ls, lok := l.(string)
		rs, rok := r.(string)

		if !lok || !rok {
			return undef{}
		}

		return ls + rs
	}

	ln, lok := toNumber(l)
	rn, rok := toNumber(r)

	if !lok || !rok {
		return undef{}
	}

	return numArith(n.op, ln, rn)
}

// evalLogical handles AND, OR and ?? (which short-circuit); ok is false for any other operator.
func (ec *evalCtx) evalLogical(n *exprBinary, env []any) (any, bool) {
	switch n.op {
	case sqlTokAnd:
		l := valueTri(ec.eval(n.l, env))
		if l == sqlFalse {
			return false, true
		}

		return triValue(triAnd(l, valueTri(ec.eval(n.r, env)))), true
	case sqlTokOr:
		l := valueTri(ec.eval(n.l, env))
		if l == sqlTrue {
			return true, true
		}

		return triValue(triOr(l, valueTri(ec.eval(n.r, env)))), true
	case sqlTokCoalesce:
		if l := ec.eval(n.l, env); !isUndef(l) {
			return l, true
		}

		return ec.eval(n.r, env), true
	default:
		return nil, false
	}
}

func (ec *evalCtx) evalIn(n *exprIn, env []any) any {
	v := ec.eval(n.x, env)
	if isUndef(v) {
		return undef{}
	}

	found := false

	for _, it := range n.list {
		if e := ec.eval(it, env); !isUndef(e) && compareValues(v, e, sqlTokEq) {
			found = true

			break
		}
	}

	return found != n.negate
}

func (ec *evalCtx) evalBetween(n *exprBetween, env []any) any {
	v, lo, hi := ec.eval(n.x, env), ec.eval(n.lo, env), ec.eval(n.hi, env)
	if isUndef(v) || isUndef(lo) || isUndef(hi) {
		return undef{}
	}

	in := compareValues(v, lo, sqlTokGte) && compareValues(v, hi, sqlTokLte)

	return in != n.negate
}

func (ec *evalCtx) evalLike(n *exprLike, env []any) any {
	v, pat := ec.eval(n.x, env), ec.eval(n.pattern, env)

	s, sok := v.(string)
	p, pok := pat.(string)

	if !sok || !pok {
		return undef{}
	}

	var esc rune

	if n.escape != "" {
		esc = []rune(n.escape)[0]
	}

	return likeMatch([]rune(s), []rune(p), esc) != n.negate
}

// likeMatch matches s against a SQL LIKE pattern ('%' any run, '_' any one rune, esc escapes).
func likeMatch(s, p []rune, esc rune) bool {
	if len(p) == 0 {
		return len(s) == 0
	}

	switch {
	case esc != 0 && p[0] == esc && len(p) > 1:
		return len(s) > 0 && s[0] == p[1] && likeMatch(s[1:], p[2:], esc)
	case p[0] == '%':
		for i := 0; i <= len(s); i++ {
			if likeMatch(s[i:], p[1:], esc) {
				return true
			}
		}

		return false
	case p[0] == '_':
		return len(s) > 0 && likeMatch(s[1:], p[1:], esc)
	default:
		return len(s) > 0 && s[0] == p[0] && likeMatch(s[1:], p[1:], esc)
	}
}

func (ec *evalCtx) evalCall(n *exprCall, env []any) any {
	if n.kind == fnAggregate {
		return ec.evalAggregate(n)
	}

	args := make([]any, len(n.args))
	for i, a := range n.args {
		args[i] = ec.eval(a, env)
	}

	switch n.kind {
	case fnDocumentID:
		return documentIDOf(n, env)
	case fnUDF:
		return ec.udf.call(n.name, args)
	case fnVector:
		return ec.evalVectorDistance(n, args)
	case fnFullText:
		return ec.evalFullText(n, args)
	case fnRank:
		return undef{}
	default:
	}

	return ec.callBuiltin(n.kind, n.name, args)
}

func (ec *evalCtx) evalSubquery(n *exprSubquery, env []any) any {
	sub := *ec
	sub.group, sub.grouped = nil, false

	rows, err := sub.runStmt(n.stmt, env, nil)
	if err != nil {
		return undef{}
	}

	switch n.kind {
	case subExists:
		return len(rows) > 0
	case subArray:
		return rows
	default:
		if len(rows) == 0 {
			return undef{}
		}

		return rows[0]
	}
}

// --- numbers ---

func toNumber(v any) (json.Number, bool) {
	switch t := v.(type) {
	case json.Number:
		return t, true
	case int:
		return json.Number(strconv.Itoa(t)), true
	case int32:
		return json.Number(strconv.FormatInt(int64(t), 10)), true
	case int64:
		return json.Number(strconv.FormatInt(t, 10)), true
	case float32:
		return formatFloatNumber(float64(t))
	case float64:
		return formatFloatNumber(t)
	default:
		return "", false
	}
}

func formatFloatNumber(f float64) (json.Number, bool) {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return "", false
	}

	format := byte('f')
	if abs := math.Abs(f); abs != 0 && (abs < 1e-6 || abs >= 1e21) {
		format = 'e'
	}

	return json.Number(strconv.FormatFloat(f, format, -1, 64)), true
}

func numFloat(n json.Number) float64 {
	f, _ := n.Float64()

	return f
}

func floatResult(f float64) any {
	n, ok := formatFloatNumber(f)
	if !ok {
		return undef{}
	}

	return n
}

// numArith applies + - * / % to a and b, staying in int64 when exact and falling back to float64.
func numArith(op sqlTokenType, a, b json.Number) any {
	ai, aErr := a.Int64()
	bi, bErr := b.Int64()

	if aErr == nil && bErr == nil {
		if v, ok := intArith(op, ai, bi); ok {
			return v
		}
	}

	af, bf := numFloat(a), numFloat(b)

	switch op {
	case sqlTokPlus:
		return floatResult(af + bf)
	case sqlTokMinus:
		return floatResult(af - bf)
	case sqlTokStar:
		return floatResult(af * bf)
	case sqlTokSlash:
		if bf == 0 {
			return undef{}
		}

		return floatResult(af / bf)
	case sqlTokPercent:
		if bf == 0 {
			return undef{}
		}

		return floatResult(math.Mod(af, bf))
	default:
		return undef{}
	}
}

func intArith(op sqlTokenType, a, b int64) (any, bool) {
	var (
		v  int64
		ok bool
	)

	switch op {
	case sqlTokPlus:
		v, ok = intAdd(a, b)
	case sqlTokMinus:
		v, ok = intSub(a, b)
	case sqlTokStar:
		v, ok = intMul(a, b)
	case sqlTokSlash:
		v, ok = intDiv(a, b)
	case sqlTokPercent:
		v, ok = intRem(a, b)
	default:
		return nil, false
	}

	if !ok {
		return nil, false
	}

	return json.Number(strconv.FormatInt(v, 10)), true
}

func intAdd(a, b int64) (int64, bool) {
	s := a + b

	return s, (s > a) == (b > 0)
}

func intSub(a, b int64) (int64, bool) {
	d := a - b

	return d, (d < a) == (b > 0)
}

func intMul(a, b int64) (int64, bool) {
	if a == 0 || b == 0 {
		return 0, true
	}

	m := a * b
	overflowsNeg := (a == math.MinInt64 && b == -1) || (b == math.MinInt64 && a == -1)

	return m, m/b == a && !overflowsNeg
}

func intDiv(a, b int64) (int64, bool) {
	if b == 0 || a%b != 0 || (a == math.MinInt64 && b == -1) {
		return 0, false
	}

	return a / b, true
}

func intRem(a, b int64) (int64, bool) {
	if b == 0 || b == -1 {
		return 0, false
	}

	return a % b, true
}

// --- comparison ---

// compareValues applies op to two defined values. A type mismatch is false (true for !=).
func compareValues(left, right any, op sqlTokenType) bool {
	if left == nil || right == nil {
		return compareNullOperand(left, right, op)
	}

	if ln, ok := toNumber(left); ok {
		rn, rok := toNumber(right)
		if !rok {
			return op == sqlTokNeq
		}

		return applySQLCompare(compareJSONNumbers(ln, rn), op)
	}

	switch lv := left.(type) {
	case string:
		rv, ok := right.(string)
		if !ok {
			return op == sqlTokNeq
		}

		return applySQLCompare(strings.Compare(lv, rv), op)
	case bool:
		rv, ok := right.(bool)
		if !ok {
			return op == sqlTokNeq
		}

		if op != sqlTokEq && op != sqlTokNeq {
			return false
		}

		return applySQLCompare(cmpBool(lv, rv), op)
	default:
		switch op {
		case sqlTokEq:
			return deepEqual(left, right)
		case sqlTokNeq:
			return !deepEqual(left, right)
		default:
			return false
		}
	}
}

func compareNullOperand(left, right any, op sqlTokenType) bool {
	if op != sqlTokEq && op != sqlTokNeq {
		return false
	}

	eq := left == nil && right == nil
	if op == sqlTokEq {
		return eq
	}

	return !eq
}

func deepEqual(a, b any) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}

	if an, ok := toNumber(a); ok {
		bn, bok := toNumber(b)

		return bok && compareJSONNumbers(an, bn) == 0
	}

	switch av := a.(type) {
	case string:
		bv, ok := b.(string)

		return ok && av == bv
	case bool:
		bv, ok := b.(bool)

		return ok && av == bv
	case []any:
		bv, ok := b.([]any)

		return ok && arraysEqual(av, bv)
	case map[string]any:
		bv, ok := b.(map[string]any)

		return ok && objectsEqual(av, bv)
	default:
		return false
	}
}

func arraysEqual(a, b []any) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if !deepEqual(a[i], b[i]) {
			return false
		}
	}

	return true
}

func objectsEqual(a, b map[string]any) bool {
	if len(a) != len(b) {
		return false
	}

	for k, e := range a {
		o, exists := b[k]
		if !exists || !deepEqual(e, o) {
			return false
		}
	}

	return true
}

// compareJSONNumbers compares as int64 when both are exact integers, else as float64.
func compareJSONNumbers(a, b json.Number) int {
	ai, aErr := a.Int64()
	bi, bErr := b.Int64()

	if aErr == nil && bErr == nil {
		switch {
		case ai < bi:
			return -1
		case ai > bi:
			return 1
		default:
			return 0
		}
	}

	af, _ := a.Float64()
	bf, _ := b.Float64()

	switch {
	case af < bf:
		return -1
	case af > bf:
		return 1
	default:
		return 0
	}
}

func applySQLCompare(cmp int, op sqlTokenType) bool {
	switch op {
	case sqlTokEq:
		return cmp == 0
	case sqlTokNeq:
		return cmp != 0
	case sqlTokLt:
		return cmp < 0
	case sqlTokLte:
		return cmp <= 0
	case sqlTokGt:
		return cmp > 0
	case sqlTokGte:
		return cmp >= 0
	default:
		return false
	}
}

func cmpBool(a, b bool) int {
	switch {
	case a == b:
		return 0
	case !a && b:
		return -1
	default:
		return 1
	}
}

const (
	rankUndefined = iota
	rankNull
	rankBool
	rankNumber
	rankString
	rankArray
	rankObject
)

// typeRank orders value types as Cosmos does for ORDER BY and MIN/MAX.
func typeRank(v any) int {
	if _, ok := toNumber(v); ok {
		return rankNumber
	}

	switch v.(type) {
	case undef:
		return rankUndefined
	case nil:
		return rankNull
	case bool:
		return rankBool
	case string:
		return rankString
	case []any:
		return rankArray
	default:
		return rankObject
	}
}

// compareTotal is a total order over all JSON values (undefined < null < bool < number < string < array < object).
func compareTotal(a, b any) int {
	ra, rb := typeRank(a), typeRank(b)
	if ra != rb {
		return ra - rb
	}

	switch ra {
	case rankBool:
		ab, _ := a.(bool)
		bb, _ := b.(bool)

		return cmpBool(ab, bb)
	case rankNumber:
		an, _ := toNumber(a)
		bn, _ := toNumber(b)

		return compareJSONNumbers(an, bn)
	case rankString:
		as, _ := a.(string)
		bs, _ := b.(string)

		return strings.Compare(as, bs)
	case rankArray:
		aa, _ := a.([]any)
		ba, _ := b.([]any)

		return compareArrays(aa, ba)
	case rankObject:
		return strings.Compare(canonicalJSON(a), canonicalJSON(b))
	default:
		return 0
	}
}

func compareArrays(a, b []any) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		if c := compareTotal(a[i], b[i]); c != 0 {
			return c
		}
	}

	return len(a) - len(b)
}

func canonicalJSON(v any) string {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}

	return string(data)
}

// sortOutputs stable-sorts outs by their precomputed order keys.
func sortOutputs(outs []outRow, orderBy []sqlOrderKey) {
	sort.SliceStable(outs, func(i, j int) bool {
		for k, key := range orderBy {
			c := compareTotal(outs[i].keys[k], outs[j].keys[k])
			if c == 0 {
				continue
			}

			if key.desc {
				return c > 0
			}

			return c < 0
		}

		return false
	})
}
