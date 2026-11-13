package dmsengine

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"
	"unicode"
)

const (
	opWordInsert  = "INSERT"
	arityTwo      = 2
	arityThree    = 3
	blobRank      = 2
	decimalBase   = 10
	nanosPerMilli = 1_000_000
	daysInWeek    = 7
)

// ErrExpression reports an add-column expression the engine cannot evaluate.
var ErrExpression = fmt.Errorf("%w: unsupported expression", ErrMapping)

// EvalCtx is what an expression sees: the source row and the change header values.
type EvalCtx struct {
	Time     time.Time
	Now      time.Time
	Op       Op
	Position string
	Row      Row
	Full     bool
}

// Expr is a compiled SQLite-style expression from a transformation rule.
type Expr struct{ eval node }

type node func(*EvalCtx) any

// Eval evaluates the expression: nil, int64, float64, string or []byte.
func (e *Expr) Eval(c *EvalCtx) any { return e.eval(c) }

type tokKind int

const (
	tokEOF tokKind = iota
	tokNum
	tokStr
	tokIdent
	tokRef
	tokOp
)

type token struct {
	text string
	kind tokKind
}

func lex(src string) ([]token, error) {
	var out []token

	rs := []rune(src)

	for i := 0; i < len(rs); {
		r := rs[i]

		switch {
		case unicode.IsSpace(r):
			i++
		case r == '\'':
			s, n, err := lexString(rs[i:])
			if err != nil {
				return nil, err
			}

			out, i = append(out, token{s, tokStr}), i+n
		case unicode.IsDigit(r) || (r == '.' && i+1 < len(rs) && unicode.IsDigit(rs[i+1])):
			n := scanWhile(rs[i:], func(c rune) bool { return unicode.IsDigit(c) || c == '.' })
			out, i = append(out, token{string(rs[i : i+n]), tokNum}), i+n
		case r == '$':
			n := scanWhile(rs[i+1:], isIdentRune)
			if n == 0 {
				return nil, fmt.Errorf("%w: empty column reference", ErrExpression)
			}

			out, i = append(out, token{string(rs[i+1 : i+1+n]), tokRef}), i+1+n
		case isIdentRune(r):
			n := scanWhile(rs[i:], isIdentRune)
			out, i = append(out, token{string(rs[i : i+n]), tokIdent}), i+n
		default:
			op, n := lexOp(rs[i:])
			if n == 0 {
				return nil, fmt.Errorf("%w: unexpected character %q", ErrExpression, r)
			}

			out, i = append(out, token{op, tokOp}), i+n
		}
	}

	return append(out, token{kind: tokEOF}), nil
}

func isIdentRune(r rune) bool { return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' }

func scanWhile(rs []rune, f func(rune) bool) int {
	n := 0
	for n < len(rs) && f(rs[n]) {
		n++
	}

	return n
}

func lexString(rs []rune) (string, int, error) {
	var sb strings.Builder

	for i := 1; i < len(rs); i++ {
		if rs[i] != '\'' {
			sb.WriteRune(rs[i])

			continue
		}

		if i+1 < len(rs) && rs[i+1] == '\'' {
			sb.WriteRune('\'')

			i++

			continue
		}

		return sb.String(), i + 1, nil
	}

	return "", 0, fmt.Errorf("%w: unterminated string literal", ErrExpression)
}

func lexOp(rs []rune) (string, int) {
	for _, op := range []string{"||", "<>", "!=", "<=", ">=", "=="} {
		if strings.HasPrefix(string(rs), op) {
			return op, len(op)
		}
	}

	if strings.ContainsRune("+-*/%=<>(),", rs[0]) {
		return string(rs[0]), 1
	}

	return "", 0
}

type parser struct {
	table Table
	toks  []token
	pos   int
}

// CompileExpr compiles an expression against the source table whose columns $name refers to.
func CompileExpr(src string, t Table) (*Expr, error) {
	toks, err := lex(src)
	if err != nil {
		return nil, err
	}

	p := &parser{toks: toks, table: t}

	n, err := p.parseOr()
	if err != nil {
		return nil, err
	}

	if p.peek().kind != tokEOF {
		return nil, fmt.Errorf("%w: unexpected %q", ErrExpression, p.peek().text)
	}

	return &Expr{eval: n}, nil
}

func (p *parser) peek() token { return p.toks[p.pos] }

func (p *parser) next() token {
	t := p.toks[p.pos]
	if t.kind != tokEOF {
		p.pos++
	}

	return t
}

func (p *parser) isKeyword(kw string) bool {
	t := p.peek()

	return t.kind == tokIdent && strings.EqualFold(t.text, kw)
}

func (p *parser) acceptKeyword(kw string) bool {
	if p.isKeyword(kw) {
		p.pos++

		return true
	}

	return false
}

func (p *parser) acceptOp(ops ...string) (string, bool) {
	t := p.peek()
	if t.kind != tokOp {
		return "", false
	}

	for _, o := range ops {
		if t.text == o {
			p.pos++

			return o, true
		}
	}

	return "", false
}

func (p *parser) expectOp(op string) error {
	if _, ok := p.acceptOp(op); !ok {
		return fmt.Errorf("%w: expected %q, found %q", ErrExpression, op, p.peek().text)
	}

	return nil
}

func (p *parser) parseOr() (node, error) {
	l, err := p.parseAnd()
	if err != nil {
		return nil, err
	}

	for p.acceptKeyword("or") {
		r, rerr := p.parseAnd()
		if rerr != nil {
			return nil, rerr
		}

		l = logic(l, r, true)
	}

	return l, nil
}

func (p *parser) parseAnd() (node, error) {
	l, err := p.parseNot()
	if err != nil {
		return nil, err
	}

	for p.acceptKeyword("and") {
		r, rerr := p.parseNot()
		if rerr != nil {
			return nil, rerr
		}

		l = logic(l, r, false)
	}

	return l, nil
}

func (p *parser) parseNot() (node, error) {
	if !p.acceptKeyword("not") {
		return p.parseCompare()
	}

	n, err := p.parseNot()
	if err != nil {
		return nil, err
	}

	return func(c *EvalCtx) any {
		v := n(c)
		if v == nil {
			return nil
		}

		return boolInt(!truthy(v))
	}, nil
}

func (p *parser) parseCompare() (node, error) {
	l, err := p.parseAdd()
	if err != nil {
		return nil, err
	}

	for {
		if p.acceptKeyword("is") {
			l, err = p.parseIs(l)
			if err != nil {
				return nil, err
			}

			continue
		}

		op, ok := p.acceptOp("=", "==", "<>", "!=", "<", "<=", ">", ">=")
		if !ok {
			return l, nil
		}

		r, rerr := p.parseAdd()
		if rerr != nil {
			return nil, rerr
		}

		l = compareNode(op, l, r)
	}
}

func (p *parser) parseIs(l node) (node, error) {
	neg := p.acceptKeyword("not")

	if p.acceptKeyword("null") {
		return func(c *EvalCtx) any { return boolInt((l(c) == nil) != neg) }, nil
	}

	r, err := p.parseAdd()
	if err != nil {
		return nil, err
	}

	return func(c *EvalCtx) any {
		a, b := l(c), r(c)
		same := (a == nil && b == nil) || (a != nil && b != nil && compareValues(a, b) == 0)

		return boolInt(same != neg)
	}, nil
}

func (p *parser) parseAdd() (node, error) {
	l, err := p.parseMul()
	if err != nil {
		return nil, err
	}

	for {
		op, ok := p.acceptOp("+", "-")
		if !ok {
			return l, nil
		}

		r, rerr := p.parseMul()
		if rerr != nil {
			return nil, rerr
		}

		l = arith(op, l, r)
	}
}

func (p *parser) parseMul() (node, error) {
	l, err := p.parseConcat()
	if err != nil {
		return nil, err
	}

	for {
		op, ok := p.acceptOp("*", "/", "%")
		if !ok {
			return l, nil
		}

		r, rerr := p.parseConcat()
		if rerr != nil {
			return nil, rerr
		}

		l = arith(op, l, r)
	}
}

func (p *parser) parseConcat() (node, error) {
	l, err := p.parseUnary()
	if err != nil {
		return nil, err
	}

	for {
		if _, ok := p.acceptOp("||"); !ok {
			return l, nil
		}

		r, rerr := p.parseUnary()
		if rerr != nil {
			return nil, rerr
		}

		a, b := l, r
		l = func(c *EvalCtx) any {
			x, y := a(c), b(c)
			if x == nil || y == nil {
				return nil
			}

			return text(x) + text(y)
		}
	}
}

func (p *parser) parseUnary() (node, error) {
	if op, ok := p.acceptOp("-", "+"); ok {
		n, err := p.parseUnary()
		if err != nil {
			return nil, err
		}

		if op == "+" {
			return n, nil
		}

		return func(c *EvalCtx) any { return arithValues("*", int64(-1), n(c)) }, nil
	}

	return p.parsePrimary()
}

func (p *parser) parsePrimary() (node, error) {
	t := p.next()

	switch t.kind {
	case tokNum:
		return numLiteral(t.text)
	case tokStr:
		return func(*EvalCtx) any { return t.text }, nil
	case tokRef:
		return p.reference(t.text)
	case tokIdent:
		return p.identifier(t.text)
	case tokOp:
		if t.text == "(" {
			n, err := p.parseOr()
			if err != nil {
				return nil, err
			}

			return n, p.expectOp(")")
		}
	case tokEOF:
		return nil, fmt.Errorf("%w: unexpected end of expression", ErrExpression)
	}

	return nil, fmt.Errorf("%w: unexpected %q", ErrExpression, t.text)
}

func numLiteral(s string) (node, error) {
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return func(*EvalCtx) any { return i }, nil
	}

	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil, fmt.Errorf("%w: bad number %q", ErrExpression, s)
	}

	return func(*EvalCtx) any { return f }, nil
}

func (p *parser) identifier(name string) (node, error) {
	switch strings.ToLower(name) {
	case "null":
		return func(*EvalCtx) any { return nil }, nil
	case "case":
		return p.parseCase()
	}

	if _, ok := p.acceptOp("("); !ok {
		return nil, fmt.Errorf("%w: unexpected identifier %q", ErrExpression, name)
	}

	return p.call(strings.ToLower(name))
}

func (p *parser) call(name string) (node, error) {
	var args []node

	if _, ok := p.acceptOp(")"); !ok {
		for {
			a, err := p.parseOr()
			if err != nil {
				return nil, err
			}

			args = append(args, a)

			if _, more := p.acceptOp(","); !more {
				break
			}
		}

		if err := p.expectOp(")"); err != nil {
			return nil, err
		}
	}

	spec, ok := exprFuncs()[name]
	if !ok {
		return nil, fmt.Errorf("%w: function %s() is not supported", ErrExpression, name)
	}

	if len(args) < spec.min || (spec.max >= 0 && len(args) > spec.max) {
		return nil, fmt.Errorf("%w: wrong number of arguments to %s()", ErrExpression, name)
	}

	return func(c *EvalCtx) any {
		vals := make([]any, len(args))
		for i, a := range args {
			vals[i] = a(c)
		}

		return spec.fn(c, vals)
	}, nil
}

type caseArm struct{ when, then node }

func (p *parser) parseCase() (node, error) {
	var subject node

	if !p.isKeyword("when") {
		s, err := p.parseOr()
		if err != nil {
			return nil, err
		}

		subject = s
	}

	var arms []caseArm

	for p.acceptKeyword("when") {
		arm, err := p.parseArm()
		if err != nil {
			return nil, err
		}

		arms = append(arms, arm)
	}

	var els node

	if p.acceptKeyword("else") {
		e, err := p.parseOr()
		if err != nil {
			return nil, err
		}

		els = e
	}

	if len(arms) == 0 || !p.acceptKeyword("end") {
		return nil, fmt.Errorf("%w: malformed CASE expression", ErrExpression)
	}

	return func(c *EvalCtx) any { return evalCase(c, subject, arms, els) }, nil
}

func (p *parser) parseArm() (caseArm, error) {
	w, err := p.parseOr()
	if err != nil {
		return caseArm{}, err
	}

	if !p.acceptKeyword("then") {
		return caseArm{}, fmt.Errorf("%w: CASE WHEN needs THEN", ErrExpression)
	}

	th, err := p.parseOr()

	return caseArm{when: w, then: th}, err
}

func evalCase(c *EvalCtx, subject node, arms []caseArm, els node) any {
	var base any
	if subject != nil {
		base = subject(c)
	}

	for _, a := range arms {
		w := a.when(c)

		matched := w != nil && truthy(w)
		if subject != nil {
			matched = base != nil && w != nil && compareValues(base, w) == 0
		}

		if matched {
			return a.then(c)
		}
	}

	if els != nil {
		return els(c)
	}

	return nil
}

func (p *parser) reference(name string) (node, error) {
	if strings.HasPrefix(name, "AR_") {
		return p.header(name)
	}

	for i, c := range p.table.Columns {
		if c.Name == name {
			kind := c.Kind

			return func(ctx *EvalCtx) any {
				if i >= len(ctx.Row) {
					return nil
				}

				return fromText(ctx.Row[i], kind)
			}, nil
		}
	}

	return nil, fmt.Errorf("%w: column $%s does not exist in %s", ErrExpression, name, p.table.Key())
}

const headerTimeLayout = "2006-01-02 15:04:05.000000"

func (p *parser) header(name string) (node, error) {
	switch name {
	case "AR_H_STREAM_POSITION":
		return func(c *EvalCtx) any {
			if c.Full {
				return ""
			}

			return c.Position
		}, nil
	case "AR_H_TIMESTAMP", "AR_H_COMMIT_TIMESTAMP":
		return func(c *EvalCtx) any {
			if c.Full || c.Time.IsZero() {
				return c.Now.UTC().Format(headerTimeLayout)
			}

			return c.Time.UTC().Format(headerTimeLayout)
		}, nil
	case "AR_H_OPERATION":
		return func(c *EvalCtx) any {
			if c.Full {
				return opWordInsert
			}

			return strings.ToUpper(string(c.Op))
		}, nil
	case "AR_H_USER":
		return func(*EvalCtx) any { return "" }, nil
	case "AR_M_SOURCE_SCHEMA":
		s := p.table.Schema

		return func(*EvalCtx) any { return s }, nil
	case "AR_M_SOURCE_TABLE_NAME":
		s := p.table.Name

		return func(*EvalCtx) any { return s }, nil
	}

	return nil, fmt.Errorf("%w: $%s is not supported", ErrExpression, name)
}

func fromText(v any, k Kind) any {
	switch x := v.(type) {
	case nil:
		return nil
	case []byte:
		return x
	case string:
		switch k {
		case KindInt:
			if i, err := strconv.ParseInt(x, 10, 64); err == nil {
				return i
			}
		case KindDecimal, KindFloat:
			if f, err := strconv.ParseFloat(x, 64); err == nil {
				return f
			}
		case KindString, KindBool, KindTime, KindBytes, KindJSON:
		}

		return x
	}

	return fmt.Sprint(v)
}

func boolInt(b bool) any {
	if b {
		return int64(1)
	}

	return int64(0)
}

func truthy(v any) bool {
	switch x := toNumber(v).(type) {
	case int64:
		return x != 0
	case float64:
		return x != 0
	}

	return false
}

// toNumber follows SQLite's numeric prefix rule: text that is not a number reads as 0.
func toNumber(v any) any {
	switch x := v.(type) {
	case int64, float64:
		return x
	case []byte:
		return toNumber(string(x))
	case string:
		s := strings.TrimSpace(x)
		if i, err := strconv.ParseInt(s, 10, 64); err == nil {
			return i
		}

		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return f
		}

		return numericPrefix(s)
	}

	return int64(0)
}

func numericPrefix(s string) any {
	for end := len(s); end > 0; end-- {
		if f, err := strconv.ParseFloat(s[:end], 64); err == nil {
			if i, ierr := strconv.ParseInt(s[:end], 10, 64); ierr == nil {
				return i
			}

			return f
		}
	}

	return int64(0)
}

func asFloat(v any) float64 {
	switch x := toNumber(v).(type) {
	case int64:
		return float64(x)
	case float64:
		return x
	}

	return 0
}

const maxBlobBytes = 1 << 20

// asInt converts a SQL value to int, clamping to the int32 range (NaN is 0).
func asInt(v any) int {
	f := asFloat(v)

	switch {
	case math.IsNaN(f):
		return 0
	case f > math.MaxInt32:
		return math.MaxInt32
	case f < math.MinInt32:
		return math.MinInt32
	}

	return int(f)
}

func logic(l, r node, or bool) node {
	return func(c *EvalCtx) any {
		a, b := l(c), r(c)
		at, bt := a != nil && truthy(a), b != nil && truthy(b)

		switch {
		case or && (at || bt):
			return int64(1)
		case !or && ((a != nil && !at) || (b != nil && !bt)):
			return int64(0)
		case a == nil || b == nil:
			return nil
		case or:
			return int64(0)
		}

		return int64(1)
	}
}

// compareValues orders values SQLite-style: numbers before text before blobs.
func compareValues(a, b any) int {
	switch x := a.(type) {
	case string:
		if y, ok := b.(string); ok {
			return strings.Compare(x, y)
		}
	case []byte:
		if y, ok := b.([]byte); ok {
			return bytes.Compare(x, y)
		}
	default:
		if valueRank(b) == 0 {
			return compareNumbers(a, b)
		}
	}

	return valueRank(a) - valueRank(b)
}

func compareNumbers(a, b any) int {
	ia, aInt := a.(int64)
	ib, bInt := b.(int64)

	if aInt && bInt {
		return cmpOrdered(ia, ib)
	}

	return cmpOrdered(asFloat(a), asFloat(b))
}

func cmpOrdered[T int64 | float64](a, b T) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}

	return 0
}

func valueRank(v any) int {
	switch v.(type) {
	case string:
		return 1
	case []byte:
		return blobRank
	}

	return 0
}

func compareNode(op string, l, r node) node {
	return func(c *EvalCtx) any {
		a, b := l(c), r(c)
		if a == nil || b == nil {
			return nil
		}

		d := compareValues(a, b)

		switch op {
		case "=", "==":
			return boolInt(d == 0)
		case "<>", "!=":
			return boolInt(d != 0)
		case "<":
			return boolInt(d < 0)
		case "<=":
			return boolInt(d <= 0)
		case ">":
			return boolInt(d > 0)
		}

		return boolInt(d >= 0)
	}
}

func arith(op string, l, r node) node {
	return func(c *EvalCtx) any { return arithValues(op, l(c), r(c)) }
}

func arithValues(op string, a, b any) any {
	if a == nil || b == nil {
		return nil
	}

	na, nb := toNumber(a), toNumber(b)
	ia, aInt := na.(int64)
	ib, bInt := nb.(int64)

	if aInt && bInt {
		return intArith(op, ia, ib)
	}

	fa, fb := asFloat(na), asFloat(nb)

	switch op {
	case "+":
		return fa + fb
	case "-":
		return fa - fb
	case "*":
		return fa * fb
	case "/":
		if fb == 0 {
			return nil
		}

		return fa / fb
	}

	if fb == 0 {
		return nil
	}

	return math.Mod(fa, fb)
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
		if b == 0 {
			return nil
		}

		return a / b
	}

	if b == 0 {
		return nil
	}

	return a % b
}

type funcSpec struct {
	fn       func(*EvalCtx, []any) any
	min, max int
}

func exprFuncs() map[string]funcSpec {
	return map[string]funcSpec{
		"lower": {min: 1, max: 1, fn: func(_ *EvalCtx, a []any) any { return mapNil(a[0], asciiCase(false)) }},
		"upper": {min: 1, max: 1, fn: func(_ *EvalCtx, a []any) any { return mapNil(a[0], asciiCase(true)) }},
		"ltrim": {min: 1, max: arityTwo, fn: trimFn(true, false)},
		"rtrim": {min: 1, max: arityTwo, fn: trimFn(false, true)},
		"trim":  {min: 1, max: arityTwo, fn: trimFn(true, true)},
		"replace": {min: arityThree, max: arityThree, fn: func(_ *EvalCtx, a []any) any {
			if a[0] == nil || a[1] == nil || a[2] == nil {
				return nil
			}

			if text(a[1]) == "" {
				return text(a[0])
			}

			return strings.ReplaceAll(text(a[0]), text(a[1]), text(a[2]))
		}},
		"substr":   {min: arityTwo, max: arityThree, fn: substrFn},
		"hex":      {min: 1, max: 1, fn: hexFn},
		"abs":      {min: 1, max: 1, fn: absFn},
		"round":    {min: 1, max: arityTwo, fn: roundFn},
		"random":   {min: 0, max: 0, fn: randomInt},
		"max":      {min: arityTwo, max: -1, fn: extremum(1)},
		"min":      {min: arityTwo, max: -1, fn: extremum(-1)},
		"coalesce": {min: arityTwo, max: -1, fn: coalesceFn},
		"ifnull":   {min: arityTwo, max: arityTwo, fn: coalesceFn},
		"nullif": {min: arityTwo, max: arityTwo, fn: func(_ *EvalCtx, a []any) any {
			if a[0] != nil && a[1] != nil && compareValues(a[0], a[1]) == 0 {
				return nil
			}

			return a[0]
		}},
		"zeroblob": {min: 1, max: 1, fn: func(_ *EvalCtx, a []any) any {
			return make([]byte, min(max(0, asInt(a[0])), maxBlobBytes))
		}},
		"randomblob": {min: 1, max: 1, fn: randomBlob},
		"hash_sha256": {min: 1, max: 1, fn: func(_ *EvalCtx, a []any) any {
			if a[0] == nil {
				return nil
			}

			sum := sha256.Sum256([]byte(text(a[0])))

			return hex.EncodeToString(sum[:])
		}},
		"operation_indicator": {min: arityThree, max: arityThree, fn: operationIndicator},
		tDate:                 {min: 0, max: 1, fn: dateFn("2006-01-02")},
		tTime:                 {min: 0, max: 1, fn: dateFn("15:04:05")},
		"datetime":            {min: 0, max: 1, fn: dateFn("2006-01-02 15:04:05")},
		"julianday":           {min: 0, max: 1, fn: julianFn},
		"strftime":            {min: 1, max: arityTwo, fn: strftimeFn},
	}
}

func text(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case []byte:
		return string(x)
	case int64:
		return strconv.FormatInt(x, 10)
	case float64:
		return formatFloat(x)
	}

	return fmt.Sprint(v)
}

func formatFloat(f float64) string {
	if math.IsInf(f, 0) || math.IsNaN(f) {
		return ""
	}

	s := strconv.FormatFloat(f, 'g', 15, 64)
	if !strings.ContainsAny(s, ".eE") {
		s += ".0"
	}

	return s
}

func mapNil(v any, f func(string) string) any {
	if v == nil {
		return nil
	}

	return f(text(v))
}

func asciiCase(upper bool) func(string) string {
	return func(s string) string {
		b := []byte(s)
		for i, c := range b {
			switch {
			case upper && c >= 'a' && c <= 'z':
				b[i] = c - 'a' + 'A'
			case !upper && c >= 'A' && c <= 'Z':
				b[i] = c - 'A' + 'a'
			}
		}

		return string(b)
	}
}

func trimFn(left, right bool) func(*EvalCtx, []any) any {
	return func(_ *EvalCtx, a []any) any {
		if a[0] == nil {
			return nil
		}

		cut := " "
		if len(a) > 1 {
			if a[1] == nil {
				return nil
			}

			cut = text(a[1])
		}

		s := text(a[0])
		if left {
			s = strings.TrimLeft(s, cut)
		}

		if right {
			s = strings.TrimRight(s, cut)
		}

		return s
	}
}

func substrFn(_ *EvalCtx, a []any) any {
	if a[0] == nil || a[1] == nil || (len(a) == arityThree && a[2] == nil) {
		return nil
	}

	rs := []rune(text(a[0]))
	start := asInt(a[1])
	length := len(rs)

	if len(a) == arityThree {
		length = asInt(a[2])
	}

	if start < 0 {
		start = len(rs) + start + 1
	}

	from, to := start, start+length

	if length < 0 {
		from, to = start+length, start
	}

	from, to = max(from, 1), min(to, len(rs)+1)
	if from >= to {
		return ""
	}

	return string(rs[from-1 : to-1])
}

func hexFn(_ *EvalCtx, a []any) any {
	if a[0] == nil {
		return nil
	}

	return strings.ToUpper(hex.EncodeToString([]byte(text(a[0]))))
}

func absFn(_ *EvalCtx, a []any) any {
	if a[0] == nil {
		return nil
	}

	switch x := toNumber(a[0]).(type) {
	case int64:
		return max(x, -x)
	case float64:
		return math.Abs(x)
	}

	return float64(0)
}

func roundFn(_ *EvalCtx, a []any) any {
	if a[0] == nil {
		return nil
	}

	digits := 0
	if len(a) == arityTwo {
		digits = asInt(a[1])
	}

	scale := math.Pow(decimalBase, float64(digits))

	return math.Round(asFloat(a[0])*scale) / scale
}

func extremum(dir int) func(*EvalCtx, []any) any {
	return func(_ *EvalCtx, a []any) any {
		best := a[0]

		for _, v := range a {
			if v == nil {
				return nil
			}

			if compareValues(v, best)*dir > 0 {
				best = v
			}
		}

		return best
	}
}

func coalesceFn(_ *EvalCtx, a []any) any {
	for _, v := range a {
		if v != nil {
			return v
		}
	}

	return nil
}

func randomInt(*EvalCtx, []any) any {
	return int64(rand.Uint64()) //nolint:gosec // SQL random() is not a security primitive
}

func randomBlob(_ *EvalCtx, a []any) any {
	b := make([]byte, min(max(1, asInt(a[0])), maxBlobBytes))
	for i := range b {
		b[i] = byte(rand.UintN(256)) //nolint:gosec,mnd // SQL randomblob()
	}

	return b
}

func operationIndicator(c *EvalCtx, a []any) any {
	if c.Full {
		return a[2]
	}

	switch c.Op {
	case OpDelete:
		return a[0]
	case OpUpdate:
		return a[1]
	case OpInsert:
	}

	return a[2]
}

func parseTimeString(c *EvalCtx, v any) (time.Time, bool) {
	s := strings.TrimSpace(text(v))
	if strings.EqualFold(s, "now") {
		return c.Now.UTC(), true
	}

	for _, layout := range []string{
		"2006-01-02 15:04:05.999999999", "2006-01-02T15:04:05.999999999", "2006-01-02 15:04", "2006-01-02", "15:04:05",
	} {
		if t, err := time.ParseInLocation(layout, s, time.UTC); err == nil {
			return t, true
		}
	}

	return time.Time{}, false
}

func timeArg(c *EvalCtx, a []any) (time.Time, bool) {
	if len(a) == 0 {
		return c.Now.UTC(), true
	}

	if a[0] == nil {
		return time.Time{}, false
	}

	return parseTimeString(c, a[0])
}

func dateFn(layout string) func(*EvalCtx, []any) any {
	return func(c *EvalCtx, a []any) any {
		t, ok := timeArg(c, a)
		if !ok {
			return nil
		}

		return t.Format(layout)
	}
}

const (
	secondsPerDay  = 86400.0
	unixEpochJulis = 2440587.5
)

func julianFn(c *EvalCtx, a []any) any {
	t, ok := timeArg(c, a)
	if !ok {
		return nil
	}

	return float64(t.UnixNano())/1e9/secondsPerDay + unixEpochJulis
}

func strftimeFn(c *EvalCtx, a []any) any {
	if a[0] == nil {
		return nil
	}

	t, ok := timeArg(c, a[1:])
	if !ok {
		return nil
	}

	var sb strings.Builder

	f := []rune(text(a[0]))
	for i := 0; i < len(f); i++ {
		if f[i] != '%' || i+1 == len(f) {
			sb.WriteRune(f[i])

			continue
		}

		i++

		s, known := strftimeVar(f[i], t)
		if !known {
			return nil
		}

		sb.WriteString(s)
	}

	return sb.String()
}

func strftimeVar(v rune, t time.Time) (string, bool) {
	switch v {
	case 'd':
		return t.Format("02"), true
	case 'H':
		return t.Format("15"), true
	case 'f':
		return fmt.Sprintf("%02d.%03d", t.Second(), t.Nanosecond()/nanosPerMilli), true
	case 'j':
		return fmt.Sprintf("%03d", t.YearDay()), true
	case 'J':
		return formatFloat(float64(t.UnixNano())/1e9/secondsPerDay + unixEpochJulis), true
	case 'm':
		return t.Format("01"), true
	case 'M':
		return t.Format("04"), true
	case 's':
		return strconv.FormatInt(t.Unix(), 10), true
	case 'S':
		return t.Format("05"), true
	case 'w':
		return strconv.Itoa(int(t.Weekday())), true
	case 'W':
		return fmt.Sprintf("%02d", (t.YearDay()+daysInWeek-1-int(t.Weekday()))/daysInWeek), true
	case 'Y':
		return t.Format("2006"), true
	case '%':
		return "%", true
	}

	return "", false
}

// Cast renders an expression result as the text form of a column of DMS type d.
func (d DMSType) Cast(v any) any {
	if v == nil {
		return nil
	}

	switch d.Kind() {
	case KindInt:
		switch x := toNumber(v).(type) {
		case float64:
			return strconv.FormatInt(int64(x), 10)
		case int64:
			return strconv.FormatInt(x, 10)
		}
	case KindDecimal, KindFloat:
		f := asFloat(v)
		if i, ok := toNumber(v).(int64); ok {
			return strconv.FormatInt(i, 10)
		}

		return strconv.FormatFloat(f, 'f', -1, 64)
	case KindBool:
		return strconv.FormatBool(truthy(v))
	case KindBytes:
		if b, ok := v.([]byte); ok {
			return b
		}

		return []byte(text(v))
	case KindString, KindTime, KindJSON:
	}

	return text(v)
}
