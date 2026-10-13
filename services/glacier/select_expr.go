package glacier

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

type selValKind int

const (
	selNull selValKind = iota
	selStr
	selNum
	selBool
)

// selVal is a scalar produced while evaluating a select expression. CSV fields are
// strings; numbers arise from literals, arithmetic and CAST.
type selVal struct {
	s    string
	f    float64
	kind selValKind
	b    bool
}

func selString(s string) selVal  { return selVal{kind: selStr, s: s} }
func selNumber(f float64) selVal { return selVal{kind: selNum, f: f} }
func selBoolean(b bool) selVal   { return selVal{kind: selBool, b: b} }

// text renders v as a CSV output field; null renders empty.
func (v selVal) text() string {
	switch v.kind {
	case selStr:
		return v.s
	case selNum:
		return strconv.FormatFloat(v.f, 'f', -1, 64)
	case selBool:
		return strconv.FormatBool(v.b)
	default:
		return ""
	}
}

// number coerces v to a number: numbers as-is, strings that parse as one.
func (v selVal) number() (float64, bool) {
	switch v.kind {
	case selNum:
		return v.f, true
	case selStr:
		f, err := strconv.ParseFloat(strings.TrimSpace(v.s), 64)

		return f, err == nil
	default:
		return 0, false
	}
}

func (v selVal) truth() (bool, bool) {
	if v.kind == selBool {
		return v.b, true
	}

	return false, false
}

type selRow struct {
	header map[string]int
	row    []string
}

type selectExpr interface {
	eval(r selRow) selVal
	isBool() bool
}

type (
	selLiteral struct{ v selVal }
	selColumn  struct{ ref string }
	selNeg     struct{ x selectExpr }
	selNot     struct{ x selectExpr }
	selArith   struct {
		l, r selectExpr
		op   byte
	}
	selCompare struct {
		l, r selectExpr
		op   string
	}
	selLogic struct {
		l, r selectExpr
		and  bool
	}
	selBetween struct {
		x, lo, hi selectExpr
		negate    bool
	}
	selIn struct {
		x      selectExpr
		list   []selectExpr
		negate bool
	}
	selLike struct {
		x, pattern, escape selectExpr
		negate             bool
	}
	selCast struct {
		x      selectExpr
		target string
	}
	selCoalesce struct{ args []selectExpr }
	selNullIf   struct{ a, b selectExpr }
)

func (selLiteral) isBool() bool  { return false }
func (selColumn) isBool() bool   { return false }
func (selNeg) isBool() bool      { return false }
func (selNot) isBool() bool      { return true }
func (selArith) isBool() bool    { return false }
func (selCompare) isBool() bool  { return true }
func (selLogic) isBool() bool    { return true }
func (selBetween) isBool() bool  { return true }
func (selIn) isBool() bool       { return true }
func (selLike) isBool() bool     { return true }
func (selCast) isBool() bool     { return false }
func (selCoalesce) isBool() bool { return false }
func (selNullIf) isBool() bool   { return false }

func (e selLiteral) eval(selRow) selVal { return e.v }

func (e selColumn) eval(r selRow) selVal {
	if s, ok := resolveSelectField(e.ref, r.row, r.header); ok {
		return selString(s)
	}

	return selVal{}
}

func (e selNeg) eval(r selRow) selVal {
	f, ok := e.x.eval(r).number()
	if !ok {
		return selVal{}
	}

	return selNumber(-f)
}

func (e selNot) eval(r selRow) selVal {
	b, ok := e.x.eval(r).truth()
	if !ok {
		return selVal{}
	}

	return selBoolean(!b)
}

func (e selArith) eval(r selRow) selVal {
	l, lok := e.l.eval(r).number()
	rv, rok := e.r.eval(r).number()

	if !lok || !rok {
		return selVal{}
	}

	switch e.op {
	case '+':
		return selNumber(l + rv)
	case '-':
		return selNumber(l - rv)
	case '*':
		return selNumber(l * rv)
	case '/':
		if rv == 0 {
			return selVal{}
		}

		return selNumber(l / rv)
	case '%':
		if rv == 0 {
			return selVal{}
		}

		return selNumber(math.Mod(l, rv))
	}

	return selVal{}
}

// compareSelVals orders a and b: numerically when both coerce to numbers, otherwise
// lexically on their text. ok is false when either side is null.
func compareSelVals(a, b selVal) (int, bool) {
	if a.kind == selNull || b.kind == selNull {
		return 0, false
	}

	if af, aok := a.number(); aok {
		if bf, bok := b.number(); bok {
			return numCompare(af, bf), true
		}
	}

	return strings.Compare(a.text(), b.text()), true
}

func (e selCompare) eval(r selRow) selVal {
	cmp, ok := compareSelVals(e.l.eval(r), e.r.eval(r))
	if !ok {
		return selVal{}
	}

	return selBoolean(compareSelectOrdered(e.op, cmp))
}

func (e selLogic) eval(r selRow) selVal {
	l, lok := e.l.eval(r).truth()
	rv, rok := e.r.eval(r).truth()

	if e.and {
		switch {
		case lok && !l, rok && !rv:
			return selBoolean(false)
		case lok && rok:
			return selBoolean(true)
		}

		return selVal{}
	}

	switch {
	case lok && l, rok && rv:
		return selBoolean(true)
	case lok && rok:
		return selBoolean(false)
	}

	return selVal{}
}

func (e selBetween) eval(r selRow) selVal {
	x := e.x.eval(r)
	lo, lok := compareSelVals(x, e.lo.eval(r))
	hi, hok := compareSelVals(x, e.hi.eval(r))

	if !lok || !hok {
		return selVal{}
	}

	return selBoolean((lo >= 0 && hi <= 0) != e.negate)
}

func (e selIn) eval(r selRow) selVal {
	x := e.x.eval(r)
	if x.kind == selNull {
		return selVal{}
	}

	found, sawNull := false, false

	for _, item := range e.list {
		cmp, ok := compareSelVals(x, item.eval(r))
		if !ok {
			sawNull = true

			continue
		}

		if cmp == 0 {
			found = true

			break
		}
	}

	if !found && sawNull {
		return selVal{}
	}

	return selBoolean(found != e.negate)
}

func (e selLike) eval(r selRow) selVal {
	x, p := e.x.eval(r), e.pattern.eval(r)
	if x.kind == selNull || p.kind == selNull {
		return selVal{}
	}

	esc := rune(-1)

	if e.escape != nil {
		ev := e.escape.eval(r).text()
		if rs := []rune(ev); len(rs) == 1 {
			esc = rs[0]
		}
	}

	return selBoolean(likeMatch([]rune(x.text()), []rune(p.text()), esc) != e.negate)
}

// likeMatch reports whether s matches the SQL LIKE pattern p ('%' any run, '_' any one
// rune, esc escapes the next pattern rune).
func likeMatch(s, p []rune, esc rune) bool {
	for len(p) > 0 {
		if p[0] == '%' {
			return likeMatchAnyRun(s, p[1:], esc)
		}

		var ok bool

		if s, p, ok = likeStep(s, p, esc); !ok {
			return false
		}
	}

	return len(s) == 0
}

func likeMatchAnyRun(s, rest []rune, esc rune) bool {
	for i := 0; i <= len(s); i++ {
		if likeMatch(s[i:], rest, esc) {
			return true
		}
	}

	return false
}

// likeStep consumes one literal, escaped or '_' pattern element against s.
func likeStep(s, p []rune, esc rune) ([]rune, []rune, bool) {
	if len(s) == 0 {
		return s, p, false
	}

	switch {
	case p[0] == esc && len(p) > 1:
		return s[1:], p[2:], s[0] == p[1]
	case p[0] == '_':
		return s[1:], p[1:], true
	default:
		return s[1:], p[1:], s[0] == p[0]
	}
}

func (e selCast) eval(r selRow) selVal {
	v := e.x.eval(r)
	if v.kind == selNull {
		return v
	}

	switch e.target {
	case "INT", "INTEGER":
		f, ok := v.number()
		if !ok || math.IsInf(f, 0) || math.IsNaN(f) {
			return selVal{}
		}

		return selNumber(math.Trunc(f))
	case "FLOAT", "DECIMAL", "NUMERIC":
		f, ok := v.number()
		if !ok {
			return selVal{}
		}

		return selNumber(f)
	case "STRING":
		return selString(v.text())
	case "BOOL", "BOOLEAN":
		for _, b := range []bool{true, false} {
			if strings.EqualFold(v.text(), strconv.FormatBool(b)) {
				return selBoolean(b)
			}
		}
	}

	return selVal{}
}

func (e selCoalesce) eval(r selRow) selVal {
	for _, a := range e.args {
		if v := a.eval(r); v.kind != selNull {
			return v
		}
	}

	return selVal{}
}

func (e selNullIf) eval(r selRow) selVal {
	a, b := e.a.eval(r), e.b.eval(r)
	if cmp, ok := compareSelVals(a, b); ok && cmp == 0 {
		return selVal{}
	}

	return a
}

// selectConditionMatches reports whether a WHERE condition holds for a CSV row; a nil
// condition (no WHERE clause) always matches.
func selectConditionMatches(cond selectExpr, row []string, header map[string]int) bool {
	if cond == nil {
		return true
	}

	b, ok := cond.eval(selRow{row: row, header: header}).truth()

	return ok && b
}

// numCompare returns -1/0/1 mirroring strings.Compare's contract, for a float pair.
func numCompare(a, b float64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

// compareSelectOrdered applies a comparison operator to a three-way compare result.
func compareSelectOrdered(op string, cmp int) bool {
	switch op {
	case "=":
		return cmp == 0
	case "!=", "<>":
		return cmp != 0
	case "<":
		return cmp < 0
	case "<=":
		return cmp <= 0
	case ">":
		return cmp > 0
	case ">=":
		return cmp >= 0
	default:
		return false
	}
}

// isSelectSQLKeyword reports whether s is a reserved word -- used to avoid consuming
// e.g. "WHERE" as a table alias.
func isSelectSQLKeyword(s string) bool {
	switch strings.ToUpper(s) {
	case "SELECT", "FROM", "WHERE", "AND", "OR", "AS", "LIMIT", "NOT", "BETWEEN", "IN", "LIKE", "ESCAPE":
		return true
	default:
		return false
	}
}

func isSelectCompareOp(s string) bool {
	switch s {
	case "=", "!=", "<>", "<", "<=", ">", ">=":
		return true
	default:
		return false
	}
}

func (p *selectSQLParser) peekPunct(v string) bool {
	t, ok := p.peek()

	return ok && t.typ == sqlTokPunct && t.val == v
}

func (p *selectSQLParser) expectPunct(v string) error {
	if !p.peekPunct(v) {
		return fmt.Errorf("%w: expected %q", ErrSelectExpression, v)
	}

	p.next()

	return nil
}

// parseExpr parses an expression: OR < AND < NOT < predicate < additive < multiplicative < unary.
func (p *selectSQLParser) parseExpr() (selectExpr, error) {
	return p.parseOr()
}

func (p *selectSQLParser) parseOr() (selectExpr, error) {
	l, err := p.parseAnd()
	if err != nil {
		return nil, err
	}

	for p.peekKeyword("OR") {
		p.next()

		r, rerr := p.parseAnd()
		if rerr != nil {
			return nil, rerr
		}

		l = selLogic{l: l, r: r}
	}

	return l, nil
}

func (p *selectSQLParser) parseAnd() (selectExpr, error) {
	l, err := p.parseNot()
	if err != nil {
		return nil, err
	}

	for p.peekKeyword("AND") {
		p.next()

		r, rerr := p.parseNot()
		if rerr != nil {
			return nil, rerr
		}

		l = selLogic{l: l, r: r, and: true}
	}

	return l, nil
}

func (p *selectSQLParser) parseNot() (selectExpr, error) {
	if p.peekKeyword("NOT") {
		p.next()

		x, err := p.parseNot()
		if err != nil {
			return nil, err
		}

		return selNot{x: x}, nil
	}

	return p.parsePredicate()
}

func (p *selectSQLParser) parsePredicate() (selectExpr, error) {
	l, err := p.parseAdditive()
	if err != nil {
		return nil, err
	}

	if t, ok := p.peek(); ok && t.typ == sqlTokPunct && isSelectCompareOp(t.val) {
		p.next()

		r, rerr := p.parseAdditive()
		if rerr != nil {
			return nil, rerr
		}

		return selCompare{l: l, r: r, op: t.val}, nil
	}

	negate := false

	if p.peekKeyword("NOT") {
		p.next()

		negate = true
	}

	switch {
	case p.peekKeyword("BETWEEN"):
		p.next()

		return p.parseBetween(l, negate)
	case p.peekKeyword("IN"):
		p.next()

		return p.parseIn(l, negate)
	case p.peekKeyword("LIKE"):
		p.next()

		return p.parseLike(l, negate)
	}

	if negate {
		return nil, fmt.Errorf("%w: expected BETWEEN, IN or LIKE after NOT", ErrSelectExpression)
	}

	return l, nil
}

func (p *selectSQLParser) parseBetween(x selectExpr, negate bool) (selectExpr, error) {
	lo, err := p.parseAdditive()
	if err != nil {
		return nil, err
	}

	if err = p.expectKeyword("AND"); err != nil {
		return nil, err
	}

	hi, err := p.parseAdditive()
	if err != nil {
		return nil, err
	}

	return selBetween{x: x, lo: lo, hi: hi, negate: negate}, nil
}

func (p *selectSQLParser) parseIn(x selectExpr, negate bool) (selectExpr, error) {
	if err := p.expectPunct("("); err != nil {
		return nil, err
	}

	list, err := p.parseExprList()
	if err != nil {
		return nil, err
	}

	return selIn{x: x, list: list, negate: negate}, nil
}

// parseExprList parses "expr (',' expr)* ')'" -- the opening paren is already consumed.
func (p *selectSQLParser) parseExprList() ([]selectExpr, error) {
	var list []selectExpr

	for {
		e, err := p.parseAdditive()
		if err != nil {
			return nil, err
		}

		list = append(list, e)

		if !p.peekPunct(",") {
			break
		}

		p.next()
	}

	if err := p.expectPunct(")"); err != nil {
		return nil, err
	}

	return list, nil
}

func (p *selectSQLParser) parseLike(x selectExpr, negate bool) (selectExpr, error) {
	pattern, err := p.parseAdditive()
	if err != nil {
		return nil, err
	}

	like := selLike{x: x, pattern: pattern, negate: negate}

	if p.peekKeyword("ESCAPE") {
		p.next()

		if like.escape, err = p.parseAdditive(); err != nil {
			return nil, err
		}
	}

	return like, nil
}

func (p *selectSQLParser) parseAdditive() (selectExpr, error) {
	l, err := p.parseMultiplicative()
	if err != nil {
		return nil, err
	}

	for p.peekPunct("+") || p.peekPunct("-") {
		op, _ := p.next()

		r, rerr := p.parseMultiplicative()
		if rerr != nil {
			return nil, rerr
		}

		l = selArith{l: l, r: r, op: op.val[0]}
	}

	return l, nil
}

func (p *selectSQLParser) parseMultiplicative() (selectExpr, error) {
	l, err := p.parseUnary()
	if err != nil {
		return nil, err
	}

	for p.peekPunct("*") || p.peekPunct("/") || p.peekPunct("%") {
		op, _ := p.next()

		r, rerr := p.parseUnary()
		if rerr != nil {
			return nil, rerr
		}

		l = selArith{l: l, r: r, op: op.val[0]}
	}

	return l, nil
}

func (p *selectSQLParser) parseUnary() (selectExpr, error) {
	if p.peekPunct("-") {
		p.next()

		x, err := p.parseUnary()
		if err != nil {
			return nil, err
		}

		return selNeg{x: x}, nil
	}

	if p.peekPunct("+") {
		p.next()

		return p.parseUnary()
	}

	return p.parsePrimary()
}

func (p *selectSQLParser) parsePrimary() (selectExpr, error) {
	t, ok := p.next()
	if !ok {
		return nil, fmt.Errorf("%w: unexpected end of expression", ErrSelectExpression)
	}

	switch t.typ {
	case sqlTokString:
		return selLiteral{v: selString(t.val)}, nil
	case sqlTokNumber:
		f, err := strconv.ParseFloat(t.val, 64)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid number %q", ErrSelectExpression, t.val)
		}

		return selLiteral{v: selNumber(f)}, nil
	case sqlTokPunct:
		if t.val != "(" {
			return nil, fmt.Errorf("%w: unexpected %q", ErrSelectExpression, t.val)
		}

		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}

		if err = p.expectPunct(")"); err != nil {
			return nil, err
		}

		return e, nil
	case sqlTokIdent:
		return p.parseIdentExpr(t)
	}

	return nil, fmt.Errorf("%w: unexpected token", ErrSelectExpression)
}

func (p *selectSQLParser) parseIdentExpr(t sqlTok) (selectExpr, error) {
	if p.peekPunct("(") {
		return p.parseFunction(strings.ToUpper(t.val))
	}

	if isSelectSQLKeyword(t.val) {
		return nil, fmt.Errorf("%w: unexpected keyword %s", ErrSelectExpression, t.val)
	}

	ref := t.val

	if p.peekPunct(".") {
		p.next()

		name, ok := p.next()
		if !ok || name.typ != sqlTokIdent {
			return nil, fmt.Errorf("%w: expected identifier after '.'", ErrSelectExpression)
		}

		ref = name.val
	}

	return selColumn{ref: ref}, nil
}

func (p *selectSQLParser) parseFunction(name string) (selectExpr, error) {
	p.next()

	switch name {
	case "CAST":
		return p.parseCast()
	case "COALESCE":
		args, err := p.parseExprList()
		if err != nil {
			return nil, err
		}

		return selCoalesce{args: args}, nil
	case "NULLIF":
		args, err := p.parseExprList()
		if err != nil {
			return nil, err
		}

		if len(args) != 2 { //nolint:mnd // NULLIF takes exactly two arguments
			return nil, fmt.Errorf("%w: NULLIF takes two arguments", ErrSelectExpression)
		}

		return selNullIf{a: args[0], b: args[1]}, nil
	}

	return nil, fmt.Errorf("%w: unsupported function %s", ErrSelectExpression, name)
}

func (p *selectSQLParser) parseCast() (selectExpr, error) {
	x, err := p.parseExpr()
	if err != nil {
		return nil, err
	}

	if err = p.expectKeyword("AS"); err != nil {
		return nil, err
	}

	typ, ok := p.next()
	if !ok || typ.typ != sqlTokIdent {
		return nil, fmt.Errorf("%w: expected type after AS", ErrSelectExpression)
	}

	target := strings.ToUpper(typ.val)

	switch target {
	case "INT", "INTEGER", "FLOAT", "DECIMAL", "NUMERIC", "STRING", "BOOL", "BOOLEAN":
	default:
		return nil, fmt.Errorf("%w: unsupported CAST type %s", ErrSelectExpression, typ.val)
	}

	if err = p.expectPunct(")"); err != nil {
		return nil, err
	}

	return selCast{x: x, target: target}, nil
}
