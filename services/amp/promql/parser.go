package promql

import (
	"fmt"
	"strings"
	"time"
)

type parser struct {
	toks []token
	i    int
}

// Parse parses a PromQL expression.
func Parse(src string) (Expr, error) {
	toks, err := tokenize(src)
	if err != nil {
		return nil, err
	}

	p := &parser{toks: toks}

	e, err := p.expr(0)
	if err != nil {
		return nil, err
	}

	if t := p.peek(); t.kind != tokEOF {
		return nil, syntaxErr(t.pos, "unexpected %q", t.text)
	}

	return e, nil
}

func (p *parser) peek() token { return p.toks[p.i] }

func (p *parser) advance() token {
	t := p.toks[p.i]
	if t.kind != tokEOF {
		p.i++
	}

	return t
}

func (p *parser) isOp(text string) bool {
	t := p.peek()

	return t.kind == tokOp && t.text == text
}

func (p *parser) expectOp(text string) error {
	if !p.isOp(text) {
		t := p.peek()

		return syntaxErr(t.pos, "expected %q", text)
	}

	p.advance()

	return nil
}

const (
	precOr = iota + 1
	precAnd
	precCmp
	precAdd
	precMul
	precPow
)

func binPrec(t token) int {
	switch t.kind {
	case tokIdent:
		return keywordPrec(strings.ToLower(t.text))
	case tokOp:
		return operatorPrec(t.text)
	default:
		return 0
	}
}

func keywordPrec(word string) int {
	switch word {
	case opOr:
		return precOr
	case opAnd, opUnless:
		return precAnd
	}

	return 0
}

func operatorPrec(op string) int {
	switch op {
	case "==", "!=", ">", "<", ">=", "<=":
		return precCmp
	case "+", "-":
		return precAdd
	case "*", "/", "%":
		return precMul
	case "^":
		return precPow
	}

	return 0
}

func (p *parser) expr(minPrec int) (Expr, error) {
	left, err := p.unary()
	if err != nil {
		return nil, err
	}

	for {
		t := p.peek()

		prec := binPrec(t)
		if prec == 0 || prec < minPrec {
			return left, nil
		}

		p.advance()

		bin := &Binary{L: left, Op: strings.ToLower(t.text)}
		if ierr := p.binModifiers(bin, prec); ierr != nil {
			return nil, ierr
		}

		next := prec + 1
		if prec == precPow {
			next = prec
		}

		right, ierr := p.expr(next)
		if ierr != nil {
			return nil, ierr
		}

		bin.R = right
		left = bin
	}
}

func (p *parser) binModifiers(bin *Binary, prec int) error {
	if prec == precCmp && p.peek().kind == tokIdent && strings.EqualFold(p.peek().text, "bool") {
		p.advance()

		bin.ReturnBool = true
	}

	t := p.peek()
	if t.kind != tokIdent || (!strings.EqualFold(t.text, "on") && !strings.EqualFold(t.text, "ignoring")) {
		return nil
	}

	p.advance()

	bin.On = strings.EqualFold(t.text, "on")

	names, err := p.labelList()
	if err != nil {
		return err
	}

	bin.MatchLabels = names

	if g := p.peek(); g.kind == tokIdent &&
		(strings.EqualFold(g.text, "group_left") || strings.EqualFold(g.text, "group_right")) {
		return syntaxErr(g.pos, "%s matching is not supported", g.text)
	}

	return nil
}

func (p *parser) labelList() ([]string, error) {
	if err := p.expectOp("("); err != nil {
		return nil, err
	}

	names := []string{}

	for !p.isOp(")") {
		t := p.advance()
		if t.kind != tokIdent {
			return nil, syntaxErr(t.pos, "expected label name")
		}

		names = append(names, t.text)

		if p.isOp(",") {
			p.advance()
		}
	}

	p.advance()

	return names, nil
}

func (p *parser) unary() (Expr, error) {
	if p.isOp("-") || p.isOp("+") {
		neg := p.advance().text == "-"

		x, err := p.expr(precPow)
		if err != nil {
			return nil, err
		}

		if !neg {
			return x, nil
		}

		return &Unary{X: x}, nil
	}

	return p.postfix()
}

func (p *parser) postfix() (Expr, error) {
	e, err := p.primary()
	if err != nil {
		return nil, err
	}

	for {
		switch {
		case p.isOp("["):
			e, err = p.rangeSuffix(e)
		case p.peek().kind == tokIdent && strings.EqualFold(p.peek().text, "offset"):
			e, err = p.offsetSuffix(e)
		default:
			return e, nil
		}

		if err != nil {
			return nil, err
		}
	}
}

func (p *parser) rangeSuffix(e Expr) (Expr, error) {
	vs, ok := e.(*VectorSelector)
	if !ok {
		return nil, syntaxErr(p.peek().pos, "range selector requires a vector selector (subqueries are not supported)")
	}

	p.advance()

	d := p.advance()
	if d.kind != tokDuration {
		return nil, syntaxErr(d.pos, "expected duration in range selector")
	}

	if !p.isOp("]") {
		return nil, syntaxErr(p.peek().pos, "expected \"]\" (subqueries are not supported)")
	}

	p.advance()

	return &MatrixSelector{VS: vs, Range: d.dur}, nil
}

func (p *parser) offsetSuffix(e Expr) (Expr, error) {
	p.advance()

	neg := false
	if p.isOp("-") {
		p.advance()

		neg = true
	}

	d := p.advance()
	if d.kind != tokDuration {
		return nil, syntaxErr(d.pos, "expected duration after offset")
	}

	off := d.dur
	if neg {
		off = -off
	}

	switch x := e.(type) {
	case *VectorSelector:
		x.Offset = off
	case *MatrixSelector:
		x.VS.Offset = off
	default:
		return nil, syntaxErr(d.pos, "offset is only valid on a selector")
	}

	return e, nil
}

func (p *parser) primary() (Expr, error) {
	t := p.peek()

	switch t.kind {
	case tokNumber:
		p.advance()

		return &NumberLit{V: t.num}, nil
	case tokString:
		p.advance()

		return &StringLit{V: t.text}, nil
	case tokDuration:
		return nil, syntaxErr(t.pos, "unexpected duration %q", t.text)
	case tokIdent:
		return p.identExpr()
	case tokOp:
		return p.opPrimary()
	case tokEOF:
		return nil, syntaxErr(t.pos, "unexpected end of input")
	}

	return nil, syntaxErr(t.pos, "unexpected %q", t.text)
}

func (p *parser) opPrimary() (Expr, error) {
	t := p.peek()

	switch t.text {
	case "(":
		p.advance()

		e, err := p.expr(0)
		if err != nil {
			return nil, err
		}

		if ierr := p.expectOp(")"); ierr != nil {
			return nil, ierr
		}

		return e, nil
	case "{":
		return p.selector("")
	}

	return nil, syntaxErr(t.pos, "unexpected %q", t.text)
}

var aggregators = map[string]bool{ //nolint:gochecknoglobals // immutable lookup
	"sum": true, "avg": true, "min": true, "max": true, "count": true, "group": true,
	"stddev": true, "stdvar": true, opTopK: true, opBottomK: true, "quantile": true,
}

var paramAggregators = map[string]bool{ //nolint:gochecknoglobals // immutable lookup
	opTopK:     true,
	opBottomK:  true,
	"quantile": true,
}

func (p *parser) identExpr() (Expr, error) {
	t := p.advance()
	name := t.text
	lower := strings.ToLower(name)

	if aggregators[lower] && (p.isOp("(") || p.isGroupingKeyword()) {
		return p.aggregation(lower)
	}

	if p.isOp("(") {
		return p.call(name)
	}

	switch lower {
	case "by", "without", "on", "ignoring", "offset", "bool", "and", "or", "unless", "group_left", "group_right":
		return nil, syntaxErr(t.pos, "unexpected keyword %q", name)
	}

	return p.selector(name)
}

func (p *parser) isGroupingKeyword() bool {
	t := p.peek()

	return t.kind == tokIdent && (strings.EqualFold(t.text, "by") || strings.EqualFold(t.text, "without"))
}

func (p *parser) grouping(agg *Aggregation) error {
	t := p.advance()
	agg.Without = strings.EqualFold(t.text, "without")

	names, err := p.labelList()
	if err != nil {
		return err
	}

	agg.Grouping = names

	return nil
}

func (p *parser) aggregation(op string) (Expr, error) {
	agg := &Aggregation{Op: op}

	if p.isGroupingKeyword() {
		if err := p.grouping(agg); err != nil {
			return nil, err
		}
	}

	if err := p.expectOp("("); err != nil {
		return nil, err
	}

	if paramAggregators[op] {
		param, err := p.expr(0)
		if err != nil {
			return nil, err
		}

		agg.Param = param

		if ierr := p.expectOp(","); ierr != nil {
			return nil, ierr
		}
	}

	arg, err := p.expr(0)
	if err != nil {
		return nil, err
	}

	agg.Expr = arg

	if ierr := p.expectOp(")"); ierr != nil {
		return nil, ierr
	}

	if p.isGroupingKeyword() {
		if agg.Grouping != nil {
			return nil, syntaxErr(p.peek().pos, "grouping given twice")
		}

		if ierr := p.grouping(agg); ierr != nil {
			return nil, ierr
		}
	}

	return agg, nil
}

func (p *parser) call(name string) (Expr, error) {
	spec, ok := functions[strings.ToLower(name)]
	if !ok {
		return nil, syntaxErr(p.peek().pos, "unknown function %q", name)
	}

	p.advance()

	args := []Expr{}

	for !p.isOp(")") {
		a, err := p.expr(0)
		if err != nil {
			return nil, err
		}

		args = append(args, a)

		if p.isOp(",") {
			p.advance()
		} else if !p.isOp(")") {
			return nil, syntaxErr(p.peek().pos, "expected \",\" or \")\"")
		}
	}

	p.advance()

	if len(args) < spec.minArgs || len(args) > spec.maxArgs {
		return nil, fmt.Errorf("%w: function %q got %d arguments", ErrSyntax, name, len(args))
	}

	return &Call{Func: strings.ToLower(name), Args: args}, nil
}

func (p *parser) selector(name string) (Expr, error) {
	vs := &VectorSelector{Name: name}

	if p.isOp("{") {
		p.advance()

		if err := p.matchers(vs); err != nil {
			return nil, err
		}
	}

	if name != "" {
		m, err := NewMatcher(MatchEqual, NameLabel, name)
		if err != nil {
			return nil, err
		}

		vs.Matchers = append(vs.Matchers, m)
	}

	if !hasNonEmptyMatcher(vs.Matchers) {
		return nil, syntaxErr(p.peek().pos, "vector selector must contain at least one non-empty matcher")
	}

	return vs, nil
}

func hasNonEmptyMatcher(ms []*Matcher) bool {
	for _, m := range ms {
		if !m.Matches("") {
			return true
		}
	}

	return false
}

func (p *parser) matchers(vs *VectorSelector) error {
	for !p.isOp("}") {
		label := p.advance()
		if label.kind != tokIdent {
			return syntaxErr(label.pos, "expected label name")
		}

		op := p.advance()

		var mt MatchType

		switch op.text {
		case "=":
			mt = MatchEqual
		case "!=":
			mt = MatchNotEqual
		case "=~":
			mt = MatchRegexp
		case "!~":
			mt = MatchNotRegexp
		default:
			return syntaxErr(op.pos, "expected label matching operator")
		}

		val := p.advance()
		if val.kind != tokString {
			return syntaxErr(val.pos, "expected label value string")
		}

		m, err := NewMatcher(mt, label.text, val.text)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrSyntax, err)
		}

		vs.Matchers = append(vs.Matchers, m)

		if p.isOp(",") {
			p.advance()
		} else if !p.isOp("}") {
			return syntaxErr(p.peek().pos, "expected \",\" or \"}\"")
		}
	}

	p.advance()

	return nil
}

// selectorRange returns the lookback bounds a selector needs around t (Unix ms).
func selectorRange(vs *VectorSelector, t int64, window time.Duration) (int64, int64) {
	end := t - vs.Offset.Milliseconds()

	return end - window.Milliseconds(), end
}
