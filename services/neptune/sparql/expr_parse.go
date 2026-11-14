package sparql

import (
	"slices"
	"strings"
)

const (
	aggCount = "COUNT"
	aggMax   = "MAX"
)

func isAggregate(name string) bool {
	switch name {
	case aggCount, "SUM", "MIN", aggMax, "AVG", "SAMPLE", "GROUP_CONCAT":
		return true
	default:
		return false
	}
}

func (p *parser) parseExpr() (Expr, error) { return p.parseOr() }

func (p *parser) parseOr() (Expr, error) {
	l, err := p.parseAnd()
	if err != nil {
		return nil, err
	}

	for p.acceptP("||") {
		r, rerr := p.parseAnd()
		if rerr != nil {
			return nil, rerr
		}

		l = exprBinary{Op: "||", L: l, R: r}
	}

	return l, nil
}

func (p *parser) parseAnd() (Expr, error) {
	l, err := p.parseRel()
	if err != nil {
		return nil, err
	}

	for p.acceptP("&&") {
		r, rerr := p.parseRel()
		if rerr != nil {
			return nil, rerr
		}

		l = exprBinary{Op: "&&", L: l, R: r}
	}

	return l, nil
}

func (p *parser) parseRel() (Expr, error) {
	l, err := p.parseAdd()
	if err != nil {
		return nil, err
	}

	t := p.peek()

	for _, op := range []string{"=", "!=", "<=", ">=", "<", ">"} {
		if t.is(op) {
			p.i++

			r, rerr := p.parseAdd()
			if rerr != nil {
				return nil, rerr
			}

			return exprBinary{Op: op, L: l, R: r}, nil
		}
	}

	not := false

	if t.kw("NOT") && p.peekNext().kw("IN") {
		p.i++
		not = true
		t = p.peek()
	}

	if t.kw("IN") {
		p.i++

		list, lerr := p.parseExprList()
		if lerr != nil {
			return nil, lerr
		}

		return exprIn{E: l, List: list, Not: not}, nil
	}

	return l, nil
}

func (p *parser) parseExprList() ([]Expr, error) {
	if err := p.expectP("("); err != nil {
		return nil, err
	}

	var out []Expr

	if p.acceptP(")") {
		return out, nil
	}

	for {
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}

		out = append(out, e)

		if p.acceptP(",") {
			continue
		}

		return out, p.expectP(")")
	}
}

func (p *parser) parseAdd() (Expr, error) {
	l, err := p.parseMul()
	if err != nil {
		return nil, err
	}

	for {
		t := p.peek()

		switch {
		case t.is("+") || t.is("-"):
			p.i++

			r, rerr := p.parseMul()
			if rerr != nil {
				return nil, rerr
			}

			l = exprBinary{Op: t.val, L: l, R: r}
		default:
			return l, nil
		}
	}
}

func (p *parser) parseMul() (Expr, error) {
	l, err := p.parseUnary()
	if err != nil {
		return nil, err
	}

	for {
		t := p.peek()
		if !t.is("*") && !t.is("/") {
			return l, nil
		}

		p.i++

		r, rerr := p.parseUnary()
		if rerr != nil {
			return nil, rerr
		}

		l = exprBinary{Op: t.val, L: l, R: r}
	}
}

func (p *parser) parseUnary() (Expr, error) {
	t := p.peek()

	switch {
	case t.is("!"):
		p.i++

		e, err := p.parseUnary()

		return exprUnary{Op: "!", E: e}, err
	case t.is("+") || t.is("-"):
		if k := p.peekNext().kind; k == tInt || k == tDecimal || k == tDouble {
			lit, err := p.parseLiteral()

			return exprTerm{T: lit}, err
		}

		p.i++

		e, err := p.parseUnary()

		return exprUnary{Op: t.val, E: e}, err
	default:
		return p.parsePrimary()
	}
}

func (p *parser) parsePrimary() (Expr, error) {
	t := p.peek()

	switch t.kind {
	case tPunct:
		if t.is("(") {
			p.i++

			e, err := p.parseExpr()
			if err != nil {
				return nil, err
			}

			return e, p.expectP(")")
		}
	case tVar:
		p.i++

		return exprVar{Name: t.val}, nil
	case tString, tInt, tDecimal, tDouble:
		lit, err := p.parseLiteral()

		return exprTerm{T: lit}, err
	case tIRI, tPName:
		iri, err := p.iriTok()
		if err != nil {
			return nil, err
		}

		if p.peek().is("(") {
			args, aerr := p.parseExprList()

			return exprCall{Name: iri.Value, Args: args}, aerr
		}

		return exprTerm{T: iri}, nil
	case tIdent:
		return p.parseIdentExpr(t)
	default:
	}

	return nil, p.errf("expected expression")
}

func (p *parser) parseIdentExpr(t token) (Expr, error) {
	name := strings.ToUpper(t.val)

	switch name {
	case "TRUE", "FALSE":
		p.i++

		return exprTerm{T: Typed(strings.ToLower(t.val), XSDBoolean)}, nil
	case "EXISTS":
		p.i++

		g, err := p.parseGroup()

		return exprExists{G: g}, err
	case "NOT":
		p.i++

		if err := p.expectKW("EXISTS"); err != nil {
			return nil, err
		}

		g, err := p.parseGroup()

		return exprExists{G: g, Not: true}, err
	}

	if !p.peekNext().is("(") {
		return nil, p.errf("unexpected %q in expression", t.val)
	}

	p.i += 2

	call := exprCall{Name: name}

	if isAggregate(name) {
		return p.parseAggregate(call)
	}

	if p.acceptP(")") {
		return call, nil
	}

	for {
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}

		call.Args = append(call.Args, e)

		if p.acceptP(",") {
			continue
		}

		return call, p.expectP(")")
	}
}

func (p *parser) parseAggregate(call exprCall) (Expr, error) {
	call.Distinct = p.acceptKW("DISTINCT")

	if call.Name == aggCount && p.acceptP("*") {
		call.Star = true

		return call, p.expectP(")")
	}

	e, err := p.parseExpr()
	if err != nil {
		return nil, err
	}

	call.Args = []Expr{e}
	call.Sep = " "

	if call.Name == "GROUP_CONCAT" && p.acceptP(";") {
		if err = p.expectKW("SEPARATOR"); err != nil {
			return nil, err
		}

		if err = p.expectP("="); err != nil {
			return nil, err
		}

		s := p.advance()
		if s.kind != tString {
			return nil, p.errf("expected separator string")
		}

		call.Sep = s.val
	}

	return call, p.expectP(")")
}

// hasAggregate reports whether e contains an aggregate call (not descending into EXISTS patterns).
func hasAggregate(e Expr) bool {
	switch v := e.(type) {
	case exprCall:
		if isAggregate(v.Name) {
			return true
		}

		if slices.ContainsFunc(v.Args, hasAggregate) {
			return true
		}
	case exprUnary:
		return hasAggregate(v.E)
	case exprBinary:
		return hasAggregate(v.L) || hasAggregate(v.R)
	case exprIn:
		if hasAggregate(v.E) {
			return true
		}

		if slices.ContainsFunc(v.List, hasAggregate) {
			return true
		}
	}

	return false
}
