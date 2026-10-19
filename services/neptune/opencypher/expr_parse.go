package opencypher

import (
	"strconv"
	"strings"
)

func (p *parser) parseExpr() (expr, error) { return p.parseBinaryLevel(0) }

func logicOp(lv int) string {
	switch lv {
	case 0:
		return opOr
	case 1:
		return "XOR"
	case 2: //nolint:mnd // AND binds tightest of the logical operators
		return opAnd
	default:
		return ""
	}
}

func (p *parser) parseBinaryLevel(lv int) (expr, error) {
	op := logicOp(lv)
	if op == "" {
		return p.parseNot()
	}

	l, err := p.parseBinaryLevel(lv + 1)
	if err != nil {
		return nil, err
	}

	for p.accept(op) {
		r, rerr := p.parseBinaryLevel(lv + 1)
		if rerr != nil {
			return nil, rerr
		}

		l = binaryExpr{op: op, l: l, r: r}
	}

	return l, nil
}

func (p *parser) parseNot() (expr, error) {
	if p.accept("NOT") {
		e, err := p.parseNot()

		return unaryExpr{op: "NOT", e: e}, err
	}

	return p.parseComparison()
}

func (p *parser) parseComparison() (expr, error) {
	l, err := p.parseAdd()
	if err != nil {
		return nil, err
	}

	for {
		op, ok := p.comparisonOp()
		if !ok {
			return l, nil
		}

		if op == "IS" {
			not := p.accept("NOT")
			if err = p.expect("NULL"); err != nil {
				return nil, err
			}

			l = isNullExpr{e: l, not: not}

			continue
		}

		r, rerr := p.parseAdd()
		if rerr != nil {
			return nil, rerr
		}

		l = binaryExpr{op: op, l: l, r: r}
	}
}

func (p *parser) comparisonOp() (string, bool) {
	t := p.peek()

	switch {
	case t.kind == tkPunct && (t.text == "=" || t.text == "<>" || t.text == "!=" || t.text == "<" ||
		t.text == ">" || t.text == "<=" || t.text == ">="):
		p.pos++

		if t.text == "!=" {
			return "<>", true
		}

		return t.text, true
	case t.is("IS"), t.is("IN"), t.is("CONTAINS"):
		p.pos++

		return strings.ToUpper(t.text), true
	case t.is("STARTS"), t.is("ENDS"):
		if p.peek2().is("WITH") {
			p.pos += 2

			return strings.ToUpper(t.text) + " WITH", true
		}
	}

	return "", false
}

func (p *parser) parseAdd() (expr, error) {
	l, err := p.parseMul()
	if err != nil {
		return nil, err
	}

	for p.peek().is("+") || p.peek().is("-") {
		op := p.next().text

		r, rerr := p.parseMul()
		if rerr != nil {
			return nil, rerr
		}

		l = binaryExpr{op: op, l: l, r: r}
	}

	return l, nil
}

func (p *parser) parseMul() (expr, error) {
	l, err := p.parsePow()
	if err != nil {
		return nil, err
	}

	for p.peek().is("*") || p.peek().is("/") || p.peek().is("%") {
		op := p.next().text

		r, rerr := p.parsePow()
		if rerr != nil {
			return nil, rerr
		}

		l = binaryExpr{op: op, l: l, r: r}
	}

	return l, nil
}

func (p *parser) parsePow() (expr, error) {
	l, err := p.parseUnary()
	if err != nil {
		return nil, err
	}

	for p.accept("^") {
		r, rerr := p.parseUnary()
		if rerr != nil {
			return nil, rerr
		}

		l = binaryExpr{op: "^", l: l, r: r}
	}

	return l, nil
}

func (p *parser) parseUnary() (expr, error) {
	if p.peek().is("-") || p.peek().is("+") {
		op := p.next().text

		e, err := p.parseUnary()

		return unaryExpr{op: op, e: e}, err
	}

	return p.parsePostfix()
}

func (p *parser) parsePostfix() (expr, error) {
	e, err := p.parseAtom()
	if err != nil {
		return nil, err
	}

	for {
		switch {
		case p.peek().is(".") && p.peek2().kind == tkIdent:
			p.pos++
			e = propExpr{e: e, key: p.next().text}
		case p.peek().is("["):
			p.pos++

			if e, err = p.parseIndex(e); err != nil {
				return nil, err
			}
		case p.peek().is(":") && p.peek2().kind == tkIdent:
			labels, lerr := p.parseLabels()
			if lerr != nil {
				return nil, lerr
			}

			e = labelsExpr{e: e, labels: labels}
		default:
			return e, nil
		}
	}
}

func (p *parser) parseIndex(base expr) (expr, error) {
	var from expr

	var err error

	if !p.peek().is("..") {
		if from, err = p.parseExpr(); err != nil {
			return nil, err
		}
	}

	if p.accept("..") {
		var to expr

		if !p.peek().is("]") {
			if to, err = p.parseExpr(); err != nil {
				return nil, err
			}
		}

		return sliceExpr{e: base, from: from, to: to}, p.expect("]")
	}

	return indexExpr{e: base, i: from}, p.expect("]")
}

func (p *parser) parseAtom() (expr, error) {
	t := p.peek()

	switch t.kind {
	case tkInt:
		p.pos++

		n, err := strconv.ParseInt(t.text, 10, 64)
		if err != nil {
			return nil, syntaxErr(t.pos, "integer literal out of range")
		}

		return litExpr{v: n}, nil
	case tkFloat:
		p.pos++
		f, _ := strconv.ParseFloat(t.text, 64)

		return litExpr{v: f}, nil
	case tkString:
		p.pos++

		return litExpr{v: t.text}, nil
	case tkParam:
		p.pos++

		return paramExpr{name: t.text}, nil
	case tkIdent:
		return p.parseIdentAtom()
	case tkPunct:
		return p.parsePunctAtom()
	default:
		return nil, p.unexpected("an expression")
	}
}

func (p *parser) parsePunctAtom() (expr, error) {
	switch {
	case p.accept("("):
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}

		return e, p.expect(")")
	case p.accept("["):
		l := listExpr{}

		for !p.peek().is("]") {
			e, err := p.parseExpr()
			if err != nil {
				return nil, err
			}

			l.items = append(l.items, e)

			if !p.accept(",") {
				break
			}
		}

		return l, p.expect("]")
	case p.peek().is("{"):
		m, err := p.parseMapLiteral()
		if err != nil {
			return nil, err
		}

		return *m, nil
	default:
		return nil, p.unexpected("an expression")
	}
}

func (p *parser) parseIdentAtom() (expr, error) {
	t := p.next()

	switch strings.ToUpper(t.text) {
	case "TRUE":
		return litExpr{v: true}, nil
	case "FALSE":
		return litExpr{v: false}, nil
	case "NULL":
		return litExpr{v: nil}, nil
	case "CASE":
		return p.parseCase()
	}

	if !p.peek().is("(") {
		return identExpr{name: t.text}, nil
	}

	p.pos++

	c := callExpr{name: strings.ToLower(t.text)}

	if p.accept("*") {
		c.star = true

		return c, p.expect(")")
	}

	c.distinct = p.accept("DISTINCT")

	for !p.peek().is(")") {
		a, err := p.parseExpr()
		if err != nil {
			return nil, err
		}

		c.args = append(c.args, a)

		if !p.accept(",") {
			break
		}
	}

	return c, p.expect(")")
}

func (p *parser) parseCase() (expr, error) {
	c := caseExpr{}

	var err error

	if !p.peek().is("WHEN") {
		if c.subject, err = p.parseExpr(); err != nil {
			return nil, err
		}
	}

	for p.accept("WHEN") {
		w, werr := p.parseExpr()
		if werr != nil {
			return nil, werr
		}

		if err = p.expect("THEN"); err != nil {
			return nil, err
		}

		th, terr := p.parseExpr()
		if terr != nil {
			return nil, terr
		}

		c.whens, c.thens = append(c.whens, w), append(c.thens, th)
	}

	if p.accept("ELSE") {
		if c.els, err = p.parseExpr(); err != nil {
			return nil, err
		}
	}

	return c, p.expect("END")
}
