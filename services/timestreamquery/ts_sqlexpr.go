package timestreamquery

import "strings"

func (p *sqlParser) parseExpr() (*node, error) {
	if err := p.enter(); err != nil {
		return nil, err
	}
	defer p.leave()

	if l, ok, err := p.tryLambda(); ok || err != nil {
		return l, err
	}

	return p.parseOr()
}

func (p *sqlParser) tryLambda() (*node, bool, error) {
	start := p.peek().start

	var params []string

	switch t := p.peek(); {
	case t.kind == tkIdent && p.peekAt(1).isOp("->"):
		params = []string{t.text}
		p.pos += argsTwo
	case t.isOp("("):
		i, names := 1, []string{}

		for p.peekAt(i).kind == tkIdent {
			names = append(names, p.peekAt(i).text)
			i++

			if !p.peekAt(i).isOp(",") {
				break
			}

			i++
		}

		if len(names) == 0 || !p.peekAt(i).isOp(")") || !p.peekAt(i+1).isOp("->") {
			return nil, false, nil
		}

		params = names
		p.pos += i + argsTwo
	default:
		return nil, false, nil
	}

	body, err := p.parseExpr()
	if err != nil {
		return nil, true, err
	}

	return &node{kind: nLambda, params: params, args: []*node{body}, start: start, end: body.end}, true, nil
}

func (p *sqlParser) binary(op string, l, r *node) *node {
	return &node{kind: nBinary, name: op, args: []*node{l, r}, start: l.start, end: r.end}
}

func (p *sqlParser) parseOr() (*node, error) {
	l, err := p.parseAnd()
	if err != nil {
		return nil, err
	}

	for p.acceptKw("or") {
		r, rerr := p.parseAnd()
		if rerr != nil {
			return nil, rerr
		}

		l = p.binary("OR", l, r)
	}

	return l, nil
}

func (p *sqlParser) parseAnd() (*node, error) {
	l, err := p.parseNot()
	if err != nil {
		return nil, err
	}

	for p.acceptKw("and") {
		r, rerr := p.parseNot()
		if rerr != nil {
			return nil, rerr
		}

		l = p.binary("AND", l, r)
	}

	return l, nil
}

func (p *sqlParser) parseNot() (*node, error) {
	if t := p.peek(); t.is("not") {
		p.pos++

		e, err := p.parseNot()
		if err != nil {
			return nil, err
		}

		return &node{kind: nUnary, name: opNot, args: []*node{e}, start: t.start, end: e.end}, nil
	}

	return p.parsePredicate()
}

func isCmpOp(op string) bool {
	switch op {
	case "=", "<>", "!=", "<", ">", "<=", ">=":
		return true
	}

	return false
}

func (p *sqlParser) parsePredicate() (*node, error) {
	l, err := p.parseConcat()
	if err != nil {
		return nil, err
	}

	for {
		t := p.peek()

		switch {
		case t.kind == tkOp && isCmpOp(t.text):
			p.pos++

			r, rerr := p.parseConcat()
			if rerr != nil {
				return nil, rerr
			}

			l = p.binary(t.text, l, r)
		case t.is("is"):
			if l, err = p.parseIs(l); err != nil {
				return nil, err
			}
		case t.is("not") || t.is("between") || t.is("in") || t.is("like"):
			if l, err = p.parseNegatable(l); err != nil {
				return nil, err
			}
		default:
			return l, nil
		}
	}
}

func (p *sqlParser) parseIs(l *node) (*node, error) {
	p.pos++

	neg := p.acceptKw("not")

	if p.acceptKw("distinct") {
		if err := p.expectKw("from"); err != nil {
			return nil, err
		}

		r, err := p.parseConcat()
		if err != nil {
			return nil, err
		}

		return p.binary("IS DISTINCT", l, r), nil
	}

	end := p.next()
	if end.kind != tkIdent {
		return nil, errSQLSyntax
	}

	return &node{kind: nIsNull, neg: neg, args: []*node{l}, start: l.start, end: end.end}, nil
}

func (p *sqlParser) parseNegatable(l *node) (*node, error) {
	neg := p.acceptKw("not")

	t := p.next()

	switch strings.ToLower(t.text) {
	case "between":
		lo, err := p.parseConcat()
		if err != nil {
			return nil, err
		}

		if err = p.expectKw("and"); err != nil {
			return nil, err
		}

		hi, err := p.parseConcat()
		if err != nil {
			return nil, err
		}

		return &node{kind: nBetween, neg: neg, args: []*node{l, lo, hi}, start: l.start, end: hi.end}, nil
	case "in":
		return p.parseIn(l, neg)
	case "like":
		pat, err := p.parseConcat()
		if err != nil {
			return nil, err
		}

		n := &node{kind: nLike, neg: neg, args: []*node{l, pat}, start: l.start, end: pat.end}
		if p.acceptKw("escape") {
			esc, eerr := p.parseConcat()
			if eerr != nil {
				return nil, eerr
			}

			n.args, n.end = append(n.args, esc), esc.end
		}

		return n, nil
	}

	return nil, errSQLSyntax
}

func (p *sqlParser) parseIn(l *node, neg bool) (*node, error) {
	if err := p.expectOp("("); err != nil {
		return nil, err
	}

	if p.peek().is("select") || p.peek().is("with") {
		qn, err := p.parseQuery()
		if err != nil {
			return nil, err
		}

		end := p.peek().end

		return &node{kind: nInSub, neg: neg, sub: qn, args: []*node{l}, start: l.start, end: end}, p.expectOp(")")
	}

	items, err := p.parseExprList(")")
	if err != nil {
		return nil, err
	}

	return &node{
		kind: nInList, neg: neg, args: append([]*node{l}, items...), start: l.start, end: p.toks[p.pos-1].end,
	}, nil
}

func (p *sqlParser) parseConcat() (*node, error) {
	l, err := p.parseAdditive()
	if err != nil {
		return nil, err
	}

	for p.peek().isOp("||") {
		p.pos++

		r, rerr := p.parseAdditive()
		if rerr != nil {
			return nil, rerr
		}

		l = p.binary("||", l, r)
	}

	return l, nil
}

func (p *sqlParser) parseAdditive() (*node, error) {
	l, err := p.parseMultiplicative()
	if err != nil {
		return nil, err
	}

	for p.peek().isOp("+") || p.peek().isOp("-") {
		opTok := p.next()

		r, rerr := p.parseMultiplicative()
		if rerr != nil {
			return nil, rerr
		}

		l = p.binary(opTok.text, l, r)
		l.opStart, l.opEnd = opTok.start, opTok.end
	}

	return l, nil
}

func (p *sqlParser) parseMultiplicative() (*node, error) {
	l, err := p.parseUnary()
	if err != nil {
		return nil, err
	}

	for p.peek().isOp("*") || p.peek().isOp("/") || p.peek().isOp("%") {
		op := p.next().text

		r, rerr := p.parseUnary()
		if rerr != nil {
			return nil, rerr
		}

		l = p.binary(op, l, r)
	}

	return l, nil
}

func (p *sqlParser) parseUnary() (*node, error) {
	if err := p.enter(); err != nil {
		return nil, err
	}
	defer p.leave()

	if t := p.peek(); t.isOp("-") || t.isOp("+") {
		p.pos++

		e, err := p.parseUnary()
		if err != nil {
			return nil, err
		}

		return &node{kind: nUnary, name: t.text, args: []*node{e}, start: t.start, end: e.end}, nil
	}

	e, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}

	return p.parsePostfix(e)
}

func (p *sqlParser) parsePostfix(e *node) (*node, error) {
	for {
		switch t := p.peek(); {
		case t.isOp("["):
			p.pos++

			idx, err := p.parseExpr()
			if err != nil {
				return nil, err
			}

			end := p.peek().end
			if err = p.expectOp("]"); err != nil {
				return nil, err
			}

			e = &node{kind: nSubscript, args: []*node{e, idx}, start: e.start, end: end}
		case t.isOp(".") && (p.peekAt(1).kind == tkIdent || p.peekAt(1).kind == tkQuoted):
			p.pos++
			f := p.next()
			e = &node{kind: nField, name: f.text, args: []*node{e}, start: e.start, end: f.end}
		case t.is("at") && p.peekAt(1).is("time"):
			return nil, errSQLSyntax
		default:
			return e, nil
		}
	}
}

func typedLitKind(lw string) tsKind {
	switch lw {
	case kwDate:
		return tyDate
	case colTime:
		return tyTime
	case typTimestamp:
		return tyTimestamp
	case typDouble, "real":
		return tyDouble
	case typBigint:
		return tyBigint
	case kwInteger:
		return tyInteger
	case typVarchar:
		return tyVarchar
	case typBoolean:
		return tyBoolean
	}

	return tyInvalid
}

func (p *sqlParser) parsePrimary() (*node, error) {
	t := p.peek()

	switch t.kind {
	case tkNumber:
		p.pos++

		k := tyInteger
		if strings.ContainsAny(t.text, ".eE") {
			k = tyDouble
		} else if n, ok := atoiSafe(t.text); !ok || n > maxInt32 {
			k = tyBigint
		}

		return &node{kind: nLiteral, name: t.text, typ: scalar(k), start: t.start, end: t.end}, nil
	case tkDuration:
		p.pos++

		return &node{kind: nLiteral, name: t.text, typ: scalar(tyIntervalDS), start: t.start, end: t.end}, nil
	case tkString:
		p.pos++

		return &node{kind: nLiteral, name: t.text, typ: scalar(tyVarchar), start: t.start, end: t.end}, nil
	case tkVar:
		p.pos++

		return &node{kind: nVar, name: t.text, start: t.start, end: t.end}, nil
	case tkQuoted:
		return p.parseNameOrCall()
	case tkIdent:
		return p.parseKeywordOrName()
	case tkOp:
		return p.parseOpPrimary()
	case tkEOF:
	}

	return nil, errSQLSyntax
}

const maxInt32 = 1<<31 - 1

func (p *sqlParser) parseOpPrimary() (*node, error) {
	t := p.peek()

	switch {
	case t.isOp("?"):
		p.pos++

		return &node{kind: nParam, start: t.start, end: t.end}, nil
	case t.isOp("("):
		p.pos++

		if p.peek().is("select") || p.peek().is("with") {
			qn, err := p.parseQuery()
			if err != nil {
				return nil, err
			}

			end := p.peek().end

			return &node{kind: nSubquery, sub: qn, start: t.start, end: end}, p.expectOp(")")
		}

		items, err := p.parseExprList(")")
		if err != nil || len(items) == 0 {
			return nil, errSQLSyntax
		}

		if len(items) == 1 {
			return items[0], nil
		}

		return &node{kind: nRowCtor, args: items, start: t.start, end: p.toks[p.pos-1].end}, nil
	}

	return nil, errSQLSyntax
}

func (p *sqlParser) parseKeywordOrName() (*node, error) {
	t := p.peek()
	lw := strings.ToLower(t.text)
	nx := p.peekAt(1)

	switch {
	case lw == kwNull:
		p.pos++

		return &node{kind: nLiteral, name: kwNull, typ: scalar(tyUnknown), start: t.start, end: t.end}, nil
	case lw == kwTrue || lw == kwFalse:
		p.pos++

		return &node{kind: nLiteral, name: lw, typ: scalar(tyBoolean), start: t.start, end: t.end}, nil
	case lw == "case":
		return p.parseCase()
	case lw == "interval":
		return p.parseInterval()
	case nx.kind == tkString && typedLitKind(lw) != tyInvalid:
		p.pos += argsTwo

		return &node{
			kind: nTypedLit, name: nx.text, typ: scalar(typedLitKind(lw)), start: t.start, end: nx.end,
		}, nil
	case nx.isOp("("):
		if n, ok, err := p.parseParenKeyword(lw); ok || err != nil {
			return n, err
		}
	case lw == "array" && nx.isOp("["):
		return p.parseArrayCtor()
	}

	return p.parseNameOrCall()
}

// parseParenKeyword parses the constructs that look like a call but have their own grammar.
func (p *sqlParser) parseParenKeyword(lw string) (*node, bool, error) {
	var (
		n   *node
		err error
	)

	switch lw {
	case "cast", "try_cast":
		n, err = p.parseCast()
	case "exists":
		n, err = p.parseExists()
	case "extract":
		n, err = p.parseExtract()
	case "row":
		n, err = p.parseRowCtor()
	default:
		return nil, false, nil
	}

	return n, true, err
}

func (p *sqlParser) parseRowCtor() (*node, error) {
	t := p.next()
	p.pos++

	items, err := p.parseExprList(")")
	if err != nil {
		return nil, err
	}

	return &node{kind: nRowCtor, args: items, start: t.start, end: p.toks[p.pos-1].end}, nil
}

func (p *sqlParser) parseNameOrCall() (*node, error) {
	t := p.next()

	if p.peek().isOp("(") && t.kind == tkIdent {
		return p.parseCall(t)
	}

	n := &node{kind: nColumn, parts: []string{t.text}, start: t.start, end: t.end}

	for p.peek().isOp(".") && (p.peekAt(1).kind == tkIdent || p.peekAt(1).kind == tkQuoted) {
		p.pos++
		f := p.next()
		n.parts = append(n.parts, f.text)
		n.end = f.end
	}

	return n, nil
}

func (p *sqlParser) parseCall(name token) (*node, error) {
	p.pos++

	n := &node{kind: nCall, name: name.text, start: name.start}

	if p.peek().isOp("*") && p.peekAt(1).isOp(")") {
		p.pos++
		n.star = true
	} else {
		if p.acceptKw("distinct") {
			n.distinct = true
		} else {
			p.acceptKw("all")
		}

		args, err := p.parseExprList(")")
		if err != nil {
			return nil, err
		}

		n.args = args
	}

	if n.star {
		p.pos++
	}

	n.end = p.toks[p.pos-1].end

	return n, p.parseCallSuffix(n)
}

func (p *sqlParser) skipBalanced() error {
	if err := p.expectOp("("); err != nil {
		return err
	}

	depth := 1

	for depth > 0 {
		t := p.next()

		switch {
		case t.kind == tkEOF:
			return errSQLSyntax
		case t.isOp("("):
			depth++
		case t.isOp(")"):
			depth--
		}
	}

	return nil
}

func (p *sqlParser) parseCallSuffix(n *node) error {
	for {
		advanced, err := p.parseOneSuffix(n)
		if err != nil {
			return err
		}

		if !advanced {
			n.end = p.toks[p.pos-1].end

			return nil
		}
	}
}

func (p *sqlParser) parseOneSuffix(n *node) (bool, error) {
	t, nx := p.peek(), p.peekAt(1)

	switch {
	case t.is("filter") && nx.isOp("("):
		p.pos++

		return true, p.skipBalanced()
	case t.is("within") && nx.is("group"):
		p.pos += argsTwo

		return true, p.skipBalanced()
	case (t.is("ignore") || t.is("respect")) && nx.is("nulls"):
		p.pos += argsTwo

		return true, nil
	case t.is("over"):
		p.pos++

		n.over = true

		if p.peek().isOp("(") {
			return true, p.skipBalanced()
		}

		if _, ok := p.identNameAny(); !ok {
			return false, errSQLSyntax
		}

		return true, nil
	}

	return false, nil
}

func (p *sqlParser) parseCase() (*node, error) {
	t := p.next()
	n := &node{kind: nCase, start: t.start}

	if !p.peek().is("when") {
		op, err := p.parseExpr()
		if err != nil {
			return nil, err
		}

		n.operand = true
		n.args = append(n.args, op)
	}

	for p.acceptKw("when") {
		w, err := p.parseExpr()
		if err != nil {
			return nil, err
		}

		if err = p.expectKw("then"); err != nil {
			return nil, err
		}

		r, err := p.parseExpr()
		if err != nil {
			return nil, err
		}

		n.args = append(n.args, w, r)
	}

	if p.acceptKw("else") {
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}

		n.hasElse = true
		n.args = append(n.args, e)
	}

	end := p.peek().end
	if err := p.expectKw("end"); err != nil {
		return nil, err
	}

	n.end = end

	return n, nil
}

func (p *sqlParser) parseCast() (*node, error) {
	t := p.next()
	p.pos++

	e, err := p.parseExpr()
	if err != nil {
		return nil, err
	}

	if err = p.expectKw("as"); err != nil {
		return nil, err
	}

	typ, err := p.parseType()
	if err != nil {
		return nil, err
	}

	end := p.peek().end

	return &node{
		kind: nCast, name: strings.ToLower(t.text), typ: typ, args: []*node{e}, start: t.start, end: end,
	}, p.expectOp(")")
}

func (p *sqlParser) parseExists() (*node, error) {
	t := p.next()
	p.pos++

	qn, err := p.parseQuery()
	if err != nil {
		return nil, err
	}

	end := p.peek().end

	return &node{kind: nExists, sub: qn, start: t.start, end: end}, p.expectOp(")")
}

func (p *sqlParser) parseArrayCtor() (*node, error) {
	t := p.next()
	p.pos++

	items, err := p.parseExprList("]")
	if err != nil {
		return nil, err
	}

	return &node{kind: nArrayCtor, args: items, start: t.start, end: p.toks[p.pos-1].end}, nil
}

func isIntervalUnit(u string) bool {
	switch strings.ToLower(u) {
	case kwYear, kwMonth, "day", "hour", "minute", "second":
		return true
	}

	return false
}

func (p *sqlParser) parseInterval() (*node, error) {
	t := p.next()

	sign := p.peek()
	if sign.isOp("-") || sign.isOp("+") {
		p.pos++
	}

	lit := p.next()
	if lit.kind != tkString && lit.kind != tkNumber {
		return nil, errSQLSyntax
	}

	unit := p.next()
	if unit.kind != tkIdent || !isIntervalUnit(unit.text) {
		return nil, errSQLSyntax
	}

	end, k := unit.end, tyIntervalDS
	if lw := strings.ToLower(unit.text); lw == kwYear || lw == kwMonth {
		k = tyIntervalYM
	}

	if p.acceptKw("to") {
		u2 := p.next()
		if u2.kind != tkIdent || !isIntervalUnit(u2.text) {
			return nil, errSQLSyntax
		}

		end = u2.end
	}

	return &node{
		kind: nInterval, name: lit.text, parts: []string{unit.text}, typ: scalar(k), start: t.start, end: end,
	}, nil
}

func (p *sqlParser) parseExtract() (*node, error) {
	t := p.next()
	p.pos++

	field := p.next()
	if field.kind != tkIdent {
		return nil, errSQLSyntax
	}

	if err := p.expectKw("from"); err != nil {
		return nil, err
	}

	e, err := p.parseExpr()
	if err != nil {
		return nil, err
	}

	end := p.peek().end

	return &node{
		kind: nCall, name: "extract", args: []*node{e}, start: t.start, end: end, parts: []string{field.text},
	}, p.expectOp(")")
}

func (p *sqlParser) parseType() (tsType, error) {
	if err := p.enter(); err != nil {
		return tsType{}, err
	}
	defer p.leave()

	t := p.next()
	if t.kind != tkIdent {
		return tsType{}, errSQLSyntax
	}

	switch lw := strings.ToLower(t.text); lw {
	case "array":
		return p.parseArrayType()
	case "row":
		return p.parseRowType()
	case "timeseries":
		inner, err := p.parseAngleOrParenType()
		if err != nil {
			return tsType{}, err
		}

		return seriesOf(inner), nil
	case "interval":
		return p.parseIntervalType()
	default:
		return p.parseScalarType(lw)
	}
}

func (p *sqlParser) parseAngleOrParenType() (tsType, error) {
	closer := ")"

	switch {
	case p.acceptOp("("):
	case p.acceptOp("<"):
		closer = ">"
	default:
		return tsType{}, errSQLSyntax
	}

	inner, err := p.parseType()
	if err != nil {
		return tsType{}, err
	}

	return inner, p.expectOp(closer)
}

func (p *sqlParser) parseArrayType() (tsType, error) {
	e, err := p.parseAngleOrParenType()
	if err != nil {
		return tsType{}, err
	}

	return arrayOf(e), nil
}

func (p *sqlParser) parseRowType() (tsType, error) {
	if err := p.expectOp("("); err != nil {
		return tsType{}, err
	}

	var fields []tsField

	for {
		name := ""
		if (p.peek().kind == tkIdent || p.peek().kind == tkQuoted) && p.peekAt(1).kind == tkIdent {
			name = p.next().text
		}

		ft, err := p.parseType()
		if err != nil {
			return tsType{}, err
		}

		fields = append(fields, tsField{name: name, typ: ft})

		if p.acceptOp(")") {
			return rowOf(fields), nil
		}

		if err = p.expectOp(","); err != nil {
			return tsType{}, err
		}
	}
}

func (p *sqlParser) parseIntervalType() (tsType, error) {
	a := p.next()
	if !isIntervalUnit(a.text) {
		return tsType{}, errSQLSyntax
	}

	if err := p.expectKw("to"); err != nil {
		return tsType{}, err
	}

	b := p.next()
	if !isIntervalUnit(b.text) {
		return tsType{}, errSQLSyntax
	}

	if lw := strings.ToLower(a.text); lw == kwYear || lw == kwMonth {
		return scalar(tyIntervalYM), nil
	}

	return scalar(tyIntervalDS), nil
}

func (p *sqlParser) skipTypeParams() {
	if p.peek().isOp("(") && p.peekAt(1).kind == tkNumber {
		for !p.peek().isOp(")") && p.peek().kind != tkEOF {
			p.pos++
		}

		p.pos++
	}
}

func (p *sqlParser) parseScalarType(lw string) (tsType, error) {
	p.skipTypeParams()

	switch lw {
	case typBigint, "long":
		return scalar(tyBigint), nil
	case "int", kwInteger, "smallint", "tinyint":
		return scalar(tyInteger), nil
	case typDouble, "real", "float", "decimal", "numeric":
		p.acceptKw("precision")

		return scalar(tyDouble), nil
	case "varchar", "char", "string":
		return scalar(tyVarchar), nil
	case typBoolean:
		return scalar(tyBoolean), nil
	case "date":
		return scalar(tyDate), nil
	case "time":
		p.skipTimeZoneSuffix()

		return scalar(tyTime), nil
	case "timestamp":
		p.skipTimeZoneSuffix()

		return scalar(tyTimestamp), nil
	}

	return tsType{}, errSQLSyntax
}

func (p *sqlParser) skipTimeZoneSuffix() {
	if (p.peek().is("with") || p.peek().is("without")) && p.peekAt(1).is("time") {
		p.pos += argsThree
	}
}
