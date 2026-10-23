package bqsql

import (
	"strings"
)

const (
	bpOr      = 1
	bpAnd     = 2
	bpNot     = 3
	bpCompare = 4
	bpBitOr   = 5
	bpBitXor  = 6
	bpBitAnd  = 7
	bpShift   = 8
	bpAdd     = 9
	bpMul     = 10
	bpUnary   = 11
)

func binaryPower() map[string]int {
	return map[string]int{
		"=": bpCompare, "!=": bpCompare, "<>": bpCompare, "<": bpCompare, ">": bpCompare, "<=": bpCompare,
		">=": bpCompare, "|": bpBitOr, "^": bpBitXor, "&": bpBitAnd, "<<": bpShift, ">>": bpShift, "+": bpAdd,
		"-": bpAdd, "*": bpMul, "/": bpMul, "||": bpMul,
	}
}

func (p *parser) expr() (*node, error) { return p.exprBP(0) }

func (p *parser) exprBP(minBP int) (*node, error) {
	if err := p.enter(); err != nil {
		return nil, err
	}
	defer p.leave()

	left, err := p.prefix()
	if err != nil {
		return nil, err
	}

	for {
		next, ok, ierr := p.infix(left, minBP)
		if ierr != nil {
			return nil, ierr
		}

		if !ok {
			return left, nil
		}

		left = next
	}
}

func (p *parser) infix(left *node, minBP int) (*node, bool, error) {
	t := p.peek()

	if t.kind == tkIdent {
		return p.keywordInfix(left, minBP)
	}

	if t.kind != tkPunct {
		return nil, false, nil
	}

	bp, ok := binaryPower()[t.text]
	if !ok || bp < minBP {
		return nil, false, nil
	}

	p.i++

	right, err := p.exprBP(bp + 1)
	if err != nil {
		return nil, false, err
	}

	op := t.text
	if op == "<>" {
		op = "!="
	}

	return &node{kind: nBinary, name: op, args: []*node{left, right}, pos: t.pos}, true, nil
}

func (p *parser) keywordInfix(left *node, minBP int) (*node, bool, error) {
	t := p.peek()

	switch strings.ToUpper(t.text) {
	case "OR":
		return p.logical(left, "OR", bpOr, minBP)
	case litAnd:
		return p.logical(left, litAnd, bpAnd, minBP)
	case "IS", "IN", litLike, "BETWEEN":
		if bpCompare < minBP {
			return nil, false, nil
		}

		return p.predicate(left, false)
	case litNot:
		nx := p.peekNext()
		if bpCompare < minBP || (!nx.isKw("IN") && !nx.isKw(litLike) && !nx.isKw("BETWEEN")) {
			return nil, false, nil
		}

		p.i++

		return p.predicate(left, true)
	}

	return nil, false, nil
}

func (p *parser) logical(left *node, op string, bp, minBP int) (*node, bool, error) {
	if bp < minBP {
		return nil, false, nil
	}

	pos := p.advance().pos

	right, err := p.exprBP(bp + 1)
	if err != nil {
		return nil, false, err
	}

	return &node{kind: nBinary, name: op, args: []*node{left, right}, pos: pos}, true, nil
}

func (p *parser) predicate(left *node, not bool) (*node, bool, error) {
	t := p.advance()
	pos := t.pos

	switch strings.ToUpper(t.text) {
	case "IS":
		n, err := p.isPredicate(left, pos)

		return n, err == nil, err
	case "IN":
		n, err := p.inPredicate(left, not, pos)

		return n, err == nil, err
	case litLike:
		right, err := p.exprBP(bpBitOr)
		if err != nil {
			return nil, false, err
		}

		return &node{kind: nLike, not: not, args: []*node{left, right}, pos: pos}, true, nil
	default:
		lo, err := p.exprBP(bpBitOr)
		if err != nil {
			return nil, false, err
		}

		if err = p.expectKw(litAnd); err != nil {
			return nil, false, err
		}

		hi, err := p.exprBP(bpBitOr)
		if err != nil {
			return nil, false, err
		}

		return &node{kind: nBetween, not: not, args: []*node{left, lo, hi}, pos: pos}, true, nil
	}
}

func (p *parser) isPredicate(left *node, pos int) (*node, error) {
	not := p.acceptKw(litNot)
	n := &node{kind: nIs, not: not, args: []*node{left}, pos: pos}

	switch {
	case p.acceptKw(litNull):
		n.name = litNull
	case p.acceptKw(litTrue):
		n.name = litTrue
	case p.acceptKw(litFalse):
		n.name = litFalse
	case p.kw(litDistinct):
		p.i++

		if err := p.expectKw("FROM"); err != nil {
			return nil, err
		}

		right, err := p.exprBP(bpBitOr)
		if err != nil {
			return nil, err
		}

		n.name, n.args = litDistinct, append(n.args, right)
	default:
		return nil, p.unexpected("expected NULL, TRUE, FALSE or DISTINCT FROM after IS")
	}

	return n, nil
}

func (p *parser) inPredicate(left *node, not bool, pos int) (*node, error) {
	n := &node{kind: nIn, not: not, args: []*node{left}, pos: pos}

	if p.kw(litUnnest) {
		p.i++

		if err := p.expectPunct("("); err != nil {
			return nil, err
		}

		e, err := p.expr()
		if err != nil {
			return nil, err
		}

		n.name, n.args = litUnnest, append(n.args, e)

		return n, p.expectPunct(")")
	}

	if err := p.expectPunct("("); err != nil {
		return nil, err
	}

	if p.kw(litSelect) || p.kw("WITH") {
		q, err := p.query()
		if err != nil {
			return nil, err
		}

		n.query = q

		return n, p.expectPunct(")")
	}

	list, err := p.exprList()
	if err != nil {
		return nil, err
	}

	n.args = append(n.args, list...)

	return n, p.expectPunct(")")
}

func (p *parser) prefix() (*node, error) {
	t := p.peek()

	switch {
	case t.isPunct("-"), t.isPunct("+"), t.isPunct("~"):
		p.i++

		arg, err := p.exprBP(bpUnary)
		if err != nil {
			return nil, err
		}

		return &node{kind: nUnary, name: t.text, args: []*node{arg}, pos: t.pos}, nil
	case t.isKw(litNot):
		p.i++

		arg, err := p.exprBP(bpNot)
		if err != nil {
			return nil, err
		}

		return &node{kind: nUnary, name: litNot, args: []*node{arg}, pos: t.pos}, nil
	}

	n, err := p.primary()
	if err != nil {
		return nil, err
	}

	return p.postfix(n)
}

func (p *parser) postfix(n *node) (*node, error) {
	for {
		t := p.peek()

		switch {
		case t.isPunct(".") && n.kind != nPath:
			p.i++

			name, err := p.anyIdent()
			if err != nil {
				return nil, err
			}

			n = &node{kind: nField, name: name, args: []*node{n}, pos: t.pos}
		case t.isPunct("["):
			p.i++

			idx, err := p.subscript(n, t.pos)
			if err != nil {
				return nil, err
			}

			n = idx
		default:
			return n, nil
		}
	}
}

func (p *parser) subscript(base *node, pos int) (*node, error) {
	mode := ""

	if t := p.peek(); t.kind == tkIdent && p.peekNext().isPunct("(") {
		switch strings.ToUpper(t.text) {
		case "OFFSET", "ORDINAL", "SAFE_OFFSET", "SAFE_ORDINAL":
			mode = strings.ToUpper(t.text)
			p.i += 2
		}
	}

	idx, err := p.expr()
	if err != nil {
		return nil, err
	}

	if mode != "" {
		if err = p.expectPunct(")"); err != nil {
			return nil, err
		}
	}

	if err = p.expectPunct("]"); err != nil {
		return nil, err
	}

	return &node{kind: nIndex, name: mode, args: []*node{base, idx}, pos: pos}, nil
}

func (p *parser) primary() (*node, error) {
	t := p.peek()

	switch t.kind {
	case tkInt:
		p.i++

		return &node{kind: nInt, str: t.text, pos: t.pos}, nil
	case tkFloat:
		p.i++

		return &node{kind: nFloat, str: t.text, pos: t.pos}, nil
	case tkString:
		p.i++

		return &node{kind: nString, str: t.text, pos: t.pos}, nil
	case tkBytes:
		p.i++

		return &node{kind: nBytes, str: t.text, pos: t.pos}, nil
	case tkParam:
		p.i++

		return &node{kind: nParam, name: t.text, pos: t.pos}, nil
	case tkQuestion:
		p.i++
		p.nextQ++

		return &node{kind: nParam, index: p.nextQ, pos: t.pos}, nil
	case tkQuoted:
		return p.pathOrCall()
	case tkIdent:
		return p.identPrimary()
	case tkPunct:
		return p.punctPrimary()
	case tkEOF:
	}

	return nil, p.unexpected("unexpected token")
}

func (p *parser) punctPrimary() (*node, error) {
	t := p.peek()

	switch t.text {
	case "(":
		p.i++

		if p.kw(litSelect) || p.kw("WITH") {
			q, err := p.query()
			if err != nil {
				return nil, err
			}

			return &node{kind: nSubquery, query: q, pos: t.pos}, p.expectPunct(")")
		}

		first, err := p.expr()
		if err != nil {
			return nil, err
		}

		if p.acceptPunct(",") {
			rest, lerr := p.exprList()
			if lerr != nil {
				return nil, lerr
			}

			args := append([]*node{first}, rest...)

			return &node{kind: nStruct, args: args, names: make([]string, len(args)), pos: t.pos}, p.expectPunct(")")
		}

		return first, p.expectPunct(")")
	case "[":
		p.i++

		return p.arrayBody(nil, t.pos)
	}

	return nil, p.unexpected("unexpected token")
}

// arrayBody parses "a, b, c]" after the opening bracket.
func (p *parser) arrayBody(elem *Type, pos int) (*node, error) {
	n := &node{kind: nArray, elemType: elem, pos: pos}

	if p.acceptPunct("]") {
		return n, nil
	}

	list, err := p.exprList()
	if err != nil {
		return nil, err
	}

	n.args = list

	return n, p.expectPunct("]")
}

func (p *parser) identPrimary() (*node, error) {
	t := p.peek()
	up := strings.ToUpper(t.text)
	nx := p.peekNext()

	switch up {
	case litNull:
		p.i++

		return &node{kind: nNull, pos: t.pos}, nil
	case litTrue, litFalse:
		p.i++

		return &node{kind: nBool, name: strings.ToLower(up), pos: t.pos}, nil
	case "CASE":
		return p.caseExpr()
	case "CAST", "SAFE_CAST":
		if nx.isPunct("(") {
			return p.castExpr(up == "SAFE_CAST")
		}
	case "EXTRACT":
		if nx.isPunct("(") {
			return p.extractExpr()
		}
	case "EXISTS":
		if nx.isPunct("(") {
			return p.existsExpr()
		}
	case litArray:
		return p.arrayExpr()
	case litStruct:
		return p.structExpr()
	case "INTERVAL":
		return p.intervalExpr()
	case litDate, litTimestamp, litDatetime, litTime, litNumeric, litBignumeric, litDecimal, litJSON:
		if nx.kind == tkString {
			p.i += 2

			return &node{kind: nTyped, name: up, str: nx.text, pos: t.pos}, nil
		}
	}

	return p.pathOrCall()
}

func (p *parser) pathOrCall() (*node, error) {
	t := p.peek()
	p.i++

	parts := []string{t.text}

	for p.peek().isPunct(".") {
		nx := p.peekNext()
		if nx.kind != tkIdent && nx.kind != tkQuoted {
			break
		}

		p.i += 2

		parts = append(parts, nx.text)
	}

	if p.peek().isPunct("(") && t.kind == tkIdent {
		return p.callExpr(parts, t.pos)
	}

	if t.kind == tkIdent && reserved()[strings.ToUpper(t.text)] && len(parts) == 1 {
		p.i--

		return nil, p.unexpected("unexpected keyword")
	}

	return &node{kind: nPath, parts: parts, pos: t.pos}, nil
}

func (p *parser) callExpr(parts []string, pos int) (*node, error) {
	p.i++

	n := &node{kind: nCall, pos: pos}
	if len(parts) > 1 && strings.EqualFold(parts[0], "SAFE") {
		n.safe, parts = true, parts[1:]
	}

	n.name = strings.ToUpper(strings.Join(parts, "."))

	if p.acceptKw(litDistinct) {
		n.distinct = true
	}

	if err := p.callArgs(n); err != nil {
		return nil, err
	}

	if p.acceptKw("OVER") {
		w, err := p.windowSpec()
		if err != nil {
			return nil, err
		}

		n.over = w
	}

	return n, nil
}

func (p *parser) callArgs(n *node) error {
	if p.acceptPunct(")") {
		return nil
	}

	if p.peek().isPunct("*") && p.peekNext().isPunct(")") {
		p.i += 2
		n.args = []*node{{kind: nStar}}

		return nil
	}

	for {
		a, err := p.expr()
		if err != nil {
			return err
		}

		n.args = append(n.args, a)

		if !p.acceptPunct(",") {
			break
		}
	}

	return p.callSuffix(n)
}

func (p *parser) callSuffix(n *node) error {
	if p.acceptKw("IGNORE") {
		if err := p.expectKw("NULLS"); err != nil {
			return err
		}

		n.nulls = true
	} else if p.acceptKw("RESPECT") {
		if err := p.expectKw("NULLS"); err != nil {
			return err
		}
	}

	if p.kw("ORDER") {
		p.i++

		if err := p.expectKw("BY"); err != nil {
			return err
		}

		o, err := p.orderList()
		if err != nil {
			return err
		}

		n.order = o
	}

	if p.acceptKw("LIMIT") {
		l, err := p.expr()
		if err != nil {
			return err
		}

		n.limit = l
	}

	return p.expectPunct(")")
}

func (p *parser) windowSpec() (*windowSpec, error) {
	if err := p.expectPunct("("); err != nil {
		return nil, err
	}

	w := &windowSpec{}

	if p.kw("PARTITION") {
		p.i++

		if err := p.expectKw("BY"); err != nil {
			return nil, err
		}

		list, err := p.exprList()
		if err != nil {
			return nil, err
		}

		w.partition = list
	}

	if p.kw("ORDER") {
		p.i++

		if err := p.expectKw("BY"); err != nil {
			return nil, err
		}

		o, err := p.orderList()
		if err != nil {
			return nil, err
		}

		w.order = o
	}

	if p.kw("ROWS") || p.kw("RANGE") {
		w.frame = p.frameClause()
	}

	return w, p.expectPunct(")")
}

func (p *parser) frameClause() string {
	var parts []string

	parts = append(parts, strings.ToUpper(p.advance().text))

	for !p.peek().isPunct(")") && p.peek().kind != tkEOF {
		t := p.advance()

		if t.kind == tkInt {
			parts = append(parts, t.text)
		} else {
			parts = append(parts, strings.ToUpper(t.text))
		}
	}

	return strings.Join(parts, " ")
}

func (p *parser) caseExpr() (*node, error) {
	pos := p.advance().pos
	n := &node{kind: nCase, pos: pos}

	if !p.kw("WHEN") {
		op, err := p.expr()
		if err != nil {
			return nil, err
		}

		n.operand = true
		n.args = append(n.args, op)
	}

	for p.acceptKw("WHEN") {
		w, err := p.expr()
		if err != nil {
			return nil, err
		}

		if err = p.expectKw("THEN"); err != nil {
			return nil, err
		}

		th, err := p.expr()
		if err != nil {
			return nil, err
		}

		n.args = append(n.args, w, th)
	}

	if p.acceptKw("ELSE") {
		e, err := p.expr()
		if err != nil {
			return nil, err
		}

		n.hasElse = true
		n.args = append(n.args, e)
	}

	return n, p.expectKw("END")
}

func (p *parser) castExpr(safe bool) (*node, error) {
	pos := p.advance().pos
	p.i++

	e, err := p.expr()
	if err != nil {
		return nil, err
	}

	if err = p.expectKw("AS"); err != nil {
		return nil, err
	}

	t, err := p.typeSpec()
	if err != nil {
		return nil, err
	}

	if p.acceptKw("FORMAT") {
		return nil, syntaxErr(p.peek().pos, "CAST ... FORMAT is not supported")
	}

	return &node{kind: nCast, safe: safe, castTo: &t, args: []*node{e}, pos: pos}, p.expectPunct(")")
}

func (p *parser) extractExpr() (*node, error) {
	pos := p.advance().pos
	p.i++

	part, err := p.anyIdent()
	if err != nil {
		return nil, err
	}

	n := &node{kind: nExtract, name: strings.ToUpper(part), pos: pos}

	if p.peek().isPunct("(") {
		p.i++

		w, werr := p.anyIdent()
		if werr != nil {
			return nil, werr
		}

		n.name += "(" + strings.ToUpper(w) + ")"

		if err = p.expectPunct(")"); err != nil {
			return nil, err
		}
	}

	if err = p.expectKw("FROM"); err != nil {
		return nil, err
	}

	e, err := p.expr()
	if err != nil {
		return nil, err
	}

	n.args = []*node{e}

	if p.acceptKw("AT") {
		return nil, syntaxErr(p.peek().pos, "AT TIME ZONE is not supported")
	}

	return n, p.expectPunct(")")
}

func (p *parser) existsExpr() (*node, error) {
	pos := p.advance().pos
	p.i++

	q, err := p.query()
	if err != nil {
		return nil, err
	}

	return &node{kind: nExists, query: q, pos: pos}, p.expectPunct(")")
}

func (p *parser) arrayExpr() (*node, error) {
	pos := p.advance().pos

	var elem *Type

	if p.peek().isPunct("<") {
		p.i--

		t, err := p.typeSpec()
		if err != nil {
			return nil, err
		}

		if t.Elem != nil {
			elem = t.Elem
		}
	}

	switch {
	case p.acceptPunct("["):
		return p.arrayBody(elem, pos)
	case p.peek().isPunct("(") && elem == nil:
		p.i++

		q, err := p.query()
		if err != nil {
			return nil, err
		}

		return &node{kind: nArrayQuery, query: q, pos: pos}, p.expectPunct(")")
	}

	return nil, p.unexpected("expected [ after ARRAY")
}

func (p *parser) structExpr() (*node, error) {
	pos := p.advance().pos

	n := &node{kind: nStruct, pos: pos}

	if p.peek().isPunct("<") {
		p.i--

		t, err := p.typeSpec()
		if err != nil {
			return nil, err
		}

		n.castTo = &t
	}

	if err := p.expectPunct("("); err != nil {
		return nil, err
	}

	if p.acceptPunct(")") {
		return n, nil
	}

	for {
		e, err := p.expr()
		if err != nil {
			return nil, err
		}

		name := ""
		if p.acceptKw("AS") {
			if name, err = p.anyIdent(); err != nil {
				return nil, err
			}
		}

		n.args = append(n.args, e)
		n.names = append(n.names, name)

		if p.acceptPunct(")") {
			return n, nil
		}

		if err = p.expectPunct(","); err != nil {
			return nil, err
		}
	}
}

func (p *parser) intervalExpr() (*node, error) {
	pos := p.advance().pos

	v, err := p.exprBP(bpUnary)
	if err != nil {
		return nil, err
	}

	part, err := p.anyIdent()
	if err != nil {
		return nil, err
	}

	return &node{kind: nInterval, name: strings.ToUpper(part), args: []*node{v}, pos: pos}, nil
}

// typeSpec parses a GoogleSQL type: scalar, ARRAY<T> or STRUCT<name T, ...>.
func (p *parser) typeSpec() (Type, error) {
	if err := p.enter(); err != nil {
		return Type{}, err
	}
	defer p.leave()

	name, err := p.anyIdent()
	if err != nil {
		return Type{}, err
	}

	switch strings.ToUpper(name) {
	case litArray:
		return p.arrayType()
	case litStruct:
		return p.structType()
	}

	k, ok := scalarTypeKinds()[strings.ToUpper(name)]
	if !ok {
		return Type{}, syntaxErr(p.toks[p.i-1].pos, "unknown type %q", name)
	}

	if p.acceptPunct("(") {
		for !p.acceptPunct(")") {
			if p.peek().kind == tkEOF {
				return Type{}, p.unexpected("expected )")
			}

			p.i++
		}
	}

	return scalar(k), nil
}

func (p *parser) arrayType() (Type, error) {
	if err := p.expectPunct("<"); err != nil {
		return Type{}, err
	}

	elem, err := p.typeSpec()
	if err != nil {
		return Type{}, err
	}

	if err = p.closeAngle(); err != nil {
		return Type{}, err
	}

	return arrayOf(elem), nil
}

func (p *parser) structType() (Type, error) {
	t := Type{Kind: KindStruct}

	if err := p.expectPunct("<"); err != nil {
		return t, err
	}

	for i := 0; ; i++ {
		col := Column{Name: ""}

		if nx := p.peekNext(); p.peek().kind == tkIdent && !nx.isPunct(",") && !nx.isPunct(">") && !nx.isPunct(">>") &&
			!nx.isPunct("<") && !nx.isPunct("(") {
			col.Name = p.advance().text
		}

		ft, err := p.typeSpec()
		if err != nil {
			return t, err
		}

		col.Type = ft

		if col.Name == "" {
			col.Name = "_field_" + itoa(i+1)
		}

		t.Fields = append(t.Fields, col)

		if !p.acceptPunct(",") {
			break
		}
	}

	return t, p.closeAngle()
}

func (p *parser) closeAngle() error {
	if p.acceptPunct(">") {
		return nil
	}

	if p.peek().isPunct(">>") {
		p.toks[p.i].text = ">"

		return nil
	}

	return p.unexpected("expected >")
}

func scalarTypeKinds() map[string]Kind {
	return map[string]Kind{
		litInt64:      KindInt,
		"INT":         KindInt,
		"SMALLINT":    KindInt,
		litInteger:    KindInt,
		"BIGINT":      KindInt,
		"TINYINT":     KindInt,
		"BYTEINT":     KindInt,
		litFloat64:    KindFloat,
		litFloat:      KindFloat,
		litBool:       KindBool,
		litBoolean:    KindBool,
		litString:     KindString,
		litBytes:      KindBytes,
		litTimestamp:  KindTimestamp,
		litDate:       KindDate,
		litTime:       KindTime,
		litDatetime:   KindDatetime,
		litNumeric:    KindNumeric,
		litDecimal:    KindNumeric,
		litBignumeric: KindBigNumeric,
		"BIGDECIMAL":  KindBigNumeric,
		litJSON:       KindJSON,
		litGeography:  KindGeography,
	}
}
