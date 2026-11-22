package timestreamquery

import (
	"strconv"
	"strings"
)

type sqlParser struct {
	q     string
	toks  []token
	pos   int
	depth int
}

func parseSQL(q string) (*queryNode, error) {
	toks, err := tokenizeSQL(q)
	if err != nil {
		return nil, err
	}

	p := &sqlParser{q: q, toks: toks}

	qn, err := p.parseQuery()
	if err != nil {
		return nil, err
	}

	if p.peek().kind != tkEOF {
		return nil, errSQLSyntax
	}

	return qn, nil
}

func (p *sqlParser) peek() token { return p.toks[p.pos] }

func (p *sqlParser) peekAt(n int) token {
	if p.pos+n >= len(p.toks) {
		return p.toks[len(p.toks)-1]
	}

	return p.toks[p.pos+n]
}

func (p *sqlParser) next() token {
	t := p.toks[p.pos]
	if t.kind != tkEOF {
		p.pos++
	}

	return t
}

func (p *sqlParser) acceptKw(kw string) bool {
	if p.peek().is(kw) {
		p.pos++

		return true
	}

	return false
}

func (p *sqlParser) acceptOp(op string) bool {
	if p.peek().isOp(op) {
		p.pos++

		return true
	}

	return false
}

func (p *sqlParser) expectOp(op string) error {
	if !p.acceptOp(op) {
		return errSQLSyntax
	}

	return nil
}

func (p *sqlParser) expectKw(kw string) error {
	if !p.acceptKw(kw) {
		return errSQLSyntax
	}

	return nil
}

func (p *sqlParser) enter() error {
	p.depth++
	if p.depth > maxSQLDepth {
		return errSQLSyntax
	}

	return nil
}

func (p *sqlParser) leave() { p.depth-- }

func isReservedWord(lw string) bool {
	switch lw {
	case "from", kwWhere, "group", kwHaving, "order", kwLimit, "offset", "union", "intersect", "except", "on",
		"using", "join", "inner", "left", "right", "full", "cross", "natural", "window", "fetch", "as", "when",
		"then", "else", "end", "and", "or", opNotLower, "is", "in", "between", "like", "select", "with", "over":
		return true
	}

	return false
}

func (p *sqlParser) identName() (string, bool) {
	t := p.peek()
	if t.kind == tkQuoted || (t.kind == tkIdent && !isReservedWord(strings.ToLower(t.text))) {
		p.pos++

		return t.text, true
	}

	return "", false
}

func (p *sqlParser) parseQuery() (*queryNode, error) {
	if err := p.enter(); err != nil {
		return nil, err
	}
	defer p.leave()

	var ctes []cteDef

	if p.acceptKw("with") {
		if p.peek().is("recursive") {
			return nil, errSQLSyntax
		}

		for {
			c, err := p.parseCTE()
			if err != nil {
				return nil, err
			}

			ctes = append(ctes, c)

			if !p.acceptOp(",") {
				break
			}
		}
	}

	qn, err := p.parseSetExpr(1)
	if err != nil {
		return nil, err
	}

	qn.ctes = append(ctes, qn.ctes...)

	if err = p.parseTail(qn); err != nil {
		return nil, err
	}

	return qn, nil
}

func (p *sqlParser) parseCTE() (cteDef, error) {
	name, ok := p.identName()
	if !ok {
		return cteDef{}, errSQLSyntax
	}

	def := cteDef{name: name}

	if p.acceptOp("(") {
		cols, err := p.parseNameList()
		if err != nil {
			return def, err
		}

		def.cols = cols
	}

	if err := p.expectKw("as"); err != nil {
		return def, err
	}

	if err := p.expectOp("("); err != nil {
		return def, err
	}

	qn, err := p.parseQuery()
	if err != nil {
		return def, err
	}

	def.query = qn

	return def, p.expectOp(")")
}

// parseNameList reads identifiers up to the closing paren (the opening one is consumed).
func (p *sqlParser) parseNameList() ([]string, error) {
	var out []string

	for {
		n, ok := p.identName()
		if !ok {
			return nil, errSQLSyntax
		}

		out = append(out, n)

		if p.acceptOp(")") {
			return out, nil
		}

		if err := p.expectOp(","); err != nil {
			return nil, err
		}
	}
}

func setOpPrec(t token) int {
	switch {
	case t.is("intersect"):
		return argsTwo
	case t.is("union"), t.is("except"):
		return 1
	}

	return 0
}

func (p *sqlParser) parseSetExpr(minPrec int) (*queryNode, error) {
	left, err := p.parseSetPrimary()
	if err != nil {
		return nil, err
	}

	for {
		prec := setOpPrec(p.peek())
		if prec == 0 || prec < minPrec {
			return left, nil
		}

		op := strings.ToUpper(p.next().text)
		if p.peek().is("all") || p.peek().is("distinct") {
			p.pos++
		}

		right, rerr := p.parseSetExpr(prec + 1)
		if rerr != nil {
			return nil, rerr
		}

		left = &queryNode{left: left, right: right, setOp: op}
	}
}

func (p *sqlParser) parseSetPrimary() (*queryNode, error) {
	if p.peek().isOp("(") {
		p.pos++

		qn, err := p.parseQuery()
		if err != nil {
			return nil, err
		}

		return qn, p.expectOp(")")
	}

	if p.peek().is("values") {
		rel, err := p.parseValues()
		if err != nil {
			return nil, err
		}

		return &queryNode{sel: &selectSpec{items: []selectItem{{star: true}}, from: rel}}, nil
	}

	return p.parseSelect()
}

func (p *sqlParser) parseTail(qn *queryNode) error {
	if p.acceptKw("order") {
		if err := p.parseOrderBy(qn); err != nil {
			return err
		}
	}

	for p.peek().is("offset") || p.peek().is("limit") {
		p.pos++

		if p.acceptKw("all") {
			continue
		}

		e, err := p.parseExpr()
		if err != nil {
			return err
		}

		qn.limit = append(qn.limit, e)
	}

	return nil
}

func (p *sqlParser) parseOrderBy(qn *queryNode) error {
	if err := p.expectKw("by"); err != nil {
		return err
	}

	for {
		e, err := p.parseExpr()
		if err != nil {
			return err
		}

		qn.orderBy = append(qn.orderBy, orderItem{expr: e})

		if p.peek().is("asc") || p.peek().is("desc") {
			p.pos++
		}

		if p.acceptKw("nulls") {
			p.pos++
		}

		if !p.acceptOp(",") {
			return nil
		}
	}
}

func (p *sqlParser) parseSelect() (*queryNode, error) {
	if err := p.expectKw("select"); err != nil {
		return nil, err
	}

	sel := &selectSpec{}

	if p.acceptKw("distinct") {
		sel.distinct = true
	} else {
		p.acceptKw("all")
	}

	for {
		it, err := p.parseSelectItem()
		if err != nil {
			return nil, err
		}

		sel.items = append(sel.items, it)

		if !p.acceptOp(",") {
			break
		}
	}

	if err := p.parseSelectClauses(sel); err != nil {
		return nil, err
	}

	return &queryNode{sel: sel}, nil
}

func (p *sqlParser) parseSelectClauses(sel *selectSpec) error {
	var err error

	if p.acceptKw("from") {
		if sel.from, err = p.parseFrom(); err != nil {
			return err
		}
	}

	if p.acceptKw("where") {
		if sel.where, err = p.parseExpr(); err != nil {
			return err
		}
	}

	if p.acceptKw("group") {
		if sel.groupBy, err = p.parseGroupBy(); err != nil {
			return err
		}
	}

	if p.acceptKw("having") {
		if sel.having, err = p.parseExpr(); err != nil {
			return err
		}
	}

	if p.acceptKw("window") {
		return p.skipWindowDefs()
	}

	return nil
}

// skipWindowDefs consumes `name AS (spec) [, ...]`; window specs never change a column's type.
func (p *sqlParser) skipWindowDefs() error {
	for {
		if _, ok := p.identNameAny(); !ok {
			return errSQLSyntax
		}

		if err := p.expectKw("as"); err != nil {
			return err
		}

		if err := p.skipBalanced(); err != nil {
			return err
		}

		if !p.acceptOp(",") {
			return nil
		}
	}
}

func (p *sqlParser) parseGroupBy() ([]*node, error) {
	if err := p.expectKw("by"); err != nil {
		return nil, err
	}

	p.acceptKw("distinct")

	var out []*node

	for {
		if p.peek().is("grouping") && p.peekAt(1).is("sets") {
			p.pos += argsTwo

			if err := p.skipBalanced(); err != nil {
				return nil, err
			}

			if !p.acceptOp(",") {
				return out, nil
			}

			continue
		}

		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}

		out = append(out, e)

		if !p.acceptOp(",") {
			return out, nil
		}
	}
}

func (p *sqlParser) parseSelectItem() (selectItem, error) {
	if p.peek().isOp("*") {
		p.pos++

		return selectItem{star: true}, nil
	}

	if t := p.peek(); (t.kind == tkIdent || t.kind == tkQuoted) && p.starQualifier() {
		var qual []string

		for !p.peek().isOp("*") {
			qual = append(qual, p.next().text)
			p.acceptOp(".")
		}

		p.pos++

		return selectItem{star: true, qual: qual}, nil
	}

	e, err := p.parseExpr()
	if err != nil {
		return selectItem{}, err
	}

	it := selectItem{expr: e}

	if p.acceptKw("as") {
		n, ok := p.identNameAny()
		if !ok {
			return it, errSQLSyntax
		}

		it.alias = n
	} else if n, ok := p.identName(); ok {
		it.alias = n
	}

	return it, nil
}

func (p *sqlParser) identNameAny() (string, bool) {
	t := p.peek()
	if t.kind == tkQuoted || t.kind == tkIdent {
		p.pos++

		return t.text, true
	}

	return "", false
}

// starQualifier reports whether the upcoming tokens are `ident(.ident)* . *`.
func (p *sqlParser) starQualifier() bool {
	for i := 0; ; i += argsTwo {
		t := p.peekAt(i)
		if t.kind != tkIdent && t.kind != tkQuoted {
			return false
		}

		if !p.peekAt(i + 1).isOp(".") {
			return false
		}

		if p.peekAt(i + argsTwo).isOp("*") {
			return true
		}
	}
}

func (p *sqlParser) parseValues() (*relation, error) {
	p.pos++

	rel := &relation{kind: relValues}

	for {
		if err := p.expectOp("("); err != nil {
			return nil, err
		}

		row, err := p.parseExprList(")")
		if err != nil {
			return nil, err
		}

		rel.rows = append(rel.rows, row)

		if !p.acceptOp(",") {
			return rel, nil
		}
	}
}

// parseExprList reads comma-separated expressions up to and including closer.
func (p *sqlParser) parseExprList(closer string) ([]*node, error) {
	var out []*node

	if p.acceptOp(closer) {
		return out, nil
	}

	for {
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}

		out = append(out, e)

		if p.acceptOp(closer) {
			return out, nil
		}

		if err = p.expectOp(","); err != nil {
			return nil, err
		}
	}
}

func (p *sqlParser) parseFrom() (*relation, error) {
	left, err := p.parseJoined()
	if err != nil {
		return nil, err
	}

	for p.acceptOp(",") {
		right, rerr := p.parseJoined()
		if rerr != nil {
			return nil, rerr
		}

		left = &relation{kind: relJoin, left: left, right: right, joinType: "CROSS"}
	}

	return left, nil
}

func (p *sqlParser) joinKind() (string, bool) {
	t := p.peek()

	switch {
	case t.is("join"):
		p.pos++

		return "INNER", true
	case t.is("inner"), t.is("cross"):
		p.pos++

		return strings.ToUpper(t.text), p.expectKw("join") == nil
	case t.is("left"), t.is("right"), t.is("full"):
		p.pos++
		p.acceptKw("outer")

		return strings.ToUpper(t.text), p.expectKw("join") == nil
	}

	return "", false
}

func (p *sqlParser) parseJoined() (*relation, error) {
	left, err := p.parsePrimaryRel()
	if err != nil {
		return nil, err
	}

	for {
		kind, ok := p.joinKind()
		if !ok {
			return left, nil
		}

		right, rerr := p.parsePrimaryRel()
		if rerr != nil {
			return nil, rerr
		}

		j := &relation{kind: relJoin, left: left, right: right, joinType: kind}

		switch {
		case p.acceptKw("on"):
			if j.on, rerr = p.parseExpr(); rerr != nil {
				return nil, rerr
			}
		case p.acceptKw("using"):
			if rerr = p.expectOp("("); rerr != nil {
				return nil, rerr
			}

			if j.using, rerr = p.parseNameList(); rerr != nil {
				return nil, rerr
			}
		}

		left = j
	}
}

func (p *sqlParser) parseRelAlias(rel *relation) error {
	p.acceptKw("as")

	if n, ok := p.identName(); ok {
		rel.alias = n

		if p.acceptOp("(") {
			cols, err := p.parseNameList()
			if err != nil {
				return err
			}

			rel.colAliases = cols
		}
	}

	return nil
}

func (p *sqlParser) parsePrimaryRel() (*relation, error) {
	if err := p.enter(); err != nil {
		return nil, err
	}
	defer p.leave()

	t := p.peek()

	switch {
	case t.isOp("("):
		return p.parseParenRel()
	case t.is("unnest"):
		p.pos++

		if err := p.expectOp("("); err != nil {
			return nil, err
		}

		args, err := p.parseExprList(")")
		if err != nil {
			return nil, err
		}

		rel := &relation{kind: relUnnest, args: args}

		if p.acceptKw("with") {
			if err = p.expectKw("ordinality"); err != nil {
				return nil, err
			}

			rel.ordinality = true
		}

		return rel, p.parseRelAlias(rel)
	case t.kind == tkIdent || t.kind == tkQuoted:
		var ref []string

		for {
			n, ok := p.identNameAny()
			if !ok {
				return nil, errSQLSyntax
			}

			ref = append(ref, n)

			if !p.acceptOp(".") {
				break
			}
		}

		rel := &relation{kind: relTable, ref: ref}

		return rel, p.parseRelAlias(rel)
	}

	return nil, errSQLSyntax
}

func (p *sqlParser) parseParenRel() (*relation, error) {
	p.pos++

	if p.peek().is("select") || p.peek().is("with") || p.peek().is("values") || p.peek().isOp("(") {
		qn, err := p.parseQuery()
		if err != nil {
			return nil, err
		}

		if err = p.expectOp(")"); err != nil {
			return nil, err
		}

		rel := &relation{kind: relSubquery, sub: qn}

		return rel, p.parseRelAlias(rel)
	}

	inner, err := p.parseFrom()
	if err != nil {
		return nil, err
	}

	return inner, p.expectOp(")")
}

func atoiSafe(s string) (int, bool) {
	n, err := strconv.Atoi(s)

	return n, err == nil
}
