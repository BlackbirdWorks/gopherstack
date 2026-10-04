package iot

import (
	"fmt"
	"slices"
	"strings"
)

// sqlParser is a recursive-descent parser for the AWS IoT SQL dialect.
// Errors latch: the first one is kept and the token stream is forced to EOF.
type sqlParser struct {
	err     error
	funcs   map[string]funcDef
	lex     sqlLexer
	tok     sqlToken
	prevEnd int
	v2016   bool
}

func newSQLParser(src string, v2016 bool) *sqlParser {
	p := &sqlParser{lex: sqlLexer{src: src}, v2016: v2016, funcs: funcTable()}
	p.advance()

	return p
}

func (p *sqlParser) advance() {
	p.prevEnd = p.lex.pos
	p.tok = p.lex.next()

	if p.tok.kind == tokBad {
		p.fail(p.tok.text)
	}
}

func (p *sqlParser) fail(msg string) {
	if p.err == nil {
		p.err = fmt.Errorf("%w: %s at position %d", ErrSQLParse, msg, p.tok.pos)
	}

	p.tok = sqlToken{kind: tokEOF, pos: p.tok.pos}
}

func (p *sqlParser) isPunct(s string) bool { return p.tok.kind == tokPunct && p.tok.text == s }

func (p *sqlParser) isKw(kw string) bool { return p.tok.kind == tokIdent && kwEqual(p.tok.text, kw) }

func (p *sqlParser) acceptPunct(s string) bool {
	if p.isPunct(s) {
		p.advance()

		return true
	}

	return false
}

func (p *sqlParser) acceptKw(kw string) bool {
	if p.isKw(kw) {
		p.advance()

		return true
	}

	return false
}

func (p *sqlParser) expectPunct(s string) {
	if !p.acceptPunct(s) {
		p.fail("expected " + s)
	}
}

func (p *sqlParser) expectKw(kw string) {
	if !p.acceptKw(kw) {
		p.fail("expected " + kw)
	}
}

func (p *sqlParser) ident() string {
	if p.tok.kind != tokIdent {
		p.fail("expected identifier")

		return ""
	}

	s := p.tok.text
	p.advance()

	return s
}

// parseTopStatement parses SELECT ... FROM <topic filter> [WHERE ...] to EOF.
func (p *sqlParser) parseTopStatement() (*selectStmt, string) {
	stmt := p.parseSelectHead()
	topic := ""

	if p.expectFromKw() {
		topic = p.parseTopicFilter()
	}

	if p.acceptKw("WHERE") {
		stmt.where = p.parseExpr()
	}

	if p.tok.kind != tokEOF {
		p.fail("unexpected " + p.tok.text)
	}

	return stmt, topic
}

func (p *sqlParser) expectFromKw() bool {
	if p.isKw("FROM") {
		return true
	}

	p.fail("expected FROM")

	return false
}

// parseTopicFilter reads the FROM operand straight off the source: topics are not tokens.
func (p *sqlParser) parseTopicFilter() string {
	topic, ok := p.lex.topicFilter()
	if !ok {
		p.fail("expected topic filter after FROM")

		return ""
	}

	if !validTopicFilter(topic) {
		p.fail("invalid topic filter " + topic)

		return ""
	}

	p.advance()

	return topic
}

func validTopicFilter(f string) bool {
	segs := strings.Split(f, "/")

	for i, s := range segs {
		switch {
		case s == "#" && i != len(segs)-1:
			return false
		case s != "+" && s != "#" && strings.ContainsAny(s, "+#"):
			return false
		}
	}

	return f != ""
}

func (p *sqlParser) parseSelectHead() *selectStmt {
	p.expectKw("SELECT")

	stmt := &selectStmt{}
	if p.isKw("VALUE") && !p.valueIsField() {
		p.advance()

		stmt.value = true
	}

	for {
		stmt.items = append(stmt.items, p.parseItem())

		if !p.acceptPunct(",") {
			break
		}
	}

	if stmt.value && len(stmt.items) != 1 {
		p.fail("SELECT VALUE takes a single expression")
	}

	return stmt
}

// valueIsField reports whether a leading VALUE word is a field name, not the keyword.
func (p *sqlParser) valueIsField() bool {
	save := p.lex
	next := p.lex.next()
	p.lex = save

	if next.kind == tokIdent {
		return kwEqual(next.text, "FROM") || kwEqual(next.text, "AS")
	}

	return next.kind == tokPunct && slices.Contains([]string{",", ".", "["}, next.text)
}

func (p *sqlParser) parseItem() selectItem {
	if p.isPunct("*") {
		p.advance()

		return selectItem{star: true}
	}

	start := p.tok.pos
	expr := p.parseExpr()
	text := strings.TrimSpace(p.lex.src[start:p.prevEnd])

	if p.acceptKw("AS") {
		return selectItem{expr: expr, alias: p.aliasPath()}
	}

	if path := keyPath(expr); path != nil {
		return selectItem{expr: expr, alias: path}
	}

	return selectItem{expr: expr, alias: []string{text}}
}

func (p *sqlParser) aliasPath() []string {
	path := []string{p.ident()}

	for p.acceptPunct(".") {
		path = append(path, p.ident())
	}

	return path
}

// keyPath is the field path of a plain field reference, or nil.
func keyPath(n sqlNode) []string {
	switch t := n.(type) {
	case identNode:
		return []string{t.name}
	case memberNode:
		if base := keyPath(t.base); base != nil {
			return append(base, t.name)
		}
	case indexNode:
		return keyPath(t.base)
	}

	return nil
}

func (p *sqlParser) parseSubSelect() sqlNode {
	if !p.v2016 {
		p.fail("nested SELECT needs SQL version 2016-03-23")

		return litNode{sqlUndefined{}}
	}

	stmt := p.parseSelectHead()

	if p.expectFromKw() {
		p.advance()
		stmt.from = p.parsePostfix()

		if p.acceptKw("AS") {
			stmt.fromAlias = p.ident()
		}
	}

	if p.acceptKw("WHERE") {
		stmt.where = p.parseExpr()
	}

	return subSelectNode{stmt: stmt}
}

func (p *sqlParser) parseExpr() sqlNode { return p.parseOr() }

func (p *sqlParser) parseOr() sqlNode {
	l := p.parseAnd()

	for p.acceptKw("OR") {
		l = binaryNode{l: l, r: p.parseAnd(), op: opOr}
	}

	return l
}

func (p *sqlParser) parseAnd() sqlNode {
	l := p.parseNot()

	for p.acceptKw("AND") {
		l = binaryNode{l: l, r: p.parseNot(), op: opAnd}
	}

	return l
}

func (p *sqlParser) parseNot() sqlNode {
	if p.acceptKw("NOT") {
		return unaryNode{x: p.parseNot(), op: opNot}
	}

	return p.parseCmp()
}

func (p *sqlParser) parseCmp() sqlNode {
	l := p.parseAdd()

	if p.acceptKw("IN") {
		return inNode{l: l, r: p.parseAdd()}
	}

	if p.tok.kind == tokPunct {
		switch op := p.tok.text; op {
		case "=", "<>", "!=", "<", "<=", ">", ">=":
			p.advance()

			return binaryNode{l: l, r: p.parseAdd(), op: op}
		}
	}

	return l
}

func (p *sqlParser) parseAdd() sqlNode {
	l := p.parseMul()

	for p.isPunct("+") || p.isPunct("-") {
		op := p.tok.text
		p.advance()
		l = binaryNode{l: l, r: p.parseMul(), op: op}
	}

	return l
}

func (p *sqlParser) parseMul() sqlNode {
	l := p.parseUnary()

	for p.isPunct("*") || p.isPunct("/") || p.isPunct("%") {
		op := p.tok.text
		p.advance()
		l = binaryNode{l: l, r: p.parseUnary(), op: op}
	}

	return l
}

func (p *sqlParser) parseUnary() sqlNode {
	if p.acceptPunct("-") {
		return unaryNode{x: p.parseUnary(), op: "-"}
	}

	return p.parsePostfix()
}

func (p *sqlParser) parsePostfix() sqlNode {
	n := p.parsePrimary()

	for {
		switch {
		case p.acceptPunct("."):
			n = memberNode{base: n, name: p.ident()}
		case p.acceptPunct("["):
			idx := p.parseExpr()
			p.expectPunct("]")

			n = indexNode{base: n, idx: idx}
		default:
			return n
		}
	}
}

func (p *sqlParser) parsePrimary() sqlNode {
	switch p.tok.kind {
	case tokNumber:
		v := sqlNumber(p.tok.text, p.v2016)
		p.advance()

		return litNode{v}
	case tokString:
		s := p.tok.text
		p.advance()

		return litNode{s}
	case tokIdent:
		return p.parseWord()
	case tokPunct:
		return p.parsePunctPrimary()
	case tokEOF, tokBad:
	}

	p.fail("expected expression")

	return litNode{sqlUndefined{}}
}

func (p *sqlParser) parsePunctPrimary() sqlNode {
	switch {
	case p.acceptPunct("("):
		var n sqlNode
		if p.isKw("SELECT") {
			n = p.parseSubSelect()
		} else {
			n = p.parseExpr()
		}

		p.expectPunct(")")

		return n
	case p.acceptPunct("["):
		return p.parseArrayLit()
	case p.acceptPunct("{"):
		return p.parseObjectLit()
	}

	p.fail("unexpected " + p.tok.text)

	return litNode{sqlUndefined{}}
}

func (p *sqlParser) parseArrayLit() sqlNode {
	var items []sqlNode

	for !p.isPunct("]") && p.err == nil {
		items = append(items, p.parseExpr())

		if !p.acceptPunct(",") {
			break
		}
	}

	p.expectPunct("]")

	return arrayNode{items: items}
}

func (p *sqlParser) parseObjectLit() sqlNode {
	var n objectNode

	for !p.isPunct("}") && p.err == nil {
		if p.tok.kind != tokString && p.tok.kind != tokIdent {
			p.fail("expected object key")

			break
		}

		n.keys = append(n.keys, p.tok.text)
		p.advance()
		p.expectPunct(":")
		n.vals = append(n.vals, p.parseExpr())

		if !p.acceptPunct(",") {
			break
		}
	}

	p.expectPunct("}")

	return n
}

func (p *sqlParser) parseWord() sqlNode {
	word := p.tok.text

	switch strings.ToUpper(word) {
	case "TRUE":
		p.advance()

		return litNode{true}
	case "FALSE":
		p.advance()

		return litNode{false}
	case "NULL":
		p.advance()

		return litNode{nil}
	case "UNDEFINED":
		p.advance()

		return litNode{sqlUndefined{}}
	case "CASE":
		p.advance()

		return p.parseCase()
	case "EXISTS":
		p.advance()

		return existsNode{x: p.parsePostfix()}
	}

	p.advance()

	if p.isPunct("(") {
		return p.parseCall(word)
	}

	return identNode{name: word}
}

func (p *sqlParser) parseCase() sqlNode {
	n := caseNode{subject: p.parseExpr()}

	for p.acceptKw("WHEN") {
		test := p.parseExpr()
		p.expectKw("THEN")
		n.whens = append(n.whens, caseWhen{test: test, result: p.parseExpr()})
	}

	if len(n.whens) == 0 {
		p.fail("CASE needs at least one WHEN")
	}

	if p.acceptKw("ELSE") {
		n.els = p.parseExpr()
	}

	p.expectKw("END")

	return n
}

func (p *sqlParser) parseCall(name string) sqlNode {
	def, ok := p.funcs[strings.ToLower(name)]

	switch {
	case !ok:
		p.fail("unknown or unsupported function " + name + "()")

		return litNode{sqlUndefined{}}
	case def.since2016 && !p.v2016:
		p.fail(name + "() needs SQL version 2016-03-23")

		return litNode{sqlUndefined{}}
	}

	p.expectPunct("(")

	if strings.EqualFold(name, "cast") {
		return p.parseCast()
	}

	var args []sqlNode

	for !p.isPunct(")") && p.err == nil {
		args = append(args, p.parseArg(strings.EqualFold(name, "encode") && len(args) == 0))

		if !p.acceptPunct(",") {
			break
		}
	}

	p.expectPunct(")")

	return callNode{fn: def.impl, args: args}
}

func (p *sqlParser) parseArg(rawStar bool) sqlNode {
	if p.isPunct("*") {
		p.advance()

		return starNode{raw: rawStar}
	}

	return p.parseExpr()
}

func (p *sqlParser) parseCast() sqlNode {
	x := p.parseExpr()
	p.expectKw("AS")

	typ := strings.ToLower(p.ident())
	if !validCastType(typ, p.v2016) {
		p.fail("unsupported cast type " + typ)
	}

	p.expectPunct(")")

	return callNode{fn: castFunc(typ), args: []sqlNode{x}}
}
