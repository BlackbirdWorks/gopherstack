package bqsql

import (
	"strings"
)

type parser struct {
	src   string
	toks  []token
	i     int
	nextQ int
	depth int
}

const maxParseDepth = 200

func reserved() map[string]bool {
	words := []string{
		litSelect,
		"FROM",
		"WHERE",
		"GROUP",
		"BY",
		"HAVING",
		"ORDER",
		"LIMIT",
		"OFFSET",
		litUnion,
		litIntersect,
		litExcept,
		"JOIN",
		litInner,
		litLeft,
		litRight,
		"FULL",
		litCross,
		"ON",
		"USING",
		"WINDOW",
		"QUALIFY",
		"AS",
		"WITH",
		"WHEN",
		"THEN",
		"ELSE",
		"END",
		litAnd,
		"OR",
		litNot,
		"IN",
		"IS",
		litLike,
		"BETWEEN",
		"SET",
		"VALUES",
		"INTO",
		"OVER",
		"TABLESAMPLE",
		"FOR",
		"NATURAL",
		"CASE",
		"CAST",
		litNull,
		litTrue,
		litFalse,
		litDistinct,
		"ALL",
		"ASC",
		"DESC",
		"EXISTS",
		litArray,
		litStruct,
		"INTERVAL",
		"ROWS",
		"RANGE",
		"PARTITION",
		litUnnest,
		"LATERAL",
		"NEW",
		"DEFAULT",
		"ESCAPE",
		"COLLATE",
		"CURRENT",
		"PRECEDING",
		"FOLLOWING",
		"UNBOUNDED",
		"OUTER",
		"ANY",
		"SOME",
		"IF",
	}
	m := make(map[string]bool, len(words))

	for _, w := range words {
		m[w] = true
	}

	return m
}

func (p *parser) peek() token { return p.toks[p.i] }

func (p *parser) peekNext() token {
	if p.i+1 >= len(p.toks) {
		return p.toks[len(p.toks)-1]
	}

	return p.toks[p.i+1]
}

func (p *parser) advance() token {
	t := p.toks[p.i]
	if t.kind != tkEOF {
		p.i++
	}

	return t
}

func (p *parser) kw(w string) bool { return p.peek().isKw(w) }

func (p *parser) acceptKw(w string) bool {
	if p.kw(w) {
		p.i++

		return true
	}

	return false
}

func (p *parser) acceptPunct(s string) bool {
	if p.peek().isPunct(s) {
		p.i++

		return true
	}

	return false
}

func (p *parser) expectKw(w string) error {
	if !p.acceptKw(w) {
		return p.unexpected("expected " + w)
	}

	return nil
}

func (p *parser) expectPunct(s string) error {
	if !p.acceptPunct(s) {
		return p.unexpected("expected \"" + s + "\"")
	}

	return nil
}

func (p *parser) unexpected(msg string) error {
	t := p.peek()
	if t.kind == tkEOF {
		return syntaxErr(t.pos, "%s but got end of query", msg)
	}

	return syntaxErr(t.pos, "%s but got %q", msg, t.text)
}

func (p *parser) enter() error {
	p.depth++
	if p.depth > maxParseDepth {
		return syntaxErr(p.peek().pos, "expression nesting too deep")
	}

	return nil
}

func (p *parser) leave() { p.depth-- }

// identName reads an identifier token (plain or quoted) and returns its text.
func (p *parser) identName() (string, error) {
	t := p.peek()
	if t.kind == tkQuoted || t.kind == tkIdent && !reserved()[strings.ToUpper(t.text)] {
		p.i++

		return t.text, nil
	}

	return "", p.unexpected("expected identifier")
}

// anyIdent reads any identifier token including reserved words (for member names after a dot).
func (p *parser) anyIdent() (string, error) {
	t := p.peek()
	if t.kind == tkQuoted || t.kind == tkIdent {
		p.i++

		return t.text, nil
	}

	return "", p.unexpected("expected identifier")
}

// ParseStatement parses a single GoogleSQL statement.
func ParseStatement(sql string) (*Statement, error) {
	toks, err := lex(sql)
	if err != nil {
		return nil, err
	}

	p := &parser{src: sql, toks: toks}

	st, err := p.statement()
	if err != nil {
		return nil, err
	}

	p.acceptPunct(";")

	if p.peek().kind != tkEOF {
		return nil, p.unexpected("unexpected input after statement; scripts with several statements are not supported,")
	}

	st.Text = sql

	return st, nil
}

func (p *parser) statement() (*Statement, error) {
	switch {
	case p.kw(litSelect), p.kw("WITH"), p.peek().isPunct("("):
		q, err := p.query()
		if err != nil {
			return nil, err
		}

		return &Statement{Kind: StmtQuery, query: q}, nil
	case p.kw(litInsert):
		return p.insertStmt()
	case p.kw(litUpdate):
		return p.updateStmt()
	case p.kw(litDelete):
		return p.deleteStmt()
	case p.kw("MERGE"):
		return p.mergeStmt()
	case p.kw("CREATE"):
		return p.createStmt()
	case p.kw("DROP"):
		return p.dropStmt()
	case p.kw("TRUNCATE"):
		return p.truncateStmt()
	case p.kw("ALTER"):
		return p.alterStmt()
	}

	return nil, p.unexpected("expected a statement")
}

// query parses [WITH ...] set-expression [ORDER BY] [LIMIT].
func (p *parser) query() (*Query, error) {
	if err := p.enter(); err != nil {
		return nil, err
	}
	defer p.leave()

	q := &Query{}

	if p.acceptKw("WITH") {
		if err := p.withClauses(q); err != nil {
			return nil, err
		}
	}

	body, err := p.setExpr()
	if err != nil {
		return nil, err
	}

	q.body = body

	if err = p.orderLimit(q); err != nil {
		return nil, err
	}

	return q, nil
}

func (p *parser) withClauses(q *Query) error {
	p.acceptKw("RECURSIVE")

	for {
		name, err := p.identName()
		if err != nil {
			return err
		}

		c := cte{name: name}

		if p.acceptPunct("(") {
			c.cols, err = p.identList()
			if err != nil {
				return err
			}
		}

		if err = p.expectKw("AS"); err != nil {
			return err
		}

		if err = p.expectPunct("("); err != nil {
			return err
		}

		c.query, err = p.query()
		if err != nil {
			return err
		}

		if err = p.expectPunct(")"); err != nil {
			return err
		}

		q.with = append(q.with, c)

		if !p.acceptPunct(",") {
			return nil
		}
	}
}

// identList parses "a, b, c)" after the opening parenthesis.
func (p *parser) identList() ([]string, error) {
	var out []string

	for {
		n, err := p.identName()
		if err != nil {
			return nil, err
		}

		out = append(out, n)

		if p.acceptPunct(")") {
			return out, nil
		}

		if err = p.expectPunct(","); err != nil {
			return nil, err
		}
	}
}

func (p *parser) orderLimit(q *Query) error {
	var err error

	if p.kw("ORDER") {
		p.i++

		if err = p.expectKw("BY"); err != nil {
			return err
		}

		q.order, err = p.orderList()
		if err != nil {
			return err
		}
	}

	if p.acceptKw("LIMIT") {
		if q.limit, err = p.expr(); err != nil {
			return err
		}

		if p.acceptKw("OFFSET") {
			if q.offset, err = p.expr(); err != nil {
				return err
			}
		}
	}

	return nil
}

func (p *parser) orderList() ([]orderItem, error) {
	var out []orderItem

	for {
		e, err := p.expr()
		if err != nil {
			return nil, err
		}

		it := orderItem{expr: e}

		if p.acceptKw("DESC") {
			it.desc = true
		} else {
			p.acceptKw("ASC")
		}

		if p.acceptKw("NULLS") {
			if !p.acceptKw("FIRST") && !p.acceptKw("LAST") {
				return nil, p.unexpected("expected FIRST or LAST")
			}
		}

		out = append(out, it)

		if !p.acceptPunct(",") {
			return out, nil
		}
	}
}

func (p *parser) setExpr() (*setExpr, error) {
	left, err := p.setOperand()
	if err != nil {
		return nil, err
	}

	for {
		var op string

		switch {
		case p.kw(litUnion):
			op = litUnion
		case p.kw(litIntersect):
			op = litIntersect
		case p.kw(litExcept) && !p.peekNext().isPunct("("):
			op = litExcept
		default:
			return left, nil
		}

		p.i++

		var mode string

		switch {
		case p.acceptKw("ALL"):
			mode = "ALL"
		case p.acceptKw(litDistinct):
			mode = litDistinct
		default:
			return nil, p.unexpected("expected ALL or DISTINCT after " + op)
		}

		right, rerr := p.setOperand()
		if rerr != nil {
			return nil, rerr
		}

		left = &setExpr{op: op + " " + mode, left: left, right: right}
	}
}

func (p *parser) setOperand() (*setExpr, error) {
	if p.acceptPunct("(") {
		q, err := p.query()
		if err != nil {
			return nil, err
		}

		if err = p.expectPunct(")"); err != nil {
			return nil, err
		}

		return &setExpr{sub: q}, nil
	}

	s, err := p.selectStmt()
	if err != nil {
		return nil, err
	}

	return &setExpr{sel: s}, nil
}

func (p *parser) selectStmt() (*selectStmt, error) {
	if err := p.expectKw(litSelect); err != nil {
		return nil, err
	}

	s := &selectStmt{}

	if p.acceptKw(litDistinct) {
		s.distinct = true
	} else {
		p.acceptKw("ALL")
	}

	if p.kw("AS") && p.peekNext().isKw(litStruct) {
		p.i += 2
		s.asStruct = true
	}

	var err error

	if s.items, err = p.selectItems(); err != nil {
		return nil, err
	}

	if p.acceptKw("FROM") {
		if s.from, err = p.fromClause(); err != nil {
			return nil, err
		}
	}

	return s, p.selectTail(s)
}

func (p *parser) selectTail(s *selectStmt) error {
	var err error

	if p.acceptKw("WHERE") {
		if s.where, err = p.expr(); err != nil {
			return err
		}
	}

	if p.kw("GROUP") {
		p.i++

		if err = p.expectKw("BY"); err != nil {
			return err
		}

		if s.groupBy, err = p.exprList(); err != nil {
			return err
		}
	}

	if p.acceptKw("HAVING") {
		if s.having, err = p.expr(); err != nil {
			return err
		}
	}

	if p.acceptKw("QUALIFY") {
		if s.qualify, err = p.expr(); err != nil {
			return err
		}
	}

	return nil
}

func (p *parser) exprList() ([]*node, error) {
	var out []*node

	for {
		e, err := p.expr()
		if err != nil {
			return nil, err
		}

		out = append(out, e)

		if !p.acceptPunct(",") {
			return out, nil
		}
	}
}

func (p *parser) selectItems() ([]selectItem, error) {
	var out []selectItem

	for {
		it, err := p.selectItem()
		if err != nil {
			return nil, err
		}

		out = append(out, it)

		if !p.acceptPunct(",") {
			return out, nil
		}
	}
}

func (p *parser) selectItem() (selectItem, error) {
	if p.peek().isPunct("*") {
		p.i++

		return p.starTail(&node{kind: nStar})
	}

	e, err := p.expr()
	if err != nil {
		return selectItem{}, err
	}

	if p.peek().isPunct(".") && p.peekNext().isPunct("*") && e.kind == nPath {
		p.i += 2
		star := &node{kind: nStar, name: strings.Join(e.parts, ".")}

		return p.starTail(star)
	}

	it := selectItem{expr: e}

	if p.acceptKw("AS") {
		if it.alias, err = p.anyIdent(); err != nil {
			return it, err
		}

		return it, nil
	}

	if t := p.peek(); t.kind == tkQuoted || t.kind == tkIdent && !reserved()[strings.ToUpper(t.text)] {
		p.i++
		it.alias = t.text
	}

	return it, nil
}

func (p *parser) starTail(star *node) (selectItem, error) {
	if p.kw(litExcept) && p.peekNext().isPunct("(") {
		p.i += 2

		names, err := p.identList()
		if err != nil {
			return selectItem{}, err
		}

		star.except = names
	}

	if p.kw("REPLACE") && p.peekNext().isPunct("(") {
		p.i += 2

		if err := p.starReplace(star); err != nil {
			return selectItem{}, err
		}
	}

	return selectItem{expr: star}, nil
}

func (p *parser) starReplace(star *node) error {
	for {
		e, err := p.expr()
		if err != nil {
			return err
		}

		if err = p.expectKw("AS"); err != nil {
			return err
		}

		name, err := p.anyIdent()
		if err != nil {
			return err
		}

		star.replace = append(star.replace, selectItem{expr: e, alias: name})

		if p.acceptPunct(")") {
			return nil
		}

		if err = p.expectPunct(","); err != nil {
			return err
		}
	}
}

func (p *parser) fromClause() (*fromItem, error) {
	left, err := p.fromPrimary()
	if err != nil {
		return nil, err
	}

	for {
		jt, ok := p.joinType()
		if !ok {
			return left, nil
		}

		right, rerr := p.fromPrimary()
		if rerr != nil {
			return nil, rerr
		}

		j := &fromItem{kind: fromJoin, left: left, right: right, joinType: jt}

		if err = p.joinCondition(j); err != nil {
			return nil, err
		}

		left = j
	}
}

func (p *parser) joinType() (string, bool) {
	if p.acceptPunct(",") {
		return litCross, true
	}

	jt := litInner

	switch {
	case p.kw("JOIN"):
	case p.kw(litInner):
		p.i++
	case p.kw(litCross):
		p.i++

		jt = litCross
	case p.kw(litLeft), p.kw(litRight), p.kw("FULL"):
		jt = strings.ToUpper(p.advance().text)
		p.acceptKw("OUTER")
	default:
		return "", false
	}

	if !p.acceptKw("JOIN") {
		p.i--

		return "", false
	}

	return jt, true
}

func (p *parser) joinCondition(j *fromItem) error {
	var err error

	switch {
	case p.acceptKw("ON"):
		j.on, err = p.expr()
	case p.acceptKw("USING"):
		if err = p.expectPunct("("); err != nil {
			return err
		}

		j.using, err = p.identList()
	}

	return err
}

func (p *parser) fromPrimary() (*fromItem, error) {
	if err := p.enter(); err != nil {
		return nil, err
	}
	defer p.leave()

	switch {
	case p.peek().isPunct("("):
		return p.fromParen()
	case p.kw(litUnnest):
		return p.fromUnnest()
	}

	path, err := p.tablePath()
	if err != nil {
		return nil, err
	}

	f := &fromItem{kind: fromTable, path: path}

	if err = p.skipSystemTime(); err != nil {
		return nil, err
	}

	f.alias, err = p.optAlias()

	return f, err
}

func (p *parser) skipSystemTime() error {
	if p.kw("FOR") && p.peekNext().isKw("SYSTEM_TIME") {
		return syntaxErr(p.peek().pos, "FOR SYSTEM_TIME AS OF is not supported")
	}

	return nil
}

func (p *parser) fromParen() (*fromItem, error) {
	p.i++

	if p.kw(litSelect) || p.kw("WITH") || p.peek().isPunct("(") {
		q, err := p.query()
		if err != nil {
			return nil, err
		}

		if err = p.expectPunct(")"); err != nil {
			return nil, err
		}

		f := &fromItem{kind: fromSubquery, query: q}
		f.alias, err = p.optAlias()

		return f, err
	}

	inner, err := p.fromClause()
	if err != nil {
		return nil, err
	}

	return inner, p.expectPunct(")")
}

func (p *parser) fromUnnest() (*fromItem, error) {
	p.i++

	if err := p.expectPunct("("); err != nil {
		return nil, err
	}

	e, err := p.expr()
	if err != nil {
		return nil, err
	}

	if err = p.expectPunct(")"); err != nil {
		return nil, err
	}

	f := &fromItem{kind: fromUnnest, unnest: e}

	if f.alias, err = p.optAlias(); err != nil {
		return nil, err
	}

	if p.kw("WITH") && p.peekNext().isKw("OFFSET") {
		p.i += 2
		f.withOffset = true

		if f.offsetAlias, err = p.optAlias(); err != nil {
			return nil, err
		}
	}

	return f, nil
}

func (p *parser) optAlias() (string, error) {
	if p.acceptKw("AS") {
		return p.anyIdent()
	}

	if t := p.peek(); t.kind == tkQuoted || t.kind == tkIdent && !reserved()[strings.ToUpper(t.text)] {
		p.i++

		return t.text, nil
	}

	return "", nil
}

// tablePath parses project.dataset.table, with backtick quoting and dashes allowed in unquoted segments.
func (p *parser) tablePath() ([]string, error) {
	var parts []string

	for {
		t := p.peek()

		switch t.kind {
		case tkQuoted:
			p.i++

			parts = append(parts, strings.Split(t.text, ".")...)
		case tkIdent:
			p.i++

			var seg strings.Builder

			seg.WriteString(t.text)

			for p.peek().isPunct("-") && !p.peek().space && !p.peekNext().space &&
				(p.peekNext().kind == tkIdent || p.peekNext().kind == tkInt) {
				seg.WriteString("-" + p.peekNext().text)
				p.i += 2
			}

			parts = append(parts, seg.String())
		default:
			return nil, p.unexpected("expected table name")
		}

		if !p.peek().isPunct(".") || p.peek().space {
			return parts, nil
		}

		p.i++
	}
}
