package bqsql

import (
	"strings"
)

type scriptKind uint8

const (
	skSQL scriptKind = iota
	skDeclare
	skSet
	skIf
	skWhile
	skLoop
	skRepeat
	skFor
	skBlock
	skBreak
	skContinue
	skReturn
	skRaise
	skExecute
	skCall
	skBegin
	skCommit
	skRollback
	skAssert
)

type scriptStmt struct {
	typ        *Type
	expr       *node
	query      *Query
	stmt       *Statement
	label      string
	names      []string
	exprs      []*node
	usingNames []string
	target     []string
	conds      []*node
	blocks     [][]*scriptStmt
	body       []*scriptStmt
	handler    []*scriptStmt
	pos        int
	kind       scriptKind
	hasHandler bool
	tuple      bool
}

// Script is a parsed multi-statement GoogleSQL script.
type Script struct {
	Text string
	body []*scriptStmt
}

// Single returns the only statement of a script that is one plain SQL statement.
func (s *Script) Single() (*Statement, bool) {
	if len(s.body) == 1 && s.body[0].kind == skSQL {
		return s.body[0].stmt, true
	}

	return nil, false
}

const maxScriptDepth = 64

// ParseScript parses one or more statements with control flow.
func ParseScript(sql string) (*Script, error) {
	toks, err := lex(sql)
	if err != nil {
		return nil, err
	}

	p := &parser{src: sql, toks: toks}

	body, err := p.scriptList()
	if err != nil {
		return nil, err
	}

	if p.peek().kind != tkEOF {
		return nil, p.unexpected("unexpected input")
	}

	if len(body) == 0 {
		return nil, syntaxErr(0, "empty script")
	}

	return &Script{Text: sql, body: body}, nil
}

func (p *parser) atScriptEnd() bool {
	t := p.peek()

	return t.kind == tkEOF || t.isKw("END") || t.isKw("ELSE") || t.isKw("ELSEIF") || t.isKw("UNTIL") ||
		t.isKw("EXCEPTION") || t.isKw("WHEN")
}

func (p *parser) scriptList() ([]*scriptStmt, error) {
	if err := p.enter(); err != nil {
		return nil, err
	}
	defer p.leave()

	if p.depth > maxScriptDepth {
		return nil, syntaxErr(p.peek().pos, "script nesting too deep")
	}

	var out []*scriptStmt

	for !p.atScriptEnd() {
		if p.acceptPunct(";") {
			continue
		}

		s, err := p.scriptStatement()
		if err != nil {
			return nil, err
		}

		out = append(out, s)

		if p.atScriptEnd() {
			break
		}

		if !p.acceptPunct(";") {
			return nil, p.unexpected("expected \";\"")
		}
	}

	return out, nil
}

func (p *parser) scriptStatement() (*scriptStmt, error) {
	t := p.peek()
	label := ""

	if (t.kind == tkIdent || t.kind == tkQuoted) && p.peekNext().isPunct(":") {
		label = t.text
		p.i += 2
		t = p.peek()
	}

	s, err := p.scriptDispatch(t, label)
	if err != nil {
		return nil, err
	}

	s.pos, s.label = t.pos, label

	return s, nil
}

func (p *parser) scriptDispatch(t token, label string) (*scriptStmt, error) {
	switch {
	case t.isKw("DECLARE"):
		return p.declareStmt()
	case t.isKw("SET") && !p.peekNext().isPunct("="):
		return p.setStmt()
	case t.isKw("BEGIN"):
		return p.beginStmt(label)
	case t.isKw("EXECUTE") && p.peekNext().isKw("IMMEDIATE"):
		return p.executeStmt()
	case t.isKw("CALL"):
		return p.callStmt()
	case t.isKw("RAISE"):
		return p.raiseStmt()
	case t.isKw("ASSERT"):
		return p.assertStmt()
	}

	if s, ok, err := p.controlStmt(t); ok || err != nil {
		return s, err
	}

	start := t.pos
	st, err := p.statement()
	if err != nil {
		return nil, err
	}

	end := p.peek().pos
	if p.peek().kind == tkEOF {
		end = len(p.src)
	}

	st.Text = strings.TrimSpace(p.src[start:end])

	return &scriptStmt{kind: skSQL, stmt: st}, nil
}

// controlStmt parses IF, loops, jumps, RETURN and the transaction statements.
func (p *parser) controlStmt(t token) (*scriptStmt, bool, error) {
	var (
		s   *scriptStmt
		err error
	)

	switch {
	case t.isKw("IF"):
		s, err = p.ifStmt()
	case t.isKw("CASE"):
		s, err = p.caseStmt()
	case t.isKw("WHILE"):
		s, err = p.whileStmt()
	case t.isKw("LOOP"):
		s, err = p.loopStmt()
	case t.isKw("REPEAT"):
		s, err = p.repeatStmt()
	case t.isKw("FOR") && p.peekNext().kind != tkEOF:
		s, err = p.forStmt()
	default:
		s = p.simpleControl(t)

		return s, s != nil, nil
	}

	return s, true, err
}

// simpleControl parses the control statements that cannot fail: jumps, RETURN, COMMIT and ROLLBACK.
func (p *parser) simpleControl(t token) *scriptStmt {
	switch {
	case t.isKw("BREAK") || t.isKw("LEAVE"):
		return p.jumpStmt(skBreak)
	case t.isKw("CONTINUE") || t.isKw("ITERATE"):
		return p.jumpStmt(skContinue)
	case t.isKw("RETURN"):
		p.i++

		return &scriptStmt{kind: skReturn}
	case t.isKw("COMMIT") || t.isKw("ROLLBACK"):
		p.i++
		p.acceptKw("TRANSACTION")

		if t.isKw("ROLLBACK") {
			return &scriptStmt{kind: skRollback}
		}

		return &scriptStmt{kind: skCommit}
	}

	return nil
}

func (p *parser) declareStmt() (*scriptStmt, error) {
	p.i++

	s := &scriptStmt{kind: skDeclare}

	names, err := p.nameList()
	if err != nil {
		return nil, err
	}

	s.names = names

	if !p.kw("DEFAULT") {
		t, terr := p.typeSpec()
		if terr != nil {
			return nil, terr
		}

		s.typ = &t
	}

	if p.acceptKw("DEFAULT") {
		e, eerr := p.expr()
		if eerr != nil {
			return nil, eerr
		}

		s.expr = e
	}

	if s.typ == nil && s.expr == nil {
		return nil, p.unexpected("expected a type or DEFAULT")
	}

	return s, nil
}

func (p *parser) setStmt() (*scriptStmt, error) {
	p.i++

	s := &scriptStmt{kind: skSet}

	if p.acceptPunct("(") {
		names, err := p.identList()
		if err != nil {
			return nil, err
		}

		s.names, s.tuple = names, true
	} else {
		n, err := p.identName()
		if err != nil {
			return nil, err
		}

		s.names = []string{n}
	}

	if err := p.expectPunct("="); err != nil {
		return nil, err
	}

	if s.tuple && p.peek().isPunct("(") {
		save := p.i
		p.i++

		list, err := p.exprList()
		if err == nil && p.acceptPunct(")") && len(list) == len(s.names) && (p.peek().isPunct(";") || p.atScriptEnd()) {
			s.exprs = list

			return s, nil
		}

		p.i = save
	}

	e, err := p.expr()
	if err != nil {
		return nil, err
	}

	s.expr = e

	return s, nil
}

func (p *parser) thenList(terms ...string) ([]*scriptStmt, error) {
	body, err := p.scriptList()
	if err != nil {
		return nil, err
	}

	for _, t := range terms {
		if p.peek().isKw(t) {
			return body, nil
		}
	}

	return nil, p.unexpected("expected " + strings.Join(terms, " or "))
}

func (p *parser) endKw(words ...string) error {
	if err := p.expectKw("END"); err != nil {
		return err
	}

	for _, w := range words {
		if err := p.expectKw(w); err != nil {
			return err
		}
	}

	if t := p.peek(); (t.kind == tkIdent || t.kind == tkQuoted) && !p.atScriptEnd() {
		p.i++
	}

	return nil
}

func (p *parser) ifStmt() (*scriptStmt, error) {
	p.i++

	s := &scriptStmt{kind: skIf}

	for {
		cond, err := p.expr()
		if err != nil {
			return nil, err
		}

		if err = p.expectKw("THEN"); err != nil {
			return nil, err
		}

		body, err := p.thenList("ELSEIF", "ELSE", "END")
		if err != nil {
			return nil, err
		}

		s.conds, s.blocks = append(s.conds, cond), append(s.blocks, body)

		if !p.acceptKw("ELSEIF") {
			break
		}
	}

	if p.acceptKw("ELSE") {
		body, err := p.thenList("END")
		if err != nil {
			return nil, err
		}

		s.body = body
	}

	return s, p.endKw("IF")
}

// caseStmt parses CASE [search] WHEN ... THEN ... [ELSE ...] END CASE as an IF chain; a search expression is
// compared with each WHEN value by equality.
func (p *parser) caseStmt() (*scriptStmt, error) {
	p.i++

	var search *node

	if !p.kw("WHEN") {
		var err error
		if search, err = p.expr(); err != nil {
			return nil, err
		}
	}

	s := &scriptStmt{kind: skIf}

	for p.acceptKw("WHEN") {
		cond, err := p.expr()
		if err != nil {
			return nil, err
		}

		if search != nil {
			cond = &node{kind: nBinary, name: "=", args: []*node{search, cond}, pos: cond.pos}
		}

		if err = p.expectKw("THEN"); err != nil {
			return nil, err
		}

		body, err := p.thenList("WHEN", "ELSE", "END")
		if err != nil {
			return nil, err
		}

		s.conds, s.blocks = append(s.conds, cond), append(s.blocks, body)
	}

	if len(s.conds) == 0 {
		return nil, p.unexpected("expected WHEN")
	}

	if p.acceptKw("ELSE") {
		body, err := p.thenList("END")
		if err != nil {
			return nil, err
		}

		s.body = body
	}

	return s, p.endKw("CASE")
}

func (p *parser) whileStmt() (*scriptStmt, error) {
	p.i++

	cond, err := p.expr()
	if err != nil {
		return nil, err
	}

	if err = p.expectKw("DO"); err != nil {
		return nil, err
	}

	body, err := p.thenList("END")
	if err != nil {
		return nil, err
	}

	return &scriptStmt{kind: skWhile, expr: cond, body: body}, p.endKw("WHILE")
}

func (p *parser) loopStmt() (*scriptStmt, error) {
	p.i++

	body, err := p.thenList("END")
	if err != nil {
		return nil, err
	}

	return &scriptStmt{kind: skLoop, body: body}, p.endKw("LOOP")
}

func (p *parser) repeatStmt() (*scriptStmt, error) {
	p.i++

	body, err := p.thenList("UNTIL")
	if err != nil {
		return nil, err
	}

	p.i++

	cond, err := p.expr()
	if err != nil {
		return nil, err
	}

	return &scriptStmt{kind: skRepeat, expr: cond, body: body}, p.endKw("REPEAT")
}

func (p *parser) forStmt() (*scriptStmt, error) {
	p.i++

	name, err := p.identName()
	if err != nil {
		return nil, err
	}

	if err = p.expectKw("IN"); err != nil {
		return nil, err
	}

	if err = p.expectPunct("("); err != nil {
		return nil, err
	}

	q, err := p.query()
	if err != nil {
		return nil, err
	}

	if err = p.expectPunct(")"); err != nil {
		return nil, err
	}

	if err = p.expectKw("DO"); err != nil {
		return nil, err
	}

	body, err := p.thenList("END")
	if err != nil {
		return nil, err
	}

	return &scriptStmt{kind: skFor, names: []string{name}, query: q, body: body}, p.endKw("FOR")
}

func (p *parser) beginStmt(label string) (*scriptStmt, error) {
	if next := p.peekNext(); next.isKw("TRANSACTION") || next.isPunct(";") || next.kind == tkEOF {
		p.i++
		p.acceptKw("TRANSACTION")

		return &scriptStmt{kind: skBegin}, nil
	}

	p.i++

	body, err := p.thenList("END", "EXCEPTION")
	if err != nil {
		return nil, err
	}

	s := &scriptStmt{kind: skBlock, body: body}

	if p.acceptKw("EXCEPTION") {
		if err = p.expectKw("WHEN"); err != nil {
			return nil, err
		}

		if err = p.expectKw("ERROR"); err != nil {
			return nil, err
		}

		if err = p.expectKw("THEN"); err != nil {
			return nil, err
		}

		if s.handler, err = p.thenList("END"); err != nil {
			return nil, err
		}

		s.hasHandler = true
	}

	if err = p.expectKw("END"); err != nil {
		return nil, err
	}

	if t := p.peek(); label != "" && strings.EqualFold(t.text, label) && (t.kind == tkIdent || t.kind == tkQuoted) {
		p.i++
	}

	return s, nil
}

func (p *parser) jumpStmt(kind scriptKind) *scriptStmt {
	p.i++

	s := &scriptStmt{kind: kind}

	if t := p.peek(); (t.kind == tkIdent || t.kind == tkQuoted) && !t.isKw("END") && !t.isKw("ELSE") &&
		!t.isKw("ELSEIF") && !t.isKw("UNTIL") && !t.isKw("EXCEPTION") {
		s.names = []string{t.text}
		p.i++
	}

	return s
}

func (p *parser) raiseStmt() (*scriptStmt, error) {
	p.i++

	s := &scriptStmt{kind: skRaise}

	if p.acceptKw("USING") {
		if err := p.expectKw("MESSAGE"); err != nil {
			return nil, err
		}

		if err := p.expectPunct("="); err != nil {
			return nil, err
		}

		e, err := p.expr()
		if err != nil {
			return nil, err
		}

		s.expr = e
	}

	return s, nil
}

func (p *parser) executeStmt() (*scriptStmt, error) {
	p.i += 2

	e, err := p.expr()
	if err != nil {
		return nil, err
	}

	s := &scriptStmt{kind: skExecute, expr: e}

	if p.acceptKw("INTO") {
		if s.names, err = p.nameList(); err != nil {
			return nil, err
		}
	}

	if p.acceptKw("USING") {
		err = p.usingList(s)
	}

	return s, err
}

func (p *parser) nameList() ([]string, error) {
	var out []string

	for {
		n, err := p.identName()
		if err != nil {
			return nil, err
		}

		out = append(out, n)

		if !p.acceptPunct(",") {
			return out, nil
		}
	}
}

func (p *parser) usingList(s *scriptStmt) error {
	for {
		a, err := p.expr()
		if err != nil {
			return err
		}

		name := ""
		if p.acceptKw("AS") {
			if name, err = p.identName(); err != nil {
				return err
			}
		}

		s.exprs, s.usingNames = append(s.exprs, a), append(s.usingNames, name)

		if !p.acceptPunct(",") {
			return nil
		}
	}
}

func (p *parser) callStmt() (*scriptStmt, error) {
	p.i++

	path, err := p.tablePath()
	if err != nil {
		return nil, err
	}

	if err = p.expectPunct("("); err != nil {
		return nil, err
	}

	s := &scriptStmt{kind: skCall, target: path}

	if p.acceptPunct(")") {
		return s, nil
	}

	if s.exprs, err = p.exprList(); err != nil {
		return nil, err
	}

	return s, p.expectPunct(")")
}

func (p *parser) procedureBody(r *RoutineData) error {
	start := p.peek().pos

	if _, err := p.beginStmt(""); err != nil {
		return err
	}

	end := p.peek().pos
	if p.peek().kind == tkEOF {
		end = len(p.src)
	}

	r.Body = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(p.src[start:end]), ";"))

	return nil
}

func (p *parser) assertStmt() (*scriptStmt, error) {
	p.i++

	e, err := p.expr()
	if err != nil {
		return nil, err
	}

	s := &scriptStmt{kind: skAssert, expr: e}

	if p.acceptKw("AS") {
		d, derr := p.expr()
		if derr != nil {
			return nil, derr
		}

		s.exprs = []*node{d}
	}

	return s, nil
}
