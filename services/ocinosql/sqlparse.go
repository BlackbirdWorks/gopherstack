package ocinosql

import (
	"encoding/json"
	"strconv"
	"strings"
)

type step struct {
	key   string
	idx   int
	isIdx bool
}

type expr interface{}

type (
	binExpr struct {
		l, r expr
		op   string
	}
	notExpr struct{ e expr }
	inExpr  struct {
		e    expr
		list []expr
		not  bool
	}
	nullExpr struct {
		e   expr
		not bool
	}
	pathExpr struct{ steps []step }
	litExpr  struct{ v any }
	varExpr  struct{ name string }
)

type selItem struct {
	alias string
	path  []step
}

type orderItem struct {
	path []step
	desc bool
}

type selectStmt struct {
	where  expr
	vars   map[string]*dtype
	limit  *int64
	offset *int64
	table  string
	alias  string
	items  []selItem
	order  []orderItem
	star   bool
	count  bool
}

var unsupportedKw = map[string]bool{ //nolint:gochecknoglobals // immutable lookup
	"GROUP": true, "JOIN": true, "HAVING": true, "UNION": true, "LEFT": true, "INNER": true, "DISTINCT": true,
	"LIKE": true, "EXISTS": true, "SELECT": true,
}

func (p *parser) unsupported(what string) *APIError {
	return errParam("syntax error: " + what + " is not supported by this SQL subset")
}

func parseSelect(sql string) (*selectStmt, *APIError) {
	p, err := newParser(sql)
	if err != nil {
		return nil, err
	}

	st := &selectStmt{vars: map[string]*dtype{}}

	if p.acceptKw("DECLARE") {
		if e := p.parseDeclare(st); e != nil {
			return nil, e
		}
	}

	if e := p.expectKw("SELECT"); e != nil {
		return nil, e
	}

	if e := p.parseSelectList(st); e != nil {
		return nil, e
	}

	if e := p.parseFromTail(st); e != nil {
		return nil, e
	}

	if !p.atEOF() {
		if t := p.peek(); unsupportedKw[t.up] {
			return nil, p.unsupported(t.up)
		}

		return nil, p.fail("end of statement")
	}

	return st, nil
}

func (p *parser) parseDeclare(st *selectStmt) *APIError {
	for p.peek().kind == tkVar {
		name := p.next().text

		t, e := p.parseType()
		if e != nil {
			return e
		}

		st.vars[name] = t

		if e = p.expectPunct(";"); e != nil {
			return e
		}
	}

	if len(st.vars) == 0 {
		return p.fail("a variable declaration")
	}

	return nil
}

func (p *parser) parseSelectList(st *selectStmt) *APIError {
	if p.unsupportedHere() != nil {
		return p.unsupportedHere()
	}

	if p.acceptPunct("*") {
		st.star = true

		return nil
	}

	for {
		if p.isKw("COUNT") {
			p.pos++

			if e := firstErr(p.expectPunct("("), p.expectPunct("*"), p.expectPunct(")")); e != nil {
				return e
			}

			st.count = true
		} else {
			steps, e := p.parsePath()
			if e != nil {
				return e
			}

			it := selItem{path: steps}
			if p.acceptKw("AS") {
				if it.alias, e = p.ident("an alias"); e != nil {
					return e
				}
			}

			st.items = append(st.items, it)
		}

		if !p.acceptPunct(",") {
			break
		}
	}

	if st.count && len(st.items) > 0 {
		return p.unsupported("mixing COUNT(*) with other select items")
	}

	return nil
}

func (p *parser) unsupportedHere() *APIError {
	if t := p.peek(); t.kind == tkIdent && (t.up == "DISTINCT" || t.up == "SELECT") {
		return p.unsupported(t.up)
	}

	return nil
}

func (p *parser) parseFromTail(st *selectStmt) *APIError {
	if e := p.expectKw("FROM"); e != nil {
		return e
	}

	name, e := p.ident("a table name")
	if e != nil {
		return e
	}

	if p.isPunct(".") {
		return p.unsupported("child tables")
	}

	st.table, st.alias = name, name

	if p.acceptKw("AS") {
		if st.alias, e = p.ident("an alias"); e != nil {
			return e
		}
	} else if t := p.peek(); t.kind == tkIdent && !isClauseKw(t.up) && !unsupportedKw[t.up] {
		st.alias = p.next().text
	}

	if p.acceptKw("WHERE") {
		if st.where, e = p.parseOr(); e != nil {
			return e
		}
	}

	return p.parseOrderLimit(st)
}

func isClauseKw(up string) bool {
	switch up {
	case "WHERE", "ORDER", "LIMIT", "OFFSET":
		return true
	}

	return false
}

func (p *parser) parseOrderLimit(st *selectStmt) *APIError {
	if p.acceptKw("ORDER") {
		if e := p.expectKw("BY"); e != nil {
			return e
		}

		for {
			steps, e := p.parsePath()
			if e != nil {
				return e
			}

			it := orderItem{path: steps}
			if p.acceptKw("DESC") {
				it.desc = true
			} else {
				p.acceptKw("ASC")
			}

			st.order = append(st.order, it)

			if !p.acceptPunct(",") {
				break
			}
		}
	}

	for {
		switch {
		case p.acceptKw("LIMIT"):
			n, e := p.nonNegInt("LIMIT")
			if e != nil {
				return e
			}

			st.limit = &n
		case p.acceptKw("OFFSET"):
			n, e := p.nonNegInt("OFFSET")
			if e != nil {
				return e
			}

			st.offset = &n
		default:
			return nil
		}
	}
}

func (p *parser) nonNegInt(what string) (int64, *APIError) {
	n, err := strconv.ParseInt(p.next().text, 10, 64)
	if err != nil || n < 0 {
		return 0, errParam("syntax error: " + what + " must be a non-negative integer")
	}

	return n, nil
}

func (p *parser) parsePath() ([]step, *APIError) {
	key, e := p.ident("a column name")
	if e != nil {
		return nil, e
	}

	if p.isPunct("(") {
		return nil, p.unsupported("function " + key + "()")
	}

	steps := []step{{key: key}}

	for {
		switch {
		case p.acceptPunct("."):
			k, e2 := p.ident("a field name")
			if e2 != nil {
				return nil, e2
			}

			steps = append(steps, step{key: k})
		case p.acceptPunct("["):
			n, err := strconv.Atoi(p.next().text)
			if err != nil || n < 0 {
				return nil, p.unsupported("array slice or filter steps")
			}

			if e2 := p.expectPunct("]"); e2 != nil {
				return nil, e2
			}

			steps = append(steps, step{idx: n, isIdx: true})
		default:
			return steps, nil
		}
	}
}

func (p *parser) parseOr() (expr, *APIError) {
	l, e := p.parseAnd()
	if e != nil {
		return nil, e
	}

	for p.acceptKw("OR") {
		r, e2 := p.parseAnd()
		if e2 != nil {
			return nil, e2
		}

		l = &binExpr{op: "OR", l: l, r: r}
	}

	return l, nil
}

func (p *parser) parseAnd() (expr, *APIError) {
	l, e := p.parseNot()
	if e != nil {
		return nil, e
	}

	for p.acceptKw("AND") {
		r, e2 := p.parseNot()
		if e2 != nil {
			return nil, e2
		}

		l = &binExpr{op: "AND", l: l, r: r}
	}

	return l, nil
}

func (p *parser) parseNot() (expr, *APIError) {
	if p.acceptKw("NOT") {
		e, err := p.parseNot()
		if err != nil {
			return nil, err
		}

		return &notExpr{e: e}, nil
	}

	return p.parseCompare()
}

func (p *parser) parseCompare() (expr, *APIError) {
	l, e := p.parseOperand()
	if e != nil {
		return nil, e
	}

	t := p.peek()

	switch {
	case t.kind == tkPunct && (t.text == "=" || t.text == "<" || t.text == "<=" || t.text == ">" ||
		t.text == ">=" || t.text == "<>" || t.text == "!="):
		p.pos++

		r, e2 := p.parseOperand()
		if e2 != nil {
			return nil, e2
		}

		op := t.text
		if op == "<>" {
			op = "!="
		}

		return &binExpr{op: op, l: l, r: r}, nil
	case t.kind == tkIdent && t.up == "IS":
		p.pos++
		not := p.acceptKw("NOT")

		return &nullExpr{e: l, not: not}, p.expectKw("NULL")
	case t.kind == tkIdent && (t.up == "IN" || t.up == "NOT" || t.up == "BETWEEN"):
		return p.parseInBetween(l)
	}

	return l, nil
}

func (p *parser) parseInBetween(l expr) (expr, *APIError) {
	not := p.acceptKw("NOT")

	if p.acceptKw("BETWEEN") {
		lo, e := p.parseOperand()
		if e != nil {
			return nil, e
		}

		if e = p.expectKw("AND"); e != nil {
			return nil, e
		}

		hi, e := p.parseOperand()
		if e != nil {
			return nil, e
		}

		var out expr = &binExpr{op: "AND", l: &binExpr{op: ">=", l: l, r: lo}, r: &binExpr{op: "<=", l: l, r: hi}}
		if not {
			out = &notExpr{e: out}
		}

		return out, nil
	}

	if e := p.expectKw("IN"); e != nil {
		return nil, e
	}

	if e := p.expectPunct("("); e != nil {
		return nil, e
	}

	in := &inExpr{e: l, not: not}

	for {
		v, e := p.parseOperand()
		if e != nil {
			return nil, e
		}

		in.list = append(in.list, v)

		if !p.acceptPunct(",") {
			break
		}
	}

	return in, p.expectPunct(")")
}

func (p *parser) parseOperand() (expr, *APIError) {
	t := p.peek()

	switch {
	case t.kind == tkPunct && t.text == "(":
		p.pos++

		e, err := p.parseOr()
		if err != nil {
			return nil, err
		}

		return e, p.expectPunct(")")
	case t.kind == tkPunct && t.text == "-":
		p.pos++

		n := p.next()
		if n.kind != tkNumber {
			return nil, p.fail("a number")
		}

		return &litExpr{v: json.Number("-" + n.text)}, nil
	case t.kind == tkNumber:
		p.pos++

		return &litExpr{v: json.Number(t.text)}, nil
	case t.kind == tkString:
		p.pos++

		return &litExpr{v: t.text}, nil
	case t.kind == tkVar:
		p.pos++

		return &varExpr{name: t.text}, nil
	case t.kind == tkIdent && (t.up == "TRUE" || t.up == "FALSE"):
		p.pos++

		return &litExpr{v: t.up == "TRUE"}, nil
	case t.kind == tkIdent && t.up == "NULL":
		p.pos++

		return &litExpr{v: nil}, nil
	case t.kind == tkIdent:
		steps, err := p.parsePath()
		if err != nil {
			return nil, err
		}

		return &pathExpr{steps: steps}, nil
	}

	return nil, p.fail("an expression")
}

// stmtKind classifies the leading keywords of a statement for SummarizeStatement.
type stmtSummary struct {
	syntaxError string
	operation   string
	table       string
	index       string
	ifExists    bool
	ifNotExists bool
}

func summarize(sql string) stmtSummary {
	p, err := newParser(sql)
	if err != nil {
		return stmtSummary{syntaxError: err.Message}
	}

	if p.isKw("DECLARE") {
		return summarizeSelect(sql)
	}

	first := p.next().up

	switch first {
	case "SELECT":
		return summarizeSelect(sql)
	case "CREATE":
		return summarizeCreate(p, sql)
	case "ALTER":
		s := stmtSummary{operation: "ALTER_TABLE"}
		if _, e := parseAlter(sql); e != nil {
			s.syntaxError = e.Message
		}

		p.acceptKw("TABLE")
		s.table = p.next().text

		return s
	case "DROP":
		return summarizeDrop(p)
	case "INSERT":
		p.acceptKw("INTO")

		return stmtSummary{operation: "INSERT", table: p.next().text}
	case "UPSERT":
		return stmtSummary{operation: "INSERT", table: p.next().text}
	case "UPDATE":
		return stmtSummary{operation: "UPDATE", table: p.next().text}
	case "DELETE":
		p.acceptKw("FROM")

		return stmtSummary{operation: "DELETE", table: p.next().text}
	}

	return stmtSummary{syntaxError: "syntax error: unrecognized statement '" + strings.TrimSpace(first) + "'"}
}

func summarizeSelect(sql string) stmtSummary {
	s := stmtSummary{operation: "SELECT"}

	st, e := parseSelect(sql)
	if e != nil {
		s.syntaxError = e.Message

		return s
	}

	s.table = st.table

	return s
}

func summarizeCreate(p *parser, sql string) stmtSummary {
	if p.acceptKw("TABLE") {
		s := stmtSummary{operation: "CREATE_TABLE"}

		spec, e := parseCreate(sql)
		if e != nil {
			s.syntaxError = e.Message
			s.ifNotExists = p.isKw("IF")
			s.table = p.next().text

			return s
		}

		s.table, s.ifNotExists = spec.name, spec.ifNotExists

		return s
	}

	s := stmtSummary{operation: "CREATE_INDEX"}

	if !p.acceptKw("INDEX") {
		return stmtSummary{syntaxError: "syntax error: expected TABLE or INDEX after CREATE"}
	}

	if p.acceptKw("IF") {
		s.ifNotExists = p.acceptKw("NOT") && p.acceptKw("EXISTS")
	}

	s.index = p.next().text

	if e := p.expectKw("ON"); e != nil {
		s.syntaxError = e.Message

		return s
	}

	s.table = p.next().text

	if e := p.expectPunct("("); e != nil {
		s.syntaxError = e.Message
	}

	return s
}

func summarizeDrop(p *parser) stmtSummary {
	kind := p.next().up
	s := stmtSummary{}

	switch kind {
	case "TABLE":
		s.operation = "DROP_TABLE"
	case "INDEX":
		s.operation = "DROP_INDEX"
	default:
		return stmtSummary{syntaxError: "syntax error: expected TABLE or INDEX after DROP"}
	}

	if p.acceptKw("IF") {
		s.ifExists = p.acceptKw("EXISTS")
	}

	name := p.next().text
	if kind == "TABLE" {
		s.table = name

		return s
	}

	s.index = name

	if e := p.expectKw("ON"); e != nil {
		s.syntaxError = e.Message

		return s
	}

	s.table = p.next().text

	return s
}
