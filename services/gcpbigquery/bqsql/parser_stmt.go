package bqsql

import (
	"strconv"
	"strings"
)

func itoa(n int) string { return strconv.Itoa(n) }

func (p *parser) insertStmt() (*Statement, error) {
	p.i++
	p.acceptKw("INTO")

	path, err := p.tablePath()
	if err != nil {
		return nil, err
	}

	st := &Statement{Kind: StmtInsert, target: path}

	if p.peek().isPunct("(") && !p.peekNext().isKw(litSelect) {
		p.i++

		if st.cols, err = p.identList(); err != nil {
			return nil, err
		}
	}

	if p.acceptKw("VALUES") {
		st.hasValues = true

		return st, p.valueRows(st)
	}

	st.query, err = p.query()

	return st, err
}

func (p *parser) valueRows(st *Statement) error {
	for {
		row, err := p.valueRow()
		if err != nil {
			return err
		}

		st.rows = append(st.rows, row)

		if !p.acceptPunct(",") {
			return nil
		}
	}
}

func (p *parser) valueRow() ([]*node, error) {
	if err := p.expectPunct("("); err != nil {
		return nil, err
	}

	var row []*node

	for {
		if p.acceptKw("DEFAULT") {
			row = append(row, &node{kind: nNull})
		} else {
			e, err := p.expr()
			if err != nil {
				return nil, err
			}

			row = append(row, e)
		}

		if p.acceptPunct(")") {
			return row, nil
		}

		if err := p.expectPunct(","); err != nil {
			return nil, err
		}
	}
}

func (p *parser) updateStmt() (*Statement, error) {
	p.i++

	path, err := p.tablePath()
	if err != nil {
		return nil, err
	}

	st := &Statement{Kind: StmtUpdate, target: path}

	if st.alias, err = p.optAlias(); err != nil {
		return nil, err
	}

	if err = p.expectKw("SET"); err != nil {
		return nil, err
	}

	if st.set, err = p.assignments(); err != nil {
		return nil, err
	}

	if p.acceptKw("FROM") {
		if st.from, err = p.fromClause(); err != nil {
			return nil, err
		}
	}

	if err = p.expectKw("WHERE"); err != nil {
		return nil, err
	}

	st.where, err = p.expr()

	return st, err
}

func (p *parser) assignments() ([]assignment, error) {
	var out []assignment

	for {
		target, err := p.tablePath()
		if err != nil {
			return nil, err
		}

		if err = p.expectPunct("="); err != nil {
			return nil, err
		}

		var val *node
		if p.acceptKw("DEFAULT") {
			val = &node{kind: nNull}
		} else if val, err = p.expr(); err != nil {
			return nil, err
		}

		out = append(out, assignment{target: target, value: val})

		if !p.acceptPunct(",") {
			return out, nil
		}
	}
}

func (p *parser) deleteStmt() (*Statement, error) {
	p.i++
	p.acceptKw("FROM")

	path, err := p.tablePath()
	if err != nil {
		return nil, err
	}

	st := &Statement{Kind: StmtDelete, target: path}

	if st.alias, err = p.optAlias(); err != nil {
		return nil, err
	}

	if err = p.expectKw("WHERE"); err != nil {
		return nil, err
	}

	st.where, err = p.expr()

	return st, err
}

func (p *parser) mergeStmt() (*Statement, error) {
	p.i++
	p.acceptKw("INTO")

	path, err := p.tablePath()
	if err != nil {
		return nil, err
	}

	st := &Statement{Kind: StmtMerge, target: path}

	if st.alias, err = p.optAlias(); err != nil {
		return nil, err
	}

	if err = p.expectKw("USING"); err != nil {
		return nil, err
	}

	if st.source, err = p.fromPrimary(); err != nil {
		return nil, err
	}

	if err = p.expectKw("ON"); err != nil {
		return nil, err
	}

	if st.on, err = p.expr(); err != nil {
		return nil, err
	}

	for p.kw("WHEN") {
		w, werr := p.mergeWhen()
		if werr != nil {
			return nil, werr
		}

		st.whens = append(st.whens, w)
	}

	if len(st.whens) == 0 {
		return nil, p.unexpected("expected WHEN")
	}

	return st, nil
}

func (p *parser) mergeWhen() (mergeWhen, error) {
	p.i++

	w := mergeWhen{matched: true}

	if p.acceptKw(litNot) {
		w.matched = false
	}

	if err := p.expectKw("MATCHED"); err != nil {
		return w, err
	}

	if p.acceptKw("BY") {
		switch {
		case p.acceptKw("SOURCE"):
			w.bySource = true
		case p.acceptKw("TARGET"):
		default:
			return w, p.unexpected("expected SOURCE or TARGET")
		}
	}

	if p.acceptKw(litAnd) {
		c, err := p.expr()
		if err != nil {
			return w, err
		}

		w.cond = c
	}

	if err := p.expectKw("THEN"); err != nil {
		return w, err
	}

	return w, p.mergeAction(&w)
}

func (p *parser) mergeAction(w *mergeWhen) error {
	var err error

	switch {
	case p.acceptKw(litDelete):
		w.action = litDelete
	case p.acceptKw(litUpdate):
		w.action = litUpdate

		if err = p.expectKw("SET"); err != nil {
			return err
		}

		w.set, err = p.assignments()
	case p.acceptKw(litInsert):
		w.action = litInsert

		return p.mergeInsert(w)
	default:
		return p.unexpected("expected DELETE, UPDATE or INSERT")
	}

	return err
}

func (p *parser) mergeInsert(w *mergeWhen) error {
	var err error

	if p.peek().isPunct("(") {
		p.i++

		if w.insertCol, err = p.identList(); err != nil {
			return err
		}
	}

	if p.acceptKw("ROW") {
		w.insertAll = true

		return nil
	}

	if err = p.expectKw("VALUES"); err != nil {
		return err
	}

	if err = p.expectPunct("("); err != nil {
		return err
	}

	w.insertVal, err = p.exprList()
	if err != nil {
		return err
	}

	return p.expectPunct(")")
}

func (p *parser) createStmt() (*Statement, error) {
	p.i++

	st := &Statement{}

	if p.kw("OR") {
		p.i++

		if err := p.expectKw("REPLACE"); err != nil {
			return nil, err
		}

		st.orReplace = true
	}

	if p.acceptKw("TEMP") || p.acceptKw("TEMPORARY") {
		st.temp = true
	}

	p.acceptKw("EXTERNAL")

	switch {
	case p.acceptKw("TABLE"):
		st.Kind = StmtCreateTable
	case p.acceptKw("VIEW"):
		st.Kind = StmtCreateView
	case p.acceptKw("SCHEMA"):
		st.Kind = StmtCreateSchema
	default:
		return nil, p.unexpected("expected TABLE, VIEW or SCHEMA")
	}

	if p.kw("IF") {
		p.i++

		if err := p.expectKw(litNot); err != nil {
			return nil, err
		}

		if err := p.expectKw("EXISTS"); err != nil {
			return nil, err
		}

		st.ifNot = true
	}

	path, err := p.tablePath()
	if err != nil {
		return nil, err
	}

	st.target = path

	return st, p.createBody(st)
}

func (p *parser) createBody(st *Statement) error {
	var err error

	if p.peek().isPunct("(") && st.Kind == StmtCreateTable {
		p.i++

		if st.colDefs, err = p.colDefs(); err != nil {
			return err
		}
	}

	if p.peek().isPunct("(") && st.Kind == StmtCreateView {
		p.i++

		if st.cols, err = p.identList(); err != nil {
			return err
		}
	}

	for p.kw("PARTITION") || p.kw("CLUSTER") {
		p.i++

		if err = p.expectKw("BY"); err != nil {
			return err
		}

		if _, err = p.exprList(); err != nil {
			return err
		}
	}

	if p.kw("OPTIONS") {
		if st.options, err = p.optionsClause(); err != nil {
			return err
		}
	}

	if p.kw("AS") {
		p.i++

		st.viewSQL = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(p.src[p.peek().pos:]), ";"))
		st.query, err = p.query()

		return err
	}

	if st.Kind == StmtCreateView {
		return p.unexpected("expected AS")
	}

	return nil
}

func (p *parser) colDefs() ([]colDef, error) {
	var out []colDef

	for {
		name, err := p.identName()
		if err != nil {
			return nil, err
		}

		t, err := p.typeSpec()
		if err != nil {
			return nil, err
		}

		cd := colDef{name: name, typ: t}

		if p.kw(litNot) && p.peekNext().isKw(litNull) {
			p.i += 2
			cd.required = true
		}

		if p.kw("OPTIONS") {
			if _, err = p.optionsClause(); err != nil {
				return nil, err
			}
		}

		out = append(out, cd)

		if p.acceptPunct(")") {
			return out, nil
		}

		if err = p.expectPunct(","); err != nil {
			return nil, err
		}
	}
}

// optionsClause parses OPTIONS(name=value, ...) keeping each value's source text.
func (p *parser) optionsClause() (map[string]string, error) {
	p.i++

	if err := p.expectPunct("("); err != nil {
		return nil, err
	}

	opts := map[string]string{}

	for !p.acceptPunct(")") {
		name, err := p.anyIdent()
		if err != nil {
			return nil, err
		}

		if err = p.expectPunct("="); err != nil {
			return nil, err
		}

		start := p.peek().pos

		e, err := p.expr()
		if err != nil {
			return nil, err
		}

		opts[strings.ToLower(name)] = optionText(p.src[start:p.peek().pos], e)

		p.acceptPunct(",")
	}

	return opts, nil
}

func optionText(src string, e *node) string {
	if e.kind == nString {
		return e.str
	}

	return strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(src), ","))
}

func (p *parser) dropStmt() (*Statement, error) {
	p.i++

	st := &Statement{Kind: StmtDrop}

	switch {
	case p.acceptKw("TABLE"):
		st.dropKind = "TABLE"
	case p.acceptKw("VIEW"):
		st.dropKind = "VIEW"
	case p.acceptKw("SCHEMA"):
		st.dropKind = "SCHEMA"
	default:
		return nil, p.unexpected("expected TABLE, VIEW or SCHEMA")
	}

	if p.kw("IF") {
		p.i++

		if err := p.expectKw("EXISTS"); err != nil {
			return nil, err
		}

		st.ifExists = true
	}

	path, err := p.tablePath()
	if err != nil {
		return nil, err
	}

	st.target = path

	if p.acceptKw("CASCADE") {
		st.cascade = true
	} else {
		p.acceptKw("RESTRICT")
	}

	return st, nil
}

func (p *parser) truncateStmt() (*Statement, error) {
	p.i++

	if err := p.expectKw("TABLE"); err != nil {
		return nil, err
	}

	path, err := p.tablePath()
	if err != nil {
		return nil, err
	}

	return &Statement{Kind: StmtTruncate, target: path}, nil
}

func (p *parser) alterStmt() (*Statement, error) {
	p.i++

	if err := p.expectKw("TABLE"); err != nil {
		return nil, err
	}

	st := &Statement{Kind: StmtAlter}

	if p.kw("IF") {
		p.i++

		if err := p.expectKw("EXISTS"); err != nil {
			return nil, err
		}

		st.ifExists = true
	}

	path, err := p.tablePath()
	if err != nil {
		return nil, err
	}

	st.target = path

	switch {
	case p.kw("ADD") && p.peekNext().isKw("COLUMN"):
		p.i += 2
		st.alterOp = "ADD"
		st.alterCols, err = p.alterAddColumn(st)
	case p.kw("DROP") && p.peekNext().isKw("COLUMN"):
		p.i += 2
		st.alterOp = "DROP"
		st.alterCols, err = p.alterDropColumn()
	case p.kw("SET") && p.peekNext().isKw("OPTIONS"):
		p.i++
		st.alterOp = "OPTIONS"
		st.options, err = p.optionsClause()
	default:
		return nil, p.unexpected("only ADD COLUMN, DROP COLUMN and SET OPTIONS are supported; got")
	}

	return st, err
}

func (p *parser) alterAddColumn(st *Statement) ([]colDef, error) {
	if p.kw("IF") {
		p.i++

		if err := p.expectKw(litNot); err != nil {
			return nil, err
		}

		if err := p.expectKw("EXISTS"); err != nil {
			return nil, err
		}

		st.ifNot = true
	}

	name, err := p.identName()
	if err != nil {
		return nil, err
	}

	t, err := p.typeSpec()
	if err != nil {
		return nil, err
	}

	return []colDef{{name: name, typ: t}}, nil
}

func (p *parser) alterDropColumn() ([]colDef, error) {
	if p.kw("IF") {
		p.i++

		if err := p.expectKw("EXISTS"); err != nil {
			return nil, err
		}
	}

	name, err := p.identName()
	if err != nil {
		return nil, err
	}

	return []colDef{{name: name}}, nil
}
