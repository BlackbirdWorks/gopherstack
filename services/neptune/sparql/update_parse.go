package sparql

// ParseUpdate parses a SPARQL 1.1 Update request.
func ParseUpdate(src string) (*Update, error) {
	p, err := newParser(src)
	if err != nil {
		return nil, err
	}

	u := &Update{}

	for {
		if err = p.prologue(); err != nil {
			return nil, err
		}

		if p.peek().kind == tEOF {
			break
		}

		op, oerr := p.parseUpdateOp()
		if oerr != nil {
			return nil, oerr
		}

		u.ops = append(u.ops, op)

		if !p.acceptP(";") {
			break
		}
	}

	if p.peek().kind != tEOF {
		return nil, p.errf("unexpected trailing input")
	}

	if len(u.ops) == 0 {
		return nil, syntaxf("empty update request")
	}

	return u, nil
}

func (p *parser) parseUpdateOp() (updateOp, error) {
	t := p.peek()

	switch {
	case t.kw("LOAD"):
		return p.parseLoad()
	case t.kw("CLEAR"), t.kw("DROP"):
		return p.parseClearDrop()
	case t.kw("CREATE"):
		return p.parseCreate()
	case t.kw("ADD"), t.kw("MOVE"), t.kw("COPY"):
		return p.parseAddMoveCopy()
	case t.kw("INSERT") && p.peekNext().kw("DATA"):
		p.i += 2

		return p.parseData(upInsertData)
	case t.kw("DELETE") && p.peekNext().kw("DATA"):
		p.i += 2

		return p.parseData(upDeleteData)
	case t.kw("DELETE") && p.peekNext().kw("WHERE"):
		p.i += 2

		return p.parseDeleteWhere()
	case t.kw("WITH"), t.kw("DELETE"), t.kw("INSERT"):
		return p.parseModify()
	default:
		return updateOp{}, p.errf("expected update operation")
	}
}

func (p *parser) parseCreate() (updateOp, error) {
	p.i++

	op := updateOp{Kind: upCreate, Silent: p.acceptKW("SILENT")}
	if err := p.expectKW("GRAPH"); err != nil {
		return op, err
	}

	var err error
	op.Graph, err = p.iriTok()

	return op, err
}

func (p *parser) parseDeleteWhere() (updateOp, error) {
	qp, err := p.parseQuadPatterns()
	if err != nil {
		return updateOp{}, err
	}

	g := &group{}
	for _, q := range qp {
		g.Elems = append(g.Elems, graphOrBGP(q))
	}

	return updateOp{Kind: upModify, Del: qp, Where: g}, nil
}

func graphOrBGP(q quadPat) element {
	b := &bgpEl{Triples: []triplePat{q.T}}
	if q.G.isVar() || q.G.Term.Bound() {
		return &graphEl{Name: q.G, G: &group{Elems: []element{b}}}
	}

	return b
}

func (p *parser) parseLoad() (updateOp, error) {
	p.i++

	op := updateOp{Kind: upLoad, Silent: p.acceptKW("SILENT")}

	src, err := p.iriTok()
	if err != nil {
		return op, err
	}

	op.Src = src.Value

	if p.acceptKW("INTO") {
		if err = p.expectKW("GRAPH"); err != nil {
			return op, err
		}

		op.Into, err = p.iriTok()
	}

	return op, err
}

func (p *parser) parseClearDrop() (updateOp, error) {
	op := updateOp{Kind: upClear}
	if p.advance().kw("DROP") {
		op.Kind = upDrop
	}

	op.Silent = p.acceptKW("SILENT")

	var err error

	op.Target, op.Graph, err = p.parseGraphRefAll()

	return op, err
}

func (p *parser) parseGraphRefAll() (graphTarget, Term, error) {
	switch {
	case p.acceptKW("DEFAULT"):
		return tgDefault, Term{}, nil
	case p.acceptKW("NAMED"):
		return tgNamed, Term{}, nil
	case p.acceptKW("ALL"):
		return tgAll, Term{}, nil
	default:
		if err := p.expectKW("GRAPH"); err != nil {
			return 0, Term{}, err
		}

		g, err := p.iriTok()

		return tgGraph, g, err
	}
}

func (p *parser) parseGraphOrDefault() (graphTarget, Term, error) {
	if p.acceptKW("DEFAULT") {
		return tgDefault, Term{}, nil
	}

	p.acceptKW("GRAPH")

	g, err := p.iriTok()

	return tgGraph, g, err
}

func (p *parser) parseAddMoveCopy() (updateOp, error) {
	op := updateOp{}

	switch t := p.advance(); {
	case t.kw("ADD"):
		op.Kind = upAdd
	case t.kw("MOVE"):
		op.Kind = upMove
	default:
		op.Kind = upCopy
	}

	op.Silent = p.acceptKW("SILENT")

	var err error
	if op.Target, op.Graph, err = p.parseGraphOrDefault(); err != nil {
		return op, err
	}

	if err = p.expectKW("TO"); err != nil {
		return op, err
	}

	op.ToTarget, op.To, err = p.parseGraphOrDefault()

	return op, err
}

func (p *parser) parseData(kind updateKind) (updateOp, error) {
	qp, err := p.parseQuadPatterns()
	if err != nil {
		return updateOp{}, err
	}

	for _, q := range qp {
		if q.G.isVar() || q.T.S.isVar() || q.T.P.isVar() || q.T.O.isVar() || q.T.Path != nil {
			if kind == upInsertData && isBlankVar(q.T) {
				continue
			}

			return updateOp{}, syntaxf("variables are not allowed in INSERT DATA / DELETE DATA")
		}
	}

	op := updateOp{Kind: kind}
	if kind == upInsertData {
		op.Ins = qp
	} else {
		op.Del = qp
	}

	return op, nil
}

// isBlankVar reports whether a pattern's only "variables" are parser-generated blank nodes.
func isBlankVar(t triplePat) bool {
	ok := func(n node) bool { return !n.isVar() || (len(n.Var) > 2 && n.Var[:2] == "_:") }

	return t.Path == nil && ok(t.S) && ok(t.P) && ok(t.O)
}

func (p *parser) parseModify() (updateOp, error) {
	op := updateOp{Kind: upModify}

	if p.acceptKW("WITH") {
		w, err := p.iriTok()
		if err != nil {
			return op, err
		}

		op.With = w
	}

	var err error

	switch {
	case p.acceptKW("DELETE"):
		if op.Del, err = p.parseQuadPatterns(); err != nil {
			return op, err
		}

		if p.acceptKW("INSERT") {
			op.Ins, err = p.parseQuadPatterns()
		}
	case p.acceptKW("INSERT"):
		op.Ins, err = p.parseQuadPatterns()
	default:
		err = p.errf("expected DELETE or INSERT")
	}

	if err != nil {
		return op, err
	}

	for p.acceptKW("USING") {
		named := p.acceptKW("NAMED")

		g, gerr := p.iriTok()
		if gerr != nil {
			return op, gerr
		}

		if named {
			op.UsingNamed = append(op.UsingNamed, g)
		} else {
			op.Using = append(op.Using, g)
		}
	}

	if err = p.expectKW("WHERE"); err != nil {
		return op, err
	}

	op.Where, err = p.parseGroup()

	return op, err
}

// parseQuadPatterns parses '{' (triples | GRAPH g { triples })* '}'.
func (p *parser) parseQuadPatterns() ([]quadPat, error) {
	if err := p.expectP("{"); err != nil {
		return nil, err
	}

	var out []quadPat

	for !p.peek().is("}") {
		if p.peek().kind == tEOF {
			return nil, p.errf("unterminated quad pattern")
		}

		if p.acceptP(".") {
			continue
		}

		var err error
		if p.acceptKW("GRAPH") {
			out, err = p.parseGraphBlock(out)
		} else {
			err = p.parseTriplesSameSubject(func(tp triplePat) { out = append(out, quadPat{T: tp}) })
		}

		if err != nil {
			return nil, err
		}
	}

	p.i++

	return out, nil
}

// parseGraphBlock parses 'varOrIRI { triples }' after GRAPH, appending quads for that graph.
func (p *parser) parseGraphBlock(out []quadPat) ([]quadPat, error) {
	name, err := p.parseVarOrIRI()
	if err != nil {
		return nil, err
	}

	if err = p.expectP("{"); err != nil {
		return nil, err
	}

	for !p.peek().is("}") {
		if p.peek().kind == tEOF {
			return nil, p.errf("unterminated GRAPH block")
		}

		if p.acceptP(".") {
			continue
		}

		if err = p.parseTriplesSameSubject(
			func(tp triplePat) { out = append(out, quadPat{G: name, T: tp}) },
		); err != nil {
			return nil, err
		}
	}

	p.i++

	return out, nil
}
