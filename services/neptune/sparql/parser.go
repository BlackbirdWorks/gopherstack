package sparql

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

type parser struct {
	prefixes map[string]string
	base     string
	toks     []token
	i        int
	bn       int
}

func newParser(src string) (*parser, error) {
	toks, err := lexAll(src)
	if err != nil {
		return nil, err
	}

	return &parser{
		toks: toks, base: DefaultBase,
		prefixes: map[string]string{"rdf": NSRDF, "rdfs": NSRDFS, "owl": NSOWL, "xsd": NSXSD},
	}, nil
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
	if t.kind != tEOF {
		p.i++
	}

	return t
}

func (p *parser) errf(format string, args ...any) error {
	t := p.peek()

	return syntaxf("%s (at offset %d near %q)", fmt.Sprintf(format, args...), t.pos, t.val)
}

func (p *parser) acceptP(s string) bool {
	if p.peek().is(s) {
		p.i++

		return true
	}

	return false
}

func (p *parser) expectP(s string) error {
	if !p.acceptP(s) {
		return p.errf("expected %q", s)
	}

	return nil
}

func (p *parser) acceptKW(k string) bool {
	if p.peek().kw(k) {
		p.i++

		return true
	}

	return false
}

func (p *parser) expectKW(k string) error {
	if !p.acceptKW(k) {
		return p.errf("expected %s", k)
	}

	return nil
}

func (p *parser) freshBlank() node {
	p.bn++

	return varNode("_:b" + strconv.Itoa(p.bn))
}

func (p *parser) resolve(ref string) string {
	if u, err := url.Parse(ref); err == nil && u.Scheme != "" {
		return ref
	}

	b, err := url.Parse(p.base)
	if err != nil {
		return ref
	}

	r, err := url.Parse(ref)
	if err != nil {
		return ref
	}

	return b.ResolveReference(r).String()
}

func (p *parser) prologue() error {
	for {
		switch {
		case p.acceptKW("BASE"):
			t := p.advance()
			if t.kind != tIRI {
				return p.errf("expected IRI after BASE")
			}

			p.base = p.resolve(t.val)
		case p.acceptKW("PREFIX"):
			t := p.advance()
			if t.kind != tPName || !strings.HasSuffix(t.val, ":") {
				return p.errf("expected prefix name")
			}

			iri := p.advance()
			if iri.kind != tIRI {
				return p.errf("expected IRI in PREFIX")
			}

			p.prefixes[strings.TrimSuffix(t.val, ":")] = p.resolve(iri.val)
		default:
			return nil
		}
	}
}

func (p *parser) expandPName(v string) (string, error) {
	i := strings.IndexByte(v, ':')
	ns, ok := p.prefixes[v[:i]]

	if !ok {
		return "", syntaxf("undefined prefix %q", v[:i])
	}

	local := v[i+1:]
	if strings.Contains(local, `\`) {
		local = strings.NewReplacer(`\`, "").Replace(local)
	}

	return ns + local, nil
}

// iriTok consumes an IRI or prefixed name.
func (p *parser) iriTok() (Term, error) {
	t := p.peek()

	switch t.kind {
	case tIRI:
		p.i++

		return IRI(p.resolve(t.val)), nil
	case tPName:
		p.i++

		s, err := p.expandPName(t.val)
		if err != nil {
			return Term{}, err
		}

		return IRI(s), nil
	default:
		return Term{}, p.errf("expected IRI")
	}
}

func (p *parser) isIRIStart() bool {
	k := p.peek().kind

	return k == tIRI || k == tPName
}

// ParseQuery parses a SPARQL 1.1 query.
func ParseQuery(src string) (*Query, error) {
	p, err := newParser(src)
	if err != nil {
		return nil, err
	}

	if err = p.prologue(); err != nil {
		return nil, err
	}

	q, err := p.parseQueryBody()
	if err != nil {
		return nil, err
	}

	if p.peek().kind != tEOF {
		return nil, p.errf("unexpected trailing input")
	}

	q.Prefixes, q.Base = p.prefixes, p.base

	return q, nil
}

func (p *parser) parseQueryBody() (*Query, error) {
	q := &Query{Limit: -1}

	var err error

	switch {
	case p.peek().kw("SELECT"):
		err = p.parseSelectClause(q)
	case p.acceptKW("ASK"):
		q.Form = formAsk
	case p.acceptKW("CONSTRUCT"):
		q.Form = formConstruct
		err = p.parseConstructHead(q)
	case p.acceptKW("DESCRIBE"):
		q.Form = formDescribe
		err = p.parseDescribeHead(q)
	default:
		return nil, p.errf("expected SELECT, ASK, CONSTRUCT or DESCRIBE")
	}

	if err != nil {
		return nil, err
	}

	if err = p.parseDatasetClauses(q); err != nil {
		return nil, err
	}

	if q.Where == nil {
		p.acceptKW("WHERE")

		if q.Form == formDescribe && !p.peek().is("{") {
			q.Where = &group{}
		} else if q.Where, err = p.parseGroup(); err != nil {
			return nil, err
		}
	}

	if err = p.parseModifiers(q); err != nil {
		return nil, err
	}

	if p.peek().kw("VALUES") {
		p.i++

		if q.Values, err = p.parseValues(); err != nil {
			return nil, err
		}
	}

	return q, nil
}

func (p *parser) parseSelectClause(q *Query) error {
	p.i++
	q.Form = formSelect

	if p.acceptKW("DISTINCT") || p.acceptKW("REDUCED") {
		q.Distinct = true
	}

	if p.acceptP("*") {
		q.Star = true

		return nil
	}

	for {
		switch t := p.peek(); {
		case t.kind == tVar:
			p.i++
			q.Proj = append(q.Proj, projItem{Var: t.val})
		case t.is("("):
			item, err := p.parseBracketedAlias(true)
			if err != nil {
				return err
			}

			q.Proj = append(q.Proj, item)
		default:
			if len(q.Proj) == 0 {
				return p.errf("expected projection")
			}

			return nil
		}
	}
}

// parseBracketedAlias parses '(' expr [AS ?var] ')'; the alias is mandatory when required is set.
func (p *parser) parseBracketedAlias(required bool) (projItem, error) {
	p.i++

	e, err := p.parseExpr()
	if err != nil {
		return projItem{}, err
	}

	item := projItem{Expr: e}

	if required || p.peek().kw("AS") {
		if err = p.expectKW("AS"); err != nil {
			return item, err
		}

		v := p.advance()
		if v.kind != tVar {
			return item, p.errf("expected variable after AS")
		}

		item.Var = v.val
	}

	return item, p.expectP(")")
}

func (p *parser) parseConstructHead(q *Query) error {
	if p.peek().is("{") {
		p.i++

		var tps []triplePat

		for !p.peek().is("}") {
			if p.peek().kind == tEOF {
				return p.errf("unterminated CONSTRUCT template")
			}

			if err := p.parseTriplesSameSubject(func(tp triplePat) { tps = append(tps, tp) }); err != nil {
				return err
			}

			p.acceptP(".")
		}

		p.i++
		q.Template = tps

		return nil
	}

	// CONSTRUCT WHERE { triples }: the template is the pattern itself.
	if err := p.parseDatasetClauses(q); err != nil {
		return err
	}

	if err := p.expectKW("WHERE"); err != nil {
		return err
	}

	g, err := p.parseGroup()
	if err != nil {
		return err
	}

	q.Where = g

	for _, el := range g.Elems {
		b, ok := el.(*bgpEl)
		if !ok {
			return syntaxf("CONSTRUCT WHERE allows only triple patterns")
		}

		q.Template = append(q.Template, b.Triples...)
	}

	return nil
}

func (p *parser) parseDescribeHead(q *Query) error {
	if p.acceptP("*") {
		q.Star = true

		return nil
	}

	for {
		switch t := p.peek(); {
		case t.kind == tVar:
			p.i++
			q.Describe = append(q.Describe, varNode(t.val))
		case p.isIRIStart():
			iri, err := p.iriTok()
			if err != nil {
				return err
			}

			q.Describe = append(q.Describe, termNode(iri))
		default:
			if len(q.Describe) == 0 {
				return p.errf("expected DESCRIBE target")
			}

			return nil
		}
	}
}

func (p *parser) parseDatasetClauses(q *Query) error {
	for p.peek().kw("FROM") {
		p.i++

		named := p.acceptKW("NAMED")

		iri, err := p.iriTok()
		if err != nil {
			return err
		}

		if named {
			q.FromNamed = append(q.FromNamed, iri)
		} else {
			q.From = append(q.From, iri)
		}
	}

	return nil
}

func (p *parser) parseModifiers(q *Query) error {
	if p.acceptKW("GROUP") {
		if err := p.expectKW("BY"); err != nil {
			return err
		}

		if err := p.parseGroupBy(q); err != nil {
			return err
		}
	}

	for p.acceptKW("HAVING") {
		e, err := p.parsePrimary()
		if err != nil {
			return err
		}

		q.Having = append(q.Having, e)
	}

	if p.acceptKW("ORDER") {
		if err := p.expectKW("BY"); err != nil {
			return err
		}

		if err := p.parseOrderBy(q); err != nil {
			return err
		}
	}

	return p.parseSlice(q)
}

func (p *parser) parseSlice(q *Query) error {
	for {
		var dst *int

		switch {
		case p.acceptKW("LIMIT"):
			dst = &q.Limit
		case p.acceptKW("OFFSET"):
			dst = &q.Offset
		default:
			return nil
		}

		n, err := p.intTok()
		if err != nil {
			return err
		}

		*dst = n
	}
}

func (p *parser) intTok() (int, error) {
	t := p.advance()
	if t.kind != tInt {
		return 0, p.errf("expected integer")
	}

	n, err := strconv.Atoi(t.val)
	if err != nil {
		return 0, syntaxf("integer out of range")
	}

	return n, nil
}

func (p *parser) parseGroupBy(q *Query) error {
	q.grouped = true

	for n := 0; ; n++ {
		switch t := p.peek(); {
		case t.kind == tVar:
			p.i++
			q.GroupBy = append(q.GroupBy, projItem{Var: t.val, Expr: exprVar{Name: t.val}})
		case t.is("("):
			item, err := p.parseBracketedAlias(false)
			if err != nil {
				return err
			}

			q.GroupBy = append(q.GroupBy, item)
		case t.kind == tIdent && p.peekNext().is("(") && !t.kw("HAVING"), p.isIRIStart():
			e, err := p.parsePrimary()
			if err != nil {
				return err
			}

			q.GroupBy = append(q.GroupBy, projItem{Expr: e})
		default:
			if n == 0 {
				return p.errf("expected GROUP BY condition")
			}

			return nil
		}
	}
}

func (p *parser) parseOrderBy(q *Query) error {
	for n := 0; ; n++ {
		desc := p.peek().kw("DESC")
		explicit := desc || p.peek().kw("ASC")

		if explicit {
			p.i++
		}

		t := p.peek()

		switch {
		case t.kind == tVar && !explicit:
			p.i++
			q.Order = append(q.Order, orderCond{Expr: exprVar{Name: t.val}})
		case t.is("(") || explicit || (t.kind == tIdent && p.peekNext().is("(")) || p.isIRIStart():
			e, err := p.parsePrimary()
			if err != nil {
				return err
			}

			q.Order = append(q.Order, orderCond{Expr: e, Desc: desc})
		default:
			if n == 0 {
				return p.errf("expected ORDER BY condition")
			}

			return nil
		}
	}
}

func (p *parser) parseValues() (*valuesData, error) {
	vd := &valuesData{}
	multi := p.acceptP("(")

	if err := p.parseValuesVars(vd, multi); err != nil {
		return nil, err
	}

	if err := p.expectP("{"); err != nil {
		return nil, err
	}

	for !p.peek().is("}") {
		row, err := p.parseValuesRow(multi)
		if err != nil {
			return nil, err
		}

		if len(row) != len(vd.Vars) {
			return nil, syntaxf("VALUES row has %d terms for %d variables", len(row), len(vd.Vars))
		}

		vd.Rows = append(vd.Rows, row)
	}

	p.i++

	return vd, nil
}

func (p *parser) parseValuesVars(vd *valuesData, multi bool) error {
	if !multi {
		t := p.advance()
		if t.kind != tVar {
			return p.errf("expected variable in VALUES")
		}

		vd.Vars = []string{t.val}

		return nil
	}

	for p.peek().kind == tVar {
		vd.Vars = append(vd.Vars, p.advance().val)
	}

	return p.expectP(")")
}

func (p *parser) parseValuesRow(multi bool) ([]Term, error) {
	if !multi {
		t, err := p.parseDataValue()

		return []Term{t}, err
	}

	if err := p.expectP("("); err != nil {
		return nil, err
	}

	var row []Term

	for !p.peek().is(")") {
		if p.peek().kind == tEOF {
			return nil, p.errf("unterminated VALUES row")
		}

		t, err := p.parseDataValue()
		if err != nil {
			return nil, err
		}

		row = append(row, t)
	}

	p.i++

	return row, nil
}

func (p *parser) parseDataValue() (Term, error) {
	if p.acceptKW("UNDEF") {
		return Term{}, nil
	}

	n, err := p.parseGraphTerm()
	if err != nil {
		return Term{}, err
	}

	if n.isVar() {
		return Term{}, syntaxf("variable not allowed in VALUES data")
	}

	return n.Term, nil
}

func (p *parser) parseGroup() (*group, error) {
	if err := p.expectP("{"); err != nil {
		return nil, err
	}

	if p.peek().kw("SELECT") {
		sub, err := p.parseSubSelect()
		if err != nil {
			return nil, err
		}

		if err = p.expectP("}"); err != nil {
			return nil, err
		}

		return &group{Elems: []element{&subSelectEl{Q: sub}}}, nil
	}

	g := &group{}

	for !p.peek().is("}") {
		if p.peek().kind == tEOF {
			return nil, p.errf("unterminated group pattern")
		}

		if err := p.parseElement(g); err != nil {
			return nil, err
		}
	}

	p.i++

	return g, nil
}

func (p *parser) parseSubSelect() (*Query, error) {
	q := &Query{Limit: -1}

	if err := p.parseSelectClause(q); err != nil {
		return nil, err
	}

	p.acceptKW("WHERE")

	var err error
	if q.Where, err = p.parseGroup(); err != nil {
		return nil, err
	}

	if err = p.parseModifiers(q); err != nil {
		return nil, err
	}

	if p.peek().kw("VALUES") {
		p.i++

		if q.Values, err = p.parseValues(); err != nil {
			return nil, err
		}
	}

	return q, nil
}

func (p *parser) parseElement(g *group) error {
	t := p.peek()

	switch {
	case t.is("."):
		p.i++

		return nil
	case t.kw("OPTIONAL"):
		return p.parseSubGroup(g, func(sub *group) element { return &optionalEl{G: sub} })
	case t.kw("MINUS"):
		return p.parseSubGroup(g, func(sub *group) element { return &minusEl{G: sub} })
	case t.kw("GRAPH"):
		return p.parseGraphElement(g)
	case t.kw("FILTER"):
		p.i++

		e, err := p.parsePrimary()
		if err != nil {
			return err
		}

		g.Elems = append(g.Elems, &filterEl{E: e})

		return nil
	case t.kw("BIND"):
		return p.parseBind(g)
	case t.kw("VALUES"):
		p.i++

		vd, err := p.parseValues()
		if err != nil {
			return err
		}

		g.Elems = append(g.Elems, &valuesEl{V: vd})

		return nil
	case t.kw("SERVICE"):
		return syntaxf("SERVICE federation is not supported")
	case t.is("{"):
		return p.parseUnionChain(g)
	default:
		return p.parseTriplesBlock(g)
	}
}

func (p *parser) parseSubGroup(g *group, mk func(*group) element) error {
	p.i++

	sub, err := p.parseGroup()
	if err != nil {
		return err
	}

	g.Elems = append(g.Elems, mk(sub))

	return nil
}

func (p *parser) parseGraphElement(g *group) error {
	p.i++

	name, err := p.parseVarOrIRI()
	if err != nil {
		return err
	}

	sub, err := p.parseGroup()
	if err != nil {
		return err
	}

	g.Elems = append(g.Elems, &graphEl{Name: name, G: sub})

	return nil
}

func (p *parser) parseTriplesBlock(g *group) error {
	var tps []triplePat

	if err := p.parseTriplesSameSubject(func(tp triplePat) { tps = append(tps, tp) }); err != nil {
		return err
	}

	if n := len(g.Elems); n > 0 {
		if b, ok := g.Elems[n-1].(*bgpEl); ok {
			b.Triples = append(b.Triples, tps...)

			return nil
		}
	}

	g.Elems = append(g.Elems, &bgpEl{Triples: tps})

	return nil
}

func (p *parser) parseBind(g *group) error {
	p.i++

	if err := p.expectP("("); err != nil {
		return err
	}

	e, err := p.parseExpr()
	if err != nil {
		return err
	}

	if err = p.expectKW("AS"); err != nil {
		return err
	}

	v := p.advance()
	if v.kind != tVar {
		return p.errf("expected variable after AS")
	}

	if err = p.expectP(")"); err != nil {
		return err
	}

	g.Elems = append(g.Elems, &bindEl{E: e, Var: v.val})

	return nil
}

func (p *parser) parseUnionChain(g *group) error {
	first, err := p.parseGroup()
	if err != nil {
		return err
	}

	if !p.peek().kw("UNION") {
		g.Elems = append(g.Elems, &groupEl{G: first})

		return nil
	}

	alts := []*group{first}

	for p.acceptKW("UNION") {
		next, gerr := p.parseGroup()
		if gerr != nil {
			return gerr
		}

		alts = append(alts, next)
	}

	g.Elems = append(g.Elems, &unionEl{Alts: alts})

	return nil
}

func (p *parser) parseVarOrIRI() (node, error) {
	if t := p.peek(); t.kind == tVar {
		p.i++

		return varNode(t.val), nil
	}

	iri, err := p.iriTok()

	return termNode(iri), err
}

// parseTriplesSameSubject parses subject + property list (and nested blank nodes / collections).
func (p *parser) parseTriplesSameSubject(emit func(triplePat)) error {
	subj, done, err := p.parseSubject(emit)
	if err != nil || done {
		return err
	}

	return p.parsePropertyList(subj, emit)
}

// parseSubject parses a triples subject; done is set for a bare '[ ... ]' statement with no further predicates.
func (p *parser) parseSubject(emit func(triplePat)) (node, bool, error) {
	if !p.peek().is("[") || p.peekNext().is("]") {
		n, err := p.parseNodeWithCollection(emit)

		return n, false, err
	}

	p.i++
	subj := p.freshBlank()

	if err := p.parsePropertyList(subj, emit); err != nil {
		return subj, false, err
	}

	if err := p.expectP("]"); err != nil {
		return subj, false, err
	}

	return subj, p.peek().is(".") || p.peek().is("}"), nil
}

func (p *parser) parseNodeWithCollection(emit func(triplePat)) (node, error) {
	switch {
	case p.peek().is("("):
		return p.parseCollection(emit)
	case p.peek().is("[") && !p.peekNext().is("]"):
		p.i++
		b := p.freshBlank()

		if err := p.parsePropertyList(b, emit); err != nil {
			return node{}, err
		}

		return b, p.expectP("]")
	default:
		return p.parseGraphTerm()
	}
}

func (p *parser) parseCollection(emit func(triplePat)) (node, error) {
	p.i++

	var items []node

	for !p.peek().is(")") {
		if p.peek().kind == tEOF {
			return node{}, p.errf("unterminated collection")
		}

		n, err := p.parseNodeWithCollection(emit)
		if err != nil {
			return node{}, err
		}

		items = append(items, n)
	}

	p.i++

	if len(items) == 0 {
		return termNode(IRI(RDFNil)), nil
	}

	head := p.freshBlank()
	cur := head

	for i, it := range items {
		emit(triplePat{S: cur, P: termNode(IRI(RDFFirst)), O: it})

		if i == len(items)-1 {
			emit(triplePat{S: cur, P: termNode(IRI(RDFRest)), O: termNode(IRI(RDFNil))})

			break
		}

		next := p.freshBlank()
		emit(triplePat{S: cur, P: termNode(IRI(RDFRest)), O: next})
		cur = next
	}

	return head, nil
}

func (p *parser) parsePropertyList(subj node, emit func(triplePat)) error {
	for {
		verb, pth, err := p.parseVerb()
		if err != nil {
			return err
		}

		for {
			obj, oerr := p.parseNodeWithCollection(emit)
			if oerr != nil {
				return oerr
			}

			emit(triplePat{S: subj, P: verb, Path: pth, O: obj})

			if !p.acceptP(",") {
				break
			}
		}

		if !p.acceptP(";") {
			return nil
		}

		for p.acceptP(";") {
		}

		if t := p.peek(); t.is(".") || t.is("}") || t.is("]") || t.kind == tEOF {
			return nil
		}
	}
}

func (p *parser) parseVerb() (node, *path, error) {
	t := p.peek()

	if t.kind == tVar {
		p.i++

		return varNode(t.val), nil, nil
	}

	pth, err := p.parsePathAlt()
	if err != nil {
		return node{}, nil, err
	}

	if pth.kind == pLink {
		return termNode(pth.iri), nil, nil
	}

	return node{}, pth, nil
}

func (p *parser) parsePathAlt() (*path, error) {
	first, err := p.parsePathSeq()
	if err != nil {
		return nil, err
	}

	if !p.peek().is("|") {
		return first, nil
	}

	alt := &path{kind: pAlt, subs: []*path{first}}

	for p.acceptP("|") {
		next, serr := p.parsePathSeq()
		if serr != nil {
			return nil, serr
		}

		alt.subs = append(alt.subs, next)
	}

	return alt, nil
}

func (p *parser) parsePathSeq() (*path, error) {
	first, err := p.parsePathElt()
	if err != nil {
		return nil, err
	}

	if !p.peek().is("/") {
		return first, nil
	}

	seq := &path{kind: pSeq, subs: []*path{first}}

	for p.acceptP("/") {
		next, serr := p.parsePathElt()
		if serr != nil {
			return nil, serr
		}

		seq.subs = append(seq.subs, next)
	}

	return seq, nil
}

func (p *parser) parsePathElt() (*path, error) {
	inv := p.acceptP("^")

	prim, err := p.parsePathPrimary()
	if err != nil {
		return nil, err
	}

	switch {
	case p.acceptP("*"):
		prim = &path{kind: pZeroOrMore, subs: []*path{prim}}
	case p.acceptP("+"):
		prim = &path{kind: pOneOrMore, subs: []*path{prim}}
	case p.acceptP("?"):
		prim = &path{kind: pZeroOrOne, subs: []*path{prim}}
	}

	if inv {
		prim = &path{kind: pInv, subs: []*path{prim}}
	}

	return prim, nil
}

func (p *parser) parsePathPrimary() (*path, error) {
	t := p.peek()

	switch {
	case t.kw("a"):
		p.i++

		return &path{kind: pLink, iri: IRI(RDFType)}, nil
	case t.is("("):
		p.i++

		sub, err := p.parsePathAlt()
		if err != nil {
			return nil, err
		}

		return sub, p.expectP(")")
	case t.is("!"):
		p.i++

		return p.parseNegatedSet()
	default:
		iri, err := p.iriTok()
		if err != nil {
			return nil, err
		}

		return &path{kind: pLink, iri: iri}, nil
	}
}

func (p *parser) parseNegatedSet() (*path, error) {
	neg := &path{kind: pNeg}

	one := func() error {
		inv := p.acceptP("^")

		if p.acceptKW("a") {
			neg.neg = append(neg.neg, negEntry{iri: IRI(RDFType), inv: inv})

			return nil
		}

		iri, err := p.iriTok()
		if err != nil {
			return err
		}

		neg.neg = append(neg.neg, negEntry{iri: iri, inv: inv})

		return nil
	}

	if !p.acceptP("(") {
		return neg, one()
	}

	if !p.peek().is(")") {
		if err := one(); err != nil {
			return nil, err
		}

		for p.acceptP("|") {
			if err := one(); err != nil {
				return nil, err
			}
		}
	}

	return neg, p.expectP(")")
}

// parseGraphTerm parses a variable, IRI, literal or blank node.
func (p *parser) parseGraphTerm() (node, error) {
	t := p.peek()

	switch t.kind {
	case tVar:
		p.i++

		return varNode(t.val), nil
	case tIRI, tPName:
		iri, err := p.iriTok()

		return termNode(iri), err
	case tBlank:
		p.i++

		return varNode("_:" + t.val), nil
	case tString, tInt, tDecimal, tDouble:
		lit, err := p.parseLiteral()

		return termNode(lit), err
	case tIdent:
		if t.kw(lexTrue) || t.kw(lexFalse) {
			p.i++

			return termNode(Typed(strings.ToLower(t.val), XSDBoolean)), nil
		}
	case tPunct:
		switch {
		case t.is("[") && p.peekNext().is("]"):
			p.i += 2

			return p.freshBlank(), nil
		case t.is("+") || t.is("-"):
			lit, err := p.parseLiteral()

			return termNode(lit), err
		}
	default:
	}

	return node{}, p.errf("expected RDF term")
}

func (p *parser) parseLiteral() (Term, error) {
	t := p.advance()
	sign := ""

	if t.is("+") || t.is("-") {
		if t.val == "-" {
			sign = "-"
		}

		t = p.advance()
	}

	switch t.kind {
	case tInt:
		if _, err := strconv.ParseInt(sign+t.val, 10, 64); err != nil {
			return Term{}, &NumericDataError{
				Msg: "Integer literal " + sign + t.val + " exceeds the 64-bit storage limit",
			}
		}

		return Typed(sign+t.val, XSDInteger), nil
	case tDecimal:
		return Typed(sign+t.val, XSDDecimal), nil
	case tDouble:
		return Typed(sign+t.val, XSDDouble), nil
	case tString:
		if lt := p.peek(); lt.kind == tLang {
			p.i++

			return LangLiteral(t.val, lt.val), nil
		}

		if p.acceptP("^^") {
			dt, err := p.iriTok()
			if err != nil {
				return Term{}, err
			}

			return Typed(t.val, dt.Value), nil
		}

		return Literal(t.val), nil
	default:
		return Term{}, p.errf("expected literal")
	}
}
