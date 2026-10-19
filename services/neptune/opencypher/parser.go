package opencypher

import (
	"strconv"
	"strings"
)

type parser struct {
	src  string
	toks []token
	pos  int
}

func parse(src string) (*query, error) {
	toks, err := lex(src)
	if err != nil {
		return nil, err
	}

	p := &parser{src: src, toks: toks}
	q := &query{}

	for {
		for p.accept(";") {
		}

		if p.peek().kind == tkEOF {
			break
		}

		c, werr := p.parseClause()
		if werr != nil {
			return nil, werr
		}

		switch c.(type) {
		case createClause, mergeClause, setClause, removeClause, deleteClause:
			q.write = true
		}

		q.clauses = append(q.clauses, c)
	}

	if len(q.clauses) == 0 {
		return nil, malformed("Query is empty")
	}

	return q, p.validate(q)
}

func (p *parser) validate(q *query) error {
	last := q.clauses[len(q.clauses)-1]
	switch last.(type) {
	case returnClause, createClause, mergeClause, setClause, removeClause, deleteClause:
		return nil
	default:
		return malformed("Query cannot conclude with %s (must be RETURN or an update clause)", clauseName(last))
	}
}

func clauseName(c clause) string {
	switch c.(type) {
	case matchClause:
		return "MATCH"
	case withClause:
		return "WITH"
	case unwindClause:
		return "UNWIND"
	default:
		return "clause"
	}
}

func (p *parser) peek() token { return p.toks[p.pos] }

func (p *parser) peek2() token {
	if p.pos+1 >= len(p.toks) {
		return p.toks[len(p.toks)-1]
	}

	return p.toks[p.pos+1]
}

func (p *parser) next() token {
	t := p.toks[p.pos]
	if t.kind != tkEOF {
		p.pos++
	}

	return t
}

func (p *parser) accept(word string) bool {
	if p.peek().is(word) {
		p.pos++

		return true
	}

	return false
}

func (p *parser) expect(word string) error {
	if !p.accept(word) {
		return p.unexpected("'" + word + "'")
	}

	return nil
}

func (p *parser) unexpected(want string) error {
	t := p.peek()
	got := "end of input"

	if t.kind != tkEOF {
		got = "'" + t.text + "'"
	}

	return syntaxErr(t.pos, "Invalid input "+got+": expected "+want)
}

func (p *parser) ident() (string, error) {
	t := p.peek()
	if t.kind != tkIdent {
		return "", p.unexpected("identifier")
	}

	p.pos++

	return t.text, nil
}

func (p *parser) parseClause() (clause, error) {
	t := p.peek()
	if t.kind != tkIdent {
		return nil, p.unexpected("a clause")
	}

	switch strings.ToUpper(t.text) {
	case "MATCH", "OPTIONAL":
		return p.parseMatch()
	case "CREATE":
		p.pos++
		pats, err := p.parsePatterns()

		return createClause{pats: pats}, err
	case "MERGE":
		return p.parseMerge()
	case "SET":
		p.pos++
		items, err := p.parseSetItems()

		return setClause{items: items}, err
	case "REMOVE":
		p.pos++
		items, err := p.parseRemoveItems()

		return removeClause{items: items}, err
	case "DELETE", "DETACH":
		return p.parseDelete()
	case "WITH":
		return p.parseWith()
	case "UNWIND":
		return p.parseUnwind()
	case "RETURN":
		p.pos++
		proj, err := p.parseProjection()

		return returnClause{proj: proj}, err
	case "CALL", "UNION", "FOREACH", "LOAD":
		return nil, unsupported("%s is not supported by this openCypher implementation", strings.ToUpper(t.text))
	default:
		return nil, p.unexpected("a clause")
	}
}

func (p *parser) parseMatch() (clause, error) {
	m := matchClause{}
	if p.accept("OPTIONAL") {
		m.optional = true
	}

	if err := p.expect("MATCH"); err != nil {
		return nil, err
	}

	pats, err := p.parsePatterns()
	if err != nil {
		return nil, err
	}

	m.pats = pats

	if p.accept("WHERE") {
		if m.where, err = p.parseExpr(); err != nil {
			return nil, err
		}
	}

	return m, nil
}

func (p *parser) parseMerge() (clause, error) {
	p.pos++

	pat, err := p.parsePattern()
	if err != nil {
		return nil, err
	}

	m := mergeClause{pat: pat}

	for p.peek().is("ON") {
		p.pos++

		create := p.peek().is("CREATE")
		if !create && !p.peek().is("MATCH") {
			return nil, p.unexpected("CREATE or MATCH")
		}

		p.pos++

		if err = p.expect("SET"); err != nil {
			return nil, err
		}

		items, ierr := p.parseSetItems()
		if ierr != nil {
			return nil, ierr
		}

		if create {
			m.onCreate = append(m.onCreate, items...)
		} else {
			m.onMatch = append(m.onMatch, items...)
		}
	}

	return m, nil
}

func (p *parser) parseDelete() (clause, error) {
	d := deleteClause{}
	if p.accept("DETACH") {
		d.detach = true
	}

	if err := p.expect("DELETE"); err != nil {
		return nil, err
	}

	for {
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}

		d.exprs = append(d.exprs, e)

		if !p.accept(",") {
			return d, nil
		}
	}
}

func (p *parser) parseWith() (clause, error) {
	p.pos++

	proj, err := p.parseProjection()
	if err != nil {
		return nil, err
	}

	w := withClause{proj: proj}

	if p.accept("WHERE") {
		if w.where, err = p.parseExpr(); err != nil {
			return nil, err
		}
	}

	return w, nil
}

func (p *parser) parseUnwind() (clause, error) {
	p.pos++

	e, err := p.parseExpr()
	if err != nil {
		return nil, err
	}

	if err = p.expect("AS"); err != nil {
		return nil, err
	}

	v, err := p.ident()

	return unwindClause{e: e, v: v}, err
}

func (p *parser) parseProjection() (projection, error) {
	var proj projection

	if p.accept("DISTINCT") {
		proj.distinct = true
	}

	if p.accept("*") {
		proj.star = true

		if !p.accept(",") {
			return p.parseProjectionTail(proj)
		}
	}

	for {
		start := p.peek().pos
		e, err := p.parseExpr()
		if err != nil {
			return proj, err
		}

		name := strings.TrimSpace(p.src[start:p.peek().pos])
		if p.accept("AS") {
			if name, err = p.ident(); err != nil {
				return proj, err
			}
		}

		proj.items = append(proj.items, projItem{e: e, name: name})

		if !p.accept(",") {
			break
		}
	}

	return p.parseProjectionTail(proj)
}

func (p *parser) parseProjectionTail(proj projection) (projection, error) {
	var err error

	if p.accept("ORDER") {
		if proj.order, err = p.parseOrderBy(); err != nil {
			return proj, err
		}
	}

	if p.accept("SKIP") {
		if proj.skip, err = p.parseExpr(); err != nil {
			return proj, err
		}
	}

	if p.accept("LIMIT") {
		if proj.limit, err = p.parseExpr(); err != nil {
			return proj, err
		}
	}

	return proj, nil
}

func (p *parser) parseOrderBy() ([]orderItem, error) {
	if err := p.expect("BY"); err != nil {
		return nil, err
	}

	var items []orderItem

	for {
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}

		it := orderItem{e: e}
		if p.accept("DESC") || p.accept("DESCENDING") {
			it.desc = true
		} else if !p.accept("ASC") {
			p.accept("ASCENDING")
		}

		items = append(items, it)

		if !p.accept(",") {
			return items, nil
		}
	}
}

func (p *parser) parseSetItems() ([]setItem, error) {
	var items []setItem

	for {
		it, err := p.parseSetItem()
		if err != nil {
			return nil, err
		}

		items = append(items, it)

		if !p.accept(",") {
			return items, nil
		}
	}
}

func (p *parser) parseSetItem() (setItem, error) {
	if p.peek().kind == tkIdent && p.peek2().is(":") {
		v, _ := p.ident()

		labels, err := p.parseLabels()

		return setItem{v: v, labels: labels, op: ":"}, err
	}

	target, err := p.parsePostfix()
	if err != nil {
		return setItem{}, err
	}

	op := p.peek().text
	if op != "=" && op != "+=" {
		return setItem{}, p.unexpected("'=' or '+='")
	}

	p.pos++

	val, err := p.parseExpr()

	return setItem{target: target, val: val, op: op}, err
}

func (p *parser) parseRemoveItems() ([]setItem, error) {
	var items []setItem

	for {
		if p.peek().kind == tkIdent && p.peek2().is(":") {
			v, _ := p.ident()

			labels, err := p.parseLabels()
			if err != nil {
				return nil, err
			}

			items = append(items, setItem{v: v, labels: labels, op: ":"})
		} else {
			target, err := p.parsePostfix()
			if err != nil {
				return nil, err
			}

			items = append(items, setItem{target: target, op: "remove"})
		}

		if !p.accept(",") {
			return items, nil
		}
	}
}

func (p *parser) parseLabels() ([]string, error) {
	var labels []string

	for p.accept(":") {
		l, err := p.ident()
		if err != nil {
			return nil, err
		}

		labels = append(labels, l)
	}

	return labels, nil
}

func (p *parser) parsePatterns() ([]pattern, error) {
	var pats []pattern

	for {
		pat, err := p.parsePattern()
		if err != nil {
			return nil, err
		}

		pats = append(pats, pat)

		if !p.accept(",") {
			return pats, nil
		}
	}
}

func (p *parser) parsePattern() (pattern, error) {
	var pat pattern

	if p.peek().kind == tkIdent && p.peek2().is("=") {
		pat.pathVar, _ = p.ident()
		p.pos++
	}

	n, err := p.parseNodePat()
	if err != nil {
		return pat, err
	}

	pat.nodes = append(pat.nodes, n)

	for p.peek().is("-") || p.peek().is("<") {
		r, rerr := p.parseRelPat()
		if rerr != nil {
			return pat, rerr
		}

		n, err = p.parseNodePat()
		if err != nil {
			return pat, err
		}

		pat.rels = append(pat.rels, r)
		pat.nodes = append(pat.nodes, n)
	}

	return pat, nil
}

func (p *parser) parseNodePat() (nodePat, error) {
	var n nodePat

	if err := p.expect("("); err != nil {
		return n, err
	}

	if p.peek().kind == tkIdent {
		n.v, _ = p.ident()
	}

	var err error
	if n.labels, err = p.parseLabels(); err != nil {
		return n, err
	}

	if p.peek().is("{") {
		if n.props, err = p.parseMapLiteral(); err != nil {
			return n, err
		}
	}

	return n, p.expect(")")
}

func (p *parser) parseRelPat() (relPat, error) {
	r := relPat{minHops: 1, maxHops: 1}
	left := p.accept("<")

	if err := p.expect("-"); err != nil {
		return r, err
	}

	if p.accept("[") {
		if err := p.parseRelDetail(&r); err != nil {
			return r, err
		}
	}

	if err := p.expect("-"); err != nil {
		return r, err
	}

	right := p.accept(">")

	switch {
	case left && right:
		return r, syntaxErr(p.peek().pos, "a relationship cannot point in both directions")
	case left:
		r.dir = -1
	case right:
		r.dir = 1
	}

	return r, nil
}

func (p *parser) parseRelDetail(r *relPat) error {
	if p.peek().kind == tkIdent {
		r.v, _ = p.ident()
	}

	if p.accept(":") {
		for {
			t, err := p.ident()
			if err != nil {
				return err
			}

			r.types = append(r.types, t)

			if !p.accept("|") {
				break
			}

			p.accept(":")
		}
	}

	if p.accept("*") {
		p.parseHops(r)
	}

	if p.peek().is("{") {
		props, err := p.parseMapLiteral()
		if err != nil {
			return err
		}

		r.props = props
	}

	return p.expect("]")
}

func (p *parser) parseHops(r *relPat) {
	r.varLen, r.minHops, r.maxHops = true, 1, 0
	r.unbounded = true

	if p.peek().kind == tkInt {
		n, _ := strconv.Atoi(p.next().text)
		r.minHops, r.maxHops, r.unbounded = n, n, false
	}

	if p.accept("..") {
		r.unbounded = true

		if p.peek().kind == tkInt {
			r.maxHops, _ = strconv.Atoi(p.next().text)
			r.unbounded = false
		}
	}
}

func (p *parser) parseMapLiteral() (*mapExpr, error) {
	if err := p.expect("{"); err != nil {
		return nil, err
	}

	m := &mapExpr{}

	for !p.peek().is("}") {
		k := p.next()
		if k.kind != tkIdent && k.kind != tkString {
			return nil, syntaxErr(k.pos, "expected map key")
		}

		if err := p.expect(":"); err != nil {
			return nil, err
		}

		v, err := p.parseExpr()
		if err != nil {
			return nil, err
		}

		m.keys, m.vals = append(m.keys, k.text), append(m.vals, v)

		if !p.accept(",") {
			break
		}
	}

	return m, p.expect("}")
}
