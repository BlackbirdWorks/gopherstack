package kql

import (
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"time"
)

type parser struct {
	toks []token
	i    int
}

func parseQuery(src string) (*query, error) {
	toks, err := lex(src)
	if err != nil {
		return nil, err
	}

	p := &parser{toks: toks}
	q := &query{}

	for {
		switch {
		case p.isKw("let"):
			st, perr := p.parseLet()
			if perr != nil {
				return nil, perr
			}

			q.lets = append(q.lets, st)
		case p.isKw("set"):
			for !p.isOp(";") && p.cur().kind != tkEOF {
				p.i++
			}
		default:
			body, perr := p.parseTabular()
			if perr != nil {
				return nil, perr
			}

			q.body = body

			if p.isOp(";") {
				p.i++
			}

			if p.cur().kind != tkEOF {
				return nil, p.unexpected()
			}

			return q, nil
		}

		if err = p.expectOp(";"); err != nil {
			return nil, err
		}
	}
}

func (p *parser) cur() token { return p.toks[p.i] }

func (p *parser) peek(n int) token {
	if p.i+n < len(p.toks) {
		return p.toks[p.i+n]
	}

	return p.toks[len(p.toks)-1]
}

func (p *parser) isOp(s string) bool { t := p.cur(); return t.kind == tkOp && t.text == s }

func (p *parser) isKw(s string) bool {
	t := p.cur()

	return t.kind == tkIdent && strings.EqualFold(t.text, s)
}

func (p *parser) unexpected() error {
	t := p.cur()
	if t.kind == tkEOF {
		return syntaxErr("unexpected end of query")
	}

	if t.kind == tkTyped {
		return syntaxErr("unexpected %s(...) at offset %d", t.text, t.pos)
	}

	return syntaxErr("unexpected %q at offset %d", t.text, t.pos)
}

func (p *parser) expectOp(s string) error {
	if !p.isOp(s) {
		return p.unexpected()
	}

	p.i++

	return nil
}

func (p *parser) expectKw(s string) error {
	if !p.isKw(s) {
		return p.unexpected()
	}

	p.i++

	return nil
}

func (p *parser) parseLet() (letStmt, error) {
	p.i++

	t := p.cur()
	if t.kind != tkIdent {
		return letStmt{}, p.unexpected()
	}

	p.i++

	if err := p.expectOp("="); err != nil {
		return letStmt{}, err
	}

	if p.isOp("(") && p.looksLikeFunction() {
		return letStmt{}, unsupported("user-defined functions in let")
	}

	def, err := p.parseTabular()
	if err != nil {
		return letStmt{}, err
	}

	return letStmt{name: t.text, def: def}, nil
}

func (p *parser) looksLikeFunction() bool {
	depth := 0

	for j := p.i; j < len(p.toks); j++ {
		t := p.toks[j]
		if t.kind == tkOp && t.text == "(" {
			depth++
		}

		if t.kind == tkOp && t.text == ")" {
			depth--

			if depth == 0 {
				n := p.toks[min(j+1, len(p.toks)-1)]

				return n.kind == tkOp && n.text == "{"
			}
		}
	}

	return false
}

func (p *parser) parseTabular() (node, error) {
	src, err := p.parseSource()
	if err != nil {
		return nil, err
	}

	var ops []node

	for p.isOp("|") {
		p.i++

		op, oerr := p.parseOp()
		if oerr != nil {
			return nil, oerr
		}

		ops = append(ops, op)
	}

	if len(ops) == 0 {
		return src, nil
	}

	return &pipeNode{src: src, ops: ops}, nil
}

func (p *parser) parseSource() (node, error) {
	t := p.cur()
	if t.kind != tkIdent {
		return p.parseExpr()
	}

	switch strings.ToLower(t.text) {
	case "union":
		p.i++

		return p.parseUnion()
	case "datatable":
		p.i++

		return p.parseDatatable()
	case "range":
		p.i++

		return p.parseRange()
	case "print":
		p.i++

		items, err := p.parseNamedList()
		if err != nil {
			return nil, err
		}

		return &printNode{items: items}, nil
	case "search", "find", "externaldata", "evaluate", "database", "cluster", "materialize", "toscalar":
		return nil, unsupported("'" + t.text + "'")
	case "workspace":
		if p.peek(1).kind == tkOp && p.peek(1).text == "(" {
			return p.parseWorkspaceRef()
		}
	}

	return p.parseExpr()
}

func (p *parser) parseWorkspaceRef() (node, error) {
	p.i += 2

	ws := p.cur()
	if ws.kind != tkString {
		return nil, p.unexpected()
	}

	p.i++

	if err := p.expectOp(")"); err != nil {
		return nil, err
	}

	if err := p.expectOp("."); err != nil {
		return nil, err
	}

	name, err := p.parseName()
	if err != nil {
		return nil, err
	}

	return &tableRef{name: name, workspace: ws.text}, nil
}

func (p *parser) parseUnion() (node, error) {
	for p.cur().kind == tkIdent && p.peek(1).kind == tkOp && p.peek(1).text == "=" {
		p.i += 3
	}

	var tables []node

	for {
		t, err := p.parseUnionMember()
		if err != nil {
			return nil, err
		}

		tables = append(tables, t)

		if !p.isOp(",") {
			return &unionNode{tables: tables}, nil
		}

		p.i++
	}
}

func (p *parser) parseUnionMember() (node, error) {
	if p.isKw("workspace") && p.peek(1).kind == tkOp && p.peek(1).text == "(" {
		return p.parseWorkspaceRef()
	}

	if p.isOp("(") {
		p.i++

		inner, err := p.parseTabular()
		if err != nil {
			return nil, err
		}

		return inner, p.expectOp(")")
	}

	t := p.cur()
	if t.kind != tkIdent {
		return nil, p.unexpected()
	}

	p.i++

	if p.isOp("*") {
		return nil, unsupported("wildcard union")
	}

	return &identNode{name: t.text}, nil
}

func (p *parser) parseName() (string, error) {
	if p.isOp("[") && p.peek(1).kind == tkString {
		name := p.peek(1).text
		p.i += 2

		return name, p.expectOp("]")
	}

	t := p.cur()
	if t.kind != tkIdent {
		return "", p.unexpected()
	}

	p.i++

	return t.text, nil
}

var typeNames = map[string]Type{ //nolint:gochecknoglobals // static lookup
	"string": TString, "long": TLong, "int": TInt, "real": TReal, "double": TReal, "bool": TBool,
	"boolean": TBool, "datetime": TDatetime, "date": TDatetime, "timespan": TTimespan, "time": TTimespan,
	"dynamic": TDynamic, "guid": TGUID, "decimal": TReal,
}

func (p *parser) parseDatatable() (node, error) {
	if err := p.expectOp("("); err != nil {
		return nil, err
	}

	var cols []Col

	for !p.isOp(")") {
		name, err := p.parseName()
		if err != nil {
			return nil, err
		}

		if err = p.expectOp(":"); err != nil {
			return nil, err
		}

		tn := p.cur()
		if tn.kind != tkIdent && tn.kind != tkTyped {
			return nil, p.unexpected()
		}

		typ, ok := typeNames[strings.ToLower(tn.text)]
		if !ok {
			return nil, syntaxErr("unknown column type %q", tn.text)
		}

		p.i++

		cols = append(cols, Col{Name: name, Type: typ})

		if p.isOp(",") {
			p.i++
		}
	}

	p.i++

	if err := p.expectOp("["); err != nil {
		return nil, err
	}

	var vals []node

	for !p.isOp("]") {
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}

		vals = append(vals, e)

		if p.isOp(",") {
			p.i++
		}
	}

	p.i++

	if len(cols) == 0 || len(vals)%len(cols) != 0 {
		return nil, semErr("datatable values do not fill whole rows")
	}

	return &datatableNode{cols: cols, vals: vals}, nil
}

func (p *parser) parseRange() (node, error) {
	name, err := p.parseName()
	if err != nil {
		return nil, err
	}

	r := &rangeNode{name: name}

	if err = p.expectKw("from"); err != nil {
		return nil, err
	}

	if r.from, err = p.parseAdd(); err != nil {
		return nil, err
	}

	if err = p.expectKw("to"); err != nil {
		return nil, err
	}

	if r.to, err = p.parseAdd(); err != nil {
		return nil, err
	}

	if err = p.expectKw("step"); err != nil {
		return nil, err
	}

	if r.step, err = p.parseAdd(); err != nil {
		return nil, err
	}

	return r, nil
}

func (p *parser) parseNamedList() ([]namedExpr, error) {
	var out []namedExpr

	for {
		ne, err := p.parseNamed()
		if err != nil {
			return nil, err
		}

		out = append(out, ne)

		if !p.isOp(",") {
			return out, nil
		}

		p.i++
	}
}

func (p *parser) parseNamed() (namedExpr, error) {
	name := ""

	if p.cur().kind == tkIdent && p.peek(1).kind == tkOp && p.peek(1).text == "=" {
		name = p.cur().text
		p.i += 2
	} else if p.isOp("[") && p.peek(1).kind == tkString && p.peek(2).kind == tkOp && p.peek(2).text == "]" &&
		p.peek(3).kind == tkOp && p.peek(3).text == "=" {
		name = p.peek(1).text
		p.i += 4
	}

	e, err := p.parseExpr()

	return namedExpr{name: name, e: e}, err
}

func (p *parser) parseNameList() ([]string, error) {
	var out []string

	for {
		n, err := p.parseName()
		if err != nil {
			return nil, err
		}

		if p.isOp("*") {
			n += "*"
			p.i++
		}

		out = append(out, n)

		if !p.isOp(",") {
			return out, nil
		}

		p.i++
	}
}

func (p *parser) parseOp() (node, error) {
	t := p.cur()
	if t.kind != tkIdent {
		return nil, p.unexpected()
	}

	p.i++

	switch strings.ToLower(t.text) {
	case "where", "filter":
		e, err := p.parseExpr()

		return &whereOp{e: e}, err
	case "project":
		items, err := p.parseNamedList()

		return &projectOp{items: items}, err
	case "project-away":
		n, err := p.parseNameList()

		return &projectAwayOp{names: n}, err
	case "project-keep":
		n, err := p.parseNameList()

		return &projectKeepOp{names: n}, err
	case "project-reorder":
		n, err := p.parseNameList()

		return &projectOrderOp{names: n}, err
	case "project-rename":
		items, err := p.parseNamedList()

		return &projectRenOp{pairs: items}, err
	case "extend":
		items, err := p.parseNamedList()

		return &extendOp{items: items}, err
	case "summarize":
		return p.parseSummarize()
	case "sort", "order":
		return p.parseSort()
	case "top":
		return p.parseTop()
	case "take", "limit":
		e, err := p.parseAdd()

		return &takeOp{n: e}, err
	case "distinct":
		return p.parseDistinct()
	case "count":
		op := &countOp{name: "Count"}

		if p.isKw("as") {
			p.i++

			n, err := p.parseName()
			if err != nil {
				return nil, err
			}

			op.name = n
		}

		return op, nil
	case "union":
		u, err := p.parseUnion()
		if err != nil {
			return nil, err
		}

		return &unionOp{tables: u.(*unionNode).tables}, nil //nolint:forcetypeassert // parseUnion returns *unionNode
	case "render":
		return p.parseRender()
	case "getschema":
		return &getschemaOp{}, nil
	}

	return nil, unsupported("the '" + t.text + "' operator")
}

func (p *parser) parseRender() (node, error) {
	p.i++

	if p.isKw("with") {
		p.i++
		depth := 0

		for p.cur().kind != tkEOF {
			if p.isOp("(") {
				depth++
			}

			if p.isOp(")") {
				depth--
			}

			p.i++

			if depth == 0 {
				break
			}
		}
	}

	return &renderOp{}, nil
}

func (p *parser) parseSummarize() (node, error) {
	op := &summarizeOp{}

	if !p.isKw("by") {
		aggs, err := p.parseNamedList()
		if err != nil {
			return nil, err
		}

		op.aggs = aggs
	}

	if p.isKw("by") {
		p.i++

		by, err := p.parseNamedList()
		if err != nil {
			return nil, err
		}

		op.by = by
	}

	return op, nil
}

func (p *parser) parseSortKey(defaultDesc bool) (sortKey, error) {
	e, err := p.parseExpr()
	if err != nil {
		return sortKey{}, err
	}

	k := sortKey{e: e, desc: defaultDesc}

	switch {
	case p.isKw("asc"):
		k.desc = false
		p.i++
	case p.isKw("desc"):
		k.desc = true
		p.i++
	}

	if p.isKw("nulls") {
		p.i++

		last := p.isKw("last")
		if !last && !p.isKw("first") {
			return k, p.unexpected()
		}

		p.i++
		k.nullsLast = &last
	}

	return k, nil
}

func (p *parser) parseSort() (node, error) {
	if err := p.expectKw("by"); err != nil {
		return nil, err
	}

	op := &sortOp{}

	for {
		k, err := p.parseSortKey(true)
		if err != nil {
			return nil, err
		}

		op.keys = append(op.keys, k)

		if !p.isOp(",") {
			return op, nil
		}

		p.i++
	}
}

func (p *parser) parseTop() (node, error) {
	n, err := p.parseAdd()
	if err != nil {
		return nil, err
	}

	if err = p.expectKw("by"); err != nil {
		return nil, err
	}

	k, err := p.parseSortKey(true)

	return &topOp{n: n, key: k}, err
}

func (p *parser) parseDistinct() (node, error) {
	if p.isOp("*") {
		p.i++

		return &distinctOp{all: true}, nil
	}

	items, err := p.parseNamedList()

	return &distinctOp{items: items}, err
}

func (p *parser) parseExpr() (node, error) { return p.parseOr() }

func (p *parser) parseOr() (node, error) {
	l, err := p.parseAnd()
	if err != nil {
		return nil, err
	}

	for p.isKw("or") {
		p.i++

		r, rerr := p.parseAnd()
		if rerr != nil {
			return nil, rerr
		}

		l = &binNode{op: "or", l: l, r: r}
	}

	return l, nil
}

func (p *parser) parseAnd() (node, error) {
	l, err := p.parseRel()
	if err != nil {
		return nil, err
	}

	for p.isKw("and") {
		p.i++

		r, rerr := p.parseRel()
		if rerr != nil {
			return nil, rerr
		}

		l = &binNode{op: "and", l: l, r: r}
	}

	return l, nil
}

var stringOps = map[string]bool{ //nolint:gochecknoglobals // static lookup
	"contains": true, "contains_cs": true, "has": true, "has_cs": true, "startswith": true,
	"startswith_cs": true, "endswith": true, "endswith_cs": true, "hasprefix": true, "hassuffix": true,
	"hasprefix_cs": true, "hassuffix_cs": true,
}

func (p *parser) parseRel() (node, error) {
	l, err := p.parseAdd()
	if err != nil {
		return nil, err
	}

	for {
		t := p.cur()
		neg := false
		off := 0

		if t.kind == tkOp && t.text == "!" && p.peek(1).kind == tkIdent {
			neg = true
			off = 1
			t = p.peek(1)
		}

		switch {
		case t.kind == tkOp && !neg && isCmpOp(t.text):
			p.i++

			r, rerr := p.parseAdd()
			if rerr != nil {
				return nil, rerr
			}

			op := t.text
			if op == "<>" {
				op = "!="
			}

			l = &binNode{op: op, l: l, r: r}
		case t.kind == tkIdent && stringOps[strings.ToLower(t.text)]:
			p.i += 1 + off

			r, rerr := p.parseAdd()
			if rerr != nil {
				return nil, rerr
			}

			op := strings.ToLower(t.text)
			if neg {
				op = "!" + op
			}

			l = &binNode{op: op, l: l, r: r}
		case t.kind == tkIdent && strings.EqualFold(t.text, "matches"):
			p.i += 1 + off

			if err = p.expectKw("regex"); err != nil {
				return nil, err
			}

			r, rerr := p.parseAdd()
			if rerr != nil {
				return nil, rerr
			}

			op := "matches regex"
			if neg {
				op = "!" + op
			}

			l = &binNode{op: op, l: l, r: r}
		case t.kind == tkIdent && (strings.EqualFold(t.text, "in") || strings.EqualFold(t.text, "in~")):
			p.i += 1 + off

			list, lerr := p.parseInList()
			if lerr != nil {
				return nil, lerr
			}

			l = &inNode{x: l, list: list, neg: neg, ci: strings.EqualFold(t.text, "in~")}
		case t.kind == tkIdent && (strings.EqualFold(t.text, "has_any") || strings.EqualFold(t.text, "has_all")):
			p.i += 1 + off

			list, lerr := p.parseInList()
			if lerr != nil {
				return nil, lerr
			}

			l = &callNode{name: "__" + strings.ToLower(t.text), args: append([]node{l}, list...)}
		case t.kind == tkIdent && strings.EqualFold(t.text, "between"):
			p.i += 1 + off

			b, berr := p.parseBetween(l, neg)
			if berr != nil {
				return nil, berr
			}

			l = b
		default:
			return l, nil
		}
	}
}

func isCmpOp(s string) bool {
	switch s {
	case "==", "!=", "<>", "<", "<=", ">", ">=", "=~", "!~":
		return true
	}

	return false
}

func (p *parser) parseInList() ([]node, error) {
	if err := p.expectOp("("); err != nil {
		return nil, err
	}

	var list []node

	for !p.isOp(")") {
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}

		list = append(list, e)

		if p.isOp(",") {
			p.i++
		}
	}

	p.i++

	return list, nil
}

func (p *parser) parseBetween(x node, neg bool) (node, error) {
	if err := p.expectOp("("); err != nil {
		return nil, err
	}

	lo, err := p.parseAdd()
	if err != nil {
		return nil, err
	}

	if err = p.expectOp(".."); err != nil {
		return nil, err
	}

	hi, err := p.parseAdd()
	if err != nil {
		return nil, err
	}

	return &betweenNode{x: x, lo: lo, hi: hi, neg: neg}, p.expectOp(")")
}

func (p *parser) parseAdd() (node, error) {
	l, err := p.parseMul()
	if err != nil {
		return nil, err
	}

	for p.isOp("+") || p.isOp("-") {
		op := p.cur().text
		p.i++

		r, rerr := p.parseMul()
		if rerr != nil {
			return nil, rerr
		}

		l = &binNode{op: op, l: l, r: r}
	}

	return l, nil
}

func (p *parser) parseMul() (node, error) {
	l, err := p.parseUnary()
	if err != nil {
		return nil, err
	}

	for p.isOp("*") || p.isOp("/") || p.isOp("%") {
		op := p.cur().text
		p.i++

		r, rerr := p.parseUnary()
		if rerr != nil {
			return nil, rerr
		}

		l = &binNode{op: op, l: l, r: r}
	}

	return l, nil
}

func (p *parser) parseUnary() (node, error) {
	if p.isOp("-") || p.isOp("+") {
		op := p.cur().text
		p.i++

		x, err := p.parseUnary()
		if err != nil {
			return nil, err
		}

		return &unNode{op: op, x: x}, nil
	}

	return p.parsePostfix()
}

func (p *parser) parsePostfix() (node, error) {
	x, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}

	for {
		switch {
		case p.isOp(".") && p.peek(1).kind == tkIdent:
			x = &memberNode{x: x, name: p.peek(1).text}
			p.i += 2
		case p.isOp("["):
			p.i++

			idx, ierr := p.parseExpr()
			if ierr != nil {
				return nil, ierr
			}

			if err = p.expectOp("]"); err != nil {
				return nil, err
			}

			x = &indexNode{x: x, idx: idx}
		default:
			return x, nil
		}
	}
}

func (p *parser) parsePrimary() (node, error) {
	t := p.cur()

	switch t.kind {
	case tkInt:
		p.i++

		n, err := strconv.ParseInt(t.text, 10, 64)
		if err != nil {
			f, _ := strconv.ParseFloat(t.text, 64)

			return &litNode{v: f, t: TReal}, nil
		}

		return &litNode{v: n, t: TLong}, nil
	case tkReal:
		p.i++
		f, _ := strconv.ParseFloat(t.text, 64)

		return &litNode{v: f, t: TReal}, nil
	case tkString:
		p.i++

		return &litNode{v: t.text, t: TString}, nil
	case tkTimespan:
		p.i++
		ns, _ := strconv.ParseInt(t.text, 10, 64)

		return &litNode{v: time.Duration(ns), t: TTimespan}, nil
	case tkTyped:
		p.i++

		return typedLiteral(t)
	case tkOp:
		return p.parsePrimaryOp(t)
	case tkIdent:
		return p.parseIdentPrimary(t)
	case tkEOF:
	}

	return nil, p.unexpected()
}

func (p *parser) parsePrimaryOp(t token) (node, error) {
	switch t.text {
	case "(":
		p.i++

		inner, err := p.parseTabular()
		if err != nil {
			return nil, err
		}

		return inner, p.expectOp(")")
	case "[":
		if p.peek(1).kind == tkString && p.peek(2).kind == tkOp && p.peek(2).text == "]" {
			name := p.peek(1).text
			p.i += 3

			return &identNode{name: name}, nil
		}
	}

	return nil, p.unexpected()
}

func (p *parser) parseIdentPrimary(t token) (node, error) {
	p.i++

	lower := strings.ToLower(t.text)

	if p.isOp("(") {
		return p.parseCall(t.text)
	}

	switch lower {
	case "true":
		return &litNode{v: true, t: TBool}, nil
	case "false":
		return &litNode{v: false, t: TBool}, nil
	case "null":
		return &litNode{v: nil, t: TDynamic}, nil
	}

	return &identNode{name: t.text}, nil
}

func (p *parser) parseCall(name string) (node, error) {
	p.i++

	c := &callNode{name: name}

	for !p.isOp(")") {
		var a node

		if next := p.peek(1); p.isOp("*") && next.kind == tkOp && (next.text == "," || next.text == ")") {
			p.i++

			a = &identNode{name: "*"}
		} else {
			var err error
			if a, err = p.parseExpr(); err != nil {
				return nil, err
			}
		}

		c.args = append(c.args, a)

		if p.isOp(",") {
			p.i++
		} else if !p.isOp(")") {
			return nil, p.unexpected()
		}
	}

	p.i++

	return c, nil
}

var datetimeLayouts = []string{ //nolint:gochecknoglobals // static lookup
	time.RFC3339Nano, "2006-01-02T15:04:05.9999999", "2006-01-02T15:04:05", "2006-01-02 15:04:05.9999999",
	"2006-01-02 15:04:05", "2006-01-02 15:04", "2006-01-02T15:04", "2006-01-02", "2006-1-2 15:04:05", "2006-1-2",
	"2006-01-02T15:04:05Z07:00", "Jan 2 2006", "Jan 02, 2006", "2 Jan 2006",
}

// ParseDatetime parses the datetime formats KQL literals and ingested JSON accept.
func ParseDatetime(s string) (time.Time, bool) {
	s = strings.TrimSpace(s)
	for _, layout := range datetimeLayouts {
		if t, err := time.ParseInLocation(layout, s, time.UTC); err == nil {
			return t.UTC(), true
		}
	}

	return time.Time{}, false
}

// ParseTimespan parses KQL timespan literal bodies such as 1d, 00:00:05 or 1.02:03:04.
func ParseTimespan(s string) (time.Duration, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, false
	}

	for i := range len(s) {
		if c := s[i]; (c < '0' || c > '9') && c != '.' && c != '-' {
			if u, ok := timeUnits[strings.ToLower(s[i:])]; ok && i > 0 {
				f, err := strconv.ParseFloat(s[:i], 64)

				return time.Duration(f * float64(u)), err == nil
			}

			break
		}
	}

	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")

	var days int64

	if before, after, ok := strings.Cut(s, "."); ok && strings.Contains(after, ":") {
		d, err := strconv.ParseInt(before, 10, 64)
		if err != nil {
			return 0, false
		}

		days, s = d, after
	}

	parts := strings.Split(s, ":")
	if len(parts) < 2 || len(parts) > 3 {
		return 0, false
	}

	var d time.Duration

	d += time.Duration(days) * 24 * time.Hour

	h, err1 := strconv.ParseInt(parts[0], 10, 64)
	m, err2 := strconv.ParseInt(parts[1], 10, 64)

	if err1 != nil || err2 != nil {
		return 0, false
	}

	d += time.Duration(h)*time.Hour + time.Duration(m)*time.Minute

	if len(parts) == 3 {
		sec, err := strconv.ParseFloat(parts[2], 64)
		if err != nil {
			return 0, false
		}

		d += time.Duration(sec * float64(time.Second))
	}

	if neg {
		d = -d
	}

	return d, true
}

func typedLiteral(t token) (node, error) {
	raw := strings.TrimSpace(t.raw)
	isNull := strings.EqualFold(raw, "null") || raw == ""

	switch t.text {
	case "datetime":
		if isNull {
			return &litNode{t: TDatetime}, nil
		}

		if v, ok := ParseDatetime(raw); ok {
			return &litNode{v: v, t: TDatetime}, nil
		}
	case "timespan", "time":
		if isNull {
			return &litNode{t: TTimespan}, nil
		}

		if v, ok := ParseTimespan(raw); ok {
			return &litNode{v: v, t: TTimespan}, nil
		}
	case "guid":
		return &litNode{v: strings.Trim(raw, `"'`), t: TGUID}, nil
	case "bool", "boolean":
		if isNull {
			return &litNode{t: TBool}, nil
		}

		if b, err := strconv.ParseBool(raw); err == nil {
			return &litNode{v: b, t: TBool}, nil
		}
	case "long", "int":
		if isNull {
			return &litNode{t: TLong}, nil
		}

		if n, err := strconv.ParseInt(raw, 10, 64); err == nil {
			return &litNode{v: n, t: Type(t.text)}, nil
		}
	case "real", "double", "decimal":
		if isNull {
			return &litNode{t: TReal}, nil
		}

		if f, err := strconv.ParseFloat(raw, 64); err == nil && !math.IsNaN(f) {
			return &litNode{v: f, t: TReal}, nil
		}
	case "dynamic":
		return dynamicLiteral(raw)
	}

	return nil, syntaxErr("invalid %s literal %q", t.text, raw)
}

func dynamicLiteral(raw string) (node, error) {
	if strings.EqualFold(raw, "null") || raw == "" {
		return &litNode{t: TDynamic}, nil
	}

	var v any

	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		alt := strings.ReplaceAll(raw, "'", `"`)
		if err2 := json.Unmarshal([]byte(alt), &v); err2 != nil {
			return nil, syntaxErr("invalid dynamic literal")
		}
	}

	return &litNode{v: NormalizeJSON(v), t: TDynamic}, nil
}
