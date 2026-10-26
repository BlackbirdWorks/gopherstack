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

	for p.isKw("let") || p.isKw("set") {
		if err = p.parseStatement(q); err != nil {
			return nil, err
		}

		if err = p.expectOp(";"); err != nil {
			return nil, err
		}
	}

	if q.body, err = p.parseTabular(); err != nil {
		return nil, err
	}

	if p.isOp(";") {
		p.i++
	}

	if p.cur().kind != tkEOF {
		return nil, p.unexpected()
	}

	return q, nil
}

// parseStatement parses one let or set statement (without its trailing semicolon).
func (p *parser) parseStatement(q *query) error {
	if p.isKw("set") {
		for !p.isOp(";") && p.cur().kind != tkEOF {
			p.i++
		}

		return nil
	}

	st, err := p.parseLet()
	if err != nil {
		return err
	}

	q.lets = append(q.lets, st)

	return nil
}

func (p *parser) cur() token { return p.toks[p.i] }

func (p *parser) peek(n int) token {
	if p.i+n < len(p.toks) {
		return p.toks[p.i+n]
	}

	return p.toks[len(p.toks)-1]
}

func (p *parser) opAt(n int, s string) bool {
	t := p.peek(n)

	return t.kind == tkOp && t.text == s
}

func (p *parser) isOp(s string) bool {
	t := p.cur()

	return t.kind == tkOp && t.text == s
}

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
	for p.cur().kind == tkIdent && p.opAt(1, "=") {
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
	"string": TString, string(TLong): TLong, string(TInt): TInt, string(TReal): TReal, kwDouble: TReal, kwBool: TBool,
	kwBoolean: TBool, kwDatetime: TDatetime, "date": TDatetime, string(TTimespan): TTimespan, kwTime: TTimespan,
	kwDynamic: TDynamic, "guid": TGUID, kwDecimal: TReal,
}

func (p *parser) parseDatatableCols() ([]Col, error) {
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

	return cols, nil
}

func (p *parser) parseDatatable() (node, error) {
	cols, err := p.parseDatatableCols()
	if err != nil {
		return nil, err
	}

	if err = p.expectOp("["); err != nil {
		return nil, err
	}

	var vals []node

	for !p.isOp("]") {
		e, perr := p.parseExpr()
		if perr != nil {
			return nil, perr
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

	if p.cur().kind == tkIdent && p.opAt(1, "=") {
		name = p.cur().text
		p.i += 2
	} else if p.isOp("[") && p.peek(1).kind == tkString && p.opAt(peekThird, "]") && p.opAt(peekFourth, "=") {
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

//nolint:gochecknoglobals // static lookup
var opAliases = map[string]string{
	"filter": "where",
	"order":  "sort",
	"limit":  "take",
}

func (p *parser) parseOp() (node, error) {
	t := p.cur()
	if t.kind != tkIdent {
		return nil, p.unexpected()
	}

	p.i++

	kw := strings.ToLower(t.text)
	if canon, ok := opAliases[kw]; ok {
		kw = canon
	}

	if op, handled, err := p.parseRowOp(kw); handled {
		return op, err
	}

	if op, handled, err := p.parseShapeOp(kw); handled {
		return op, err
	}

	return nil, unsupported("the '" + t.text + "' operator")
}

// parseRowOp handles where, project* and extend.
func (p *parser) parseRowOp(kw string) (node, bool, error) {
	switch kw {
	case "where":
		e, err := p.parseExpr()

		return &whereOp{e: e}, true, err
	case "project":
		items, err := p.parseNamedList()

		return &projectOp{items: items}, true, err
	case "project-away":
		n, err := p.parseNameList()

		return &projectAwayOp{names: n}, true, err
	case "project-keep":
		n, err := p.parseNameList()

		return &projectKeepOp{names: n}, true, err
	case "project-reorder":
		n, err := p.parseNameList()

		return &projectOrderOp{names: n}, true, err
	case "project-rename":
		items, err := p.parseNamedList()

		return &projectRenOp{pairs: items}, true, err
	case "extend":
		items, err := p.parseNamedList()

		return &extendOp{items: items}, true, err
	}

	return nil, false, nil
}

// parseShapeOp handles the operators that reshape or reorder the whole table.
func (p *parser) parseShapeOp(kw string) (node, bool, error) {
	var (
		op  node
		err error
	)

	switch kw {
	case "summarize":
		op, err = p.parseSummarize()
	case "sort":
		op, err = p.parseSort()
	case "top":
		op, err = p.parseTop()
	case "take":
		var e node
		if e, err = p.parseAdd(); err == nil {
			op = &takeOp{n: e}
		}
	case "distinct":
		op, err = p.parseDistinct()
	case kwCount:
		op, err = p.parseCountOp()
	case "union":
		op, err = p.parseUnionOp()
	case "render":
		op = p.parseRender()
	case "getschema":
		op = &getschemaOp{}
	default:
		return nil, false, nil
	}

	return op, true, err
}

func (p *parser) parseCountOp() (node, error) {
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
}

func (p *parser) parseUnionOp() (node, error) {
	u, err := p.parseUnion()
	if err != nil {
		return nil, err
	}

	un, ok := u.(*unionNode)
	if !ok {
		return nil, p.unexpected()
	}

	return &unionOp{tables: un.tables}, nil
}

func (p *parser) parseRender() node {
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

	return &renderOp{}
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

	for p.isKw(opOr) {
		p.i++

		r, rerr := p.parseAnd()
		if rerr != nil {
			return nil, rerr
		}

		l = &binNode{op: opOr, l: l, r: r}
	}

	return l, nil
}

func (p *parser) parseAnd() (node, error) {
	l, err := p.parseRel()
	if err != nil {
		return nil, err
	}

	for p.isKw(opAnd) {
		p.i++

		r, rerr := p.parseRel()
		if rerr != nil {
			return nil, rerr
		}

		l = &binNode{op: opAnd, l: l, r: r}
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
		next, matched, stepErr := p.parseRelStep(l)
		if stepErr != nil {
			return nil, stepErr
		}

		if !matched {
			return l, nil
		}

		l = next
	}
}

func (p *parser) parseRelStep(l node) (node, bool, error) {
	t := p.cur()
	neg := false
	off := 0

	if t.kind == tkOp && t.text == "!" && p.peek(1).kind == tkIdent {
		neg, off, t = true, 1, p.peek(1)
	}

	switch {
	case t.kind == tkOp && !neg && isCmpOp(t.text):
		p.i++

		r, err := p.parseAdd()
		if err != nil {
			return nil, false, err
		}

		op := t.text
		if op == "<>" {
			op = "!="
		}

		return &binNode{op: op, l: l, r: r}, true, nil
	case t.kind == tkIdent:
		p.i += 1 + off

		return p.parseWordRel(l, strings.ToLower(t.text), neg)
	}

	return nil, false, nil
}

// parseWordRel parses the keyword relations (contains, has, in, between, ...); the keyword is already consumed.
func (p *parser) parseWordRel(l node, word string, neg bool) (node, bool, error) {
	unmatched := func() (node, bool, error) {
		p.i--
		if neg {
			p.i--
		}

		return nil, false, nil
	}

	switch {
	case stringOps[word]:
		return p.finishBinary(l, negate(word, neg))
	case word == "matches":
		if err := p.expectKw("regex"); err != nil {
			return nil, false, err
		}

		return p.finishBinary(l, negate(opMatchesRegex, neg))
	case word == "in" || word == kwInTilde:
		list, err := p.parseInList()

		return &inNode{x: l, list: list, neg: neg, ci: word == kwInTilde}, err == nil, err
	case word == "has_any" || word == "has_all":
		list, err := p.parseInList()

		return &callNode{name: "__" + word, args: append([]node{l}, list...)}, err == nil, err
	case word == "between":
		b, err := p.parseBetween(l, neg)

		return b, err == nil, err
	}

	return unmatched()
}

func negate(op string, neg bool) string {
	if neg {
		return "!" + op
	}

	return op
}

func (p *parser) finishBinary(l node, op string) (node, bool, error) {
	r, err := p.parseAdd()
	if err != nil {
		return nil, false, err
	}

	return &binNode{op: op, l: l, r: r}, true, nil
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

		if n, err := strconv.ParseInt(t.text, 10, 64); err == nil {
			return &litNode{v: n, t: TLong}, nil
		}

		f, _ := strconv.ParseFloat(t.text, 64)

		return &litNode{v: f, t: TReal}, nil
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
		if p.peek(1).kind == tkString && p.peek(peekThird).kind == tkOp && p.peek(peekThird).text == "]" {
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

	if d, ok, matched := parseUnitTimespan(s); matched {
		return d, ok
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

	d, ok := parseClock(s)
	if !ok {
		return 0, false
	}

	d += time.Duration(days) * dayDuration

	if neg {
		d = -d
	}

	return d, true
}

// parseUnitTimespan parses "<number><unit>" forms such as 1d or 1.5h; matched is false for other shapes.
func parseUnitTimespan(s string) (time.Duration, bool, bool) {
	i := strings.IndexFunc(s, func(r rune) bool { return (r < '0' || r > '9') && r != '.' && r != '-' })
	if i <= 0 {
		return 0, false, false
	}

	u, ok := timeUnits[strings.ToLower(s[i:])]
	if !ok {
		return 0, false, false
	}

	f, err := strconv.ParseFloat(s[:i], 64)

	return time.Duration(f * float64(u)), err == nil, true
}

const (
	clockMinParts = 2
	clockMaxParts = 3
)

// parseClock parses hh:mm[:ss[.fffffff]].
func parseClock(s string) (time.Duration, bool) {
	parts := strings.Split(s, ":")
	if len(parts) < clockMinParts || len(parts) > clockMaxParts {
		return 0, false
	}

	h, err1 := strconv.ParseInt(parts[0], 10, 64)
	m, err2 := strconv.ParseInt(parts[1], 10, 64)

	if err1 != nil || err2 != nil {
		return 0, false
	}

	d := time.Duration(h)*time.Hour + time.Duration(m)*time.Minute

	if len(parts) == clockMaxParts {
		sec, err := strconv.ParseFloat(parts[2], 64)
		if err != nil {
			return 0, false
		}

		d += time.Duration(sec * float64(time.Second))
	}

	return d, true
}

func isNullBody(raw string) bool { return strings.EqualFold(raw, "null") || raw == "" }

func typedLiteral(t token) (node, error) {
	raw := strings.TrimSpace(t.raw)

	var (
		v    any
		typ  Type
		ok   bool
		null = isNullBody(raw)
	)

	switch t.text {
	case kwDatetime:
		typ = TDatetime
		v, ok = parseTyped(null, raw, ParseDatetime)
	case string(TTimespan), kwTime:
		typ = TTimespan
		v, ok = parseTyped(null, raw, ParseTimespan)
	case kwGUID:
		return &litNode{v: strings.Trim(raw, `"'`), t: TGUID}, nil
	case kwBool, kwBoolean:
		typ = TBool
		v, ok = parseTyped(null, raw, parseBoolLit)
	case string(TLong), string(TInt):
		typ = Type(t.text)
		v, ok = parseTyped(null, raw, parseIntLit)
	case string(TReal), kwDouble, kwDecimal:
		typ = TReal
		v, ok = parseTyped(null, raw, parseRealLit)
	case kwDynamic:
		return dynamicLiteral(raw)
	}

	if !ok {
		return nil, syntaxErr("invalid %s literal %q", t.text, raw)
	}

	return &litNode{v: v, t: typ}, nil
}

// parseTyped parses a typed literal body; a null body yields a typed null.
func parseTyped[T any](null bool, raw string, parse func(string) (T, bool)) (any, bool) {
	if null {
		return nil, true
	}

	v, ok := parse(raw)

	return v, ok
}

func parseBoolLit(s string) (bool, bool) {
	b, err := strconv.ParseBool(s)

	return b, err == nil
}

func parseIntLit(s string) (int64, bool) {
	n, err := strconv.ParseInt(s, 10, 64)

	return n, err == nil
}

func parseRealLit(s string) (float64, bool) {
	f, err := strconv.ParseFloat(s, 64)

	return f, err == nil && !math.IsNaN(f)
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
