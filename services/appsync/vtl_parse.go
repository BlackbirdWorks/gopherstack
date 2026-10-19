package appsync

import (
	"fmt"
	"strconv"
	"strings"
)

type vnode any

type (
	vtext    string
	vrefNode struct {
		ref   *vref
		raw   string
		quiet bool
	}
	vsetNode struct {
		target *vref
		val    vexpr
	}
	vifNode struct {
		conds  []vexpr
		bodies [][]vnode
		els    []vnode
	}
	vforeachNode struct {
		iter vexpr
		name string
		body []vnode
	}
	vreturnNode struct{ val vexpr }
	vbreakNode  struct{}
	vstopNode   struct{}
)

type vexpr any

type (
	vlit    struct{ v any }
	vinterp struct{ parts []vnode }
	vlistEx struct{ items []vexpr }
	vrange  struct{ lo, hi vexpr }
	vmapEx  struct{ keys, vals []vexpr }
	vbin    struct {
		l, r vexpr
		op   string
	}
	vunary struct {
		x  vexpr
		op string
	}
)

const (
	accProp = iota
	accMethod
	accIndex
)

type vacc struct {
	idx  vexpr
	name string
	args []vexpr
	kind int
}

type vref struct {
	name  string
	chain []vacc
}

type vtlParser struct {
	src      string
	pos      int
	refsOnly bool
	ls       bool
}

func parseVTL(src string) ([]vnode, error) {
	p := &vtlParser{src: src}

	nodes, term, err := p.parseBlock()
	if err != nil {
		return nil, err
	}

	if term != "" {
		return nil, fmt.Errorf("%w: unexpected #%s", ErrValidation, term)
	}

	return nodes, nil
}

func (p *vtlParser) eof() bool { return p.pos >= len(p.src) }

func (p *vtlParser) peek() byte {
	if p.eof() {
		return 0
	}

	return p.src[p.pos]
}

func (p *vtlParser) hasPrefix(s string) bool { return strings.HasPrefix(p.src[p.pos:], s) }

func isIdentStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

func isIdentChar(c byte) bool { return isIdentStart(c) || isDigit(c) }

func (p *vtlParser) ident() string {
	start := p.pos
	for !p.eof() && isIdentChar(p.src[p.pos]) {
		p.pos++
	}

	return p.src[start:p.pos]
}

func (p *vtlParser) skipWS() {
	for !p.eof() && strings.IndexByte(" \t\r\n", p.src[p.pos]) >= 0 {
		p.pos++
	}
}

const (
	dirEnd    = "end"
	dirElse   = "else"
	dirElseif = "elseif"
	dirSet    = "set"
	dirIf     = "if"
	dirEach   = "foreach"
	dirReturn = "return"
	dirBreak  = "break"
	dirStop   = "stop"
)

type blockState struct {
	buf   strings.Builder
	nodes []vnode
}

func (st *blockState) flush() {
	if st.buf.Len() > 0 {
		st.nodes = append(st.nodes, vtext(st.buf.String()))
		st.buf.Reset()
	}
}

func (st *blockState) add(n vnode) {
	st.flush()
	st.nodes = append(st.nodes, n)
}

// parseBlock reads nodes until EOF or an #end/#else/#elseif directive,
// returning that directive's name ("" at EOF).
func (p *vtlParser) parseBlock() ([]vnode, string, error) {
	st := &blockState{}

	for !p.eof() {
		term, err := p.parseStep(st)
		if err != nil {
			return nil, "", err
		}

		if term != "" {
			st.flush()

			return st.nodes, term, nil
		}
	}

	st.flush()

	return st.nodes, "", nil
}

func (p *vtlParser) parseStep(st *blockState) (string, error) {
	c := p.src[p.pos]

	switch {
	case c == '$':
		if n, ok := p.parseRefNode(); ok {
			st.add(n)

			return "", nil
		}
	case c == '#' && !p.refsOnly:
		return p.stepDirective(st)
	}

	st.buf.WriteByte(c)
	p.pos++

	return "", nil
}

func (p *vtlParser) stepDirective(st *blockState) (string, error) {
	res, err := p.parseDirective(&st.buf)
	if err != nil {
		return "", err
	}

	switch {
	case res.term != "":
		return res.term, nil
	case !res.handled:
		st.buf.WriteByte('#')
		p.pos++
	case res.node != nil:
		st.add(res.node)
	default:
		st.flush()
	}

	return "", nil
}

func (p *vtlParser) parseRefNode() (vnode, bool) {
	start := p.pos
	p.pos++

	quiet := false
	if p.peek() == '!' {
		quiet = true
		p.pos++
	}

	ref, ok := p.parseRefBody()
	if !ok {
		p.pos = start

		return nil, false
	}

	return &vrefNode{ref: ref, quiet: quiet, raw: p.src[start:p.pos]}, true
}

// parseRefBody parses what follows '$' (or '$!'): name or {name...}, plus chain.
func (p *vtlParser) parseRefBody() (*vref, bool) {
	braced := false
	if p.peek() == '{' {
		braced = true
		p.pos++
	}

	if !isIdentStart(p.peek()) {
		return nil, false
	}

	ref := &vref{name: p.ident()}

	for {
		acc, ok := p.parseAccessor()
		if !ok {
			break
		}

		ref.chain = append(ref.chain, acc)
	}

	if braced {
		if p.peek() != '}' {
			return nil, false
		}

		p.pos++
	}

	return ref, true
}

func (p *vtlParser) parseAccessor() (vacc, bool) {
	switch p.peek() {
	case '.':
		if p.pos+1 >= len(p.src) || !isIdentStart(p.src[p.pos+1]) {
			return vacc{}, false
		}

		p.pos++
		name := p.ident()

		if p.peek() == '(' {
			args, err := p.parseArgs()
			if err != nil {
				return vacc{}, false
			}

			return vacc{kind: accMethod, name: name, args: args}, true
		}

		return vacc{kind: accProp, name: name}, true
	case '[':
		save := p.pos
		p.pos++

		idx, err := p.parseExpr()
		if err != nil {
			p.pos = save

			return vacc{}, false
		}

		p.skipWS()

		if p.peek() != ']' {
			p.pos = save

			return vacc{}, false
		}

		p.pos++

		return vacc{kind: accIndex, idx: idx}, true
	}

	return vacc{}, false
}

func (p *vtlParser) parseArgs() ([]vexpr, error) {
	p.pos++ // '('
	p.skipWS()

	var args []vexpr

	if p.peek() == ')' {
		p.pos++

		return args, nil
	}

	for {
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}

		args = append(args, e)
		p.skipWS()

		switch p.peek() {
		case ',':
			p.pos++
		case ')':
			p.pos++

			return args, nil
		default:
			return nil, fmt.Errorf("%w: expected ',' or ')' in argument list", ErrValidation)
		}
	}
}

// lineStartBefore reports whether only blanks precede pos on its line.
func (p *vtlParser) lineStartBefore(pos int) bool {
	for i := pos - 1; i >= 0; i-- {
		switch p.src[i] {
		case '\n':
			return true
		case ' ', '\t':
		default:
			return false
		}
	}

	return true
}

// swallowLine drops the rest of the line after a directive when it is blank.
func (p *vtlParser) swallowLine() {
	i := p.pos
	for i < len(p.src) && (p.src[i] == ' ' || p.src[i] == '\t' || p.src[i] == '\r') {
		i++
	}

	if i < len(p.src) && p.src[i] == '\n' {
		p.pos = i + 1
	} else if i >= len(p.src) {
		p.pos = i
	}
}

func trimLineIndent(buf *strings.Builder) {
	s := buf.String()
	t := strings.TrimRight(s, " \t")

	if len(t) != len(s) && (t == "" || strings.HasSuffix(t, "\n")) {
		buf.Reset()
		buf.WriteString(t)
	}
}

type dirResult struct {
	node    vnode
	term    string
	handled bool
}

// parseDirective handles a '#' at pos. term is set for #end/#else/#elseif;
// handled is false when the '#' is plain text.
func (p *vtlParser) parseDirective(buf *strings.Builder) (dirResult, error) {
	start := p.pos

	switch {
	case p.hasPrefix("##"):
		p.skipLineComment(buf, start)

		return dirResult{handled: true}, nil
	case p.hasPrefix("#*"):
		end := strings.Index(p.src[p.pos:], "*#")
		if end < 0 {
			return dirResult{}, fmt.Errorf("%w: unterminated #* comment", ErrValidation)
		}

		p.pos += end + len("*#")
		p.swallowAfter(buf, start)

		return dirResult{handled: true}, nil
	case p.hasPrefix("#[["):
		end := strings.Index(p.src[p.pos:], "]]#")
		if end < 0 {
			return dirResult{}, fmt.Errorf("%w: unterminated #[[ literal", ErrValidation)
		}

		buf.WriteString(p.src[p.pos+len("#[[") : p.pos+end])
		p.pos += end + len("]]#")

		return dirResult{handled: true}, nil
	}

	name, ok := p.directiveName()
	if !ok || !p.isDirective(name) {
		p.pos = start

		return dirResult{}, nil
	}

	p.ls = p.lineStartBefore(start)
	if p.ls {
		trimLineIndent(buf)
	}

	n, term, err := p.directiveBody(name)
	if err != nil {
		return dirResult{}, err
	}

	return dirResult{node: n, term: term, handled: true}, nil
}

// directiveName consumes '#' plus the (optionally braced) directive name.
func (p *vtlParser) directiveName() (string, bool) {
	p.pos++

	braced := p.peek() == '{'
	if braced {
		p.pos++
	}

	name := p.ident()
	if braced {
		if p.peek() != '}' {
			return "", false
		}

		p.pos++
	}

	return name, name != ""
}

// isDirective reports whether name (already consumed) starts a real directive here.
func (p *vtlParser) isDirective(name string) bool {
	switch name {
	case dirEnd, dirElse, dirElseif, dirBreak, dirStop, dirReturn:
		return true
	case dirSet, dirIf, dirEach:
		save := p.pos

		p.skipWS()

		ok := p.peek() == '('
		p.pos = save

		return ok
	}

	return false
}

func (p *vtlParser) swallowIfLS() {
	if p.ls {
		p.swallowLine()
	}
}

func (p *vtlParser) skipLineComment(buf *strings.Builder, start int) {
	atLineStart := p.lineStartBefore(start)
	if atLineStart {
		trimLineIndent(buf)
	}

	for !p.eof() && p.src[p.pos] != '\n' {
		p.pos++
	}

	if atLineStart && !p.eof() {
		p.pos++
	}
}

func (p *vtlParser) swallowAfter(buf *strings.Builder, start int) {
	if p.lineStartBefore(start) {
		trimLineIndent(buf)
		p.swallowLine()
	}
}

func (p *vtlParser) directiveBody(name string) (vnode, string, error) {
	switch name {
	case dirEnd, dirElse:
		p.swallowIfLS()

		return nil, name, nil
	case dirElseif:
		return nil, name, nil
	case dirBreak:
		p.swallowIfLS()

		return &vbreakNode{}, "", nil
	case dirStop:
		p.swallowIfLS()

		return &vstopNode{}, "", nil
	}

	save := p.pos

	p.skipWS()

	if p.peek() != '(' {
		p.pos = save
		p.swallowIfLS()

		return &vreturnNode{}, "", nil
	}

	n, err := p.parseParenDirective(name)

	return n, "", err
}

func (p *vtlParser) parseParenDirective(name string) (vnode, error) {
	p.pos++ // '('
	p.skipWS()

	switch name {
	case dirSet:
		return p.parseSet()
	case dirIf:
		return p.parseIf()
	case dirEach:
		return p.parseForeach()
	default:
		return p.parseReturn()
	}
}

func (p *vtlParser) expectClose() error {
	p.skipWS()

	if p.peek() != ')' {
		return fmt.Errorf("%w: expected ')' at offset %d", ErrValidation, p.pos)
	}

	p.pos++

	return nil
}

func (p *vtlParser) parseReturn() (vnode, error) {
	p.skipWS()

	if p.peek() == ')' {
		p.pos++
		p.swallowIfLS()

		return &vreturnNode{}, nil
	}

	e, err := p.parseExpr()
	if err != nil {
		return nil, err
	}

	if err = p.expectClose(); err != nil {
		return nil, err
	}

	p.swallowIfLS()

	return &vreturnNode{val: e}, nil
}

func (p *vtlParser) parseSet() (vnode, error) {
	if p.peek() != '$' {
		return nil, fmt.Errorf("%w: #set target must be a reference", ErrValidation)
	}

	p.pos++
	if p.peek() == '!' {
		p.pos++
	}

	ref, ok := p.parseRefBody()
	if !ok {
		return nil, fmt.Errorf("%w: invalid #set target", ErrValidation)
	}

	p.skipWS()

	if p.peek() != '=' {
		return nil, fmt.Errorf("%w: expected '=' in #set", ErrValidation)
	}

	p.pos++

	val, err := p.parseExpr()
	if err != nil {
		return nil, err
	}

	if err = p.expectClose(); err != nil {
		return nil, err
	}

	p.swallowIfLS()

	return &vsetNode{target: ref, val: val}, nil
}

func (p *vtlParser) parseCond() (vexpr, error) {
	cond, err := p.parseExpr()
	if err != nil {
		return nil, err
	}

	if err = p.expectClose(); err != nil {
		return nil, err
	}

	p.swallowIfLS()

	return cond, nil
}

func (p *vtlParser) parseElseifCond() (vexpr, error) {
	p.skipWS()

	if p.peek() != '(' {
		return nil, fmt.Errorf("%w: #elseif requires a condition", ErrValidation)
	}

	p.pos++

	return p.parseCond()
}

func (p *vtlParser) parseIf() (vnode, error) {
	n := &vifNode{}

	cond, err := p.parseCond()
	if err != nil {
		return nil, err
	}

	for {
		body, term, berr := p.parseBlock()
		if berr != nil {
			return nil, berr
		}

		n.conds = append(n.conds, cond)
		n.bodies = append(n.bodies, body)

		switch term {
		case dirEnd:
			return n, nil
		case dirElse:
			return p.finishElse(n)
		case dirElseif:
			if cond, err = p.parseElseifCond(); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("%w: #if without #end", ErrValidation)
		}
	}
}

func (p *vtlParser) finishElse(n *vifNode) (vnode, error) {
	els, term, err := p.parseBlock()
	if err != nil {
		return nil, err
	}

	if term != dirEnd {
		return nil, fmt.Errorf("%w: #if without #end", ErrValidation)
	}

	n.els = els

	return n, nil
}

func (p *vtlParser) parseForeach() (vnode, error) {
	if p.peek() != '$' {
		return nil, fmt.Errorf("%w: #foreach variable must be a reference", ErrValidation)
	}

	p.pos++

	name := p.ident()

	p.skipWS()

	if !p.hasPrefix("in") {
		return nil, fmt.Errorf("%w: expected 'in' in #foreach", ErrValidation)
	}

	p.pos += len("in")

	iter, err := p.parseExpr()
	if err != nil {
		return nil, err
	}

	if err = p.expectClose(); err != nil {
		return nil, err
	}

	p.swallowIfLS()

	body, term, err := p.parseBlock()
	if err != nil {
		return nil, err
	}

	if term != "end" {
		return nil, fmt.Errorf("%w: #foreach without #end", ErrValidation)
	}

	return &vforeachNode{name: name, iter: iter, body: body}, nil
}

// parseExpr parses a full expression (lowest precedence: ||).
func (p *vtlParser) parseExpr() (vexpr, error) { return p.parseBinary(0) }

func vtlBinLevels() [][]string {
	return [][]string{{"||"}, {"&&"}, {"==", "!="}, {"<=", ">=", "<", ">"}, {"+", "-"}, {"*", "/", "%"}}
}

func (p *vtlParser) matchOp(level int) string {
	p.skipWS()

	for _, op := range vtlBinLevels()[level] {
		if p.hasPrefix(op) {
			if (op == "<" || op == ">") && strings.HasPrefix(p.src[p.pos+1:], "=") {
				continue
			}

			return op
		}
	}

	return ""
}

func (p *vtlParser) parseBinary(level int) (vexpr, error) {
	if level >= len(vtlBinLevels()) {
		return p.parseUnary()
	}

	left, err := p.parseBinary(level + 1)
	if err != nil {
		return nil, err
	}

	for {
		op := p.matchOp(level)
		if op == "" {
			return left, nil
		}

		p.pos += len(op)

		right, rerr := p.parseBinary(level + 1)
		if rerr != nil {
			return nil, rerr
		}

		left = &vbin{op: op, l: left, r: right}
	}
}

func (p *vtlParser) parseUnary() (vexpr, error) {
	p.skipWS()

	switch p.peek() {
	case '!':
		p.pos++

		x, err := p.parseUnary()

		return &vunary{op: "!", x: x}, err
	case '-':
		p.pos++

		if isDigit(p.peek()) {
			e, err := p.parseNumber()
			if err != nil {
				return nil, err
			}

			return &vunary{op: "-", x: e}, nil
		}

		x, err := p.parseUnary()

		return &vunary{op: "-", x: x}, err
	}

	return p.parsePrimary()
}

func (p *vtlParser) parseNumber() (vexpr, error) {
	start := p.pos
	for isDigit(p.peek()) {
		p.pos++
	}

	isFloat := false

	if p.peek() == '.' && p.pos+1 < len(p.src) && isDigit(p.src[p.pos+1]) {
		isFloat = true
		p.pos++

		for isDigit(p.peek()) {
			p.pos++
		}
	}

	txt := p.src[start:p.pos]

	if isFloat {
		f, err := strconv.ParseFloat(txt, 64)

		return &vlit{v: f}, err
	}

	i, err := strconv.ParseInt(txt, 10, 64)

	return &vlit{v: i}, err
}

func (p *vtlParser) parsePrimary() (vexpr, error) {
	p.skipWS()

	c := p.peek()

	switch {
	case c == '$':
		p.pos++
		if p.peek() == '!' {
			p.pos++
		}

		ref, ok := p.parseRefBody()
		if !ok {
			return nil, fmt.Errorf("%w: invalid reference at offset %d", ErrValidation, p.pos)
		}

		return ref, nil
	case isDigit(c):
		return p.parseNumber()
	case c == '"':
		return p.parseDoubleString()
	case c == '\'':
		return p.parseSingleString()
	case c == '(':
		p.pos++

		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}

		return e, p.expectClose()
	case c == '[':
		return p.parseListOrRange()
	case c == '{':
		return p.parseMapLit()
	case isIdentStart(c):
		return p.parseKeyword()
	}

	return nil, fmt.Errorf("%w: unexpected %q at offset %d", ErrValidation, string(c), p.pos)
}

func (p *vtlParser) parseKeyword() (vexpr, error) {
	switch w := p.ident(); w {
	case "true":
		return &vlit{v: true}, nil
	case "false":
		return &vlit{v: false}, nil
	case "null":
		return &vlit{v: nil}, nil
	default:
		return nil, fmt.Errorf("%w: unexpected word %q", ErrValidation, w)
	}
}

func (p *vtlParser) parseSingleString() (vexpr, error) {
	p.pos++

	var b strings.Builder

	for !p.eof() {
		c := p.src[p.pos]

		if c == '\'' {
			if p.pos+1 < len(p.src) && p.src[p.pos+1] == '\'' {
				b.WriteByte('\'')

				p.pos += 2

				continue
			}

			p.pos++

			return &vlit{v: b.String()}, nil
		}

		b.WriteByte(c)
		p.pos++
	}

	return nil, fmt.Errorf("%w: unterminated string", ErrValidation)
}

func (p *vtlParser) parseDoubleString() (vexpr, error) {
	p.pos++

	var b strings.Builder

	for !p.eof() {
		c := p.src[p.pos]

		if c == '"' {
			if p.pos+1 < len(p.src) && p.src[p.pos+1] == '"' {
				b.WriteByte('"')

				p.pos += 2

				continue
			}

			p.pos++

			return interpString(b.String())
		}

		b.WriteByte(c)
		p.pos++
	}

	return nil, fmt.Errorf("%w: unterminated string", ErrValidation)
}

func interpString(s string) (vexpr, error) {
	if !strings.Contains(s, "$") {
		return &vlit{v: s}, nil
	}

	sub := &vtlParser{src: s, refsOnly: true}

	nodes, _, err := sub.parseBlock()
	if err != nil {
		return nil, err
	}

	return &vinterp{parts: nodes}, nil
}

func (p *vtlParser) parseListOrRange() (vexpr, error) {
	p.pos++
	p.skipWS()

	if p.peek() == ']' {
		p.pos++

		return &vlistEx{}, nil
	}

	first, err := p.parseExpr()
	if err != nil {
		return nil, err
	}

	p.skipWS()

	if p.hasPrefix("..") {
		p.pos += len("..")

		hi, herr := p.parseExpr()
		if herr != nil {
			return nil, herr
		}

		p.skipWS()

		if p.peek() != ']' {
			return nil, fmt.Errorf("%w: expected ']' after range", ErrValidation)
		}

		p.pos++

		return &vrange{lo: first, hi: hi}, nil
	}

	items := []vexpr{first}

	for {
		p.skipWS()

		switch p.peek() {
		case ',':
			p.pos++

			e, eerr := p.parseExpr()
			if eerr != nil {
				return nil, eerr
			}

			items = append(items, e)
		case ']':
			p.pos++

			return &vlistEx{items: items}, nil
		default:
			return nil, fmt.Errorf("%w: expected ',' or ']' in list", ErrValidation)
		}
	}
}

func (p *vtlParser) parseMapLit() (vexpr, error) {
	p.pos++

	m := &vmapEx{}

	for {
		p.skipWS()

		if p.peek() == '}' {
			p.pos++

			return m, nil
		}

		k, err := p.parseExpr()
		if err != nil {
			return nil, err
		}

		p.skipWS()

		if p.peek() != ':' {
			return nil, fmt.Errorf("%w: expected ':' in map literal", ErrValidation)
		}

		p.pos++

		v, err := p.parseExpr()
		if err != nil {
			return nil, err
		}

		m.keys = append(m.keys, k)
		m.vals = append(m.vals, v)

		p.skipWS()

		if p.peek() == ',' {
			p.pos++
		}
	}
}
