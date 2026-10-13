package apigateway

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const blockEnd = "end"

var errVTLSyntax = errors.New("velocity syntax error")

type vtlText string

type vtlRefNode struct {
	ref *vtlRef
}

type vtlSetNode struct {
	target *vtlRef
	expr   any
}

type vtlIfNode struct {
	conds    []any
	bodies   [][]any
	elseBody []any
}

type vtlForeachNode struct {
	list    any
	varName string
	body    []any
}

type vtlMacroDef struct {
	name   string
	params []string
	body   []any
}

type vtlMacroCall struct {
	name string
	args []any
}

type (
	vtlBreakNode struct{}
	vtlStopNode  struct{}
)

type (
	vtlLiteral struct{ value any }
	vtlInterp  struct{ nodes []any }
	vtlListLit struct{ items []any }
	vtlMapLit  struct{ keys, values []any }
	vtlRange   struct{ from, to any }
	vtlUnary   struct {
		operand any
		op      string
	}
	vtlBinary struct {
		left, right any
		op          string
	}
)

type vtlAccessor struct {
	index  any
	name   string
	args   []any
	isCall bool
}

type vtlRef struct {
	name      string
	raw       string
	accessors []vtlAccessor
	quiet     bool
}

type vtlParser struct {
	macros map[string]*vtlMacroDef
	src    string
	pos    int
}

func parseVTL(src string) ([]any, map[string]*vtlMacroDef, error) {
	p := &vtlParser{src: src, macros: map[string]*vtlMacroDef{}}

	nodes, term, err := p.parseNodes()
	if err != nil {
		return nil, nil, err
	}

	if term != "" {
		return nil, nil, fmt.Errorf("%w: unexpected #%s", errVTLSyntax, term)
	}

	return nodes, p.macros, nil
}

func (p *vtlParser) eof() bool { return p.pos >= len(p.src) }

func (p *vtlParser) peekByte(off int) byte {
	if p.pos+off >= len(p.src) {
		return 0
	}

	return p.src[p.pos+off]
}

func (p *vtlParser) hasPrefix(s string) bool { return strings.HasPrefix(p.src[p.pos:], s) }

func (p *vtlParser) skipSpaces() {
	for !p.eof() && (p.src[p.pos] == ' ' || p.src[p.pos] == '\t' || p.src[p.pos] == '\n' || p.src[p.pos] == '\r') {
		p.pos++
	}
}

// skipLineEnd swallows trailing blanks plus one newline after a directive.
func (p *vtlParser) skipLineEnd() {
	i := p.pos
	for i < len(p.src) && (p.src[i] == ' ' || p.src[i] == '\t') {
		i++
	}

	switch {
	case i < len(p.src) && p.src[i] == '\n':
		p.pos = i + 1
	case strings.HasPrefix(p.src[i:], "\r\n"):
		p.pos = i + len("\r\n")
	case i >= len(p.src):
		p.pos = i
	}
}

func isIdentStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isIdentChar(c byte) bool {
	return isIdentStart(c) || (c >= '0' && c <= '9') || c == '-'
}

func (p *vtlParser) readIdent() string {
	start := p.pos
	if p.eof() || !isIdentStart(p.src[p.pos]) {
		return ""
	}

	p.pos++

	for !p.eof() && isIdentChar(p.src[p.pos]) {
		p.pos++
	}

	return p.src[start:p.pos]
}

// parseNodes reads nodes until EOF or a block terminator (end/else/elseif).
func (p *vtlParser) parseNodes() ([]any, string, error) {
	var (
		nodes []any
		text  strings.Builder
	)

	flush := func() {
		if text.Len() > 0 {
			nodes = append(nodes, vtlText(text.String()))
			text.Reset()
		}
	}

	for !p.eof() {
		c := p.src[p.pos]

		switch {
		case c == '#':
			flush()

			node, term, err := p.parseHash(&text)
			if err != nil {
				return nil, "", err
			}

			if term != "" {
				return nodes, term, nil
			}

			if node != nil {
				nodes = append(nodes, node)
			}
		case c == '$':
			ref, ok := p.tryParseRef()
			if !ok {
				text.WriteByte(c)
				p.pos++

				continue
			}

			flush()

			nodes = append(nodes, &vtlRefNode{ref: ref})
		case c == '\\' && (p.peekByte(1) == '$' || p.peekByte(1) == '#'):
			text.WriteByte(p.peekByte(1))
			p.pos += 2
		default:
			text.WriteByte(c)
			p.pos++
		}
	}

	flush()

	return nodes, "", nil
}

// parseHash handles a '#': comments, directives, macro calls, or a literal '#'.
func (p *vtlParser) parseHash(text *strings.Builder) (any, string, error) {
	switch {
	case p.hasPrefix("##"):
		for !p.eof() && p.src[p.pos] != '\n' {
			p.pos++
		}

		if !p.eof() {
			p.pos++
		}

		return nil, "", nil
	case p.hasPrefix("#*"):
		end := strings.Index(p.src[p.pos+2:], "*#")
		if end < 0 {
			return nil, "", fmt.Errorf("%w: unterminated comment", errVTLSyntax)
		}

		p.pos += len("#*") + end + len("*#")

		return nil, "", nil
	case p.hasPrefix("#[["):
		end := strings.Index(p.src[p.pos+3:], "]]#")
		if end < 0 {
			return nil, "", fmt.Errorf("%w: unterminated literal block", errVTLSyntax)
		}

		text.WriteString(p.src[p.pos+3 : p.pos+3+end])
		p.pos += len("#[[") + end + len("]]#")

		return nil, "", nil
	}

	save := p.pos
	p.pos++

	formal := false
	if p.peekByte(0) == '{' {
		formal = true
		p.pos++
	}

	name := p.readIdent()
	if formal && p.peekByte(0) == '}' {
		p.pos++
	}

	return p.parseDirective(name, save, text)
}

func (p *vtlParser) parseDirective(name string, save int, text *strings.Builder) (any, string, error) {
	switch name {
	case "set":
		n, err := p.parseSet()

		return n, "", err
	case "if":
		n, err := p.parseIf()

		return n, "", err
	case "foreach":
		n, err := p.parseForeach()

		return n, "", err
	case "macro":
		return nil, "", p.parseMacro()
	case "break":
		p.skipLineEnd()

		return vtlBreakNode{}, "", nil
	case "stop":
		p.skipLineEnd()

		return vtlStopNode{}, "", nil
	case blockEnd, "else", "elseif":
		return nil, name, nil
	}

	if _, ok := p.macros[name]; ok && p.nextIsOpenParen() {
		args, err := p.parseCallArgs()
		if err != nil {
			return nil, "", err
		}

		p.skipLineEnd()

		return &vtlMacroCall{name: name, args: args}, "", nil
	}

	p.pos = save + 1
	text.WriteByte('#')

	return nil, "", nil
}

func (p *vtlParser) nextIsOpenParen() bool {
	i := p.pos
	for i < len(p.src) && (p.src[i] == ' ' || p.src[i] == '\t') {
		i++
	}

	return i < len(p.src) && p.src[i] == '('
}

func (p *vtlParser) expect(c byte) error {
	p.skipSpaces()

	if p.peekByte(0) != c {
		return fmt.Errorf("%w: expected %q at offset %d", errVTLSyntax, c, p.pos)
	}

	p.pos++

	return nil
}

func (p *vtlParser) parseSet() (any, error) {
	if err := p.expect('('); err != nil {
		return nil, err
	}

	p.skipSpaces()

	target, ok := p.tryParseRef()
	if !ok {
		return nil, fmt.Errorf("%w: #set needs a reference", errVTLSyntax)
	}

	if err := p.expect('='); err != nil {
		return nil, err
	}

	expr, err := p.parseExpr()
	if err != nil {
		return nil, err
	}

	if err = p.expect(')'); err != nil {
		return nil, err
	}

	p.skipLineEnd()

	return &vtlSetNode{target: target, expr: expr}, nil
}

func (p *vtlParser) parseIf() (any, error) {
	node := &vtlIfNode{}

	for {
		if err := p.expect('('); err != nil {
			return nil, err
		}

		cond, err := p.parseExpr()
		if err != nil {
			return nil, err
		}

		if err = p.expect(')'); err != nil {
			return nil, err
		}

		p.skipLineEnd()

		body, term, err := p.parseNodes()
		if err != nil {
			return nil, err
		}

		node.conds = append(node.conds, cond)
		node.bodies = append(node.bodies, body)

		switch term {
		case "elseif":
			continue
		case "else":
			p.skipLineEnd()

			elseBody, term2, err2 := p.parseNodes()
			if err2 != nil {
				return nil, err2
			}

			if term2 != blockEnd {
				return nil, fmt.Errorf("%w: #if without #end", errVTLSyntax)
			}

			node.elseBody = elseBody
			p.skipLineEnd()

			return node, nil
		case blockEnd:
			p.skipLineEnd()

			return node, nil
		default:
			return nil, fmt.Errorf("%w: #if without #end", errVTLSyntax)
		}
	}
}

func (p *vtlParser) parseForeach() (any, error) {
	if err := p.expect('('); err != nil {
		return nil, err
	}

	p.skipSpaces()

	ref, ok := p.tryParseRef()
	if !ok || len(ref.accessors) > 0 {
		return nil, fmt.Errorf("%w: #foreach needs a loop variable", errVTLSyntax)
	}

	p.skipSpaces()

	if !p.hasPrefix("in") {
		return nil, fmt.Errorf("%w: #foreach needs 'in'", errVTLSyntax)
	}

	p.pos += 2

	list, err := p.parseExpr()
	if err != nil {
		return nil, err
	}

	if err = p.expect(')'); err != nil {
		return nil, err
	}

	p.skipLineEnd()

	body, term, err := p.parseNodes()
	if err != nil {
		return nil, err
	}

	if term != blockEnd {
		return nil, fmt.Errorf("%w: #foreach without #end", errVTLSyntax)
	}

	p.skipLineEnd()

	return &vtlForeachNode{varName: ref.name, list: list, body: body}, nil
}

func (p *vtlParser) parseMacro() error {
	if err := p.expect('('); err != nil {
		return err
	}

	p.skipSpaces()

	nameStart := p.pos
	name := p.readIdent()

	if name == "" {
		return fmt.Errorf("%w: #macro needs a name at offset %d", errVTLSyntax, nameStart)
	}

	var params []string

	for {
		p.skipSpaces()

		if p.peekByte(0) == ')' {
			p.pos++

			break
		}

		ref, ok := p.tryParseRef()
		if !ok {
			return fmt.Errorf("%w: bad #macro parameter", errVTLSyntax)
		}

		params = append(params, ref.name)
	}

	p.skipLineEnd()

	def := &vtlMacroDef{name: name, params: params}
	p.macros[name] = def

	body, term, err := p.parseNodes()
	if err != nil {
		return err
	}

	if term != blockEnd {
		return fmt.Errorf("%w: #macro without #end", errVTLSyntax)
	}

	def.body = body

	p.skipLineEnd()

	return nil
}

func (p *vtlParser) parseCallArgs() ([]any, error) {
	if err := p.expect('('); err != nil {
		return nil, err
	}

	var args []any

	for {
		p.skipSpaces()

		if p.peekByte(0) == ')' {
			p.pos++

			return args, nil
		}

		arg, err := p.parseExpr()
		if err != nil {
			return nil, err
		}

		args = append(args, arg)

		p.skipSpaces()

		if p.peekByte(0) == ',' {
			p.pos++
		}
	}
}

// tryParseRef parses $name, $!name, ${name} with accessors; ok is false when '$' does not start a reference.
func (p *vtlParser) tryParseRef() (*vtlRef, bool) {
	start := p.pos
	if p.peekByte(0) != '$' {
		return nil, false
	}

	i := 1
	quiet := false

	if p.peekByte(i) == '!' {
		quiet = true
		i++
	}

	formal := false
	if p.peekByte(i) == '{' {
		formal = true
		i++
	}

	if !isIdentStart(p.peekByte(i)) {
		return nil, false
	}

	p.pos += i
	ref := &vtlRef{name: p.readIdent(), quiet: quiet}

	if err := p.parseAccessors(ref); err != nil {
		p.pos = start

		return nil, false
	}

	if formal {
		if p.peekByte(0) != '}' {
			p.pos = start

			return nil, false
		}

		p.pos++
	}

	ref.raw = p.src[start:p.pos]

	return ref, true
}

func (p *vtlParser) parseAccessors(ref *vtlRef) error {
	for {
		switch {
		case p.peekByte(0) == '.' && isIdentStart(p.peekByte(1)):
			p.pos++
			acc := vtlAccessor{name: p.readIdent()}

			if p.peekByte(0) == '(' {
				args, err := p.parseCallArgs()
				if err != nil {
					return err
				}

				acc.isCall = true
				acc.args = args
			}

			ref.accessors = append(ref.accessors, acc)
		case p.peekByte(0) == '[':
			p.pos++

			idx, err := p.parseExpr()
			if err != nil {
				return err
			}

			if err = p.expect(']'); err != nil {
				return err
			}

			ref.accessors = append(ref.accessors, vtlAccessor{index: idx})
		default:
			return nil
		}
	}
}

func (p *vtlParser) parseExpr() (any, error) { return p.parseBinary(0) }

const (
	vtlLevelOr = iota
	vtlLevelAnd
	vtlLevelEquality
	vtlLevelRelational
	vtlLevelAdditive
	vtlBinaryLevelCount
)

func vtlBinaryLevel(level int) []string {
	switch level {
	case vtlLevelOr:
		return []string{"||", "or"}
	case vtlLevelAnd:
		return []string{"&&", "and"}
	case vtlLevelEquality:
		return []string{"==", "!=", "eq", "ne"}
	case vtlLevelRelational:
		return []string{"<=", ">=", "<", ">", "le", "ge", "lt", "gt"}
	case vtlLevelAdditive:
		return []string{"+", "-"}
	}

	return []string{"*", "/", "%"}
}

func (p *vtlParser) parseBinary(level int) (any, error) {
	if level > vtlBinaryLevelCount {
		return p.parseUnary()
	}

	left, err := p.parseBinary(level + 1)
	if err != nil {
		return nil, err
	}

	for {
		p.skipSpaces()

		op := p.matchOperator(vtlBinaryLevel(level))
		if op == "" {
			return left, nil
		}

		right, rerr := p.parseBinary(level + 1)
		if rerr != nil {
			return nil, rerr
		}

		left = &vtlBinary{op: normalizeOp(op), left: left, right: right}
	}
}

func normalizeOp(op string) string {
	switch op {
	case "or":
		return "||"
	case "and":
		return "&&"
	case "eq":
		return "=="
	case "ne":
		return "!="
	case "le":
		return "<="
	case "ge":
		return ">="
	case "lt":
		return "<"
	case "gt":
		return ">"
	}

	return op
}

func (p *vtlParser) matchOperator(ops []string) string {
	for _, op := range ops {
		if !p.hasPrefix(op) {
			continue
		}

		if isIdentStart(op[0]) {
			if isIdentChar(p.peekByte(len(op))) {
				continue
			}
		} else if op == "<" || op == ">" || op == "!" {
			if p.peekByte(len(op)) == '=' {
				continue
			}
		}

		if (op == "-" || op == "+") && p.peekByte(1) == ')' {
			continue
		}

		p.pos += len(op)

		return op
	}

	return ""
}

func (p *vtlParser) parseUnary() (any, error) {
	p.skipSpaces()

	switch {
	case p.peekByte(0) == '!' && p.peekByte(1) != '=':
		p.pos++

		operand, err := p.parseUnary()
		if err != nil {
			return nil, err
		}

		return &vtlUnary{op: "!", operand: operand}, nil
	case p.hasPrefix("not") && !isIdentChar(p.peekByte(len("not"))):
		p.pos += 3

		operand, err := p.parseUnary()
		if err != nil {
			return nil, err
		}

		return &vtlUnary{op: "!", operand: operand}, nil
	case p.peekByte(0) == '-' && !isDigit(p.peekByte(1)):
		p.pos++

		operand, err := p.parseUnary()
		if err != nil {
			return nil, err
		}

		return &vtlUnary{op: "-", operand: operand}, nil
	}

	return p.parsePrimary()
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

func (p *vtlParser) parsePrimary() (any, error) {
	p.skipSpaces()

	c := p.peekByte(0)

	switch {
	case c == '(':
		p.pos++

		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}

		return e, p.expect(')')
	case c == '$':
		ref, ok := p.tryParseRef()
		if !ok {
			return nil, fmt.Errorf("%w: bad reference at offset %d", errVTLSyntax, p.pos)
		}

		return ref, nil
	case c == '\'' || c == '"':
		return p.parseString(c)
	case isDigit(c) || (c == '-' && isDigit(p.peekByte(1))):
		return p.parseNumber()
	case c == '[':
		return p.parseListOrRange()
	case c == '{':
		return p.parseMapLiteral()
	case isIdentStart(c):
		return p.parseWord()
	}

	return nil, fmt.Errorf("%w: unexpected %q at offset %d", errVTLSyntax, c, p.pos)
}

func (p *vtlParser) parseWord() (any, error) {
	word := p.readIdent()

	switch word {
	case "true":
		return &vtlLiteral{value: true}, nil
	case "false":
		return &vtlLiteral{value: false}, nil
	case jsonNull:
		return &vtlLiteral{value: nil}, nil
	}

	return nil, fmt.Errorf("%w: unexpected word %q", errVTLSyntax, word)
}

func (p *vtlParser) parseNumber() (any, error) {
	start := p.pos
	if p.peekByte(0) == '-' {
		p.pos++
	}

	for isDigit(p.peekByte(0)) {
		p.pos++
	}

	if p.peekByte(0) == '.' && isDigit(p.peekByte(1)) {
		p.pos++

		for isDigit(p.peekByte(0)) {
			p.pos++
		}

		f, err := strconv.ParseFloat(p.src[start:p.pos], 64)
		if err != nil {
			return nil, fmt.Errorf("%w: bad number", errVTLSyntax)
		}

		return &vtlLiteral{value: f}, nil
	}

	n, err := strconv.ParseInt(p.src[start:p.pos], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%w: bad number", errVTLSyntax)
	}

	return &vtlLiteral{value: n}, nil
}

func (p *vtlParser) parseString(quote byte) (any, error) {
	p.pos++

	var sb strings.Builder

	for {
		if p.eof() {
			return nil, fmt.Errorf("%w: unterminated string", errVTLSyntax)
		}

		c := p.src[p.pos]

		if c == quote {
			if p.peekByte(1) == quote {
				sb.WriteByte(quote)
				p.pos += 2

				continue
			}

			p.pos++

			break
		}

		sb.WriteByte(c)
		p.pos++
	}

	s := sb.String()
	if quote == '\'' || !strings.ContainsAny(s, "$#") {
		return &vtlLiteral{value: s}, nil
	}

	nodes, _, err := parseVTL(s)
	if err != nil {
		return nil, err
	}

	return &vtlInterp{nodes: nodes}, nil
}

func (p *vtlParser) parseListOrRange() (any, error) {
	p.pos++
	p.skipSpaces()

	if p.peekByte(0) == ']' {
		p.pos++

		return &vtlListLit{}, nil
	}

	first, err := p.parseExpr()
	if err != nil {
		return nil, err
	}

	p.skipSpaces()

	if p.hasPrefix("..") {
		p.pos += 2

		to, terr := p.parseExpr()
		if terr != nil {
			return nil, terr
		}

		return &vtlRange{from: first, to: to}, p.expect(']')
	}

	items := []any{first}

	for {
		p.skipSpaces()

		if p.peekByte(0) == ']' {
			p.pos++

			return &vtlListLit{items: items}, nil
		}

		if p.peekByte(0) != ',' {
			return nil, fmt.Errorf("%w: expected ',' in list at offset %d", errVTLSyntax, p.pos)
		}

		p.pos++

		item, ierr := p.parseExpr()
		if ierr != nil {
			return nil, ierr
		}

		items = append(items, item)
	}
}

func (p *vtlParser) parseMapLiteral() (any, error) {
	p.pos++

	lit := &vtlMapLit{}

	for {
		p.skipSpaces()

		if p.peekByte(0) == '}' {
			p.pos++

			return lit, nil
		}

		key, err := p.parseExpr()
		if err != nil {
			return nil, err
		}

		if err = p.expect(':'); err != nil {
			return nil, err
		}

		val, err := p.parseExpr()
		if err != nil {
			return nil, err
		}

		lit.keys = append(lit.keys, key)
		lit.values = append(lit.values, val)

		p.skipSpaces()

		if p.peekByte(0) == ',' {
			p.pos++
		}
	}
}
