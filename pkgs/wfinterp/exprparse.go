package wfinterp

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

type tokKind int

const (
	opAnd     = "and"
	opOr      = "or"
	fnDefault = "default"
	fnIf      = "if"
)

const (
	tokEOF tokKind = iota
	tokNum
	tokStr
	tokIdent
	tokOp
)

type token struct {
	val  any
	text string
	kind tokKind
}

type node any

type (
	litNode    struct{ v any }
	nameNode   struct{ name string }
	memberNode struct {
		x    node
		name string
	}
	indexNode struct{ x, i node }
	callNode  struct {
		fn   node
		args []node
	}
	unaryNode struct {
		x  node
		op string
	}
	binNode struct {
		l, r node
		op   string
	}
	listNode struct{ items []node }
	mapNode  struct{ keys, vals []node }
)

// Expr is a parsed ${...} expression.
type Expr struct {
	root node
	src  string
}

// Source returns the expression text without the ${ } wrapper.
func (e *Expr) Source() string { return e.src }

type parseError struct{ msg string }

func (e *parseError) Error() string { return e.msg }

func perr(format string, a ...any) error { return &parseError{msg: fmt.Sprintf(format, a...)} }

func isIdentStart(c rune) bool { return c == '_' || unicode.IsLetter(c) }

func isIdentPart(c rune) bool { return isIdentStart(c) || unicode.IsDigit(c) }

func lex(src string) ([]token, error) {
	var toks []token

	rs := []rune(src)

	for i := 0; i < len(rs); {
		if unicode.IsSpace(rs[i]) {
			i++

			continue
		}

		t, n, err := lexToken(rs, i)
		if err != nil {
			return nil, err
		}

		toks, i = append(toks, t), n
	}

	return append(toks, token{kind: tokEOF}), nil
}

func lexToken(rs []rune, i int) (token, int, error) {
	c := rs[i]

	switch {
	case unicode.IsDigit(c) || (c == '.' && i+1 < len(rs) && unicode.IsDigit(rs[i+1])):
		return lexNumber(rs, i)
	case c == '"' || c == '\'':
		return lexString(rs, i)
	case isIdentStart(c):
		j := i
		for j < len(rs) && isIdentPart(rs[j]) {
			j++
		}

		return token{kind: tokIdent, text: string(rs[i:j])}, j, nil
	}

	op := lexOp(rs, i)
	if op == "" {
		return token{}, 0, perr("unexpected character %q in expression", string(c))
	}

	return token{kind: tokOp, text: op}, i + len([]rune(op)), nil
}

func lexOp(rs []rune, i int) string {
	if i+1 < len(rs) {
		two := string(rs[i : i+2])
		switch two {
		case "==", "!=", "<=", ">=", "//":
			return two
		}
	}

	if strings.ContainsRune("+-*/%()[]{},:.<>", rs[i]) {
		return string(rs[i])
	}

	return ""
}

// scanNumber returns the end of the number starting at i and whether it is a float.
func scanNumber(rs []rune, i int) (int, bool) {
	j, isFloat := i, false

	for j < len(rs) {
		c := rs[j]

		switch {
		case unicode.IsDigit(c):
			j++
		case c == '.' && !isFloat && j+1 < len(rs) && unicode.IsDigit(rs[j+1]):
			isFloat = true
			j++
		case (c == 'e' || c == 'E') && j+1 < len(rs) && (unicode.IsDigit(rs[j+1]) || rs[j+1] == '-' || rs[j+1] == '+'):
			isFloat = true
			j += 2
		default:
			return j, isFloat
		}
	}

	return j, isFloat
}

func lexNumber(rs []rune, i int) (token, int, error) {
	j, isFloat := scanNumber(rs, i)
	text := string(rs[i:j])

	if !isFloat {
		n, err := strconv.ParseInt(text, 10, 64)
		if err != nil {
			return token{}, 0, perr("invalid number %q", text)
		}

		return token{kind: tokNum, val: n, text: text}, j, nil
	}

	f, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return token{}, 0, perr("invalid number %q", text)
	}

	return token{kind: tokNum, val: f, text: text}, j, nil
}

func lexString(rs []rune, i int) (token, int, error) {
	quote := rs[i]

	var b strings.Builder

	for j := i + 1; j < len(rs); j++ {
		switch c := rs[j]; {
		case c == quote:
			return token{kind: tokStr, val: b.String()}, j + 1, nil
		case c == '\\' && j+1 < len(rs):
			j++

			s, adv, err := lexEscape(rs, j)
			if err != nil {
				return token{}, 0, err
			}

			b.WriteString(s)

			j += adv
		default:
			b.WriteRune(c)
		}
	}

	return token{}, 0, perr("unterminated string literal")
}

func simpleEscape(r rune) (string, bool) {
	switch r {
	case 'n':
		return "\n", true
	case 't':
		return "\t", true
	case 'r':
		return "\r", true
	case 'b':
		return "\b", true
	case 'f':
		return "\f", true
	default:
		return "", false
	}
}

// lexEscape decodes the escape starting at rs[j] and returns the text and how many extra runes it used.
func lexEscape(rs []rune, j int) (string, int, error) {
	if s, ok := simpleEscape(rs[j]); ok {
		return s, 0, nil
	}

	if rs[j] != 'u' && rs[j] != 'U' {
		return string(rs[j]), 0, nil
	}

	width := 4
	if rs[j] == 'U' {
		width = 8
	}

	if j+width >= len(rs) {
		return "", 0, perr("invalid unicode escape")
	}

	n, err := strconv.ParseUint(string(rs[j+1:j+1+width]), 16, 32)
	if err != nil || n > unicode.MaxRune {
		return "", 0, perr("invalid unicode escape")
	}

	return string(rune(n)), width, nil
}

type parser struct {
	toks []token
	pos  int
}

// ParseExpr parses expression source (without the ${ } wrapper).
func ParseExpr(src string) (*Expr, error) {
	toks, err := lex(src)
	if err != nil {
		return nil, err
	}

	p := &parser{toks: toks}

	root, err := p.parseOr()
	if err != nil {
		return nil, err
	}

	if p.peek().kind != tokEOF {
		return nil, perr("unexpected %q in expression", p.peek().text)
	}

	return &Expr{root: root, src: strings.TrimSpace(src)}, nil
}

func (p *parser) peek() token { return p.toks[p.pos] }

func (p *parser) next() token {
	t := p.toks[p.pos]
	if t.kind != tokEOF {
		p.pos++
	}

	return t
}

func (p *parser) isOp(s string) bool {
	t := p.peek()

	return t.kind == tokOp && t.text == s
}

func (p *parser) isWord(s string) bool {
	t := p.peek()

	return t.kind == tokIdent && t.text == s
}

func (p *parser) expectOp(s string) error {
	if !p.isOp(s) {
		return perr("expected %q in expression, found %q", s, p.peek().text)
	}

	p.pos++

	return nil
}

func (p *parser) parseOr() (node, error) {
	l, err := p.parseAnd()
	if err != nil {
		return nil, err
	}

	for p.isWord(opOr) {
		p.pos++

		r, rerr := p.parseAnd()
		if rerr != nil {
			return nil, rerr
		}

		l = &binNode{op: opOr, l: l, r: r}
	}

	return l, nil
}

func (p *parser) parseAnd() (node, error) {
	l, err := p.parseNot()
	if err != nil {
		return nil, err
	}

	for p.isWord(opAnd) {
		p.pos++

		r, rerr := p.parseNot()
		if rerr != nil {
			return nil, rerr
		}

		l = &binNode{op: opAnd, l: l, r: r}
	}

	return l, nil
}

func (p *parser) parseNot() (node, error) {
	if p.isWord("not") {
		p.pos++

		x, err := p.parseNot()
		if err != nil {
			return nil, err
		}

		return &unaryNode{op: "not", x: x}, nil
	}

	return p.parseCmp()
}

func (p *parser) cmpOp() string {
	t := p.peek()

	switch {
	case t.kind == tokOp && (t.text == "==" || t.text == "!=" || t.text == "<" || t.text == ">" ||
		t.text == "<=" || t.text == ">="):
		return t.text
	case t.kind == tokIdent && t.text == "in":
		return "in"
	default:
		return ""
	}
}

func (p *parser) parseCmp() (node, error) {
	l, err := p.parseAdd()
	if err != nil {
		return nil, err
	}

	for op := p.cmpOp(); op != ""; op = p.cmpOp() {
		p.pos++

		r, rerr := p.parseAdd()
		if rerr != nil {
			return nil, rerr
		}

		l = &binNode{op: op, l: l, r: r}
	}

	return l, nil
}

func (p *parser) parseAdd() (node, error) {
	l, err := p.parseMul()
	if err != nil {
		return nil, err
	}

	for p.isOp("+") || p.isOp("-") {
		op := p.next().text

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

	for p.isOp("*") || p.isOp("/") || p.isOp("%") || p.isOp("//") {
		op := p.next().text

		r, rerr := p.parseUnary()
		if rerr != nil {
			return nil, rerr
		}

		l = &binNode{op: op, l: l, r: r}
	}

	return l, nil
}

func (p *parser) parseUnary() (node, error) {
	if p.isOp("-") {
		p.pos++

		x, err := p.parseUnary()
		if err != nil {
			return nil, err
		}

		return &unaryNode{op: "-", x: x}, nil
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
		case p.isOp("."):
			p.pos++

			t := p.next()
			if t.kind != tokIdent {
				return nil, perr("expected a field name after '.', found %q", t.text)
			}

			x = &memberNode{x: x, name: t.text}
		case p.isOp("["):
			p.pos++

			i, ierr := p.parseOr()
			if ierr != nil {
				return nil, ierr
			}

			if err = p.expectOp("]"); err != nil {
				return nil, err
			}

			x = &indexNode{x: x, i: i}
		case p.isOp("("):
			p.pos++

			args, aerr := p.parseArgs()
			if aerr != nil {
				return nil, aerr
			}

			x = &callNode{fn: x, args: args}
		default:
			return x, nil
		}
	}
}

func (p *parser) parseArgs() ([]node, error) {
	var args []node

	if p.isOp(")") {
		p.pos++

		return args, nil
	}

	for {
		a, err := p.parseOr()
		if err != nil {
			return nil, err
		}

		args = append(args, a)

		if p.isOp(",") {
			p.pos++

			continue
		}

		return args, p.expectOp(")")
	}
}

func (p *parser) parsePrimary() (node, error) {
	t := p.next()

	switch t.kind {
	case tokNum, tokStr:
		return &litNode{v: t.val}, nil
	case tokIdent:
		switch t.text {
		case "true":
			return &litNode{v: true}, nil
		case "false":
			return &litNode{v: false}, nil
		case "null":
			return &litNode{v: nil}, nil
		}

		return &nameNode{name: t.text}, nil
	case tokOp:
		return p.parseGroup(t)
	default:
		return nil, perr("unexpected end of expression")
	}
}

func (p *parser) parseGroup(t token) (node, error) {
	switch t.text {
	case "(":
		x, err := p.parseOr()
		if err != nil {
			return nil, err
		}

		return x, p.expectOp(")")
	case "[":
		return p.parseListLit()
	case "{":
		return p.parseMapLit()
	default:
		return nil, perr("unexpected %q in expression", t.text)
	}
}

func (p *parser) parseListLit() (node, error) {
	l := &listNode{}

	for !p.isOp("]") {
		x, err := p.parseOr()
		if err != nil {
			return nil, err
		}

		l.items = append(l.items, x)

		if !p.isOp(",") {
			break
		}

		p.pos++
	}

	return l, p.expectOp("]")
}

func (p *parser) parseMapLit() (node, error) {
	m := &mapNode{}

	for !p.isOp("}") {
		k, err := p.parseOr()
		if err != nil {
			return nil, err
		}

		if err = p.expectOp(":"); err != nil {
			return nil, err
		}

		v, verr := p.parseOr()
		if verr != nil {
			return nil, verr
		}

		m.keys, m.vals = append(m.keys, k), append(m.vals, v)

		if !p.isOp(",") {
			break
		}

		p.pos++
	}

	return m, p.expectOp("}")
}

// calls returns the dotted names of every function called in the expression.
func (e *Expr) calls() []string {
	var out []string

	walk(e.root, func(n node) {
		if c, ok := n.(*callNode); ok {
			if name, isName := dotted(c.fn); isName {
				out = append(out, name)
			} else {
				out = append(out, "")
			}
		}
	})

	return out
}

func walk(n node, visit func(node)) {
	visit(n)

	switch x := n.(type) {
	case *memberNode:
		walk(x.x, visit)
	case *indexNode:
		walk(x.x, visit)
		walk(x.i, visit)
	case *callNode:
		walk(x.fn, visit)

		for _, a := range x.args {
			walk(a, visit)
		}
	case *unaryNode:
		walk(x.x, visit)
	case *binNode:
		walk(x.l, visit)
		walk(x.r, visit)
	case *listNode:
		for _, a := range x.items {
			walk(a, visit)
		}
	case *mapNode:
		for i := range x.keys {
			walk(x.keys[i], visit)
			walk(x.vals[i], visit)
		}
	}
}

// dotted renders a name.a.b chain as "name.a.b".
func dotted(n node) (string, bool) {
	switch x := n.(type) {
	case *nameNode:
		return x.name, true
	case *memberNode:
		base, ok := dotted(x.x)

		return base + "." + x.name, ok
	default:
		return "", false
	}
}
