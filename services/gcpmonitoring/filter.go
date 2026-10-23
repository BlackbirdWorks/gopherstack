package gcpmonitoring

import (
	"regexp"
	"slices"
	"strings"
)

const (
	opHasSubstring = "has_substring"
	opStartsWith   = "starts_with"
	opEndsWith     = "ends_with"
	opOneOf        = "one_of"
	opRegex        = "regex"
	opNotEqual     = "!="
	twoBytes       = 2

	maxFilterLen = 8192
	maxRegexLen  = 1000
	maxFilterAST = 256
)

// env resolves a filter field path to its values.
type env interface {
	values(path string) []string
}

type mapEnv map[string][]string

func (m mapEnv) values(path string) []string { return m[path] }

type node interface {
	eval(e env) bool
	fields(into map[string]struct{})
}

// filter is a parsed Monitoring filter expression.
type filter struct {
	root node
}

func (f *filter) matches(e env) bool {
	if f == nil || f.root == nil {
		return true
	}

	return f.root.eval(e)
}

// referenced returns the field paths the filter reads.
func (f *filter) referenced() map[string]struct{} {
	out := map[string]struct{}{}
	if f != nil && f.root != nil {
		f.root.fields(out)
	}

	return out
}

type boolNode struct {
	kids []node
	or   bool
}

func (n *boolNode) eval(e env) bool {
	for _, k := range n.kids {
		if k.eval(e) == n.or {
			return n.or
		}
	}

	return !n.or
}

func (n *boolNode) fields(into map[string]struct{}) {
	for _, k := range n.kids {
		k.fields(into)
	}
}

type notNode struct{ kid node }

func (n *notNode) eval(e env) bool                 { return !n.kid.eval(e) }
func (n *notNode) fields(into map[string]struct{}) { n.kid.fields(into) }

type cmpNode struct {
	re    *regexp.Regexp
	field string
	op    string
	args  []string
}

func (n *cmpNode) fields(into map[string]struct{}) { into[n.field] = struct{}{} }

func (n *cmpNode) eval(e env) bool {
	vals := e.values(n.field)
	if len(vals) == 0 {
		return n.op == opNotEqual
	}

	if n.op == opNotEqual {
		for _, v := range vals {
			if n.test("=", v) {
				return false
			}
		}

		return true
	}

	for _, v := range vals {
		if n.test(n.op, v) {
			return true
		}
	}

	return false
}

func (n *cmpNode) test(op, v string) bool {
	switch op {
	case "=":
		return v == n.args[0]
	case "<":
		return v < n.args[0]
	case "<=":
		return v <= n.args[0]
	case ">":
		return v > n.args[0]
	case ">=":
		return v >= n.args[0]
	case ":", opHasSubstring:
		return strings.Contains(v, n.args[0])
	case opStartsWith:
		return strings.HasPrefix(v, n.args[0])
	case opEndsWith:
		return strings.HasSuffix(v, n.args[0])
	case opOneOf:
		return containsStr(n.args, v)
	case opRegex:
		return n.re.MatchString(v)
	}

	return false
}

func containsStr(list []string, s string) bool { return slices.Contains(list, s) }

type tokKind int

const (
	tkEOF tokKind = iota
	tkIdent
	tkString
	tkOp
	tkLParen
	tkRParen
	tkComma
)

type token struct {
	text string
	kind tokKind
}

func isIdentByte(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' ||
		c == '_' || c == '.' || c == '-' || c == '/' || c == '*' || c == '+'
}

func lexString(src string, i int) (token, int, error) {
	quote := src[i]
	i++

	var sb strings.Builder

	for i < len(src) {
		c := src[i]

		switch {
		case c == '\\' && i+1 < len(src):
			sb.WriteByte(src[i+1])

			i += 2
		case c == quote:
			return token{kind: tkString, text: sb.String()}, i + 1, nil
		default:
			sb.WriteByte(c)

			i++
		}
	}

	return token{}, i, errInvalid("Unparseable filter: unterminated string literal.")
}

func lexOp(src string, i int) (token, int, bool) {
	if i+1 < len(src) && src[i+1] == '=' && strings.ContainsRune("!<>", rune(src[i])) {
		return token{kind: tkOp, text: src[i : i+twoBytes]}, i + twoBytes, true
	}

	if strings.ContainsRune("=<>:", rune(src[i])) {
		return token{kind: tkOp, text: src[i : i+1]}, i + 1, true
	}

	return token{}, i, false
}

func lexPunct(c byte) (token, bool) {
	switch c {
	case '(':
		return token{kind: tkLParen, text: "("}, true
	case ')':
		return token{kind: tkRParen, text: ")"}, true
	case ',':
		return token{kind: tkComma, text: ","}, true
	}

	return token{}, false
}

func isSpace(c byte) bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r' }

func lexIdent(src string, i int) (token, int) {
	j := i
	for j < len(src) && isIdentByte(src[j]) {
		j++
	}

	return token{kind: tkIdent, text: src[i:j]}, j
}

// lexOne reads the token at src[i], returning the next index; ok is false for whitespace.
func lexOne(src string, i int) (token, int, bool, error) {
	c := src[i]

	switch {
	case isSpace(c):
		return token{}, i + 1, false, nil
	case c == '"' || c == '\'':
		tok, next, err := lexString(src, i)

		return tok, next, err == nil, err
	case isIdentByte(c):
		tok, next := lexIdent(src, i)

		return tok, next, true, nil
	}

	if t, isPunct := lexPunct(c); isPunct {
		return t, i + 1, true, nil
	}

	if t, n, isOp := lexOp(src, i); isOp {
		return t, n, true, nil
	}

	return token{}, i, false, errInvalid("Unparseable filter: unexpected character %q.", string(c))
}

func lex(src string) ([]token, error) {
	var toks []token

	for i := 0; i < len(src); {
		t, next, ok, err := lexOne(src, i)
		if err != nil {
			return nil, err
		}

		if ok {
			toks = append(toks, t)
		}

		i = next
	}

	return append(toks, token{kind: tkEOF}), nil
}

type parser struct {
	toks  []token
	pos   int
	nodes int
}

func (p *parser) peek() token { return p.toks[p.pos] }

func (p *parser) next() token {
	t := p.toks[p.pos]
	if t.kind != tkEOF {
		p.pos++
	}

	return t
}

func (p *parser) isKeyword(word string) bool {
	t := p.peek()

	return t.kind == tkIdent && strings.EqualFold(t.text, word)
}

// parseFilter parses a Monitoring filter; an empty string yields a filter that matches everything.
func parseFilter(src string) (*filter, error) {
	if len(src) > maxFilterLen {
		return nil, errInvalid("Filter is too long.")
	}

	if strings.TrimSpace(src) == "" {
		return &filter{}, nil
	}

	toks, err := lex(src)
	if err != nil {
		return nil, err
	}

	p := &parser{toks: toks}

	root, err := p.parseOr()
	if err != nil {
		return nil, err
	}

	if p.peek().kind != tkEOF {
		return nil, errInvalid("Unparseable filter: unexpected %q.", p.peek().text)
	}

	return &filter{root: root}, nil
}

func parseOptionalFilter(src string) (*filter, error) { return parseFilter(src) }

func (p *parser) parseOr() (node, error) {
	return p.parseChain("OR", true, p.parseAnd)
}

func (p *parser) parseAnd() (node, error) {
	return p.parseChain("AND", false, p.parseUnary)
}

func (p *parser) parseChain(word string, or bool, sub func() (node, error)) (node, error) {
	first, err := sub()
	if err != nil {
		return nil, err
	}

	kids := []node{first}

	for p.isKeyword(word) {
		p.next()

		k, subErr := sub()
		if subErr != nil {
			return nil, subErr
		}

		kids = append(kids, k)
	}

	if len(kids) == 1 {
		return first, nil
	}

	return &boolNode{kids: kids, or: or}, nil
}

func (p *parser) parseUnary() (node, error) {
	p.nodes++
	if p.nodes > maxFilterAST {
		return nil, errInvalid("Filter is too complex.")
	}

	if p.isKeyword("NOT") {
		p.next()

		kid, err := p.parseUnary()
		if err != nil {
			return nil, err
		}

		return &notNode{kid: kid}, nil
	}

	if p.peek().kind == tkLParen {
		p.next()

		inner, err := p.parseOr()
		if err != nil {
			return nil, err
		}

		if p.next().kind != tkRParen {
			return nil, errInvalid("Unparseable filter: missing closing parenthesis.")
		}

		return inner, nil
	}

	return p.parseCmp()
}

func (p *parser) parseCmp() (node, error) {
	field := p.next()
	if field.kind != tkIdent {
		return nil, errInvalid("Unparseable filter: expected a field name, got %q.", field.text)
	}

	opTok := p.next()
	if opTok.kind != tkOp {
		return nil, errInvalid("Unparseable filter: expected a comparison operator after %q.", field.text)
	}

	n := &cmpNode{field: field.text, op: opTok.text}

	val := p.peek()
	if val.kind == tkIdent && p.toks[p.pos+1].kind == tkLParen {
		return p.parseFunc(n)
	}

	p.next()

	if val.kind != tkString && val.kind != tkIdent {
		return nil, errInvalid("Unparseable filter: expected a value after %q.", opTok.text)
	}

	n.args = []string{val.text}

	return n, nil
}

var funcOps = map[string]string{ //nolint:gochecknoglobals // immutable lookup
	"starts_with":                 "starts_with",
	"ends_with":                   "ends_with",
	"has_substring":               "has_substring",
	"one_of":                      "one_of",
	"monitoring.regex.full_match": opRegex,
}

func (p *parser) parseFunc(n *cmpNode) (node, error) {
	name := p.next().text

	op, ok := funcOps[name]
	if !ok || (n.op != "=" && n.op != opNotEqual) {
		return nil, errInvalid("Unparseable filter: unsupported function %q.", name)
	}

	p.next()

	for p.peek().kind != tkRParen {
		a := p.next()
		if a.kind != tkString && a.kind != tkIdent {
			return nil, errInvalid("Unparseable filter: bad argument to %s.", name)
		}

		n.args = append(n.args, a.text)

		if p.peek().kind == tkComma {
			p.next()
		}
	}

	p.next()

	if len(n.args) == 0 {
		return nil, errInvalid("Unparseable filter: %s needs an argument.", name)
	}

	if op == opRegex {
		if len(n.args[0]) > maxRegexLen {
			return nil, errInvalid("Filter regular expression is too long.")
		}

		re, err := regexp.Compile("^(?:" + n.args[0] + ")$")
		if err != nil {
			return nil, errInvalid("Invalid regular expression %q in filter.", n.args[0])
		}

		n.re = re
	}

	return wrapFunc(n, op), nil
}

// wrapFunc makes `field != fn(...)` negate the function and `field = fn(...)` apply it.
func wrapFunc(n *cmpNode, op string) node {
	negate := n.op == opNotEqual
	n.op = op

	if negate {
		return &notNode{kid: n}
	}

	return n
}
