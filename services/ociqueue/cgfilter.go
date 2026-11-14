package ociqueue

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const (
	maxFilterLen = 256
	opPairLen    = 2
)

var errFilterSyntax = errors.New("invalid filter")

// matcher evaluates a compiled consumer group filter against message attributes.
type matcher func(attrs map[string]string) bool

type tokenKind int

const (
	tokEOF tokenKind = iota
	tokKey
	tokString
	tokNumber
	tokWord
	tokOp
	tokLParen
	tokRParen
	tokComma
)

type token struct {
	text string
	kind tokenKind
}

func lexFilter(s string) ([]token, error) {
	var out []token

	for i := 0; i < len(s); {
		t, n, err := lexOne(s, i)
		if err != nil {
			return nil, err
		}

		if t.kind != tokEOF {
			out = append(out, t)
		}

		i = n
	}

	return append(out, token{kind: tokEOF}), nil
}

// lexOne reads the token at i; whitespace yields a tokEOF placeholder that the caller skips.
func lexOne(s string, i int) (token, int, error) {
	c := s[i]

	if k, ok := punct(c); ok {
		return token{kind: k}, i + 1, nil
	}

	switch {
	case c == ':':
		return lexKey(s, i+1)
	case c == '"':
		return lexString(s, i)
	case strings.IndexByte("=!<>", c) >= 0:
		return lexOp(s, i)
	case c == '-' || (c >= '0' && c <= '9'):
		return lexRun(s, i, tokNumber, "0123456789.eE+-"), nextAfter(s, i, "0123456789.eE+-"), nil
	case isKeyStart(c):
		return lexRun(s, i, tokWord, ""), nextAfter(s, i, ""), nil
	}

	return token{}, 0, fmt.Errorf("%w: unexpected character %q", errFilterSyntax, c)
}

func punct(c byte) (tokenKind, bool) {
	switch c {
	case ' ', '\t', '\n', '\r':
		return tokEOF, true
	case '(':
		return tokLParen, true
	case ')':
		return tokRParen, true
	case ',':
		return tokComma, true
	}

	return 0, false
}

// nextAfter returns the index after the run starting at i; an empty set means key characters.
func nextAfter(s string, i int, set string) int {
	j := i + 1
	for j < len(s) && ((set == "" && isKeyChar(s[j])) || (set != "" && strings.IndexByte(set, s[j]) >= 0)) {
		j++
	}

	return j
}

func lexRun(s string, i int, kind tokenKind, set string) token {
	return token{kind: kind, text: s[i:nextAfter(s, i, set)]}
}

func isKeyStart(c byte) bool { return c == '_' || (c|0x20 >= 'a' && c|0x20 <= 'z') }

func isKeyChar(c byte) bool { return isKeyStart(c) || c == '-' || (c >= '0' && c <= '9') }

func lexKey(s string, i int) (token, int, error) {
	if i < len(s) && s[i] == '"' {
		t, n, err := lexString(s, i)

		return token{kind: tokKey, text: t.text}, n, err
	}

	j := i
	for j < len(s) && isKeyChar(s[j]) {
		j++
	}

	if j == i || !isKeyStart(s[i]) {
		return token{}, 0, fmt.Errorf("%w: attribute key must start with a letter or underscore", errFilterSyntax)
	}

	return token{kind: tokKey, text: s[i:j]}, j, nil
}

func lexString(s string, i int) (token, int, error) {
	var sb strings.Builder

	for j := i + 1; j < len(s); j++ {
		switch s[j] {
		case '\\':
			j++
			if j >= len(s) {
				return token{}, 0, fmt.Errorf("%w: unterminated string", errFilterSyntax)
			}

			sb.WriteByte(s[j])
		case '"':
			return token{kind: tokString, text: sb.String()}, j + 1, nil
		default:
			sb.WriteByte(s[j])
		}
	}

	return token{}, 0, fmt.Errorf("%w: unterminated string", errFilterSyntax)
}

func lexOp(s string, i int) (token, int, error) {
	if i+1 < len(s) && s[i+1] == '=' {
		return token{kind: tokOp, text: s[i : i+opPairLen]}, i + opPairLen, nil
	}

	if s[i] == '!' {
		return token{}, 0, fmt.Errorf("%w: '!' must be followed by '='", errFilterSyntax)
	}

	return token{kind: tokOp, text: s[i : i+1]}, i + 1, nil
}

type filterParser struct {
	toks []token
	pos  int
}

func (p *filterParser) peek() token { return p.toks[p.pos] }

func (p *filterParser) next() token {
	t := p.toks[p.pos]
	if t.kind != tokEOF {
		p.pos++
	}

	return t
}

func (p *filterParser) isWord(w string) bool {
	t := p.peek()

	return t.kind == tokWord && t.text == w
}

func (p *filterParser) expect(k tokenKind, what string) error {
	if p.next().kind != k {
		return fmt.Errorf("%w: expected %s", errFilterSyntax, what)
	}

	return nil
}

// compileFilter parses a consumer group filter; an empty filter matches every message.
func compileFilter(src string) (matcher, error) {
	if len(src) > maxFilterLen {
		return nil, fmt.Errorf("%w: a filter is limited to %d characters", errFilterSyntax, maxFilterLen)
	}

	if strings.TrimSpace(src) == "" {
		return func(map[string]string) bool { return true }, nil
	}

	toks, err := lexFilter(src)
	if err != nil {
		return nil, err
	}

	p := &filterParser{toks: toks}

	m, err := p.parseOr()
	if err != nil {
		return nil, err
	}

	if p.peek().kind != tokEOF {
		return nil, fmt.Errorf("%w: unexpected trailing input", errFilterSyntax)
	}

	return m, nil
}

func (p *filterParser) parseOr() (matcher, error) {
	l, err := p.parseAnd()
	if err != nil {
		return nil, err
	}

	for p.isWord("OR") {
		p.next()

		r, rerr := p.parseAnd()
		if rerr != nil {
			return nil, rerr
		}

		l = or(l, r)
	}

	return l, nil
}

func or(l, r matcher) matcher { return func(a map[string]string) bool { return l(a) || r(a) } }

func and(l, r matcher) matcher { return func(a map[string]string) bool { return l(a) && r(a) } }

func (p *filterParser) parseAnd() (matcher, error) {
	l, err := p.parseNot()
	if err != nil {
		return nil, err
	}

	for p.isWord("AND") {
		p.next()

		r, rerr := p.parseNot()
		if rerr != nil {
			return nil, rerr
		}

		l = and(l, r)
	}

	return l, nil
}

func (p *filterParser) parseNot() (matcher, error) {
	if p.isWord("NOT") {
		p.next()

		inner, err := p.parseNot()
		if err != nil {
			return nil, err
		}

		return func(a map[string]string) bool { return !inner(a) }, nil
	}

	return p.parsePrimary()
}

func (p *filterParser) parsePrimary() (matcher, error) {
	t := p.peek()

	switch {
	case t.kind == tokLParen:
		p.next()

		m, err := p.parseOr()
		if err != nil {
			return nil, err
		}

		return m, p.expect(tokRParen, "')'")
	case t.kind == tokWord && (t.text == "hasPrefix" || t.text == "hasSuffix"):
		return p.parseStringFunc()
	case t.kind == tokKey:
		p.next()

		if p.peek().kind != tokOp {
			key := t.text

			return func(a map[string]string) bool {
				_, ok := a[key]

				return ok
			}, nil
		}

		return p.parseComparison(t.text)
	}

	return nil, fmt.Errorf("%w: expected an attribute, function or '('", errFilterSyntax)
}

func (p *filterParser) parseStringFunc() (matcher, error) {
	fn := p.next().text

	if err := p.expect(tokLParen, "'('"); err != nil {
		return nil, err
	}

	k := p.next()
	if k.kind != tokKey {
		return nil, fmt.Errorf("%w: %s takes an attribute first", errFilterSyntax, fn)
	}

	if err := p.expect(tokComma, "','"); err != nil {
		return nil, err
	}

	lit := p.next()
	if lit.kind != tokString {
		return nil, fmt.Errorf("%w: %s takes a string second", errFilterSyntax, fn)
	}

	if err := p.expect(tokRParen, "')'"); err != nil {
		return nil, err
	}

	test := strings.HasPrefix
	if fn == "hasSuffix" {
		test = strings.HasSuffix
	}

	return func(a map[string]string) bool {
		v, ok := a[k.text]

		return ok && test(v, lit.text)
	}, nil
}

func (p *filterParser) parseComparison(key string) (matcher, error) {
	op := p.next().text
	lit := p.next()

	if lit.kind == tokString {
		if op != "=" && op != "!=" {
			return nil, fmt.Errorf("%w: strings only compare with = and !=", errFilterSyntax)
		}

		return func(a map[string]string) bool {
			v, ok := a[key]

			return ok && ((v == lit.text) == (op == "="))
		}, nil
	}

	if lit.kind != tokNumber {
		return nil, fmt.Errorf("%w: expected a string or number after %s", errFilterSyntax, op)
	}

	n, err := strconv.ParseFloat(lit.text, 64)
	if err != nil {
		return nil, fmt.Errorf("%w: bad number %q", errFilterSyntax, lit.text)
	}

	return numberMatcher(key, op, n), nil
}

func numberMatcher(key, op string, n float64) matcher {
	return func(a map[string]string) bool {
		raw, ok := a[key]
		if !ok {
			return false
		}

		v, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return op == "!="
		}

		switch op {
		case "=":
			return v == n
		case "!=":
			return v != n
		case ">":
			return v > n
		case ">=":
			return v >= n
		case "<":
			return v < n
		default:
			return v <= n
		}
	}
}
