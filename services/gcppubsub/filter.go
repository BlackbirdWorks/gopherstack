package gcppubsub

import (
	"strings"
)

const maxFilterBytes = 256

// filterExpr is a compiled subscription filter evaluated against message attributes.
type filterExpr interface {
	eval(attrs map[string]string) bool
}

type (
	andExpr    struct{ l, r filterExpr }
	orExpr     struct{ l, r filterExpr }
	notExpr    struct{ x filterExpr }
	hasExpr    struct{ key string }
	cmpExpr    struct{ key, val string }
	neqExpr    struct{ key, val string }
	prefixExpr struct{ key, prefix string }
)

func (e andExpr) eval(a map[string]string) bool { return e.l.eval(a) && e.r.eval(a) }
func (e orExpr) eval(a map[string]string) bool  { return e.l.eval(a) || e.r.eval(a) }
func (e notExpr) eval(a map[string]string) bool { return !e.x.eval(a) }

func (e hasExpr) eval(a map[string]string) bool {
	_, ok := a[e.key]

	return ok
}

func (e cmpExpr) eval(a map[string]string) bool {
	v, ok := a[e.key]

	return ok && v == e.val
}

func (e neqExpr) eval(a map[string]string) bool {
	v, ok := a[e.key]

	return !ok || v != e.val
}

func (e prefixExpr) eval(a map[string]string) bool {
	v, ok := a[e.key]

	return ok && strings.HasPrefix(v, e.prefix)
}

type tokKind int

const (
	tokEOF tokKind = iota
	tokIdent
	tokString
	tokPunct
)

type token struct {
	text string
	kind tokKind
}

func tokenizeFilter(s string) ([]token, error) {
	var toks []token

	for i := 0; i < len(s); {
		if strings.IndexByte(" \t\n\r", s[i]) >= 0 {
			i++

			continue
		}

		tok, next, err := lexToken(s, i)
		if err != nil {
			return nil, err
		}

		toks = append(toks, tok)
		i = next
	}

	return append(toks, token{kind: tokEOF}), nil
}

// lexToken reads the token starting at s[i] and returns it with the index after it.
func lexToken(s string, i int) (token, int, error) {
	c := s[i]

	switch {
	case c == '"':
		end := strings.IndexByte(s[i+1:], '"')
		if end < 0 {
			return token{}, 0, errInvalid("invalid filter: unterminated string literal")
		}

		return token{kind: tokString, text: s[i+1 : i+1+end]}, i + 1 + end + 1, nil
	case c == '!' && i+1 < len(s) && s[i+1] == '=':
		return token{kind: tokPunct, text: "!="}, i + len("!="), nil
	case strings.IndexByte("()=:,.", c) >= 0:
		return token{kind: tokPunct, text: string(c)}, i + 1, nil
	case isIdentByte(c):
		j := i
		for j < len(s) && isIdentByte(s[j]) {
			j++
		}

		return token{kind: tokIdent, text: s[i:j]}, j, nil
	default:
		return token{}, 0, errInvalid("invalid filter: unexpected character %q", c)
	}
}

func isIdentByte(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-'
}

type filterParser struct {
	toks []token
	pos  int
}

// compileFilter parses a Pub/Sub subscription filter; an empty filter matches everything.
func compileFilter(s string) (filterExpr, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil //nolint:nilnil // nil expression means "match all"
	}

	if len(s) > maxFilterBytes {
		return nil, errInvalid("filter exceeds the maximum length of %d bytes", maxFilterBytes)
	}

	toks, err := tokenizeFilter(s)
	if err != nil {
		return nil, err
	}

	p := &filterParser{toks: toks}

	expr, err := p.parseOr()
	if err != nil {
		return nil, err
	}

	if p.peek().kind != tokEOF {
		return nil, errInvalid("invalid filter: unexpected %q", p.peek().text)
	}

	return expr, nil
}

func (p *filterParser) peek() token { return p.toks[p.pos] }

func (p *filterParser) next() token {
	t := p.toks[p.pos]
	if t.kind != tokEOF {
		p.pos++
	}

	return t
}

func (p *filterParser) isKeyword(kw string) bool {
	t := p.peek()

	return t.kind == tokIdent && t.text == kw
}

func (p *filterParser) expectPunct(s string) error {
	if t := p.next(); t.kind != tokPunct || t.text != s {
		return errInvalid("invalid filter: expected %q", s)
	}

	return nil
}

func (p *filterParser) parseOr() (filterExpr, error) {
	l, err := p.parseAnd()
	if err != nil {
		return nil, err
	}

	for p.isKeyword("OR") {
		p.next()

		r, rerr := p.parseAnd()
		if rerr != nil {
			return nil, rerr
		}

		l = orExpr{l, r}
	}

	return l, nil
}

func (p *filterParser) parseAnd() (filterExpr, error) {
	l, err := p.parseNot()
	if err != nil {
		return nil, err
	}

	for p.isKeyword("AND") {
		p.next()

		r, rerr := p.parseNot()
		if rerr != nil {
			return nil, rerr
		}

		l = andExpr{l, r}
	}

	return l, nil
}

func (p *filterParser) parseNot() (filterExpr, error) {
	if p.isKeyword("NOT") {
		p.next()

		x, err := p.parseNot()
		if err != nil {
			return nil, err
		}

		return notExpr{x}, nil
	}

	return p.parsePrimary()
}

func (p *filterParser) parsePrimary() (filterExpr, error) {
	t := p.peek()

	switch {
	case t.kind == tokPunct && t.text == "(":
		p.next()

		x, err := p.parseOr()
		if err != nil {
			return nil, err
		}

		if err = p.expectPunct(")"); err != nil {
			return nil, err
		}

		return x, nil
	case t.kind == tokIdent && t.text == "hasPrefix":
		return p.parseHasPrefix()
	case t.kind == tokIdent && t.text == "attributes":
		return p.parseAttrTest()
	default:
		return nil, errInvalid("invalid filter: unexpected %q", t.text)
	}
}

// parseAttrKey parses "attributes" followed by ".name" or ":name" and returns the key and separator.
func (p *filterParser) parseAttrKey() (string, string, error) {
	p.next()

	s := p.next()
	if s.kind != tokPunct || (s.text != "." && s.text != ":") {
		return "", "", errInvalid("invalid filter: expected '.' or ':' after attributes")
	}

	k := p.next()
	if k.kind != tokIdent && k.kind != tokString {
		return "", "", errInvalid("invalid filter: expected attribute name")
	}

	return k.text, s.text, nil
}

func (p *filterParser) parseAttrTest() (filterExpr, error) {
	key, sep, err := p.parseAttrKey()
	if err != nil {
		return nil, err
	}

	if sep == ":" {
		return hasExpr{key}, nil
	}

	op := p.next()
	if op.kind != tokPunct || (op.text != "=" && op.text != "!=") {
		return nil, errInvalid("invalid filter: expected '=' or '!=' after attributes.%s", key)
	}

	v := p.next()
	if v.kind != tokString {
		return nil, errInvalid("invalid filter: attribute value must be a quoted string")
	}

	if op.text == "=" {
		return cmpExpr{key, v.text}, nil
	}

	return neqExpr{key, v.text}, nil
}

func (p *filterParser) parseHasPrefix() (filterExpr, error) {
	p.next()

	if err := p.expectPunct("("); err != nil {
		return nil, err
	}

	if !p.isKeyword("attributes") {
		return nil, errInvalid("invalid filter: hasPrefix requires an attributes.name argument")
	}

	key, sep, err := p.parseAttrKey()
	if err != nil {
		return nil, err
	}

	if sep != "." {
		return nil, errInvalid("invalid filter: hasPrefix requires attributes.name")
	}

	if err = p.expectPunct(","); err != nil {
		return nil, err
	}

	v := p.next()
	if v.kind != tokString {
		return nil, errInvalid("invalid filter: hasPrefix prefix must be a quoted string")
	}

	if err = p.expectPunct(")"); err != nil {
		return nil, err
	}

	return prefixExpr{key, v.text}, nil
}
