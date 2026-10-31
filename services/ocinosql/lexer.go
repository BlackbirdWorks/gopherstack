package ocinosql

import (
	"strings"
	"unicode"
)

const twoRunes = 2

type tokKind int

const (
	tkEOF tokKind = iota
	tkIdent
	tkNumber
	tkString
	tkVar
	tkPunct
)

type tok struct {
	text string
	up   string
	kind tokKind
}

// lex splits a NoSQL SQL or DDL statement into tokens.
func lex(s string) ([]tok, *APIError) {
	var out []tok

	rs := []rune(s)

	for i := 0; i < len(rs); {
		t, n, err := lexOne(rs, i)
		if err != nil {
			return nil, err
		}

		if t != nil {
			out = append(out, *t)
		}

		i = n
	}

	return append(out, tok{kind: tkEOF}), nil
}

func lexOne(rs []rune, i int) (*tok, int, *APIError) {
	c := rs[i]

	switch {
	case unicode.IsSpace(c):
		return nil, i + 1, nil
	case c == '\'':
		text, n, ok := lexString(rs, i)
		if !ok {
			return nil, 0, errParam("syntax error: unterminated string literal")
		}

		return &tok{kind: tkString, text: text}, n, nil
	case c == '"':
		end := indexRune(rs, i+1, '"')
		if end < 0 {
			return nil, 0, errParam("syntax error: unterminated quoted identifier")
		}

		t := string(rs[i+1 : end])

		return &tok{kind: tkIdent, text: t, up: strings.ToUpper(t)}, end + 1, nil
	case unicode.IsDigit(c):
		j := lexNumberEnd(rs, i)

		return &tok{kind: tkNumber, text: string(rs[i:j])}, j, nil
	case c == '$' || c == '_' || unicode.IsLetter(c):
		return lexWord(rs, i)
	}

	t, n, ok := lexPunct(rs, i)
	if !ok {
		return nil, 0, errParam("syntax error: unexpected character '" + string(c) + "'")
	}

	return &tok{kind: tkPunct, text: t, up: t}, n, nil
}

func lexWord(rs []rune, i int) (*tok, int, *APIError) {
	j := i + 1
	for j < len(rs) && (unicode.IsLetter(rs[j]) || unicode.IsDigit(rs[j]) || rs[j] == '_') {
		j++
	}

	t := string(rs[i:j])

	kind := tkIdent
	if rs[i] == '$' {
		kind = tkVar
	}

	return &tok{kind: kind, text: t, up: strings.ToUpper(t)}, j, nil
}

func lexNumberEnd(rs []rune, i int) int {
	j := i
	for j < len(rs) {
		c := rs[j]
		signAfterExp := (c == '-' || c == '+') && (rs[j-1] == 'e' || rs[j-1] == 'E')

		if !unicode.IsDigit(c) && c != '.' && c != 'e' && c != 'E' && !signAfterExp {
			break
		}

		j++
	}

	return j
}

func indexRune(rs []rune, from int, r rune) int {
	for i := from; i < len(rs); i++ {
		if rs[i] == r {
			return i
		}
	}

	return -1
}

func lexString(rs []rune, start int) (string, int, bool) {
	var sb strings.Builder

	for i := start + 1; i < len(rs); i++ {
		if rs[i] != '\'' {
			sb.WriteRune(rs[i])

			continue
		}

		if i+1 < len(rs) && rs[i+1] == '\'' {
			sb.WriteRune('\'')

			i++

			continue
		}

		return sb.String(), i + 1, true
	}

	return "", 0, false
}

func lexPunct(rs []rune, i int) (string, int, bool) {
	if i+1 < len(rs) {
		two := string(rs[i : i+twoRunes])
		switch two {
		case "<=", ">=", "<>", "!=":
			return two, i + twoRunes, true
		}
	}

	if strings.ContainsRune("(),.;=<>*[]:+-", rs[i]) {
		return string(rs[i]), i + 1, true
	}

	return "", 0, false
}

type parser struct {
	toks []tok
	pos  int
}

func newParser(sql string) (*parser, *APIError) {
	toks, err := lex(sql)
	if err != nil {
		return nil, err
	}

	return &parser{toks: toks}, nil
}

func (p *parser) peek() tok { return p.toks[p.pos] }

func (p *parser) next() tok {
	t := p.toks[p.pos]
	if t.kind != tkEOF {
		p.pos++
	}

	return t
}

func (p *parser) atEOF() bool {
	for p.peek().up == ";" {
		p.pos++
	}

	return p.peek().kind == tkEOF
}

func (p *parser) isKw(kw string) bool {
	t := p.peek()

	return t.kind == tkIdent && t.up == kw
}

func (p *parser) isPunct(s string) bool {
	t := p.peek()

	return t.kind == tkPunct && t.text == s
}

func (p *parser) acceptKw(kw string) bool {
	if p.isKw(kw) {
		p.pos++

		return true
	}

	return false
}

func (p *parser) acceptPunct(s string) bool {
	if p.isPunct(s) {
		p.pos++

		return true
	}

	return false
}

func (p *parser) fail(want string) *APIError {
	t := p.peek()
	got := "end of statement"

	if t.kind != tkEOF {
		got = "'" + t.text + "'"
	}

	return errParam("syntax error: expected " + want + " but found " + got)
}

func (p *parser) expectKw(kw string) *APIError {
	if !p.acceptKw(kw) {
		return p.fail(kw)
	}

	return nil
}

func (p *parser) expectPunct(s string) *APIError {
	if !p.acceptPunct(s) {
		return p.fail("'" + s + "'")
	}

	return nil
}

func (p *parser) ident(what string) (string, *APIError) {
	t := p.peek()
	if t.kind != tkIdent {
		return "", p.fail(what)
	}

	p.pos++

	return t.text, nil
}
