package gcpsecretmanager

import (
	"slices"
	"strings"
	"time"
	"unicode"
)

// item maps filter field names (e.g. "name", "labels.env") to their values; "*" holds every searchable value.
type item map[string][]string

type filterFn func(item) bool

type tokKind int

const (
	tokWord tokKind = iota
	tokString
	tokLParen
	tokRParen
	tokOp
)

type token struct {
	text string
	kind tokKind
}

const opChars = "=!<>:"

func readQuoted(rs []rune, start int) (string, int, error) {
	var sb strings.Builder

	for end := start + 1; end < len(rs); end++ {
		switch {
		case rs[end] == '"':
			return sb.String(), end + 1, nil
		case rs[end] == '\\' && end+1 < len(rs):
			end++
			sb.WriteRune(rs[end])
		default:
			sb.WriteRune(rs[end])
		}
	}

	return "", 0, errInvalid("Invalid filter: unterminated quoted string.")
}

func tokenize(s string) ([]token, error) {
	var (
		toks    []token
		rs      = []rune(s)
		afterOp bool
	)

	for i := 0; i < len(rs); {
		r := rs[i]

		switch {
		case unicode.IsSpace(r):
			i++
		case r == '(' || r == ')':
			kind := tokLParen
			if r == ')' {
				kind = tokRParen
			}

			toks = append(toks, token{kind: kind, text: string(r)})
			i++
		case r == '"':
			text, next, err := readQuoted(rs, i)
			if err != nil {
				return nil, err
			}

			toks = append(toks, token{kind: tokString, text: text})
			i, afterOp = next, false
		case !afterOp && strings.ContainsRune(opChars, r):
			op := readOp(rs, i)
			toks = append(toks, token{kind: tokOp, text: op})
			i, afterOp = i+len(op), true
		default:
			end := wordEnd(rs, i, afterOp)
			toks = append(toks, token{kind: tokWord, text: string(rs[i:end])})
			i, afterOp = end, false
		}
	}

	return toks, nil
}

func readOp(rs []rune, i int) string {
	if i+1 < len(rs) && rs[i+1] == '=' && rs[i] != '=' && rs[i] != ':' {
		return string(rs[i : i+2])
	}

	return string(rs[i])
}

// wordEnd returns the end of the bare word at i; operator characters end it unless the word is a value.
func wordEnd(rs []rune, i int, isValue bool) int {
	end := i
	for end < len(rs) && !unicode.IsSpace(rs[end]) && !strings.ContainsRune(`()"`, rs[end]) &&
		(isValue || !strings.ContainsRune(opChars, rs[end])) {
		end++
	}

	return end
}

type parser struct {
	toks []token
	pos  int
}

// compileFilter parses a Secret Manager list filter; an empty filter matches everything.
func compileFilter(s string) (filterFn, error) {
	if strings.TrimSpace(s) == "" {
		return func(item) bool { return true }, nil
	}

	toks, err := tokenize(s)
	if err != nil {
		return nil, err
	}

	p := &parser{toks: toks}

	fn, err := p.parseOr()
	if err != nil {
		return nil, err
	}

	if p.pos != len(p.toks) {
		return nil, errInvalid("Invalid filter: unexpected %q.", p.toks[p.pos].text)
	}

	return fn, nil
}

func (p *parser) peek() (token, bool) {
	if p.pos >= len(p.toks) {
		return token{}, false
	}

	return p.toks[p.pos], true
}

func (p *parser) keyword(kw string) bool {
	t, ok := p.peek()

	return ok && t.kind == tokWord && t.text == kw
}

func (p *parser) parseOr() (filterFn, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}

	for p.keyword("OR") {
		p.pos++

		right, rerr := p.parseAnd()
		if rerr != nil {
			return nil, rerr
		}

		l, r := left, right
		left = func(it item) bool { return l(it) || r(it) }
	}

	return left, nil
}

func (p *parser) parseAnd() (filterFn, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}

	for {
		t, ok := p.peek()
		if !ok || t.kind == tokRParen || p.keyword("OR") {
			return left, nil
		}

		if p.keyword("AND") {
			p.pos++
		}

		right, rerr := p.parseNot()
		if rerr != nil {
			return nil, rerr
		}

		l, r := left, right
		left = func(it item) bool { return l(it) && r(it) }
	}
}

func (p *parser) parseNot() (filterFn, error) {
	if p.keyword("NOT") {
		p.pos++

		inner, err := p.parseNot()
		if err != nil {
			return nil, err
		}

		return func(it item) bool { return !inner(it) }, nil
	}

	return p.parsePrimary()
}

func (p *parser) parsePrimary() (filterFn, error) {
	t, ok := p.peek()
	if !ok {
		return nil, errInvalid("Invalid filter: unexpected end of expression.")
	}

	p.pos++

	switch t.kind {
	case tokLParen:
		inner, err := p.parseOr()
		if err != nil {
			return nil, err
		}

		if c, more := p.peek(); !more || c.kind != tokRParen {
			return nil, errInvalid("Invalid filter: missing closing parenthesis.")
		}

		p.pos++

		return inner, nil
	case tokWord, tokString:
		return p.parseTerm(t)
	default:
		return nil, errInvalid("Invalid filter: unexpected %q.", t.text)
	}
}

func (p *parser) parseTerm(t token) (filterFn, error) {
	field, negate := t.text, false
	if t.kind == tokWord && len(field) > 1 && field[0] == '-' {
		field, negate = field[1:], true
	}

	var fn filterFn

	if o, ok := p.peek(); ok && o.kind == tokOp && t.kind == tokWord {
		p.pos++

		v, vok := p.peek()
		if !vok || (v.kind != tokWord && v.kind != tokString) {
			return nil, errInvalid("Invalid filter: missing value after %q.", o.text)
		}

		p.pos++
		fn = comparison(field, o.text, v.text)
	} else {
		needle := strings.ToLower(field)
		fn = func(it item) bool { return anyContains(it["*"], needle) }
	}

	if negate {
		inner := fn

		return func(it item) bool { return !inner(it) }, nil
	}

	return fn, nil
}

func anyContains(vals []string, lowerNeedle string) bool {
	for _, v := range vals {
		if strings.Contains(strings.ToLower(v), lowerNeedle) {
			return true
		}
	}

	return false
}

func comparison(field, op, want string) filterFn {
	return func(it item) bool {
		vals := it[field]

		switch op {
		case ":":
			if want == "*" {
				return len(vals) > 0
			}

			return anyContains(vals, strings.ToLower(want))
		case "=":
			return anyMatch(vals, func(v string) bool { return v == want })
		case "!=":
			return !anyMatch(vals, func(v string) bool { return v == want })
		default:
			return anyMatch(vals, func(v string) bool { return ordered(op, v, want) })
		}
	}
}

func anyMatch(vals []string, f func(string) bool) bool {
	return slices.ContainsFunc(vals, f)
}

func parseFilterTime(s string) (time.Time, bool) {
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t, true
		}
	}

	return time.Time{}, false
}

func ordered(op, have, want string) bool {
	cmp := strings.Compare(have, want)

	if ht, ok := parseFilterTime(have); ok {
		if wt, wok := parseFilterTime(want); wok {
			cmp = ht.Compare(wt)
		}
	}

	switch op {
	case "<":
		return cmp < 0
	case "<=":
		return cmp <= 0
	case ">":
		return cmp > 0
	case ">=":
		return cmp >= 0
	}

	return false
}
