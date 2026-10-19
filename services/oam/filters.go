package oam

import (
	"strings"
	"unicode"
)

const maxFilterLen = 2000

// validateFilter checks a LogGroupConfiguration or MetricConfiguration filter expression:
// clauses "<key> [NOT] IN ('a', 'b')" or "<key> [NOT] LIKE 'pattern'" joined by AND/OR.
func validateFilter(expr, key string) error {
	if expr == "" || len(expr) > maxFilterLen {
		return fail(ErrInvalidParameter, "Filter must be 1-%d characters", maxFilterLen)
	}

	p := &filterParser{toks: tokenizeFilter(expr), key: key}
	if !p.parse() {
		return fail(ErrInvalidParameter, "Filter %q is not a valid %s filter expression", expr, key)
	}

	return nil
}

type filterParser struct {
	key  string
	toks []string
	pos  int
}

func tokenizeFilter(s string) []string {
	var toks []string

	for i := 0; i < len(s); {
		c := rune(s[i])

		switch {
		case unicode.IsSpace(c):
			i++
		case c == '\'':
			j := strings.IndexByte(s[i+1:], '\'')
			if j < 0 {
				return append(toks, "'unterminated")
			}

			toks = append(toks, s[i:i+j+2])
			i += j + 2 //nolint:mnd // quote pair
		case c == '(' || c == ')' || c == ',':
			toks = append(toks, string(c))
			i++
		default:
			j := i
			for j < len(s) && !unicode.IsSpace(rune(s[j])) && !strings.ContainsRune("(),'", rune(s[j])) {
				j++
			}

			toks = append(toks, s[i:j])
			i = j
		}
	}

	return toks
}

func (p *filterParser) peek() string {
	if p.pos < len(p.toks) {
		return p.toks[p.pos]
	}

	return ""
}

func (p *filterParser) next() string {
	t := p.peek()
	p.pos++

	return t
}

func (p *filterParser) parse() bool {
	if !p.clause() {
		return false
	}

	for p.pos < len(p.toks) {
		if c := strings.ToUpper(p.next()); c != "AND" && c != "OR" {
			return false
		}

		if !p.clause() {
			return false
		}
	}

	return true
}

func isQuoted(t string) bool { return len(t) >= 2 && t[0] == '\'' && t[len(t)-1] == '\'' }

func (p *filterParser) clause() bool {
	if !strings.EqualFold(p.next(), p.key) {
		return false
	}

	op := strings.ToUpper(p.next())
	if op == "NOT" {
		op = strings.ToUpper(p.next())
		if op != "IN" && op != "LIKE" {
			return false
		}
	}

	switch op {
	case "LIKE":
		return isQuoted(p.next())
	case "IN":
		return p.list()
	default:
		return false
	}
}

func (p *filterParser) list() bool {
	if p.next() != "(" {
		return false
	}

	for {
		if !isQuoted(p.next()) {
			return false
		}

		switch p.next() {
		case ")":
			return true
		case ",":
		default:
			return false
		}
	}
}
