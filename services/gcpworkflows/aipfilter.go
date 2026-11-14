package gcpworkflows

import (
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"
)

type valueKind int

const (
	kindString valueKind = iota
	kindEnum
	kindTime
	kindDuration
)

// fieldAccess reads the values of one filterable field; no values means the field is unset.
type fieldAccess[T any] struct {
	get   func(T) []string
	valid func(string) bool
	kind  valueKind
}

type fieldResolver[T any] func(field string) (fieldAccess[T], bool)

type filterTok struct {
	text   string
	quoted bool
}

type filterParser[T any] struct {
	resolve fieldResolver[T]
	toks    []filterTok
	pos     int
}

const (
	opChars   = ":=!<>"
	delimiter = "()\""
)

func readFilterQuoted(rs []rune, i int) (filterTok, int, error) {
	var sb strings.Builder

	for j := i + 1; j < len(rs); j++ {
		switch {
		case rs[j] == '"':
			return filterTok{text: sb.String(), quoted: true}, j + 1, nil
		case rs[j] == '\\' && j+1 < len(rs):
			j++

			sb.WriteRune(rs[j])
		default:
			sb.WriteRune(rs[j])
		}
	}

	return filterTok{}, 0, errInvalid("filter has an unterminated quoted string.")
}

func readFilterOp(rs []rune, i int) (string, int) {
	j := i + 1
	if j < len(rs) && rs[j] == '=' && rs[i] != '=' && rs[i] != ':' {
		j++
	}

	return string(rs[i:j]), j
}

func readFilterWord(rs []rune, i int, stops string) (filterTok, int) {
	j := i
	for j < len(rs) && !unicode.IsSpace(rs[j]) && !strings.ContainsRune(stops, rs[j]) {
		j++
	}

	return filterTok{text: string(rs[i:j])}, j
}

func isNegation(rs []rune, i int, afterOp bool) bool {
	return rs[i] == '-' && !afterOp && i+1 < len(rs) && !unicode.IsSpace(rs[i+1])
}

func tokenizeFilter(s string) ([]filterTok, error) {
	var toks []filterTok

	rs := []rune(s)
	afterOp := false

	for i := 0; i < len(rs); {
		r := rs[i]

		switch {
		case unicode.IsSpace(r):
			i++
		case r == '(' || r == ')' || isNegation(rs, i, afterOp):
			toks = append(toks, filterTok{text: string(r)})
			i++
			afterOp = false
		case r == '"':
			tok, next, err := readFilterQuoted(rs, i)
			if err != nil {
				return nil, err
			}

			toks, i, afterOp = append(toks, tok), next, false
		case strings.ContainsRune(opChars, r) && !afterOp:
			op, next := readFilterOp(rs, i)
			toks, i, afterOp = append(toks, filterTok{text: op}), next, true
		default:
			stops := delimiter + opChars
			if afterOp {
				stops = delimiter
			}

			tok, next := readFilterWord(rs, i, stops)
			if next == i {
				return nil, errInvalid("filter has an unexpected character %q.", string(r))
			}

			toks, i, afterOp = append(toks, tok), next, false
		}
	}

	return toks, nil
}

// parseFilter evaluates the AIP-160 subset the Workflows list calls document: restrictions of the form
// field OP value (OP one of = != < <= > >= :), AND, OR, NOT, a leading minus and parentheses. Adjacent
// restrictions are ANDed and OR binds tighter than AND.
func parseFilter[T any](s string, resolve fieldResolver[T]) (func(T) bool, error) {
	if strings.TrimSpace(s) == "" {
		return func(T) bool { return true }, nil
	}

	toks, err := tokenizeFilter(s)
	if err != nil {
		return nil, err
	}

	p := &filterParser[T]{resolve: resolve, toks: toks}

	pred, err := p.parseAnd()
	if err != nil {
		return nil, err
	}

	if p.pos < len(p.toks) {
		return nil, errInvalid("filter has an unexpected %q.", p.toks[p.pos].text)
	}

	return pred, nil
}

func (p *filterParser[T]) is(text string) bool {
	return p.pos < len(p.toks) && !p.toks[p.pos].quoted && p.toks[p.pos].text == text
}

func (p *filterParser[T]) take(text string) bool {
	if p.is(text) {
		p.pos++

		return true
	}

	return false
}

func (p *filterParser[T]) parseAnd() (func(T) bool, error) {
	var preds []func(T) bool

	for {
		next, err := p.parseOr()
		if err != nil {
			return nil, err
		}

		preds = append(preds, next)

		p.take("AND")

		if p.pos >= len(p.toks) || p.is(")") {
			break
		}
	}

	return func(v T) bool {
		return !slices.ContainsFunc(preds, func(f func(T) bool) bool { return !f(v) })
	}, nil
}

func (p *filterParser[T]) parseOr() (func(T) bool, error) {
	first, err := p.parseTerm()
	if err != nil {
		return nil, err
	}

	preds := []func(T) bool{first}

	for p.take("OR") {
		next, nerr := p.parseTerm()
		if nerr != nil {
			return nil, nerr
		}

		preds = append(preds, next)
	}

	return func(v T) bool {
		return slices.ContainsFunc(preds, func(f func(T) bool) bool { return f(v) })
	}, nil
}

func (p *filterParser[T]) parseTerm() (func(T) bool, error) {
	switch {
	case p.take("NOT"), p.take("-"):
		inner, err := p.parseTerm()
		if err != nil {
			return nil, err
		}

		return func(v T) bool { return !inner(v) }, nil
	case p.take("("):
		inner, err := p.parseAnd()
		if err != nil {
			return nil, err
		}

		if !p.take(")") {
			return nil, errInvalid("filter is missing a closing parenthesis.")
		}

		return inner, nil
	}

	return p.parseRestriction()
}

func (p *filterParser[T]) parseRestriction() (func(T) bool, error) {
	if p.pos+3 > len(p.toks) {
		return nil, errInvalid("filter restrictions must have the form field OP value.")
	}

	field, op, val := p.toks[p.pos], p.toks[p.pos+1], p.toks[p.pos+2]
	p.pos += 3

	if field.quoted || op.quoted || !strings.ContainsAny(op.text, opChars) {
		return nil, errInvalid("filter restrictions must have the form field OP value.")
	}

	access, ok := p.resolve(field.text)
	if !ok {
		return nil, errInvalid("filter field %q is not supported.", field.text)
	}

	return buildRestriction(access, field.text, op.text, val)
}

func buildRestriction[T any](
	access fieldAccess[T], field, op string, val filterTok,
) (func(T) bool, error) {
	if op == ":" && !val.quoted && val.text == "*" {
		return func(v T) bool { return len(access.get(v)) > 0 }, nil
	}

	test, err := valueTest(access, field, op, val.text)
	if err != nil {
		return nil, err
	}

	return func(v T) bool {
		got := access.get(v)
		if op == "!=" {
			return !slices.ContainsFunc(got, test)
		}

		return slices.ContainsFunc(got, test)
	}, nil
}

func valueTest[T any](access fieldAccess[T], field, op, want string) (func(string) bool, error) {
	switch access.kind {
	case kindTime:
		t, err := parseFilterTime(want)
		if err != nil {
			return nil, errInvalid("filter %s value %q is not an RFC 3339 timestamp or date.", field, want)
		}

		return orderedTest(op, field, func(got string) (int, bool) {
			g, gerr := time.Parse(time.RFC3339Nano, got)

			return g.Compare(t), gerr == nil
		})
	case kindDuration:
		d, err := parseFilterDuration(want)
		if err != nil {
			return nil, errInvalid("filter %s value %q is not a duration such as 1.5s.", field, want)
		}

		return orderedTest(op, field, func(got string) (int, bool) {
			g, gerr := time.ParseDuration(got)

			return int(g - d), gerr == nil
		})
	case kindEnum:
		if access.valid != nil && !access.valid(want) {
			return nil, errInvalid("filter %s value %q is not valid.", field, want)
		}

		if op != "=" && op != "!=" && op != ":" {
			return nil, errInvalid("filter field %q only supports = and !=.", field)
		}

		return func(got string) bool { return got == want }, nil
	default:
		return stringTest(field, op, want)
	}
}

func orderedTest(op, field string, cmpFn func(string) (int, bool)) (func(string) bool, error) {
	var ok func(int) bool

	switch op {
	case "=", ":", "!=":
		ok = func(c int) bool { return c == 0 }
	case "<":
		ok = func(c int) bool { return c < 0 }
	case "<=":
		ok = func(c int) bool { return c <= 0 }
	case ">":
		ok = func(c int) bool { return c > 0 }
	case ">=":
		ok = func(c int) bool { return c >= 0 }
	default:
		return nil, errInvalid("filter operator %q is not supported for %s.", op, field)
	}

	return func(got string) bool {
		c, valid := cmpFn(got)

		return valid && ok(c)
	}, nil
}

func stringTest(field, op, want string) (func(string) bool, error) {
	switch op {
	case "=", "!=":
		if strings.Contains(want, "*") {
			return func(got string) bool { return globMatch(want, got) }, nil
		}

		return func(got string) bool { return got == want }, nil
	case ":":
		return func(got string) bool { return strings.Contains(got, want) }, nil
	case "<", "<=", ">", ">=":
		return orderedTest(op, field, func(got string) (int, bool) { return strings.Compare(got, want), true })
	default:
		return nil, errInvalid("filter operator %q is not supported for %s.", op, field)
	}
}

func globMatch(pattern, s string) bool {
	parts := strings.Split(pattern, "*")

	if !strings.HasPrefix(s, parts[0]) {
		return false
	}

	s = s[len(parts[0]):]

	for _, mid := range parts[1 : len(parts)-1] {
		i := strings.Index(s, mid)
		if i < 0 {
			return false
		}

		s = s[i+len(mid):]
	}

	return strings.HasSuffix(s, parts[len(parts)-1])
}

func parseFilterTime(s string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}

	return time.Parse(time.DateOnly, s)
}

func parseFilterDuration(s string) (time.Duration, error) {
	if secs, ok := strings.CutSuffix(s, "s"); ok {
		if f, err := strconv.ParseFloat(secs, 64); err == nil {
			return time.Duration(f * float64(time.Second)), nil
		}
	}

	return time.ParseDuration(s)
}
