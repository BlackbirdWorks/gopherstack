package iot

import (
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

type queryEnv struct {
	values func(field string) ([]string, bool)
	bare   func(term string) bool
}

type queryNode interface {
	eval(env queryEnv) bool
}

type matchAllNode struct{}

func (matchAllNode) eval(queryEnv) bool { return true }

type (
	andNode  struct{ l, r queryNode }
	orNode   struct{ l, r queryNode }
	notNode  struct{ n queryNode }
	termNode struct {
		field string
		match func(v string) bool
		raw   string
		bare  bool
	}
)

func (n andNode) eval(e queryEnv) bool { return n.l.eval(e) && n.r.eval(e) }
func (n orNode) eval(e queryEnv) bool  { return n.l.eval(e) || n.r.eval(e) }
func (n notNode) eval(e queryEnv) bool { return !n.n.eval(e) }

func (n termNode) eval(e queryEnv) bool {
	if n.bare {
		return e.bare(n.raw)
	}

	vals, ok := e.values(n.field)

	return ok && slices.ContainsFunc(vals, n.match)
}

type queryParser struct {
	src string
	pos int
}

// compileQuery parses a fleet-indexing query (Lucene subset: terms, AND/OR/NOT, -, parentheses,
// quoted phrases, * and ? wildcards, [a TO b] ranges and >,>=,<,<= comparisons). Adjacent terms are
// ANDed. An empty query matches everything.
func compileQuery(q string) (queryNode, error) {
	if strings.TrimSpace(q) == "" {
		return matchAllNode{}, nil
	}

	p := &queryParser{src: q}

	n, err := p.parseOr("")
	if err != nil {
		return nil, err
	}

	p.skipSpace()

	if p.pos < len(p.src) {
		return nil, fmt.Errorf("%w: unexpected %q in query string", ErrInvalidQuery, p.src[p.pos:])
	}

	return n, nil
}

func (p *queryParser) skipSpace() {
	for p.pos < len(p.src) && p.src[p.pos] == ' ' {
		p.pos++
	}
}

func (p *queryParser) peekKeyword(kw string) bool {
	p.skipSpace()

	rest := p.src[p.pos:]
	if !strings.HasPrefix(rest, kw) {
		return false
	}

	end := len(kw)

	return end == len(rest) || rest[end] == ' ' || rest[end] == '('
}

func (p *queryParser) parseOr(defField string) (queryNode, error) {
	left, err := p.parseAnd(defField)
	if err != nil {
		return nil, err
	}

	for p.peekKeyword("OR") {
		p.pos += len("OR")

		right, rErr := p.parseAnd(defField)
		if rErr != nil {
			return nil, rErr
		}

		left = orNode{left, right}
	}

	return left, nil
}

func (p *queryParser) parseAnd(defField string) (queryNode, error) {
	left, err := p.parseUnary(defField)
	if err != nil {
		return nil, err
	}

	for {
		p.skipSpace()

		if p.pos >= len(p.src) || p.src[p.pos] == ')' || p.peekKeyword("OR") {
			return left, nil
		}

		if p.peekKeyword("AND") {
			p.pos += len("AND")
		}

		right, rErr := p.parseUnary(defField)
		if rErr != nil {
			return nil, rErr
		}

		left = andNode{left, right}
	}
}

func (p *queryParser) parseUnary(defField string) (queryNode, error) {
	p.skipSpace()

	switch {
	case p.peekKeyword("NOT"):
		p.pos += len("NOT")

		n, err := p.parseUnary(defField)
		if err != nil {
			return nil, err
		}

		return notNode{n}, nil
	case p.pos < len(p.src) && p.src[p.pos] == '-':
		p.pos++

		n, err := p.parseUnary(defField)
		if err != nil {
			return nil, err
		}

		return notNode{n}, nil
	}

	return p.parsePrimary(defField)
}

func (p *queryParser) parsePrimary(defField string) (queryNode, error) {
	p.skipSpace()

	if p.pos >= len(p.src) {
		return nil, fmt.Errorf("%w: unexpected end of query string", ErrInvalidQuery)
	}

	if p.src[p.pos] == '(' {
		p.pos++

		n, err := p.parseOr(defField)
		if err != nil {
			return nil, err
		}

		p.skipSpace()

		if p.pos >= len(p.src) || p.src[p.pos] != ')' {
			return nil, fmt.Errorf("%w: missing closing parenthesis", ErrInvalidQuery)
		}

		p.pos++

		return n, nil
	}

	word, err := p.readWord()
	if err != nil {
		return nil, err
	}

	field, value, fielded := defField, word, defField != ""

	if i := fieldSeparator(word); i > 0 {
		field, value, fielded = word[:i], word[i+1:], true

		if value == "" && p.pos < len(p.src) && p.src[p.pos] == '(' {
			return p.parseGroupedField(field)
		}
	}

	return buildTerm(field, value, fielded)
}

func (p *queryParser) parseGroupedField(field string) (queryNode, error) {
	p.pos++

	n, err := p.parseOr(field)
	if err != nil {
		return nil, err
	}

	p.skipSpace()

	if p.pos >= len(p.src) || p.src[p.pos] != ')' {
		return nil, fmt.Errorf("%w: missing closing parenthesis", ErrInvalidQuery)
	}

	p.pos++

	return n, nil
}

// fieldSeparator returns the index of the field/value ':' (outside quotes/brackets), or -1.
func fieldSeparator(word string) int {
	for i, r := range word {
		switch r {
		case '"', '[', '{':
			return -1
		case ':':
			return i
		}
	}

	return -1
}

func (p *queryParser) readWord() (string, error) {
	start := p.pos

	for p.pos < len(p.src) {
		switch c := p.src[p.pos]; c {
		case ' ', ')':
			return p.finishWord(start)
		case '(':
			if p.src[p.pos-1] == ':' {
				return p.finishWord(start)
			}

			return "", fmt.Errorf("%w: unexpected '('", ErrInvalidQuery)
		case '"', '[', '{':
			closers := map[byte]string{'"': `"`, '[': "]}", '{': "]}"}[c]

			end := strings.IndexAny(p.src[p.pos+1:], closers)
			if end < 0 {
				return "", fmt.Errorf("%w: unterminated %q", ErrInvalidQuery, string(c))
			}

			p.pos += end + len(`""`)
		default:
			p.pos++
		}
	}

	return p.finishWord(start)
}

func (p *queryParser) finishWord(start int) (string, error) {
	word := p.src[start:p.pos]
	if word == "" {
		return "", fmt.Errorf("%w: expected a search term", ErrInvalidQuery)
	}

	return word, nil
}

func buildTerm(field, value string, fielded bool) (queryNode, error) {
	if !fielded {
		return termNode{bare: true, raw: strings.Trim(value, `"`)}, nil
	}

	if value == "" {
		return nil, fmt.Errorf("%w: missing value for field %q", ErrInvalidQuery, field)
	}

	match, err := valueMatcher(value)
	if err != nil {
		return nil, err
	}

	return termNode{field: field, match: match}, nil
}

func valueMatcher(value string) (func(string) bool, error) {
	switch {
	case value[0] == '[' || value[0] == '{':
		return rangeMatcher(value)
	case strings.HasPrefix(value, ">=") || strings.HasPrefix(value, "<="):
		return cmpMatcher(value[:2], value[2:])
	case value[0] == '>' || value[0] == '<':
		return cmpMatcher(value[:1], value[1:])
	}

	value = strings.Trim(value, `"`)
	if !strings.ContainsAny(value, "*?") {
		return func(v string) bool { return strings.EqualFold(v, value) }, nil
	}

	re, err := regexp.Compile("(?is)^" + globToRegexp(value) + "$")
	if err != nil {
		return nil, fmt.Errorf("%w: bad wildcard %q", ErrInvalidQuery, value)
	}

	return re.MatchString, nil
}

func globToRegexp(glob string) string {
	var sb strings.Builder

	for _, r := range glob {
		switch r {
		case '*':
			sb.WriteString(".*")
		case '?':
			sb.WriteString(".")
		default:
			sb.WriteString(regexp.QuoteMeta(string(r)))
		}
	}

	return sb.String()
}

func compareValues(a, b string) int {
	fa, errA := strconv.ParseFloat(a, 64)
	fb, errB := strconv.ParseFloat(b, 64)

	if errA == nil && errB == nil {
		switch {
		case fa < fb:
			return -1
		case fa > fb:
			return 1
		}

		return 0
	}

	return strings.Compare(strings.ToLower(a), strings.ToLower(b))
}

func cmpMatcher(op, operand string) (func(string) bool, error) {
	operand = strings.Trim(operand, `"`)
	if operand == "" {
		return nil, fmt.Errorf("%w: missing operand for %q", ErrInvalidQuery, op)
	}

	return func(v string) bool {
		c := compareValues(v, operand)

		switch op {
		case ">":
			return c > 0
		case ">=":
			return c >= 0
		case "<":
			return c < 0
		default:
			return c <= 0
		}
	}, nil
}

func rangeMatcher(value string) (func(string) bool, error) {
	if len(value) < len("[a TO b]") {
		return nil, fmt.Errorf("%w: malformed range %q", ErrInvalidQuery, value)
	}

	lowerInclusive := value[0] == '['
	upperInclusive := value[len(value)-1] == ']'
	lo, hi, ok := strings.Cut(value[1:len(value)-1], " TO ")

	if !ok || strings.TrimSpace(lo) == "" || strings.TrimSpace(hi) == "" {
		return nil, fmt.Errorf("%w: malformed range %q", ErrInvalidQuery, value)
	}

	lo, hi = strings.TrimSpace(lo), strings.TrimSpace(hi)

	return func(v string) bool {
		if lo != "*" {
			if c := compareValues(v, lo); c < 0 || (c == 0 && !lowerInclusive) {
				return false
			}
		}

		if hi != "*" {
			if c := compareValues(v, hi); c > 0 || (c == 0 && !upperInclusive) {
				return false
			}
		}

		return true
	}, nil
}
