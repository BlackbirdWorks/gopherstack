package azuresearch

import (
	"regexp"
	"strconv"
	"strings"
	"unicode"
)

type occur int

const (
	occShould occur = iota
	occMust
	occMustNot
)

type qNode interface{ isNode() }

type clause struct {
	node qNode
	occ  occur
}

type qBool struct {
	clauses []clause
	boost   float64
}

type qTerm struct {
	field string
	text  string
	boost float64
	fuzzy int
}

type qPhrase struct {
	field string
	text  string
	slop  int
	boost float64
}

type qRegex struct {
	field string
	re    *regexp.Regexp
	boost float64
}

type qAll struct{}

func (*qBool) isNode()   {}
func (*qTerm) isNode()   {}
func (*qPhrase) isNode() {}
func (*qRegex) isNode()  {}
func (*qAll) isNode()    {}

type parser struct {
	src       []rune
	pos       int
	full      bool
	allMode   bool
	validate  func(field string) error
	maxDepth  int
	curDepth  int
	fieldHint string
}

const maxQueryDepth = 32

// parseQuery parses a simple or full Lucene query into a node tree.
func parseQuery(q string, full, allMode bool, validate func(string) error) (qNode, error) {
	q = strings.TrimSpace(q)
	if q == "" || q == "*" {
		return &qAll{}, nil
	}

	p := &parser{src: []rune(q), full: full, allMode: allMode, validate: validate}

	n, err := p.parseExpr("")
	if err != nil {
		return nil, err
	}

	if p.pos < len(p.src) {
		return nil, badf("The query is invalid: unexpected ')' at position %d.", p.pos)
	}

	return n, nil
}

func (p *parser) skipSpace() {
	for p.pos < len(p.src) && unicode.IsSpace(p.src[p.pos]) {
		p.pos++
	}
}

func (p *parser) peekWord() string {
	p.skipSpace()

	end := p.pos
	for end < len(p.src) && !unicode.IsSpace(p.src[end]) && !strings.ContainsRune("()\"", p.src[end]) {
		end++
	}

	return string(p.src[p.pos:end])
}

// operator consumes a binary operator when one is next, returning "and", "or" or "".
func (p *parser) operator() string {
	w := p.peekWord()

	switch {
	case p.full && (w == "AND" || w == "&&"), !p.full && w == "+":
		p.pos += len([]rune(w))

		return "and"
	case p.full && (w == "OR" || w == "||"), !p.full && w == "|":
		p.pos += len([]rune(w))

		return "or"
	}

	return ""
}

func (p *parser) parseExpr(field string) (qNode, error) {
	if p.curDepth++; p.curDepth > maxQueryDepth {
		return nil, badf("The query is invalid: nesting too deep.")
	}

	defer func() { p.curDepth-- }()

	out := &qBool{boost: 1}
	op := ""

	for {
		p.skipSpace()

		if p.pos >= len(p.src) || p.src[p.pos] == ')' {
			break
		}

		if o := p.operator(); o != "" {
			op = o

			continue
		}

		occ, node, err := p.parseClause(field)
		if err != nil {
			return nil, err
		}

		if node == nil {
			continue
		}

		out.add(node, occ, op, p.allMode)
		op = ""
	}

	return out, nil
}

func (b *qBool) add(node qNode, occ occur, op string, allMode bool) {
	if occ == occShould {
		switch {
		case op == "and" || (op == "" && allMode):
			occ = occMust
		}
	}

	if op == "and" && len(b.clauses) > 0 {
		last := &b.clauses[len(b.clauses)-1]
		if last.occ == occShould {
			last.occ = occMust
		}
	}

	b.clauses = append(b.clauses, clause{node: node, occ: occ})
}

func (p *parser) parseClause(field string) (occur, qNode, error) {
	occ := occShould

	for p.pos < len(p.src) {
		c := p.src[p.pos]

		switch {
		case c == '+' && p.pos+1 < len(p.src) && !unicode.IsSpace(p.src[p.pos+1]):
			occ = occMust
		case c == '-' && p.pos+1 < len(p.src) && !unicode.IsSpace(p.src[p.pos+1]):
			occ = occMustNot
		case c == '!' && p.full:
			occ = occMustNot
		default:
			if p.full && p.peekWord() == "NOT" {
				p.pos += len("NOT")
				occ = occMustNot
				p.skipSpace()

				continue
			}

			n, err := p.parsePrimary(field)

			return occ, n, err
		}

		p.pos++
	}

	return occ, nil, nil
}

func (p *parser) parsePrimary(field string) (qNode, error) {
	p.skipSpace()

	if p.pos >= len(p.src) {
		return nil, nil //nolint:nilnil // trailing operator, nothing to add
	}

	switch p.src[p.pos] {
	case '(':
		p.pos++

		n, err := p.parseExpr(field)
		if err != nil {
			return nil, err
		}

		if p.pos >= len(p.src) || p.src[p.pos] != ')' {
			return nil, badf("The query is invalid: missing closing ')'.")
		}

		p.pos++

		return p.suffixBoost(n), nil
	case '"':
		return p.parsePhrase(field)
	case '/':
		if p.full {
			return p.parseRegex(field)
		}
	}

	return p.parseWord(field)
}

func (p *parser) suffixBoost(n qNode) qNode {
	if b, ok := n.(*qBool); ok && p.pos < len(p.src) && p.src[p.pos] == '^' {
		p.pos++
		b.boost = p.number(1)
	}

	return n
}

func (p *parser) number(def float64) float64 {
	start := p.pos
	for p.pos < len(p.src) && (unicode.IsDigit(p.src[p.pos]) || p.src[p.pos] == '.') {
		p.pos++
	}

	if v, err := strconv.ParseFloat(string(p.src[start:p.pos]), 64); err == nil {
		return v
	}

	return def
}

func (p *parser) parsePhrase(field string) (qNode, error) {
	p.pos++

	var sb strings.Builder

	for p.pos < len(p.src) && p.src[p.pos] != '"' {
		if p.src[p.pos] == '\\' && p.pos+1 < len(p.src) {
			p.pos++
		}

		sb.WriteRune(p.src[p.pos])
		p.pos++
	}

	if p.pos >= len(p.src) {
		return nil, badf("The query is invalid: unterminated phrase.")
	}

	p.pos++

	n := &qPhrase{field: field, text: sb.String(), boost: 1}
	p.suffixes(&n.slop, &n.boost, true)

	return n, nil
}

func (p *parser) suffixes(num *int, boost *float64, slop bool) {
	for p.pos < len(p.src) {
		switch p.src[p.pos] {
		case '~':
			p.pos++
			*num = int(p.number(defaultFuzz(slop)))
		case '^':
			p.pos++
			*boost = p.number(1)
		default:
			return
		}
	}
}

func defaultFuzz(slop bool) float64 {
	if slop {
		return 0
	}

	return 2
}

func (p *parser) parseRegex(field string) (qNode, error) {
	p.pos++
	start := p.pos

	for p.pos < len(p.src) && p.src[p.pos] != '/' {
		p.pos++
	}

	if p.pos >= len(p.src) {
		return nil, badf("The query is invalid: unterminated regular expression.")
	}

	re, err := regexp.Compile("^(?:" + string(p.src[start:p.pos]) + ")$")
	if err != nil {
		return nil, badf("The query is invalid: bad regular expression.")
	}

	p.pos++

	return &qRegex{field: field, re: re, boost: 1}, nil
}

func (p *parser) parseWord(field string) (qNode, error) {
	var sb strings.Builder

	for p.pos < len(p.src) {
		c := p.src[p.pos]

		if unicode.IsSpace(c) || c == '(' || c == ')' || c == '"' || c == '~' || (c == '^' && p.full) {
			break
		}

		if c == '\\' && p.pos+1 < len(p.src) {
			p.pos++
			c = p.src[p.pos]
		} else if c == ':' && p.full && field == "" {
			return p.fieldQuery(sb.String())
		}

		sb.WriteRune(c)
		p.pos++
	}

	if sb.Len() == 0 {
		p.pos++

		return nil, nil //nolint:nilnil // stray punctuation is ignored
	}

	n := &qTerm{field: field, text: sb.String(), boost: 1, fuzzy: -1}
	fuzz := 0

	if p.pos < len(p.src) && p.src[p.pos] == '~' {
		n.fuzzy = 2
		p.suffixes(&fuzz, &n.boost, false)

		if fuzz < 2 {
			n.fuzzy = fuzz
		}
	} else {
		p.suffixes(&fuzz, &n.boost, false)
	}

	return n, nil
}

func (p *parser) fieldQuery(field string) (qNode, error) {
	p.pos++

	if p.validate != nil {
		if err := p.validate(field); err != nil {
			return nil, err
		}
	}

	return p.parsePrimary(field)
}
