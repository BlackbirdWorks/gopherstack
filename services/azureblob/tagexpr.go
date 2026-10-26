package azureblob

import (
	"errors"
	"strings"
)

var errBadTagExpr = errors.New("azureblob: invalid tag expression")

// containerKey is the pseudo tag name that scopes Find Blobs by Tags.
const containerKey = "@container"

type tokKind int

const (
	tokEOF tokKind = iota
	tokName
	tokString
	tokOp
	tokLParen
	tokRParen
	tokAnd
	tokOr
)

type token struct {
	text string
	kind tokKind
}

func lexTagExpr(s string) ([]token, error) {
	var toks []token

	for i := 0; i < len(s); {
		if isSpace(s[i]) {
			i++

			continue
		}

		tok, next, ok := lexNext(s, i)
		if !ok {
			return nil, errBadTagExpr
		}

		toks = append(toks, tok)
		i = next
	}

	return toks, nil
}

func isSpace(ch byte) bool { return ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n' }

// lexNext reads the token starting at s[i].
func lexNext(s string, i int) (token, int, bool) {
	switch ch := s[i]; ch {
	case '(':
		return token{kind: tokLParen}, i + 1, true
	case ')':
		return token{kind: tokRParen}, i + 1, true
	case '"', '\'':
		text, next, ok := lexQuoted(s, i)
		kind := tokName
		if ch == '\'' {
			kind = tokString
		}

		return token{kind: kind, text: text}, next, ok
	case '=', '<', '>':
		op := string(ch)
		if i+1 < len(s) && (s[i+1] == '=' || (ch == '<' && s[i+1] == '>')) {
			op = s[i : i+2]
		}

		return token{kind: tokOp, text: op}, i + len(op), true
	default:
		word, next := lexWord(s, i)

		return wordToken(word), next, word != ""
	}
}

// lexQuoted reads a quoted run starting at s[i]; a doubled quote escapes it.
func lexQuoted(s string, i int) (string, int, bool) {
	q := s[i]

	var sb strings.Builder

	for j := i + 1; j < len(s); j++ {
		if s[j] != q {
			sb.WriteByte(s[j])

			continue
		}

		if j+1 < len(s) && s[j+1] == q {
			sb.WriteByte(q)

			j++

			continue
		}

		return sb.String(), j + 1, true
	}

	return "", 0, false
}

func lexWord(s string, i int) (string, int) {
	j := i
	for j < len(s) {
		c := s[j]
		if c == '_' || c == '@' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' {
			j++

			continue
		}

		break
	}

	return s[i:j], j
}

func wordToken(w string) token {
	switch strings.ToUpper(w) {
	case "AND":
		return token{kind: tokAnd}
	case "OR":
		return token{kind: tokOr}
	default:
		return token{kind: tokName, text: w}
	}
}

// tagNode is a parsed predicate: a comparison leaf (op set) or a logical
// AND/OR over left and right.
type tagNode struct {
	left, right *tagNode
	key, op     string
	val         string
	and         bool
}

func (n *tagNode) eval(tags map[string]string) bool {
	if n.op == "" {
		if n.and {
			return n.left.eval(tags) && n.right.eval(tags)
		}

		return n.left.eval(tags) || n.right.eval(tags)
	}

	got, ok := tags[n.key]
	if !ok {
		return false
	}

	switch n.op {
	case "=":
		return got == n.val
	case "<>":
		return got != n.val
	case ">":
		return got > n.val
	case ">=":
		return got >= n.val
	case "<":
		return got < n.val
	default:
		return got <= n.val
	}
}

type tagParser struct {
	toks     []token
	pos      int
	logicOps int
}

func (p *tagParser) peek() token {
	if p.pos < len(p.toks) {
		return p.toks[p.pos]
	}

	return token{kind: tokEOF}
}

func (p *tagParser) parseOr() (*tagNode, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}

	for p.peek().kind == tokOr {
		p.pos++
		p.logicOps++

		right, rerr := p.parseAnd()
		if rerr != nil {
			return nil, rerr
		}

		left = &tagNode{left: left, right: right}
	}

	return left, nil
}

func (p *tagParser) parseAnd() (*tagNode, error) {
	left, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}

	for p.peek().kind == tokAnd {
		p.pos++
		p.logicOps++

		right, rerr := p.parsePrimary()
		if rerr != nil {
			return nil, rerr
		}

		left = &tagNode{left: left, right: right, and: true}
	}

	return left, nil
}

func (p *tagParser) parsePrimary() (*tagNode, error) {
	if p.peek().kind == tokLParen {
		p.pos++

		n, err := p.parseOr()
		if err != nil {
			return nil, err
		}

		if p.peek().kind != tokRParen {
			return nil, errBadTagExpr
		}

		p.pos++

		return n, nil
	}

	if p.pos+3 > len(p.toks) {
		return nil, errBadTagExpr
	}

	name, op, val := p.toks[p.pos], p.toks[p.pos+1], p.toks[p.pos+2]
	if name.kind != tokName || op.kind != tokOp || val.kind != tokString {
		return nil, errBadTagExpr
	}

	p.pos += 3

	return &tagNode{key: name.text, op: op.text, val: val.text}, nil
}

// parseTagCondition parses an x-ms-if-tags predicate: comparisons joined by
// AND/OR with parentheses, at most 10 logical operations.
func parseTagCondition(expr string) (*tagNode, error) {
	toks, err := lexTagExpr(expr)
	if err != nil || len(toks) == 0 {
		return nil, errBadTagExpr
	}

	p := &tagParser{toks: toks}

	n, err := p.parseOr()
	if err != nil || p.pos != len(toks) || p.logicOps > maxTagPredicate {
		return nil, errBadTagExpr
	}

	return n, nil
}

// tagFilter is a parsed Find Blobs by Tags expression.
type tagFilter struct {
	container string
	clauses   []*tagNode
}

func (f *tagFilter) match(tags map[string]string) bool {
	for _, c := range f.clauses {
		if !c.eval(tags) {
			return false
		}
	}

	return len(f.clauses) > 0
}

type filterBuilder struct {
	f             *tagFilter
	sides         map[string]bool
	allowIn       bool
	seenContainer bool
}

func (b *filterBuilder) add(name, op, val string) error {
	if name == containerKey {
		if !b.allowIn || b.seenContainer || op != "=" {
			return errBadTagExpr
		}

		b.seenContainer = true
		b.f.container = val

		return nil
	}

	if side := rangeSide(op); side != "" {
		if b.sides[name+side] {
			return errBadTagExpr
		}

		b.sides[name+side] = true
	}

	b.f.clauses = append(b.f.clauses, &tagNode{key: name, op: op, val: val})

	return nil
}

// parseTagFilter parses a Find Blobs by Tags expression: AND-joined
// =, >, >=, <, <= comparisons plus an optional @container = '<name>'. Same-sided
// range operators on one key are invalid. allowContainer is false when the
// container is part of the URI.
func parseTagFilter(expr string, allowContainer bool) (*tagFilter, error) {
	toks, err := lexTagExpr(expr)
	if err != nil || len(toks) == 0 {
		return nil, errBadTagExpr
	}

	b := &filterBuilder{f: &tagFilter{}, sides: make(map[string]bool), allowIn: allowContainer}

	for pos := 0; ; pos++ {
		if pos+3 > len(toks) {
			return nil, errBadTagExpr
		}

		name, op, val := toks[pos], toks[pos+1], toks[pos+2]
		if name.kind != tokName || op.kind != tokOp || val.kind != tokString || op.text == "<>" {
			return nil, errBadTagExpr
		}

		if err = b.add(name.text, op.text, val.text); err != nil {
			return nil, err
		}

		pos += 3
		if pos == len(toks) {
			break
		}

		if toks[pos].kind != tokAnd {
			return nil, errBadTagExpr
		}
	}

	if len(b.f.clauses) == 0 {
		return nil, errBadTagExpr
	}

	return b.f, nil
}

func rangeSide(op string) string {
	switch op {
	case ">", ">=":
		return "lo"
	case "<", "<=":
		return "hi"
	default:
		return ""
	}
}
