package cloudwatchlogs

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// tokKind enumerates expression token categories.
type tokKind int

const (
	tkIdent tokKind = iota
	tkNumber
	tkString
	tkRegex
	tkOp
	tkLParen
	tkRParen
	tkComma
	tkKeyword
)

// exprToken is a single lexical token.
type exprToken struct {
	text string
	kind tokKind
}

// keywordSet returns the reserved words recognised by the expression parser.
func keywordSet() map[string]struct{} {
	return map[string]struct{}{
		"and": {}, "or": {}, "not": {}, "in": {}, "like": {},
	}
}

// isIdentStart reports whether ch can begin an identifier.
func isIdentStart(ch byte) bool {
	return ch == '@' || ch == '_' || ch == '$' ||
		(ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}

// isIdentPart reports whether ch can continue an identifier (fields may contain
// dots for JSON paths, @ for system fields, and brackets for array indexes).
func isIdentPart(ch byte) bool {
	return isIdentStart(ch) || (ch >= '0' && ch <= '9') ||
		ch == '.' || ch == '[' || ch == ']' || ch == '-'
}

// tokenizeExpr lexes an expression string into tokens.
func tokenizeExpr(s string) ([]exprToken, error) {
	var toks []exprToken
	i := 0

	for i < len(s) {
		if isExprSpace(s[i]) {
			i++

			continue
		}

		tok, next, err := lexExprToken(s, i)
		if err != nil {
			return nil, err
		}

		toks = append(toks, tok)
		i = next
	}

	return toks, nil
}

// isExprSpace reports whether ch is expression whitespace.
func isExprSpace(ch byte) bool {
	return ch == ' ' || ch == '\t' || ch == '\n'
}

// lexExprToken lexes the single token starting at index i.
func lexExprToken(s string, i int) (exprToken, int, error) {
	ch := s[i]
	switch {
	case ch == '(':
		return exprToken{text: "(", kind: tkLParen}, i + 1, nil
	case ch == ')':
		return exprToken{text: ")", kind: tkRParen}, i + 1, nil
	case ch == ',':
		return exprToken{text: ",", kind: tkComma}, i + 1, nil
	case ch == '"' || ch == '\'':
		return lexString(s, i)
	case ch == '/':
		return lexRegex(s, i)
	case isOpStart(ch):
		tok, next := lexOperator(s, i)

		return tok, next, nil
	case ch >= '0' && ch <= '9':
		tok, next := lexNumber(s, i)

		return tok, next, nil
	case isIdentStart(ch):
		tok, next := lexIdent(s, i)

		return tok, next, nil
	default:
		return exprToken{}, 0, fmt.Errorf("%w: unexpected character %q", ErrParseExpr, string(ch))
	}
}

// lexString lexes a single- or double-quoted string literal.
func lexString(s string, start int) (exprToken, int, error) {
	quote := s[start]
	var sb strings.Builder

	for i := start + 1; i < len(s); i++ {
		ch := s[i]
		if ch == '\\' && i+1 < len(s) {
			sb.WriteByte(s[i+1])
			i++

			continue
		}

		if ch == quote {
			return exprToken{text: sb.String(), kind: tkString}, i + 1, nil
		}

		sb.WriteByte(ch)
	}

	return exprToken{}, 0, fmt.Errorf("%w: unterminated string", ErrParseExpr)
}

// lexRegex lexes a /pattern/ regex literal.
func lexRegex(s string, start int) (exprToken, int, error) {
	var sb strings.Builder
	var rs regexState

	for i := start + 1; i < len(s); i++ {
		ch := s[i]

		var ended bool
		rs, ended = advanceRegex(ch, rs)
		if ended {
			return exprToken{text: sb.String(), kind: tkRegex}, i + 1, nil
		}

		sb.WriteByte(ch)
	}

	return exprToken{}, 0, fmt.Errorf("%w: unterminated regex", ErrParseExpr)
}

// isOpStart reports whether ch begins an operator token.
func isOpStart(ch byte) bool {
	switch ch {
	case '=', '!', '<', '>', '+', '-', '*', '/', '%':
		return true
	default:
		return false
	}
}

// lexOperator lexes a one- or two-character operator.
func lexOperator(s string, start int) (exprToken, int) {
	if start+1 < len(s) {
		two := s[start : start+twoCharToken]
		switch two {
		case "==", "!=", "<=", ">=", "<>":
			return exprToken{text: two, kind: tkOp}, start + twoCharToken
		}
	}

	return exprToken{text: string(s[start]), kind: tkOp}, start + 1
}

// lexNumber lexes an integer or decimal number.
func lexNumber(s string, start int) (exprToken, int) {
	i := start
	for i < len(s) && (s[i] >= '0' && s[i] <= '9' || s[i] == '.') {
		i++
	}

	return exprToken{text: s[start:i], kind: tkNumber}, i
}

// lexIdent lexes an identifier or keyword. count(*) style stars are handled by
// the stats parser; here '*' is only an operator.
func lexIdent(s string, start int) (exprToken, int) {
	i := start
	for i < len(s) && isIdentPart(s[i]) {
		i++
	}

	word := s[start:i]
	if _, ok := keywordSet()[strings.ToLower(word)]; ok {
		return exprToken{text: strings.ToLower(word), kind: tkKeyword}, i
	}

	return exprToken{text: word, kind: tkIdent}, i
}

// exprParser is a recursive-descent parser over expression tokens.
type exprParser struct {
	toks []exprToken
	pos  int
}

func (p *exprParser) peek() (exprToken, bool) {
	if p.pos >= len(p.toks) {
		return exprToken{}, false
	}

	return p.toks[p.pos], true
}

func (p *exprParser) next() exprToken {
	t := p.toks[p.pos]
	p.pos++

	return t
}

func (p *exprParser) isKeyword(word string) bool {
	t, ok := p.peek()

	return ok && t.kind == tkKeyword && t.text == word
}

// parseOr handles the lowest-precedence OR operator.
func (p *exprParser) parseOr() (exprNode, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}

	for p.isKeyword("or") {
		p.next()

		right, rerr := p.parseAnd()
		if rerr != nil {
			return nil, rerr
		}

		left = &logicNode{left: left, right: right, isOr: true}
	}

	return left, nil
}

// parseAnd handles the AND operator.
func (p *exprParser) parseAnd() (exprNode, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}

	for p.isKeyword("and") {
		p.next()

		right, rerr := p.parseNot()
		if rerr != nil {
			return nil, rerr
		}

		left = &logicNode{left: left, right: right, isOr: false}
	}

	return left, nil
}

// parseNot handles a leading NOT.
func (p *exprParser) parseNot() (exprNode, error) {
	if p.isKeyword("not") {
		p.next()

		inner, err := p.parseNot()
		if err != nil {
			return nil, err
		}

		return &notNode{inner: inner}, nil
	}

	return p.parseComparison()
}

// parseComparison handles comparison, like, and in operators.
func (p *exprParser) parseComparison() (exprNode, error) {
	left, err := p.parseAdditive()
	if err != nil {
		return nil, err
	}

	t, ok := p.peek()
	if !ok {
		return left, nil
	}

	switch {
	case t.kind == tkOp && isComparisonOp(t.text):
		return p.parseCmpTail(left)
	case t.kind == tkKeyword && t.text == "like":
		return p.parseLikeTail(left, false)
	case t.kind == tkKeyword && t.text == "in":
		return p.parseInTail(left, false)
	case t.kind == tkKeyword && t.text == "not":
		return p.parseNegatedTail(left)
	default:
		return left, nil
	}
}

// parseNegatedTail handles "not like" and "not in" following an expression.
func (p *exprParser) parseNegatedTail(left exprNode) (exprNode, error) {
	p.next() // consume "not"

	switch {
	case p.isKeyword("like"):
		return p.parseLikeTail(left, true)
	case p.isKeyword("in"):
		return p.parseInTail(left, true)
	default:
		return nil, fmt.Errorf("%w: expected like/in after not", ErrParseExpr)
	}
}

// isComparisonOp reports whether op is a comparison operator.
func isComparisonOp(op string) bool {
	switch op {
	case "=", "==", "!=", "<>", "<", "<=", ">", ">=":
		return true
	default:
		return false
	}
}

func (p *exprParser) parseCmpTail(left exprNode) (exprNode, error) {
	op := p.next().text

	right, err := p.parseAdditive()
	if err != nil {
		return nil, err
	}

	return &cmpNode{left: left, right: right, op: op}, nil
}

func (p *exprParser) parseLikeTail(left exprNode, negate bool) (exprNode, error) {
	p.next() // consume "like"

	t, ok := p.peek()
	if !ok {
		return nil, fmt.Errorf("%w: expected pattern after like", ErrParseExpr)
	}

	p.next()

	re, err := compileLikePattern(t)
	if err != nil {
		return nil, err
	}

	return &likeNode{left: left, re: re, negate: negate}, nil
}

// compileLikePattern builds a regex from a like operand (regex literal or string).
func compileLikePattern(t exprToken) (*regexp.Regexp, error) {
	if t.kind == tkRegex {
		re, err := regexp.Compile(t.text)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrParseExpr, err)
		}

		return re, nil
	}

	re, err := regexp.Compile(regexp.QuoteMeta(t.text))
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrParseExpr, err)
	}

	return re, nil
}

func (p *exprParser) parseInTail(left exprNode, negate bool) (exprNode, error) {
	p.next() // consume "in"

	if t, ok := p.peek(); !ok || t.kind != tkLParen {
		return nil, fmt.Errorf("%w: expected ( after in", ErrParseExpr)
	}

	p.next() // consume (

	values, err := p.parseValueList()
	if err != nil {
		return nil, err
	}

	return &inNode{left: left, values: values, negate: negate}, nil
}

// parseValueList parses a comma-separated list terminated by ')'.
func (p *exprParser) parseValueList() ([]exprNode, error) {
	var values []exprNode

	for {
		node, err := p.parseAdditive()
		if err != nil {
			return nil, err
		}

		values = append(values, node)

		t, ok := p.peek()
		if !ok {
			return nil, fmt.Errorf("%w: unterminated in list", ErrParseExpr)
		}

		p.next()
		if t.kind == tkRParen {
			return values, nil
		}

		if t.kind != tkComma {
			return nil, fmt.Errorf("%w: expected , or ) in list", ErrParseExpr)
		}
	}
}

// parseAdditive handles + and -.
func (p *exprParser) parseAdditive() (exprNode, error) {
	left, err := p.parseMultiplicative()
	if err != nil {
		return nil, err
	}

	for {
		t, ok := p.peek()
		if !ok || t.kind != tkOp || (t.text != "+" && t.text != "-") {
			return left, nil
		}

		p.next()

		right, rerr := p.parseMultiplicative()
		if rerr != nil {
			return nil, rerr
		}

		left = &arithNode{left: left, right: right, op: t.text[0]}
	}
}

// parseMultiplicative handles *, / and %.
func (p *exprParser) parseMultiplicative() (exprNode, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}

	for {
		t, ok := p.peek()
		if !ok || t.kind != tkOp || !isMulOp(t.text) {
			return left, nil
		}

		p.next()

		right, rerr := p.parseUnary()
		if rerr != nil {
			return nil, rerr
		}

		left = &arithNode{left: left, right: right, op: t.text[0]}
	}
}

func isMulOp(op string) bool {
	return op == "*" || op == "/" || op == "%"
}

// parseUnary handles a leading minus.
func (p *exprParser) parseUnary() (exprNode, error) {
	if t, ok := p.peek(); ok && t.kind == tkOp && t.text == "-" {
		p.next()

		inner, err := p.parseUnary()
		if err != nil {
			return nil, err
		}

		return &negNode{inner: inner}, nil
	}

	return p.parsePrimary()
}

// parsePrimary handles literals, fields, and parenthesised expressions.
func (p *exprParser) parsePrimary() (exprNode, error) {
	t, ok := p.peek()
	if !ok {
		return nil, fmt.Errorf("%w: unexpected end of expression", ErrParseExpr)
	}

	switch t.kind {
	case tkLParen:
		return p.parseParen()
	case tkNumber:
		p.next()

		f, err := strconv.ParseFloat(t.text, 64)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrParseExpr, err)
		}

		return &litNode{v: numFromFloat(f)}, nil
	case tkString:
		p.next()

		return &litNode{v: literalString(t.text)}, nil
	case tkIdent:
		p.next()

		return primaryIdent(t.text), nil
	case tkRegex, tkOp, tkComma, tkKeyword, tkRParen:
		return nil, fmt.Errorf("%w: unexpected token %q", ErrParseExpr, t.text)
	default:
		return nil, fmt.Errorf("%w: unexpected token %q", ErrParseExpr, t.text)
	}
}

// primaryIdent maps bare identifiers to fields, honouring true/false literals.
func primaryIdent(text string) exprNode {
	switch strings.ToLower(text) {
	case literalTrue:
		return &litNode{v: numFromFloat(1)}
	case literalFalse:
		return &litNode{v: numFromFloat(0)}
	case literalNull:
		return &litNode{v: cwValue{}}
	default:
		return &fieldNode{name: text}
	}
}

func (p *exprParser) parseParen() (exprNode, error) {
	p.next() // consume (

	inner, err := p.parseOr()
	if err != nil {
		return nil, err
	}

	if t, ok := p.peek(); !ok || t.kind != tkRParen {
		return nil, fmt.Errorf("%w: expected )", ErrParseExpr)
	}

	p.next() // consume )

	return inner, nil
}
