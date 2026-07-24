package iotanalytics

import (
	"cmp"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode"
)

// ----------------------------------------
// Pipeline expression language
//
// RunPipelineActivity's "filter" and "math" pipeline activities each carry a
// small SQL-like expression string (AWS docs example: filter "temp > 50",
// math "(temp - 32) / 1.8"). This file implements a self-contained
// tokenizer, recursive-descent parser, and evaluator for that expression
// language: numeric/string/boolean/null literals, message-attribute
// identifiers (looked up by top-level JSON key), the arithmetic operators
// + - * / %, the comparison operators = != <> < <= > >=, the logical
// operators AND / OR / NOT, and parenthesized grouping.
//
// AWS's real grammar is an open SQL-like superset that also supports
// functions (e.g. TRIM, SUBSTR, date functions). Those are not implemented
// here -- expressions using them fail to parse. See PARITY.md
// items_still_open.
// ----------------------------------------

// ErrExprSyntax is returned when a pipeline filter/math expression fails to parse or evaluate.
var ErrExprSyntax = errors.New("pipeline expression error")

type tokenKind int

const (
	tokEOF tokenKind = iota
	tokNumber
	tokString
	tokIdent
	tokAnd
	tokOr
	tokNot
	tokTrue
	tokFalse
	tokNull
	tokEq
	tokNeq
	tokLt
	tokLte
	tokGt
	tokGte
	tokPlus
	tokMinus
	tokStar
	tokSlash
	tokPercent
	tokLParen
	tokRParen
)

type token struct {
	text string
	kind tokenKind
	num  float64
}

// tokenizeExpr splits an expression string into tokens.
func tokenizeExpr(s string) ([]token, error) {
	var tokens []token

	r := []rune(s)
	i := 0

	for i < len(r) {
		tok, n, err := nextToken(r, i)
		if err != nil {
			return nil, err
		}

		i = n
		if tok != nil {
			tokens = append(tokens, *tok)
		}
	}

	tokens = append(tokens, token{kind: tokEOF})

	return tokens, nil
}

// twoCharTokenLen is the rune width consumed by a two-character operator (!=, <>, <=, >=).
const twoCharTokenLen = 2

// nextToken scans a single token starting at rune index i, delegating to
// scanMultiCharOp and singleCharToken to keep each dispatch table's cyclomatic complexity
// small on its own.
func nextToken(r []rune, i int) (*token, int, error) {
	c := r[i]

	if unicode.IsSpace(c) {
		return nil, i + 1, nil
	}

	if tok, n, ok := scanMultiCharOp(r, i); ok {
		return tok, n, nil
	}

	if kind, ok := singleCharToken(c); ok {
		return &token{kind: kind}, i + 1, nil
	}

	switch {
	case c == '\'' || c == '"':
		return scanString(r, i)
	case unicode.IsDigit(c):
		tok, n := scanNumber(r, i)

		return &tok, n, nil
	case unicode.IsLetter(c) || c == '_':
		tok, n := scanIdent(r, i)

		return &tok, n, nil
	default:
		return nil, 0, fmt.Errorf("%w: unexpected character %q", ErrExprSyntax, c)
	}
}

// scanMultiCharOp recognizes the two-character comparison operators (!=, <>, <=, >=) at
// rune index i. Returns ok=false if none match, leaving c to fall through to
// singleCharToken (e.g. a lone "<" or ">").
func scanMultiCharOp(r []rune, i int) (*token, int, bool) {
	if i+1 >= len(r) {
		return nil, 0, false
	}

	switch string(r[i : i+twoCharTokenLen]) {
	case "!=", "<>":
		return &token{kind: tokNeq}, i + twoCharTokenLen, true
	case "<=":
		return &token{kind: tokLte}, i + twoCharTokenLen, true
	case ">=":
		return &token{kind: tokGte}, i + twoCharTokenLen, true
	default:
		return nil, 0, false
	}
}

// singleCharToken maps a single-character operator/punctuation rune to its token kind.
func singleCharToken(c rune) (tokenKind, bool) {
	switch c {
	case '(':
		return tokLParen, true
	case ')':
		return tokRParen, true
	case '+':
		return tokPlus, true
	case '-':
		return tokMinus, true
	case '*':
		return tokStar, true
	case '/':
		return tokSlash, true
	case '%':
		return tokPercent, true
	case '=':
		return tokEq, true
	case '<':
		return tokLt, true
	case '>':
		return tokGt, true
	default:
		return tokEOF, false
	}
}

func scanString(r []rune, start int) (*token, int, error) {
	quote := r[start]
	i := start + 1

	var sb strings.Builder

	for i < len(r) && r[i] != quote {
		sb.WriteRune(r[i])
		i++
	}

	if i >= len(r) {
		return nil, 0, fmt.Errorf("%w: unterminated string literal", ErrExprSyntax)
	}

	return &token{kind: tokString, text: sb.String()}, i + 1, nil
}

func scanNumber(r []rune, start int) (token, int) {
	i := start
	for i < len(r) && (unicode.IsDigit(r[i]) || r[i] == '.') {
		i++
	}

	f, _ := strconv.ParseFloat(string(r[start:i]), 64)

	return token{kind: tokNumber, num: f}, i
}

func scanIdent(r []rune, start int) (token, int) {
	i := start
	for i < len(r) && (unicode.IsLetter(r[i]) || unicode.IsDigit(r[i]) || r[i] == '_') {
		i++
	}

	word := string(r[start:i])
	if kind, ok := exprKeyword(strings.ToUpper(word)); ok {
		return token{kind: kind}, i
	}

	return token{kind: tokIdent, text: word}, i
}

// exprKeyword maps a case-normalized expression keyword to its token kind.
func exprKeyword(word string) (tokenKind, bool) {
	switch word {
	case "AND":
		return tokAnd, true
	case "OR":
		return tokOr, true
	case "NOT":
		return tokNot, true
	case "TRUE":
		return tokTrue, true
	case "FALSE":
		return tokFalse, true
	case "NULL":
		return tokNull, true
	default:
		return tokEOF, false
	}
}

// ----------------------------------------
// Parser
// ----------------------------------------

// exprNode is a parsed pipeline expression AST node.
type exprNode interface {
	eval(msg map[string]any) (any, error)
}

type exprParser struct {
	tokens []token
	pos    int
}

// parseExpr tokenizes and parses a full pipeline filter/math expression.
func parseExpr(s string) (exprNode, error) {
	tokens, err := tokenizeExpr(s)
	if err != nil {
		return nil, err
	}

	p := &exprParser{tokens: tokens}

	node, err := p.parseOr()
	if err != nil {
		return nil, err
	}

	if p.peek().kind != tokEOF {
		return nil, fmt.Errorf("%w: unexpected trailing input", ErrExprSyntax)
	}

	return node, nil
}

func (p *exprParser) peek() token { return p.tokens[p.pos] }

func (p *exprParser) next() token {
	t := p.tokens[p.pos]
	if p.pos < len(p.tokens)-1 {
		p.pos++
	}

	return t
}

func (p *exprParser) parseOr() (exprNode, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}

	for p.peek().kind == tokOr {
		p.next()

		right, rerr := p.parseAnd()
		if rerr != nil {
			return nil, rerr
		}

		left = binaryNode{op: tokOr, l: left, r: right}
	}

	return left, nil
}

func (p *exprParser) parseAnd() (exprNode, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}

	for p.peek().kind == tokAnd {
		p.next()

		right, rerr := p.parseNot()
		if rerr != nil {
			return nil, rerr
		}

		left = binaryNode{op: tokAnd, l: left, r: right}
	}

	return left, nil
}

func (p *exprParser) parseNot() (exprNode, error) {
	if p.peek().kind == tokNot {
		p.next()

		x, err := p.parseNot()
		if err != nil {
			return nil, err
		}

		return unaryNode{op: tokNot, x: x}, nil
	}

	return p.parseComparison()
}

// isComparisonOp reports whether kind is one of the single-shot comparison operators
// (=, !=, <>, <, <=, >, >=) parseComparison accepts.
func isComparisonOp(kind tokenKind) bool {
	switch kind {
	case tokEq, tokNeq, tokLt, tokLte, tokGt, tokGte:
		return true
	default:
		return false
	}
}

func (p *exprParser) parseComparison() (exprNode, error) {
	left, err := p.parseAdditive()
	if err != nil {
		return nil, err
	}

	if isComparisonOp(p.peek().kind) {
		op := p.next().kind

		right, rerr := p.parseAdditive()
		if rerr != nil {
			return nil, rerr
		}

		return binaryNode{op: op, l: left, r: right}, nil
	}

	return left, nil
}

func (p *exprParser) parseAdditive() (exprNode, error) {
	left, err := p.parseMultiplicative()
	if err != nil {
		return nil, err
	}

	for p.peek().kind == tokPlus || p.peek().kind == tokMinus {
		op := p.next().kind

		right, rerr := p.parseMultiplicative()
		if rerr != nil {
			return nil, rerr
		}

		left = binaryNode{op: op, l: left, r: right}
	}

	return left, nil
}

func (p *exprParser) parseMultiplicative() (exprNode, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}

	for p.peek().kind == tokStar || p.peek().kind == tokSlash || p.peek().kind == tokPercent {
		op := p.next().kind

		right, rerr := p.parseUnary()
		if rerr != nil {
			return nil, rerr
		}

		left = binaryNode{op: op, l: left, r: right}
	}

	return left, nil
}

func (p *exprParser) parseUnary() (exprNode, error) {
	if p.peek().kind == tokMinus {
		p.next()

		x, err := p.parseUnary()
		if err != nil {
			return nil, err
		}

		return unaryNode{op: tokMinus, x: x}, nil
	}

	return p.parsePrimary()
}

func (p *exprParser) parsePrimary() (exprNode, error) {
	t := p.next()

	switch t.kind {
	case tokNumber:
		return numberLit{v: t.num}, nil
	case tokString:
		return stringLit{v: t.text}, nil
	case tokTrue:
		return boolLit{v: true}, nil
	case tokFalse:
		return boolLit{v: false}, nil
	case tokNull:
		return nullLit{}, nil
	case tokIdent:
		return identNode{name: t.text}, nil
	case tokLParen:
		return p.parseParenGroup()
	default:
		return nil, fmt.Errorf("%w: unexpected token", ErrExprSyntax)
	}
}

func (p *exprParser) parseParenGroup() (exprNode, error) {
	node, err := p.parseOr()
	if err != nil {
		return nil, err
	}

	if p.peek().kind != tokRParen {
		return nil, fmt.Errorf("%w: expected closing parenthesis", ErrExprSyntax)
	}

	p.next()

	return node, nil
}

// ----------------------------------------
// AST node evaluation
// ----------------------------------------

type numberLit struct{ v float64 }

func (n numberLit) eval(map[string]any) (any, error) { return n.v, nil }

type stringLit struct{ v string }

func (s stringLit) eval(map[string]any) (any, error) { return s.v, nil }

type boolLit struct{ v bool }

func (b boolLit) eval(map[string]any) (any, error) { return b.v, nil }

// exprNull is the evaluation result of the NULL literal. It is a distinct type (not a bare
// Go nil) so eval never returns the ambiguous (nil, nil) pair -- compareValues treats an
// exprNull operand as matching neither float64, string, nor bool, so NULL comparisons fall
// through to the mismatched-type default (= is false, != is true, ordering is an error),
// which is a defensible simplification of SQL's three-valued NULL logic.
type exprNull struct{}

type nullLit struct{}

func (nullLit) eval(map[string]any) (any, error) { return exprNull{}, nil }

// identNode looks up a top-level message attribute by name. A missing
// attribute is a soft evaluation error (see applyFilter/applyMath, which
// treat it as "this message doesn't match / can't be transformed" rather
// than failing the whole RunPipelineActivity call).
type identNode struct{ name string }

func (id identNode) eval(msg map[string]any) (any, error) {
	v, ok := msg[id.name]
	if !ok {
		return nil, fmt.Errorf("%w: unknown attribute %q", ErrExprSyntax, id.name)
	}

	return v, nil
}

type unaryNode struct {
	x  exprNode
	op tokenKind
}

func (u unaryNode) eval(msg map[string]any) (any, error) {
	v, err := u.x.eval(msg)
	if err != nil {
		return nil, err
	}

	switch u.op {
	case tokMinus:
		f, ok := toFloat(v)
		if !ok {
			return nil, fmt.Errorf("%w: unary - requires a number", ErrExprSyntax)
		}

		return -f, nil
	case tokNot:
		b, ok := v.(bool)
		if !ok {
			return nil, fmt.Errorf("%w: NOT requires a boolean", ErrExprSyntax)
		}

		return !b, nil
	default:
		return nil, fmt.Errorf("%w: unsupported unary operator", ErrExprSyntax)
	}
}

type binaryNode struct {
	l, r exprNode
	op   tokenKind
}

func (b binaryNode) eval(msg map[string]any) (any, error) {
	switch b.op {
	case tokAnd, tokOr:
		return b.evalLogical(msg)
	case tokEq, tokNeq, tokLt, tokLte, tokGt, tokGte:
		return b.evalComparison(msg)
	case tokPlus, tokMinus, tokStar, tokSlash, tokPercent:
		return b.evalArithmetic(msg)
	default:
		return nil, fmt.Errorf("%w: unsupported operator", ErrExprSyntax)
	}
}

func (b binaryNode) evalLogical(msg map[string]any) (any, error) {
	lv, err := b.l.eval(msg)
	if err != nil {
		return nil, err
	}

	lb, ok := lv.(bool)
	if !ok {
		return nil, fmt.Errorf("%w: AND/OR requires boolean operands", ErrExprSyntax)
	}

	if b.op == tokAnd && !lb {
		return false, nil
	}

	if b.op == tokOr && lb {
		return true, nil
	}

	rv, err := b.r.eval(msg)
	if err != nil {
		return nil, err
	}

	rb, ok := rv.(bool)
	if !ok {
		return nil, fmt.Errorf("%w: AND/OR requires boolean operands", ErrExprSyntax)
	}

	return rb, nil
}

func (b binaryNode) evalComparison(msg map[string]any) (any, error) {
	lv, err := b.l.eval(msg)
	if err != nil {
		return nil, err
	}

	rv, err := b.r.eval(msg)
	if err != nil {
		return nil, err
	}

	return compareValues(b.op, lv, rv)
}

func (b binaryNode) evalArithmetic(msg map[string]any) (any, error) {
	lv, err := b.l.eval(msg)
	if err != nil {
		return nil, err
	}

	rv, err := b.r.eval(msg)
	if err != nil {
		return nil, err
	}

	lf, ok1 := toFloat(lv)
	rf, ok2 := toFloat(rv)

	if !ok1 || !ok2 {
		return nil, fmt.Errorf("%w: arithmetic requires numeric operands", ErrExprSyntax)
	}

	return arithmetic(b.op, lf, rf)
}

func arithmetic(op tokenKind, lf, rf float64) (any, error) {
	switch op {
	case tokPlus:
		return lf + rf, nil
	case tokMinus:
		return lf - rf, nil
	case tokStar:
		return lf * rf, nil
	case tokSlash:
		if rf == 0 {
			return nil, fmt.Errorf("%w: division by zero", ErrExprSyntax)
		}

		return lf / rf, nil
	case tokPercent:
		if rf == 0 {
			return nil, fmt.Errorf("%w: division by zero", ErrExprSyntax)
		}

		return math.Mod(lf, rf), nil
	default:
		return nil, fmt.Errorf("%w: unsupported arithmetic operator", ErrExprSyntax)
	}
}

// toFloat coerces a decoded JSON value to float64. JSON numbers decode as
// float64 via encoding/json's default map[string]any unmarshaling.
func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	default:
		return 0, false
	}
}

// compareValues implements =, !=, <>, <, <=, >, >= across numeric, string,
// and boolean operands. Equality/inequality across mismatched types is
// well-defined (never equal), matching typical SQL semantics; ordering
// comparisons (<, <=, >, >=) between mismatched types have no well-defined
// meaning and are an evaluation error.
func compareValues(op tokenKind, l, r any) (any, error) {
	if lf, lOK := l.(float64); lOK {
		if rf, rOK := r.(float64); rOK {
			return compareOrdered(op, lf, rf)
		}
	}

	if ls, lOK := l.(string); lOK {
		if rs, rOK := r.(string); rOK {
			return compareOrdered(op, ls, rs)
		}
	}

	if lb, lOK := l.(bool); lOK {
		if rb, rOK := r.(bool); rOK {
			return compareBool(op, lb, rb)
		}
	}

	switch op {
	case tokEq:
		return false, nil
	case tokNeq:
		return true, nil
	default:
		return nil, fmt.Errorf("%w: cannot order-compare mismatched types", ErrExprSyntax)
	}
}

func compareBool(op tokenKind, l, r bool) (any, error) {
	switch op {
	case tokEq:
		return l == r, nil
	case tokNeq:
		return l != r, nil
	default:
		return nil, fmt.Errorf("%w: booleans only support = and !=", ErrExprSyntax)
	}
}

func compareOrdered[T cmp.Ordered](op tokenKind, l, r T) (any, error) {
	switch op {
	case tokEq:
		return l == r, nil
	case tokNeq:
		return l != r, nil
	case tokLt:
		return l < r, nil
	case tokLte:
		return l <= r, nil
	case tokGt:
		return l > r, nil
	case tokGte:
		return l >= r, nil
	default:
		return nil, fmt.Errorf("%w: unsupported comparison operator", ErrExprSyntax)
	}
}
