package glue

import (
	"cmp"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"unicode"
)

// Sentinel parse errors for partition expression parsing.
var (
	errExprUnexpectedToken     = errors.New("unexpected token")
	errExprExpectedCloseParen  = errors.New("expected closing parenthesis")
	errExprExpectedColumn      = errors.New("expected column name")
	errExprExpectedOperator    = errors.New("expected operator after column")
	errExprExpectedIN          = errors.New("expected IN or BETWEEN after NOT")
	errExprExpectedAnd         = errors.New("expected AND in BETWEEN")
	errExprExpectedString      = errors.New("expected string literal after operator")
	errExprExpectedOpenParen   = errors.New("expected ( after IN")
	errExprUnterminatedIN      = errors.New("unterminated IN list")
	errExprExpectedCommaInList = errors.New("expected , in IN list")
	errExprExpectedStringInIN  = errors.New("expected string in IN list")
)

// partitionExpr is a compiled predicate for filtering partitions by their values.
type partitionExpr interface {
	eval(keys []Column, values []string) bool
}

// exprAnd evaluates left AND right.
type exprAnd struct{ left, right partitionExpr }

func (e *exprAnd) eval(k []Column, v []string) bool { return e.left.eval(k, v) && e.right.eval(k, v) }

// exprOr evaluates left OR right.
type exprOr struct{ left, right partitionExpr }

func (e *exprOr) eval(k []Column, v []string) bool { return e.left.eval(k, v) || e.right.eval(k, v) }

// exprNot negates its child.
type exprNot struct{ child partitionExpr }

func (e *exprNot) eval(k []Column, v []string) bool { return !e.child.eval(k, v) }

// exprCmp compares a partition column to a literal using op.
type exprCmp struct {
	col string
	op  string
	val string
}

func (e *exprCmp) eval(keys []Column, values []string) bool {
	idx, typ := partitionKeyIndex(keys, e.col)
	if idx < 0 || idx >= len(values) {
		return false
	}

	pv := values[idx]

	if e.op == "LIKE" {
		return likeMatch(e.val, pv)
	}

	c := comparePartitionValues(typ, pv, e.val)

	switch e.op {
	case "=":
		return c == 0
	case "<>", "!=":
		return c != 0
	case ">":
		return c > 0
	case ">=":
		return c >= 0
	case "<":
		return c < 0
	case "<=":
		return c <= 0
	default:
		return false
	}
}

// exprBetween checks low <= value <= high (both inclusive).
type exprBetween struct {
	col    string
	low    string
	high   string
	negate bool
}

func (e *exprBetween) eval(keys []Column, values []string) bool {
	idx, typ := partitionKeyIndex(keys, e.col)
	if idx < 0 || idx >= len(values) {
		return false
	}

	in := comparePartitionValues(typ, values[idx], e.low) >= 0 &&
		comparePartitionValues(typ, values[idx], e.high) <= 0

	return in != e.negate
}

// partitionKeyIndex returns the index and type of the partition key named
// col (case-insensitive), or -1.
func partitionKeyIndex(keys []Column, col string) (int, string) {
	for i, k := range keys {
		if strings.EqualFold(k.Name, col) {
			return i, k.Type
		}
	}

	return -1, ""
}

// comparePartitionValues orders numerically for numeric key types
// (api_op_GetPartitions.go:101-122) when both sides parse, lexically otherwise.
func comparePartitionValues(typ, a, b string) int {
	if isNumericPartitionType(typ) {
		fa, errA := strconv.ParseFloat(strings.TrimSpace(a), 64)
		fb, errB := strconv.ParseFloat(strings.TrimSpace(b), 64)

		if errA == nil && errB == nil {
			return cmp.Compare(fa, fb)
		}
	}

	return strings.Compare(a, b)
}

func isNumericPartitionType(typ string) bool {
	t := strings.ToLower(typ)

	switch t {
	case "int", "bigint", "long", "tinyint", "smallint":
		return true
	default:
		return strings.HasPrefix(t, "decimal")
	}
}

// exprIn checks whether the partition column value is in a set.
type exprIn struct {
	col    string
	values []string
	negate bool
}

func (e *exprIn) eval(keys []Column, values []string) bool {
	idx, typ := partitionKeyIndex(keys, e.col)
	if idx < 0 || idx >= len(values) {
		return e.negate
	}

	pv := values[idx]

	for _, v := range e.values {
		if comparePartitionValues(typ, pv, v) == 0 {
			return !e.negate
		}
	}

	return e.negate
}

// likeMatch implements SQL LIKE: % matches any sequence, _ matches one char.
func likeMatch(pattern, s string) bool {
	var sb strings.Builder

	sb.WriteString("^")

	for _, ch := range pattern {
		switch ch {
		case '%':
			sb.WriteString(".*")
		case '_':
			sb.WriteString(".")
		default:
			sb.WriteString(regexp.QuoteMeta(string(ch)))
		}
	}

	sb.WriteString("$")

	matched, err := regexp.MatchString(sb.String(), s)

	return err == nil && matched
}

// parsePartitionExpr parses a SQL WHERE expression for partition filtering.
func parsePartitionExpr(expr string) (partitionExpr, error) {
	p := &exprParser{tokens: tokenize(expr)}

	result, err := p.parseOr()
	if err != nil {
		return nil, err
	}

	if !p.done() {
		return nil, fmt.Errorf("%w: %q", errExprUnexpectedToken, p.peek())
	}

	return result, nil
}

// tableNameRegexp converts a Glue table Expression (which is a regex) to a compiled regexp.
func tableNameRegexp(expr string) (*regexp.Regexp, error) {
	return regexp.Compile(expr)
}

// token kinds.
const (
	tokWord   = "WORD"
	tokString = "STRING"
	tokNumber = "NUMBER"
	tokLParen = "LPAREN"
	tokRParen = "RPAREN"
	tokComma  = "COMMA"
	tokOp     = "OP"
)

type token struct {
	kind string
	val  string
}

// tokenize scans s into a slice of tokens; unknown characters are skipped.
func tokenize(s string) []token {
	var tokens []token

	for i := 0; i < len(s); {
		tok, next, ok := scanToken(s, i)
		if ok {
			tokens = append(tokens, tok)
		}

		i = next
	}

	return tokens
}

func punctuationKind(ch rune) string {
	switch ch {
	case '(':
		return tokLParen
	case ')':
		return tokRParen
	case ',':
		return tokComma
	default:
		return ""
	}
}

// scanToken scans the token starting at s[i], returning it and the index just
// past it; ok is false for whitespace and unknown characters.
func scanToken(s string, i int) (token, int, bool) {
	ch := rune(s[i])

	if kind := punctuationKind(ch); kind != "" {
		return token{kind, string(ch)}, i + 1, true
	}

	var tok token

	next := i + 1

	switch {
	case ch == '\'':
		tok, next = tokenizeString(s, i)
	case ch == '>' || ch == '<' || ch == '=' || ch == '!':
		tok, next = tokenizeOperator(s, i)
	case startsNumber(s, i):
		tok, next = tokenizeNumber(s, i)
	case unicode.IsLetter(ch) || ch == '_':
		tok, next = tokenizeWord(s, i)
	default:
		return token{}, next, false
	}

	return tok, next, true
}

// tokenizeString scans a single-quoted string literal (with a doubled quote
// as an escaped quote) starting at i, which must point at the opening quote. It returns
// the STRING token and the index just past the closing quote.
func tokenizeString(s string, i int) (token, int) {
	j := i + 1

	var sb strings.Builder

	for j < len(s) {
		if s[j] == '\'' {
			if j+1 < len(s) && s[j+1] == '\'' {
				sb.WriteByte('\'')
				j += 2

				continue
			}

			break
		}

		sb.WriteByte(s[j])
		j++
	}

	return token{tokString, sb.String()}, j + 1
}

// tokenizeOperator scans one of >=, <=, <>, !=, >, <, = starting at i. It
// returns the OP token and the index just past the operator.
func tokenizeOperator(s string, i int) (token, int) {
	ch := s[i]
	op := string(ch)
	next := i + 1

	if i+1 < len(s) && (s[i+1] == '=' || s[i+1] == '>') {
		op += string(s[i+1])
		next++
	}

	return token{tokOp, op}, next
}

// startsNumber reports whether a numeric literal begins at s[i].
func startsNumber(s string, i int) bool {
	if unicode.IsDigit(rune(s[i])) {
		return true
	}

	return (s[i] == '-' || s[i] == '.') && i+1 < len(s) && unicode.IsDigit(rune(s[i+1]))
}

// tokenizeNumber scans an unquoted numeric literal (optional sign, digits,
// optional fraction) starting at i.
func tokenizeNumber(s string, i int) (token, int) {
	j := i + 1

	for j < len(s) && (unicode.IsDigit(rune(s[j])) || s[j] == '.') {
		j++
	}

	return token{tokNumber, s[i:j]}, j
}

// tokenizeWord scans an identifier or keyword starting at i. It returns the
// WORD token and the index just past the word.
func tokenizeWord(s string, i int) (token, int) {
	j := i

	for j < len(s) && (unicode.IsLetter(rune(s[j])) || unicode.IsDigit(rune(s[j])) || s[j] == '_') {
		j++
	}

	return token{tokWord, s[i:j]}, j
}

type exprParser struct {
	tokens []token
	pos    int
}

func (p *exprParser) peek() string {
	if p.pos >= len(p.tokens) {
		return ""
	}

	return p.tokens[p.pos].val
}

func (p *exprParser) peekKind() string {
	if p.pos >= len(p.tokens) {
		return ""
	}

	return p.tokens[p.pos].kind
}

func (p *exprParser) consume() token {
	t := p.tokens[p.pos]
	p.pos++

	return t
}

func (p *exprParser) done() bool { return p.pos >= len(p.tokens) }

func (p *exprParser) parseOr() (partitionExpr, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}

	for !p.done() && strings.EqualFold(p.peek(), "OR") {
		p.consume()

		var right partitionExpr

		right, err = p.parseAnd()
		if err != nil {
			return nil, err
		}

		left = &exprOr{left, right}
	}

	return left, nil
}

func (p *exprParser) parseAnd() (partitionExpr, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}

	for !p.done() && strings.EqualFold(p.peek(), "AND") {
		p.consume()

		var right partitionExpr

		right, err = p.parseUnary()
		if err != nil {
			return nil, err
		}

		left = &exprAnd{left, right}
	}

	return left, nil
}

func (p *exprParser) parseUnary() (partitionExpr, error) {
	if !p.done() && strings.EqualFold(p.peek(), "NOT") {
		p.consume()

		child, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}

		return &exprNot{child}, nil
	}

	return p.parsePrimary()
}

// parsePrimary parses a parenthesized expression or a column predicate
// (comparison, LIKE, IN, or NOT IN).
func (p *exprParser) parsePrimary() (partitionExpr, error) {
	if !p.done() && p.peekKind() == tokLParen {
		return p.parseParenExpr()
	}

	col, err := p.consumeColumn()
	if err != nil {
		return nil, err
	}

	return p.parseColumnPredicate(col)
}

// parseParenExpr parses a fully parenthesized sub-expression, assuming the
// next token is the opening paren.
func (p *exprParser) parseParenExpr() (partitionExpr, error) {
	p.consume() // (

	inner, err := p.parseOr()
	if err != nil {
		return nil, err
	}

	if p.done() || p.peekKind() != tokRParen {
		return nil, errExprExpectedCloseParen
	}

	p.consume() // )

	return inner, nil
}

// consumeColumn consumes and returns the column-name token that starts a
// primary expression.
func (p *exprParser) consumeColumn() (string, error) {
	if p.done() || p.peekKind() != tokWord {
		return "", fmt.Errorf("%w, got %q", errExprExpectedColumn, p.peek())
	}

	col := p.consume().val

	if p.done() {
		return "", fmt.Errorf("%w %q", errExprExpectedOperator, col)
	}

	return col, nil
}

// parseColumnPredicate parses the predicate that follows a column name:
// IN (...), NOT IN (...), or a comparison/LIKE operator with a string value.
func (p *exprParser) parseColumnPredicate(col string) (partitionExpr, error) {
	// Check for NOT IN or IN
	if strings.EqualFold(p.peek(), "IN") {
		return p.parseIn(col, false)
	}

	if strings.EqualFold(p.peek(), "BETWEEN") {
		return p.parseBetween(col, false)
	}

	if strings.EqualFold(p.peek(), "NOT") {
		p.consume() // NOT

		switch {
		case !p.done() && strings.EqualFold(p.peek(), "IN"):
			return p.parseIn(col, true)
		case !p.done() && strings.EqualFold(p.peek(), "BETWEEN"):
			return p.parseBetween(col, true)
		default:
			return nil, errExprExpectedIN
		}
	}

	if p.done() || (p.peekKind() != tokOp && !strings.EqualFold(p.peek(), "LIKE")) {
		return nil, fmt.Errorf("%w %q, got %q", errExprExpectedOperator, col, p.peek())
	}

	op := strings.ToUpper(p.consume().val)

	if p.done() || !isLiteralToken(p.peekKind()) {
		return nil, fmt.Errorf("%w, got %q", errExprExpectedString, p.peek())
	}

	val := p.consume().val

	return &exprCmp{col: col, op: op, val: val}, nil
}

func isLiteralToken(kind string) bool {
	return kind == tokString || kind == tokNumber
}

// parseBetween parses "BETWEEN low AND high" after the column (and optional
// NOT) has been consumed; the AND here belongs to BETWEEN, not the boolean.
func (p *exprParser) parseBetween(col string, negate bool) (partitionExpr, error) {
	p.consume() // BETWEEN

	if p.done() || !isLiteralToken(p.peekKind()) {
		return nil, fmt.Errorf("%w, got %q", errExprExpectedString, p.peek())
	}

	low := p.consume().val

	if p.done() || !strings.EqualFold(p.peek(), "AND") {
		return nil, fmt.Errorf("%w, got %q", errExprExpectedAnd, p.peek())
	}

	p.consume() // AND

	if p.done() || !isLiteralToken(p.peekKind()) {
		return nil, fmt.Errorf("%w, got %q", errExprExpectedString, p.peek())
	}

	return &exprBetween{col: col, low: low, high: p.consume().val, negate: negate}, nil
}

func (p *exprParser) parseIn(col string, negate bool) (partitionExpr, error) {
	p.consume() // IN

	if p.done() || p.peekKind() != tokLParen {
		return nil, errExprExpectedOpenParen
	}

	p.consume() // (

	var values []string

	for {
		if p.done() {
			return nil, errExprUnterminatedIN
		}

		if p.peekKind() == tokRParen {
			p.consume()

			break
		}

		if len(values) > 0 {
			if p.peekKind() != tokComma {
				return nil, fmt.Errorf("%w, got %q", errExprExpectedCommaInList, p.peek())
			}

			p.consume()
		}

		if p.done() || !isLiteralToken(p.peekKind()) {
			return nil, fmt.Errorf("%w, got %q", errExprExpectedStringInIN, p.peek())
		}

		values = append(values, p.consume().val)
	}

	return &exprIn{col: col, values: values, negate: negate}, nil
}
