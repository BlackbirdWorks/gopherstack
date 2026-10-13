package glacier

import (
	"errors"
	"fmt"
	"strings"
)

// ErrSelectExpression is the wrapping sentinel for SQL parse errors surfaced from a
// select job's Expression (used to satisfy err113 rather than dynamic errors.New calls).
var ErrSelectExpression = errors.New("select expression")

// selectQuery is the parsed form of a Glacier Select SQL expression -- see select.go's
// package doc for the exact grammar supported.
type selectQuery struct {
	where     selectExpr
	columns   []selectExpr
	selectAll bool
}

// sqlTokKind identifies the lexical class of a sqlTok.
type sqlTokKind int

const (
	sqlTokIdent sqlTokKind = iota
	sqlTokString
	sqlTokNumber
	sqlTokPunct
)

// sqlTok is a single lexed token of a select Expression.
type sqlTok struct {
	val string
	typ sqlTokKind
}

// parseSelectQuery tokenizes and parses a Glacier Select SQL expression.
func parseSelectQuery(expr string) (*selectQuery, error) {
	toks, err := tokenizeSelectExpr(expr)
	if err != nil {
		return nil, err
	}

	p := &selectSQLParser{toks: toks}

	q, err := p.parse()
	if err != nil {
		return nil, err
	}

	if p.pos != len(p.toks) {
		return nil, fmt.Errorf("%w: unexpected trailing tokens", ErrSelectExpression)
	}

	return q, nil
}

// selectSQLParser is a hand-rolled recursive-descent parser over a fixed token slice.
type selectSQLParser struct {
	toks []sqlTok
	pos  int
}

func (p *selectSQLParser) peek() (sqlTok, bool) {
	if p.pos >= len(p.toks) {
		return sqlTok{}, false
	}

	return p.toks[p.pos], true
}

func (p *selectSQLParser) next() (sqlTok, bool) {
	t, ok := p.peek()
	if ok {
		p.pos++
	}

	return t, ok
}

// peekKeyword reports whether the next token is the identifier kw (case-insensitive)
// without consuming it.
func (p *selectSQLParser) peekKeyword(kw string) bool {
	t, ok := p.peek()

	return ok && t.typ == sqlTokIdent && strings.EqualFold(t.val, kw)
}

func (p *selectSQLParser) expectKeyword(kw string) error {
	t, ok := p.next()
	if !ok || t.typ != sqlTokIdent || !strings.EqualFold(t.val, kw) {
		return fmt.Errorf("%w: expected %s", ErrSelectExpression, kw)
	}

	return nil
}

func (p *selectSQLParser) parse() (*selectQuery, error) {
	q := &selectQuery{}

	if err := p.expectKeyword("SELECT"); err != nil {
		return nil, err
	}

	if err := p.parseSelectList(q); err != nil {
		return nil, err
	}

	if err := p.expectKeyword("FROM"); err != nil {
		return nil, err
	}

	if err := p.parseFromClause(); err != nil {
		return nil, err
	}

	if p.peekKeyword("WHERE") {
		p.next()

		cond, err := p.parseExpr()
		if err != nil {
			return nil, err
		}

		if !cond.isBool() {
			return nil, fmt.Errorf("%w: WHERE requires a boolean condition", ErrSelectExpression)
		}

		q.where = cond
	}

	if p.peekKeyword("LIMIT") {
		// Real S3 Glacier Select's SELECT command documents LIMIT as
		// "(Amazon S3 Select only)": "S3 Glacier Select does not support the
		// LIMIT clause" (doc_source/s3-glacier-select-sql-reference-select.md,
		// awsdocs/amazon-glacier-developer-guide). Rejecting it here matches that
		// real 400 parse-error behavior rather than silently accepting a
		// Glacier-Select-only superset.
		return nil, fmt.Errorf("%w: LIMIT is not supported by S3 Glacier Select", ErrSelectExpression)
	}

	return q, nil
}

// parseFromClause consumes the table name and optional alias.
func (p *selectSQLParser) parseFromClause() error {
	if _, ok := p.next(); !ok {
		return fmt.Errorf("%w: expected table name after FROM", ErrSelectExpression)
	}

	if t, ok := p.peek(); ok && t.typ == sqlTokIdent && !isSelectSQLKeyword(t.val) {
		p.next()
	}

	return nil
}

// parseSelectList parses "*" or a comma-separated list of scalar expressions (each with
// an optional AS alias -- accepted for syntax completeness but without effect on output,
// which is always positional per real Glacier/S3 Select CSV output).
func (p *selectSQLParser) parseSelectList(q *selectQuery) error {
	if t, ok := p.peek(); ok && t.typ == sqlTokPunct && t.val == "*" {
		p.next()

		q.selectAll = true

		return nil
	}

	for {
		e, err := p.parseExpr()
		if err != nil {
			return err
		}

		q.columns = append(q.columns, e)

		if p.peekKeyword("AS") {
			p.next()

			if _, ok := p.next(); !ok {
				return fmt.Errorf("%w: expected alias after AS", ErrSelectExpression)
			}
		}

		t, ok := p.peek()
		if !ok || t.typ != sqlTokPunct || t.val != "," {
			break
		}

		p.next()
	}

	return nil
}

// isSelectWhitespace reports whether c is an insignificant whitespace character.
func isSelectWhitespace(c byte) bool {
	switch c {
	case ' ', '\t', '\n', '\r':
		return true
	default:
		return false
	}
}

// isSelectSimplePunct reports whether c is a single-character punctuation token that
// needs no lookahead ("*", ",", "(", ")", ".", "+", "-", "/", "%").
func isSelectSimplePunct(c byte) bool {
	switch c {
	case '*', ',', '(', ')', '.', '+', '-', '/', '%':
		return true
	default:
		return false
	}
}

// tokenizeSelectExpr lexes a Glacier Select SQL expression into a flat token slice.
func tokenizeSelectExpr(s string) ([]sqlTok, error) {
	var toks []sqlTok

	i, n := 0, len(s)

	for i < n {
		c := s[i]

		switch {
		case isSelectWhitespace(c):
			i++
		case c == '\'':
			tok, next, err := lexSelectString(s, i)
			if err != nil {
				return nil, err
			}

			toks = append(toks, tok)
			i = next
		case isSelectSimplePunct(c):
			toks = append(toks, sqlTok{typ: sqlTokPunct, val: string(c)})
			i++
		default:
			tok, next, ok := lexSelectOperatorOrLiteral(s, i)
			if !ok {
				return nil, fmt.Errorf("%w: unexpected character %q", ErrSelectExpression, c)
			}

			toks = append(toks, tok)
			i = next
		}
	}

	return toks, nil
}

// lexSelectString lexes a single-quoted string literal starting at s[start] (which
// must be '\”), supporting ” as an escaped quote. Returns the token and the index
// just past the closing quote.
func lexSelectString(s string, start int) (sqlTok, int, error) {
	var sb strings.Builder

	j := start + 1
	n := len(s)

	for j < n {
		if s[j] == '\'' {
			if j+1 < n && s[j+1] == '\'' {
				sb.WriteByte('\'')
				j += 2

				continue
			}

			return sqlTok{typ: sqlTokString, val: sb.String()}, j + 1, nil
		}

		sb.WriteByte(s[j])
		j++
	}

	return sqlTok{}, 0, fmt.Errorf("%w: unterminated string literal", ErrSelectExpression)
}

// selectTwoCharOpLen is the token length of a two-character comparison operator
// ("!=", "<=", "<>", ">=").
const selectTwoCharOpLen = 2

// lexSelectOperatorOrLiteral lexes a comparison operator, number, or identifier
// starting at s[i] by trying each sub-lexer in turn. ok is false if s[i] cannot start
// any of these (an unrecognized character).
func lexSelectOperatorOrLiteral(s string, i int) (sqlTok, int, bool) {
	if tok, next, ok := lexSelectOperator(s, i); ok {
		return tok, next, true
	}

	if tok, next, ok := lexSelectNumber(s, i); ok {
		return tok, next, true
	}

	if tok, next, ok := lexSelectIdent(s, i); ok {
		return tok, next, true
	}

	return sqlTok{}, 0, false
}

// lexSelectOperator lexes a comparison operator ("=", "!=", "<", "<=", "<>", ">",
// ">=") starting at s[i].
func lexSelectOperator(s string, i int) (sqlTok, int, bool) {
	switch s[i] {
	case '=':
		return sqlTok{typ: sqlTokPunct, val: "="}, i + 1, true
	case '!':
		return lexSelectBangOperator(s, i)
	case '<':
		return lexSelectLessOperator(s, i)
	case '>':
		return lexSelectGreaterOperator(s, i)
	default:
		return sqlTok{}, 0, false
	}
}

// lexSelectBangOperator lexes "!=" starting at s[i] (s[i] == '!').
func lexSelectBangOperator(s string, i int) (sqlTok, int, bool) {
	if i+1 < len(s) && s[i+1] == '=' {
		return sqlTok{typ: sqlTokPunct, val: "!="}, i + selectTwoCharOpLen, true
	}

	return sqlTok{}, 0, false
}

// lexSelectLessOperator lexes "<", "<=", or "<>" starting at s[i] (s[i] == '<').
func lexSelectLessOperator(s string, i int) (sqlTok, int, bool) {
	if i+1 < len(s) {
		switch s[i+1] {
		case '=':
			return sqlTok{typ: sqlTokPunct, val: "<="}, i + selectTwoCharOpLen, true
		case '>':
			return sqlTok{typ: sqlTokPunct, val: "<>"}, i + selectTwoCharOpLen, true
		}
	}

	return sqlTok{typ: sqlTokPunct, val: "<"}, i + 1, true
}

// lexSelectGreaterOperator lexes ">" or ">=" starting at s[i] (s[i] == '>').
func lexSelectGreaterOperator(s string, i int) (sqlTok, int, bool) {
	if i+1 < len(s) && s[i+1] == '=' {
		return sqlTok{typ: sqlTokPunct, val: ">="}, i + selectTwoCharOpLen, true
	}

	return sqlTok{typ: sqlTokPunct, val: ">"}, i + 1, true
}

// lexSelectNumber lexes an unsigned (possibly fractional) number literal starting at
// s[i]; a leading minus is the unary operator, handled by the parser.
func lexSelectNumber(s string, i int) (sqlTok, int, bool) {
	if s[i] < '0' || s[i] > '9' {
		return sqlTok{}, 0, false
	}

	n := len(s)
	j := i + 1

	for j < n && (s[j] == '.' || (s[j] >= '0' && s[j] <= '9')) {
		j++
	}

	return sqlTok{typ: sqlTokNumber, val: s[i:j]}, j, true
}

// lexSelectIdent lexes an identifier (keyword or column reference) starting at s[i].
func lexSelectIdent(s string, i int) (sqlTok, int, bool) {
	if !isSelectIdentStart(s[i]) {
		return sqlTok{}, 0, false
	}

	n := len(s)
	j := i + 1

	for j < n && isSelectIdentPart(s[j]) {
		j++
	}

	return sqlTok{typ: sqlTokIdent, val: s[i:j]}, j, true
}

func isSelectIdentStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isSelectIdentPart(c byte) bool {
	return isSelectIdentStart(c) || (c >= '0' && c <= '9')
}
