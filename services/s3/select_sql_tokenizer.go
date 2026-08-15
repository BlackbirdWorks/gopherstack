package s3

import (
	"errors"
	"fmt"
	"unicode"
)

// sqlTokenType identifies the kind of a SQL token.
type sqlTokenType int

const (
	tokEOF sqlTokenType = iota
	tokIdent
	tokString
	tokNumber
	tokStar
	tokComma
	tokDot
	tokLParen
	tokRParen
	tokEq  // =
	tokNeq // != or <>
	tokLt
	tokLte
	tokGt
	tokGte
)

// Sentinel errors for the SQL tokeniser and parser.
var (
	errUnterminatedString  = errors.New("unterminated string literal")
	errUnterminatedIdent   = errors.New("unterminated quoted identifier")
	errExpectedLIKE        = errors.New("expected LIKE after NOT")
	errExpectedInListToken = errors.New("expected , or ) in IN list")
	errUnexpectedChar      = errors.New("unexpected character")
	errUnexpectedToken     = errors.New("unexpected token")
	errExpectedKeyword     = errors.New("expected keyword")
	errExpectedTokenType   = errors.New("expected token type")
	errUnknownOperator     = errors.New("unknown operator")
	errNonAggregateColumn  = errors.New("non-aggregate column in aggregate query")
	errNegativeLimit       = errors.New("LIMIT value must be non-negative")
)

// sqlNullType is the internal representation of SQL NULL.
// Using a named type avoids (nil, nil) returns from eval functions.
type sqlNullType struct{}

// sqlNullValue is the single sentinel value representing SQL NULL.
var sqlNullValue any = sqlNullType{} //nolint:gochecknoglobals // package-level singleton for SQL NULL representation

// sqlToken is a single token produced by the SQL tokeniser.
type sqlToken struct {
	val string
	typ sqlTokenType
}

// sqlTokeniser produces tokens from a SQL string.
type sqlTokeniser struct {
	src string
	pos int
}

func newSQLTokeniser(src string) *sqlTokeniser {
	return &sqlTokeniser{src: src}
}

func (t *sqlTokeniser) peek() (sqlToken, error) {
	saved := t.pos
	tok, err := t.next()
	t.pos = saved

	return tok, err
}

// next returns the next token.
func (t *sqlTokeniser) next() (sqlToken, error) {
	t.skipWhitespace()

	if t.pos >= len(t.src) {
		return sqlToken{typ: tokEOF}, nil
	}

	ch := t.src[t.pos]

	if ok, tok, err := t.nextQuoteOrPunct(ch); ok {
		return tok, err
	}

	if t.isNumberStart(ch) {
		return t.readNumber()
	}

	if unicode.IsLetter(rune(ch)) || ch == '_' {
		return t.readIdent()
	}

	return t.readOperator(ch)
}

// isNumberStart returns true if ch starts a numeric token.
func (t *sqlTokeniser) isNumberStart(ch byte) bool {
	return unicode.IsDigit(rune(ch)) ||
		(ch == '-' && t.pos+1 < len(t.src) && unicode.IsDigit(rune(t.src[t.pos+1])))
}

// nextQuoteOrPunct handles quote characters and single-character punctuation tokens.
func (t *sqlTokeniser) nextQuoteOrPunct(ch byte) (bool, sqlToken, error) {
	switch ch {
	case '\'':
		tok, err := t.readString()

		return true, tok, err
	case '"':
		tok, err := t.readQuotedIdent()

		return true, tok, err
	case '*':
		t.pos++

		return true, sqlToken{typ: tokStar, val: "*"}, nil
	case ',':
		t.pos++

		return true, sqlToken{typ: tokComma, val: ","}, nil
	case '.':
		t.pos++

		return true, sqlToken{typ: tokDot, val: "."}, nil
	case '(':
		t.pos++

		return true, sqlToken{typ: tokLParen, val: "("}, nil
	case ')':
		t.pos++

		return true, sqlToken{typ: tokRParen, val: ")"}, nil
	case '=':
		t.pos++

		return true, sqlToken{typ: tokEq, val: "="}, nil
	}

	return false, sqlToken{}, nil
}

func (t *sqlTokeniser) skipWhitespace() {
	for t.pos < len(t.src) && unicode.IsSpace(rune(t.src[t.pos])) {
		t.pos++
	}
}

func (t *sqlTokeniser) readOperator(ch byte) (sqlToken, error) {
	switch {
	case ch == '!' && t.pos+1 < len(t.src) && t.src[t.pos+1] == '=':
		t.pos += 2

		return sqlToken{typ: tokNeq, val: "!="}, nil
	case ch == '<' && t.pos+1 < len(t.src) && t.src[t.pos+1] == '>':
		t.pos += 2

		return sqlToken{typ: tokNeq, val: "<>"}, nil
	case ch == '<' && t.pos+1 < len(t.src) && t.src[t.pos+1] == '=':
		t.pos += 2

		return sqlToken{typ: tokLte, val: "<="}, nil
	case ch == '>' && t.pos+1 < len(t.src) && t.src[t.pos+1] == '=':
		t.pos += 2

		return sqlToken{typ: tokGte, val: ">="}, nil
	case ch == '<':
		t.pos++

		return sqlToken{typ: tokLt, val: "<"}, nil
	case ch == '>':
		t.pos++

		return sqlToken{typ: tokGt, val: ">"}, nil
	}

	return sqlToken{}, fmt.Errorf(
		"unexpected character %q at position %d: %w",
		ch,
		t.pos,
		errUnexpectedChar,
	)
}

func (t *sqlTokeniser) readString() (sqlToken, error) {
	t.pos++ // skip opening quote
	start := t.pos

	for t.pos < len(t.src) && t.src[t.pos] != '\'' {
		t.pos++
	}

	if t.pos >= len(t.src) {
		return sqlToken{}, errUnterminatedString
	}

	val := t.src[start:t.pos]
	t.pos++ // skip closing quote

	return sqlToken{typ: tokString, val: val}, nil
}

func (t *sqlTokeniser) readQuotedIdent() (sqlToken, error) {
	t.pos++ // skip opening quote
	start := t.pos

	for t.pos < len(t.src) && t.src[t.pos] != '"' {
		t.pos++
	}

	if t.pos >= len(t.src) {
		return sqlToken{}, errUnterminatedIdent
	}

	val := t.src[start:t.pos]
	t.pos++ // skip closing quote

	return sqlToken{typ: tokIdent, val: val}, nil
}

func (t *sqlTokeniser) readNumber() (sqlToken, error) {
	start := t.pos

	if t.pos < len(t.src) && t.src[t.pos] == '-' {
		t.pos++
	}

	for t.pos < len(t.src) && (unicode.IsDigit(rune(t.src[t.pos])) || t.src[t.pos] == '.') {
		t.pos++
	}

	return sqlToken{typ: tokNumber, val: t.src[start:t.pos]}, nil
}

func (t *sqlTokeniser) readIdent() (sqlToken, error) {
	start := t.pos

	for t.pos < len(t.src) {
		ch := t.src[t.pos]
		if !unicode.IsLetter(rune(ch)) && !unicode.IsDigit(rune(ch)) && ch != '_' {
			break
		}

		t.pos++
	}

	return sqlToken{typ: tokIdent, val: t.src[start:t.pos]}, nil
}
