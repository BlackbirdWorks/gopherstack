package cosmosdb

import (
	"strings"
	"unicode/utf8"
)

// sqlTokenType enumerates the lexical token kinds of a Cosmos SQL query.
type sqlTokenType int

const (
	sqlTokEOF sqlTokenType = iota
	sqlTokError
	sqlTokIdent
	sqlTokParam
	sqlTokString
	sqlTokNumber
	sqlTokStar
	sqlTokComma
	sqlTokDot
	sqlTokLParen
	sqlTokRParen
	sqlTokLBracket
	sqlTokRBracket
	sqlTokLBrace
	sqlTokRBrace
	sqlTokColon
	sqlTokQuestion
	sqlTokCoalesce
	sqlTokPlus
	sqlTokMinus
	sqlTokSlash
	sqlTokPercent
	sqlTokConcat
	sqlTokEq
	sqlTokNeq
	sqlTokLt
	sqlTokLte
	sqlTokGt
	sqlTokGte
	// Keywords; sqlTokSelect must stay the first keyword (see isWordToken).
	sqlTokSelect
	sqlTokFrom
	sqlTokWhere
	sqlTokAs
	sqlTokTop
	sqlTokOrder
	sqlTokBy
	sqlTokAsc
	sqlTokDesc
	sqlTokOffset
	sqlTokLimit
	sqlTokAnd
	sqlTokOr
	sqlTokNot
	sqlTokIs
	sqlTokNull
	sqlTokTrue
	sqlTokFalse
	sqlTokJoin
	sqlTokIn
	sqlTokDistinct
	sqlTokGroup
	sqlTokValue
	sqlTokBetween
	sqlTokLike
	sqlTokEscape
	sqlTokUndefined
)

const keywordValue = "value"

type sqlToken struct {
	lit string
	typ sqlTokenType
}

// keywordType classifies a lowercased word as a keyword token type, or sqlTokIdent.
func keywordType(lower string) sqlTokenType {
	switch lower {
	case "select":
		return sqlTokSelect
	case "from":
		return sqlTokFrom
	case "where":
		return sqlTokWhere
	case "as":
		return sqlTokAs
	case "top":
		return sqlTokTop
	case "order":
		return sqlTokOrder
	case "by":
		return sqlTokBy
	case "asc":
		return sqlTokAsc
	}

	return keywordTypeMore(lower)
}

func keywordTypeMore(lower string) sqlTokenType {
	switch lower {
	case "desc":
		return sqlTokDesc
	case "offset":
		return sqlTokOffset
	case "limit":
		return sqlTokLimit
	case "and":
		return sqlTokAnd
	case "or":
		return sqlTokOr
	case "not":
		return sqlTokNot
	case "is":
		return sqlTokIs
	case nullText:
		return sqlTokNull
	}

	return keywordTypeLast(lower)
}

func keywordTypeLast(lower string) sqlTokenType {
	switch lower {
	case trueText:
		return sqlTokTrue
	case "false":
		return sqlTokFalse
	case "join":
		return sqlTokJoin
	case "in":
		return sqlTokIn
	case "distinct":
		return sqlTokDistinct
	case "group":
		return sqlTokGroup
	case keywordValue:
		return sqlTokValue
	case "between":
		return sqlTokBetween
	case "like":
		return sqlTokLike
	case "escape":
		return sqlTokEscape
	case "undefined":
		return sqlTokUndefined
	default:
		return sqlTokIdent
	}
}

// isWordToken reports whether t is an identifier or a keyword, so keywords can be property names after '.'.
func isWordToken(t sqlTokenType) bool { return t == sqlTokIdent || t >= sqlTokSelect }

type sqlLexer struct {
	input string
	pos   int
}

func newSQLLexer(s string) *sqlLexer { return &sqlLexer{input: s} }

func punctuationType(ch byte) (sqlTokenType, bool) {
	switch ch {
	case '(':
		return sqlTokLParen, true
	case ')':
		return sqlTokRParen, true
	case '[':
		return sqlTokLBracket, true
	case ']':
		return sqlTokRBracket, true
	case '{':
		return sqlTokLBrace, true
	case '}':
		return sqlTokRBrace, true
	case ',':
		return sqlTokComma, true
	case '.':
		return sqlTokDot, true
	}

	return operatorType(ch)
}

func operatorType(ch byte) (sqlTokenType, bool) {
	switch ch {
	case '*':
		return sqlTokStar, true
	case '=':
		return sqlTokEq, true
	case ':':
		return sqlTokColon, true
	case '+':
		return sqlTokPlus, true
	case '-':
		return sqlTokMinus, true
	case '/':
		return sqlTokSlash, true
	case '%':
		return sqlTokPercent, true
	default:
		return sqlTokError, false
	}
}

func (l *sqlLexer) next() sqlToken {
	l.skipSpace()

	if l.pos >= len(l.input) {
		return sqlToken{typ: sqlTokEOF}
	}

	ch := l.input[l.pos]

	if typ, ok := punctuationType(ch); ok {
		l.pos++

		return sqlToken{typ: typ, lit: string(ch)}
	}

	return l.readLiteralOrOperator(ch)
}

func (l *sqlLexer) readLiteralOrOperator(ch byte) sqlToken {
	switch {
	case ch == '@':
		return l.readParam()
	case ch == '\'' || ch == '"':
		return l.readString(ch)
	case isSQLDigit(ch):
		return l.readNumber()
	case isSQLAlpha(ch):
		return l.readWord()
	case ch == '!':
		return l.readBangOp()
	case ch == '<':
		return l.readLtOp()
	case ch == '>':
		return l.readGtOp()
	case ch == '|':
		return l.readPairOp('|', sqlTokConcat, "expected ||")
	case ch == '?':
		return l.readQuestion()
	default:
		l.pos++

		return sqlToken{typ: sqlTokError, lit: "unexpected character " + string(ch)}
	}
}

func (l *sqlLexer) readQuestion() sqlToken {
	l.pos++

	if l.pos < len(l.input) && l.input[l.pos] == '?' {
		l.pos++

		return sqlToken{typ: sqlTokCoalesce, lit: "??"}
	}

	return sqlToken{typ: sqlTokQuestion, lit: "?"}
}

func (l *sqlLexer) readPairOp(second byte, typ sqlTokenType, errLit string) sqlToken {
	l.pos++

	if l.pos < len(l.input) && l.input[l.pos] == second {
		l.pos++

		return sqlToken{typ: typ, lit: string([]byte{second, second})}
	}

	return sqlToken{typ: sqlTokError, lit: errLit}
}

func (l *sqlLexer) readGtOp() sqlToken {
	l.pos++

	if l.pos < len(l.input) && l.input[l.pos] == '=' {
		l.pos++

		return sqlToken{typ: sqlTokGte, lit: ">="}
	}

	return sqlToken{typ: sqlTokGt, lit: ">"}
}

func (l *sqlLexer) readBangOp() sqlToken {
	l.pos++

	if l.pos < len(l.input) && l.input[l.pos] == '=' {
		l.pos++

		return sqlToken{typ: sqlTokNeq, lit: "!="}
	}

	return sqlToken{typ: sqlTokError, lit: "expected != "}
}

func (l *sqlLexer) readLtOp() sqlToken {
	l.pos++

	switch {
	case l.pos < len(l.input) && l.input[l.pos] == '=':
		l.pos++

		return sqlToken{typ: sqlTokLte, lit: "<="}
	case l.pos < len(l.input) && l.input[l.pos] == '>':
		l.pos++

		return sqlToken{typ: sqlTokNeq, lit: "<>"}
	default:
		return sqlToken{typ: sqlTokLt, lit: "<"}
	}
}

func (l *sqlLexer) skipSpace() {
	for l.pos < len(l.input) {
		switch l.input[l.pos] {
		case ' ', '\t', '\n', '\r':
			l.pos++
		default:
			return
		}
	}
}

// readString consumes a quote-delimited literal. A doubled delimiter or a backslash escape yields one literal char.
func (l *sqlLexer) readString(quote byte) sqlToken {
	l.pos++

	var buf []byte

	for {
		if l.pos >= len(l.input) {
			return sqlToken{typ: sqlTokError, lit: "unterminated string literal"}
		}

		c := l.input[l.pos]

		switch {
		case c == quote && l.pos+1 < len(l.input) && l.input[l.pos+1] == quote:
			buf = append(buf, quote)
			l.pos += 2
		case c == quote:
			l.pos++

			return sqlToken{typ: sqlTokString, lit: string(buf)}
		case c == '\\':
			var ok bool

			if buf, ok = l.appendEscape(buf); !ok {
				return sqlToken{typ: sqlTokError, lit: "invalid escape sequence in string literal"}
			}
		default:
			buf = append(buf, c)
			l.pos++
		}
	}
}

const (
	unicodeEscapeLen = 4
	hexRadix         = 16
	hexLetterOffset  = 10
)

func hexDigitValue(r rune) (rune, bool) {
	switch {
	case r >= '0' && r <= '9':
		return r - '0', true
	case r >= 'a' && r <= 'f':
		return r - 'a' + hexLetterOffset, true
	case r >= 'A' && r <= 'F':
		return r - 'A' + hexLetterOffset, true
	default:
		return 0, false
	}
}

// appendEscape consumes a backslash escape at l.pos and appends its value to buf.
func (l *sqlLexer) appendEscape(buf []byte) ([]byte, bool) {
	l.pos++

	if l.pos >= len(l.input) {
		return buf, false
	}

	c := l.input[l.pos]
	l.pos++

	switch c {
	case 'n':
		return append(buf, '\n'), true
	case 't':
		return append(buf, '\t'), true
	case 'r':
		return append(buf, '\r'), true
	case 'b':
		return append(buf, '\b'), true
	case 'f':
		return append(buf, '\f'), true
	case '\\', '\'', '"', '/':
		return append(buf, c), true
	case 'u':
		if l.pos+unicodeEscapeLen > len(l.input) {
			return buf, false
		}

		var code rune

		for _, d := range l.input[l.pos : l.pos+unicodeEscapeLen] {
			v, ok := hexDigitValue(d)
			if !ok {
				return buf, false
			}

			code = code*hexRadix + v
		}

		l.pos += unicodeEscapeLen

		return utf8.AppendRune(buf, code), true
	default:
		return buf, false
	}
}

func (l *sqlLexer) readParam() sqlToken {
	start := l.pos
	l.pos++

	for l.pos < len(l.input) && isSQLAlnum(l.input[l.pos]) {
		l.pos++
	}

	if l.pos == start+1 {
		return sqlToken{typ: sqlTokError, lit: "expected parameter name after @"}
	}

	return sqlToken{typ: sqlTokParam, lit: l.input[start:l.pos]}
}

// readNumber reads an unsigned integer or floating-point literal with an optional exponent.
func (l *sqlLexer) readNumber() sqlToken {
	start := l.pos

	for l.pos < len(l.input) && isSQLDigit(l.input[l.pos]) {
		l.pos++
	}

	if l.pos < len(l.input) && l.input[l.pos] == '.' && l.pos+1 < len(l.input) && isSQLDigit(l.input[l.pos+1]) {
		l.pos++

		for l.pos < len(l.input) && isSQLDigit(l.input[l.pos]) {
			l.pos++
		}
	}

	if l.pos < len(l.input) && (l.input[l.pos] == 'e' || l.input[l.pos] == 'E') {
		l.readExponent()
	}

	return sqlToken{typ: sqlTokNumber, lit: l.input[start:l.pos]}
}

func (l *sqlLexer) readExponent() {
	save := l.pos
	l.pos++

	if l.pos < len(l.input) && (l.input[l.pos] == '+' || l.input[l.pos] == '-') {
		l.pos++
	}

	if l.pos >= len(l.input) || !isSQLDigit(l.input[l.pos]) {
		l.pos = save

		return
	}

	for l.pos < len(l.input) && isSQLDigit(l.input[l.pos]) {
		l.pos++
	}
}

func (l *sqlLexer) readWord() sqlToken {
	start := l.pos
	for l.pos < len(l.input) && isSQLAlnum(l.input[l.pos]) {
		l.pos++
	}

	word := l.input[start:l.pos]

	return sqlToken{typ: keywordType(strings.ToLower(word)), lit: word}
}

func isSQLDigit(ch byte) bool { return ch >= '0' && ch <= '9' }
func isSQLAlpha(ch byte) bool {
	return ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch == '_'
}
func isSQLAlnum(ch byte) bool { return isSQLAlpha(ch) || isSQLDigit(ch) }
