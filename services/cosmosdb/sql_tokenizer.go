package cosmosdb

import "strings"

// sql_tokenizer.go implements a hand-written lexer for the Cosmos DB SQL
// query subset (see sql_parser.go's grammar comment and sql_exec.go for the
// executor), modeled on services/azuretable's odata_filter.go lexer and
// services/s3's select_sql_tokenizer.go -- gopherstack's two existing
// precedents for a hand-written SQL/filter tokenizer -> recursive-descent
// parser -> AST -> executor pipeline. Keywords are matched
// case-insensitively (real Cosmos SQL keywords are case-insensitive);
// property identifiers are matched case-sensitively (Cosmos documents are
// case-sensitive JSON).

// sqlTokenType enumerates the lexical token kinds a Cosmos SQL query uses.
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
	sqlTokEq
	sqlTokNeq
	sqlTokLt
	sqlTokLte
	sqlTokGt
	sqlTokGte
	// Keywords.
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
)

// sqlToken is one lexical token.
type sqlToken struct {
	lit string
	typ sqlTokenType
}

// sqlKeywords maps a lowercased keyword spelling to its token type.
var sqlKeywords = map[string]sqlTokenType{ //nolint:gochecknoglobals // fixed keyword table
	"select": sqlTokSelect,
	"from":   sqlTokFrom,
	"where":  sqlTokWhere,
	"as":     sqlTokAs,
	"top":    sqlTokTop,
	"order":  sqlTokOrder,
	"by":     sqlTokBy,
	"asc":    sqlTokAsc,
	"desc":   sqlTokDesc,
	"offset": sqlTokOffset,
	"limit":  sqlTokLimit,
	"and":    sqlTokAnd,
	"or":     sqlTokOr,
	"not":    sqlTokNot,
	"is":     sqlTokIs,
	"null":   sqlTokNull,
	"true":   sqlTokTrue,
	"false":  sqlTokFalse,
}

// sqlLexer tokenizes a Cosmos SQL query string.
type sqlLexer struct {
	input string
	pos   int
}

func newSQLLexer(s string) *sqlLexer { return &sqlLexer{input: s} }

//nolint:cyclop // straightforward per-character-class dispatch; splitting would obscure it
func (l *sqlLexer) next() sqlToken {
	l.skipSpace()

	if l.pos >= len(l.input) {
		return sqlToken{typ: sqlTokEOF}
	}

	ch := l.input[l.pos]

	switch {
	case ch == '(':
		l.pos++

		return sqlToken{typ: sqlTokLParen, lit: "("}
	case ch == ')':
		l.pos++

		return sqlToken{typ: sqlTokRParen, lit: ")"}
	case ch == ',':
		l.pos++

		return sqlToken{typ: sqlTokComma, lit: ","}
	case ch == '.':
		l.pos++

		return sqlToken{typ: sqlTokDot, lit: "."}
	case ch == '*':
		l.pos++

		return sqlToken{typ: sqlTokStar, lit: "*"}
	case ch == '@':
		return l.readParam()
	case ch == '\'' || ch == '"':
		return l.readString(ch)
	case ch == '-' || isSQLDigit(ch):
		return l.readNumber()
	case isSQLAlpha(ch):
		return l.readWord()
	case ch == '=':
		l.pos++

		return sqlToken{typ: sqlTokEq, lit: "="}
	case ch == '!':
		return l.readBangOp()
	case ch == '<':
		return l.readLtOp()
	case ch == '>':
		l.pos++

		if l.pos < len(l.input) && l.input[l.pos] == '=' {
			l.pos++

			return sqlToken{typ: sqlTokGte, lit: ">="}
		}

		return sqlToken{typ: sqlTokGt, lit: ">"}
	default:
		l.pos++

		return sqlToken{typ: sqlTokError, lit: "unexpected character " + string(ch)}
	}
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

// readString consumes a quote-delimited string literal (quote is either '
// or ", per Cosmos SQL's own acceptance of both). Escaped by doubling: the
// delimiter written twice in a row means one literal occurrence.
func (l *sqlLexer) readString(quote byte) sqlToken {
	l.pos++

	buf := make([]byte, 0, len(l.input)-l.pos)

	for {
		if l.pos >= len(l.input) {
			return sqlToken{typ: sqlTokError, lit: "unterminated string literal"}
		}

		c := l.input[l.pos]

		if c == quote {
			if l.pos+1 < len(l.input) && l.input[l.pos+1] == quote {
				buf = append(buf, quote)
				l.pos += 2

				continue
			}

			l.pos++

			return sqlToken{typ: sqlTokString, lit: string(buf)}
		}

		buf = append(buf, c)
		l.pos++
	}
}

// readParam consumes an "@name"-shaped query parameter reference.
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

// readNumber reads an integer or floating-point literal, including an
// optional leading '-' and optional exponent (e.g. 1.5e10).
func (l *sqlLexer) readNumber() sqlToken {
	start := l.pos
	if l.input[l.pos] == '-' {
		l.pos++
	}

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

// readWord reads an identifier-shaped word and classifies it as a keyword
// or a plain identifier (case-insensitive keyword match, per Cosmos SQL).
func (l *sqlLexer) readWord() sqlToken {
	start := l.pos
	for l.pos < len(l.input) && isSQLAlnum(l.input[l.pos]) {
		l.pos++
	}

	word := l.input[start:l.pos]

	if typ, ok := sqlKeywords[strings.ToLower(word)]; ok {
		return sqlToken{typ: typ, lit: word}
	}

	return sqlToken{typ: sqlTokIdent, lit: word}
}

func isSQLDigit(ch byte) bool { return ch >= '0' && ch <= '9' }
func isSQLAlpha(ch byte) bool {
	return ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch == '_'
}
func isSQLAlnum(ch byte) bool { return isSQLAlpha(ch) || isSQLDigit(ch) }
