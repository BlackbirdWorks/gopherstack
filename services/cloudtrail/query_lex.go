package cloudtrail

import "strings"

// sqlTokKind classifies a single lexed token of a CloudTrail Lake SQL
// statement (see query_parse.go for the grammar built on top of these).
type sqlTokKind int

const (
	sqlTokEOF sqlTokKind = iota
	sqlTokIdent
	sqlTokString
	sqlTokNumber
	sqlTokPunct
)

type sqlToken struct {
	text string
	kind sqlTokKind
}

// tokenizeLakeSQL lexes stmt into a flat token stream: bare identifiers
// (including dotted paths like userIdentity.arn), single-quoted string
// literals (a doubled single quote escapes an embedded one, per the SQL
// standard), integer literals, and the punctuation this grammar's subset
// needs: parens, comma, star, equals, not-equals, and angle-bracket
// not-equals.
// The second return is false for anything else (an unterminated string, a
// quoting style this lexer doesn't support, an unrecognized character) --
// callers treat that identically to any other unsupported construct: the
// query still reaches a terminal state, but FAILED, never a silent empty
// FINISHED (see query_exec.go).
func tokenizeLakeSQL(raw string) ([]sqlToken, bool) {
	stmt := strings.TrimSuffix(strings.TrimSpace(raw), ";")
	runes := []rune(stmt)
	n := len(runes)

	var toks []sqlToken

	for i := 0; i < n; {
		if isSQLSpace(runes[i]) {
			i++

			continue
		}

		tok, next, ok := lexToken(runes, i)
		if !ok {
			return nil, false
		}

		toks, i = append(toks, tok), next
	}

	return toks, true
}

func isSQLSpace(r rune) bool {
	return r == ' ' || r == '\t' || r == '\n' || r == '\r'
}

// lexToken lexes exactly one non-whitespace token starting at runes[i].
func lexToken(runes []rune, i int) (sqlToken, int, bool) {
	switch r := runes[i]; {
	case r == '\'':
		return lexString(runes, i)
	case isIdentStart(r):
		tok, next := lexIdent(runes, i)

		return tok, next, true
	case r >= '0' && r <= '9':
		tok, next := lexNumber(runes, i)

		return tok, next, true
	default:
		return lexPunct(runes, i)
	}
}

// twoCharPunctLen is the token length of the two-character punctuation this
// lexer recognizes (!=, <>).
const twoCharPunctLen = 2

// lexPunct lexes a single- or double-character punctuation token
// ( ) , * = != <>, or reports failure for anything else.
func lexPunct(runes []rune, i int) (sqlToken, int, bool) {
	n := len(runes)
	r := runes[i]

	switch {
	case strings.ContainsRune("(),*=", r):
		return sqlToken{kind: sqlTokPunct, text: string(r)}, i + 1, true
	case r == '!' && i+1 < n && runes[i+1] == '=':
		return sqlToken{kind: sqlTokPunct, text: "!="}, i + twoCharPunctLen, true
	case r == '<' && i+1 < n && runes[i+1] == '>':
		return sqlToken{kind: sqlTokPunct, text: "<>"}, i + twoCharPunctLen, true
	default:
		return sqlToken{}, 0, false
	}
}

func isIdentStart(r rune) bool {
	return r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
}

// isIdentPart's character set is wider than a typical SQL identifier so a
// bare FROM-clause target -- an event data store ARN or ID, e.g.
// "arn:aws:cloudtrail:us-east-1:123456789012:eventdatastore/12345678-...",
// or a test fixture like "eds-000001" -- lexes as a single IDENT token
// without a dedicated ARN grammar; column names (letters/digits/underscore/
// dot) are a subset of this and unaffected.
func isIdentPart(r rune) bool {
	return isIdentStart(r) || (r >= '0' && r <= '9') ||
		r == '.' || r == '-' || r == ':' || r == '/'
}

func lexIdent(runes []rune, start int) (sqlToken, int) {
	i := start
	for i < len(runes) && isIdentPart(runes[i]) {
		i++
	}

	return sqlToken{kind: sqlTokIdent, text: string(runes[start:i])}, i
}

func lexNumber(runes []rune, start int) (sqlToken, int) {
	i := start
	for i < len(runes) && ((runes[i] >= '0' && runes[i] <= '9') || runes[i] == '.') {
		i++
	}

	return sqlToken{kind: sqlTokNumber, text: string(runes[start:i])}, i
}

// lexString scans a single-quoted string literal starting at runes[start]
// (the opening quote), returning the token, the index just past the closing
// quote, and false if the string is unterminated.
func lexString(runes []rune, start int) (sqlToken, int, bool) {
	var b strings.Builder

	n := len(runes)
	i := start + 1

	for i < n {
		if runes[i] == '\'' {
			if i+1 < n && runes[i+1] == '\'' {
				b.WriteRune('\'')
				i += 2

				continue
			}

			return sqlToken{kind: sqlTokString, text: b.String()}, i + 1, true
		}

		b.WriteRune(runes[i])
		i++
	}

	return sqlToken{}, 0, false
}
