package iot

import (
	"strings"
	"unicode"
)

type sqlTokKind int

const (
	tokEOF sqlTokKind = iota
	tokIdent
	tokString
	tokNumber
	tokPunct
	tokBad
)

type sqlToken struct {
	text string
	kind sqlTokKind
	pos  int
}

// sqlLexer scans an IoT SQL statement on demand.
type sqlLexer struct {
	src string
	pos int
}

func (l *sqlLexer) skipSpace() {
	for l.pos < len(l.src) && unicode.IsSpace(rune(l.src[l.pos])) {
		l.pos++
	}
}

func isIdentStart(c byte) bool {
	return c == '_' || c == '$' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

func (l *sqlLexer) next() sqlToken {
	l.skipSpace()

	if l.pos >= len(l.src) {
		return sqlToken{kind: tokEOF, pos: l.pos}
	}

	start := l.pos
	c := l.src[l.pos]

	switch {
	case c == '@' && l.pos+1 < len(l.src) && isIdentStart(l.src[l.pos+1]):
		l.pos++

		for l.pos < len(l.src) && (isIdentStart(l.src[l.pos]) || isDigit(l.src[l.pos])) {
			l.pos++
		}

		return sqlToken{kind: tokIdent, text: l.src[start:l.pos], pos: start}
	case isIdentStart(c):
		for l.pos < len(l.src) && (isIdentStart(l.src[l.pos]) || isDigit(l.src[l.pos])) {
			l.pos++
		}

		return sqlToken{kind: tokIdent, text: l.src[start:l.pos], pos: start}
	case isDigit(c):
		return l.number()
	case c == '\'' || c == '"':
		return l.quoted(c)
	}

	return l.punct()
}

func (l *sqlLexer) number() sqlToken {
	start := l.pos

	l.digits()

	if l.pos+1 < len(l.src) && l.src[l.pos] == '.' && isDigit(l.src[l.pos+1]) {
		l.pos++
		l.digits()
	}

	if l.pos < len(l.src) && (l.src[l.pos] == 'e' || l.src[l.pos] == 'E') {
		save := l.pos
		l.pos++

		if l.pos < len(l.src) && (l.src[l.pos] == '-' || l.src[l.pos] == '+') {
			l.pos++
		}

		if l.pos < len(l.src) && isDigit(l.src[l.pos]) {
			l.digits()
		} else {
			l.pos = save
		}
	}

	return sqlToken{kind: tokNumber, text: l.src[start:l.pos], pos: start}
}

func (l *sqlLexer) digits() {
	for l.pos < len(l.src) && isDigit(l.src[l.pos]) {
		l.pos++
	}
}

// quoted reads a string; a doubled quote or backslash escapes the quote char.
func (l *sqlLexer) quoted(q byte) sqlToken {
	start := l.pos
	l.pos++

	var sb strings.Builder

	for l.pos < len(l.src) {
		c := l.src[l.pos]

		switch {
		case c == '\\' && l.pos+1 < len(l.src):
			sb.WriteByte(l.src[l.pos+1])
			l.pos += 2
		case c == q && l.pos+1 < len(l.src) && l.src[l.pos+1] == q:
			sb.WriteByte(q)
			l.pos += 2
		case c == q:
			l.pos++

			return sqlToken{kind: tokString, text: sb.String(), pos: start}
		default:
			sb.WriteByte(c)
			l.pos++
		}
	}

	return sqlToken{kind: tokBad, text: "unterminated string", pos: start}
}

func (l *sqlLexer) punct() sqlToken {
	start := l.pos

	if l.pos+1 < len(l.src) {
		switch two := l.src[l.pos : l.pos+2]; two {
		case "<>", "<=", ">=", "!=":
			l.pos += 2

			return sqlToken{kind: tokPunct, text: two, pos: start}
		}
	}

	c := l.src[l.pos]
	l.pos++

	if strings.IndexByte("=<>+-*/%()[]{},.:", c) < 0 {
		return sqlToken{kind: tokBad, text: "unexpected character " + string(c), pos: start}
	}

	return sqlToken{kind: tokPunct, text: string(c), pos: start}
}

// topicFilter reads a quoted or bare FROM topic filter.
func (l *sqlLexer) topicFilter() (string, bool) {
	l.skipSpace()

	if l.pos >= len(l.src) {
		return "", false
	}

	if c := l.src[l.pos]; c == '\'' || c == '"' {
		t := l.quoted(c)

		return t.text, t.kind == tokString
	}

	start := l.pos

	for l.pos < len(l.src) && !unicode.IsSpace(rune(l.src[l.pos])) {
		l.pos++
	}

	return l.src[start:l.pos], l.pos > start
}
