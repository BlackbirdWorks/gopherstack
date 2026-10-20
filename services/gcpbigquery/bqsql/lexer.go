package bqsql

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"
)

type tokKind uint8

const (
	tkEOF tokKind = iota
	tkIdent
	tkQuoted
	tkString
	tkBytes
	tkInt
	tkFloat
	tkParam
	tkQuestion
	tkPunct
)

type token struct {
	text  string
	pos   int
	kind  tokKind
	space bool
}

// ErrSyntax is wrapped by every parse failure.
var ErrSyntax = errors.New("syntax error")

func syntaxErr(pos int, format string, args ...any) error {
	return fmt.Errorf("%w: %s at position %d", ErrSyntax, fmt.Sprintf(format, args...), pos)
}

func (t token) isKw(w string) bool { return t.kind == tkIdent && strings.EqualFold(t.text, w) }

func (t token) isPunct(p string) bool { return t.kind == tkPunct && t.text == p }

func isIdentStart(c byte) bool {
	return c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= utf8.RuneSelf
}

func isIdentPart(c byte) bool { return isIdentStart(c) || c >= '0' && c <= '9' }

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

func puncts() []string {
	return []string{
		"<<", ">>", "<=", ">=", "!=", "<>", "||", "=>", "(", ")", "[", "]", ",", ".", ";", "*", "+", "-", "/", "%",
		"=", "<", ">", "|", "&", "^", "~", ":",
	}
}

type lexer struct {
	src  string
	toks []token
	i    int
}

func lex(src string) ([]token, error) {
	l := &lexer{src: src}

	for {
		space := l.skipSpace()
		if l.i >= len(l.src) {
			l.toks = append(l.toks, token{kind: tkEOF, pos: l.i, space: space})

			return l.toks, nil
		}

		t, err := l.next()
		if err != nil {
			return nil, err
		}

		t.space = space
		l.toks = append(l.toks, t)
	}
}

func (l *lexer) skipSpace() bool {
	start := l.i

	for l.i < len(l.src) {
		c := l.src[l.i]

		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			l.i++
		case c == '#' || c == '-' && strings.HasPrefix(l.src[l.i:], "--"):
			for l.i < len(l.src) && l.src[l.i] != '\n' {
				l.i++
			}
		case c == '/' && strings.HasPrefix(l.src[l.i:], "/*"):
			end := strings.Index(l.src[l.i+2:], "*/")
			if end < 0 {
				l.i = len(l.src)
			} else {
				l.i += end + 4 //nolint:mnd // skip the comment delimiters
			}
		default:
			return l.i > start
		}
	}

	return l.i > start
}

func (l *lexer) next() (token, error) {
	c := l.src[l.i]
	pos := l.i

	switch {
	case c == '`':
		return l.backtick()
	case c == '\'' || c == '"':
		return l.stringTok(pos, l.i, false, false)
	case isDigit(c) || c == '.' && l.i+1 < len(l.src) && isDigit(l.src[l.i+1]):
		return l.number(), nil
	case c == '@':
		return l.param(), nil
	case c == '?':
		l.i++

		return token{kind: tkQuestion, text: "?", pos: pos}, nil
	case isIdentStart(c):
		return l.identOrPrefixed()
	}

	for _, p := range puncts() {
		if strings.HasPrefix(l.src[l.i:], p) {
			l.i += len(p)

			return token{kind: tkPunct, text: p, pos: pos}, nil
		}
	}

	return token{}, syntaxErr(pos, "unexpected character %q", c)
}

func (l *lexer) backtick() (token, error) {
	pos := l.i
	end := strings.IndexByte(l.src[l.i+1:], '`')

	if end < 0 {
		return token{}, syntaxErr(pos, "unterminated quoted identifier")
	}

	text := l.src[l.i+1 : l.i+1+end]
	l.i += end + 2 //nolint:mnd // both backticks

	return token{kind: tkQuoted, text: text, pos: pos}, nil
}

func (l *lexer) param() token {
	pos := l.i
	l.i++

	for l.i < len(l.src) && l.src[l.i] == '@' {
		l.i++
	}

	start := l.i
	for l.i < len(l.src) && isIdentPart(l.src[l.i]) {
		l.i++
	}

	return token{kind: tkParam, text: l.src[start:l.i], pos: pos}
}

func (l *lexer) number() token {
	pos := l.i

	if strings.HasPrefix(l.src[l.i:], "0x") || strings.HasPrefix(l.src[l.i:], "0X") {
		return l.hexNumber(pos)
	}

	isFloat := false

	for l.i < len(l.src) && (isDigit(l.src[l.i]) || l.src[l.i] == '.') {
		if l.src[l.i] == '.' {
			isFloat = true
		}

		l.i++
	}

	if end, ok := l.exponentEnd(); ok {
		l.i, isFloat = end, true
	}

	kind := tkInt
	if isFloat {
		kind = tkFloat
	}

	return token{kind: kind, text: l.src[pos:l.i], pos: pos}
}

func (l *lexer) hexNumber(pos int) token {
	l.i += hexPrefixLen

	for l.i < len(l.src) && strings.IndexByte("0123456789abcdefABCDEF", l.src[l.i]) >= 0 {
		l.i++
	}

	n, _ := strconv.ParseInt(l.src[pos+hexPrefixLen:l.i], hexBase, bitSize)

	return token{kind: tkInt, text: strconv.FormatInt(n, decimalBase), pos: pos}
}

// exponentEnd returns the end of an e/E exponent starting at the cursor, if one is present.
func (l *lexer) exponentEnd() (int, bool) {
	if l.i >= len(l.src) || l.src[l.i] != 'e' && l.src[l.i] != 'E' {
		return 0, false
	}

	j := l.i + 1
	if j < len(l.src) && (l.src[j] == '+' || l.src[j] == '-') {
		j++
	}

	if j >= len(l.src) || !isDigit(l.src[j]) {
		return 0, false
	}

	for j < len(l.src) && isDigit(l.src[j]) {
		j++
	}

	return j, true
}

func (l *lexer) identOrPrefixed() (token, error) {
	pos := l.i
	start := l.i

	for l.i < len(l.src) && isIdentPart(l.src[l.i]) {
		l.i++
	}

	word := l.src[start:l.i]
	lw := strings.ToLower(word)

	if l.i < len(l.src) && (l.src[l.i] == '\'' || l.src[l.i] == '"') {
		switch lw {
		case "r":
			return l.stringTok(pos, l.i, true, false)
		case "b":
			return l.stringTok(pos, l.i, false, true)
		case "rb", "br":
			return l.stringTok(pos, l.i, true, true)
		}
	}

	return token{kind: tkIdent, text: word, pos: pos}, nil
}

func (l *lexer) stringTok(pos, at int, raw, isBytes bool) (token, error) {
	quote := l.src[at]
	delim := string(quote)

	if strings.HasPrefix(l.src[at:], strings.Repeat(delim, 3)) { //nolint:mnd // triple-quoted string
		delim = strings.Repeat(delim, 3) //nolint:mnd // triple-quoted string
	}

	var sb strings.Builder

	i := at + len(delim)

	for {
		if i >= len(l.src) {
			return token{}, syntaxErr(pos, "unterminated string literal")
		}

		if strings.HasPrefix(l.src[i:], delim) {
			i += len(delim)

			break
		}

		if l.src[i] == '\n' && len(delim) == 1 {
			return token{}, syntaxErr(pos, "unterminated string literal")
		}

		if l.src[i] == '\\' && !raw {
			n, err := unescape(l.src, i, &sb, isBytes)
			if err != nil {
				return token{}, syntaxErr(i, "%v", err)
			}

			i = n

			continue
		}

		sb.WriteByte(l.src[i])
		i++
	}

	l.i = i
	kind := tkString

	if isBytes {
		kind = tkBytes
	}

	return token{kind: kind, text: sb.String(), pos: pos}, nil
}

const (
	hexPrefixLen = 2
	hexBase      = 16
	escapeLen    = 2
	maxRune      = 0x10FFFF
)

var errEscape = errors.New("invalid escape sequence")

const (
	widthByte  = 2
	widthShort = 4
	widthLong  = 8
)

func escapeWidth(c byte) int {
	switch c {
	case 'x', 'X':
		return widthByte
	case 'u':
		return widthShort
	case 'U':
		return widthLong
	}

	return 0
}

func escapeTable() map[byte]byte {
	return map[byte]byte{
		'n': '\n', 't': '\t', 'r': '\r', 'a': '\a', 'b': '\b', 'f': '\f', 'v': '\v', '\\': '\\', '\'': '\'', '"': '"',
		'`': '`', '?': '?',
	}
}

func unescape(src string, i int, sb *strings.Builder, isBytes bool) (int, error) {
	if i+1 >= len(src) {
		return 0, errEscape
	}

	c := src[i+1]

	if r, ok := escapeTable()[c]; ok {
		sb.WriteByte(r)

		return i + escapeLen, nil
	}

	width := escapeWidth(c)
	if width == 0 || i+escapeLen+width > len(src) {
		return 0, errEscape
	}

	n, err := strconv.ParseUint(src[i+escapeLen:i+escapeLen+width], hexBase, bitSize)
	if err != nil || n > maxRune {
		return 0, errEscape
	}

	if (c == 'x' || c == 'X') && isBytes {
		sb.WriteByte(byte(n)) //nolint:gosec // two hex digits fit in a byte
	} else {
		sb.WriteRune(rune(n))
	}

	return i + escapeLen + width, nil
}
