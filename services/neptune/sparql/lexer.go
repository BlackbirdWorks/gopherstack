package sparql

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

type tokKind uint8

const (
	tEOF tokKind = iota
	tIRI
	tPName
	tVar
	tBlank
	tString
	tLang
	tInt
	tDecimal
	tDouble
	tIdent
	tPunct
)

type token struct {
	val  string
	kind tokKind
	pos  int
}

func (t token) is(p string) bool { return t.kind == tPunct && t.val == p }

func (t token) kw(k string) bool { return t.kind == tIdent && strings.EqualFold(t.val, k) }

// SyntaxError reports malformed query, update or RDF input.
type SyntaxError struct{ Msg string }

func (e *SyntaxError) Error() string { return e.Msg }

// NumericDataError is an integer literal beyond the 64-bit range Neptune stores.
type NumericDataError struct{ Msg string }

func (e *NumericDataError) Error() string { return e.Msg }

func syntaxf(format string, args ...any) error {
	return &SyntaxError{Msg: fmt.Sprintf(format, args...)}
}

type lexer struct {
	src string
	pos int
}

func isNameStart(r rune) bool { return r == '_' || unicode.IsLetter(r) }

func isNameChar(r rune) bool {
	return isNameStart(r) || unicode.IsDigit(r) || r == '-' || r == '·'
}

func lexAll(src string) ([]token, error) {
	l := &lexer{src: src}

	var out []token

	for {
		t, err := l.next()
		if err != nil {
			return nil, err
		}

		out = append(out, t)

		if t.kind == tEOF {
			return out, nil
		}
	}
}

func (l *lexer) skipSpace() {
	for l.pos < len(l.src) {
		c := l.src[l.pos]

		switch c {
		case ' ', '\t', '\n', '\r':
			l.pos++
		case '#':
			for l.pos < len(l.src) && l.src[l.pos] != '\n' {
				l.pos++
			}
		default:
			return
		}
	}
}

func (l *lexer) peekRune() (rune, int) {
	if l.pos >= len(l.src) {
		return 0, 0
	}

	return utf8.DecodeRuneInString(l.src[l.pos:])
}

func (l *lexer) tok(k tokKind, v string, start int) (token, error) {
	return token{kind: k, val: v, pos: start}, nil
}

func (l *lexer) next() (token, error) {
	l.skipSpace()

	start := l.pos
	if l.pos >= len(l.src) {
		return token{kind: tEOF, pos: start}, nil
	}

	c := l.src[l.pos]

	switch {
	case c == '<':
		return l.lexAngle(start)
	case c == '"' || c == '\'':
		return l.lexString(start)
	case c == '?' || c == '$':
		if v := l.scanName(l.pos + 1); v != "" {
			l.pos += 1 + len(v)

			return l.tok(tVar, v, start)
		}

		l.pos++

		return l.tok(tPunct, "?", start)
	case c == '@':
		v := l.scanName(l.pos + 1)
		if v == "" {
			return token{}, syntaxf("invalid language tag at offset %d", start)
		}

		l.pos += 1 + len(v)

		return l.tok(tLang, v, start)
	case c == '_' && l.at(1) == ':':
		v := l.scanLocal(l.pos + blankPrefixLen)
		l.pos += blankPrefixLen + len(v)

		return l.tok(tBlank, v, start)
	case isDigit(c), c == '.' && isDigit(l.at(1)):
		return l.lexNumber(start)
	}

	return l.lexOther(c, start)
}

const (
	blankPrefixLen = 2
	longQuoteLen   = 3
)

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

// at returns the byte n positions ahead of the cursor, or 0 past the end.
func (l *lexer) at(n int) byte {
	if l.pos+n >= len(l.src) {
		return 0
	}

	return l.src[l.pos+n]
}

func (l *lexer) lexAngle(start int) (token, error) {
	if end := l.iriEnd(); end > 0 {
		raw := l.src[l.pos+1 : end]
		l.pos = end + 1

		v, err := unescape(raw, true)
		if err != nil {
			return token{}, err
		}

		return l.tok(tIRI, v, start)
	}

	if l.at(1) == '=' {
		l.pos += 2

		return l.tok(tPunct, "<=", start)
	}

	l.pos++

	return l.tok(tPunct, "<", start)
}

func (l *lexer) lexOther(c byte, start int) (token, error) {
	if r, _ := l.peekRune(); isNameStart(r) || c == ':' {
		return l.lexName(start)
	}

	for _, p := range []string{"^^", "&&", "||", ">=", "!="} {
		if strings.HasPrefix(l.src[l.pos:], p) {
			l.pos += len(p)

			return l.tok(tPunct, p, start)
		}
	}

	if strings.ContainsRune("{}()[].,;*/+-^|!=>&", rune(c)) {
		l.pos++

		return l.tok(tPunct, string(c), start)
	}

	return token{}, syntaxf("unexpected character %q at offset %d", c, start)
}

// iriEnd returns the index of the closing '>' when '<' at l.pos opens an IRIREF, else -1.
func (l *lexer) iriEnd() int {
	for i := l.pos + 1; i < len(l.src); i++ {
		switch l.src[i] {
		case '>':
			return i
		case ' ', '\t', '\n', '\r', '<', '"', '{', '}', '|', '^', '`':
			return -1
		}
	}

	return -1
}

func (l *lexer) scanName(from int) string {
	i := from
	for i < len(l.src) {
		r, n := utf8.DecodeRuneInString(l.src[i:])
		if !isNameChar(r) {
			break
		}

		i += n
	}

	return l.src[from:i]
}

// scanLocal scans a blank-node label or local part: dots allowed inside but not trailing.
func (l *lexer) scanLocal(from int) string {
	i := from

	for i < len(l.src) {
		r, n := utf8.DecodeRuneInString(l.src[i:])

		switch {
		case isNameChar(r) || r == ':' || r == '%':
			i += n
		case r == '\\' && i+1 < len(l.src):
			i += 2
		case r == '.' && i+1 < len(l.src):
			nr, _ := utf8.DecodeRuneInString(l.src[i+1:])
			if isNameChar(nr) || nr == ':' || nr == '%' || nr == '.' {
				i += n

				continue
			}

			return strings.TrimRight(l.src[from:i], ".")
		default:
			return strings.TrimRight(l.src[from:i], ".")
		}
	}

	return strings.TrimRight(l.src[from:i], ".")
}

func (l *lexer) lexName(start int) (token, error) {
	i := l.pos

	for i < len(l.src) {
		r, n := utf8.DecodeRuneInString(l.src[i:])
		if isNameChar(r) {
			i += n

			continue
		}

		if r == '.' && i+1 < len(l.src) {
			nr, _ := utf8.DecodeRuneInString(l.src[i+1:])
			if isNameChar(nr) {
				if strings.ContainsRune(l.src[start:i], ':') || l.pnameAhead(i) {
					i += n

					continue
				}
			}
		}

		break
	}

	if i < len(l.src) && l.src[i] == ':' {
		prefix := l.src[start:i]
		local := l.scanLocal(i + 1)
		l.pos = i + 1 + len(local)

		return token{kind: tPName, val: prefix + ":" + local, pos: start}, nil
	}

	word := l.src[start:i]
	// a dotted word without ':' is a keyword followed by the end-of-statement dot
	if j := strings.IndexByte(word, '.'); j >= 0 {
		word = word[:j]
	}

	l.pos = start + len(word)

	return token{kind: tIdent, val: word, pos: start}, nil
}

// pnameAhead reports whether the dotted run starting at i ends in ':' (a prefix containing dots).
func (l *lexer) pnameAhead(i int) bool {
	for j := i; j < len(l.src); j++ {
		r, _ := utf8.DecodeRuneInString(l.src[j:])

		switch {
		case r == ':':
			return true
		case isNameChar(r) || r == '.':
		default:
			return false
		}
	}

	return false
}

func (l *lexer) digitsEnd(i int) int {
	for i < len(l.src) && isDigit(l.src[i]) {
		i++
	}

	return i
}

func (l *lexer) lexNumber(start int) (token, error) {
	i := l.digitsEnd(l.pos)
	kind := tInt

	switch {
	case i+1 < len(l.src) && l.src[i] == '.' && isDigit(l.src[i+1]):
		kind = tDecimal
		i = l.digitsEnd(i + 1)
	case i < len(l.src) && l.src[i] == '.' && i == l.pos:
		return token{}, syntaxf("invalid number at offset %d", start)
	}

	if end, ok := l.exponentEnd(i); ok {
		kind, i = tDouble, end
	}

	v := l.src[l.pos:i]
	l.pos = i

	return token{kind: kind, val: v, pos: start}, nil
}

// exponentEnd returns the end of an exponent part starting at i, if there is one.
func (l *lexer) exponentEnd(i int) (int, bool) {
	if i >= len(l.src) || (l.src[i] != 'e' && l.src[i] != 'E') {
		return 0, false
	}

	j := i + 1
	if j < len(l.src) && (l.src[j] == '+' || l.src[j] == '-') {
		j++
	}

	if j >= len(l.src) || !isDigit(l.src[j]) {
		return 0, false
	}

	return l.digitsEnd(j), true
}

func (l *lexer) lexString(start int) (token, error) {
	q := l.src[l.pos]
	long := strings.HasPrefix(l.src[l.pos:], strings.Repeat(string(q), longQuoteLen))

	var closer string

	if long {
		closer = strings.Repeat(string(q), longQuoteLen)
		l.pos += longQuoteLen
	} else {
		closer = string(q)
		l.pos++
	}

	bodyStart := l.pos

	for l.pos < len(l.src) {
		switch {
		case l.src[l.pos] == '\\':
			l.pos += 2

			continue
		case strings.HasPrefix(l.src[l.pos:], closer):
			raw := l.src[bodyStart:l.pos]
			l.pos += len(closer)

			v, err := unescape(raw, false)
			if err != nil {
				return token{}, err
			}

			return token{kind: tString, val: v, pos: start}, nil
		case !long && (l.src[l.pos] == '\n' || l.src[l.pos] == '\r'):
			return token{}, syntaxf("unterminated string at offset %d", start)
		}

		l.pos++
	}

	return token{}, syntaxf("unterminated string at offset %d", start)
}

func simpleEscape(c byte) (byte, bool) {
	switch c {
	case 't':
		return '\t', true
	case 'n':
		return '\n', true
	case 'r':
		return '\r', true
	case 'b':
		return '\b', true
	case 'f':
		return '\f', true
	case '"', '\'', '\\':
		return c, true
	default:
		return 0, false
	}
}

func unescape(s string, iri bool) (string, error) {
	if !strings.Contains(s, `\`) {
		return s, nil
	}

	var sb strings.Builder

	for i := 0; i < len(s); i++ {
		if s[i] != '\\' {
			sb.WriteByte(s[i])

			continue
		}

		i++
		if i >= len(s) {
			return "", syntaxf("dangling escape")
		}

		if s[i] == 'u' || s[i] == 'U' {
			r, _, tail, err := strconv.UnquoteChar(s[i-1:], 0)
			if err != nil {
				return "", syntaxf("bad unicode escape")
			}

			sb.WriteRune(r)

			i = len(s) - len(tail) - 1

			continue
		}

		c, ok := simpleEscape(s[i])
		if !ok || iri {
			return "", syntaxf("invalid escape \\%c", s[i])
		}

		sb.WriteByte(c)
	}

	return sb.String(), nil
}
