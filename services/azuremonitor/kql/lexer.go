package kql

import (
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

type tokKind int

const (
	tkEOF tokKind = iota
	tkIdent
	tkInt
	tkReal
	tkString
	tkTimespan
	tkOp
	tkTyped
)

type token struct {
	text string
	// raw holds the typed-literal body for tkTyped (text is the type name).
	raw  string
	kind tokKind
	pos  int
}

var hyphenated = map[string]bool{ //nolint:gochecknoglobals // static lookup
	"project-away": true, "project-keep": true, "project-rename": true, "project-reorder": true,
	"mv-expand": true, "mv-apply": true, "top-nested": true, "top-hitters": true, "make-series": true,
	"project-smart": true,
}

var typedPrefixes = map[string]bool{ //nolint:gochecknoglobals // static lookup
	kwDatetime: true, string(TTimespan): true, kwTime: true, kwGUID: true, kwDynamic: true, kwBool: true,
	kwBoolean: true, string(TLong): true, string(TInt): true, string(TReal): true, kwDouble: true, kwDecimal: true,
}

var timeUnits = map[string]time.Duration{ //nolint:gochecknoglobals // static lookup
	"d": dayDuration, "day": dayDuration, "days": dayDuration,
	"h": time.Hour, "hr": time.Hour, "hrs": time.Hour, "hour": time.Hour, "hours": time.Hour,
	"m": time.Minute, "min": time.Minute, "minute": time.Minute, "minutes": time.Minute,
	"s": time.Second, "sec": time.Second, "second": time.Second, "seconds": time.Second,
	"ms": time.Millisecond, "milli": time.Millisecond, "millis": time.Millisecond,
	"millisecond": time.Millisecond, "milliseconds": time.Millisecond,
	"microsecond": time.Microsecond, "microseconds": time.Microsecond,
	"tick": nanosPerTick * time.Nanosecond, "ticks": nanosPerTick * time.Nanosecond,
}

func isIdentStart(r rune) bool { return r == '_' || unicode.IsLetter(r) }
func isIdentPart(r rune) bool  { return isIdentStart(r) || unicode.IsDigit(r) }

type lexer struct {
	src  string
	toks []token
	pos  int
}

func lex(src string) ([]token, error) {
	l := &lexer{src: src}

	for {
		l.skipSpace()

		if l.pos >= len(l.src) {
			l.toks = append(l.toks, token{kind: tkEOF, pos: l.pos})

			return l.toks, nil
		}

		if err := l.next(); err != nil {
			return nil, err
		}
	}
}

func (l *lexer) skipSpace() {
	for l.pos < len(l.src) {
		r, w := utf8.DecodeRuneInString(l.src[l.pos:])

		switch {
		case unicode.IsSpace(r):
			l.pos += w
		case strings.HasPrefix(l.src[l.pos:], "//"):
			for l.pos < len(l.src) && l.src[l.pos] != '\n' {
				l.pos++
			}
		default:
			return
		}
	}
}

func (l *lexer) peekByte(off int) byte {
	if l.pos+off < len(l.src) {
		return l.src[l.pos+off]
	}

	return 0
}

func (l *lexer) next() error {
	start := l.pos
	r, w := utf8.DecodeRuneInString(l.src[l.pos:])

	switch {
	case isIdentStart(r):
		return l.lexIdent()
	case r >= '0' && r <= '9':
		return l.lexNumber()
	case r == '"' || r == '\'':
		s, err := l.lexQuoted(false)
		if err != nil {
			return err
		}

		l.toks = append(l.toks, token{kind: tkString, text: s, pos: start})

		return nil
	case r == '@' && (l.peekByte(1) == '"' || l.peekByte(1) == '\''):
		l.pos++

		s, err := l.lexQuoted(true)
		if err != nil {
			return err
		}

		l.toks = append(l.toks, token{kind: tkString, text: s, pos: start})

		return nil
	}

	for _, op := range []string{"==", "!=", "=~", "!~", "<=", ">=", "<>", "..", "=>"} {
		if strings.HasPrefix(l.src[l.pos:], op) {
			l.pos += len(op)
			l.toks = append(l.toks, token{kind: tkOp, text: op, pos: start})

			return nil
		}
	}

	if strings.ContainsRune("|=<>+-*/%(),;[].!:{}~", r) {
		l.pos += w
		l.toks = append(l.toks, token{kind: tkOp, text: string(r), pos: start})

		return nil
	}

	return syntaxErr("unexpected character %q at offset %d", r, start)
}

func (l *lexer) scanWord() {
	for l.pos < len(l.src) {
		r, w := utf8.DecodeRuneInString(l.src[l.pos:])
		if !isIdentPart(r) {
			return
		}

		l.pos += w
	}
}

// scanHyphenated extends a word such as "project" with "-away" when the combination is a known operator.
func (l *lexer) scanHyphenated(start int) string {
	if l.peekByte(0) != '-' {
		return l.src[start:l.pos]
	}

	j := l.pos + 1
	for j < len(l.src) && isIdentPart(rune(l.src[j])) {
		j++
	}

	if hyphenated[strings.ToLower(l.src[start:j])] {
		l.pos = j
	}

	return l.src[start:l.pos]
}

func (l *lexer) lexIdent() error {
	start := l.pos

	l.scanWord()

	word := l.scanHyphenated(start)
	lower := strings.ToLower(word)

	if lower == "h" && (l.peekByte(0) == '"' || l.peekByte(0) == '\'') {
		s, err := l.lexQuoted(false)
		if err != nil {
			return err
		}

		l.toks = append(l.toks, token{kind: tkString, text: s, pos: start})

		return nil
	}

	if lower == "in" && l.peekByte(0) == '~' {
		l.pos++
		word = kwInTilde
	}

	if typedPrefixes[lower] && l.peekByte(0) == '(' {
		raw, err := l.captureParens()
		if err != nil {
			return err
		}

		l.toks = append(l.toks, token{kind: tkTyped, text: lower, raw: raw, pos: start})

		return nil
	}

	l.toks = append(l.toks, token{kind: tkIdent, text: word, pos: start})

	return nil
}

// captureParens consumes "(...)" and returns the body.
func (l *lexer) captureParens() (string, error) {
	depth := 0
	begin := l.pos

	for l.pos < len(l.src) {
		c := l.src[l.pos]

		switch c {
		case '(':
			depth++
		case ')':
			depth--

			if depth == 0 {
				body := l.src[begin+1 : l.pos]
				l.pos++

				return body, nil
			}
		case '"', '\'':
			if _, err := l.lexQuoted(false); err != nil {
				return "", err
			}

			continue
		}

		l.pos++
	}

	return "", syntaxErr("unbalanced parenthesis in literal")
}

func (l *lexer) isDigitAt(off int) bool {
	c := l.peekByte(off)

	return c >= '0' && c <= '9'
}

func (l *lexer) scanDigits() {
	for l.isDigitAt(0) {
		l.pos++
	}
}

// scanReal consumes an optional fraction and exponent and reports whether either was present.
const expPrefixLen = 2

func (l *lexer) scanReal() bool {
	isReal := false

	if l.peekByte(0) == '.' && l.isDigitAt(1) {
		isReal = true
		l.pos++
		l.scanDigits()
	}

	c := l.peekByte(0)
	if c != 'e' && c != 'E' {
		return isReal
	}

	signed := (l.peekByte(1) == '-' || l.peekByte(1) == '+') && l.isDigitAt(expPrefixLen)
	if l.isDigitAt(1) || signed {
		isReal = true
		l.pos += expPrefixLen
		l.scanDigits()
	}

	return isReal
}

func (l *lexer) lexNumber() error {
	start := l.pos

	l.scanDigits()

	isReal := l.scanReal()
	num := l.src[start:l.pos]

	ustart := l.pos
	for l.pos < len(l.src) && unicode.IsLetter(rune(l.src[l.pos])) {
		l.pos++
	}

	if suffix := l.src[ustart:l.pos]; suffix != "" {
		d, ok := timeUnits[strings.ToLower(suffix)]
		if !ok {
			return syntaxErr("unknown numeric suffix %q", suffix)
		}

		f, _ := strconv.ParseFloat(num, 64)
		l.toks = append(l.toks, token{kind: tkTimespan, text: strconv.FormatInt(int64(f*float64(d)), 10), pos: start})

		return nil
	}

	kind := tkInt
	if isReal {
		kind = tkReal
	}

	l.toks = append(l.toks, token{kind: kind, text: num, pos: start})

	return nil
}

var simpleEscapes = map[byte]byte{'n': '\n', 't': '\t', 'r': '\r'} //nolint:gochecknoglobals // static lookup

const unicodeEscapeLen = 4

// readEscape consumes the escape sequence after a backslash (l.pos is on the escape character).
func (l *lexer) readEscape(sb *strings.Builder) {
	e := l.src[l.pos]

	if r, ok := simpleEscapes[e]; ok {
		sb.WriteByte(r)

		return
	}

	if e == 'u' && l.pos+unicodeEscapeLen < len(l.src) {
		seq := l.src[l.pos-1 : l.pos+1+unicodeEscapeLen]
		if r, _, _, err := strconv.UnquoteChar(seq, '"'); err == nil {
			sb.WriteRune(r)

			l.pos += unicodeEscapeLen

			return
		}
	}

	sb.WriteByte(e)
}

func (l *lexer) lexQuoted(verbatim bool) (string, error) {
	quote := l.src[l.pos]
	l.pos++

	var sb strings.Builder

	for l.pos < len(l.src) {
		c := l.src[l.pos]

		switch {
		case c == quote && verbatim && l.peekByte(1) == quote:
			sb.WriteByte(quote)

			l.pos += 2
		case c == quote:
			l.pos++

			return sb.String(), nil
		case c == '\\' && !verbatim:
			if l.pos+1 >= len(l.src) {
				return "", syntaxErr("unterminated string literal")
			}

			l.pos++
			l.readEscape(&sb)
			l.pos++
		default:
			sb.WriteByte(c)

			l.pos++
		}
	}

	return "", syntaxErr("unterminated string literal")
}
