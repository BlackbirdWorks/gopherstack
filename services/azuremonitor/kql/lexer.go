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
	"datetime": true, "timespan": true, "time": true, "guid": true, "dynamic": true, "bool": true,
	"boolean": true, "long": true, "int": true, "real": true, "double": true, "decimal": true,
}

var timeUnits = map[string]time.Duration{ //nolint:gochecknoglobals // static lookup
	"d": 24 * time.Hour, "day": 24 * time.Hour, "days": 24 * time.Hour,
	"h": time.Hour, "hr": time.Hour, "hrs": time.Hour, "hour": time.Hour, "hours": time.Hour,
	"m": time.Minute, "min": time.Minute, "minute": time.Minute, "minutes": time.Minute,
	"s": time.Second, "sec": time.Second, "second": time.Second, "seconds": time.Second,
	"ms": time.Millisecond, "milli": time.Millisecond, "millis": time.Millisecond,
	"millisecond": time.Millisecond, "milliseconds": time.Millisecond,
	"microsecond": time.Microsecond, "microseconds": time.Microsecond,
	"tick": 100 * time.Nanosecond, "ticks": 100 * time.Nanosecond,
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

func (l *lexer) lexIdent() error {
	start := l.pos

	for l.pos < len(l.src) {
		r, w := utf8.DecodeRuneInString(l.src[l.pos:])
		if !isIdentPart(r) {
			break
		}

		l.pos += w
	}

	word := l.src[start:l.pos]

	if l.peekByte(0) == '-' {
		j := l.pos + 1
		for j < len(l.src) && (isIdentPart(rune(l.src[j]))) {
			j++
		}

		if hyphenated[strings.ToLower(l.src[start:j])] {
			word = l.src[start:j]
			l.pos = j
		}
	}

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
		word = "in~"
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

func (l *lexer) lexNumber() error {
	start := l.pos
	isReal := false

	for l.pos < len(l.src) && l.src[l.pos] >= '0' && l.src[l.pos] <= '9' {
		l.pos++
	}

	if l.peekByte(0) == '.' && l.peekByte(1) != '.' && l.peekByte(1) >= '0' && l.peekByte(1) <= '9' {
		isReal = true
		l.pos++

		for l.pos < len(l.src) && l.src[l.pos] >= '0' && l.src[l.pos] <= '9' {
			l.pos++
		}
	}

	if c := l.peekByte(0); (c == 'e' || c == 'E') &&
		(l.peekByte(1) >= '0' && l.peekByte(1) <= '9' ||
			(l.peekByte(1) == '-' || l.peekByte(1) == '+') && l.peekByte(2) >= '0' && l.peekByte(2) <= '9') {
		isReal = true
		l.pos += 2

		for l.pos < len(l.src) && l.src[l.pos] >= '0' && l.src[l.pos] <= '9' {
			l.pos++
		}
	}

	num := l.src[start:l.pos]

	ustart := l.pos
	for l.pos < len(l.src) && unicode.IsLetter(rune(l.src[l.pos])) {
		l.pos++
	}

	if unit := strings.ToLower(l.src[ustart:l.pos]); unit != "" {
		d, ok := timeUnits[unit]
		if !ok {
			return syntaxErr("unknown numeric suffix %q", l.src[ustart:l.pos])
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

func (l *lexer) lexQuoted(verbatim bool) (string, error) {
	quote := l.src[l.pos]
	l.pos++

	var sb strings.Builder

	for l.pos < len(l.src) {
		c := l.src[l.pos]

		switch {
		case c == quote:
			if verbatim && l.peekByte(1) == quote {
				sb.WriteByte(quote)

				l.pos += 2

				continue
			}

			l.pos++

			return sb.String(), nil
		case c == '\\' && !verbatim:
			if l.pos+1 >= len(l.src) {
				return "", syntaxErr("unterminated string literal")
			}

			l.pos++

			switch e := l.src[l.pos]; e {
			case 'n':
				sb.WriteByte('\n')
			case 't':
				sb.WriteByte('\t')
			case 'r':
				sb.WriteByte('\r')
			case 'u':
				if l.pos+4 < len(l.src) {
					if n, err := strconv.ParseUint(l.src[l.pos+1:l.pos+5], 16, 32); err == nil {
						sb.WriteRune(rune(n))

						l.pos += 4
					}
				}
			default:
				sb.WriteByte(e)
			}

			l.pos++
		default:
			sb.WriteByte(c)

			l.pos++
		}
	}

	return "", syntaxErr("unterminated string literal")
}
