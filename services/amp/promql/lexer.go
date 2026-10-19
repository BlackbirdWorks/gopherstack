package promql

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode"
)

type tokKind int

const (
	tokEOF tokKind = iota
	tokIdent
	tokNumber
	tokDuration
	tokString
	tokOp
)

type token struct {
	text string
	num  float64
	dur  time.Duration
	kind tokKind
	pos  int
}

// ErrSyntax marks a parse failure.
var ErrSyntax = errors.New("parse error")

func syntaxErr(pos int, format string, args ...any) error {
	return fmt.Errorf("%w: at position %d: %s", ErrSyntax, pos, fmt.Sprintf(format, args...))
}

var durationUnits = []struct { //nolint:gochecknoglobals // immutable lookup, longest suffix first
	suffix string
	d      time.Duration
}{
	{"ms", time.Millisecond},
	{"s", time.Second},
	{"m", time.Minute},
	{"h", time.Hour},
	{"d", 24 * time.Hour},
	{"w", 7 * 24 * time.Hour},
	{"y", 365 * 24 * time.Hour},
}

// ParseDuration parses a PromQL duration such as 5m or 1h30m.
func ParseDuration(s string) (time.Duration, error) {
	if s == "" {
		return 0, fmt.Errorf("%w: empty duration", ErrSyntax)
	}

	var total time.Duration

	rest := s

	for rest != "" {
		i := 0
		for i < len(rest) && rest[i] >= '0' && rest[i] <= '9' {
			i++
		}

		if i == 0 {
			return 0, fmt.Errorf("%w: invalid duration %q", ErrSyntax, s)
		}

		n, err := strconv.ParseInt(rest[:i], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("%w: invalid duration %q", ErrSyntax, s)
		}

		rest = rest[i:]
		matched := false

		for _, u := range durationUnits {
			if strings.HasPrefix(rest, u.suffix) && (u.suffix != "m" || !strings.HasPrefix(rest, "ms")) {
				total += time.Duration(n) * u.d
				rest = rest[len(u.suffix):]
				matched = true

				break
			}
		}

		if !matched {
			return 0, fmt.Errorf("%w: unknown unit in duration %q", ErrSyntax, s)
		}
	}

	return total, nil
}

type lexer struct {
	src  string
	toks []token
	pos  int
}

func tokenize(src string) ([]token, error) {
	lx := &lexer{src: src}

	for {
		lx.skipSpace()

		if lx.pos >= len(lx.src) {
			lx.toks = append(lx.toks, token{kind: tokEOF, pos: lx.pos})

			return lx.toks, nil
		}

		if err := lx.next(); err != nil {
			return nil, err
		}
	}
}

func (lx *lexer) skipSpace() {
	for lx.pos < len(lx.src) {
		c := lx.src[lx.pos]

		switch {
		case c == '#':
			for lx.pos < len(lx.src) && lx.src[lx.pos] != '\n' {
				lx.pos++
			}
		case unicode.IsSpace(rune(c)):
			lx.pos++
		default:
			return
		}
	}
}

func isIdentStart(c byte) bool {
	return c == '_' || c == ':' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

func (lx *lexer) next() error {
	c := lx.src[lx.pos]

	switch {
	case isIdentStart(c):
		lx.ident()
	case isDigit(c) || (c == '.' && lx.pos+1 < len(lx.src) && isDigit(lx.src[lx.pos+1])):
		return lx.number()
	case c == '"' || c == '\'' || c == '`':
		return lx.str(c)
	default:
		return lx.operator()
	}

	return nil
}

func (lx *lexer) ident() {
	start := lx.pos
	for lx.pos < len(lx.src) && (isIdentStart(lx.src[lx.pos]) || isDigit(lx.src[lx.pos])) {
		lx.pos++
	}

	text := lx.src[start:lx.pos]

	switch strings.ToLower(text) {
	case "inf":
		lx.toks = append(lx.toks, token{kind: tokNumber, text: text, num: math.Inf(1), pos: start})
	case "nan":
		lx.toks = append(lx.toks, token{kind: tokNumber, text: text, num: math.NaN(), pos: start})
	default:
		lx.toks = append(lx.toks, token{kind: tokIdent, text: text, pos: start})
	}
}

func (lx *lexer) number() error {
	start := lx.pos
	lx.scanNumber()

	if lx.pos < len(lx.src) && isDurationUnit(lx.src[lx.pos]) && !strings.ContainsAny(lx.src[start:lx.pos], ".eE") {
		return lx.duration(start)
	}

	f, err := strconv.ParseFloat(lx.src[start:lx.pos], 64)
	if err != nil {
		return syntaxErr(start, "invalid number %q", lx.src[start:lx.pos])
	}

	lx.toks = append(lx.toks, token{kind: tokNumber, text: lx.src[start:lx.pos], num: f, pos: start})

	return nil
}

// scanNumber advances over digits, an optional fraction and an optional exponent.
func (lx *lexer) scanNumber() {
	for lx.pos < len(lx.src) && (isDigit(lx.src[lx.pos]) || lx.src[lx.pos] == '.') {
		lx.pos++
	}

	if lx.pos >= len(lx.src) || (lx.src[lx.pos] != 'e' && lx.src[lx.pos] != 'E') {
		return
	}

	j := lx.pos + 1
	if j < len(lx.src) && (lx.src[j] == '+' || lx.src[j] == '-') {
		j++
	}

	if j >= len(lx.src) || !isDigit(lx.src[j]) {
		return
	}

	for j < len(lx.src) && isDigit(lx.src[j]) {
		j++
	}

	lx.pos = j
}

func (lx *lexer) duration(start int) error {
	for lx.pos < len(lx.src) && (isDigit(lx.src[lx.pos]) || isDurationUnit(lx.src[lx.pos])) {
		lx.pos++
	}

	d, err := ParseDuration(lx.src[start:lx.pos])
	if err != nil {
		return syntaxErr(start, "invalid duration %q", lx.src[start:lx.pos])
	}

	lx.toks = append(lx.toks, token{kind: tokDuration, text: lx.src[start:lx.pos], dur: d, pos: start})

	return nil
}

func isDurationUnit(c byte) bool {
	return c == 's' || c == 'm' || c == 'h' || c == 'd' || c == 'w' || c == 'y'
}

func (lx *lexer) str(quote byte) error {
	start := lx.pos
	lx.pos++

	var sb strings.Builder

	for lx.pos < len(lx.src) {
		c := lx.src[lx.pos]

		switch {
		case c == quote:
			lx.pos++
			lx.toks = append(lx.toks, token{kind: tokString, text: sb.String(), pos: start})

			return nil
		case c == '\\' && quote != '`' && lx.pos+1 < len(lx.src):
			sb.WriteByte(unescape(lx.src[lx.pos+1]))
			lx.pos += 2
		default:
			sb.WriteByte(c)
			lx.pos++
		}
	}

	return syntaxErr(start, "unterminated string literal")
}

func unescape(c byte) byte {
	switch c {
	case 'n':
		return '\n'
	case 't':
		return '\t'
	case 'r':
		return '\r'
	default:
		return c
	}
}

var twoCharOps = []string{"==", "!=", ">=", "<=", "=~", "!~"} //nolint:gochecknoglobals // immutable lookup

func (lx *lexer) operator() error {
	for _, op := range twoCharOps {
		if strings.HasPrefix(lx.src[lx.pos:], op) {
			lx.toks = append(lx.toks, token{kind: tokOp, text: op, pos: lx.pos})
			lx.pos += 2

			return nil
		}
	}

	c := lx.src[lx.pos]
	if strings.IndexByte("+-*/%^=<>(){}[],", c) < 0 {
		return syntaxErr(lx.pos, "unexpected character %q", c)
	}

	lx.toks = append(lx.toks, token{kind: tokOp, text: string(c), pos: lx.pos})
	lx.pos++

	return nil
}
