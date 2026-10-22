package gremlin

import (
	"strconv"
	"strings"
)

type tokKind int

const (
	tEOF tokKind = iota
	tIdent
	tString
	tInt
	tFloat
	tPunct
)

type token struct {
	text string
	kind tokKind
	pos  int
}

func isIdentStart(c byte) bool {
	return c == '_' || c == '$' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

func lex(src string) ([]token, error) {
	var toks []token

	for i := 0; i < len(src); {
		if isSpace(src[i]) {
			i++

			continue
		}

		if src[i] == '/' && i+1 < len(src) && src[i+1] == '/' {
			i = skipLine(src, i)

			continue
		}

		t, n, err := lexOne(src, i)
		if err != nil {
			return nil, err
		}

		toks = append(toks, t)
		i = n
	}

	return append(toks, token{kind: tEOF, pos: len(src)}), nil
}

func isSpace(c byte) bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r' }

func lexOne(src string, i int) (token, int, error) {
	c := src[i]

	switch {
	case isIdentStart(c):
		j := i
		for j < len(src) && (isIdentStart(src[j]) || isDigit(src[j])) {
			j++
		}

		return token{kind: tIdent, text: src[i:j], pos: i}, j, nil
	case isDigit(c):
		t, n := lexNumber(src, i)

		return t, n, nil
	case c == '\'' || c == '"':
		return lexString(src, i)
	case strings.IndexByte("()[],.:;=-", c) >= 0:
		return token{kind: tPunct, text: string(c), pos: i}, i + 1, nil
	}

	return token{}, 0, malformed("Unexpected character '%c' at offset %d", c, i)
}

func skipLine(src string, i int) int {
	for i < len(src) && src[i] != '\n' {
		i++
	}

	return i
}

func digitsEnd(src string, j int) int {
	for j < len(src) && isDigit(src[j]) {
		j++
	}

	return j
}

// fractionEnd returns the end of a ".digits" fraction starting at j, or j when there is none.
func fractionEnd(src string, j int) int {
	if j+1 < len(src) && src[j] == '.' && isDigit(src[j+1]) {
		return digitsEnd(src, j+1)
	}

	return j
}

// exponentEnd returns the end of an "e[+-]digits" exponent starting at j, or j when there is none.
func exponentEnd(src string, j int) int {
	if j >= len(src) || (src[j] != 'e' && src[j] != 'E') {
		return j
	}

	k := j + 1
	if k < len(src) && (src[k] == '+' || src[k] == '-') {
		k++
	}

	if k < len(src) && isDigit(src[k]) {
		return digitsEnd(src, k)
	}

	return j
}

func lexNumber(src string, i int) (token, int) {
	j := digitsEnd(src, i)
	j = fractionEnd(src, j)
	j = exponentEnd(src, j)
	isFloat := strings.ContainsAny(src[i:j], ".eE")
	text := src[i:j]

	switch {
	case j < len(src) && strings.IndexByte("lLiIsSbB", src[j]) >= 0:
		j++
	case j < len(src) && strings.IndexByte("fFdDgG", src[j]) >= 0:
		isFloat = true
		j++
	}

	kind := tInt
	if isFloat {
		kind = tFloat
	}

	return token{kind: kind, text: text, pos: i}, j
}

func lexString(src string, i int) (token, int, error) {
	quote := src[i]

	var sb strings.Builder

	for j := i + 1; j < len(src); j++ {
		c := src[j]

		switch {
		case c == quote:
			return token{kind: tString, text: sb.String(), pos: i}, j + 1, nil
		case c == '\\' && j+1 < len(src):
			j++
			sb.WriteString(unescape(src, &j))
		default:
			sb.WriteByte(c)
		}
	}

	return token{}, 0, malformed("Unterminated string literal at offset %d", i)
}

func unescape(src string, j *int) string {
	switch c := src[*j]; c {
	case 'n':
		return "\n"
	case 't':
		return "\t"
	case 'r':
		return "\r"
	case 'u':
		if *j+4 < len(src) {
			if r, err := strconv.ParseUint(src[*j+1:*j+5], 16, 32); err == nil {
				*j += 4

				return string(rune(r)) //nolint:gosec // 16-bit code point
			}
		}

		return "u"
	default:
		return string(c)
	}
}
