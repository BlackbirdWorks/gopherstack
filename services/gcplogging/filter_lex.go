package gcplogging

import (
	"strings"
)

type tokKind int

const (
	tokEOF tokKind = iota
	tokWord
	tokString
	tokLParen
	tokRParen
	tokComma
	tokOp
	tokMinus
)

type token struct {
	text string
	kind tokKind
	pos  int
	// spaceBefore is true when whitespace (or the start of input) precedes the token.
	spaceBefore bool
}

const (
	maxFilterLen  = 20000
	maxFilterToks = 4000
)

func isWordStop(c byte) bool {
	switch c {
	case ' ', '\t', '\r', '\n', '(', ')', ',', ':', '=', '!', '<', '>', '~', '"':
		return true
	}

	return false
}

func lexFilter(src string) ([]token, error) {
	if len(src) > maxFilterLen {
		return nil, errInvalid("Unparsable filter: filter exceeds %d characters.", maxFilterLen)
	}

	var toks []token

	i, n := 0, len(src)

	for i < n {
		space := i == 0

		for i < n && strings.IndexByte(" \t\r\n", src[i]) >= 0 {
			i++
			space = true
		}

		if i >= n {
			break
		}

		start := i

		tok, next, err := lexOne(src, i)
		if err != nil {
			return nil, err
		}

		if tok.kind == tokEOF {
			i = next

			continue
		}

		tok.pos, tok.spaceBefore = start, space
		toks = append(toks, tok)
		i = next

		if len(toks) > maxFilterToks {
			return nil, errInvalid("Unparsable filter: too many tokens.")
		}
	}

	return append(toks, token{kind: tokEOF, pos: n, spaceBefore: true}), nil
}

// lexOne reads one token at i; a comment yields tokEOF with the index after it.
func lexOne(src string, i int) (token, int, error) {
	c := src[i]

	switch {
	case c == '(':
		return token{kind: tokLParen, text: "("}, i + 1, nil
	case c == ')':
		return token{kind: tokRParen, text: ")"}, i + 1, nil
	case c == ',':
		return token{kind: tokComma, text: ","}, i + 1, nil
	case c == '"':
		s, next, err := readQuoted(src, i)

		return token{kind: tokString, text: s}, next, err
	case c == '-' && strings.HasPrefix(src[i:], "--"):
		j := strings.IndexByte(src[i:], '\n')
		if j < 0 {
			return token{kind: tokEOF}, len(src), nil
		}

		return token{kind: tokEOF}, i + j + 1, nil
	case strings.IndexByte(":=!<>~", c) >= 0:
		return lexOp(src, i)
	}

	if c == '-' && i+1 < len(src) && strings.IndexByte(" \t\r\n)", src[i+1]) < 0 && !isDigitOrDot(src[i+1]) {
		return token{kind: tokMinus, text: "-"}, i + 1, nil
	}

	j := readWord(src, i)

	return token{kind: tokWord, text: src[i:j]}, j, nil
}

func isDigitOrDot(c byte) bool { return c >= '0' && c <= '9' || c == '.' }

func lexOp(src string, i int) (token, int, error) {
	two := ""
	if i+1 < len(src) {
		two = src[i : i+2]
	}

	switch two {
	case "!=", "<=", ">=", "=~", "!~":
		return token{kind: tokOp, text: two}, i + len(two), nil
	}

	switch src[i] {
	case ':', '=', '<', '>':
		return token{kind: tokOp, text: src[i : i+1]}, i + 1, nil
	}

	return token{}, 0, errInvalid("Unparsable filter: unexpected %q at offset %d.", src[i:i+1], i)
}

// readWord reads an unquoted word; a quoted segment is allowed right after a dot (labels."a.b").
func readWord(src string, i int) int {
	j := i

	for j < len(src) {
		c := src[j]
		if c == '"' && j > i && src[j-1] == '.' {
			_, next, err := readQuoted(src, j)
			if err != nil {
				return j
			}

			j = next

			continue
		}

		if isWordStop(c) {
			break
		}

		j++
	}

	if j == i {
		return i + 1
	}

	return j
}

func readQuoted(src string, i int) (string, int, error) {
	var sb strings.Builder

	j := i + 1

	for j < len(src) {
		c := src[j]

		switch c {
		case '"':
			return sb.String(), j + 1, nil
		case '\\':
			if j+1 >= len(src) {
				return "", 0, errInvalid("Unparsable filter: unterminated string at offset %d.", i)
			}

			sb.WriteString(unescape(src[j+1]))

			j += 2
		default:
			sb.WriteByte(c)
			j++
		}
	}

	return "", 0, errInvalid("Unparsable filter: unterminated string at offset %d.", i)
}

func unescape(c byte) string {
	switch c {
	case 'n':
		return "\n"
	case 't':
		return "\t"
	case 'r':
		return "\r"
	case '"':
		return `"`
	case '\\':
		return `\`
	}

	return `\` + string(c)
}
