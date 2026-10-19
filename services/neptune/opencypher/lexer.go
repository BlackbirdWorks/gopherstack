package opencypher

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

type tokKind int

const (
	tkEOF tokKind = iota
	tkIdent
	tkInt
	tkFloat
	tkString
	tkParam
	tkPunct
)

type token struct {
	text string
	pos  int
	kind tokKind
}

func (t token) is(word string) bool {
	return (t.kind == tkIdent && strings.EqualFold(t.text, word)) || (t.kind == tkPunct && t.text == word)
}

func lex(src string) ([]token, error) {
	var toks []token

	for i := 0; i < len(src); {
		tok, next, err := lexNext(src, i)
		if err != nil {
			return nil, err
		}

		if tok.kind != tkEOF {
			toks = append(toks, tok)
		}

		i = next
	}

	return append(toks, token{kind: tkEOF, pos: len(src)}), nil
}

// lexNext scans one token at i; whitespace and comments yield a tkEOF token that the caller drops.
func lexNext(src string, i int) (token, int, error) {
	r, size := utf8.DecodeRuneInString(src[i:])

	switch {
	case unicode.IsSpace(r):
		return token{}, i + size, nil
	case strings.HasPrefix(src[i:], "//"):
		return token{}, skipLine(src, i), nil
	case r == '\'' || r == '"':
		return lexString(src, i)
	case r == '`':
		return lexQuotedIdent(src, i)
	case r >= '0' && r <= '9':
		tok, next := lexNumber(src, i)

		return tok, next, nil
	case r == '$':
		return lexParam(src, i)
	case r == '_' || unicode.IsLetter(r):
		j := identEnd(src, i)

		return token{kind: tkIdent, text: src[i:j], pos: i}, j, nil
	default:
		tok, err := lexPunct(src, i)

		return tok, i + len(tok.text), err
	}
}

func lexQuotedIdent(src string, i int) (token, int, error) {
	end := strings.IndexByte(src[i+1:], '`')
	if end < 0 {
		return token{}, 0, syntaxErr(i, "unterminated escaped identifier")
	}

	return token{kind: tkIdent, text: src[i+1 : i+1+end], pos: i}, i + end + quotedIdentOverhead, nil
}

func lexParam(src string, i int) (token, int, error) {
	j := identEnd(src, i+1)
	if j == i+1 {
		return token{}, 0, syntaxErr(i, "expected parameter name after $")
	}

	return token{kind: tkParam, text: src[i+1 : j], pos: i}, j, nil
}

func skipLine(src string, i int) int {
	if nl := strings.IndexByte(src[i:], '\n'); nl >= 0 {
		return i + nl
	}

	return len(src)
}

func identEnd(src string, i int) int {
	for i < len(src) {
		r, size := utf8.DecodeRuneInString(src[i:])
		if r != '_' && !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			break
		}

		i += size
	}

	return i
}

func digitsEnd(src string, i int) int {
	for i < len(src) && src[i] >= '0' && src[i] <= '9' {
		i++
	}

	return i
}

func isDigitAt(src string, i int) bool { return i < len(src) && src[i] >= '0' && src[i] <= '9' }

func lexNumber(src string, i int) (token, int) {
	j, kind := digitsEnd(src, i), tkInt

	if j < len(src) && src[j] == '.' && isDigitAt(src, j+1) {
		kind, j = tkFloat, digitsEnd(src, j+1)
	}

	if j < len(src) && (src[j] == 'e' || src[j] == 'E') {
		k := j + 1
		if k < len(src) && (src[k] == '+' || src[k] == '-') {
			k++
		}

		if isDigitAt(src, k) {
			kind, j = tkFloat, digitsEnd(src, k)
		}
	}

	return token{kind: kind, text: src[i:j], pos: i}, j
}

const quotedIdentOverhead = 2

func lexString(src string, i int) (token, int, error) {
	quote := src[i]

	var sb strings.Builder

	for j := i + 1; j < len(src); j++ {
		c := src[j]

		switch {
		case c == quote:
			return token{kind: tkString, text: sb.String(), pos: i}, j + 1, nil
		case c == '\\' && j+1 < len(src):
			j++
			sb.WriteString(unescape(src[j]))
		default:
			sb.WriteByte(c)
		}
	}

	return token{}, 0, syntaxErr(i, "unterminated string literal")
}

func unescape(c byte) string {
	switch c {
	case 'n':
		return "\n"
	case 't':
		return "\t"
	case 'r':
		return "\r"
	case 'b':
		return "\b"
	case 'f':
		return "\f"
	default:
		return string(c)
	}
}

func lexPunct(src string, i int) (token, error) {
	for p := range strings.FieldsSeq("<> != <= >= += ..") {
		if strings.HasPrefix(src[i:], p) {
			return token{kind: tkPunct, text: p, pos: i}, nil
		}
	}

	if strings.IndexByte("()[]{},:.|+-*/%^=<>;", src[i]) >= 0 {
		return token{kind: tkPunct, text: src[i : i+1], pos: i}, nil
	}

	return token{}, syntaxErr(i, fmt.Sprintf("invalid input %q", src[i:i+1]))
}
