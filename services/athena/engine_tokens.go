package athena

import (
	"fmt"
	"strings"
)

type tokKind int

const (
	tkSpace tokKind = iota
	tkWord
	tkQuoted
	tkString
	tkNumber
	tkPunct
)

type token struct {
	text string
	kind tokKind
}

func (t token) isWord(w string) bool { return t.kind == tkWord && strings.EqualFold(t.text, w) }

func (t token) isPunct(p string) bool { return t.kind == tkPunct && t.text == p }

// ident returns the unquoted, lower-cased identifier for a word or quoted token.
func (t token) ident() string {
	if t.kind == tkQuoted {
		return strings.ToLower(t.text[1 : len(t.text)-1])
	}

	return strings.ToLower(t.text)
}

func isWordStart(c byte) bool {
	return c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

func isWordPart(c byte) bool { return isWordStart(c) || c >= '0' && c <= '9' || c == '$' }

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

// scanQuoted returns the end index (exclusive) of the quoted run starting at
// s[i], where doubled quote characters escape the quote.
func scanQuoted(s string, i int) (int, bool) {
	q := s[i]

	for j := i + 1; j < len(s); j++ {
		if s[j] != q {
			continue
		}

		if j+1 < len(s) && s[j+1] == q {
			j++

			continue
		}

		return j + 1, true
	}

	return 0, false
}

func scanNumber(s string, i int) int {
	j := i
	for j < len(s) && (isDigit(s[j]) || s[j] == '.') {
		j++
	}

	if j < len(s) && (s[j] == 'e' || s[j] == 'E') {
		k := j + 1
		if k < len(s) && (s[k] == '+' || s[k] == '-') {
			k++
		}

		if k < len(s) && isDigit(s[k]) {
			for k < len(s) && isDigit(s[k]) {
				k++
			}

			j = k
		}
	}

	return j
}

func scanComment(s string, i int) (int, bool) {
	switch {
	case strings.HasPrefix(s[i:], "--"):
		if nl := strings.IndexByte(s[i:], '\n'); nl >= 0 {
			return i + nl, true
		}

		return len(s), true
	case strings.HasPrefix(s[i:], "/*"):
		if end := strings.Index(s[i+2:], "*/"); end >= 0 {
			return i + commentDelimLen + end + commentDelimLen, true
		}

		return len(s), true
	default:
		return 0, false
	}
}

// tokenize splits SQL into tokens; comments become whitespace.
func tokenize(s string) ([]token, error) {
	var toks []token

	for i := 0; i < len(s); {
		tok, n, err := nextToken(s, i)
		if err != nil {
			return nil, err
		}

		toks = append(toks, tok)
		i += n
	}

	return toks, nil
}

func isSpaceByte(c byte) bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r' }

// nextToken returns the token starting at s[i] and its byte length.
func nextToken(s string, i int) (token, int, error) {
	c := s[i]

	if end, ok := scanComment(s, i); ok {
		return token{kind: tkSpace, text: " "}, end - i, nil
	}

	switch {
	case isSpaceByte(c):
		j := i
		for j < len(s) && isSpaceByte(s[j]) {
			j++
		}

		return token{kind: tkSpace, text: s[i:j]}, j - i, nil
	case c == '\'' || c == '"' || c == '`':
		end, ok := scanQuoted(s, i)
		if !ok {
			return token{}, 0, &syntaxError{fmt.Sprintf("line 1:%d: unterminated quoted literal", i+1)}
		}

		kind := tkQuoted
		if c == '\'' {
			kind = tkString
		}

		return token{kind: kind, text: s[i:end]}, end - i, nil
	case isWordStart(c):
		j := i
		for j < len(s) && isWordPart(s[j]) {
			j++
		}

		return token{kind: tkWord, text: s[i:j]}, j - i, nil
	case isDigit(c):
		j := scanNumber(s, i)

		return token{kind: tkNumber, text: s[i:j]}, j - i, nil
	default:
		return token{kind: tkPunct, text: string(c)}, 1, nil
	}
}

func joinTokens(toks []token) string {
	var sb strings.Builder
	for _, t := range toks {
		sb.WriteString(t.text)
	}

	return sb.String()
}

// stringLiteral returns the unescaped value of a single-quoted token.
func stringLiteral(t token) string {
	return strings.ReplaceAll(t.text[1:len(t.text)-1], "''", "'")
}

// significant returns the tokens that are not whitespace.
func significant(toks []token) []token {
	out := make([]token, 0, len(toks))

	for _, t := range toks {
		if t.kind != tkSpace {
			out = append(out, t)
		}
	}

	return out
}

// matchParen returns the index of the ')' closing the '(' at toks[open].
func matchParen(toks []token, open int) int {
	depth := 0

	for i := open; i < len(toks); i++ {
		switch {
		case toks[i].isPunct("("):
			depth++
		case toks[i].isPunct(")"):
			depth--
			if depth == 0 {
				return i
			}
		}
	}

	return -1
}

// splitTokens splits toks on the punctuation sep at parenthesis depth zero
// (angle brackets are not tracked, so callers split column lists on commas that
// are not inside type parameters).
func splitTokens(toks []token, sep string) [][]token {
	var (
		parts [][]token
		cur   []token
		depth int
		angle int
	)

	for _, t := range toks {
		switch {
		case t.isPunct("("):
			depth++
		case t.isPunct(")"):
			depth--
		case t.isPunct("<"):
			angle++
		case t.isPunct(">"):
			if angle > 0 {
				angle--
			}
		}

		if depth == 0 && angle == 0 && t.isPunct(sep) {
			parts = append(parts, cur)
			cur = nil

			continue
		}

		cur = append(cur, t)
	}

	return append(parts, cur)
}
