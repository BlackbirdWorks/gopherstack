package gcpspanner

import (
	"strings"
	"unicode"
)

const (
	maxDDLTokens = 200_000
	maxDDLDepth  = 128
	tripleQuote  = 3
	commentMark  = 2
)

type ddlTok struct {
	text       string
	start, end int
	word       bool
}

func (t ddlTok) is(kw string) bool { return t.word && strings.EqualFold(t.text, kw) }

func skipQuoted(s string, i int, q byte) (int, bool) {
	triple := q != '`' && strings.HasPrefix(s[i:], strings.Repeat(string(q), tripleQuote))
	if triple {
		end := strings.Index(s[i+tripleQuote:], strings.Repeat(string(q), tripleQuote))
		if end < 0 {
			return 0, false
		}

		return i + tripleQuote + end + tripleQuote, true
	}

	for j := i + 1; j < len(s); j++ {
		switch s[j] {
		case '\\':
			if q != '`' {
				j++
			}
		case q:
			return j + 1, true
		case '\n':
			return 0, false
		}
	}

	return 0, false
}

func skipComment(s string, i int) (int, bool, bool) {
	switch {
	case s[i] == '#' || strings.HasPrefix(s[i:], "--"):
		if nl := strings.IndexByte(s[i:], '\n'); nl >= 0 {
			return i + nl + 1, true, true
		}

		return len(s), true, true
	case strings.HasPrefix(s[i:], "/*"):
		end := strings.Index(s[i+commentMark:], "*/")
		if end < 0 {
			return 0, true, false
		}

		return i + commentMark + end + commentMark, true, true
	}

	return i, false, true
}

func isWordByte(c byte) bool {
	return c == '_' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= 0x80
}

// nextDDLToken reads the token at s[i]; i must not be whitespace or a comment.
func nextDDLToken(s string, i int) (ddlTok, error) {
	c := s[i]

	switch {
	case c == '\'' || c == '"' || c == '`':
		end, ok := skipQuoted(s, i, c)
		if !ok {
			return ddlTok{}, errInvalid("Syntax error: unterminated quoted string or identifier.")
		}

		return ddlTok{text: s[i:end], start: i, end: end, word: c == '`' || c == '"'}, nil
	case isWordByte(c):
		j := i
		for j < len(s) && isWordByte(s[j]) {
			j++
		}

		return ddlTok{text: s[i:j], start: i, end: j, word: true}, nil
	default:
		return ddlTok{text: s[i : i+1], start: i, end: i + 1}, nil
	}
}

// tokenizeDDL splits a DDL statement into words, quoted identifiers, string literals and punctuation, skipping
// comments. Parentheses must balance within maxDDLDepth.
func tokenizeDDL(s string) ([]ddlTok, error) {
	var toks []ddlTok

	depth := 0

	for i := 0; i < len(s); {
		if next, isComment, ok := skipComment(s, i); isComment {
			if !ok {
				return nil, errInvalid("Syntax error: unterminated comment.")
			}

			i = next

			continue
		}

		if unicode.IsSpace(rune(s[i])) {
			i++

			continue
		}

		tok, err := nextDDLToken(s, i)
		if err != nil {
			return nil, err
		}

		depth += parenDelta(tok.text)

		if depth < 0 || depth > maxDDLDepth {
			return nil, errInvalid("Syntax error: unbalanced or too deeply nested parentheses.")
		}

		if toks = append(toks, tok); len(toks) > maxDDLTokens {
			return nil, errInvalid("DDL statement has more than %d tokens.", maxDDLTokens)
		}

		i = tok.end
	}

	if depth != 0 {
		return nil, errInvalid("Syntax error: unbalanced parentheses.")
	}

	return toks, nil
}

func parenDelta(t string) int {
	switch t {
	case "(", "[":
		return 1
	case ")", "]":
		return -1
	}

	return 0
}

// identName unquotes an identifier token.
func identName(t ddlTok) string {
	if len(t.text) >= 2 && (t.text[0] == '`' || t.text[0] == '"') {
		return t.text[1 : len(t.text)-1]
	}

	return t.text
}

// matchParen returns the index of the token closing the parenthesis opened at toks[open].
func matchParen(toks []ddlTok, open int) int {
	depth := 0

	for i := open; i < len(toks); i++ {
		switch toks[i].text {
		case "(":
			depth++
		case ")":
			depth--

			if depth == 0 {
				return i
			}
		}
	}

	return -1
}

// splitTop splits the tokens between the parentheses open and closeIdx at top-level commas.
func splitTop(toks []ddlTok, open, closeIdx int) [][]ddlTok {
	var items [][]ddlTok

	depth, from := 0, open+1

	for i := open + 1; i < closeIdx; i++ {
		switch toks[i].text {
		case "(", "[":
			depth++
		case ")", "]":
			depth--
		case ",":
			if depth == 0 {
				items = append(items, toks[from:i])
				from = i + 1
			}
		}
	}

	return append(items, toks[from:closeIdx])
}
