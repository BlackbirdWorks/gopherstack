package redshiftdata

import (
	"fmt"
	"strconv"
	"strings"
)

// statementError is a SQL error whose text is already in engine form.
type statementError string

func (e statementError) Error() string { return string(e) }

func stmtErrorf(format string, args ...any) error {
	return statementError("ERROR: " + fmt.Sprintf(format, args...))
}

const quoteContext = 20

type tokKind int

const (
	tokWord tokKind = iota
	tokString
	tokQuoted
	tokPunct
)

type token struct {
	text string
	raw  string
	kind tokKind
}

func (t token) is(word string) bool { return t.kind == tokWord && strings.EqualFold(t.text, word) }

func isWordByte(c byte) bool {
	return c == '_' || c == '$' || c == '#' || c == '@' || c >= '0' && c <= '9' ||
		c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= 0x80
}

// lexSQL splits a COPY or UNLOAD statement into tokens, dropping comments and a trailing semicolon.
func lexSQL(s string) ([]token, error) {
	var (
		toks []token
		err  error
	)

	for i := 0; ; {
		if i, err = skipBlank(s, i); err != nil {
			return nil, err
		}

		if i >= len(s) {
			break
		}

		var (
			tok  token
			next int
		)

		if tok, next, err = lexAt(s, i); err != nil {
			return nil, err
		}

		toks, i = append(toks, tok), next
	}

	if n := len(toks); n > 0 && toks[n-1].kind == tokPunct && toks[n-1].text == ";" {
		toks = toks[:n-1]
	}

	return toks, nil
}

// skipBlank skips whitespace and comments starting at s[i].
func skipBlank(s string, i int) (int, error) {
	for i < len(s) {
		switch {
		case s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r':
			i++
		case strings.HasPrefix(s[i:], "--"):
			i = skipLine(s, i)
		case strings.HasPrefix(s[i:], "/*"):
			end := strings.Index(s[i+2:], "*/")
			if end < 0 {
				return 0, stmtErrorf("unterminated /* comment")
			}

			i += end + len("/**/")
		default:
			return i, nil
		}
	}

	return i, nil
}

// lexAt scans the token at s[i] and returns the next offset.
func lexAt(s string, i int) (token, int, error) {
	c := s[i]

	switch {
	case c == '\'' || c == '"':
		return lexQuoted(s, i)
	case strings.HasPrefix(s[i:], "$$"):
		end := strings.Index(s[i+2:], "$$")
		if end < 0 {
			return token{}, 0, stmtErrorf("unterminated dollar-quoted string")
		}

		next := i + end + len("$$$$")

		return token{kind: tokString, text: s[i+2 : i+2+end], raw: s[i:next]}, next, nil
	case isWordByte(c):
		j := i
		for j < len(s) && isWordByte(s[j]) {
			j++
		}

		return token{kind: tokWord, text: s[i:j], raw: s[i:j]}, j, nil
	default:
		return token{kind: tokPunct, text: string(c), raw: string(c)}, i + 1, nil
	}
}

func skipLine(s string, i int) int {
	if nl := strings.IndexByte(s[i:], '\n'); nl >= 0 {
		return i + nl
	}

	return len(s)
}

func lexQuoted(s string, i int) (token, int, error) {
	q := s[i]

	var b strings.Builder

	for j := i + 1; j < len(s); j++ {
		if s[j] != q {
			b.WriteByte(s[j])

			continue
		}

		if j+1 < len(s) && s[j+1] == q {
			b.WriteByte(q)
			j++

			continue
		}

		kind := tokString
		if q == '"' {
			kind = tokQuoted
		}

		return token{kind: kind, text: b.String(), raw: s[i : j+1]}, j + 1, nil
	}

	near := strconv.Quote(s[i:min(len(s), i+quoteContext)])

	return token{}, 0, stmtErrorf("unterminated quoted string at or near %s", near)
}

type cursor struct {
	toks []token
	pos  int
}

func (c *cursor) done() bool { return c.pos >= len(c.toks) }

func (c *cursor) peek() (token, bool) {
	if c.done() {
		return token{}, false
	}

	return c.toks[c.pos], true
}

func (c *cursor) next() (token, bool) {
	t, ok := c.peek()
	if ok {
		c.pos++
	}

	return t, ok
}

func (c *cursor) syntaxErr() error {
	if t, ok := c.peek(); ok {
		return stmtErrorf("syntax error at or near %q", t.raw)
	}

	return stmtErrorf("syntax error at end of input")
}

func (c *cursor) expectWord(w string) error {
	if t, ok := c.peek(); !ok || !t.is(w) {
		return c.syntaxErr()
	}

	c.pos++

	return nil
}

func (c *cursor) expectPunct(p string) error {
	if t, ok := c.peek(); !ok || t.kind != tokPunct || t.text != p {
		return c.syntaxErr()
	}

	c.pos++

	return nil
}

func (c *cursor) acceptPunct(p string) bool {
	if t, ok := c.peek(); ok && t.kind == tokPunct && t.text == p {
		c.pos++

		return true
	}

	return false
}

func (c *cursor) acceptWord(w string) bool {
	if t, ok := c.peek(); ok && t.is(w) {
		c.pos++

		return true
	}

	return false
}

func (c *cursor) str() (string, error) {
	t, ok := c.peek()
	if !ok || t.kind != tokString {
		return "", c.syntaxErr()
	}

	c.pos++

	return t.text, nil
}

// optString consumes the string that may follow an option keyword.
func (c *cursor) optString() (string, bool) {
	if t, ok := c.peek(); ok && t.kind == tokString {
		c.pos++

		return t.text, true
	}

	return "", false
}

// asString reads `[AS] 'value'`.
func (c *cursor) asString() (string, error) {
	c.acceptWord("as")

	return c.str()
}

// asInt reads `[AS] n`.
func (c *cursor) asInt() (int, error) {
	c.acceptWord("as")

	t, ok := c.peek()
	if !ok {
		return 0, c.syntaxErr()
	}

	n, err := strconv.Atoi(t.text)
	if err != nil || n < 0 {
		return 0, c.syntaxErr()
	}

	c.pos++

	return n, nil
}

// ident reads a possibly schema-qualified identifier and returns its raw text and normalized parts.
func (c *cursor) ident() (string, []string, error) {
	var raw strings.Builder

	var parts []string

	for {
		t, ok := c.peek()
		if !ok || t.kind != tokWord && t.kind != tokQuoted {
			return "", nil, c.syntaxErr()
		}

		c.pos++
		raw.WriteString(t.raw)

		if t.kind == tokQuoted {
			parts = append(parts, t.text)
		} else {
			parts = append(parts, strings.ToLower(t.text))
		}

		if !c.acceptPunct(".") {
			return raw.String(), parts, nil
		}

		raw.WriteByte('.')
	}
}

// skipCredentials consumes the value of an IAM_ROLE / CREDENTIALS style option.
func (c *cursor) skipCredentials(word string) error {
	if word == wordIAMRole && c.acceptWord("default") {
		return nil
	}

	_, err := c.asString()

	return err
}

// s3URI splits s3://bucket/key.
func s3URI(u string) (string, string, bool) {
	rest, found := cutPrefixFold(u, "s3://")
	if !found {
		return "", "", false
	}

	bucket, key, _ := strings.Cut(rest, "/")

	return bucket, key, bucket != ""
}

func cutPrefixFold(s, prefix string) (string, bool) {
	if len(s) >= len(prefix) && strings.EqualFold(s[:len(prefix)], prefix) {
		return s[len(prefix):], true
	}

	return s, false
}

func quoteLiteral(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
