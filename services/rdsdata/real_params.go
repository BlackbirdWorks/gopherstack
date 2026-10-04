package rdsdata

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	castWidth        = 2
	commentOpenWidth = 2
)

// translated is a statement rewritten for a driver's positional placeholders.
type translated struct {
	SQL       string
	Args      []any
	Returning bool
}

// pgHintCast gives typeHint parameters their SQL type on PostgreSQL
// (SqlParameter.TypeHint, API_SqlParameter.html); JSON stays untyped so json and jsonb both accept it.
func pgHintCast(hint string) string {
	switch hint {
	case typeHintDate:
		return "::date"
	case typeHintTime:
		return "::time"
	case typeHintTimestamp:
		return "::timestamp"
	case typeHintDecimal:
		return "::numeric"
	case typeHintUUID:
		return "::uuid"
	default:
		return ""
	}
}

type placeholderRewriter struct {
	params  map[string]SQLParameter
	slots   map[string]int
	args    []any
	kind    string
	src     string
	out     strings.Builder
	returns bool
}

// translatePlaceholders rewrites :name placeholders to $n (postgres) or ? (mysql) and collects
// driver args; quoted text, comments, dollar quotes and ::casts are left alone.
func translatePlaceholders(kind, sqlText string, params []SQLParameter) (translated, error) {
	rw := &placeholderRewriter{
		kind:   kind,
		src:    sqlText,
		params: make(map[string]SQLParameter, len(params)),
		slots:  make(map[string]int),
	}

	for _, p := range params {
		rw.params[p.Name] = p
	}

	if err := rw.run(); err != nil {
		return translated{}, err
	}

	return translated{SQL: rw.out.String(), Args: rw.args, Returning: rw.returns}, nil
}

func (rw *placeholderRewriter) run() error {
	src := rw.src

	for i := 0; i < len(src); {
		next, err := rw.step(i)
		if err != nil {
			return err
		}

		i = next
	}

	return nil
}

func (rw *placeholderRewriter) copyTo(from, to int) int {
	rw.out.WriteString(rw.src[from:to])

	return to
}

func (rw *placeholderRewriter) step(i int) (int, error) {
	if end, ok := rw.skipLiteral(i); ok {
		return rw.copyTo(i, end), nil
	}

	c := rw.src[i]

	switch {
	case c == ':':
		return rw.colon(i)
	case isIdentStart(c) && (i == 0 || !isIdentChar(rw.src[i-1])):
		end := i
		for end < len(rw.src) && isIdentChar(rw.src[end]) {
			end++
		}

		if strings.EqualFold(rw.src[i:end], "RETURNING") {
			rw.returns = true
		}

		return rw.copyTo(i, end), nil
	}

	return rw.copyTo(i, i+1), nil
}

// skipLiteral returns the end of a quoted string, identifier or comment starting at i.
func (rw *placeholderRewriter) skipLiteral(i int) (int, bool) {
	src := rw.src
	c := src[i]
	pg := rw.kind == kindPostgres
	hasNext := i+1 < len(src)

	switch {
	case c == '\'':
		return skipQuoted(src, i, c, rw.backslashEscapes(i)), true
	case c == '"':
		return skipQuoted(src, i, c, !pg), true
	case c == '`' && !pg:
		return skipQuoted(src, i, c, false), true
	case c == '#' && !pg, c == '-' && hasNext && src[i+1] == '-':
		return skipLine(src, i), true
	case c == '/' && hasNext && src[i+1] == '*':
		return skipBlockComment(src, i, pg), true
	case c == '$' && pg:
		if end := skipDollarQuote(src, i); end > i {
			return end, true
		}
	}

	return i, false
}

func (rw *placeholderRewriter) backslashEscapes(i int) bool {
	if rw.kind != kindPostgres {
		return true
	}

	src := rw.src

	return i > 0 && (src[i-1] == 'E' || src[i-1] == 'e') && (i < 2 || !isIdentChar(src[i-2]))
}

func (rw *placeholderRewriter) colon(i int) (int, error) {
	src := rw.src
	if i+1 < len(src) && src[i+1] == ':' {
		return rw.copyTo(i, i+castWidth), nil
	}

	if i+1 >= len(src) || !isIdentStart(src[i+1]) || (i > 0 && precedesNoPlaceholder(src[i-1])) {
		return rw.copyTo(i, i+1), nil
	}

	end := i + 1
	for end < len(src) && isIdentChar(src[end]) {
		end++
	}

	name := src[i+1 : end]

	p, ok := rw.params[name]
	if !ok {
		return 0, fmt.Errorf("%w: no value supplied for parameter %q", ErrValidation, name)
	}

	rw.emit(name, p)

	return end, nil
}

func (rw *placeholderRewriter) emit(name string, p SQLParameter) {
	if rw.kind != kindPostgres {
		rw.out.WriteByte('?')
		rw.args = append(rw.args, fieldToValue(p.Value))

		return
	}

	slot, seen := rw.slots[name]
	if !seen {
		rw.args = append(rw.args, fieldToValue(p.Value))
		slot = len(rw.args)
		rw.slots[name] = slot
	}

	rw.out.WriteString("$" + strconv.Itoa(slot) + pgHintCast(p.TypeHint))
}

func precedesNoPlaceholder(b byte) bool { return isIdentChar(b) || b == ']' || b == ')' }

func isIdentStart(b byte) bool {
	return b == '_' || (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
}

func isIdentChar(b byte) bool { return isIdentStart(b) || (b >= '0' && b <= '9') }

// skipQuoted returns the index after the quoted run starting at i; a doubled quote is an escape.
func skipQuoted(src string, i int, quote byte, backslash bool) int {
	for j := i + 1; j < len(src); j++ {
		switch {
		case backslash && src[j] == '\\':
			j++
		case src[j] == quote:
			if j+1 < len(src) && src[j+1] == quote {
				j++

				continue
			}

			return j + 1
		}
	}

	return len(src)
}

func skipLine(src string, i int) int {
	if j := strings.IndexByte(src[i:], '\n'); j >= 0 {
		return i + j + 1
	}

	return len(src)
}

// skipBlockComment returns the index after the comment at i; PostgreSQL comments nest.
func skipBlockComment(src string, i int, nested bool) int {
	depth := 1

	for j := i + commentOpenWidth; j < len(src)-1; j++ {
		switch {
		case nested && src[j] == '/' && src[j+1] == '*':
			depth++
			j++
		case src[j] == '*' && src[j+1] == '/':
			depth--
			j++

			if depth == 0 {
				return j + 1
			}
		}
	}

	return len(src)
}

// skipDollarQuote returns the index after a $tag$...$tag$ string at i, or i when none starts there.
func skipDollarQuote(src string, i int) int {
	j := i + 1
	for j < len(src) && isIdentChar(src[j]) {
		j++
	}

	if j >= len(src) || src[j] != '$' || (j > i+1 && src[i+1] >= '0' && src[i+1] <= '9') {
		return i
	}

	tag := src[i : j+1]
	if end := strings.Index(src[j+1:], tag); end >= 0 {
		return j + 1 + end + len(tag)
	}

	return len(src)
}
