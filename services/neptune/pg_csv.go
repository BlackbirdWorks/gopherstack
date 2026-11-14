package neptune

import (
	"errors"
	"fmt"
	"strings"
)

var errCSVParse = errors.New("csv parse error")

type csvField struct {
	val    string
	quoted bool
}

// parseCSV reads RFC 4180 records, keeping whether each field was quoted. On a malformed record it returns the
// records read so far and an error naming the record number.
func parseCSV(data []byte) ([][]csvField, error) {
	src := strings.TrimPrefix(string(data), "\xef\xbb\xbf")
	src = strings.ReplaceAll(src, "\r\n", "\n")

	var (
		recs [][]csvField
		cur  []csvField
		i    int
	)

	for i < len(src) {
		f, next, err := readCSVField(src, i)
		if err != nil {
			return recs, fmt.Errorf("%w: record %d: %s", errCSVParse, len(recs)+1, err.Error())
		}

		cur = append(cur, f)
		i = next

		switch {
		case i >= len(src):
			recs = append(recs, cur)
			cur = nil
		case src[i] == ',':
			i++
			if i >= len(src) {
				cur = append(cur, csvField{})
				recs = append(recs, cur)
				cur = nil
			}
		default:
			i++
			recs = append(recs, cur)
			cur = nil
		}
	}

	return recs, nil
}

var errCSVQuote = errors.New("quote handling")

// readCSVField reads one field at i and returns the index of the delimiter that ended it.
func readCSVField(s string, i int) (csvField, int, error) {
	i = skipBlanks(s, i)

	if i < len(s) && s[i] == '"' {
		return readQuotedField(s, i+1)
	}

	start := i
	for i < len(s) && s[i] != ',' && s[i] != '\n' {
		if s[i] == '"' {
			return csvField{}, i, fmt.Errorf("%w: field with a double quote must be quoted", errCSVQuote)
		}

		i++
	}

	return csvField{val: strings.TrimSpace(s[start:i])}, i, nil
}

func skipBlanks(s string, i int) int {
	for i < len(s) && (s[i] == ' ' || s[i] == '\t') {
		i++
	}

	return i
}

// readQuotedField reads a quoted field whose opening quote precedes i.
func readQuotedField(s string, i int) (csvField, int, error) {
	var b strings.Builder

	for ; i < len(s); i++ {
		if s[i] != '"' {
			b.WriteByte(s[i])

			continue
		}

		if i+1 < len(s) && s[i+1] == '"' {
			b.WriteByte('"')
			i++

			continue
		}

		i = skipBlanks(s, i+1)
		if i < len(s) && s[i] != ',' && s[i] != '\n' {
			return csvField{}, i, fmt.Errorf("%w: unexpected character after closing quote", errCSVQuote)
		}

		return csvField{val: b.String(), quoted: true}, i, nil
	}

	return csvField{}, i, fmt.Errorf("%w: unterminated quoted field", errCSVQuote)
}

// splitUnescaped splits s on sep, honouring a backslash escape of sep, and unescapes it.
func splitUnescaped(s string, sep byte) []string {
	var (
		out []string
		b   strings.Builder
	)

	for i := 0; i < len(s); i++ {
		switch {
		case s[i] == '\\' && i+1 < len(s) && s[i+1] == sep:
			b.WriteByte(sep)
			i++
		case s[i] == sep:
			out = append(out, b.String())
			b.Reset()
		default:
			b.WriteByte(s[i])
		}
	}

	return append(out, b.String())
}

// lastUnescapedColon returns the index of the last ':' not preceded by a backslash, or -1.
func lastUnescapedColon(s string) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == ':' && (i == 0 || s[i-1] != '\\') {
			return i
		}
	}

	return -1
}
