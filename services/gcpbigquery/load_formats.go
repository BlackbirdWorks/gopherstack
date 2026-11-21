package gcpbigquery

import (
	"bytes"
	"encoding/csv"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/orcio"
	"github.com/blackbirdworks/gopherstack/pkgs/parquetio"
	"github.com/blackbirdworks/gopherstack/services/gcpbigquery/bqsql"
)

func decodeText(data []byte, encoding string) []byte {
	if encoding != "ISO-8859-1" {
		return bytes.TrimPrefix(data, []byte("\xef\xbb\xbf"))
	}

	runes := make([]rune, len(data))
	for i, c := range data {
		runes[i] = rune(c)
	}

	return []byte(string(runes))
}

func delimiterRune(d string) (rune, error) {
	switch strings.ToLower(d) {
	case `\t`, "tab", "\t":
		return '\t', nil
	}

	r := []rune(d)
	if len(r) != 1 {
		return 0, fmt.Errorf("%w: fieldDelimiter must be a single character", errLoad)
	}

	return r[0], nil
}

func readCSVRecords(lc loadConfig, data []byte) ([][]string, error) {
	delim, err := delimiterRune(lc.Delimiter)
	if err != nil {
		return nil, err
	}

	text := decodeText(data, lc.Encoding)

	if lc.Quote != nil && *lc.Quote == "" {
		var recs [][]string

		for line := range strings.SplitSeq(strings.TrimRight(string(text), "\r\n"), "\n") {
			recs = append(recs, strings.Split(strings.TrimSuffix(line, "\r"), string(delim)))
		}

		return recs, nil
	}

	if lc.Quote != nil && *lc.Quote != `"` {
		q := []rune(*lc.Quote)
		if len(q) != 1 || q[0] == delim || q[0] == '\n' || q[0] == '\r' {
			return nil, fmt.Errorf(
				"%w: quote must be a single character other than the delimiter or a newline",
				errLoad,
			)
		}

		return splitQuotedCSV(string(text), delim, q[0])
	}

	r := csv.NewReader(bytes.NewReader(text))
	r.Comma, r.FieldsPerRecord, r.LazyQuotes = delim, -1, true

	recs, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("CSV parse: %w", err)
	}

	return recs, nil
}

func parseCSVInput(lc loadConfig, fields []bqsql.Field, data []byte) ([][]any, []error, error) {
	recs, err := readCSVRecords(lc, data)
	if err != nil {
		return nil, nil, err
	}

	for _, f := range fields {
		if f.Type == litRecord || f.Mode == bqsql.ModeRepeated {
			return nil, nil, fmt.Errorf("%w: CSV files can't contain RECORD or REPEATED columns", errLoad)
		}
	}

	if lc.SkipRows > len(recs) {
		recs = nil
	} else {
		recs = recs[lc.SkipRows:]
	}

	var (
		rows [][]any
		bad  []error
	)

	for i, rec := range recs {
		row, rerr := csvRow(lc, fields, rec)
		if rerr != nil {
			bad = append(bad, fmt.Errorf("row %d: %w", i+1+lc.SkipRows, rerr))

			continue
		}

		rows = append(rows, row)
	}

	return rows, bad, nil
}

func csvRow(lc loadConfig, fields []bqsql.Field, rec []string) ([]any, error) {
	if len(rec) > len(fields) && !lc.IgnoreExtra {
		return nil, fmt.Errorf("%w: too many values: expected %d columns, got %d", errBadRow, len(fields), len(rec))
	}

	if len(rec) < len(fields) && !lc.Jagged {
		return nil, fmt.Errorf("%w: too few values: expected %d columns, got %d", errBadRow, len(fields), len(rec))
	}

	obj := make(map[string]any, len(fields))

	for i, f := range fields {
		if i >= len(rec) || rec[i] == lc.NullMarker {
			continue
		}

		obj[f.Name] = rec[i]
	}

	return bqsql.ParseRecord(fields, obj, bqsql.ParseOptions{})
}

func parseJSONInput(lc loadConfig, fields []bqsql.Field, data []byte) ([][]any, []error, error) {
	dec := jsontext.NewDecoder(bytes.NewReader(decodeText(data, lc.Encoding)), jsonx.V1())

	var (
		rows [][]any
		bad  []error
	)

	for n := 1; ; n++ {
		var obj map[string]any

		err := json.UnmarshalDecode(dec, &obj, jsonx.V1(), jsonx.UseNumber())
		if errors.Is(err, io.EOF) {
			return rows, bad, nil
		}

		if err != nil {
			return nil, nil, fmt.Errorf("JSON parsing error in row starting at position %d: %w", dec.InputOffset(), err)
		}

		row, perr := bqsql.ParseRecord(fields, obj, bqsql.ParseOptions{IgnoreUnknown: lc.IgnoreExtra})
		if perr != nil {
			bad = append(bad, fmt.Errorf("row %d: %w", n, perr))

			continue
		}

		rows = append(rows, row)
	}
}

// columnarFile is what the Parquet and ORC readers share.
type columnarFile interface {
	Columns() []string
	ForEach(cols []string, fn func(vals []any) error) error
}

func parseParquetInput(lc loadConfig, fields []bqsql.Field, data []byte) ([][]any, []error, error) {
	f, err := parquetio.Open(data)
	if err != nil {
		return nil, nil, fmt.Errorf("parquet: %w", err)
	}

	return parseColumnar(lc, fields, f, "parquet")
}

func parseORCInput(lc loadConfig, fields []bqsql.Field, data []byte) ([][]any, []error, error) {
	f, err := orcio.Open(data)
	if err != nil {
		return nil, nil, fmt.Errorf("orc: %w", err)
	}

	return parseColumnar(lc, fields, f, "orc")
}

func parseColumnar(lc loadConfig, fields []bqsql.Field, f columnarFile, label string) ([][]any, []error, error) {
	cols := f.Columns()

	var (
		rows [][]any
		bad  []error
	)

	n := 0

	err := f.ForEach(cols, func(vals []any) error {
		n++

		obj := make(map[string]any, len(cols))
		for i, c := range cols {
			obj[c] = vals[i]
		}

		row, perr := bqsql.ParseRecord(fields, obj, bqsql.ParseOptions{IgnoreUnknown: lc.IgnoreExtra})
		if perr != nil {
			bad = append(bad, fmt.Errorf("row %d: %w", n, perr))

			return nil
		}

		rows = append(rows, row)

		return nil
	})
	if err != nil {
		return nil, nil, fmt.Errorf("%s: %w", label, err)
	}

	return rows, bad, nil
}

func selfDescribing(format string) bool {
	return format == formatParquet || format == formatAvro || format == formatORC
}

// quotedCSV reads CSV records whose quote character is q: a field is quoted only when it starts with q, a doubled q
// inside it is one literal q, and quoted fields may span lines.
type quotedCSV struct {
	recs   [][]string
	rec    []string
	field  strings.Builder
	delim  rune
	quote  rune
	quoted bool
	wasQ   bool
	start  bool
}

func splitQuotedCSV(text string, delim, q rune) ([][]string, error) {
	p := &quotedCSV{delim: delim, quote: q, start: true}

	for i := 0; i < len(text); {
		n, err := p.step(text, i)
		if err != nil {
			return nil, err
		}

		i += n
	}

	if p.quoted {
		return nil, fmt.Errorf("%w: unclosed quoted field", errLoad)
	}

	if len(p.rec) > 0 || p.field.Len() > 0 || p.wasQ {
		p.flush()

		p.recs = append(p.recs, p.rec)
	}

	return p.recs, nil
}

func (p *quotedCSV) flush() {
	p.rec = append(p.rec, p.field.String())
	p.field.Reset()
	p.wasQ, p.start = false, true
}

// step consumes the rune at text[i] (plus a following doubled quote or LF of a CRLF) and returns the bytes used.
func (p *quotedCSV) step(text string, i int) (int, error) {
	c, w := utf8.DecodeRuneInString(text[i:])

	var (
		next rune
		nw   int
	)

	if i+w < len(text) {
		next, nw = utf8.DecodeRuneInString(text[i+w:])
	}

	if p.quoted {
		if p.inQuotes(c, next) {
			return w + nw, nil
		}

		return w, nil
	}

	switch {
	case p.start && c == p.quote:
		p.quoted, p.wasQ, p.start = true, true, false
	case c == p.delim:
		p.flush()
	case c == '\n' || c == '\r' && next == '\n':
		p.flush()

		p.recs, p.rec = append(p.recs, p.rec), nil

		if c == '\r' {
			return w + 1, nil
		}
	case p.wasQ:
		return 0, fmt.Errorf("%w: data between close quote and field separator", errLoad)
	default:
		p.field.WriteRune(c)

		p.start = false
	}

	return w, nil
}

// inQuotes consumes c inside a quoted field and reports whether it also swallowed a doubled quote's second half.
func (p *quotedCSV) inQuotes(c, next rune) bool {
	switch {
	case c == p.quote && next == p.quote:
		p.field.WriteRune(p.quote)

		return true
	case c == p.quote:
		p.quoted = false
	default:
		p.field.WriteRune(c)
	}

	return false
}
