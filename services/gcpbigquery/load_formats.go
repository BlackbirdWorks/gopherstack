package gcpbigquery

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

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
		return nil, fmt.Errorf("%w: only the default quote character is supported", errLoad)
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
	dec := json.NewDecoder(bytes.NewReader(decodeText(data, lc.Encoding)))
	dec.UseNumber()

	var (
		rows [][]any
		bad  []error
	)

	for n := 1; ; n++ {
		var obj map[string]any

		err := dec.Decode(&obj)
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

func parseParquetInput(lc loadConfig, fields []bqsql.Field, data []byte) ([][]any, []error, error) {
	f, err := parquetio.Open(data)
	if err != nil {
		return nil, nil, fmt.Errorf("parquet: %w", err)
	}

	cols := f.Columns()

	var (
		rows [][]any
		bad  []error
	)

	n := 0

	err = f.ForEach(cols, func(vals []any) error {
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
		return nil, nil, fmt.Errorf("parquet: %w", err)
	}

	return rows, bad, nil
}
