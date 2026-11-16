package athena

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"maps"
	"regexp"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/orcio"
	"github.com/blackbirdworks/gopherstack/pkgs/parquetio"
)

// columnSet is the lower-cased column names a query mentions; all=true loads every column.
type columnSet struct {
	names map[string]bool
	all   bool
}

func (s columnSet) has(name string) bool { return s.all || s.names[strings.ToLower(name)] }

// with returns a copy that also loads the named columns.
func (s columnSet) with(names []string) columnSet {
	if s.all || len(names) == 0 {
		return s
	}

	out := columnSet{names: make(map[string]bool, len(s.names)+len(names))}
	maps.Copy(out.names, s.names)

	for _, n := range names {
		out.names[strings.ToLower(n)] = true
	}

	return out
}

// referencedColumns over-approximates the columns a translated query reads
// from its identifiers; a select-star (but not count(*)) needs every column.
func referencedColumns(sqlText string) columnSet {
	toks, err := tokenize(sqlText)
	if err != nil {
		return columnSet{all: true}
	}

	set := columnSet{names: map[string]bool{}}
	prev := -1

	for i, t := range toks {
		if t.kind == tkSpace {
			continue
		}

		switch {
		case t.kind == tkWord || t.kind == tkQuoted:
			set.names[t.ident()] = true
		case t.isPunct("*") && prev >= 0 && isStarContext(toks[prev]):
			set.all = true
		}

		prev = i
	}

	return set
}

func isStarContext(prev token) bool {
	return prev.isWord("select") || prev.isWord("distinct") || prev.isPunct(",") || prev.isPunct(".")
}

// columnarFile is an opened Parquet or ORC file.
type columnarFile interface {
	Columns() []string
	ColumnIDs() map[int]string
	ScanBytes(cols []string) int64
	ForEach(cols []string, fn func(vals []any) error) error
}

func openColumnar(kind dataFormat, data []byte) (columnarFile, error) {
	if kind == fmtORC {
		return orcio.Open(data)
	}

	return parquetio.Open(data)
}

func isBadColumnarFile(err error) bool {
	return errors.Is(err, parquetio.ErrBadFile) || errors.Is(err, orcio.ErrBadFile)
}

var legacyORCName = regexp.MustCompile(`^_col\d+$`)

// fileNames maps table columns to file column names; ORC files written by older Hive carry only _colN names.
func fileNames(pf columnarFile, kind dataFormat, cols []Column) []string {
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}

	if kind != fmtORC {
		return names
	}

	fc := pf.Columns()

	for _, n := range fc {
		if !legacyORCName.MatchString(n) {
			return names
		}
	}

	for i := range names {
		if i < len(fc) {
			names[i] = fc[i]
		}
	}

	return names
}

func (l *tableLoader) loadColumnar(
	ctx context.Context, body io.Reader, kind dataFormat, bucket, key string, cols []Column, sink *rowSink,
) error {
	data, err := io.ReadAll(io.LimitReader(body, l.limits.MaxScanBytes+1))
	if err != nil {
		return fmt.Errorf("HIVE_CURSOR_ERROR: s3://%s/%s: %w", bucket, key, err)
	}

	if int64(len(data)) > l.limits.MaxScanBytes {
		return fmt.Errorf("%w: more than %d bytes", errScanLimit, l.limits.MaxScanBytes)
	}

	pf, err := openColumnar(kind, data)
	if err != nil {
		return fmt.Errorf("HIVE_CANNOT_OPEN_SPLIT: Error opening Hive split s3://%s/%s: %w", bucket, key, err)
	}

	names := make([]string, 0, len(cols))
	pos := make([]int, 0, len(cols))
	inFile := fileNames(pf, kind, cols)

	for i, c := range cols {
		if l.want.has(c.Name) {
			names = append(names, inFile[i])
			pos = append(pos, i)
		}
	}

	l.scanned += pf.ScanBytes(names)
	if l.scanned > l.limits.MaxScanBytes {
		return fmt.Errorf("%w: more than %d bytes", errScanLimit, l.limits.MaxScanBytes)
	}

	err = pf.ForEach(names, func(vals []any) error {
		rec := make([]string, len(cols))
		nulls := make([]bool, len(cols))

		for i := range nulls {
			nulls[i] = true
		}

		for j, v := range vals {
			if v != nil {
				rec[pos[j]], nulls[pos[j]] = parquetCell(v), false
			}
		}

		return l.insertJSON(ctx, sink, rec, nulls)
	})
	if err != nil && !isBadColumnarFile(err) {
		return err
	}

	if err != nil {
		return fmt.Errorf("HIVE_CURSOR_ERROR: s3://%s/%s: %w", bucket, key, err)
	}

	return nil
}

func parquetCell(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case int64:
		return strconv.FormatInt(t, 10)
	case float64:
		return strconv.FormatFloat(t, 'g', -1, 64)
	case bool:
		return strconv.FormatBool(t)
	default:
		b, err := json.Marshal(t, jsonx.V1())
		if err != nil {
			return fmt.Sprint(t)
		}

		return string(b)
	}
}

// encodeParquetRows writes one Parquet file holding the given result rows.
func encodeParquetRows(res *sqlResult, rows []int, cols []outCol, codec string) ([]byte, error) {
	schema := make([]parquetio.Column, len(cols))
	for i, c := range cols {
		schema[i] = parquetio.Column{Name: c.name, Type: c.typ, FieldID: c.id, NestedIDs: c.nestedIDs}
	}

	cells := make([][]any, len(rows))

	for ri, r := range rows {
		row := make([]any, len(cols))

		for i, c := range cols {
			if v, null := res.cell(r, c.src); !null {
				row[i] = v
			}
		}

		cells[ri] = row
	}

	var buf bytes.Buffer

	if err := parquetio.Write(&buf, schema, cells, codec); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
