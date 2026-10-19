package billingexport

import (
	"archive/zip"
	"bytes"
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"io"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/parquetio"
)

// Column types understood by Encode.
const (
	TypeString = "string"
	TypeDouble = "double"
)

// Column is one output column.
type Column struct {
	Name string
	Type string
}

// Table is a header plus rows; a cell is a string, float64 or nil.
type Table struct {
	Columns []Column
	Rows    [][]any
}

// Output file formats and compressions.
const (
	FormatCSV     = "csv"
	FormatParquet = "parquet"

	CompressNone = "none"
	CompressGzip = "gzip"
	CompressZip  = "zip"
)

func bytesReader(b []byte) io.Reader { return bytes.NewReader(b) }

// Encode renders t as format with compression and returns the body and the file extension.
// Parquet is always internally compressed (snappy) and ignores compression.
func Encode(t *Table, format, compression, baseName string) ([]byte, string, error) {
	if format == FormatParquet {
		var buf bytes.Buffer

		cols := make([]parquetio.Column, len(t.Columns))
		for i, c := range t.Columns {
			cols[i] = parquetio.Column{Name: c.Name, Type: c.Type}
		}

		if err := parquetio.Write(&buf, cols, t.Rows, "snappy"); err != nil {
			return nil, "", fmt.Errorf("encode parquet: %w", err)
		}

		return buf.Bytes(), ".snappy.parquet", nil
	}

	raw, err := encodeCSV(t)
	if err != nil {
		return nil, "", err
	}

	switch compression {
	case CompressGzip:
		var buf bytes.Buffer

		zw := gzip.NewWriter(&buf)
		if _, err = zw.Write(raw); err != nil {
			return nil, "", fmt.Errorf("gzip report: %w", err)
		}

		if err = zw.Close(); err != nil {
			return nil, "", fmt.Errorf("gzip report: %w", err)
		}

		return buf.Bytes(), ".csv.gz", nil
	case CompressZip:
		var buf bytes.Buffer

		zw := zip.NewWriter(&buf)

		w, werr := zw.Create(baseName + ".csv")
		if werr != nil {
			return nil, "", fmt.Errorf("zip report: %w", werr)
		}

		if _, err = w.Write(raw); err != nil {
			return nil, "", fmt.Errorf("zip report: %w", err)
		}

		if err = zw.Close(); err != nil {
			return nil, "", fmt.Errorf("zip report: %w", err)
		}

		return buf.Bytes(), ".csv.zip", nil
	default:
		return raw, ".csv", nil
	}
}

func encodeCSV(t *Table) ([]byte, error) {
	var buf bytes.Buffer

	w := csv.NewWriter(&buf)

	header := make([]string, len(t.Columns))
	for i, c := range t.Columns {
		header[i] = c.Name
	}

	if err := w.Write(header); err != nil {
		return nil, fmt.Errorf("write csv header: %w", err)
	}

	rec := make([]string, len(t.Columns))

	for _, row := range t.Rows {
		for i := range rec {
			rec[i] = cellString(row[i])
		}

		if err := w.Write(rec); err != nil {
			return nil, fmt.Errorf("write csv row: %w", err)
		}
	}

	w.Flush()

	return buf.Bytes(), w.Error()
}

func cellString(v any) string {
	switch c := v.(type) {
	case nil:
		return ""
	case string:
		return c
	case float64:
		return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.10f", c), "0"), ".")
	default:
		return fmt.Sprint(c)
	}
}
