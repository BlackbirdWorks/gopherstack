package asl

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/parquetio"
)

const (
	parquetMaxRowGroupBytes = 256 << 20
	parquetMaxFooterBytes   = 5 << 20
	parquetTrailerBytes     = 8
)

var (
	// ErrParquetInvalid is returned for bytes that do not decode as a Parquet file.
	ErrParquetInvalid = errors.New("ItemReader: unable to read Parquet file")
	// ErrParquetRowGroupTooLarge is returned for a row group over the documented 256MB limit.
	ErrParquetRowGroupTooLarge = errors.New("ItemReader: Parquet row group exceeds the 256MB limit")
	// ErrParquetFooterTooLarge is returned for a footer over the documented 5MB limit.
	ErrParquetFooterTooLarge = errors.New("ItemReader: Parquet footer exceeds the 5MB limit")
)

// decodeParquetItems turns every row of a Parquet object into a JSON object
// item (AWS docs input-output-itemreader.html, "Parquet file in an Amazon S3
// bucket": 256MB row groups, 5MB footers, internal GZIP/ZSTD/Snappy).
func decodeParquetItems(data []byte) ([]any, error) {
	if n := len(data); n >= parquetTrailerBytes && string(data[n-4:]) == "PAR1" {
		if footer := binary.LittleEndian.Uint32(data[n-parquetTrailerBytes:]); footer > parquetMaxFooterBytes {
			return nil, ErrParquetFooterTooLarge
		}
	}

	pf, err := parquetio.Open(data)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrParquetInvalid, err)
	}

	if pf.MaxRowGroupBytes() > parquetMaxRowGroupBytes {
		return nil, ErrParquetRowGroupTooLarge
	}

	cols := pf.Columns()
	items := make([]any, 0, pf.NumRows())

	err = pf.ForEach(cols, func(vals []any) error {
		row := make(map[string]any, len(cols))
		for i, c := range cols {
			row[c] = vals[i]
		}

		items = append(items, row)

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrParquetInvalid, err)
	}

	return normalizeJSONNumbers(items)
}

// normalizeJSONNumbers round-trips items so numbers carry the float64 type
// every other ItemReader decoder yields.
func normalizeJSONNumbers(items []any) ([]any, error) {
	raw, err := json.Marshal(items)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrItemReaderInvalidData, err)
	}

	var out []any
	if err = json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrItemReaderInvalidData, err)
	}

	return out, nil
}
