package s3

import (
	"fmt"
	"io"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/parquetio"
)

// evaluateParquetQuery reads every row of a Parquet object, applies the query
// and emits the result. Parquet is only valid with CompressionType NONE: the
// file's own page codecs are handled by the reader.
func evaluateParquetQuery(w io.Writer, query *sqlQuery, data []byte, req *selectRequest) (int64, error) {
	if ct := strings.ToUpper(req.InputSerialization.CompressionType); ct != "" && ct != compressionNone {
		return 0, fmt.Errorf("%w: %q is not valid for Parquet input", errInvalidCompressionFormat, ct)
	}

	pf, err := parquetio.Open(data)
	if err != nil {
		return 0, fmt.Errorf("ParquetParsingError: %w", err)
	}

	cols := pf.Columns()

	var rows []map[string]any

	err = pf.ForEach(cols, func(vals []any) error {
		row := make(map[string]any, len(cols))
		for i, c := range cols {
			row[c] = vals[i]
		}

		rows = append(rows, row)

		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("ParquetParsingError: %w", err)
	}

	resultRows, err := evalQueryJSON(query, rows)
	if err != nil {
		return 0, err
	}

	if len(resultRows) == 0 {
		return 0, nil
	}

	body, err := serializeJSONQueryResultsOrdered(query, resultRows, req.OutputSerialization, cols)
	if err != nil {
		return 0, err
	}

	if len(body) == 0 {
		return 0, nil
	}

	if err = writeSelectEvent(w, "Records", "application/octet-stream", body); err != nil {
		return 0, err
	}

	return int64(len(body)), nil
}
