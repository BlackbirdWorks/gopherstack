package redshiftdata

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"strconv"
)

const (
	keyRecords        = "Records"
	keyColumnMetadata = "ColumnMetadata"
	keyTotalNumRows   = "TotalNumRows"
)

// engineStatementResult renders an engine result set in the requested format; getErr is the lookup error.
func (h *Handler) engineStatementResult(
	ctx context.Context, id, nextToken, format string, rs *ResultSet, getErr error,
) ([]byte, error) {
	if getErr != nil {
		return nil, getErr
	}

	stmt, err := h.Backend.DescribeStatement(ctx, parentStatementID(id))
	if err != nil {
		return nil, err
	}

	if want := statementResultFormat(stmt); want != format {
		return nil, fmt.Errorf("%w: statement %s result format is not %s", ErrValidation, id, format)
	}

	if nextToken != "" {
		return nil, fmt.Errorf("%w: invalid NextToken, the result fits on one page", ErrValidation)
	}

	if format == resultFormatCSV {
		return renderCSVResult(rs)
	}

	return json.Marshal(map[string]any{
		keyRecords:        rs.Rows,
		keyColumnMetadata: rs.Columns,
		keyTotalNumRows:   int64(len(rs.Rows)),
		keyNextToken:      "",
	})
}

func renderCSVResult(rs *ResultSet) ([]byte, error) {
	records := make([]map[string]any, 0, len(rs.Rows))

	for _, row := range rs.Rows {
		var buf bytes.Buffer

		w := csv.NewWriter(&buf)
		cells := make([]string, len(row))

		for i, f := range row {
			cells[i] = csvCell(f)
		}

		if err := w.Write(cells); err != nil {
			return nil, fmt.Errorf("encode csv record: %w", err)
		}

		w.Flush()

		records = append(records, map[string]any{keyCSVRecords: string(bytes.TrimRight(buf.Bytes(), "\n"))})
	}

	return json.Marshal(map[string]any{
		keyRecords:        records,
		keyColumnMetadata: rs.Columns,
		keyTotalNumRows:   int64(len(rs.Rows)),
		keyResultFormat:   resultFormatCSV,
		keyNextToken:      "",
	})
}

func csvCell(f Field) string {
	switch {
	case f.IsNull:
		return ""
	case f.StringValue != nil:
		return *f.StringValue
	case f.LongValue != nil:
		return strconv.FormatInt(*f.LongValue, 10)
	case f.DoubleValue != nil:
		return fmt.Sprint(*f.DoubleValue)
	case f.BooleanValue != nil:
		return strconv.FormatBool(*f.BooleanValue)
	case f.BlobValue != nil:
		return *f.BlobValue
	default:
		return ""
	}
}
