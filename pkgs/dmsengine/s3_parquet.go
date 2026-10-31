package dmsengine

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
)

func hiveType(c Column) string {
	p := parseNative(c.Native)

	switch c.Kind {
	case KindInt:
		return "bigint"
	case KindFloat:
		return "double"
	case KindBool:
		return "boolean"
	case KindDecimal:
		if p.args != "" && (p.base == "decimal" || p.base == "numeric") {
			return "decimal(" + strings.ReplaceAll(p.args, " ", "") + ")"
		}

		return "string"
	case KindTime:
		switch p.base {
		case "date":
			return "date"
		case "datetime", "timestamp", "timestamp without time zone", "timestamp with time zone":
			return "timestamp"
		}
	}

	return "string"
}

func parquetColumns(tt TargetTable, withOp bool, stampCol string) []parquetColumn {
	cols := make([]parquetColumn, 0, len(tt.Columns)+2)
	if withOp {
		cols = append(cols, parquetColumn{Name: "Op", Type: "string"})
	}

	if stampCol != "" {
		cols = append(cols, parquetColumn{Name: stampCol, Type: "string"})
	}

	for _, c := range tt.Columns {
		cols = append(cols, parquetColumn{Name: c.Name, Type: hiveType(c.Column)})
	}

	return cols
}

func parquetCell(v any, hive string) (any, error) {
	switch x := v.(type) {
	case nil:
		return nil, nil
	case []byte:
		return fmt.Sprintf("%x", x), nil
	case string:
		switch hive {
		case "bigint":
			n, err := strconv.ParseInt(x, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("value %q is not a bigint: %w", x, err)
			}

			return n, nil
		case "double":
			f, err := strconv.ParseFloat(x, 64)
			if err != nil {
				return nil, fmt.Errorf("value %q is not a double: %w", x, err)
			}

			return f, nil
		case "boolean":
			return x == "1" || x == "t" || strings.EqualFold(x, "true"), nil
		}

		return x, nil
	}

	return fmt.Sprint(v), nil
}

type pqBuf struct {
	table TargetTable
	cols  []parquetColumn
	rows  [][]any
}

func newPQBuf(tt TargetTable, withOp bool, stampCol string) *pqBuf {
	return &pqBuf{table: tt, cols: parquetColumns(tt, withOp, stampCol)}
}

func (b *pqBuf) add(op, stamp string, row Row, withOp bool, stampCol string) error {
	cells := make([]any, 0, len(b.cols))
	if withOp {
		cells = append(cells, op)
	}

	if stampCol != "" {
		cells = append(cells, stamp)
	}

	off := len(cells)

	for i, v := range row {
		cell, err := parquetCell(v, b.cols[off+i].Type)
		if err != nil {
			return err
		}

		cells = append(cells, cell)
	}

	b.rows = append(b.rows, cells)

	return nil
}

func (t *s3Target) writeParquet(ctx context.Context, b *pqBuf, file string) error {
	if len(b.rows) == 0 {
		return nil
	}

	var out bytes.Buffer
	if err := writeParquetFile(&out, b.cols, b.rows); err != nil {
		return err
	}

	return t.store.PutObject(ctx, t.opts.BucketName, t.keyFor(b.table, file), out.Bytes())
}
