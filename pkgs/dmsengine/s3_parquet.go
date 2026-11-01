package dmsengine

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
)

func hiveType(c Column) string {
	if c.Kind == KindInt {
		return tBigint
	}

	if c.Kind == KindFloat {
		return tDouble
	}

	if c.Kind == KindBool {
		return tBoolean
	}

	p := parseNative(c.Native)

	if c.Kind == KindDecimal && p.args != "" && (p.base == tDecimal || p.base == tNumeric) {
		return "decimal(" + strings.ReplaceAll(p.args, " ", "") + ")"
	}

	if c.Kind == KindTime {
		switch p.base {
		case tDate:
			return tDate
		case tDatetime, tTimestamp, tTimestampN, tTimestampZ:
			return tTimestamp
		}
	}

	return tString
}

func parquetColumns(tt TargetTable, withOp bool, stampCol string) []parquetColumn {
	cols := make([]parquetColumn, 0, len(tt.Columns)+extraColumns)
	if withOp {
		cols = append(cols, parquetColumn{Name: "Op", Type: tString})
	}

	if stampCol != "" {
		cols = append(cols, parquetColumn{Name: stampCol, Type: tString})
	}

	for _, c := range tt.Columns {
		cols = append(cols, parquetColumn{Name: c.Name, Type: hiveType(c.Column)})
	}

	return cols
}

func parquetCell(v any, hive string) (any, error) {
	if v == nil {
		return v, nil
	}

	if b, ok := v.([]byte); ok {
		return hex.EncodeToString(b), nil
	}

	x, ok := v.(string)
	if !ok {
		return fmt.Sprint(v), nil
	}

	switch hive {
	case tBigint:
		n, err := strconv.ParseInt(x, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("value %q is not a bigint: %w", x, err)
		}

		return n, nil
	case tDouble:
		f, err := strconv.ParseFloat(x, 64)
		if err != nil {
			return nil, fmt.Errorf("value %q is not a double: %w", x, err)
		}

		return f, nil
	case tBoolean:
		return x == "1" || x == "t" || strings.EqualFold(x, "true"), nil
	}

	return x, nil
}

type pqBuf struct {
	cols  []parquetColumn
	rows  [][]any
	table TargetTable
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
