package parquetio

import (
	"fmt"
	"io"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
)

const (
	secondsPerDay = 86400
	bitsPerByte   = 8
	int8Bits      = 8
	int16Bits     = 16
	int32Bits     = 32
	int64Bits     = 64
	createdBy     = "gopherstack"
	decimalBase   = 10
)

// Column is one top-level column to write; Type is a Hive/Athena type name.
type Column struct {
	Name string
	Type string
	// FieldID is written as the Parquet field id when positive (Iceberg data files).
	FieldID int
}

type cellEnc func(v any) (parquet.Value, error)

type colEnc struct {
	node parquet.Node
	enc  cellEnc
}

func codecFor(name string) (compress.Codec, error) {
	switch strings.ToUpper(strings.TrimSpace(name)) {
	case "", "SNAPPY":
		return &parquet.Snappy, nil
	case "GZIP":
		return &parquet.Gzip, nil
	case "ZSTD":
		return &parquet.Zstd, nil
	case "LZ4":
		return &parquet.Lz4Raw, nil
	case "NONE", "UNCOMPRESSED":
		return &parquet.Uncompressed, nil
	default:
		return nil, fmt.Errorf("%w: compression %q", ErrUnsupportedType, name)
	}
}

// Write encodes rows as one Parquet file. Cells are nil, bool, int64,
// float64 or string; date/timestamp/decimal cells are strings. Nested Hive
// types (array/map/struct) are stored as UTF-8 strings.
func Write(w io.Writer, cols []Column, rows [][]any, compression string) error {
	codec, err := codecFor(compression)
	if err != nil {
		return err
	}

	encs := make([]colEnc, len(cols))
	group := newOrderedGroup()

	for i, c := range cols {
		if encs[i], err = encoderFor(c.Type); err != nil {
			return fmt.Errorf("column %q: %w", c.Name, err)
		}

		node := parquet.Optional(encs[i].node)
		if c.FieldID > 0 {
			node = parquet.FieldID(node, c.FieldID)
		}

		group.add(c.Name, node)
	}

	schema := parquet.NewSchema("hive_schema", group)
	order := schema.Columns()
	pos := make(map[string]int, len(order))

	for i, p := range order {
		pos[strings.Join(p, ".")] = i
	}

	pw := parquet.NewWriter(w, schema, parquet.Compression(codec), parquet.CreatedBy(createdBy, "", ""))
	batch := make([]parquet.Row, 0, len(rows))

	for _, r := range rows {
		row := make(parquet.Row, len(cols))

		for i, c := range cols {
			var cell any
			if i < len(r) {
				cell = r[i]
			}

			col := pos[c.Name]
			if cell == nil {
				row[col] = parquet.NullValue().Level(0, 0, col)

				continue
			}

			v, verr := encs[i].enc(cell)
			if verr != nil {
				return fmt.Errorf("column %q: %w", c.Name, verr)
			}

			row[col] = v.Level(0, 1, col)
		}

		batch = append(batch, row)
	}

	if _, err = pw.WriteRows(batch); err != nil {
		return err
	}

	return pw.Close()
}

func encoderFor(typ string) (colEnc, error) {
	switch baseType(typ) {
	case typeBoolean:
		return colEnc{parquet.Leaf(parquet.BooleanType), encBool}, nil
	case typeTinyint:
		return colEnc{parquet.Int(int8Bits), encIntN(int8Bits)}, nil
	case typeSmall:
		return colEnc{parquet.Int(int16Bits), encIntN(int16Bits)}, nil
	case typeInt, typeInteger:
		return colEnc{parquet.Int(int32Bits), encIntN(int32Bits)}, nil
	case typeBigint:
		return colEnc{parquet.Int(int64Bits), encInt64}, nil
	case typeFloat, typeReal:
		return colEnc{parquet.Leaf(parquet.FloatType), encFloat}, nil
	case typeDouble:
		return colEnc{parquet.Leaf(parquet.DoubleType), encDouble}, nil
	case typeBinary:
		return colEnc{parquet.Leaf(parquet.ByteArrayType), encBytes}, nil
	case typeDate:
		return colEnc{parquet.Date(), encDate}, nil
	case typeTS:
		return colEnc{parquet.TimestampAdjusted(parquet.Microsecond, false), encTimestamp}, nil
	case typeDecimal, typeNumeric:
		return decimalEncoder(typ)
	default:
		return colEnc{parquet.String(), encString}, nil
	}
}

func asString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	case int64:
		return strconv.FormatInt(t, 10)
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(t)
	default:
		return fmt.Sprint(t)
	}
}

func encString(v any) (parquet.Value, error) { return parquet.ByteArrayValue([]byte(asString(v))), nil }

func encBytes(v any) (parquet.Value, error) { return parquet.ByteArrayValue([]byte(asString(v))), nil }

func encBool(v any) (parquet.Value, error) {
	if b, ok := v.(bool); ok {
		return parquet.BooleanValue(b), nil
	}

	b, err := strconv.ParseBool(asString(v))
	if err != nil {
		return parquet.Value{}, fmt.Errorf("%w: %v as boolean", ErrBadValue, v)
	}

	return parquet.BooleanValue(b), nil
}

func toInt64(v any) (int64, error) {
	switch t := v.(type) {
	case int64:
		return t, nil
	case float64:
		return int64(t), nil
	}

	n, err := strconv.ParseInt(strings.TrimSpace(asString(v)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: %v as integer", ErrBadValue, v)
	}

	return n, nil
}

func encIntN(bits uint) cellEnc {
	lo, hi := -(int64(1) << (bits - 1)), int64(1)<<(bits-1)-1

	return func(v any) (parquet.Value, error) {
		n, err := toInt64(v)
		if err != nil {
			return parquet.Value{}, err
		}

		if n < lo || n > hi {
			return parquet.Value{}, fmt.Errorf("%w: %d out of range for %d-bit integer", ErrBadValue, n, bits)
		}

		return parquet.Int32Value(clampInt32(n)), nil
	}
}

func encInt64(v any) (parquet.Value, error) {
	n, err := toInt64(v)
	if err != nil {
		return parquet.Value{}, err
	}

	return parquet.Int64Value(n), nil
}

func toFloat(v any) (float64, error) {
	switch t := v.(type) {
	case float64:
		return t, nil
	case int64:
		return float64(t), nil
	}

	f, err := strconv.ParseFloat(strings.TrimSpace(asString(v)), 64)
	if err != nil {
		return 0, fmt.Errorf("%w: %v as floating point", ErrBadValue, v)
	}

	return f, nil
}

func encFloat(v any) (parquet.Value, error) {
	f, err := toFloat(v)
	if err != nil {
		return parquet.Value{}, err
	}

	return parquet.FloatValue(float32(f)), nil
}

func encDouble(v any) (parquet.Value, error) {
	f, err := toFloat(v)
	if err != nil {
		return parquet.Value{}, err
	}

	return parquet.DoubleValue(f), nil
}

func parseTime(s string) (time.Time, error) {
	s = strings.TrimSpace(s)

	for _, l := range []string{
		"2006-01-02 15:04:05.999999999", "2006-01-02T15:04:05.999999999", time.RFC3339Nano, "2006-01-02",
	} {
		if t, err := time.Parse(l, s); err == nil {
			return t.UTC(), nil
		}
	}

	return time.Time{}, fmt.Errorf("%w: %q as date/timestamp", ErrBadValue, s)
}

func encDate(v any) (parquet.Value, error) {
	t, err := parseTime(asString(v))
	if err != nil {
		return parquet.Value{}, err
	}

	return parquet.Int32Value(clampInt32(t.Unix() / secondsPerDay)), nil
}

func encTimestamp(v any) (parquet.Value, error) {
	t, err := parseTime(asString(v))
	if err != nil {
		return parquet.Value{}, err
	}

	return parquet.Int64Value(t.UnixMicro()), nil
}

func decimalEncoder(typ string) (colEnc, error) {
	p, s := decimalParams(typ)
	if p < 1 || p > maxDecPrec || s < 0 || s > p {
		return colEnc{}, fmt.Errorf("%w: %s", ErrUnsupportedType, typ)
	}

	unscaled := func(v any) (*big.Int, error) {
		r, ok := new(big.Rat).SetString(strings.TrimSpace(asString(v)))
		if !ok {
			return nil, fmt.Errorf("%w: %v as decimal", ErrBadValue, v)
		}

		scale := new(big.Int).Exp(big.NewInt(decimalBase), big.NewInt(int64(s)), nil)
		r.Mul(r, new(big.Rat).SetInt(scale))

		q, _ := new(big.Int).QuoRem(r.Num(), r.Denom(), new(big.Int))

		if len(new(big.Int).Abs(q).String()) > p {
			return nil, fmt.Errorf("%w: %v exceeds decimal(%d,%d)", ErrBadValue, v, p, s)
		}

		return q, nil
	}

	switch {
	case p <= maxInt32Prec:
		return colEnc{parquet.Decimal(s, p, parquet.Int32Type), func(v any) (parquet.Value, error) {
			u, err := unscaled(v)
			if err != nil {
				return parquet.Value{}, err
			}

			return parquet.Int32Value(clampInt32(u.Int64())), nil
		}}, nil
	case p <= maxInt64Prec:
		return colEnc{parquet.Decimal(s, p, parquet.Int64Type), func(v any) (parquet.Value, error) {
			u, err := unscaled(v)
			if err != nil {
				return parquet.Value{}, err
			}

			return parquet.Int64Value(u.Int64()), nil
		}}, nil
	default:
		n := decimalBytes(p)

		return colEnc{parquet.Decimal(s, p, parquet.FixedLenByteArrayType(n)), func(v any) (parquet.Value, error) {
			u, err := unscaled(v)
			if err != nil {
				return parquet.Value{}, err
			}

			return parquet.FixedLenByteArrayValue(twosComplement(u, n)), nil
		}}, nil
	}
}

// decimalBytes is the minimal big-endian width that holds a p-digit decimal.
func decimalBytes(p int) int {
	limit := new(big.Int).Exp(big.NewInt(decimalBase), big.NewInt(int64(p)), nil)

	return (limit.BitLen() + 1 + bitsPerByte - 1) / bitsPerByte
}

func twosComplement(u *big.Int, n int) []byte {
	if u.Sign() < 0 {
		mod := new(big.Int).Lsh(big.NewInt(1), uint(n*bitsPerByte))
		u = new(big.Int).Add(u, mod)
	}

	out := make([]byte, n)
	u.FillBytes(out)

	return out
}

func clampInt32(n int64) int32 {
	if n < math.MinInt32 {
		return math.MinInt32
	}

	if n > math.MaxInt32 {
		return math.MaxInt32
	}

	return int32(n)
}
