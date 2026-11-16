package athena

import (
	"database/sql/driver"
	"fmt"
	"math"
	"strings"
)

const (
	fnRowCompare = "athena_row_compare"
	fnRowKey     = "athena_row_sortkey"
	fnRowMin     = "athena_row_min"
	fnRowMax     = "athena_row_max"
	fnRowCall    = "athena_row"
	fnRowCast    = "athena_row_cast"
	sortNull     = "\x03"
	sortNumber   = "\x01"
	sortString   = "\x02"
	sortHexWidth = 16
	signMask     = uint64(1) << 63
)

func registerRowCompareFuncs() {
	registerScalar("row_compare", argsTwo, udfRowCompare)
	registerScalar("row_sortkey", 1, udfRowSortKey)
	registerAgg("row_min", 1, func() *aggState { return rowExtreme(-1) })
	registerAgg("row_max", 1, func() *aggState { return rowExtreme(1) })
}

// rowFields returns the ordered field values of a decoded ROW (or anonymous row array).
func rowFields(v any) ([]any, bool) {
	switch t := v.(type) {
	case []any:
		return t, true
	case map[string]any:
		names, ok := rowNames(t)
		if !ok {
			return nil, false
		}

		out := make([]any, len(names))
		for i, n := range names {
			out[i] = t[n]
		}

		return out, true
	default:
		return nil, false
	}
}

func decodeRow(v driver.Value) (any, bool) {
	s, ok := asString(v)
	if !ok {
		return nil, false
	}

	d, ok := decodeComplex(s)
	if !ok {
		return nil, false
	}

	return numbersByText(d), true
}

// compareDecoded orders two decoded values; null=true means a NULL made the order indeterminate.
func compareDecoded(a, b any) (int, bool) {
	if a == nil || b == nil {
		return 0, true
	}

	if af, bf, ok := numberPair(a, b); ok {
		return cmpFloat(af, bf), false
	}

	if as, ok := a.(string); ok {
		if bs, isStr := b.(string); isStr {
			return strings.Compare(as, bs), false
		}
	}

	if ab, ok := a.(bool); ok {
		if bb, isBool := b.(bool); isBool {
			return cmpFloat(boolFloat(ab), boolFloat(bb)), false
		}
	}

	if af, ok := rowFields(a); ok {
		if bf, isRow := rowFields(b); isRow {
			return compareFields(af, bf)
		}
	}

	return strings.Compare(fmt.Sprint(a), fmt.Sprint(b)), false
}

func compareFields(a, b []any) (int, bool) {
	for i := range min(len(a), len(b)) {
		c, null := compareDecoded(a[i], b[i])
		if null {
			return 0, true
		}

		if c != 0 {
			return c, false
		}
	}

	return cmpFloat(float64(len(a)), float64(len(b))), false
}

func numberPair(a, b any) (float64, float64, bool) {
	af, aok := jsonFloat(a)
	bf, bok := jsonFloat(b)

	return af, bf, aok && bok
}

func boolFloat(b bool) float64 {
	if b {
		return 1
	}

	return 0
}

func cmpFloat(a, b float64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func udfRowCompare(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	a, aok := decodeRow(args[0])
	b, bok := decodeRow(args[1])

	if !aok || !bok {
		return sqlNull(), nil
	}

	c, null := compareDecoded(a, b)
	if null {
		return sqlNull(), nil
	}

	return int64(c), nil
}

// sortKey encodes a decoded value so plain text comparison matches field-by-field ordering.
func sortKey(v any, sb *strings.Builder) {
	switch t := v.(type) {
	case nil:
		sb.WriteString(sortNull)
	case string:
		sb.WriteString(sortString + t + "\x00")
	case bool:
		sb.WriteString(sortNumber + sortableFloat(boolFloat(t)))
	case []any, map[string]any:
		if fields, ok := rowFields(t); ok {
			for _, f := range fields {
				sortKey(f, sb)
			}

			return
		}

		sb.WriteString(sortString + fmt.Sprint(t) + "\x00")
	default:
		if f, ok := jsonFloat(t); ok {
			sb.WriteString(sortNumber + sortableFloat(f))

			return
		}

		sb.WriteString(sortString + fmt.Sprint(t) + "\x00")
	}
}

func sortableFloat(f float64) string {
	bits := math.Float64bits(f)
	if bits&signMask != 0 {
		bits = ^bits
	} else {
		bits |= signMask
	}

	return fmt.Sprintf("%0*x", sortHexWidth, bits)
}

func udfRowSortKey(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	v, ok := decodeRow(args[0])
	if !ok {
		return sqlNull(), nil
	}

	var sb strings.Builder

	sortKey(v, &sb)

	return sb.String(), nil
}

// rowExtreme keeps the smallest (dir<0) or largest (dir>0) ROW seen; NULL fields never decide the order.
func rowExtreme(dir int) *aggState {
	var (
		best any
		text driver.Value
	)

	return &aggState{
		step: func(args []driver.Value) error {
			if args[0] == nil {
				return nil
			}

			v, ok := decodeRow(args[0])
			if !ok {
				return nil
			}

			if best == nil {
				best, text = v, args[0]

				return nil
			}

			if c, null := compareDecoded(v, best); !null && c*dir > 0 {
				best, text = v, args[0]
			}

			return nil
		},
		final: func() driver.Value {
			if text == nil {
				return sqlNull()
			}

			return text
		},
	}
}
