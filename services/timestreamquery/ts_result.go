package timestreamquery

import (
	"database/sql"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	reIdent  = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_:]*$`)
	reTSText = regexp.MustCompile(`^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.(\d{3}|\d{9})$`)
)

const twoPow63 = float64(1 << 63)

const (
	kwNull  = "null"
	kwTrue  = "true"
	kwFalse = "false"
)

func isLiteralName(name string) bool {
	switch strings.ToLower(name) {
	case kwNull, kwTrue, kwFalse:
		return true
	}

	return false
}

// resultSet is a fully materialised query result.
type resultSet struct {
	cols    []ColumnInfo
	static  []bool
	scans   []tableScanInsight
	rows    []Row
	scanned int64
	tables  int
}

func (r *resultSet) isStatic(i int) bool { return i < len(r.static) && r.static[i] }

// outBytes is the size of the rendered scalar values, reported as insights OutputBytes.
func (r *resultSet) outBytes() int64 {
	var n int64

	for _, row := range r.rows {
		for _, d := range row.Data {
			n += datumBytes(d)
		}
	}

	return n
}

// datumBytes is the rendered size of a cell, counting every nested array, row and timeseries value.
func datumBytes(d Datum) int64 {
	var n int64

	if d.ScalarValue != nil {
		n += int64(len(*d.ScalarValue))
	}

	for _, e := range d.ArrayValue {
		n += datumBytes(e)
	}

	if d.RowValue != nil {
		for _, e := range d.RowValue.Data {
			n += datumBytes(e)
		}
	}

	for _, p := range d.TimeSeriesValue {
		n += int64(len(p.Time)) + datumBytes(p.Value)
	}

	return n
}

func columnName(name string, i int) string {
	if reIdent.MatchString(name) && !isLiteralName(name) {
		return name
	}

	return "_col" + strconv.Itoa(i)
}

func declType(decl string) string {
	switch strings.ToUpper(decl) {
	case declTimestamp:
		return ScalarTypeTimestamp
	case declBigint:
		return ScalarTypeBigint
	case declDouble:
		return ScalarTypeDouble
	case declBoolean:
		return ScalarTypeBoolean
	case declVarchar:
		return ScalarTypeVarchar
	default:
		return ""
	}
}

func inferType(col []any) string {
	for _, v := range col {
		switch x := v.(type) {
		case nil:
			continue
		case int64:
			return ScalarTypeBigint
		case float64:
			return ScalarTypeDouble
		case bool:
			return ScalarTypeBoolean
		case string:
			if reTSText.MatchString(x) {
				return ScalarTypeTimestamp
			}

			return ScalarTypeVarchar
		default:
			return ScalarTypeVarchar
		}
	}

	return ScalarTypeVarchar
}

func readRows(rows *sql.Rows, plan []tsType) (*resultSet, error) {
	names, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	ctypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	raw, err := scanAll(rows, len(names))
	if err != nil {
		return nil, err
	}

	rs := &resultSet{rows: make([]Row, 0, len(raw))}
	types := make([]string, len(names))

	for i, n := range names {
		col := make([]any, len(raw))
		for r := range raw {
			col[r] = raw[r][i]
		}

		var static tsType
		if i < len(plan) {
			static = reconcileStatic(plan[i], col)
			plan[i] = static
		}

		info, typ, isStatic := columnFor(columnName(n, i), ctypes[i].DatabaseTypeName(), static, col)
		rs.cols = append(rs.cols, info)
		rs.static = append(rs.static, isStatic)
		types[i] = typ
	}

	for _, r := range raw {
		data := make([]Datum, len(r))
		for i, v := range r {
			if rs.static[i] {
				data[i] = renderValue(v, plan[i])

				continue
			}

			data[i] = datumFor(v, types[i])
		}

		rs.rows = append(rs.rows, Row{Data: data})
	}

	return rs, nil
}

// reconcileStatic keeps a numeric static type only while every value fits it; otherwise it widens the type
// (32-bit overflow, a float SQLite overflowed to) or returns the zero type so value-based typing applies.
func reconcileStatic(t tsType, col []any) tsType {
	switch t.kind {
	case tyInteger, tyBigint:
		return reconcileIntegral(t, col)
	case tyDouble:
		for _, v := range col {
			if s, ok := v.(string); ok {
				if _, err := strconv.ParseFloat(s, 64); err != nil {
					return tsType{}
				}
			}
		}
	default:
	}

	return t
}

func reconcileIntegral(t tsType, col []any) tsType {
	out := t

	for _, v := range col {
		var n float64

		switch x := v.(type) {
		case int64:
			n = float64(x)
		case float64:
			if x != math.Trunc(x) || x >= twoPow63 || x < -twoPow63 {
				return scalar(tyDouble)
			}

			n = x
		case string, []byte:
			return tsType{}
		default:
			continue
		}

		if out.kind == tyInteger && (n > math.MaxInt32 || n < math.MinInt32) {
			out = scalar(tyBigint)
		}
	}

	return out
}

func scanAll(rows *sql.Rows, width int) ([][]any, error) {
	var raw [][]any

	for rows.Next() {
		dest := make([]any, width)
		ptrs := make([]any, width)

		for i := range dest {
			ptrs[i] = &dest[i]
		}

		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}

		raw = append(raw, dest)
	}

	return raw, rows.Err()
}

// columnFor describes one result column: the statically inferred type when known, else one derived from the values.
func columnFor(name, decl string, static tsType, col []any) (ColumnInfo, string, bool) {
	if static.known() {
		return static.columnInfo(name), "", true
	}

	if kind := complexKind(col); kind != "" {
		return complexColumnInfo(name, kind, col), kind, false
	}

	t := declType(decl)
	if t == "" {
		t = inferType(col)
	}

	return scalarColumnInfo(name, t), t, false
}

func datumOf(v any, typ string) Datum {
	if v == nil {
		return NullDatum()
	}

	switch x := v.(type) {
	case []byte:
		return ScalarDatum(string(x))
	case int64:
		if typ == ScalarTypeBoolean {
			return ScalarDatum(strconv.FormatBool(x != 0))
		}

		if typ == ScalarTypeDouble {
			return ScalarDatum(javaDouble(float64(x)))
		}

		return ScalarDatum(strconv.FormatInt(x, 10))
	case float64:
		if typ == ScalarTypeBigint {
			return ScalarDatum(strconv.FormatInt(int64(x), 10))
		}

		return ScalarDatum(javaDouble(x))
	case bool:
		return ScalarDatum(strconv.FormatBool(x))
	case time.Time:
		return ScalarDatum(formatTS(x))
	case string:
		if typ == ScalarTypeTimestamp {
			if t, ok := parseTSValue(x); ok {
				return ScalarDatum(formatTS(t))
			}
		}

		return ScalarDatum(x)
	default:
		return ScalarDatum("")
	}
}

// javaDouble renders a double the way Timestream (Java Double.toString) does:
// plain decimal in [1e-3, 1e7) with at least one fractional digit, else scientific.
func javaDouble(f float64) string {
	switch {
	case math.IsNaN(f):
		return nanText
	case math.IsInf(f, 1):
		return "Infinity"
	case math.IsInf(f, -1):
		return "-Infinity"
	case f == 0:
		return "0.0"
	}

	abs := math.Abs(f)
	if abs >= 1e-3 && abs < 1e7 {
		s := strconv.FormatFloat(f, 'f', -1, 64)
		if !strings.Contains(s, ".") {
			s += ".0"
		}

		return s
	}

	s := strconv.FormatFloat(f, 'E', -1, 64)
	mant, exp, _ := strings.Cut(s, "E")

	if !strings.Contains(mant, ".") {
		mant += ".0"
	}

	exp = strings.TrimPrefix(exp, "+")
	neg := strings.HasPrefix(exp, "-")
	exp = strings.TrimLeft(strings.TrimPrefix(exp, "-"), "0")

	if neg {
		exp = "-" + exp
	}

	return mant + "E" + exp
}
