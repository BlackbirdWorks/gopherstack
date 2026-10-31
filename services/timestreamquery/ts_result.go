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

// resultSet is a fully materialised query result.
type resultSet struct {
	cols    []ColumnInfo
	rows    []Row
	scanned int64
	tables  int
}

// outBytes is the size of the rendered scalar values, reported as insights OutputBytes.
func (r *resultSet) outBytes() int64 {
	var n int64

	for _, row := range r.rows {
		for _, d := range row.Data {
			if d.ScalarValue != nil {
				n += int64(len(*d.ScalarValue))
			}
		}
	}

	return n
}

func columnName(name string, i int) string {
	if reIdent.MatchString(name) && !strings.EqualFold(name, "null") {
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

func readRows(rows *sql.Rows) (*resultSet, error) {
	names, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	ctypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	var raw [][]any

	for rows.Next() {
		dest := make([]any, len(names))
		ptrs := make([]any, len(names))

		for i := range dest {
			ptrs[i] = &dest[i]
		}

		if err = rows.Scan(ptrs...); err != nil {
			return nil, err
		}

		raw = append(raw, dest)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	rs := &resultSet{rows: make([]Row, 0, len(raw))}
	types := make([]string, len(names))

	for i, n := range names {
		col := make([]any, len(raw))
		for r := range raw {
			col[r] = raw[r][i]
		}

		t := declType(ctypes[i].DatabaseTypeName())
		if t == "" {
			t = inferType(col)
		}

		types[i] = t
		rs.cols = append(rs.cols, scalarColumnInfo(columnName(n, i), t))
	}

	for _, r := range raw {
		data := make([]Datum, len(r))
		for i, v := range r {
			data[i] = datumOf(v, types[i])
		}

		rs.rows = append(rs.rows, Row{Data: data})
	}

	return rs, nil
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
		return "NaN"
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
