package rdsdata

import (
	"database/sql"
	"fmt"
	"math/big"
	"slices"
	"strconv"
	"strings"
	"time"
)

// java.sql.Types codes reported in ColumnMetadata.Type (API_ColumnMetadata.html).
const (
	jdbcBit          = -7
	jdbcTinyInt      = -6
	jdbcBigInt       = -5
	jdbcLongVarBin   = -4
	jdbcVarBinary    = -3
	jdbcBinary       = -2
	jdbcLongVarchar  = -1
	jdbcChar         = 1
	jdbcNumeric      = 2
	jdbcSmallInt     = 5
	jdbcReal         = 7
	jdbcDate         = 91
	jdbcTime         = 92
	jdbcTimestamp    = 93
	jdbcOther        = 1111
	jdbcArray        = 2003
	timestampLayout  = "2006-01-02 15:04:05.999999"
	pgArrayPrefix    = "_"
	mysqlUnsignedTag = "UNSIGNED "
)

// SQL type names shared with the SQLite affinity classifier.
const (
	typeInteger = "INTEGER"
	typeText    = "TEXT"
	typeNumeric = "NUMERIC"
)

// jdbcTypeFor maps an upper-cased driver type name to its java.sql.Types code.
func jdbcTypeFor(kind, name string) int32 {
	name = strings.TrimPrefix(name, mysqlUnsignedTag)

	if kind == kindPostgres && strings.HasPrefix(name, pgArrayPrefix) {
		return jdbcArray
	}

	if code, ok := numericJDBCType(name); ok {
		return code
	}

	switch name {
	case "BPCHAR", "CHAR":
		return jdbcChar
	case typeText:
		return textJDBCType(kind)
	case "BYTEA", "BLOB", "LONGBLOB", "MEDIUMBLOB", "TINYBLOB":
		return blobJDBCType(kind)
	case "BINARY":
		return jdbcBinary
	case "VARBINARY":
		return jdbcVarBinary
	case "DATE":
		return jdbcDate
	case "TIME", "TIMETZ":
		return jdbcTime
	case "TIMESTAMP", "TIMESTAMPTZ", "DATETIME":
		return jdbcTimestamp
	case "UUID", "JSON", "JSONB":
		return jdbcOther
	default:
		return jdbcTypeVarchar
	}
}

func numericJDBCType(name string) (int32, bool) {
	switch name {
	case "INT2", "SMALLINT", "YEAR":
		return jdbcSmallInt, true
	case "INT4", "INT", typeInteger, "MEDIUMINT":
		return jdbcTypeInteger, true
	case "INT8", "BIGINT":
		return jdbcBigInt, true
	case "TINYINT":
		return jdbcTinyInt, true
	case "FLOAT4", "FLOAT":
		return jdbcReal, true
	case "FLOAT8", "DOUBLE":
		return jdbcTypeDouble, true
	case typeNumeric:
		return jdbcNumeric, true
	case "DECIMAL":
		return jdbcTypeDecimal, true
	case "BOOL", "BIT":
		return jdbcBit, true
	}

	return 0, false
}

func textJDBCType(kind string) int32 {
	if kind == kindPostgres {
		return jdbcTypeVarchar
	}

	return jdbcLongVarchar
}

func blobJDBCType(kind string) int32 {
	if kind == kindPostgres {
		return jdbcBinary
	}

	return jdbcLongVarBin
}

func isIntegralType(name string) bool {
	switch strings.TrimPrefix(name, mysqlUnsignedTag) {
	case "INT2", "INT4", "INT8", "SMALLINT", "INT", typeInteger, "MEDIUMINT", "BIGINT", "TINYINT", "YEAR":
		return true
	}

	return false
}

func isFloatType(name string) bool {
	switch name {
	case "FLOAT4", "FLOAT8", "FLOAT", "DOUBLE":
		return true
	}

	return false
}

func isTextualType(kind, name string) bool {
	switch name {
	case typeText, "VARCHAR", "BPCHAR", "CHAR", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT":
		return true
	}

	return kind == kindPostgres && name == "NAME"
}

// realColumnMetadata builds ColumnMetadata from the driver's column type; table/schema stay empty
// because database/sql exposes no column origin.
func realColumnMetadata(kind string, ct *sql.ColumnType) ColumnMetadata {
	name := strings.ToUpper(ct.DatabaseTypeName())
	typeName := ct.DatabaseTypeName()

	if kind == kindPostgres {
		typeName = strings.ToLower(typeName)
	}

	meta := ColumnMetadata{
		Name:            ct.Name(),
		Label:           ct.Name(),
		TypeName:        typeName,
		Type:            jdbcTypeFor(kind, name),
		Nullable:        columnNullableUnknown,
		IsSigned:        isIntegralType(name) || isFloatType(name) || name == typeNumeric || name == "DECIMAL",
		IsCaseSensitive: isTextualType(kind, name),
	}

	if strings.HasPrefix(name, mysqlUnsignedTag) {
		meta.IsSigned = false
	}

	if nullable, ok := ct.Nullable(); ok {
		meta.Nullable = columnNoNulls
		if nullable {
			meta.Nullable = columnNullable
		}
	}

	if precision, scale, ok := ct.DecimalSize(); ok {
		meta.Precision, meta.Scale = int32(precision), int32(scale) //nolint:gosec // bounded by driver
	}

	if meta.Type == jdbcArray {
		meta.ArrayBaseColumnType = jdbcTypeFor(kind, strings.TrimPrefix(name, pgArrayPrefix))
	}

	return meta
}

// realField converts a scanned driver value into a Field union member.
func realField(kind string, meta ColumnMetadata, v any) (Field, error) {
	switch typed := v.(type) {
	case nil:
		return fieldFromValue(nil), nil
	case string:
		if meta.Type == jdbcArray {
			return pgArrayField(meta, typed)
		}

		return fieldFromValue(typed), nil
	case bool:
		return fieldFromValue(typed), nil
	case int64:
		return Field{LongValue: &typed}, nil
	case uint64:
		return unsignedField(typed), nil
	case float32:
		d := float64(typed)

		return Field{DoubleValue: &d}, nil
	case float64:
		return Field{DoubleValue: &typed}, nil
	case time.Time:
		s := formatRealTime(meta.Type, typed)

		return Field{StringValue: &s}, nil
	case []byte:
		return realBytesField(kind, meta, typed)
	default:
		return Field{}, errUnsupportedResult(fmt.Sprintf("unsupported column type %q", meta.TypeName))
	}
}

func unsignedField(u uint64) Field {
	if u > 1<<63-1 {
		s := strconv.FormatUint(u, 10)

		return Field{StringValue: &s}
	}

	l := int64(u)

	return Field{LongValue: &l}
}

func formatRealTime(jdbcType int32, t time.Time) string {
	if jdbcType == jdbcDate {
		return t.Format(time.DateOnly)
	}

	return t.UTC().Format(timestampLayout)
}

func realBytesField(kind string, meta ColumnMetadata, b []byte) (Field, error) {
	name := strings.ToUpper(meta.TypeName)

	switch {
	case meta.Type == jdbcArray:
		return pgArrayField(meta, string(b))
	case meta.Type == jdbcBinary || meta.Type == jdbcVarBinary || meta.Type == jdbcLongVarBin:
		return Field{BlobValue: append([]byte{}, b...)}, nil
	case kind == kindMySQL && name == "BIT":
		return mysqlBitField(b), nil
	case kind == kindMySQL && isIntegralType(name):
		return parseIntegral(b), nil
	case kind == kindMySQL && isFloatType(name):
		f, err := strconv.ParseFloat(string(b), 64)
		if err != nil {
			return Field{}, errUnsupportedResult("unparsable floating point value")
		}

		return Field{DoubleValue: &f}, nil
	default:
		s := string(b)

		return Field{StringValue: &s}, nil
	}
}

func parseIntegral(b []byte) Field {
	if l, err := strconv.ParseInt(string(b), 10, 64); err == nil {
		return Field{LongValue: &l}
	}

	s := string(b)

	return Field{StringValue: &s}
}

func mysqlBitField(b []byte) Field {
	if len(b) == 1 && b[0] <= 1 {
		v := b[0] == 1

		return Field{BooleanValue: &v}
	}

	n := new(big.Int).SetBytes(b)
	if n.IsInt64() {
		l := n.Int64()

		return Field{LongValue: &l}
	}

	s := n.String()

	return Field{StringValue: &s}
}

// pgArrayField parses a one-dimensional PostgreSQL array literal into an ArrayValue.
func pgArrayField(meta ColumnMetadata, text string) (Field, error) {
	elems, err := parsePGArray(text)
	if err != nil {
		return Field{}, err
	}

	arr := &ArrayValue{}

	switch meta.ArrayBaseColumnType {
	case jdbcSmallInt, jdbcTypeInteger, jdbcBigInt:
		arr.LongValues = make([]int64, 0, len(elems))
	case jdbcBit:
		arr.BooleanValues = make([]bool, 0, len(elems))
	case jdbcReal, jdbcTypeDouble:
		arr.DoubleValues = make([]float64, 0, len(elems))
	default:
		arr.StringValues = make([]string, 0, len(elems))
	}

	for _, e := range elems {
		if err = appendArrayElem(arr, meta.ArrayBaseColumnType, e); err != nil {
			return Field{}, err
		}
	}

	return Field{ArrayValue: arr}, nil
}

func appendArrayElem(arr *ArrayValue, base int32, e string) error {
	var err error

	switch base {
	case jdbcSmallInt, jdbcTypeInteger, jdbcBigInt:
		var l int64
		if l, err = strconv.ParseInt(e, 10, 64); err == nil {
			arr.LongValues = append(arr.LongValues, l)
		}
	case jdbcBit:
		arr.BooleanValues = append(arr.BooleanValues, e == "t" || e == "true")
	case jdbcReal, jdbcTypeDouble:
		var f float64
		if f, err = strconv.ParseFloat(e, 64); err == nil {
			arr.DoubleValues = append(arr.DoubleValues, f)
		}
	default:
		arr.StringValues = append(arr.StringValues, e)
	}

	if err != nil {
		return errUnsupportedResult("unparsable array element")
	}

	return nil
}

// parsePGArray splits a one-dimensional array literal; NULL elements and nesting are unsupported results.
func parsePGArray(text string) ([]string, error) {
	if len(text) < 2 || text[0] != '{' || text[len(text)-1] != '}' {
		return nil, errUnsupportedResult("unsupported array representation")
	}

	body := text[1 : len(text)-1]
	if body == "" {
		return []string{}, nil
	}

	var (
		elems []string
		cur   strings.Builder
	)

	for i := 0; i < len(body); i++ {
		switch body[i] {
		case '{':
			return nil, errUnsupportedResult("multidimensional arrays are not supported")
		case '"':
			var quoted string

			quoted, i = readQuotedElem(body, i)
			cur.WriteString(quoted)
		case ',':
			elems = append(elems, cur.String())
			cur.Reset()
		default:
			cur.WriteByte(body[i])
		}
	}

	elems = append(elems, cur.String())

	if slices.Contains(elems, "NULL") {
		return nil, errUnsupportedResult("arrays containing NULL are not supported")
	}

	return elems, nil
}

// readQuotedElem unescapes the quoted element at i and returns the index of its closing quote.
func readQuotedElem(body string, i int) (string, int) {
	var out strings.Builder

	j := i + 1
	for ; j < len(body) && body[j] != '"'; j++ {
		if body[j] == '\\' {
			j++
		}

		if j < len(body) {
			out.WriteByte(body[j])
		}
	}

	return out.String(), j
}

// shapeRealField applies ResultSetOptions (API_ResultSetOptions.html) to numeric columns.
func shapeRealField(f Field, meta ColumnMetadata, opts resultSetOptions) Field {
	switch meta.Type {
	case jdbcTinyInt, jdbcSmallInt, jdbcTypeInteger, jdbcBigInt:
		if opts.LongReturnType == longReturnTypeString {
			return longFieldAsString(f)
		}
	case jdbcNumeric, jdbcTypeDecimal:
		if opts.DecimalReturnType == decimalReturnTypeDoubleOrLong {
			return decimalFieldAsDoubleOrLong(f)
		}
	}

	return f
}
