// Package parquetio reads and writes Parquet files for the SQL engines
// (Athena, S3 Select, Firehose format conversion).
package parquetio

import (
	"errors"
	"strconv"
	"strings"
)

var (
	// ErrUnsupportedType is returned for a column type the writer cannot encode.
	ErrUnsupportedType = errors.New("unsupported parquet column type")
	// ErrBadValue is returned when a cell does not parse as its column type.
	ErrBadValue = errors.New("invalid parquet cell value")
	// ErrBadFile is returned when the bytes are not a readable Parquet file.
	ErrBadFile = errors.New("invalid parquet file")
)

const (
	typeBoolean  = "boolean"
	typeTinyint  = "tinyint"
	typeSmall    = "smallint"
	typeInt      = "int"
	typeInteger  = "integer"
	typeBigint   = "bigint"
	typeFloat    = "float"
	typeReal     = "real"
	typeDouble   = "double"
	typeString   = "string"
	typeVarchar  = "varchar"
	typeChar     = "char"
	typeBinary   = "binary"
	typeDate     = "date"
	typeTS       = "timestamp"
	typeDecimal  = "decimal"
	typeNumeric  = "numeric"
	defaultPrec  = 10
	maxDecPrec   = 38
	maxInt32Prec = 9
	maxInt64Prec = 18
)

// baseType lower-cases a Hive/Athena type and strips its parameters.
func baseType(t string) string {
	b, _, _ := strings.Cut(strings.ToLower(strings.TrimSpace(t)), "(")

	return strings.TrimSpace(b)
}

// decimalParams returns precision and scale of decimal(p,s); bare decimal is (10,0).
func decimalParams(t string) (int, int) {
	_, rest, ok := strings.Cut(strings.ToLower(t), "(")
	if !ok {
		return defaultPrec, 0
	}

	rest = strings.TrimSuffix(strings.TrimSpace(rest), ")")
	ps, ss, hasScale := strings.Cut(rest, ",")

	p, err := strconv.Atoi(strings.TrimSpace(ps))
	if err != nil {
		return defaultPrec, 0
	}

	s := 0
	if hasScale {
		s, _ = strconv.Atoi(strings.TrimSpace(ss))
	}

	return p, s
}
