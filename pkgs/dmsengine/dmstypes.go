package dmsengine

import (
	"fmt"
	"strings"
)

// DMSType is a documented AWS DMS data type with its length, precision and scale.
type DMSType struct {
	Name      string
	Length    int
	Precision int
	Scale     int
}

// dmsTypeKinds maps the DMS data types the transformation rules and S3 sources accept to a value class.
func dmsTypeKinds() map[string]Kind {
	return map[string]Kind{
		tBytes: KindBytes, tBlob: KindBytes,
		tDate: KindTime, tTime: KindTime, "datetime": KindTime,
		tInt1: KindInt, tInt2: KindInt, tInt4: KindInt, tInt8: KindInt,
		"uint1": KindInt, "uint2": KindInt, "uint4": KindInt, "uint8": KindInt,
		"numeric": KindDecimal, tReal4: KindFloat, tReal8: KindFloat,
		"string": KindString, "wstring": KindString, tClob: KindString, tNclob: KindString,
		"boolean": KindBool,
	}
}

// ParseDMSType validates a DMS data type name.
func ParseDMSType(name string) (DMSType, error) {
	n := strings.ToLower(strings.TrimSpace(name))
	if _, ok := dmsTypeKinds()[n]; !ok {
		return DMSType{}, fmt.Errorf("%w: unsupported data-type %q", ErrMapping, name)
	}

	return DMSType{Name: n}, nil
}

// Kind is the value class of the type.
func (d DMSType) Kind() Kind { return dmsTypeKinds()[d.Name] }

// Column derives a nullable column of the type.
func (d DMSType) Column(name string) Column {
	return Column{Name: name, Native: d.Name, Kind: d.Kind(), Nullable: true, DMS: d}
}

// dmsTypeOf maps an engine-native type to a DMS type name; empty when unknown.
func dmsTypeOf(native string) string {
	p := parseNative(native)
	m := map[string]string{
		tTinyint: tInt1, tSmallint: tInt2, tInt2: tInt2, tMediumint: tInt4, tInt: tInt4, tInteger: tInt4,
		tInt4: tInt4, tSerial: tInt4, tBigint: tInt8, tInt8: tInt8, tBigserial: tInt8,
		tDecimal: tNumeric, tNumeric: tNumeric, tFloat: tReal4, tReal: tReal4, tFloat4: tReal4,
		tDouble: tReal8, tDoublePrec: tReal8, "float8": tReal8, tBoolean: tBoolean, tBool: tBoolean,
		tDate: tDate, tTime: tTime, tDatetime: tDatetime, tTimestamp: tDatetime, tTimestampN: tDatetime,
		tTimestampZ: tDatetime, tVarchar: tString, tChar: tString, tText: tString, "character varying": tString,
		"character": tString, "bpchar": tString, tLongtext: tClob, "mediumtext": tClob, "tinytext": tString,
		tBlob: tBlob, tLongblob: tBlob, tBytea: tBlob, tBinary: tBytes, tVarbinary: tBytes,
	}

	return m[p.base]
}

const (
	maxDatetimeScale = 6
	maxMySQLVarchar  = 16383
	maxMySQLVarbin   = 65535
)

// fixedSQLTypes are the DMS types whose SQL form takes no length, precision or scale: {PostgreSQL, MySQL}.
func fixedSQLTypes() map[string][2]string {
	return map[string][2]string{
		tInt1: {
			tSmallint,
			"tinyint",
		}, tInt2: {tSmallint, tSmallint}, tInt4: {tInteger, tInt}, tInt8: {tBigint, tBigint},
		"uint1": {tSmallint, "tinyint unsigned"}, "uint2": {tInteger, "smallint unsigned"},
		"uint4": {tBigint, "int unsigned"}, "uint8": {"numeric(20)", "bigint unsigned"},
		tReal4: {tReal, tFloat}, tReal8: {tDoublePrec, tDouble}, tBoolean: {tBoolean, tTinyint1},
		tDate: {tDate, tDate}, tTime: {tTime, tTime},
	}
}

// SQLType renders the DMS type for a SQL target of family fam.
func (d DMSType) SQLType(fam Family) string {
	pg := fam == FamilyPostgres

	if v, ok := fixedSQLTypes()[d.Name]; ok {
		return pick(pg, v[0], v[1])
	}

	switch d.Name {
	case tNumeric:
		return d.numeric()
	case tDatetime:
		return d.datetime(pg)
	case tBytes, tBlob:
		return d.binary(pg)
	default:
		return d.text(pg)
	}
}

func pick(first bool, a, b string) string {
	if first {
		return a
	}

	return b
}

func (d DMSType) numeric() string {
	switch {
	case d.Precision > 0 && d.Scale > 0:
		return fmt.Sprintf("numeric(%d,%d)", d.Precision, d.Scale)
	case d.Precision > 0:
		return fmt.Sprintf("numeric(%d)", d.Precision)
	default:
		return tNumeric
	}
}

func (d DMSType) datetime(pg bool) string {
	scale := d.Scale
	if scale == 0 {
		scale = d.Precision
	}

	base := pick(pg, tTimestamp, tDatetime)
	if scale > 0 {
		return fmt.Sprintf("%s(%d)", base, min(scale, maxDatetimeScale))
	}

	return base
}

func (d DMSType) binary(pg bool) string {
	switch {
	case pg:
		return tBytea
	case d.Name == tBytes && d.Length > 0 && d.Length <= maxMySQLVarbin:
		return fmt.Sprintf("varbinary(%d)", d.Length)
	default:
		return tLongblob
	}
}

func (d DMSType) text(pg bool) string {
	long := d.Name == tClob || d.Name == tNclob || d.Length <= 0

	switch {
	case pg && long:
		return tText
	case pg:
		return fmt.Sprintf("varchar(%d)", d.Length)
	case long || d.Length > maxMySQLVarchar:
		return tLongtext
	default:
		return fmt.Sprintf("varchar(%d)", d.Length)
	}
}
