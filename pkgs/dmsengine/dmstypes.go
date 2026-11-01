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
		"bytes": KindBytes, "blob": KindBytes,
		"date": KindTime, "time": KindTime, "datetime": KindTime,
		"int1": KindInt, "int2": KindInt, "int4": KindInt, "int8": KindInt,
		"uint1": KindInt, "uint2": KindInt, "uint4": KindInt, "uint8": KindInt,
		"numeric": KindDecimal, "real4": KindFloat, "real8": KindFloat,
		"string": KindString, "wstring": KindString, "clob": KindString, "nclob": KindString,
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
		tTinyint: "int1", tSmallint: "int2", "int2": "int2", tMediumint: "int4", tInt: "int4", tInteger: "int4",
		"int4": "int4", "serial": "int4", tBigint: "int8", "int8": "int8", "bigserial": "int8",
		tDecimal: tNumeric, tNumeric: tNumeric, tFloat: "real4", tReal: "real4", "float4": "real4",
		tDouble: "real8", tDoublePrec: "real8", "float8": "real8", tBoolean: tBoolean, "bool": tBoolean,
		tDate: tDate, tTime: tTime, tDatetime: tDatetime, tTimestamp: tDatetime, tTimestampN: tDatetime,
		tTimestampZ: tDatetime, tVarchar: tString, tChar: tString, tText: tString, "character varying": tString,
		"character": tString, "bpchar": tString, tLongtext: "clob", "mediumtext": "clob", "tinytext": tString,
		"blob": "blob", tLongblob: "blob", tBytea: "blob", "binary": "bytes", "varbinary": "bytes",
	}

	return m[p.base]
}

const (
	maxMySQLVarchar = 16383
	maxMySQLVarbin  = 65535
)

// SQLType renders the DMS type for a SQL target of family fam.
func (d DMSType) SQLType(fam Family) string {
	pg := fam == FamilyPostgres

	switch d.Name {
	case "int1":
		return pick(pg, tSmallint, "tinyint")
	case "int2":
		return tSmallint
	case "int4":
		return pick(pg, tInteger, tInt)
	case "int8":
		return tBigint
	case "uint1":
		return pick(pg, tSmallint, "tinyint unsigned")
	case "uint2":
		return pick(pg, tInteger, "smallint unsigned")
	case "uint4":
		return pick(pg, tBigint, "int unsigned")
	case "uint8":
		return pick(pg, "numeric(20)", "bigint unsigned")
	case tNumeric:
		return d.numeric()
	case "real4":
		return pick(pg, tReal, tFloat)
	case "real8":
		return pick(pg, tDoublePrec, tDouble)
	case tBoolean:
		return pick(pg, tBoolean, "tinyint(1)")
	case tDate:
		return tDate
	case tTime:
		return tTime
	case tDatetime:
		return d.datetime(pg)
	case "bytes", "blob":
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
		return fmt.Sprintf("%s(%d)", base, min(scale, 6))
	}

	return base
}

func (d DMSType) binary(pg bool) string {
	switch {
	case pg:
		return tBytea
	case d.Name == "bytes" && d.Length > 0 && d.Length <= maxMySQLVarbin:
		return fmt.Sprintf("varbinary(%d)", d.Length)
	default:
		return tLongblob
	}
}

func (d DMSType) text(pg bool) string {
	long := d.Name == "clob" || d.Name == "nclob" || d.Length <= 0

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
