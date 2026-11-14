package dmsengine

import (
	"regexp"
	"strings"
)

var typeRe = regexp.MustCompile(`^\s*([a-zA-Z_][a-zA-Z0-9_ ]*?)\s*(?:\(([^)]*)\))?\s*(unsigned)?\s*(zerofill)?\s*$`)

type parsedType struct {
	base     string
	args     string
	unsigned bool
}

func parseNative(native string) parsedType {
	n := strings.ToLower(strings.TrimSpace(native))
	if strings.HasPrefix(n, "enum(") || strings.HasPrefix(n, "set(") {
		return parsedType{base: n[:strings.IndexByte(n, '(')], args: n[strings.IndexByte(n, '(')+1 : len(n)-1]}
	}

	m := typeRe.FindStringSubmatch(n)
	if m == nil {
		return parsedType{base: n}
	}

	return parsedType{base: strings.TrimSpace(m[1]), args: m[2], unsigned: m[3] != ""}
}

const (
	tBigint     = "bigint"
	tInt        = "int"
	tInteger    = "integer"
	tSmallint   = "smallint"
	tTinyint    = "tinyint"
	tDecimal    = "decimal"
	tNumeric    = "numeric"
	tDouble     = "double"
	tDoublePrec = "double precision"
	tBoolean    = "boolean"
	tDate       = "date"
	tDatetime   = "datetime"
	tTimestamp  = "timestamp"
	tTime       = "time"
	tBytea      = "bytea"
	tJSON       = "json"
	tLongtext   = "longtext"
	tText       = "text"
	tChar       = "char"
	tVarchar    = "varchar"
	tString     = "string"
	tReal       = "real"
	tFloat      = "float"
	tTimestampN = "timestamp without time zone"
	tTimestampZ = "timestamp with time zone"
	tMediumint  = "mediumint"
	tLongblob   = "longblob"
	tJSONB      = "jsonb"
	tDatetime6  = "datetime(6)"
	tBlob       = "blob"
	tBinary     = "binary"
	tVarbinary  = "varbinary"
	tTinyblob   = "tinyblob"
	tMediumblob = "mediumblob"
	tSerial     = "serial"
	tBigserial  = "bigserial"
	tInt1       = "int1"
	tInt2       = "int2"
	tInt4       = "int4"
	tInt8       = "int8"
	tReal4      = "real4"
	tFloat4     = "float4"
	tFloat8     = "float8"
	tBool       = "bool"
	tClob       = "clob"
	tBytes      = "bytes"
	tReal8      = "real8"
	tNclob      = "nclob"
	tTinyint1   = "tinyint(1)"
)

func kindByBase() map[string]Kind {
	return map[string]Kind{
		tTinyint:                 KindInt,
		tSmallint:                KindInt,
		tMediumint:               KindInt,
		tInt:                     KindInt,
		tInteger:                 KindInt,
		tBigint:                  KindInt,
		"year":                   KindInt,
		tSerial:                  KindInt,
		tBigserial:               KindInt,
		"smallserial":            KindInt,
		tInt2:                    KindInt,
		tInt4:                    KindInt,
		tInt8:                    KindInt,
		tDecimal:                 KindDecimal,
		tNumeric:                 KindDecimal,
		"dec":                    KindDecimal,
		"fixed":                  KindDecimal,
		"money":                  KindDecimal,
		tFloat:                   KindFloat,
		tDouble:                  KindFloat,
		tDoublePrec:              KindFloat,
		tReal:                    KindFloat,
		tFloat4:                  KindFloat,
		tFloat8:                  KindFloat,
		tBoolean:                 KindBool,
		tBool:                    KindBool,
		tDate:                    KindTime,
		tDatetime:                KindTime,
		tTimestamp:               KindTime,
		tTime:                    KindTime,
		tTimestampN:              KindTime,
		tTimestampZ:              KindTime,
		"time without time zone": KindTime,
		"time with time zone":    KindTime,
		"timestamptz":            KindTime,
		"timetz":                 KindTime,
		tBinary:                  KindBytes,
		tVarbinary:               KindBytes,
		tTinyblob:                KindBytes,
		tBlob:                    KindBytes,
		tMediumblob:              KindBytes,
		tLongblob:                KindBytes,
		tBytea:                   KindBytes,
		"bit":                    KindBytes,
		"geometry":               KindBytes,
		tJSON:                    KindJSON,
		tJSONB:                   KindJSON,
	}
}

// KindOf classifies an engine-native type.
func KindOf(native string) Kind {
	return kindByBase()[parseNative(native).base]
}

func mysqlToPGBase() map[string]string {
	return map[string]string{
		tTinyint: tSmallint, tMediumint: tInteger, "year": tSmallint, tFloat: tReal, tDouble: tDoublePrec,
		tDoublePrec: tDoublePrec, tDatetime: tTimestamp, tTimestamp: tTimestamp, tDate: tDate, tTime: tTime,
		tBinary: tBytea, tVarbinary: tBytea, tTinyblob: tBytea, tBlob: tBytea, tMediumblob: tBytea,
		tLongblob: tBytea, "bit": tBytea, "geometry": tBytea, tJSON: tJSONB,
	}
}

func mysqlToPG(p parsedType) string {
	switch p.base {
	case tSmallint:
		if p.unsigned {
			return tInteger
		}

		return tSmallint
	case tInt, tInteger:
		if p.unsigned {
			return tBigint
		}

		return tInteger
	case tBigint:
		if p.unsigned {
			return "numeric(20)"
		}

		return tBigint
	case tVarchar, tChar:
		if p.args == "" {
			return tText
		}

		return p.base + "(" + p.args + ")"
	case tDecimal, tNumeric, "dec", "fixed":
		return numericWithArgs(p.args)
	}

	if v, ok := mysqlToPGBase()[p.base]; ok {
		return v
	}

	return tText
}

func numericWithArgs(args string) string {
	if args == "" {
		return tNumeric
	}

	return "numeric(" + args + ")"
}

func pgToMySQLBase() map[string]string {
	return map[string]string{
		tSmallint: tSmallint, tInt2: tSmallint, "smallserial": tSmallint, tInteger: tInt, tInt: tInt, tInt4: tInt,
		tSerial: tInt, tBigint: tBigint, tInt8: tBigint, tBigserial: tBigint, tBoolean: tTinyint1,
		tBool: tTinyint1, "money": "decimal(19,2)", tReal: tFloat, tFloat4: tFloat, tDoublePrec: tDouble,
		tFloat8: tDouble, tTimestampN: tDatetime6, tTimestampZ: tDatetime6, tTimestamp: tDatetime6,
		"timestamptz": tDatetime6, tDate: tDate, "time without time zone": tTime, "time with time zone": tTime,
		tTime: tTime, "timetz": tTime, tBytea: tLongblob, tJSON: tJSON, tJSONB: tJSON, "uuid": "char(36)",
	}
}

func pgToMySQL(p parsedType) string {
	switch p.base {
	case "character varying", tVarchar:
		if p.args == "" {
			return tLongtext
		}

		return "varchar(" + p.args + ")"
	case "character", tChar, "bpchar":
		if p.args == "" {
			return "char(1)"
		}

		return "char(" + p.args + ")"
	case tNumeric, tDecimal:
		if p.args == "" {
			return "decimal(65,30)"
		}

		return "decimal(" + p.args + ")"
	}

	if v, ok := pgToMySQLBase()[p.base]; ok {
		return v
	}

	return tLongtext
}

// TargetType renders the column type to use on a target of family dst when the source is src.
func TargetType(src, dst Family, native string) string {
	if src == dst {
		return native
	}

	p := parseNative(native)

	switch {
	case src == FamilyMySQL && dst == FamilyPostgres:
		return mysqlToPG(p)
	case src == FamilyPostgres && dst == FamilyMySQL:
		return pgToMySQL(p)
	default:
		return native
	}
}

// QuoteIdent quotes an identifier for the family's dialect.
func QuoteIdent(f Family, name string) string {
	if f == FamilyPostgres {
		return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
	}

	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}
