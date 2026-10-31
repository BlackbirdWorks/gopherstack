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

// KindOf classifies an engine-native type.
func KindOf(native string) Kind {
	p := parseNative(native)

	switch p.base {
	case "tinyint", "smallint", "mediumint", "int", "integer", "bigint", "year", "serial", "bigserial", "smallserial",
		"int2", "int4", "int8":
		return KindInt
	case "decimal", "numeric", "dec", "fixed", "money":
		return KindDecimal
	case "float", "double", "double precision", "real", "float4", "float8":
		return KindFloat
	case "boolean", "bool":
		return KindBool
	case "date", "datetime", "timestamp", "time", "timestamp without time zone", "timestamp with time zone",
		"time without time zone", "time with time zone", "timestamptz", "timetz":
		return KindTime
	case "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob", "bytea", "bit", "geometry":
		return KindBytes
	case "json", "jsonb":
		return KindJSON
	default:
		return KindString
	}
}

func mysqlToPG(p parsedType) string {
	switch p.base {
	case "tinyint", "smallint":
		if p.unsigned && p.base == "smallint" {
			return "integer"
		}

		return "smallint"
	case "mediumint":
		return "integer"
	case "int", "integer":
		if p.unsigned {
			return "bigint"
		}

		return "integer"
	case "bigint":
		if p.unsigned {
			return "numeric(20)"
		}

		return "bigint"
	case "year":
		return "smallint"
	case "varchar", "char":
		if p.args == "" {
			return "text"
		}

		return map[string]string{"varchar": "varchar", "char": "char"}[p.base] + "(" + p.args + ")"
	case "decimal", "numeric", "dec", "fixed":
		return numericWithArgs(p.args)
	case "float":
		return "real"
	case "double", "double precision":
		return "double precision"
	case "datetime", "timestamp":
		return "timestamp"
	case "date":
		return "date"
	case "time":
		return "time"
	case "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob":
		return "bytea"
	case "json":
		return "jsonb"
	case "bit", "geometry":
		return "bytea"
	default:
		return "text"
	}
}

func numericWithArgs(args string) string {
	if args == "" {
		return "numeric"
	}

	return "numeric(" + args + ")"
}

func pgToMySQL(p parsedType) string {
	switch p.base {
	case "smallint", "int2", "smallserial":
		return "smallint"
	case "integer", "int", "int4", "serial":
		return "int"
	case "bigint", "int8", "bigserial":
		return "bigint"
	case "boolean", "bool":
		return "tinyint(1)"
	case "character varying", "varchar":
		if p.args == "" {
			return "longtext"
		}

		return "varchar(" + p.args + ")"
	case "character", "char", "bpchar":
		if p.args == "" {
			return "char(1)"
		}

		return "char(" + p.args + ")"
	case "numeric", "decimal":
		if p.args == "" {
			return "decimal(65,30)"
		}

		return "decimal(" + p.args + ")"
	case "money":
		return "decimal(19,2)"
	case "real", "float4":
		return "float"
	case "double precision", "float8":
		return "double"
	case "timestamp without time zone", "timestamp with time zone", "timestamp", "timestamptz":
		return "datetime(6)"
	case "date":
		return "date"
	case "time without time zone", "time with time zone", "time", "timetz":
		return "time"
	case "bytea":
		return "longblob"
	case "json", "jsonb":
		return "json"
	case "uuid":
		return "char(36)"
	default:
		return "longtext"
	}
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
