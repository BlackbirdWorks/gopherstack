package iot

import (
	"maps"
)

type funcImpl func(c *sqlCtx, args []any) any

type funcDef struct {
	impl      funcImpl
	since2016 bool
}

// funcTable lists every supported SQL function by lower-case name.
func funcTable() map[string]funcDef {
	t := map[string]funcDef{}

	maps.Copy(t, mathFuncs())
	maps.Copy(t, stringFuncs())
	maps.Copy(t, messageFuncs())
	maps.Copy(t, valueFuncs())
	maps.Copy(t, hashFuncs())
	t["cast"] = funcDef{impl: func(*sqlCtx, []any) any { return sqlUndefined{} }}

	return t
}

func arg(args []any, i int) any {
	if i < len(args) {
		return args[i]
	}

	return sqlUndefined{}
}

func strArg(args []any, i int) (string, bool) { return toStringConv(arg(args, i)) }

func validCastType(t string, v2016 bool) bool {
	switch t {
	case "string", "nvarchar", "text", "ntext", "varchar", "int", "integer", "double":
		return true
	case "decimal", "bool", "boolean":
		return v2016
	}

	return false
}

// castFunc implements cast(x AS type) per the documented casting rules.
func castFunc(typ string) funcImpl {
	return func(c *sqlCtx, args []any) any {
		v := arg(args, 0)

		switch typ {
		case "string", "nvarchar", "text", "ntext", "varchar":
			if s, ok := toStringConv(v); ok {
				return s
			}
		case "int", "integer":
			return castInt(v)
		case "double", "decimal":
			if f, ok := castDecimal(v); ok {
				return c.num(f)
			}
		case "bool", "boolean":
			return castBool(v)
		}

		return sqlUndefined{}
	}
}

func castDecimal(v any) (float64, bool) {
	if b, ok := v.(bool); ok {
		if b {
			return 1, true
		}

		return 0, true
	}

	return toDecimal(v)
}

func castInt(v any) any {
	if b, ok := v.(bool); ok {
		if b {
			return int64(1)
		}

		return int64(0)
	}

	if f, ok := v.(float64); ok {
		if i, fok := floatToInt(floorF(f)); fok {
			return i
		}

		return sqlUndefined{}
	}

	if i, ok := toIntConv(v); ok {
		return i
	}

	return sqlUndefined{}
}

func castBool(v any) any {
	switch t := v.(type) {
	case int64:
		return t != 0
	case float64:
		return t != 0
	}

	if b, ok := toBoolConv(v); ok {
		return b
	}

	return sqlUndefined{}
}
