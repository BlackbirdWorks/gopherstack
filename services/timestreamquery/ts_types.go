package timestreamquery

import "strings"

type tsKind uint8

const (
	tyInvalid tsKind = iota
	tyUnknown
	tyBoolean
	tyInteger
	tyBigint
	tyDouble
	tyVarchar
	tyDate
	tyTime
	tyTimestamp
	tyIntervalDS
	tyIntervalYM
	tyArray
	tyRow
	tyTimeseries
)

type tsField struct {
	name string
	typ  tsType
}

// tsType is a Timestream type; the zero value means the type could not be inferred.
type tsType struct {
	elem   *tsType
	fields []tsField
	kind   tsKind
}

func scalar(k tsKind) tsType { return tsType{kind: k} }

func arrayOf(e tsType) tsType { return tsType{kind: tyArray, elem: &e} }

func seriesOf(e tsType) tsType { return tsType{kind: tyTimeseries, elem: &e} }

func rowOf(fields []tsField) tsType { return tsType{kind: tyRow, fields: fields} }

// known reports whether the type and every nested component were inferred.
func (t tsType) known() bool {
	switch t.kind {
	case tyInvalid:
		return false
	case tyArray, tyTimeseries:
		return t.elem != nil && t.elem.known()
	case tyRow:
		for _, f := range t.fields {
			if !f.typ.known() {
				return false
			}
		}
	default:
	}

	return true
}

func (t tsType) isNumeric() bool {
	return t.kind == tyInteger || t.kind == tyBigint || t.kind == tyDouble
}

func (t tsType) isInterval() bool { return t.kind == tyIntervalDS || t.kind == tyIntervalYM }

func (t tsType) isTemporal() bool {
	return t.kind == tyDate || t.kind == tyTimestamp || t.kind == tyTime
}

func scalarName(k tsKind) string {
	switch k {
	case tyUnknown:
		return ScalarTypeUnknown
	case tyBoolean:
		return ScalarTypeBoolean
	case tyInteger:
		return "INTEGER"
	case tyBigint:
		return ScalarTypeBigint
	case tyDouble:
		return ScalarTypeDouble
	case tyVarchar:
		return ScalarTypeVarchar
	case tyDate:
		return ScalarTypeDate
	case tyTime:
		return ScalarTypeTime
	case tyTimestamp:
		return ScalarTypeTimestamp
	case tyIntervalDS:
		return ScalarTypeIntervalDayToSecond
	case tyIntervalYM:
		return ScalarTypeIntervalYearToMonth
	default:
		return ""
	}
}

func scalarTypeofName(k tsKind) string {
	switch k {
	case tyIntervalDS:
		return "interval day to second"
	case tyIntervalYM:
		return "interval year to month"
	case tyInteger:
		return kwInteger
	default:
		return strings.ToLower(scalarName(k))
	}
}

// typeofName spells the type the way typeof() prints it.
func (t tsType) typeofName() string {
	switch t.kind {
	case tyArray:
		return "array(" + t.elem.typeofName() + ")"
	case tyTimeseries:
		return "timeseries(" + t.elem.typeofName() + ")"
	case tyRow:
		parts := make([]string, len(t.fields))
		for i, f := range t.fields {
			parts[i] = f.typ.typeofName()
			if f.name != "" {
				parts[i] = f.name + " " + parts[i]
			}
		}

		return "row(" + strings.Join(parts, ", ") + ")"
	default:
		return scalarTypeofName(t.kind)
	}
}

func (t tsType) columnType() ColumnType {
	switch t.kind {
	case tyArray:
		return ColumnType{ArrayColumnInfo: &ColumnInfo{Type: t.elem.columnType()}}
	case tyTimeseries:
		return ColumnType{TimeSeriesMeasureValueColumnInfo: &ColumnInfo{Type: t.elem.columnType()}}
	case tyRow:
		fields := make([]ColumnInfo, len(t.fields))
		for i, f := range t.fields {
			fields[i] = ColumnInfo{Name: f.name, Type: f.typ.columnType()}
		}

		return ColumnType{RowColumnInfo: fields}
	default:
		return ColumnType{ScalarType: scalarName(t.kind)}
	}
}

func (t tsType) columnInfo(name string) ColumnInfo {
	return ColumnInfo{Name: name, Type: t.columnType()}
}

func typeFromDecl(decl string) tsType {
	switch decl {
	case typBigint:
		return scalar(tyBigint)
	case typBoolean:
		return scalar(tyBoolean)
	case typDouble:
		return scalar(tyDouble)
	case typTimestamp:
		return scalar(tyTimestamp)
	case typVarchar:
		return scalar(tyVarchar)
	default:
		return tsType{}
	}
}

func numericRank(k tsKind) int {
	switch k {
	case tyInteger:
		return 1
	case tyBigint:
		return argsTwo
	case tyDouble:
		return argsThree
	default:
		return 0
	}
}

// supertype is the common type two values coerce to; the zero type when none exists.
func supertype(a, b tsType) tsType {
	switch {
	case a.kind == tyInvalid || b.kind == tyInvalid:
		return tsType{}
	case a.kind == tyUnknown:
		return b
	case b.kind == tyUnknown:
		return a
	case a.isNumeric() && b.isNumeric():
		if numericRank(a.kind) >= numericRank(b.kind) {
			return a
		}

		return b
	case a.isTemporal() && b.isTemporal() && a.kind != b.kind:
		return superTemporal(a, b)
	case a.kind != b.kind:
		return tsType{}
	}

	return superSame(a, b)
}

func superTemporal(a, b tsType) tsType {
	if a.kind != tyTime && b.kind != tyTime {
		return scalar(tyTimestamp)
	}

	return tsType{}
}

func superSame(a, b tsType) tsType {
	switch a.kind {
	case tyArray, tyTimeseries:
		e := supertype(*a.elem, *b.elem)
		if e.kind == tyInvalid {
			return tsType{}
		}

		return tsType{kind: a.kind, elem: &e}
	case tyRow:
		return superRow(a, b)
	default:
		return a
	}
}

func superRow(a, b tsType) tsType {
	if len(a.fields) != len(b.fields) {
		return tsType{}
	}

	fields := make([]tsField, len(a.fields))

	for i, f := range a.fields {
		st := supertype(f.typ, b.fields[i].typ)
		if st.kind == tyInvalid {
			return tsType{}
		}

		fields[i] = tsField{name: f.name, typ: st}
	}

	return rowOf(fields)
}
