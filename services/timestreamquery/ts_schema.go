package timestreamquery

import (
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	tsLayout        = "2006-01-02 15:04:05.000000000"
	colMeasureName  = "measure_name"
	colTime         = "time"
	colMeasurePfx   = "measure_value::"
	declTimestamp   = "TIMESTAMP_TEXT"
	declVarchar     = "VARCHAR"
	declBigint      = "BIGINT"
	declDouble      = "DOUBLE"
	declBoolean     = "BOOLEAN"
	attrDimension   = "DIMENSION"
	attrMeasureName = "MEASURE_NAME"
	attrTimestamp   = "TIMESTAMP"
	attrMeasureVal  = "MEASURE_VALUE"
	attrMulti       = "MULTI"
)

// tsColumn is one column of a table's derived schema; typ is a lower-case Timestream type.
type tsColumn struct {
	name string
	typ  string
	attr string
}

// measureInfo describes one measure_name for SHOW MEASURES.
type measureInfo struct {
	name string
	typ  string
	dims []string
}

type tableSchema struct {
	cols     []tsColumn
	measures []measureInfo
}

func scalarTypeOf(measureType string) string {
	switch strings.ToUpper(measureType) {
	case measureTypeBigint:
		return "bigint"
	case measureTypeBoolean:
		return "boolean"
	case measureTypeTimestamp:
		return "timestamp"
	case measureTypeVarchar:
		return "varchar"
	default:
		return "double"
	}
}

func isMulti(r SourceRecord) bool { return strings.EqualFold(r.MeasureValueType, measureTypeMulti) }

var singleColumnOrder = []string{"bigint", "boolean", "double", "timestamp", "varchar"}

// deriveSchema builds the table schema from its records: dimensions in order of
// first appearance, then measure_name, time, then one measure_value::T column per
// single-measure type seen and one column per MULTI attribute.
func deriveSchema(records []SourceRecord) tableSchema {
	var dims, multiNames []string

	seenDim := map[string]bool{}
	multiType := map[string]string{}
	single := map[string]bool{}
	minfo := map[string]*measureInfo{}

	var order []string

	for _, r := range records {
		mi := minfo[r.MeasureName]
		if mi == nil {
			mi = &measureInfo{name: r.MeasureName}
			minfo[r.MeasureName] = mi
			order = append(order, r.MeasureName)
		}

		for _, d := range r.Dimensions {
			if !seenDim[d.Name] {
				seenDim[d.Name] = true
				dims = append(dims, d.Name)
			}

			if !containsStr(mi.dims, d.Name) {
				mi.dims = append(mi.dims, d.Name)
			}
		}

		if isMulti(r) {
			mi.typ = "multi"

			for _, m := range r.Measures {
				if _, ok := multiType[m.Name]; !ok {
					multiType[m.Name] = scalarTypeOf(m.Type)
					multiNames = append(multiNames, m.Name)
				}
			}

			continue
		}

		t := scalarTypeOf(r.MeasureValueType)
		single[t] = true
		mi.typ = t
	}

	s := tableSchema{}
	for _, d := range dims {
		s.cols = append(s.cols, tsColumn{name: d, typ: "varchar", attr: attrDimension})
	}

	s.cols = append(s.cols,
		tsColumn{name: colMeasureName, typ: "varchar", attr: attrMeasureName},
		tsColumn{name: colTime, typ: "timestamp", attr: attrTimestamp},
	)

	for _, t := range singleColumnOrder {
		if single[t] {
			s.cols = append(s.cols, tsColumn{name: colMeasurePfx + t, typ: t, attr: attrMeasureVal})
		}
	}

	for _, n := range multiNames {
		if seenDim[n] || n == colTime || n == colMeasureName {
			continue
		}

		s.cols = append(s.cols, tsColumn{name: n, typ: multiType[n], attr: attrMulti})
	}

	for _, n := range order {
		s.measures = append(s.measures, *minfo[n])
	}

	sort.Slice(s.measures, func(i, j int) bool { return s.measures[i].name < s.measures[j].name })

	return s
}

func containsStr(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}

	return false
}

func declOf(typ string) string {
	switch typ {
	case "bigint":
		return declBigint
	case "boolean":
		return declBoolean
	case "timestamp":
		return declTimestamp
	case "varchar":
		return declVarchar
	default:
		return declDouble
	}
}

func formatTS(t time.Time) string { return t.UTC().Format(tsLayout) }

// sqlValue converts a stored measure string into the value bound to its SQLite column.
func sqlValue(typ, v string) any {
	switch typ {
	case "bigint":
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil
		}

		return n
	case "boolean":
		if strings.EqualFold(v, "true") {
			return int64(1)
		}

		return int64(0)
	case "double":
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return nil
		}

		return f
	default:
		return v
	}
}

// recordBytes approximates the stored size of a record, used for bytes-scanned accounting.
func recordBytes(r SourceRecord) int64 {
	const timeBytes = 8

	n := int64(timeBytes + len(r.MeasureName) + len(r.MeasureValue))
	for _, d := range r.Dimensions {
		n += int64(len(d.Name) + len(d.Value))
	}

	for _, m := range r.Measures {
		n += int64(len(m.Name) + len(m.Value))
	}

	return n
}

// rowValues returns the record's values aligned with the schema columns.
func (s tableSchema) rowValues(r SourceRecord) []any {
	dim := make(map[string]string, len(r.Dimensions))
	for _, d := range r.Dimensions {
		dim[d.Name] = d.Value
	}

	multi := make(map[string]SourceMeasure, len(r.Measures))
	for _, m := range r.Measures {
		multi[m.Name] = m
	}

	out := make([]any, len(s.cols))

	for i, c := range s.cols {
		switch c.attr {
		case attrDimension:
			if v, ok := dim[c.name]; ok {
				out[i] = v
			}
		case attrMeasureName:
			out[i] = r.MeasureName
		case attrTimestamp:
			out[i] = formatTS(r.Time)
		case attrMeasureVal:
			if !isMulti(r) && scalarTypeOf(r.MeasureValueType) == c.typ {
				out[i] = sqlValue(c.typ, r.MeasureValue)
			}
		case attrMulti:
			if m, ok := multi[c.name]; ok && isMulti(r) {
				out[i] = sqlValue(c.typ, m.Value)
			}
		}
	}

	return out
}
