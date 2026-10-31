package timestreamquery

import (
	"slices"
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
	typBigint       = "bigint"
	typBoolean      = "boolean"
	typDouble       = "double"
	typTimestamp    = "timestamp"
	typVarchar      = "varchar"
	typMulti        = "multi"
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
		return typBigint
	case measureTypeBoolean:
		return typBoolean
	case measureTypeTimestamp:
		return typTimestamp
	case measureTypeVarchar:
		return typVarchar
	default:
		return typDouble
	}
}

func isMulti(r SourceRecord) bool { return strings.EqualFold(r.MeasureValueType, measureTypeMulti) }

func singleColumnOrder() []string {
	return []string{typBigint, typBoolean, typDouble, typTimestamp, typVarchar}
}

type schemaBuilder struct {
	seenDim   map[string]bool
	multiType map[string]string
	single    map[string]bool
	minfo     map[string]*measureInfo
	dims      []string
	multiName []string
	order     []string
}

func (sb *schemaBuilder) measure(name string) *measureInfo {
	mi := sb.minfo[name]
	if mi == nil {
		mi = &measureInfo{name: name}
		sb.minfo[name] = mi
		sb.order = append(sb.order, name)
	}

	return mi
}

func (sb *schemaBuilder) add(r SourceRecord) {
	mi := sb.measure(r.MeasureName)

	for _, d := range r.Dimensions {
		if !sb.seenDim[d.Name] {
			sb.seenDim[d.Name] = true
			sb.dims = append(sb.dims, d.Name)
		}

		if !slices.Contains(mi.dims, d.Name) {
			mi.dims = append(mi.dims, d.Name)
		}
	}

	if !isMulti(r) {
		t := scalarTypeOf(r.MeasureValueType)
		sb.single[t] = true
		mi.typ = t

		return
	}

	mi.typ = typMulti

	for _, m := range r.Measures {
		if _, ok := sb.multiType[m.Name]; !ok {
			sb.multiType[m.Name] = scalarTypeOf(m.Type)
			sb.multiName = append(sb.multiName, m.Name)
		}
	}
}

// deriveSchema builds the table schema from its records: dimensions in order of
// first appearance, then measure_name, time, then one measure_value::T column per
// single-measure type seen and one column per MULTI attribute.
func deriveSchema(records []SourceRecord) tableSchema {
	sb := &schemaBuilder{
		seenDim: map[string]bool{}, multiType: map[string]string{},
		single: map[string]bool{}, minfo: map[string]*measureInfo{},
	}
	for _, r := range records {
		sb.add(r)
	}

	s := tableSchema{}
	for _, d := range sb.dims {
		s.cols = append(s.cols, tsColumn{name: d, typ: typVarchar, attr: attrDimension})
	}

	s.cols = append(s.cols,
		tsColumn{name: colMeasureName, typ: typVarchar, attr: attrMeasureName},
		tsColumn{name: colTime, typ: typTimestamp, attr: attrTimestamp},
	)

	for _, t := range singleColumnOrder() {
		if sb.single[t] {
			s.cols = append(s.cols, tsColumn{name: colMeasurePfx + t, typ: t, attr: attrMeasureVal})
		}
	}

	for _, n := range sb.multiName {
		if sb.seenDim[n] || n == colTime || n == colMeasureName {
			continue
		}

		s.cols = append(s.cols, tsColumn{name: n, typ: sb.multiType[n], attr: attrMulti})
	}

	for _, n := range sb.order {
		s.measures = append(s.measures, *sb.minfo[n])
	}

	sort.Slice(s.measures, func(i, j int) bool { return s.measures[i].name < s.measures[j].name })

	return s
}

func declOf(typ string) string {
	switch typ {
	case typBigint:
		return declBigint
	case typBoolean:
		return declBoolean
	case typTimestamp:
		return declTimestamp
	case typVarchar:
		return declVarchar
	default:
		return declDouble
	}
}

func formatTS(t time.Time) string { return t.UTC().Format(tsLayout) }

// sqlValue converts a stored measure string into the value bound to its SQLite column.
func sqlValue(typ, v string) any {
	switch typ {
	case typBigint:
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil
		}

		return n
	case typBoolean:
		if strings.EqualFold(v, "true") {
			return int64(1)
		}

		return int64(0)
	case typDouble:
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
